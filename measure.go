// Package measure provides a Datastore wrapper that records metrics
// using github.com/ipfs/go-metrics-interface
package measure

import (
	"io"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-metrics-interface"
)

var (
	// sort latencies in buckets with following upper bounds in seconds
	datastoreLatencyBuckets = []float64{1e-4, 1e-3, 1e-2, 1e-1, 1}

	// sort sizes in buckets with following upper bounds in bytes
	datastoreSizeBuckets = []float64{1 << 6, 1 << 12, 1 << 18, 1 << 24}
)

// New wraps the datastore, providing metrics on the operations. The
// metrics are registered with names starting with prefix and a dot.
func New(prefix string, bs blockstore.Blockstore) *measure {
	m := &measure{
		backend: bs,

		putNum: metrics.New(prefix+".put_total", "Total number of Datastore.Put calls").Counter(),
		putErr: metrics.New(prefix+".put.errors_total", "Number of errored Blockstore.Put calls").Counter(),
		putLatency: metrics.New(prefix+".put.latency_seconds",
			"Latency distribution of Blockstore.Put calls").Histogram(datastoreLatencyBuckets),
		putSize: metrics.New(prefix+".put.size_bytes",
			"Size distribution of stored byte slices").Histogram(datastoreSizeBuckets),

		syncNum: metrics.New(prefix+".sync_total", "Total number of Blockstore.Sync calls").Counter(),
		syncErr: metrics.New(prefix+".sync.errors_total", "Number of errored Blockstore.Sync calls").Counter(),
		syncLatency: metrics.New(prefix+".sync.latency_seconds",
			"Latency distribution of Blockstore.Sync calls").Histogram(datastoreLatencyBuckets),

		getNum: metrics.New(prefix+".get_total", "Total number of Blockstore.Get calls").Counter(),
		getErr: metrics.New(prefix+".get.errors_total", "Number of errored Blockstore.Get calls").Counter(),
		getLatency: metrics.New(prefix+".get.latency_seconds",
			"Latency distribution of Blockstore.Get calls").Histogram(datastoreLatencyBuckets),
		getSize: metrics.New(prefix+".get.size_bytes",
			"Size distribution of retrieved byte slices").Histogram(datastoreSizeBuckets),

		hasNum: metrics.New(prefix+".has_total", "Total number of Blockstore.Has calls").Counter(),
		hasErr: metrics.New(prefix+".has.errors_total", "Number of errored Blockstore.Has calls").Counter(),
		hasLatency: metrics.New(prefix+".has.latency_seconds",
			"Latency distribution of Blockstore.Has calls").Histogram(datastoreLatencyBuckets),
		getsizeNum: metrics.New(prefix+".getsize_total", "Total number of Blockstore.GetSize calls").Counter(),
		getsizeErr: metrics.New(prefix+".getsize.errors_total", "Number of errored Blockstore.GetSize calls").Counter(),
		getsizeLatency: metrics.New(prefix+".getsize.latency_seconds",
			"Latency distribution of Blockstore.GetSize calls").Histogram(datastoreLatencyBuckets),

		deleteNum: metrics.New(prefix+".delete_total", "Total number of Blockstore.Delete calls").Counter(),
		deleteErr: metrics.New(prefix+".delete.errors_total", "Number of errored Blockstore.Delete calls").Counter(),
		deleteLatency: metrics.New(prefix+".delete.latency_seconds",
			"Latency distribution of Blockstore.Delete calls").Histogram(datastoreLatencyBuckets),

		viewNum: metrics.New(prefix+".view_total", "Total number of Blockstore.View calls").Counter(),
		viewErr: metrics.New(prefix+".view.errors_total", "Number of errored Blockstore.View calls").Counter(),
		viewLatency: metrics.New(prefix+".view.latency_seconds",
			"Latency distribution of Blockstore.View calls").Histogram(datastoreLatencyBuckets),
	}
	return m
}

type measure struct {
	backend blockstore.Blockstore

	putNum     metrics.Counter
	putErr     metrics.Counter
	putLatency metrics.Histogram
	putSize    metrics.Histogram

	syncNum     metrics.Counter
	syncErr     metrics.Counter
	syncLatency metrics.Histogram

	getNum     metrics.Counter
	getErr     metrics.Counter
	getLatency metrics.Histogram
	getSize    metrics.Histogram

	hasNum     metrics.Counter
	hasErr     metrics.Counter
	hasLatency metrics.Histogram

	getsizeNum     metrics.Counter
	getsizeErr     metrics.Counter
	getsizeLatency metrics.Histogram

	deleteNum     metrics.Counter
	deleteErr     metrics.Counter
	deleteLatency metrics.Histogram

	viewNum     metrics.Counter
	viewErr     metrics.Counter
	viewLatency metrics.Histogram
}

func recordLatency(h metrics.Histogram, start time.Time) {
	elapsed := time.Since(start)
	h.Observe(elapsed.Seconds())
}

func (m *measure) Put(blk blocks.Block) error {
	defer recordLatency(m.putLatency, time.Now())
	m.putNum.Inc()
	m.putSize.Observe(float64(len(blk.RawData())))
	err := m.backend.Put(blk)
	if err != nil {
		m.putErr.Inc()
	}
	return err
}

/*
func (m *measure) Sync(prefix datastore.Key) error {
	defer recordLatency(m.syncLatency, time.Now())
	m.syncNum.Inc()
	err := m.backend.Sync(prefix)
	if err != nil {
		m.syncErr.Inc()
	}
	return err
}
*/

func (m *measure) Get(c cid.Cid) (blocks.Block, error) {
	defer recordLatency(m.getLatency, time.Now())
	m.getNum.Inc()
	value, err := m.backend.Get(c)
	switch err {
	case nil:
		m.getSize.Observe(float64(len(value.RawData())))
	case datastore.ErrNotFound:
		// Not really an error.
	default:
		m.getErr.Inc()
	}
	return value, err
}

func (m *measure) Has(c cid.Cid) (bool, error) {
	defer recordLatency(m.hasLatency, time.Now())
	m.hasNum.Inc()
	exists, err := m.backend.Has(c)
	if err != nil {
		m.hasErr.Inc()
	}
	return exists, err
}

func (m *measure) GetSize(c cid.Cid) (size int, err error) {
	defer recordLatency(m.getsizeLatency, time.Now())
	m.getsizeNum.Inc()
	size, err = m.backend.GetSize(c)
	switch err {
	case nil, datastore.ErrNotFound:
		// Not really an error.
	default:
		m.getsizeErr.Inc()
	}
	return size, err
}

func (m *measure) DeleteBlock(c cid.Cid) error {
	defer recordLatency(m.deleteLatency, time.Now())
	m.deleteNum.Inc()
	err := m.backend.DeleteBlock(c)
	if err != nil {
		m.deleteErr.Inc()
	}
	return err
}

func (m *measure) Close() error {
	if c, ok := m.backend.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

type bsViewer interface {
	View(c cid.Cid, f func([]byte) error) error
}

func (m *measure) View(c cid.Cid, f func([]byte) error) error {
	v, ok := m.backend.(bsViewer)
	if !ok {
		blk, err := m.Get(c)
		if err != nil {
			return err
		}
		return f(blk.RawData())
	}

	defer recordLatency(m.viewLatency, time.Now())
	m.viewNum.Inc()
	err := v.View(c, f)
	switch err {
	case nil, datastore.ErrNotFound:
		// Not really an error.
	default:
		m.viewErr.Inc()
	}
	return err

}
