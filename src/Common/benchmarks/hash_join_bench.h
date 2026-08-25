#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <base/types.h>

#include <Core/Block.h>
#include <Interpreters/IJoin.h>
#include <Common/ThreadPool.h>

namespace DB
{
class TableJoin;
}

namespace DB::JoinBench
{

/// Prevents the compiler from optimizing benchmark kernels away.
extern std::atomic<UInt64> g_sink;

/// ClickHouse thread pool (threads carry ThreadStatus, so per-thread memory tracking is cheap).
/// Threads stay warm between runs (max_free_threads == max_threads), so per-iteration timing
/// does not include thread creation.
using SimpleThreadPool = ThreadPoolImpl<ThreadFromGlobalPoolImpl</*propagate_opentelemetry_context*/ false, /*global_trace_collector_allowed*/ false>>;

class WorkerPool
{
public:
    explicit WorkerPool(size_t num_threads_);

    /// Runs task(thread_index) on all threads. Returns elapsed wall seconds.
    double run(const std::function<void(size_t)> & task);

    size_t size() const { return num_threads; }

private:
    size_t num_threads;
    SimpleThreadPool pool;
};

/// Snapshot of production-code per-phase JOIN probe timings (ProfileEvents incremented by the
/// real HashJoin/ConcurrentHashJoin code paths that IJoinBench::probe drives): hash probe/match
/// building, output gather/materialization, and (NPHJ only, before two-level maps are merged)
/// probe-block dispatch across slots. These are the exact same ProfileEvents a real query
/// reports (e.g. via system.query_log), so subtracting two snapshots around a benchmark probe()
/// call gives a phase breakdown directly comparable to a real query's, instead of only the
/// fused probe+gather total `JoinStats::probe_sec` measures.
struct ProbeProfile
{
    double match_sec = 0;    /// HashJoinProbeMatchMicroseconds: hash lookups, building matched-row lists.
    double gather_sec = 0;   /// HashJoinProbeGatherMicroseconds: output block materialization.
    double dispatch_sec = 0; /// ConcurrentHashJoinProbeDispatchMicroseconds: NPHJ-only slot scatter.

    ProbeProfile operator-(const ProbeProfile & before) const
    {
        return {match_sec - before.match_sec, gather_sec - before.gather_sec, dispatch_sec - before.dispatch_sec};
    }
};

/// Current cumulative values (summed over all threads, monotonically increasing) of the
/// ProfileEvents behind ProbeProfile; take the difference of two snapshots for one timed region.
ProbeProfile currentProbeProfile();

struct JoinStats
{
    double build_sec = 0;
    double probe_sec = 0;
    double teardown_sec = 0; /// join-state destruction, timed separately (see IJoinBench::teardown)
    size_t matches = 0;
    UInt64 fingerprint = 0; /// order-independent digest of the output rows (0 unless verified)
    ProbeProfile probe_profile; /// production-code phase breakdown of probe_sec, see ProbeProfile

    /// Excludes teardown_sec on purpose: teardown is reported, not added, mirroring the
    /// production pipeline, where `IJoin` teardown happens at pipeline destruction, after the
    /// last output block has already been handed off.
    double total() const { return build_sec + probe_sec; }
};

/// One join algorithm under test. Implementations use the driver's worker pool internally;
/// the driver times the two phases.
class IJoinBench
{
public:
    virtual ~IJoinBench() = default;
    virtual std::string name() const = 0;

    /// Consume the build (right) side.
    virtual void build(const std::vector<Block> & blocks) = 0;

    /// Join the probe (left) side, materializing real output Blocks (dropped after counting).
    /// Returns the number of output rows. If `fingerprint` is non-null, additionally
    /// accumulates an order-independent digest of all output rows into it (adds overhead
    /// to the probe timing).
    virtual size_t probe(const std::vector<Block> & blocks, UInt64 * fingerprint) = 0;

    /// Optional sub-phase timing details for reporting.
    virtual std::string phaseBreakdown() const { return {}; }

    /// Releases all build/probe state (hash tables, stored blocks). Timed separately by the
    /// driver, mirroring production: `IJoin` teardown happens at pipeline destruction, after
    /// the last output block, for both competitors.
    virtual void teardown() {}
};

/// Order-independent digest of a Block's rows: commutative over rows (any output order) and
/// over columns (values are bound to column names), but sensitive to cross-column row pairing.
UInt64 blockFingerprint(const Block & block);

/// A partition holds a list of scattered column chunks.
struct Chunk
{
    Columns columns;
    size_t rows = 0;
};
using ChunkList = std::vector<Chunk>;

/// Hard memory-correctness ceiling of the SWWC scatter, not a runtime tuning knob: at fanout F
/// each worker's per-partition SWWC state is F * (64 B staging + 8 B cursor + 4 B fill) ~= 76 B,
/// so F = 8192 needs ~608 KiB, which only fits an L2 >= ~1 MiB. The *effective* per-pass fanout
/// used at runtime is the min of the measured F_max (the largest fanout still sustaining >= 80%
/// of peak scatter bandwidth), this constant, and an L2-derived cap (see the model's F_max
/// wiring in hash_join_bandwidth_model.cpp); this constant only bounds how far a single pass may
/// go before the SWWC state stops fitting in cache at all.
constexpr size_t MAX_FANOUT_PER_PASS = 8192;

/// Splits log2(p_star) partition bits into passes of at most log2(f_max) bits each.
std::vector<size_t> computePassBits(size_t p_star, size_t f_max);

/// Radix scatter of one side by a hash of its first (key) column (deliberately independent of
/// the CRC32C the hash tables use; see routeWord): this is the partitioning code the radix join
/// runs, also used to measure the scatter bandwidth term. Uses histogram + prefix sum + exact
/// allocation with direct placement, routing materialized as 2-byte partition ids for a bounded
/// window of rows at a time, column-major loop order within the window, and software
/// write-combining with non-temporal stores at fanout >= 256 (single pass up to the full
/// partition count; multiple passes only as a fallback). Consumed input is dropped eagerly
/// (per chunk batch in the first pass - for pass 0 that releases the references to the caller's
/// blocks - and per column in refine passes), keeping the scatter's resident memory near one
/// copy of the side instead of two. All UInt64 columns.
std::vector<ChunkList> scatterSide(WorkerPool & pool, const std::vector<Block> & blocks, const std::vector<size_t> & pass_bits);

/// Phase timings of one streamingWaveProbe call (wall seconds, summed over waves).
struct StreamingWaveStats
{
    double scatter_sec = 0;
    double probe_sec = 0;
};

/// BEP streaming probe (evict-all-at-budget shape): consumes the probe side in `waves`
/// consecutive windows; each window is radix-scattered to leaf depth (single pass of `bits`
/// bits) and every non-empty partition's window chunk is probed through `probe_partition`
/// (work-stealing) and dropped before the next window starts. One window = one probe-buffer
/// budget of |probe| / waves bytes; each partition is revisited once per wave.
///
/// The whole wave loop runs inside ONE pool.run: phases (histogram, fused prefix-sum +
/// allocation, fused all-columns scatter, probe) are separated by std::barrier instead of
/// per-phase pool dispatches, and per-worker scratch (SWWC staging, histogram lanes, partition
/// ids, cursors) persists across waves. This removes the per-wave overhead that dominated
/// small budgets when each wave paid its own scatterSide + probe pool.run round-trips
/// (~4 dispatches/wave, measured ~1.9 ms/wave at 96 threads).
///
/// `probe_partition` is called concurrently from all workers, receives ownership of the
/// window's chunk for that partition (freed on return), and returns the number of output
/// rows; with a non-null `digest` it must also accumulate the output fingerprint.
/// Returns total output rows. `fingerprint`, when non-null, receives the summed digest.
size_t streamingWaveProbe(
    WorkerPool & pool,
    const std::vector<Block> & blocks,
    size_t bits,
    size_t waves,
    const std::function<size_t(size_t partition, Chunk chunk, UInt64 * digest)> & probe_partition,
    UInt64 * fingerprint,
    StreamingWaveStats & stats);

/// Shared setup of the join metadata: INNER ALL join on the first column of each side.
std::shared_ptr<TableJoin> makeTableJoin(const Block & left_header, const Block & right_header);

/// Materializes all output blocks of one join result, returns the number of output rows.
/// If `fingerprint` is non-null, accumulates the order-independent digest of the output rows.
size_t drainJoinResult(JoinResultPtr result, UInt64 * fingerprint = nullptr);

/// The driver: times the two phases of a join implementation through the common interface.
/// With `verify`, the probe additionally computes the output fingerprint (JoinStats::fingerprint).
JoinStats driveJoin(IJoinBench & join, const std::vector<Block> & build_blocks, const std::vector<Block> & probe_blocks, bool verify = false);

}
