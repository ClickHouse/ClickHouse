#include "hash_join_bench.h"

#include <algorithm>
#include <atomic>
#include <barrier>
#include <bit>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string_view>

#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
#endif

#include <Columns/ColumnsNumber.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <Interpreters/TableJoin.h>
#include <QueryPipeline/SizeLimits.h>
#include <Common/CurrentMetrics.h>
#include <Common/HashTable/Hash.h>
#include <Common/PODArray.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/assert_cast.h>

namespace CurrentMetrics
{
    extern const Metric LocalThread;
    extern const Metric LocalThreadActive;
    extern const Metric LocalThreadScheduled;
}

namespace ProfileEvents
{
    extern const Event HashJoinProbeMatchMicroseconds;
    extern const Event HashJoinProbeGatherMicroseconds;
    extern const Event ConcurrentHashJoinProbeDispatchMicroseconds;
}

namespace DB::JoinBench
{

ProbeProfile currentProbeProfile()
{
    return {
        static_cast<double>(ProfileEvents::global_counters[ProfileEvents::HashJoinProbeMatchMicroseconds]) * 1e-6,
        static_cast<double>(ProfileEvents::global_counters[ProfileEvents::HashJoinProbeGatherMicroseconds]) * 1e-6,
        static_cast<double>(ProfileEvents::global_counters[ProfileEvents::ConcurrentHashJoinProbeDispatchMicroseconds]) * 1e-6,
    };
}

std::atomic<UInt64> g_sink{0};

WorkerPool::WorkerPool(size_t num_threads_)
    : num_threads(num_threads_)
    , pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, CurrentMetrics::LocalThreadScheduled,
           num_threads_, /*max_free_threads_*/ num_threads_, /*queue_size_*/ 0)
{
}

double WorkerPool::run(const std::function<void(size_t)> & task)
{
    Stopwatch watch;
    for (size_t t = 0; t < num_threads; ++t)
        pool.scheduleOrThrowOnError([&task, t] { task(t); });
    pool.wait();
    return watch.elapsedSeconds();
}

namespace
{

/// Radix scatter after origin/phj5-real's KeyRefScatter, adapted to column-by-column output:
///   - routing is materialized as 2-byte partition ids rather than recomputed per column, but
///     only for a bounded window of rows at a time (a batch of chunks in the first pass, one
///     group in refine passes), never for the whole side. The ids are emitted as a free
///     by-product of the key column's own scatter (which reads every key anyway), so per
///     payload column an 8 B key re-read becomes a 2 B id read, and - the reason the scheme
///     exists - the ids end all routing uses of the input key column, so consumed input
///     columns can be dropped eagerly (see next bullet). Every pass slices a disjoint bit
///     range of the same 32-bit route word;
///   - consumed input is dropped as early as possible to keep the scatter's resident memory
///     near one copy of the side instead of two: the first pass drops each input chunk batch
///     right after its last column is scattered (for pass 0 that releases this side's
///     reference to the caller's blocks - in a real pipeline the upstream source's blocks get
///     recycled here; for owned inputs it frees them), and refine passes drop each input
///     column right after it is scattered and allocate each output column just-in-time, so
///     the freed input extents are immediately reusable by the allocator for the next output
///     column instead of sitting dirty until decay;
///   - per-partition destination columns are allocated exactly once from a histogram
///     (prefix sum + direct placement; no piece lists, no coalescing pass, no allocator churn);
///   - columns are scattered one at a time within a routing window (column-major loop order),
///     so only `fanout` output streams and one fanout x 64 B staging set are live per worker at
///     any instant (workers may be on different columns concurrently: see the fused scatter
///     barrier in scatterPass);
///   - at fanout >= 256 a software write-combining path stages one 64-byte line per partition
///     and flushes it with a non-temporal store, avoiding both the cache pollution and the
///     read-for-ownership traffic that create the high-fanout cliff of the naive scatter.

/// All passes slice disjoint bit ranges of this single 32-bit word.
///
/// The route hash must be independent of `HashCRC32` (CRC32C, the Castagnoli polynomial) that
/// the real HashJoin/parallel_hash tables use for bucketing, otherwise partition assignment
/// correlates with in-table bucket placement and per-partition tables see a skewed hash space.
///   - aarch64: the ISO-polynomial CRC32 instruction (`__crc32d`, polynomial 0x04C11DB7) is as
///     cheap as CRC32C but a different function;
///   - elsewhere: multiply-shift routing (as origin/phj5-real does on x86-64, where only the
///     CRC32C instruction exists).
inline UInt32 routeWord(UInt64 key)
{
#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
    return __crc32d(-1U, key);
#else
    return static_cast<UInt32>((key * 0x9E3779B97F4A7C15ULL) >> 32);
#endif
}

constexpr size_t LINE_BYTES = 64;
constexpr size_t ELEMS_PER_LINE = LINE_BYTES / sizeof(UInt64);
/// Fanout from which the SWWC + non-temporal path wins over plain per-partition cursors
/// (the direct path's live output lines no longer stay cache-resident).
constexpr size_t SWWC_MIN_FANOUT = 256;

using NtLine = char __attribute__((vector_size(LINE_BYTES)));

/// Per-worker scatter state: write cursors, and for the SWWC path one 64-byte staging line per
/// partition plus a fill counter (bytes currently staged for that partition's line).
///
/// Invariant: staged bytes for partition p live at staging_line + [m, fill), where
/// m = (uintptr)cursors[p] & 63. Before the first flush of a seeding session the cursor has not
/// advanced, so m equals the misalignment seeded into `fill` by seed(); after the first flush the
/// cursor is line-aligned and m == 0. Cursors are always 8-byte aligned (UInt64 columns), so m is
/// a multiple of 8 and LINE_BYTES - m >= 8. This lets scatterChunkSwwc handle cursor misalignment
/// once per flush (at most once per partition per seeding session) instead of once per row.
struct ScatterScratch
{
    size_t fanout = 0;
    bool use_swwc = false;
    PaddedPODArray<char> staging_mem;
    char * staging = nullptr;
    PaddedPODArray<UInt64 *> cursors;
    PaddedPODArray<UInt32> fill; /// bytes currently staged for the partition's line

    void init(size_t fanout_, bool use_swwc_)
    {
        fanout = fanout_;
        use_swwc = use_swwc_;
        cursors.resize(fanout);
        if (use_swwc)
        {
            staging_mem.resize(fanout * LINE_BYTES + LINE_BYTES);
            staging = reinterpret_cast<char *>(
                (reinterpret_cast<uintptr_t>(staging_mem.data()) + LINE_BYTES - 1) & ~static_cast<uintptr_t>(LINE_BYTES - 1));
            fill.resize(fanout);
        }
    }

    void seed(size_t p, UInt64 * cursor)
    {
        cursors[p] = cursor;
        if (use_swwc)
            /// nullptr -> 0, harmless: no row ever routes to an empty (never-seeded) partition.
            fill[p] = static_cast<UInt32>(reinterpret_cast<uintptr_t>(cursor) & (LINE_BYTES - 1));
    }

    /// Flush residual staged bytes of every partition and publish the NT stores.
    void drain()
    {
        if (!use_swwc)
            return;
        for (size_t p = 0; p < fanout; ++p)
        {
            const UInt32 f = fill[p];
            if (!f)
                continue;
            UInt64 * cur = cursors[p];
            const UInt32 m = static_cast<UInt32>(reinterpret_cast<uintptr_t>(cur) & (LINE_BYTES - 1));
            /// f == m means no rows were staged since seeding: nothing to flush. f > m covers
            /// both the pre-first-flush case (data at [m, f)) and the post-flush case
            /// (m == 0, data at [0, f)).
            if (f > m)
            {
                memcpy(cur, staging + p * LINE_BYTES + m, f - m);
                cursors[p] = cur + (f - m) / sizeof(UInt64);
            }
            fill[p] = 0;
        }
        /// NT stores are weakly ordered; make them visible before the outputs are read.
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }
};

/// At low fanout, consecutive rows commonly hit the same counter and the histogram's
/// load-increment-store chain serializes (measured ~1.9x slower at fanout 2 with 4 lanes vs 1).
/// 4 UInt32 lanes stay within 32 KiB even at the largest interleaved fanout.
constexpr size_t HIST_INTERLEAVE_MAX_FANOUT = 2048;

/// First-pass batch sizing: each worker routes and scatters its chunk stripe in batches of
/// whole chunks, dropping each batch's input right after its last column is scattered. The
/// batch must be large enough that the cost of every (batch, column) boundary - the seed/save
/// cursor sweeps plus, on the SWWC path, up to one partial-line flush and one head-realignment
/// memcpy per partition - stays a small fraction of the lines written in between: 64 lines per
/// partition per batch bounds the boundary cost at ~1.5%, and targeted A/B sweeps at fanouts
/// 128-512 measured parity with the pre-batching implementation (well within this machine's
/// +-15% session drift). The row floor keeps batches at low fanout (where the SWWC boundary
/// cost is absent) big enough to amortize the boundary sweeps. The target also bounds the
/// batch's transient memory - its input rows (freed at batch end) plus 2 B/row of partition
/// ids - at 4M rows per worker at the largest per-pass fanout (MAX_FANOUT_PER_PASS).
constexpr size_t SCATTER_BATCH_MIN_ROWS = 256 << 10;
constexpr size_t SCATTER_BATCH_LINES_PER_PARTITION = 64;

size_t scatterBatchRowsTarget(size_t fanout)
{
    return std::max(SCATTER_BATCH_MIN_ROWS, fanout * SCATTER_BATCH_LINES_PER_PARTITION * ELEMS_PER_LINE);
}

/// Histograms one chunk's rows into `hist[0..fanout)`. At low fanout (`lanes` non-null, a
/// caller-owned buffer of size 4 * fanout that persists across calls for the same worker/group),
/// row i increments lane (i & 3) of its bucket instead of the shared counter directly, breaking
/// the dependency chain; the caller must reduce the lanes into `hist` via reduceHistogramLanes
/// once after all chunks are processed. At high fanout collisions are rare and 4 lanes would blow
/// the cache footprint, so `lanes` is null and rows increment `hist` directly.
void histogramChunk(const UInt64 * keys, size_t n, UInt32 shift, UInt32 mask, UInt32 * hist, UInt32 * lanes, size_t fanout)
{
    if (!lanes)
    {
        for (size_t i = 0; i < n; ++i)
            ++hist[(routeWord(keys[i]) >> shift) & mask];
        return;
    }

    size_t i = 0;
    for (; i + 4 <= n; i += 4)
    {
        ++lanes[0 * fanout + ((routeWord(keys[i + 0]) >> shift) & mask)];
        ++lanes[1 * fanout + ((routeWord(keys[i + 1]) >> shift) & mask)];
        ++lanes[2 * fanout + ((routeWord(keys[i + 2]) >> shift) & mask)];
        ++lanes[3 * fanout + ((routeWord(keys[i + 3]) >> shift) & mask)];
    }
    for (; i < n; ++i)
        ++lanes[(i & 3) * fanout + ((routeWord(keys[i]) >> shift) & mask)];
}

void reduceHistogramLanes(UInt32 * hist, const UInt32 * lanes, size_t fanout)
{
    for (size_t p = 0; p < fanout; ++p)
        hist[p] += lanes[0 * fanout + p] + lanes[1 * fanout + p] + lanes[2 * fanout + p] + lanes[3 * fanout + p];
}

/// The routing source per row: the key-column kernels compute the partition from the key (and
/// optionally emit it as a 2-byte pid); the payload-column kernels reload the emitted pid.
struct RouteFromKey
{
    const UInt64 * keys;
    UInt32 shift;
    UInt32 mask;
    UInt16 * pids; /// null when there are no payload columns to consume the ids

    ALWAYS_INLINE UInt32 partition(size_t i) const
    {
        const UInt32 p = (routeWord(keys[i]) >> shift) & mask;
        if (pids)
            pids[i] = static_cast<UInt16>(p);
        return p;
    }
};

struct RouteFromPids
{
    const UInt16 * pids;

    ALWAYS_INLINE UInt32 partition(size_t i) const { return pids[i]; }
};

template <typename Route>
void scatterChunkDirect(Route route, const UInt64 * data, size_t n, UInt64 ** cursors)
{
    for (size_t i = 0; i < n; ++i)
        *cursors[route.partition(i)]++ = data[i];
}

template <typename Route>
void scatterChunkSwwc(Route route, const UInt64 * data, size_t n, ScatterScratch & scratch)
{
    /// Hoisted like `staging` already was: the char*/vector NT store defeats TBAA hoisting, so
    /// without this the compiler reloads scratch.cursors/fill.data() every row (measured
    /// ~1.07x on clang, ~1.65x on GCC by hoisting).
    char * const staging = scratch.staging;
    UInt64 ** const cursors = scratch.cursors.data();
    UInt32 * const fill = scratch.fill.data();

    for (size_t i = 0; i < n; ++i)
    {
        const UInt32 p = route.partition(i);
        char * line = staging + static_cast<size_t>(p) * LINE_BYTES;
        UInt32 f = fill[p];
        *reinterpret_cast<UInt64 *>(line + f) = data[i];
        f += sizeof(UInt64);
        if (f == LINE_BYTES)
        {
            UInt64 * cur = cursors[p];
            const UInt32 m = static_cast<UInt32>(reinterpret_cast<uintptr_t>(cur) & (LINE_BYTES - 1));
            if (m) /// first flush of a misaligned stream: emit the partial head line with regular stores
            {
                __builtin_memcpy(cur, line + m, LINE_BYTES - m);
                cursors[p] = cur + (LINE_BYTES - m) / sizeof(UInt64);
            }
            else
            {
                /// A variant reading the 8 UInt64s individually (via volatile loads, to force 8
                /// narrow loads instead of one that the store-to-load-forwarding unit cannot
                /// service from the immediately-preceding 8 narrow stores) was measured against
                /// this wide vector load at fanouts 512 and 2048 (3 runs each, --quick --tuples
                /// 2^26): wide averaged 70.7 GB/s / 61.5 GB/s vs narrow's 65.9 GB/s / 60.1 GB/s -
                /// the narrow variant did not win at either point (run-to-run noise on this
                /// machine is +-15%), so the wide load is kept.
                __builtin_nontemporal_store(*reinterpret_cast<const NtLine *>(line), reinterpret_cast<NtLine *>(cur));
                cursors[p] = cur + ELEMS_PER_LINE;
            }
            f = 0;
        }
        fill[p] = f;
    }
}

const UInt64 * keyData(const Chunk & chunk)
{
    return assert_cast<const ColumnUInt64 &>(*chunk.columns[0]).getData().data();
}

const UInt64 * columnData(const Chunk & chunk, size_t j)
{
    return assert_cast<const ColumnUInt64 &>(*chunk.columns[j]).getData().data();
}

/// Scatters column j of one chunk. The key column (j == 0) routes from the keys it reads anyway
/// and emits the chunk's partition ids as a by-product; payload columns route through those ids
/// (a 2 B id read instead of an 8 B key re-read). `pids` is the chunk's slice of the window's
/// id buffer (null only when the chunk has no payload columns, so the ids have no consumer).
void scatterChunkColumn(const Chunk & chunk, size_t j, UInt32 shift, UInt32 mask, UInt16 * pids, bool use_swwc, ScatterScratch & scratch)
{
    const UInt64 * data = columnData(chunk, j);
    if (j == 0)
    {
        RouteFromKey route{keyData(chunk), shift, mask, pids};
        if (use_swwc)
            scatterChunkSwwc(route, data, chunk.rows, scratch);
        else
            scatterChunkDirect(route, data, chunk.rows, scratch.cursors.data());
    }
    else
    {
        RouteFromPids route{pids};
        if (use_swwc)
            scatterChunkSwwc(route, data, chunk.rows, scratch);
        else
            scatterChunkDirect(route, data, chunk.rows, scratch.cursors.data());
    }
}

/// Exactly-sized destination columns of one output partition, with raw write pointers.
struct PartitionOutput
{
    std::vector<MutableColumnPtr> columns;
    std::vector<UInt64 *> bases;
    size_t rows = 0;

    /// Appends one exactly-sized destination column. ColumnVector(n) leaves POD contents
    /// uninitialized: no memset, pages are first-touched by the scatter writes themselves.
    /// Refine passes call this just-in-time, one column per scatter round, so the allocator
    /// can serve it from the input column the previous round just dropped.
    void allocateColumn()
    {
        auto col = ColumnUInt64::create(rows);
        bases.push_back(col->getData().data());
        columns.push_back(std::move(col));
    }

    void allocate(size_t num_columns, size_t rows_)
    {
        rows = rows_;
        columns.reserve(num_columns);
        bases.reserve(num_columns);
        for (size_t j = 0; j < num_columns; ++j)
            allocateColumn();
    }

    Chunk toChunk()
    {
        Chunk chunk;
        chunk.rows = rows;
        for (auto & col : columns)
            chunk.columns.emplace_back(std::move(col));
        return chunk;
    }
};

/// One radix pass: split every input group into `fanout` sub-partitions, each materialized as
/// a single exactly-sized chunk. Consumed input is dropped eagerly (per chunk batch in the
/// first pass, per column in refine passes - see the branches below), so a pass never holds a
/// full extra copy of the side on top of its output.
std::vector<ChunkList> scatterPass(WorkerPool & pool, std::vector<ChunkList> & groups, size_t bits, size_t bits_done)
{
    const size_t threads = pool.size();
    const size_t fanout = 1ULL << bits;
    chassert(bits_done + bits <= 32);
    chassert(fanout <= (1ULL << 16)); /// partition ids are UInt16
    const UInt32 shift = static_cast<UInt32>(32 - bits_done - bits);
    const UInt32 mask = static_cast<UInt32>(fanout - 1);
    const bool use_swwc = fanout >= SWWC_MIN_FANOUT;
    std::vector<ChunkList> out(groups.size() * fanout);

    const bool interleave_hist = fanout <= HIST_INTERLEAVE_MAX_FANOUT;

    if (groups.size() == 1)
    {
        /// First pass: all threads cooperate on the single group in exactly 3 barriers (down
        /// from histogram + serial prefix-sum + allocation + one barrier per column): a fused
        /// prefix-sum/allocation barrier removes the single-threaded Phase B, and a fused
        /// all-columns scatter barrier removes the per-column barrier.
        ChunkList & chunks = groups[0];
        if (chunks.empty())
            return out;
        const size_t num_columns = chunks.front().columns.size();

        /// Barrier 1: per-worker histograms into disjoint slices of one flat array.
        PaddedPODArray<UInt32> hist;
        hist.resize(threads * fanout);
        pool.run([&](size_t tid)
        {
            UInt32 * h = hist.data() + tid * fanout;
            memset(h, 0, fanout * sizeof(UInt32));
            std::vector<UInt32> lanes;
            if (interleave_hist)
                lanes.assign(4 * fanout, 0);
            for (size_t c = tid; c < chunks.size(); c += threads)
                histogramChunk(keyData(chunks[c]), chunks[c].rows, shift, mask, h, interleave_hist ? lanes.data() : nullptr, fanout);
            if (interleave_hist)
                reduceHistogramLanes(h, lanes.data(), fanout);
        });

        /// Barrier 2: fused prefix sum + exact one-shot allocation. Each worker owns a
        /// contiguous, disjoint range of partitions, so there is no cross-worker write
        /// dependency and no separate single-threaded prefix-sum phase is needed.
        PaddedPODArray<UInt32> offsets;
        offsets.resize(threads * fanout);
        std::vector<UInt64> totals(fanout, 0);
        std::vector<PartitionOutput> parts(fanout);
        pool.run([&](size_t tid)
        {
            const size_t begin = fanout * tid / threads;
            const size_t end = fanout * (tid + 1) / threads;
            for (size_t p = begin; p < end; ++p)
            {
                UInt64 total = 0;
                for (size_t w = 0; w < threads; ++w)
                {
                    offsets[w * fanout + p] = static_cast<UInt32>(total);
                    total += hist[w * fanout + p];
                }
                totals[p] = total;
                if (total)
                    parts[p].allocate(num_columns, total);
            }
        });

        /// Barrier 3: single fused scatter run, batched. Each worker processes its chunk stripe
        /// in batches of whole chunks (~scatterBatchRowsTarget rows): the key column's scatter
        /// emits the batch's 2-byte partition ids as a by-product, the payload columns scatter
        /// through the ids, then the batch's input chunks are dropped - each chunk belongs to
        /// exactly one worker's stripe, so the drop is worker-local. On pass 0 the drop
        /// releases this side's reference to the caller's blocks (in a real pipeline the
        /// upstream source's blocks are recycled here); on later passes it frees the previous
        /// pass's output, bounding the resident overlap of input and output to one batch per
        /// worker. Each worker writes only its own [offset, offset + hist) range of every
        /// (partition, column) output buffer; those ranges are disjoint across workers and
        /// across columns, so there is no cross-worker dependency and no barrier between
        /// columns or batches - the pool.run barrier plus each worker's drain() fences (which
        /// publish the NT stores) are enough to make every worker's writes visible before the
        /// collection loop below reads the outputs.
        const size_t batch_rows_target = scatterBatchRowsTarget(fanout);
        std::vector<ScatterScratch> scratch(threads);
        pool.run([&](size_t tid)
        {
            auto & s = scratch[tid];
            if (s.fanout != fanout)
                s.init(fanout, use_swwc);

            /// Running write cursors per (column, partition), persisted across batches: this
            /// worker's disjoint output ranges, advanced batch by batch. ScatterScratch's
            /// documented invariant handles the mid-line cursor a drain leaves behind (the
            /// next batch's first flush repairs the misaligned head).
            std::vector<UInt64 *> col_cursors(num_columns * fanout);
            for (size_t j = 0; j < num_columns; ++j)
                for (size_t p = 0; p < fanout; ++p)
                    col_cursors[j * fanout + p] = totals[p] ? parts[p].bases[j] + offsets[tid * fanout + p] : nullptr;

            PaddedPODArray<UInt16> pids;
            std::vector<size_t> batch;       /// chunk indices of the current batch
            std::vector<size_t> batch_offsets; /// each chunk's start row within `pids`

            size_t c = tid;
            while (c < chunks.size())
            {
                batch.clear();
                batch_offsets.clear();
                size_t batch_rows = 0;
                for (; c < chunks.size() && batch_rows < batch_rows_target; c += threads)
                {
                    batch.push_back(c);
                    batch_offsets.push_back(batch_rows);
                    batch_rows += chunks[c].rows;
                }

                if (num_columns > 1)
                    pids.resize(batch_rows);

                for (size_t j = 0; j < num_columns; ++j)
                {
                    for (size_t p = 0; p < fanout; ++p)
                        s.seed(p, col_cursors[j * fanout + p]);

                    for (size_t b = 0; b < batch.size(); ++b)
                        scatterChunkColumn(chunks[batch[b]], j, shift, mask,
                            num_columns > 1 ? pids.data() + batch_offsets[b] : nullptr, use_swwc, s);
                    s.drain();

                    for (size_t p = 0; p < fanout; ++p)
                        col_cursors[j * fanout + p] = s.cursors[p];
                }

                /// The batch is fully consumed (the ids replaced all routing uses of the key
                /// column): drop its input chunks before starting the next batch.
                for (size_t b : batch)
                    chunks[b].columns = {};
            }
        });

        for (size_t p = 0; p < fanout; ++p)
            if (totals[p])
                out[p].push_back(parts[p].toChunk());
    }
    else
    {
        /// Refine passes (multi-pass fallback): groups are assigned to workers dynamically
        /// (an atomic counter, not a static stripe), because groups can have very different
        /// sizes and a static stripe would leave some workers idle while others are still
        /// scattering their share - the join's defense against per-group skew.
        ///
        /// Group inputs are owned (they are the previous pass's output), so memory is cycled
        /// eagerly, all worker-local: the key column's scatter emits 2-byte partition ids for
        /// the whole group (bounded: a group is at most 1/fanout_so_far of the side), after
        /// which the key column is never read again; each column round allocates its output
        /// columns just-in-time, scatters (through the ids from round 1 on), and drops the
        /// consumed input column - so the freed input extents are immediately reusable for the
        /// next round's output instead of sitting dirty until allocator decay, and a group in
        /// flight holds ~(C+1)/C of its size instead of 2x.
        std::atomic<size_t> next_group{0};
        pool.run([&](size_t /*tid*/)
        {
            ScatterScratch scratch;
            scratch.init(fanout, use_swwc);
            std::vector<UInt32> hist(fanout);
            std::vector<UInt32> lanes;
            if (interleave_hist)
                lanes.resize(4 * fanout);
            PaddedPODArray<UInt16> pids;

            for (size_t g = next_group.fetch_add(1, std::memory_order_relaxed); g < groups.size(); g = next_group.fetch_add(1, std::memory_order_relaxed))
            {
                ChunkList & chunks = groups[g];
                if (chunks.empty())
                    continue;
                const size_t num_columns = chunks.front().columns.size();

                size_t group_rows = 0;
                for (const auto & chunk : chunks)
                    group_rows += chunk.rows;
                if (num_columns > 1)
                    pids.resize(group_rows);

                std::fill(hist.begin(), hist.end(), 0);
                if (interleave_hist)
                    std::fill(lanes.begin(), lanes.end(), 0);
                for (const auto & chunk : chunks)
                    histogramChunk(keyData(chunk), chunk.rows, shift, mask, hist.data(), interleave_hist ? lanes.data() : nullptr, fanout);
                if (interleave_hist)
                    reduceHistogramLanes(hist.data(), lanes.data(), fanout);

                std::vector<PartitionOutput> parts(fanout);
                for (size_t p = 0; p < fanout; ++p)
                    parts[p].rows = hist[p];

                for (size_t j = 0; j < num_columns; ++j)
                {
                    /// Just-in-time exact allocation of this round's output columns: by now the
                    /// previous round's input column has been dropped, so its extents back this.
                    for (size_t p = 0; p < fanout; ++p)
                        if (hist[p])
                            parts[p].allocateColumn();

                    for (size_t p = 0; p < fanout; ++p)
                        scratch.seed(p, hist[p] ? parts[p].bases[j] : nullptr);

                    size_t row = 0;
                    for (const auto & chunk : chunks)
                    {
                        scatterChunkColumn(chunk, j, shift, mask,
                            num_columns > 1 ? pids.data() + row : nullptr, use_swwc, scratch);
                        row += chunk.rows;
                    }
                    scratch.drain();

                    /// Input column j is fully consumed (the ids emitted during the key
                    /// column's scatter replace all further routing uses): drop it before the
                    /// next round allocates its outputs.
                    for (auto & chunk : chunks)
                        chunk.columns[j] = nullptr;
                }

                for (size_t p = 0; p < fanout; ++p)
                    if (hist[p])
                        out[g * fanout + p].push_back(parts[p].toChunk());

                /// Only empty column shells remain; free them before moving to the next group.
                groups[g].clear();
            }
        });
    }

    return out;
}

}

std::vector<size_t> computePassBits(size_t p_star, size_t f_max)
{
    const size_t total_bits = static_cast<size_t>(std::countr_zero(std::bit_ceil(p_star)));
    const size_t f_bits = std::max<size_t>(1, static_cast<size_t>(std::bit_width(std::bit_floor(std::max<size_t>(2, f_max))) - 1));
    const size_t n_pass = (total_bits + f_bits - 1) / f_bits;
    const size_t per_pass = (total_bits + n_pass - 1) / n_pass;

    std::vector<size_t> result;
    size_t remaining = total_bits;
    while (remaining > 0)
    {
        const size_t bits = std::min(per_pass, remaining);
        result.push_back(bits);
        remaining -= bits;
    }
    return result;
}

std::vector<ChunkList> scatterSide(WorkerPool & pool, const std::vector<Block> & blocks, const std::vector<size_t> & pass_bits)
{
    /// Histogram/offset counters are UInt32 (see scatterPass): a side with more rows would
    /// silently overflow them, so this is a hard precondition, not a soft cap.
    size_t total_rows = 0;
    for (const auto & block : blocks)
        total_rows += block.rows();
    if (total_rows > std::numeric_limits<UInt32>::max())
        throw std::runtime_error("scatter supports at most 2^32-1 rows per side");

    std::vector<ChunkList> groups(1);
    groups[0].reserve(blocks.size());
    for (const auto & block : blocks)
    {
        Chunk chunk;
        chunk.rows = block.rows();
        for (size_t j = 0; j < block.columns(); ++j)
            chunk.columns.push_back(block.getByPosition(j).column);
        groups[0].push_back(std::move(chunk));
    }

    size_t bits_done = 0;
    for (size_t bits : pass_bits)
    {
        groups = scatterPass(pool, groups, bits, bits_done);
        bits_done += bits;
    }
    return groups;
}

size_t streamingWaveProbe(
    WorkerPool & pool,
    const std::vector<Block> & blocks,
    size_t bits,
    size_t waves,
    const std::function<size_t(size_t partition, Chunk chunk, UInt64 * digest)> & probe_partition,
    UInt64 * fingerprint,
    StreamingWaveStats & stats)
{
    stats = {};
    if (blocks.empty())
        return 0;

    const size_t threads = pool.size();
    const size_t fanout = 1ULL << bits;
    chassert(bits >= 1 && bits <= 16);

    const UInt32 shift = static_cast<UInt32>(32 - bits);
    const UInt32 mask = static_cast<UInt32>(fanout - 1);
    const bool use_swwc = fanout >= SWWC_MIN_FANOUT;
    const bool interleave_hist = fanout <= HIST_INTERLEAVE_MAX_FANOUT;

    std::vector<Chunk> chunks;
    chunks.reserve(blocks.size());
    size_t total_rows = 0;
    for (const auto & block : blocks)
    {
        Chunk chunk;
        chunk.rows = block.rows();
        total_rows += chunk.rows;
        for (size_t j = 0; j < block.columns(); ++j)
            chunk.columns.push_back(block.getByPosition(j).column);
        chunks.push_back(std::move(chunk));
    }
    if (total_rows > std::numeric_limits<UInt32>::max())
        throw std::runtime_error("streamingWaveProbe supports at most 2^32-1 probe rows");
    const size_t num_columns = chunks.front().columns.size();
    const size_t num_waves = std::max<size_t>(1, std::min(waves, chunks.size()));

    /// Shared per-wave state, allocated once. Every phase writes disjoint slices per worker
    /// (histogram/offset stripes, contiguous partition ranges), so barriers are the only
    /// synchronization; `next_partition` drives the probe phase's work stealing and is reset
    /// during the allocation phase (a barrier separates it from both neighboring uses).
    PaddedPODArray<UInt32> hist;
    hist.resize(threads * fanout);
    PaddedPODArray<UInt32> offsets;
    offsets.resize(threads * fanout);
    std::vector<UInt64> totals(fanout);
    std::vector<PartitionOutput> parts(fanout);
    std::atomic<size_t> next_partition{0};
    std::atomic<size_t> rows{0};
    std::atomic<UInt64> digest{0};
    std::barrier<> barrier(static_cast<std::ptrdiff_t>(threads));

    pool.run([&](size_t tid)
    {
        ScatterScratch scratch;
        scratch.init(fanout, use_swwc);
        std::vector<UInt32> lanes;
        if (interleave_hist)
            lanes.resize(4 * fanout);
        PaddedPODArray<UInt16> pids;
        std::vector<UInt64 *> col_cursors(num_columns * fanout);
        size_t local_rows = 0;
        UInt64 local_digest = 0;

        Stopwatch watch; /// consulted on tid 0 only; barriers make its spans ~wall time
        barrier.arrive_and_wait(); /// align the start so tid 0's first span excludes pool ramp-up
        if (tid == 0)
            watch.restart();

        for (size_t w = 0; w < num_waves; ++w)
        {
            const size_t begin = chunks.size() * w / num_waves;
            const size_t end = chunks.size() * (w + 1) / num_waves;

            /// Histogram of this worker's chunk stripe of the window.
            UInt32 * h = hist.data() + tid * fanout;
            memset(h, 0, fanout * sizeof(UInt32));
            if (interleave_hist)
                std::fill(lanes.begin(), lanes.end(), 0);
            for (size_t c = begin + tid; c < end; c += threads)
                histogramChunk(keyData(chunks[c]), chunks[c].rows, shift, mask, h, interleave_hist ? lanes.data() : nullptr, fanout);
            if (interleave_hist)
                reduceHistogramLanes(h, lanes.data(), fanout);
            barrier.arrive_and_wait();

            /// Fused prefix sum + exact allocation of this worker's partition range.
            for (size_t p = fanout * tid / threads; p < fanout * (tid + 1) / threads; ++p)
            {
                UInt64 total = 0;
                for (size_t worker = 0; worker < threads; ++worker)
                {
                    offsets[worker * fanout + p] = static_cast<UInt32>(total);
                    total += hist[worker * fanout + p];
                }
                totals[p] = total;
                parts[p] = PartitionOutput{};
                if (total)
                    parts[p].allocate(num_columns, total);
            }
            if (tid == 0)
                next_partition.store(0, std::memory_order_relaxed);
            barrier.arrive_and_wait();

            /// Fused all-columns scatter of the stripe (same structure as scatterPass's
            /// barrier 3, without the intra-window batching: the window is the batch).
            size_t stripe_rows = 0;
            for (size_t c = begin + tid; c < end; c += threads)
                stripe_rows += chunks[c].rows;
            if (num_columns > 1)
                pids.resize(stripe_rows);
            for (size_t j = 0; j < num_columns; ++j)
                for (size_t p = 0; p < fanout; ++p)
                    col_cursors[j * fanout + p] = totals[p] ? parts[p].bases[j] + offsets[tid * fanout + p] : nullptr;
            for (size_t j = 0; j < num_columns; ++j)
            {
                for (size_t p = 0; p < fanout; ++p)
                    scratch.seed(p, col_cursors[j * fanout + p]);
                size_t row = 0;
                for (size_t c = begin + tid; c < end; c += threads)
                {
                    scatterChunkColumn(chunks[c], j, shift, mask,
                        num_columns > 1 ? pids.data() + row : nullptr, use_swwc, scratch);
                    row += chunks[c].rows;
                }
                scratch.drain();
            }
            barrier.arrive_and_wait();
            if (tid == 0)
            {
                stats.scatter_sec += watch.elapsedSeconds();
                watch.restart();
            }

            /// Probe every non-empty partition of the window (work stealing), dropping the
            /// window's chunk on return from the callback.
            for (size_t p = next_partition.fetch_add(1, std::memory_order_relaxed); p < fanout;
                 p = next_partition.fetch_add(1, std::memory_order_relaxed))
            {
                if (!totals[p])
                    continue;
                local_rows += probe_partition(p, parts[p].toChunk(), fingerprint ? &local_digest : nullptr);
            }
            barrier.arrive_and_wait();
            if (tid == 0)
            {
                stats.probe_sec += watch.elapsedSeconds();
                watch.restart();
            }
        }

        g_sink += local_rows;
        rows += local_rows;
        digest += local_digest;
    });

    if (fingerprint)
        *fingerprint += digest;
    return rows;
}

std::shared_ptr<TableJoin> makeTableJoin(const Block & left_header, const Block & right_header)
{
    /// Construct from default query Settings so that all behavior flags match a real query —
    /// notably `enable_software_prefetch_in_join` (default true; the bare StorageJoin-style
    /// constructor leaves it false, silently disabling the join's software prefetching).
    ///
    /// INNER ALL. Note: ClickHouse ANY INNER marks right rows used-once (one output row per
    /// distinct matched right key), which does not match the model's one-match-per-probe-row
    /// assumption; benchmarks therefore use ALL with duplicate-free build keys where the output
    /// size must equal the probe side. With duplicate-free build sides, `onBuildPhaseFinish`
    /// promotes `All` to `RightAny` (`HashJoin.cpp`), so every timed probe in these benchmarks
    /// runs the promoted point-lookup path and emits exactly one row per matching probe row -
    /// the model and the one-row-per-probe output-size assumptions rely on this.
    static const Settings default_settings;
    auto table_join = std::make_shared<TableJoin>(default_settings, /*tmp_volume*/ nullptr, /*tmp_data*/ nullptr);
    table_join->setKind(JoinKind::Inner);
    table_join->getTableJoin().strictness = JoinStrictness::All;
    table_join->addDisjunct();
    table_join->getClauses().back().addKey(
        left_header.getByPosition(0).name, right_header.getByPosition(0).name, /*null_safe_comparison*/ false);

    /// Pin the production defaults this benchmark depends on.
    chassert(table_join->enableSoftwarePrefetchInJoin());
    chassert(table_join->enableColumnsLazyReplication());
    chassert(table_join->maxJoinedBlockRows() == DEFAULT_BLOCK_SIZE);

    NamesAndTypesList left_columns;
    NamesAndTypesList right_columns;
    Names used_columns;
    for (const auto & col : left_header)
    {
        left_columns.emplace_back(col.name, col.type);
        used_columns.push_back(col.name);
    }
    for (const auto & col : right_header)
    {
        right_columns.emplace_back(col.name, col.type);
        used_columns.push_back(col.name);
    }
    table_join->setInputColumns(std::move(left_columns), std::move(right_columns));
    table_join->setUsedColumns(used_columns);
    return table_join;
}

UInt64 blockFingerprint(const Block & block)
{
    const size_t rows = block.rows();
    if (rows == 0)
        return 0;

    /// Per row: a commutative sum over columns of h(value, column name), then a non-linear
    /// finalizer so cross-column row pairing matters; per block: a commutative sum over rows.
    PaddedPODArray<UInt64> acc(rows, 0);
    for (const auto & col : block)
    {
        const UInt64 name_hash = std::hash<std::string_view>{}(col.name);
        /// With lazy columns replication the join may emit ColumnReplicated (column + indexes).
        const auto full_column = col.column->convertToFullColumnIfReplicated();
        const auto & data = assert_cast<const ColumnUInt64 &>(*full_column).getData();
        for (size_t i = 0; i < rows; ++i)
            acc[i] += intHashCRC32(data[i] ^ name_hash);
    }

    UInt64 fingerprint = 0;
    for (size_t i = 0; i < rows; ++i)
        fingerprint += intHash64(acc[i]);
    return fingerprint;
}

size_t drainJoinResult(JoinResultPtr result, UInt64 * fingerprint)
{
    size_t rows = 0;
    while (true)
    {
        auto res = result->next();
        rows += res.block.rows();
        if (fingerprint)
            *fingerprint += blockFingerprint(res.block);
        if (res.is_last)
            break;
    }
    return rows;
}

JoinStats driveJoin(IJoinBench & join, const std::vector<Block> & build_blocks, const std::vector<Block> & probe_blocks, bool verify)
{
    JoinStats stats;
    Stopwatch build_watch;
    join.build(build_blocks);
    stats.build_sec = build_watch.elapsedSeconds();

    const ProbeProfile profile_before = currentProbeProfile();
    Stopwatch probe_watch;
    stats.matches = join.probe(probe_blocks, verify ? &stats.fingerprint : nullptr);
    stats.probe_sec = probe_watch.elapsedSeconds();
    stats.probe_profile = currentProbeProfile() - profile_before;

    Stopwatch teardown_watch;
    join.teardown();
    stats.teardown_sec = teardown_watch.elapsedSeconds();
    return stats;
}

}
