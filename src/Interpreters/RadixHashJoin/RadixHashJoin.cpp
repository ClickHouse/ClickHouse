#include <Interpreters/RadixHashJoin/RadixHashJoin.h>

#include <Columns/ColumnsScatter.h>

#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/TableJoin.h>

#include <Common/CurrentThread.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

#include <algorithm>
#include <atomic>
#include <bit>
#include <cstring>
#include <limits>
#include <condition_variable>
#include <mutex>
#include <utility>

namespace ProfileEvents
{
extern const Event RadixHashJoinBuildMicroseconds;
extern const Event RadixHashJoinProbeMicroseconds;
extern const Event RadixHashJoinProbePackHashRouteMicroseconds;
extern const Event RadixHashJoinLeafGroupBuilds;
extern const Event RadixHashJoinLeafGroupBuildMicroseconds;
}

namespace CurrentMetrics
{
extern const Metric RadixHashJoinPoolThreads;
extern const Metric RadixHashJoinPoolThreadsActive;
extern const Metric RadixHashJoinPoolThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
extern const int SET_SIZE_LIMIT_EXCEEDED;
}

namespace
{

/// ---------------------------------------------------------------------------------------------
/// The radix scatter kernels formerly defined here live in `ColumnsScatter` (semantically
/// identical extraction; the hot row loops run inside that TU and are called at chunk
/// granularity, so the call cost is per (batch, column), never per row). The join keeps its own
/// planning constants and the partition/pass orchestration below.
/// ---------------------------------------------------------------------------------------------

using ColumnsScatter::ScatterScratch;
using ColumnsScatter::scatterKeyChunk;
using ColumnsScatter::scatterPidChunk;
using ColumnsScatter::histogramKeyChunk;
using ColumnsScatter::histogramRouteChunk;
using ColumnsScatter::reduceHistogramLanes;
using ColumnsScatter::foldBytes;
using ColumnsScatter::finalizeRoute;
using ColumnsScatter::scatterBatchRowsTarget;
using ColumnsScatter::widthSupportsSwwc;
using ColumnsScatter::SWWC_MIN_FANOUT;
using ColumnsScatter::HIST_INTERLEAVE_MAX_FANOUT;
using ColumnsScatter::MAX_FANOUT_PER_PASS;

/// Partition-plan constants (5.1): the target leaf working set (~L2) and the per-entry hash-table
/// byte estimate (a cell at 0.5 load factor, matching the bench bandwidth model). The per-pass
/// fanout ceiling is the module's `MAX_FANOUT_PER_PASS`.
constexpr size_t LEAF_TARGET_BYTES = 1 << 20;
constexpr size_t HT_CELL_BYTES = 16;

/// Own-output merging targets: a worker merges the blocks of the leaves it probes up to the usual
/// joined-block row count (respecting `max_joined_block_size_rows`) before returning them, so one
/// executor quantum is not spent per tiny per-leaf block; the byte cap keeps wide rows bounded.
constexpr size_t MERGE_TARGET_ROWS = 65409;
constexpr size_t MERGE_TARGET_BYTES = 2 << 20;

/// The fixed-width layout of one side (build or probe): column widths in bytes and the key columns.
struct SideLayout
{
    size_t num_columns = 0;
    std::vector<size_t> col_widths;
    std::vector<size_t> key_positions;
    std::vector<size_t> key_widths;
    bool single_key = false;
    size_t key_pos = 0;
    size_t key_width = 0;
};

SideLayout makeSideLayout(const Block & header, const Names & key_names)
{
    SideLayout layout;
    layout.num_columns = header.columns();
    layout.col_widths.resize(layout.num_columns);
    for (size_t j = 0; j < layout.num_columns; ++j)
    {
        const auto & column = header.getByPosition(j).column;
        if (!column->isFixedAndContiguous())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "RadixHashJoin: column {} is not fixed-and-contiguous", header.getByPosition(j).name);
        layout.col_widths[j] = column->sizeOfValueIfFixed();
    }
    for (const auto & name : key_names)
    {
        const size_t pos = header.getPositionByName(name);
        layout.key_positions.push_back(pos);
        layout.key_widths.push_back(layout.col_widths[pos]);
    }
    layout.single_key = layout.key_positions.size() == 1;
    if (layout.single_key)
    {
        layout.key_pos = layout.key_positions[0];
        layout.key_width = layout.key_widths[0];
    }
    return layout;
}

/// Exactly-sized destination columns of one output partition, with raw write bases.
struct PartitionOutput
{
    MutableColumns columns;
    std::vector<char *> bases;
    size_t rows = 0;

    void initialize(const Block & header, size_t rows_)
    {
        rows = rows_;
        columns.resize(header.columns());
        bases.resize(header.columns(), nullptr);
    }

    /// createColumn()+insertRawUninitialized leaves POD contents uninitialized: no memset, pages are
    /// first-touched by the scatter writes themselves. Refine passes call this just-in-time so the
    /// allocator can reuse the input column released by the preceding scatter round.
    void allocateColumn(const Block & header, const std::vector<size_t> & col_widths, size_t j)
    {
        auto col = header.getByPosition(j).type->createColumn();
        auto span = col->insertRawUninitialized(rows);
        chassert(span.size() == rows * col_widths[j]);
        bases[j] = span.data();
        columns[j] = std::move(col);
    }

    void allocate(const Block & header, const std::vector<size_t> & col_widths, size_t rows_)
    {
        initialize(header, rows_);
        for (size_t j = 0; j < header.columns(); ++j)
            allocateColumn(header, col_widths, j);
    }

    Block toBlock(const Block & header)
    {
        Columns cols;
        cols.reserve(columns.size());
        for (auto & col : columns)
            cols.emplace_back(std::move(col));
        return header.cloneWithColumns(cols);
    }
};

/// Runs `fn(tid)` on `threads` pool workers and waits (rethrows the first worker exception).
void parallelRun(ThreadPool & pool, size_t threads, const ThreadGroupPtr & thread_group, const std::function<void(size_t)> & fn)
{
    try
    {
        for (size_t t = 0; t < threads; ++t)
            pool.scheduleOrThrow(
                [&fn, t, thread_group]
                {
                    ThreadGroupSwitcher switcher(thread_group, ThreadName::RADIX_JOIN);
                    fn(t);
                });
    }
    catch (...)
    {
        /// Unwinding without the wait would free `fn` and the caller's stack while the jobs that did
        /// get scheduled still reference both.
        pool.wait();
        throw;
    }
    pool.wait();
}

std::vector<size_t> computePassBits(size_t partitions, size_t max_fanout)
{
    const size_t total_bits = std::countr_zero(std::bit_ceil(partitions));
    const size_t fanout_bits = std::max<size_t>(1, std::bit_width(std::bit_floor(std::max<size_t>(2, max_fanout))) - 1);
    const size_t num_passes = (total_bits + fanout_bits - 1) / fanout_bits;
    const size_t bits_per_pass = (total_bits + num_passes - 1) / num_passes;

    std::vector<size_t> result;
    result.reserve(num_passes);
    size_t remaining = total_bits;
    while (remaining)
    {
        const size_t bits = std::min(bits_per_pass, remaining);
        result.push_back(bits);
        remaining -= bits;
    }
    return result;
}

std::vector<size_t> makeScatterOrder(const SideLayout & layout)
{
    std::vector<size_t> order;
    order.reserve(layout.num_columns);
    if (!layout.single_key)
    {
        for (size_t j = 0; j < layout.num_columns; ++j)
            order.push_back(j);
        return order;
    }

    order.push_back(layout.key_pos);
    for (size_t j = 0; j < layout.num_columns; ++j)
        if (j != layout.key_pos)
            order.push_back(j);
    return order;
}

/// Fold one key column's rows into the route accumulators with a compile-time width: `foldBytes`
/// fully unrolls into plain loads, so the row body is one load + mix per 8-byte chunk with no
/// per-row width checks or tail dispatch.
template <size_t width>
void foldKeyColumnRows(const char * base, size_t n, UInt64 * acc)
{
    for (size_t i = 0; i < n; ++i)
        acc[i] = foldBytes(acc[i], base + i * width, width);
}

/// Composite-key route materialization for one chunk of `n` rows: fold every key column's bytes
/// into per-row accumulators (`foldBytes`), then finalize each accumulator into a route word.
/// `get_key_column_base` maps a key column position to its raw data pointer. `acc` is caller-owned
/// scratch so the first pass can reuse one allocation across its whole chunk stripe. Called at
/// chunk granularity: the hot row loops live inside, never behind a per-row call. The fold row
/// loop is width-dispatched like the module's scatter kernels (the common fixed widths hit the
/// unrolled loops; anything else folds through the runtime-width loop).
template <typename GetKeyColumnBase>
void materializeCompositeRoutes(
    const SideLayout & layout, const GetKeyColumnBase & get_key_column_base, size_t n, PaddedPODArray<UInt64> & acc, UInt32 * routes)
{
    acc.resize(n);
    memset(acc.data(), 0, n * sizeof(UInt64));
    for (size_t k = 0; k < layout.key_positions.size(); ++k)
    {
        const size_t w = layout.key_widths[k];
        const char * base = get_key_column_base(layout.key_positions[k]);
        switch (w)
        {
            case 1: foldKeyColumnRows<1>(base, n, acc.data()); break;
            case 2: foldKeyColumnRows<2>(base, n, acc.data()); break;
            case 4: foldKeyColumnRows<4>(base, n, acc.data()); break;
            case 8: foldKeyColumnRows<8>(base, n, acc.data()); break;
            case 16: foldKeyColumnRows<16>(base, n, acc.data()); break;
            default:
                for (size_t i = 0; i < n; ++i)
                    acc[i] = foldBytes(acc[i], base + i * w, w);
        }
    }
    for (size_t i = 0; i < n; ++i)
        routes[i] = finalizeRoute(acc[i]);
}

/// First radix pass: all workers cooperate on the single input group in exactly three barriers
/// (histogram, fused prefix-sum + exact allocation, fused batched column-major scatter). `blocks` is
/// consumed batch-eagerly. `Counter` is selected once from the exact side row count, outside the row
/// loops: `UInt32` in the common case and `UInt64` only for sides larger than 2^32 - 1 rows.
template <typename Counter>
std::vector<PartitionOutput> scatterFirstPass(
    ThreadPool & pool,
    size_t threads,
    const ThreadGroupPtr & thread_group,
    const Block & header,
    std::vector<Block> & blocks,
    const SideLayout & layout,
    size_t bits)
{
    const size_t fanout = 1ULL << bits;
    const UInt32 route_shift = static_cast<UInt32>(32 - bits);
    const UInt32 route_mask = static_cast<UInt32>(fanout - 1);
    const size_t num_chunks = blocks.size();
    const size_t num_columns = layout.num_columns;
    const bool use_swwc_fanout = fanout >= SWWC_MIN_FANOUT;
    const bool interleave_hist = fanout <= HIST_INTERLEAVE_MAX_FANOUT;
    const bool composite = !layout.single_key;

    std::vector<size_t> chunk_rows(num_chunks);
    for (size_t c = 0; c < num_chunks; ++c)
        chunk_rows[c] = blocks[c].rows();

    /// For composite keys, the route words are materialized once (the fold genuinely reads multiple
    /// columns); the single-key path routes straight from the key column and stores nothing.
    std::vector<PaddedPODArray<UInt32>> chunk_routes;
    if (composite)
        chunk_routes.resize(num_chunks);

    /// Barrier 1: per-worker histograms into disjoint slices of one flat array.
    PaddedPODArray<Counter> hist;
    hist.resize(threads * fanout);
    parallelRun(
        pool,
        threads,
        thread_group,
        [&](size_t tid)
        {
            Counter * h = hist.data() + tid * fanout;
            memset(h, 0, fanout * sizeof(Counter));
            std::vector<Counter> lanes;
            if (interleave_hist)
                lanes.assign(4 * fanout, 0);

            PaddedPODArray<UInt64> acc; /// composite fold accumulator, reused per chunk
            for (size_t c = tid; c < num_chunks; c += threads)
            {
                const size_t n = chunk_rows[c];
                if (composite)
                {
                    chunk_routes[c].resize(n);
                    materializeCompositeRoutes(
                        layout,
                        [&](size_t pos) { return blocks[c].getByPosition(pos).column->getRawData().data(); },
                        n,
                        acc,
                        chunk_routes[c].data());
                    histogramRouteChunk(
                        chunk_routes[c].data(), n, route_shift, route_mask, h, interleave_hist ? lanes.data() : nullptr, fanout);
                }
                else
                {
                    const char * keys = blocks[c].getByPosition(layout.key_pos).column->getRawData().data();
                    histogramKeyChunk(
                        layout.key_width, keys, n, route_shift, route_mask, h, interleave_hist ? lanes.data() : nullptr, fanout);
                }
            }
            if (interleave_hist)
                reduceHistogramLanes(h, lanes.data(), fanout);
        });

    /// Barrier 2: fused prefix sum + exact one-shot allocation. Each worker owns a contiguous,
    /// disjoint range of partitions.
    PaddedPODArray<Counter> offsets; /// per (worker, partition) start row within the partition
    offsets.resize(threads * fanout);
    std::vector<size_t> totals(fanout, 0);
    std::vector<PartitionOutput> parts(fanout);
    parallelRun(
        pool,
        threads,
        thread_group,
        [&](size_t tid)
        {
            const size_t begin = fanout * tid / threads;
            const size_t end = fanout * (tid + 1) / threads;
            for (size_t p = begin; p < end; ++p)
            {
                size_t total = 0;
                for (size_t w = 0; w < threads; ++w)
                {
                    offsets[w * fanout + p] = static_cast<Counter>(total);
                    total += hist[w * fanout + p];
                }
                totals[p] = total;
                if (total)
                    parts[p].allocate(header, layout.col_widths, total);
            }
        });

    /// Barrier 3: single fused scatter run, batched. Each worker processes its chunk stripe in batches
    /// of whole chunks; the batch's input chunks are dropped after their last column is scattered.
    const size_t batch_rows_target = scatterBatchRowsTarget(fanout);

    /// Column processing order: single-key routes the key column first (emitting the pids the payload
    /// columns then consume); composite precomputes the pids from the route words up front.
    const std::vector<size_t> scatter_order = makeScatterOrder(layout);
    const bool need_pids = composite || num_columns > 1;

    parallelRun(
        pool,
        threads,
        thread_group,
        [&](size_t tid)
        {
            ScatterScratch scratch;
            scratch.init(fanout, use_swwc_fanout);

            /// Running write cursors per (column, partition), persisted across batches.
            std::vector<char *> col_cursors(num_columns * fanout, nullptr);
            for (size_t j = 0; j < num_columns; ++j)
                for (size_t p = 0; p < fanout; ++p)
                    if (totals[p])
                        col_cursors[j * fanout + p] = parts[p].bases[j] + offsets[tid * fanout + p] * layout.col_widths[j];

            PaddedPODArray<UInt16> pids;
            std::vector<size_t> batch;
            std::vector<size_t> batch_offsets;

            size_t c = tid;
            while (c < num_chunks)
            {
                batch.clear();
                batch_offsets.clear();
                size_t batch_rows = 0;
                for (; c < num_chunks && batch_rows < batch_rows_target; c += threads)
                {
                    batch.push_back(c);
                    batch_offsets.push_back(batch_rows);
                    batch_rows += chunk_rows[c];
                }

                if (need_pids)
                    pids.resize(batch_rows);

                /// Composite: derive the batch's pids from the route words before any column scatters.
                if (composite && need_pids)
                {
                    for (size_t b = 0; b < batch.size(); ++b)
                    {
                        const size_t cc = batch[b];
                        const size_t n = chunk_rows[cc];
                        UInt16 * dst = pids.data() + batch_offsets[b];
                        const UInt32 * routes = chunk_routes[cc].data();
                        for (size_t i = 0; i < n; ++i)
                            dst[i] = static_cast<UInt16>((routes[i] >> route_shift) & route_mask);
                    }
                }

                for (size_t j : scatter_order)
                {
                    const size_t w = layout.col_widths[j];
                    const bool use_swwc = use_swwc_fanout && widthSupportsSwwc(w);
                    scratch.setUseSwwc(use_swwc);
                    for (size_t p = 0; p < fanout; ++p)
                        scratch.seed(p, col_cursors[j * fanout + p]);

                    const bool key_first = !composite && j == layout.key_pos;
                    for (size_t b = 0; b < batch.size(); ++b)
                    {
                        const size_t cc = batch[b];
                        const size_t n = chunk_rows[cc];
                        if (!n)
                            continue;
                        const char * data = blocks[cc].getByPosition(j).column->getRawData().data();
                        UInt16 * pid_slice = need_pids ? pids.data() + batch_offsets[b] : nullptr;
                        if (key_first)
                        {
                            scatterKeyChunk(layout.key_width, data, n, route_shift, route_mask, pid_slice, use_swwc, scratch);
                        }
                        else
                        {
                            scatterPidChunk(w, pids.data() + batch_offsets[b], data, n, use_swwc, scratch);
                        }
                    }
                    scratch.drain();
                    for (size_t p = 0; p < fanout; ++p)
                        col_cursors[j * fanout + p] = scratch.cursors[p];
                }

                /// The batch is fully consumed: drop its input chunks before the next batch.
                for (size_t cc : batch)
                    blocks[cc].clear();
            }
        });

    return parts;
}

/// Refine one previous-pass output group. One worker owns the group for the whole operation. Output
/// columns are allocated just-in-time and each consumed input column is released before the next
/// output-column allocation, keeping resident data near one copy of the group.
template <typename Counter>
void scatterRefineGroup(
    const Block & header,
    const SideLayout & layout,
    PartitionOutput & group,
    size_t bits,
    size_t bits_done,
    std::vector<PartitionOutput> & out,
    size_t out_begin)
{
    const size_t fanout = 1ULL << bits;
    const UInt32 route_shift = static_cast<UInt32>(32 - bits_done - bits);
    const UInt32 route_mask = static_cast<UInt32>(fanout - 1);
    const bool use_swwc_fanout = fanout >= SWWC_MIN_FANOUT;
    const bool interleave_hist = fanout <= HIST_INTERLEAVE_MAX_FANOUT;
    const bool composite = !layout.single_key;
    const bool need_pids = composite || layout.num_columns > 1;

    std::vector<Counter> hist(fanout, 0);
    std::vector<Counter> lanes;
    if (interleave_hist)
        lanes.assign(4 * fanout, 0);

    PaddedPODArray<UInt16> pids;
    if (need_pids)
        pids.resize(group.rows);

    if (composite)
    {
        PaddedPODArray<UInt64> accumulators;
        PaddedPODArray<UInt32> routes;
        routes.resize(group.rows);
        materializeCompositeRoutes(
            layout,
            [&](size_t pos) { return group.columns[pos]->getRawData().data(); },
            group.rows,
            accumulators,
            routes.data());

        histogramRouteChunk(
            routes.data(), group.rows, route_shift, route_mask, hist.data(), interleave_hist ? lanes.data() : nullptr, fanout);
        for (size_t i = 0; i < group.rows; ++i)
            pids[i] = static_cast<UInt16>((routes[i] >> route_shift) & route_mask);
    }
    else
    {
        const char * keys = group.columns[layout.key_pos]->getRawData().data();
        histogramKeyChunk(
            layout.key_width, keys, group.rows, route_shift, route_mask, hist.data(), interleave_hist ? lanes.data() : nullptr, fanout);
    }
    if (interleave_hist)
        reduceHistogramLanes(hist.data(), lanes.data(), fanout);

    std::vector<PartitionOutput> parts(fanout);
    for (size_t p = 0; p < fanout; ++p)
        if (hist[p])
            parts[p].initialize(header, hist[p]);

    ScatterScratch scratch;
    scratch.init(fanout, use_swwc_fanout);
    const std::vector<size_t> scatter_order = makeScatterOrder(layout);
    for (size_t j : scatter_order)
    {
        const size_t width = layout.col_widths[j];
        const bool use_swwc = use_swwc_fanout && widthSupportsSwwc(width);
        scratch.setUseSwwc(use_swwc);
        for (size_t p = 0; p < fanout; ++p)
        {
            if (hist[p])
                parts[p].allocateColumn(header, layout.col_widths, j);
            scratch.seed(p, hist[p] ? parts[p].bases[j] : nullptr);
        }

        const char * data = group.columns[j]->getRawData().data();
        if (!composite && j == layout.key_pos)
        {
            scatterKeyChunk(
                layout.key_width, data, group.rows, route_shift, route_mask, need_pids ? pids.data() : nullptr, use_swwc, scratch);
        }
        else
        {
            scatterPidChunk(width, pids.data(), data, group.rows, use_swwc, scratch);
        }
        scratch.drain();

        group.columns[j].reset();
        group.bases[j] = nullptr;
    }

    for (size_t p = 0; p < fanout; ++p)
        if (hist[p])
            out[out_begin + p] = std::move(parts[p]);

    group.columns.clear();
    group.bases.clear();
    group.rows = 0;
}

/// Later radix passes: dynamically assign differently-sized previous-pass groups to workers. Each
/// group uses the narrowest safe histogram counters, chosen before its row loops.
std::vector<PartitionOutput> scatterRefinePass(
    ThreadPool & pool,
    size_t threads,
    const ThreadGroupPtr & thread_group,
    const Block & header,
    const SideLayout & layout,
    std::vector<PartitionOutput> & groups,
    size_t bits,
    size_t bits_done)
{
    const size_t fanout = 1ULL << bits;
    std::vector<PartitionOutput> out(groups.size() * fanout);
    std::atomic<size_t> next_group{0};

    parallelRun(
        pool,
        threads,
        thread_group,
        [&](size_t)
        {
            for (size_t g = next_group.fetch_add(1, std::memory_order_relaxed); g < groups.size();
                 g = next_group.fetch_add(1, std::memory_order_relaxed))
            {
                if (!groups[g].rows)
                    continue;
                if (groups[g].rows <= std::numeric_limits<UInt32>::max())
                    scatterRefineGroup<UInt32>(header, layout, groups[g], bits, bits_done, out, g * fanout);
                else
                    scatterRefineGroup<UInt64>(header, layout, groups[g], bits, bits_done, out, g * fanout);
            }
        });
    return out;
}

/// Radix-scatter a side according to a plan of disjoint route-word bit slices. A one-pass plan calls
/// only the cooperative three-barrier kernel above. Multi-pass plans feed its exactly-sized outputs
/// through dynamically scheduled, memory-cycling refine passes.
std::vector<PartitionOutput> scatterToPartitions(
    ThreadPool & pool,
    size_t threads,
    const ThreadGroupPtr & thread_group,
    const Block & header,
    std::vector<Block> & blocks,
    const SideLayout & layout,
    const std::vector<size_t> & pass_bits)
{
    chassert(!pass_bits.empty());
    size_t total_rows = 0;
    for (const auto & block : blocks)
        total_rows += block.rows();

    std::vector<PartitionOutput> groups;
    if (total_rows <= std::numeric_limits<UInt32>::max())
        groups = scatterFirstPass<UInt32>(pool, threads, thread_group, header, blocks, layout, pass_bits.front());
    else
        groups = scatterFirstPass<UInt64>(pool, threads, thread_group, header, blocks, layout, pass_bits.front());

    size_t bits_done = pass_bits.front();
    for (size_t pass = 1; pass < pass_bits.size(); ++pass)
    {
        groups = scatterRefinePass(pool, threads, thread_group, header, layout, groups, pass_bits[pass], bits_done);
        bits_done += pass_bits[pass];
    }
    return groups;
}

}

/// -------------------------------------------------------------------------------------------------
/// The cooperative wave engine (formal contract: WaveJoinProbe.tla)
///
/// Exactly one shared probe wave exists. While it is FILLING, the probe lanes admit their blocks
/// against the byte budget; the reservation whose atomic addition crosses the budget seals it, and
/// once every in-flight admission has landed, the same lanes drain it cooperatively by claiming
/// jobs from the explicit work graph
///
///     prepare (per first-pass partition: exact sizing + per-block write ranges)
///  -> scatter (per admitted block, stable: disjoint precomputed ranges)
///  -> refine  (per group, one refine stage per remaining radix pass)
///  -> probe   (per leaf: the smallest task; output goes to the CLAIMING worker's own result)
///
/// There are no dedicated producer or drain crews, no probe-side thread pool, no shared queue of
/// completed output, and no central scheduler: any worker claims any job with one CAS, barriers are
/// run inline by each stage's last finisher, and the delayed-blocks stream is a thin adapter that
/// runs this same machine for the final partial wave. The budget bounds only the ACCOUNTED wave
/// bytes (admitted + in-flight, overshoot at most one block); drain arenas, route words, in-flight
/// input and output live outside it.
/// -------------------------------------------------------------------------------------------------

namespace
{

/// Filling accepts admissions; Sealing waits for in-flight admissions to land; the drain phases
/// follow the work graph above; Poisoned is terminal (first error wins, or an abandoned mid-drain
/// result — fail-close, never silent truncation).
enum class WavePhase : UInt8
{
    Filling,
    Sealing,
    Preparing,
    Scattering,
    Refining,
    Probing,
    Poisoned,
};

/// One leaf probe in flight: the leaf's result chain (`max_joined_block` splitting included).
struct LeafRun
{
    HashJoin * leaf = nullptr;
    JoinResultPtr res;
};

/// One participating worker's call-local state: its not-yet-admitted input, the leaf it currently
/// probes, and its own output merged up to the flush target. Output never crosses workers: what a
/// context accumulates is returned only by its own caller.
struct WaveWorker
{
    Block pending;
    LeafRun run;
    Blocks merged;
    size_t merged_rows = 0;
    size_t merged_bytes = 0;
    /// Reused across this worker's scatter jobs (allocated once per worker, not once per job).
    ScatterScratch scratch;
    PaddedPODArray<UInt16> pids;
};

/// Probe drain order: the ids of the partitions worth probing (non-empty on both sides), largest
/// probe partition first. One worker probes one partition, so a wave's wall time is lower-bounded
/// by its largest partition; starting the largest first (LPT scheduling) keeps it off the tail
/// under imbalance, and draining the biggest buffers first also releases the most memory earliest
/// (a partition's columns are moved out when probed). Ties break by partition id for determinism.
std::vector<UInt32> probeDrainOrder(const std::vector<PartitionOutput> & parts, const std::vector<std::unique_ptr<HashJoin>> & partition_joins)
{
    std::vector<UInt32> order;
    order.reserve(parts.size());
    for (size_t p = 0; p < parts.size(); ++p)
        if (parts[p].rows && partition_joins[p])
            order.push_back(static_cast<UInt32>(p));
    std::sort(
        order.begin(),
        order.end(),
        [&](UInt32 a, UInt32 b)
        {
            if (parts[a].rows != parts[b].rows)
                return parts[a].rows > parts[b].rows;
            return a < b;
        });
    return order;
}

struct ProbeWave
{
    /// Admission word [seal:1 | in-flight:15 | reserved bytes:48]: one CAS is both the budget check
    /// and the in-flight count, so the drain can only begin after every granted reservation has
    /// landed. The seal bit exists for the EOF seal of a below-budget final wave.
    static constexpr UInt64 ADMIT_SEAL = 1ULL << 63;
    static constexpr UInt64 ADMIT_INFLIGHT = 1ULL << 48;
    static constexpr UInt64 ADMIT_BYTES_MASK = ADMIT_INFLIGHT - 1;

    /// Control word [phase:8 | stage generation:32 | next job index:24]: a claim is one CAS bound to
    /// the phase and generation it read, so it can never cross a stage boundary; every transition
    /// bumps the generation, which is also the only thing waiters need to watch.
    static constexpr UInt64 CTRL_PHASE_SHIFT = 56;
    static constexpr UInt64 CTRL_GEN_SHIFT = 24;
    static constexpr UInt64 CTRL_INDEX_MASK = (1ULL << 24) - 1;

    static constexpr UInt64 pack(WavePhase ph, UInt64 gen, UInt64 index)
    {
        return (static_cast<UInt64>(ph) << CTRL_PHASE_SHIFT) | ((gen & 0xFFFFFFFFULL) << CTRL_GEN_SHIFT) | index;
    }
    static constexpr WavePhase phaseOf(UInt64 word) { return static_cast<WavePhase>(word >> CTRL_PHASE_SHIFT); }
    static constexpr UInt64 genOf(UInt64 word) { return (word >> CTRL_GEN_SHIFT) & 0xFFFFFFFFULL; }
    static constexpr UInt64 indexOf(UInt64 word) { return word & CTRL_INDEX_MASK; }

    std::atomic<UInt64> admission{0};
    std::atomic<UInt64> control{pack(WavePhase::Filling, 0, 0)};
    std::atomic<UInt32> stage_jobs{0};
    std::atomic<UInt32> stage_remaining{0};

    std::mutex mutex; /// admissions list, primary error, delayed runs, left-side init, completion
    std::condition_variable cv; /// sealed-tail and bounded phase-transition waits
    std::exception_ptr primary; /// first exception, under mutex

    /// Plan and shared references, bound once under mutex before the first admission; they point
    /// into State, which outlives every result and the delayed stream.
    const Block * header = nullptr;
    const SideLayout * layout = nullptr;
    const std::vector<size_t> * pass_bits = nullptr;
    std::vector<std::unique_ptr<HashJoin>> * leaves = nullptr;
    size_t budget = 0;
    size_t lane_merge_rows = 1; /// own-output flush threshold in rows; 1 returns blocks as produced

    struct Admission
    {
        Block block;
        PaddedPODArray<UInt32> routes; /// composite keys only
        PaddedPODArray<UInt32> hist; /// first-pass histogram
    };
    std::vector<Admission> admitted;

    /// Stage data: written only by the single barrier owner (or the sealer) before the release
    /// store of `control`, read by claimers after their acquire load.
    size_t refine_pass = 0;
    size_t bits_done = 0;
    PaddedPODArray<UInt32> offsets; /// per (admission, first-pass partition) start rows
    std::vector<PartitionOutput> groups; /// the stage being written
    std::vector<PartitionOutput> prev; /// the refine stage's input groups
    std::vector<UInt32> probe_order;
    std::vector<LeafRun> delayed_runs; /// split leaf probes of delayed pulls, under mutex
    bool delayed_taken = false;

    /// --- admission (TLA Reserve / Admit / Seal) ---

    bool tryReserve(size_t bytes, bool & crossed)
    {
        UInt64 cur = admission.load(std::memory_order_relaxed);
        while (true)
        {
            if ((cur & ADMIT_SEAL) || (cur & ADMIT_BYTES_MASK) >= budget)
                return false;
            const UInt64 next = cur + bytes + ADMIT_INFLIGHT;
            if (admission.compare_exchange_weak(cur, next, std::memory_order_acq_rel))
            {
                crossed = (next & ADMIT_BYTES_MASK) >= budget;
                return true;
            }
        }
    }

    void admit(Block block, bool crossed)
    {
        Admission adm;
        adm.block = std::move(block);
        const size_t n = adm.block.rows();
        const size_t fanout = size_t(1) << pass_bits->front();
        const UInt32 shift = static_cast<UInt32>(32 - pass_bits->front());
        const UInt32 mask = static_cast<UInt32>(fanout - 1);
        adm.hist.resize_fill(fanout);
        {
            ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::RadixHashJoinProbePackHashRouteMicroseconds);
            if (layout->single_key)
            {
                const char * keys = adm.block.getByPosition(layout->key_pos).column->getRawData().data();
                histogramKeyChunk(layout->key_width, keys, n, shift, mask, adm.hist.data(), nullptr, fanout);
            }
            else
            {
                adm.routes.resize(n);
                PaddedPODArray<UInt64> acc;
                materializeCompositeRoutes(
                    *layout,
                    [&](size_t pos) { return adm.block.getByPosition(pos).column->getRawData().data(); },
                    n,
                    acc,
                    adm.routes.data());
                histogramRouteChunk(adm.routes.data(), n, shift, mask, adm.hist.data(), nullptr, fanout);
            }
        }

        /// The crossing admission seals; a Poisoned or already-Sealing control word stands.
        if (crossed)
        {
            UInt64 cur = control.load(std::memory_order_relaxed);
            while (phaseOf(cur) == WavePhase::Filling
                   && !control.compare_exchange_weak(cur, pack(WavePhase::Sealing, genOf(cur) + 1, 0), std::memory_order_acq_rel))
                ;
        }
        {
            std::lock_guard lock(mutex);
            admitted.push_back(std::move(adm));
        }
        /// The admission that lands last while sealed begins the drain: its in-flight decrement is
        /// ordered after the Sealing store above (its own or an earlier one).
        const UInt64 prev_word = admission.fetch_sub(ADMIT_INFLIGHT, std::memory_order_acq_rel);
        if (((prev_word >> 48) & 0x7FFF) == 1 && phaseOf(control.load(std::memory_order_acquire)) == WavePhase::Sealing)
            beginDrain();
    }

    /// --- the drain stage machine (TLA barriers; run by the sealer / each stage's last finisher) ---

    void publishStage(WavePhase ph, size_t jobs)
    {
        chassert(jobs <= CTRL_INDEX_MASK);
        stage_jobs.store(static_cast<UInt32>(jobs), std::memory_order_relaxed);
        stage_remaining.store(static_cast<UInt32>(jobs), std::memory_order_relaxed);
        const UInt64 gen = genOf(control.load(std::memory_order_relaxed));
        control.store(pack(ph, gen + 1, 0), std::memory_order_release);
        {
            std::lock_guard lock(mutex);
        }
        cv.notify_all();
    }

    /// The sealing transition only publishes the prepare stage; the sizing itself is claimable
    /// per-partition jobs (the contract's PreJob), so the exact-allocation work — dominated by the
    /// allocator recycling the previous wave's extents — is spread across every participating lane
    /// instead of serializing on the sealer while the other lanes wait.
    void beginDrain()
    {
        const size_t fanout = size_t(1) << pass_bits->front();
        offsets.resize(admitted.size() * fanout);
        groups.clear();
        groups.resize(fanout);
        bits_done = pass_bits->front();
        refine_pass = 0;
        publishStage(WavePhase::Preparing, fanout);
    }

    /// One prepare job: partition p's per-(block, partition) write ranges from the admission
    /// histograms and its exactly-sized allocation — this is what keeps the scatter stable and the
    /// arenas exactly sized. Jobs touch disjoint offset slots and disjoint groups.
    void runPrepare(size_t p)
    {
        const size_t fanout = size_t(1) << pass_bits->front();
        size_t total = 0;
        for (size_t b = 0; b < admitted.size(); ++b)
        {
            offsets[b * fanout + p] = static_cast<UInt32>(total);
            total += admitted[b].hist[p];
        }
        if (total)
            groups[p].allocate(*header, layout->col_widths, total);
    }

    void advanceStage()
    {
        switch (phaseOf(control.load(std::memory_order_relaxed)))
        {
            case WavePhase::Preparing:
                publishStage(WavePhase::Scattering, admitted.size());
                break;
            case WavePhase::Scattering:
            case WavePhase::Refining:
                if (refine_pass + 1 < pass_bits->size())
                {
                    ++refine_pass;
                    bits_done += (*pass_bits)[refine_pass];
                    prev.swap(groups);
                    groups.clear();
                    groups.resize(prev.size() << (*pass_bits)[refine_pass]);
                    publishStage(WavePhase::Refining, prev.size());
                }
                else
                {
                    prev.clear();
                    probe_order = probeDrainOrder(groups, *leaves);
                    /// Probe rows in partitions with no build partner produce nothing; release them.
                    for (size_t p = 0; p < groups.size(); ++p)
                        if (groups[p].rows && !(*leaves)[p])
                            groups[p] = {};
                    if (probe_order.empty())
                        completeWave();
                    else
                        publishStage(WavePhase::Probing, probe_order.size());
                }
                break;
            case WavePhase::Probing:
                completeWave();
                break;
            default:
                break; /// Poisoned: the wave stays wherever it was; State teardown releases it
        }
    }

    void completeWave()
    {
        std::unique_lock lock(mutex);
        admitted.clear();
        offsets.clear();
        groups.clear();
        prev.clear();
        probe_order.clear();
        const UInt64 gen = genOf(control.load(std::memory_order_relaxed));
        admission.store(0, std::memory_order_release);
        control.store(pack(WavePhase::Filling, gen + 1, 0), std::memory_order_release);
        lock.unlock();
        cv.notify_all();
    }

    void poison(std::exception_ptr e)
    {
        std::unique_lock lock(mutex);
        if (!primary)
            primary = std::move(e);
        const UInt64 gen = genOf(control.load(std::memory_order_relaxed));
        control.store(pack(WavePhase::Poisoned, gen + 1, 0), std::memory_order_release);
        lock.unlock();
        cv.notify_all();
    }

    [[noreturn]] void rethrowPrimary()
    {
        std::exception_ptr e;
        {
            std::lock_guard lock(mutex);
            e = primary;
        }
        chassert(e);
        std::rethrow_exception(e);
    }

    /// --- claims and job bodies (TLA Claim / Finish*) ---

    /// Claims job `indexOf(word)` iff the control word is still exactly `word`: one CAS, bound to
    /// the phase and generation the caller dispatched on. Takes the word BY VALUE: a failed CAS
    /// must never overwrite the caller's loop snapshot, because `tailOrWait` waits for the control
    /// word to differ from that snapshot — waiting on the post-failure CURRENT value would sleep
    /// through a transition that already happened (the 04509 delayed-seal deadlock).
    bool claim(UInt64 word)
    {
        if (indexOf(word) >= stage_jobs.load(std::memory_order_relaxed))
            return false;
        return control.compare_exchange_strong(word, word + 1, std::memory_order_acq_rel);
    }

    void finishJob()
    {
        if (stage_remaining.fetch_sub(1, std::memory_order_acq_rel) == 1)
            advanceStage();
    }

    /// Stable scatter of one admitted block into the precomputed disjoint ranges; the input block
    /// is released here, by its one job, exactly once.
    void runScatter(WaveWorker & w, size_t b)
    {
        auto & adm = admitted[b];
        const size_t n = adm.block.rows();
        const size_t fanout = size_t(1) << pass_bits->front();
        const UInt32 shift = static_cast<UInt32>(32 - pass_bits->front());
        const UInt32 mask = static_cast<UInt32>(fanout - 1);
        const bool use_swwc_fanout = fanout >= SWWC_MIN_FANOUT;
        const bool composite = !layout->single_key;
        const bool need_pids = composite || layout->num_columns > 1;

        if (w.scratch.fanout != fanout)
            w.scratch.init(fanout, use_swwc_fanout);
        if (need_pids)
            w.pids.resize(n);
        if (composite)
            for (size_t i = 0; i < n; ++i)
                w.pids[i] = static_cast<UInt16>((adm.routes[i] >> shift) & mask);

        for (size_t j : makeScatterOrder(*layout))
        {
            const size_t width = layout->col_widths[j];
            const bool use_swwc = use_swwc_fanout && widthSupportsSwwc(width);
            w.scratch.setUseSwwc(use_swwc);
            for (size_t p = 0; p < fanout; ++p)
                w.scratch.seed(p, groups[p].rows ? groups[p].bases[j] + offsets[b * fanout + p] * width : nullptr);

            const char * data = adm.block.getByPosition(j).column->getRawData().data();
            if (!composite && j == layout->key_pos)
                scatterKeyChunk(layout->key_width, data, n, shift, mask, need_pids ? w.pids.data() : nullptr, use_swwc, w.scratch);
            else
                scatterPidChunk(width, w.pids.data(), data, n, use_swwc, w.scratch);
            w.scratch.drain();
        }
        adm = {};
    }

    void runRefine(size_t g)
    {
        if (!prev[g].rows)
            return;
        const size_t bits = (*pass_bits)[refine_pass];
        if (prev[g].rows <= std::numeric_limits<UInt32>::max())
            scatterRefineGroup<UInt32>(*header, *layout, prev[g], bits, bits_done - bits, groups, g << bits);
        else
            scatterRefineGroup<UInt64>(*header, *layout, prev[g], bits, bits_done - bits, groups, g << bits);
    }

    /// --- probing: leaf runs and own-output merging ---

    static void mergeOwn(WaveWorker & w, Block out)
    {
        w.merged_rows += out.rows();
        w.merged_bytes += out.bytes();
        /// Lazily-replicated columns do not support appending; normalize before the concat.
        Columns columns = out.getColumns();
        for (auto & column : columns)
            column = column->convertToFullColumnIfReplicated();
        out.setColumns(columns);
        w.merged.push_back(std::move(out));
    }

    static Block flushOwn(WaveWorker & w)
    {
        if (w.merged.empty())
            return {};
        Block out = w.merged.size() == 1 ? std::move(w.merged.front()) : concatenateBlocks(w.merged);
        w.merged.clear();
        w.merged_rows = 0;
        w.merged_bytes = 0;
        return out;
    }

    LeafRun openLeaf(size_t index)
    {
        const size_t p = probe_order[index];
        LeafRun run;
        run.leaf = (*leaves)[p].get();
        run.res = run.leaf->joinBlock(groups[p].toBlock(*header));
        return run;
    }

    /// Drives the current leaf by one inner block into the worker's own output; true iff the leaf
    /// completed (the caller then finishes the probe job).
    static bool stepLeaf(WaveWorker & w)
    {
        auto r = w.run.res->next();
        if (r.block.rows())
            mergeOwn(w, std::move(r.block));
        if (!r.is_last)
            return false;
        if (r.next_block)
        {
            r.next_block->filterBySelector();
            Block next_block = std::move(*r.next_block).getSourceBlock();
            if (next_block.rows())
            {
                w.run.res = w.run.leaf->joinBlock(std::move(next_block));
                return false;
            }
        }
        w.run = {};
        return true;
    }

    /// --- the worker loop (shared by the lane results and the delayed-blocks stream) ---

    /// One participation quantum. A lane admits its pending input when the wave accepts it and
    /// otherwise claims sealed-wave drain work; the first delayed pull seals the final partial
    /// wave instead (TLA EOFSeal) and parks split leaf probes in `delayed_runs` for any pull to
    /// continue. Returns this worker's own output; sets `is_last` when a lane owes nothing more
    /// (a delayed pull instead returns empty exactly when the machine is finished). The only
    /// blocking wait is the bounded sealed-tail / phase-transition wait.
    Block pull(WaveWorker & w, bool delayed, bool & is_last)
    {
        try
        {
            while (true)
            {
                UInt64 word = control.load(std::memory_order_acquire);
                const WavePhase ph = phaseOf(word);
                switch (ph)
                {
                    case WavePhase::Poisoned:
                        w.run = {};
                        w.pending = {};
                        rethrowPrimary();

                    case WavePhase::Filling:
                    case WavePhase::Sealing:
                    {
                        if (delayed)
                        {
                            {
                                std::lock_guard lock(mutex);
                                if (admitted.empty())
                                    return {};
                            }
                            /// The seal CAS works on a COPY: a failed CAS must not overwrite the
                            /// loop snapshot `word`, or the wait below would use the current value
                            /// and sleep through the transition it is waiting for — lethal when the
                            /// winner drains the whole wave to the terminal state in the meantime
                            /// (the 04509 deadlock; stacks in evidence/deadlock_04509_gdb_bt.txt).
                            UInt64 expected = word;
                            if (ph == WavePhase::Filling
                                && control.compare_exchange_strong(
                                    expected, pack(WavePhase::Sealing, genOf(word) + 1, 0), std::memory_order_acq_rel))
                            {
                                admission.fetch_or(ADMIT_SEAL, std::memory_order_acq_rel);
                                beginDrain();
                            }
                            else
                                tailOrWait(w, word, delayed, is_last);
                            break;
                        }
                        if (w.pending.empty())
                        {
                            is_last = true;
                            return flushOwn(w);
                        }
                        bool crossed = false;
                        if (ph == WavePhase::Filling && tryReserve(w.pending.allocatedBytes(), crossed))
                        {
                            admit(std::exchange(w.pending, {}), crossed);
                            break;
                        }
                        /// Sealed (or sealing): wait out the bounded transition, then help drain.
                        Block out = tailOrWait(w, word, delayed, is_last);
                        if (!out.empty() || is_last)
                            return out;
                        break;
                    }

                    case WavePhase::Preparing:
                    case WavePhase::Scattering:
                    case WavePhase::Refining:
                    {
                        if (!claim(word))
                        {
                            Block out = tailOrWait(w, word, delayed, is_last);
                            if (!out.empty() || is_last)
                                return out;
                            break;
                        }
                        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::RadixHashJoinProbePackHashRouteMicroseconds);
                        if (ph == WavePhase::Preparing)
                            runPrepare(indexOf(word));
                        else if (ph == WavePhase::Scattering)
                            runScatter(w, indexOf(word));
                        else
                            runRefine(indexOf(word));
                        finishJob();
                        break;
                    }

                    case WavePhase::Probing:
                    {
                        if (!w.run.res && delayed)
                        {
                            std::lock_guard lock(mutex);
                            if (!delayed_runs.empty())
                            {
                                w.run = std::move(delayed_runs.back());
                                delayed_runs.pop_back();
                            }
                        }
                        if (!w.run.res && claim(word))
                            w.run = openLeaf(indexOf(word));
                        if (!w.run.res)
                        {
                            Block out = tailOrWait(w, word, delayed, is_last);
                            if (!out.empty() || is_last)
                                return out;
                            break;
                        }
                        if (stepLeaf(w))
                            finishJob();
                        else if (delayed)
                        {
                            std::lock_guard lock(mutex);
                            delayed_runs.push_back(std::exchange(w.run, {}));
                            cv.notify_one();
                        }
                        const size_t flush_rows = delayed ? 1 : lane_merge_rows;
                        if (w.merged_rows >= flush_rows || w.merged_bytes >= MERGE_TARGET_BYTES)
                        {
                            is_last = false;
                            return flushOwn(w);
                        }
                        break;
                    }
                }
            }
        }
        catch (...)
        {
            w.run = {};
            poison(std::current_exception());
            throw;
        }
    }

    /// Nothing is claimable in the current stage. A lane with no obligations leaves (its owed
    /// output flushed); a lane still holding pending input first returns any merged output and
    /// then waits for the next transition — bounded by the stage's last in-flight jobs. Delayed
    /// pulls always wait (their end is the machine finishing, observed as Filling-and-empty).
    Block tailOrWait(WaveWorker & w, UInt64 word, bool delayed, bool & is_last)
    {
        if (!delayed)
        {
            if (w.pending.empty())
            {
                is_last = true;
                return flushOwn(w);
            }
            Block out = flushOwn(w);
            if (!out.empty())
                return out;
        }
        std::unique_lock lock(mutex);
        cv.wait(lock, [&] { return (delayed && !delayed_runs.empty()) || control.load(std::memory_order_relaxed) != word; });
        return {};
    }

    /// A result abandoned while it still owes work (pending input or a half-probed leaf) would
    /// silently drop rows; fail close instead. Abandonment after an error keeps the first error.
    void abandon(WaveWorker & w)
    {
        if (w.pending.empty() && !w.run.res)
            return;
        w.run = {};
        w.pending = {};
        poison(std::make_exception_ptr(
            Exception(ErrorCodes::LOGICAL_ERROR, "RadixHashJoin: probe result abandoned mid-wave; its rows would be lost")));
    }
};

}

struct RadixHashJoin::State
{
    Names left_key_names;
    Names right_key_names;

    /// Build accumulation. One slot per build lane; lanes are stable per stream but not guaranteed
    /// distinct across streams (IJoin.h contract), so each has a mutex (uncontended in practice).
    struct BuildLane
    {
        std::mutex mutex;
        std::vector<Block> blocks;
    };
    std::vector<BuildLane> build_lanes;
    std::atomic<size_t> build_rows{0};
    std::atomic<size_t> build_bytes{0};

    std::vector<Block> build_blocks; /// concatenated at the build barrier
    std::atomic<bool> post_build_done{false};

    size_t fanout = 0;
    std::vector<size_t> pass_bits;
    std::vector<std::unique_ptr<HashJoin>> partition_joins; /// size fanout, nullptr = empty partition
    size_t post_build_bytes = 0;
    size_t probe_window_budget = 0;

    /// The one shared probe wave; left layout is resolved from the first probe block under its
    /// mutex. Everything the wave holds is released either at wave completion (by its last probe
    /// job) or, after poisoning, here at State destruction — exactly once either way.
    ProbeWave wave_state;
    Block left_header;
    SideLayout left_layout;
    bool left_ready = false;

    std::unique_ptr<HashJoin> schema_join;
    /// The dedicated radix pool, used only by the build side (post-build scatter, leaf builds,
    /// destructor teardown). The probe path runs entirely on the executor's own lanes.
    std::unique_ptr<ThreadPool> pool;
    bool enable_lazy_columns_indexing = true;

    /// Binds the wave's plan references once the left side is known (under the wave mutex). A
    /// worker merges its own output only where a returned block costs an executor quantum: on the
    /// lanes, and only when more than one thread runs (at one thread the copy is pure cost). The
    /// joined-block row cap is respected, unlike a merge behind a shared buffer could.
    void bindWave(const TableJoin & table_join_, size_t max_threads_)
    {
        wave_state.header = &left_header;
        wave_state.layout = &left_layout;
        wave_state.pass_bits = &pass_bits;
        wave_state.leaves = &partition_joins;
        wave_state.budget = probe_window_budget;
        if (max_threads_ > 1)
        {
            wave_state.lane_merge_rows = MERGE_TARGET_ROWS;
            if (const size_t cap = table_join_.maxJoinedBlockRows())
                wave_state.lane_merge_rows = std::min<size_t>(wave_state.lane_merge_rows, cap);
        }
    }
};

/// -------------------------------------------------------------------------------------------------
/// Probe result and delayed-blocks stream
/// -------------------------------------------------------------------------------------------------

namespace
{

/// One probe lane's result: a participating worker. Its next() admits the lane's block when the
/// wave accepts input, claims sealed-wave drain work otherwise, and returns only this worker's own
/// output — one bounded quantum per call, no lock ever held between calls.
class CooperativeWaveResult : public IJoinResult
{
public:
    CooperativeWaveResult(ProbeWave & wave_, Block block)
        : wave(wave_)
    {
        worker.pending = std::move(block);
    }

    /// Destroying a result that still owes work poisons the wave (fail-close); see ProbeWave::abandon.
    ~CooperativeWaveResult() override
    {
        wave.abandon(worker);
    }

    JoinResultBlock next() override
    {
        ProfileEventTimeIncrement<Microseconds> probe_watch(ProfileEvents::RadixHashJoinProbeMicroseconds);
        bool is_last = false;
        Block block = wave.pull(worker, /*delayed*/ false, is_last);
        return {std::move(block), nullptr, is_last};
    }

private:
    ProbeWave & wave;
    WaveWorker worker;
};

/// The final flush: a thin adapter over the same wave machine, pulled concurrently by the
/// executor's delayed-worker transforms.
class CooperativeDelayedBlocks : public IBlocksStream
{
public:
    explicit CooperativeDelayedBlocks(ProbeWave & wave_)
        : wave(wave_)
    {
    }

protected:
    Block nextImpl() override
    {
        ProfileEventTimeIncrement<Microseconds> probe_watch(ProfileEvents::RadixHashJoinProbeMicroseconds);
        WaveWorker worker;
        bool is_last = false;
        return wave.pull(worker, /*delayed*/ true, is_last);
    }

private:
    ProbeWave & wave;
};

}

/// -------------------------------------------------------------------------------------------------
/// RadixHashJoin
/// -------------------------------------------------------------------------------------------------

RadixHashJoin::RadixHashJoin(
    std::shared_ptr<TableJoin> table_join_,
    SharedHeader right_sample_block_,
    size_t max_threads_,
    std::optional<UInt64> rhs_size_estimation_,
    UInt64 max_partitions_per_pass_,
    bool size_tables_by_distinct_estimate_,
    double probe_buffer_fraction_,
    UInt64 probe_buffer_min_bytes_,
    UInt64 probe_buffer_max_bytes_,
    const StatsCollectingParams & stats_collecting_params_)
    : table_join(std::move(table_join_))
    , right_sample_block(right_sample_block_)
    , max_threads(std::max<size_t>(max_threads_, 1))
    , rhs_size_estimation(rhs_size_estimation_)
    , max_partitions_per_pass(max_partitions_per_pass_)
    , size_tables_by_distinct_estimate(size_tables_by_distinct_estimate_)
    , probe_buffer_fraction(probe_buffer_fraction_)
    , probe_buffer_min_bytes(probe_buffer_min_bytes_)
    , probe_buffer_max_bytes(probe_buffer_max_bytes_)
    , stats_collecting_params(stats_collecting_params_)
    , state(std::make_unique<State>())
{
    /// Re-check the planner-gate invariants (the planner should never let a violating shape through).
    if (!table_join->oneDisjunct())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RadixHashJoin requires a single join disjunct");
    if (table_join->kind() != JoinKind::Inner || table_join->strictness() != JoinStrictness::All)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RadixHashJoin supports only INNER ALL joins");
    if (table_join->isSpecialStorage())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RadixHashJoin does not support special storage");

    if (!(probe_buffer_fraction >= 0.0 && probe_buffer_fraction <= 1.0))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "radix_join_probe_buffer_fraction must be in [0, 1]");
    if (probe_buffer_max_bytes != 0 && probe_buffer_min_bytes > probe_buffer_max_bytes)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "radix_join_probe_buffer_min_bytes must not exceed radix_join_probe_buffer_max_bytes");

    const auto & clause = table_join->getOnlyClause();
    state->left_key_names = clause.key_names_left;
    state->right_key_names = clause.key_names_right;

    size_t packed_key_width = 0;
    for (const auto & name : state->right_key_names)
    {
        const auto * key_column = right_sample_block->findByName(name);
        if (!key_column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "RadixHashJoin: right key column {} not found", name);
        const auto & type = key_column->type;
        if (type->isNullable() || type->lowCardinality() || !type->haveMaximumSizeOfValue())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "RadixHashJoin: unsupported key column {}", name);
        packed_key_width += type->getMaximumSizeOfValueInMemory();
    }
    if (!(packed_key_width % 4 == 0 && packed_key_width >= 4 && packed_key_width <= 64))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RadixHashJoin: packed key width {} out of range", packed_key_width);

    state->build_lanes = std::vector<State::BuildLane>(max_threads);
    state->schema_join = std::make_unique<HashJoin>(
        table_join, right_sample_block, /*any_take_last_row*/ false, /*reserve_num*/ 0, "radix_schema", /*use_two_level_maps*/ false);
    state->schema_join->setEnableLazyColumnsIndexing(state->enable_lazy_columns_indexing);

    (void)rhs_size_estimation;
    (void)size_tables_by_distinct_estimate;
}

RadixHashJoin::~RadixHashJoin()
{
    /// Split leaf probes parked by the delayed stream hold results referencing the leaf joins;
    /// release them before the joins are torn down. Whatever else the wave still holds (a poisoned
    /// query's arenas and admissions) is released by the State members' own destructors.
    state->wave_state.delayed_runs.clear();

    /// Hash-table destruction can be very time-consuming; parallelise it over the pool, matching
    /// ConcurrentHashJoin's teardown.
    if (!state->pool || state->partition_joins.empty())
        return;
    try
    {
        auto thread_group = CurrentThread::getGroup();
        std::atomic<size_t> next{0};
        const size_t n = state->partition_joins.size();
        parallelRun(
            *state->pool,
            max_threads,
            thread_group,
            [&](size_t)
            {
                for (size_t p = next.fetch_add(1, std::memory_order_relaxed); p < n; p = next.fetch_add(1, std::memory_order_relaxed))
                    state->partition_joins[p].reset();
            });
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        try
        {
            if (state->pool)
                state->pool->wait();
        }
        catch (...) /// wait() rethrows escaped job exceptions; nothing may escape a destructor
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

const TableJoin & RadixHashJoin::getTableJoin() const
{
    return *table_join;
}

bool RadixHashJoin::addBlockToJoin(const Block & block, bool check_limits)
{
    return addBlockToJoin(block, block.rows(), check_limits, 0);
}

bool RadixHashJoin::addBlockToJoin(const Block & block, size_t num_rows, bool check_limits)
{
    return addBlockToJoin(block, num_rows, check_limits, 0);
}

bool RadixHashJoin::addBlockToJoin(const Block & block, size_t num_rows, bool check_limits, size_t build_lane)
{
    if (num_rows == 0)
        return true;

    Block materialized = state->schema_join->materializeColumnsFromRightBlock(block);
    state->build_rows.fetch_add(num_rows, std::memory_order_relaxed);
    state->build_bytes.fetch_add(materialized.allocatedBytes(), std::memory_order_relaxed);

    auto & lane = state->build_lanes[build_lane % state->build_lanes.size()];
    {
        std::lock_guard lock(lane.mutex);
        lane.blocks.push_back(std::move(materialized));
    }

    if (check_limits && table_join->sizeLimits().hasLimits())
        return table_join->sizeLimits().check(
            state->build_rows.load(std::memory_order_relaxed),
            state->build_bytes.load(std::memory_order_relaxed),
            "JOIN",
            ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    return true;
}

void RadixHashJoin::onBuildPhaseFinish()
{
    /// The cheap build barrier only: concatenate the per-lane block stores. The heavy scatter runs in
    /// runPostBuildPhase.
    size_t total = 0;
    for (auto & lane : state->build_lanes)
        total += lane.blocks.size();
    state->build_blocks.reserve(total);
    for (auto & lane : state->build_lanes)
    {
        for (auto & block : lane.blocks)
            state->build_blocks.push_back(std::move(block));
        lane.blocks.clear();
        lane.blocks.shrink_to_fit();
    }
}

void RadixHashJoin::runPostBuildPhase()
{
    Stopwatch build_watch;

    const size_t build_rows = state->build_rows.load(std::memory_order_relaxed);
    const size_t build_bytes = state->build_bytes.load(std::memory_order_relaxed);

    state->pool = std::make_unique<ThreadPool>(
        CurrentMetrics::RadixHashJoinPoolThreads,
        CurrentMetrics::RadixHashJoinPoolThreadsActive,
        CurrentMetrics::RadixHashJoinPoolThreadsScheduled,
        /*max_threads*/ max_threads,
        /*max_free_threads*/ max_threads,
        /*queue_size*/ 0);

    if (build_rows == 0 || state->build_blocks.empty())
    {
        state->post_build_done.store(true, std::memory_order_release);
        return;
    }

    /// 5.1 Partition plan. The leaf working set is the reserved hash table plus the stored build rows;
    /// pick the smallest power-of-two fanout that keeps it within an L2-sized budget (the benchmark
    /// bandwidth model's "HT + build within L2" criterion). build_rows is exact here.
    auto ht_bytes = [](size_t n) { return std::bit_ceil(std::max<size_t>(2 * n, 1)) * HT_CELL_BYTES; };
    auto leaf_bytes = [&](size_t p) { return ht_bytes(build_rows / p) + build_bytes / p; };

    constexpr size_t max_route_partitions = UInt64{1} << 32;
    const size_t lower = std::min<size_t>(std::max<size_t>(2, std::bit_ceil(max_threads)), max_route_partitions);
    size_t fanout = lower;
    while (fanout < max_route_partitions && leaf_bytes(fanout) > LEAF_TARGET_BYTES)
        fanout <<= 1;

    const size_t max_pass_fanout = std::min<size_t>(MAX_FANOUT_PER_PASS, std::bit_floor(std::max<size_t>(2, max_partitions_per_pass)));
    state->fanout = fanout;
    state->pass_bits = computePassBits(fanout, max_pass_fanout);

    /// Probe-buffer budget from the settings knobs, computed once against the built size below.
    Block build_header = state->build_blocks.front().cloneEmpty();
    SideLayout build_layout = makeSideLayout(build_header, state->right_key_names);

    auto thread_group = CurrentThread::getGroup();
    std::vector<PartitionOutput> parts
        = scatterToPartitions(*state->pool, max_threads, thread_group, build_header, state->build_blocks, build_layout, state->pass_bits);

    /// Release the (now-empty) build block shells.
    state->build_blocks.clear();
    state->build_blocks.shrink_to_fit();

    /// 5.5 Leaf builds — one exactly-reserved HashJoin per non-empty partition, built work-stealing.
    state->partition_joins.resize(fanout);
    std::atomic<size_t> next_partition{0};
    std::atomic<size_t> leaves_built{0};
    Stopwatch leaf_watch;
    parallelRun(
        *state->pool,
        max_threads,
        thread_group,
        [&](size_t)
        {
            size_t local_leaves = 0;
            for (size_t p = next_partition.fetch_add(1, std::memory_order_relaxed); p < fanout;
                 p = next_partition.fetch_add(1, std::memory_order_relaxed))
            {
                if (!parts[p].rows)
                    continue;
                auto join = std::make_unique<HashJoin>(
                    table_join,
                    right_sample_block,
                    /*any_take_last_row*/ false,
                    /*reserve_num*/ parts[p].rows,
                    fmt::format("radix{}", p),
                    /*use_two_level_maps*/ false);
                join->setMaxJoinedBlockRows(table_join->maxJoinedBlockRows());
                join->setMaxJoinedBlockBytes(table_join->maxJoinedBlockBytes());
                join->setEnableLazyColumnsIndexing(state->enable_lazy_columns_indexing);
                join->addBlockToJoin(parts[p].toBlock(build_header), /*check_limits*/ false);
                join->onBuildPhaseFinish();
                state->partition_joins[p] = std::move(join);
                ++local_leaves;
            }
            leaves_built.fetch_add(local_leaves, std::memory_order_relaxed);
        });
    ProfileEvents::increment(ProfileEvents::RadixHashJoinLeafGroupBuilds, leaves_built.load(std::memory_order_relaxed));
    ProfileEvents::increment(ProfileEvents::RadixHashJoinLeafGroupBuildMicroseconds, leaf_watch.elapsedMicroseconds());

    size_t post_build_bytes = 0;
    for (const auto & join : state->partition_joins)
        if (join)
            post_build_bytes += join->getTotalByteCount();
    state->post_build_bytes = post_build_bytes;

    double budget = probe_buffer_fraction * static_cast<double>(post_build_bytes);
    size_t window_budget = static_cast<size_t>(budget);
    window_budget = std::max(window_budget, static_cast<size_t>(probe_buffer_min_bytes));
    if (probe_buffer_max_bytes != 0)
        window_budget = std::min(window_budget, static_cast<size_t>(probe_buffer_max_bytes));
    state->probe_window_budget = std::max<size_t>(window_budget, 1);

    state->post_build_done.store(true, std::memory_order_release);

    ProfileEvents::increment(ProfileEvents::RadixHashJoinBuildMicroseconds, build_watch.elapsedMicroseconds());
    LOG_DEBUG(
        getLogger("RadixHashJoin"),
        "Built {} leaf partitions in {} radix pass(es) from {} rows ({}), probe window budget {}, in {} ms",
        fanout,
        state->pass_bits.size(),
        build_rows,
        ReadableSize(post_build_bytes),
        ReadableSize(state->probe_window_budget),
        build_watch.elapsedMilliseconds());
}

void RadixHashJoin::checkTypesOfKeys(const Block & block) const
{
    state->schema_join->checkTypesOfKeys(block);
}

void RadixHashJoin::setTotals(const Block & block)
{
    std::lock_guard lock(totals_mutex);
    IJoin::setTotals(block);
}

JoinResultPtr RadixHashJoin::joinBlock(Block block)
{
    return joinBlock(std::move(block), 0);
}

JoinResultPtr RadixHashJoin::joinBlock(Block block, size_t /*lane*/)
{
    /// Header/planning path (before the build barrier): delegate to the schema-only HashJoin, which
    /// produces the correct output header.
    if (!state->post_build_done.load(std::memory_order_acquire))
        return state->schema_join->joinBlock(std::move(block));

    if (block.rows() == 0 || state->build_rows.load(std::memory_order_relaxed) == 0)
        return state->schema_join->joinBlock(std::move(block));

    /// materializeColumnsFromLeftBlock is a no-op for INNER joins, but the scatter reads getRawData(),
    /// so normalize any Const/Sparse/LowCardinality wrappers to full fixed-width columns.
    {
        Columns columns = block.getColumns();
        bool changed = false;
        for (auto & column : columns)
        {
            /// Master renamed `convertToFullIfNeeded` to `convertToFullIfWrapped` (no LowCardinality).
            /// Probe scatter still needs LC unwrapped to a full fixed-width column.
            auto full = column->convertToFullIfWrapped()->convertToFullColumnIfLowCardinality();
            if (full.get() != column.get())
            {
                column = std::move(full);
                changed = true;
            }
        }
        if (changed)
            block.setColumns(columns);
    }

    {
        std::lock_guard lock(state->wave_state.mutex);
        if (!state->left_ready)
        {
            state->left_header = block.cloneEmpty();
            state->left_layout = makeSideLayout(state->left_header, state->left_key_names);
            state->bindWave(*table_join, max_threads);
            state->left_ready = true;
        }
    }

    /// The block travels inside the result: its first next() admits it (possibly sealing the wave
    /// and beginning the drain) or, when the wave is already sealed, first helps drain it. The
    /// admission itself bounds the wave's accounted bytes; nothing is buffered per lane.
    return std::make_unique<CooperativeWaveResult>(state->wave_state, std::move(block));
}

IBlocksStreamPtr RadixHashJoin::getDelayedBlocks()
{
    auto & wave = state->wave_state;
    {
        std::lock_guard lock(wave.mutex);
        if (wave.delayed_taken)
            return {};
        wave.delayed_taken = true;
        if (!state->left_ready || state->build_rows.load(std::memory_order_relaxed) == 0)
            return {};
        /// Every probe transform reached its result's end before the delayed flush starts, so the
        /// wave can only be filling (the final partial window) — or already poisoned, which the
        /// stream's first pull will surface. The stream itself seals and drains it: same machine.
        if (wave.admitted.empty() && !wave.primary)
            return {};
    }
    return std::make_shared<CooperativeDelayedBlocks>(wave);
}

size_t RadixHashJoin::getTotalRowCount() const
{
    return state->build_rows.load(std::memory_order_relaxed);
}

size_t RadixHashJoin::getTotalByteCount() const
{
    if (state->post_build_done.load(std::memory_order_acquire))
        return state->post_build_bytes;
    return state->build_bytes.load(std::memory_order_relaxed);
}

bool RadixHashJoin::alwaysReturnsEmptySet() const
{
    return state->post_build_done.load(std::memory_order_acquire) && state->build_rows.load(std::memory_order_relaxed) == 0;
}

void RadixHashJoin::setEnableLazyColumnsIndexing(bool value)
{
    state->enable_lazy_columns_indexing = value;
    if (state->schema_join)
        state->schema_join->setEnableLazyColumnsIndexing(value);
    for (auto & join : state->partition_joins)
        if (join)
            join->setEnableLazyColumnsIndexing(value);
}

IBlocksStreamPtr RadixHashJoin::getNonJoinedBlocks(
    const Block & /*left_sample_block*/, const Block & /*result_sample_block*/, UInt64 /*max_block_size*/) const
{
    /// Inner join only: no non-joined right rows.
    return {};
}

}
