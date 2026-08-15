#include "radix_hash_join_bench.h"

#include <Interpreters/HashTablesStatistics.h>
#include <Interpreters/RadixHashJoin/RadixHashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

#include <atomic>
#include <limits>

namespace ProfileEvents
{
extern const Event RadixHashJoinProbePackHashRouteMicroseconds;
}

namespace DB::JoinBench
{

namespace
{
double eventSeconds(ProfileEvents::Event e)
{
    return static_cast<double>(ProfileEvents::global_counters[e]) * 1e-6;
}
}

RadixHashJoinBench::RadixHashJoinBench(
    WorkerPool & pool_, const Block & left_header_, const Block & right_header_, size_t f_max_)
    : pool(pool_)
    , left_header(left_header_)
    , right_header(std::make_shared<const Block>(right_header_))
    , table_join(makeTableJoin(left_header_, right_header_))
    , f_max(f_max_)
{
}

RadixHashJoinBench::~RadixHashJoinBench() = default;

std::unique_ptr<RadixHashJoin> RadixHashJoinBench::makeJoin(UInt64 probe_min_bytes, UInt64 probe_max_bytes)
{
    /// Production derives the total leaf count from its L2 criterion and uses the measured `f_max`
    /// only as the per-pass fanout cap. The probe-buffer budget is set in absolute bytes (fraction 0),
    /// so the caller controls the wave count directly.
    return std::make_unique<RadixHashJoin>(
        table_join,
        right_header,
        /*max_threads*/ pool.size(),
        /*rhs_size_estimation*/ std::nullopt,
        /*max_partitions_per_pass*/ std::max<UInt64>(2, f_max),
        /*size_tables_by_distinct_estimate*/ false,
        /*probe_buffer_fraction*/ 0.0,
        /*probe_buffer_min_bytes*/ probe_min_bytes,
        /*probe_buffer_max_bytes*/ probe_max_bytes,
        StatsCollectingParams{});
}

void RadixHashJoinBench::buildJoin(RadixHashJoin & join_, const std::vector<Block> & blocks)
{
    const size_t threads = pool.size();
    /// Concurrent per-lane accumulation, exactly like the parallel build transform.
    pool.run(
        [&](size_t tid)
        {
            for (size_t b = tid; b < blocks.size(); b += threads)
                join_.addBlockToJoin(blocks[b], blocks[b].rows(), /*check_limits*/ false, tid);
        });
    /// The cheap build barrier, then the heavy parallel post-build (radix scatter + leaf tables).
    join_.onBuildPhaseFinish();
    join_.runPostBuildPhase();
}

size_t RadixHashJoinBench::driveProbe(RadixHashJoin & join_, const std::vector<Block> & blocks, UInt64 * fingerprint)
{
    const size_t threads = pool.size();
    std::atomic<size_t> rows{0};
    std::atomic<UInt64> digest{0};

    /// Buffer the probe blocks (and drain any mid-stream wave a joinBlock triggers), concurrently
    /// across the pool — the executor's per-stream joinBlock calls.
    pool.run(
        [&](size_t tid)
        {
            size_t local_rows = 0;
            UInt64 local_digest = 0;
            for (size_t b = tid; b < blocks.size(); b += threads)
                local_rows += drainJoinResult(join_.joinBlock(Block(blocks[b]), tid), fingerprint ? &local_digest : nullptr);
            g_sink += local_rows;
            rows += local_rows;
            digest += local_digest;
        });

    /// Flush the final buffered window through the delayed-blocks stream, drained across all workers
    /// exactly as the DelayedJoinedBlocksWorkerTransforms do.
    if (auto stream = join_.getDelayedBlocks())
    {
        pool.run(
            [&](size_t /*tid*/)
            {
                size_t local_rows = 0;
                UInt64 local_digest = 0;
                while (true)
                {
                    Block block = stream->next();
                    if (block.empty())
                        break;
                    local_rows += block.rows();
                    if (fingerprint)
                        local_digest += blockFingerprint(block);
                }
                g_sink += local_rows;
                rows += local_rows;
                digest += local_digest;
            });
    }

    if (fingerprint)
        *fingerprint += digest;
    return rows;
}

void RadixHashJoinBench::build(const std::vector<Block> & blocks)
{
    build_blocks = &blocks;
    /// Single-wave default: an enormous floor so the whole probe buffers and flushes as one wave
    /// through the delayed-blocks path (the benchmark's canonical `waves = 1` shape).
    join = makeJoin(/*min*/ std::numeric_limits<UInt64>::max() / 2, /*max*/ 0);
    Stopwatch build_watch;
    buildJoin(*join, blocks);
    build_sec = build_watch.elapsedSeconds();
}

size_t RadixHashJoinBench::probe(const std::vector<Block> & blocks, UInt64 * fingerprint)
{
    const double scatter_before = eventSeconds(ProfileEvents::RadixHashJoinProbePackHashRouteMicroseconds);
    Stopwatch probe_watch;
    const size_t matches = driveProbe(*join, blocks, fingerprint);
    const double total = probe_watch.elapsedSeconds();
    probe_scatter_sec = eventSeconds(ProfileEvents::RadixHashJoinProbePackHashRouteMicroseconds) - scatter_before;
    probe_join_sec = std::max(0.0, total - probe_scatter_sec);
    return matches;
}

size_t RadixHashJoinBench::probeWaves(const std::vector<Block> & blocks, size_t waves, UInt64 * fingerprint)
{
    size_t probe_bytes = 0;
    for (const auto & block : blocks)
        probe_bytes += block.allocatedBytes();

    /// Pin the probe-buffer budget so the production join produces exactly `waves` windows.
    const UInt64 budget = waves ? std::max<UInt64>(1, probe_bytes / waves) : std::numeric_limits<UInt64>::max() / 2;

    /// The probe path is one-shot (window + delayed state), so rebuild a fresh join per call.
    auto wave_join = makeJoin(/*min*/ budget, /*max*/ budget);
    buildJoin(*wave_join, *build_blocks);

    const double scatter_before = eventSeconds(ProfileEvents::RadixHashJoinProbePackHashRouteMicroseconds);
    Stopwatch probe_watch;
    const size_t matches = driveProbe(*wave_join, blocks, fingerprint);
    const double total = probe_watch.elapsedSeconds();
    probe_scatter_sec = eventSeconds(ProfileEvents::RadixHashJoinProbePackHashRouteMicroseconds) - scatter_before;
    probe_join_sec = std::max(0.0, total - probe_scatter_sec);
    return matches;
}

void RadixHashJoinBench::teardown()
{
    join.reset();
}

std::string RadixHashJoinBench::phaseBreakdown() const
{
    return fmt::format("build {:.2f} ms, probe scatter {:.2f} ms", build_sec * 1e3, probe_scatter_sec * 1e3);
}

}
