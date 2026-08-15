#pragma once

#include "hash_join_bench.h"

#include <memory>

namespace DB
{
class RadixHashJoin;
}

namespace DB::JoinBench
{

/// Radix-partitioned hash join: the real ClickHouse `RadixHashJoin` (`join_algorithm = 'radix_join'`),
/// used as-is through the `IJoin` interface — exactly as `ConcurrentHashJoinBench` wraps
/// `ConcurrentHashJoin`. The build side is radix-scattered into per-leaf `HashJoin` tables
/// (`addBlockToJoin` per lane, then `onBuildPhaseFinish` + `runPostBuildPhase`), and the probe side
/// is buffered by `joinBlock` and flushed as budgeted waves — the final window through the standard
/// delayed-blocks path (`getDelayedBlocks`), drained across the pool like the executor's
/// delayed-worker transforms.
///
/// The driver constructs the join with a probe-buffer budget that makes the whole probe one wave
/// (the benchmark's canonical `waves = 1` shape); `probeWaves` reconstructs the join with a budget
/// pinned to `probe_bytes / waves` to force a specific wave count for the BEP sweep.
class RadixHashJoinBench : public IJoinBench
{
public:
    /// Production derives `p_star` from the actual build side and uses `f_max` as the per-pass
    /// fanout cap, matching the model's pass plan.
    RadixHashJoinBench(WorkerPool & pool_, const Block & left_header_, const Block & right_header_, size_t f_max_);
    ~RadixHashJoinBench() override;

    std::string name() const override { return "RadixHashJoin"; }
    void build(const std::vector<Block> & blocks) override;
    size_t probe(const std::vector<Block> & blocks, UInt64 * fingerprint) override;
    std::string phaseBreakdown() const override;
    void teardown() override;

    /// BEP probe-budget emulation: force `waves` windows by pinning the production probe-buffer budget
    /// to `probe_bytes / waves`. Each call reconstructs and rebuilds a fresh production join (the probe
    /// path is one-shot: window + delayed-flush state). `waves == 1` is the plain single-wave probe.
    size_t probeWaves(const std::vector<Block> & blocks, size_t waves, UInt64 * fingerprint);

    double probeScatterSec() const { return probe_scatter_sec; }
    double probeJoinSec() const { return probe_join_sec; }

private:
    std::unique_ptr<RadixHashJoin> makeJoin(UInt64 probe_min_bytes, UInt64 probe_max_bytes);
    void buildJoin(RadixHashJoin & join_, const std::vector<Block> & blocks);
    size_t driveProbe(RadixHashJoin & join_, const std::vector<Block> & blocks, UInt64 * fingerprint);

    WorkerPool & pool;
    Block left_header;
    SharedHeader right_header;
    std::shared_ptr<TableJoin> table_join;
    size_t f_max;

    std::unique_ptr<RadixHashJoin> join;
    const std::vector<Block> * build_blocks = nullptr;

    double build_sec = 0;
    double probe_scatter_sec = 0;
    double probe_join_sec = 0;
};

}
