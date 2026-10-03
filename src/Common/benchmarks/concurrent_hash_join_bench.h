#pragma once

#include "hash_join_bench.h"

namespace DB
{
class ConcurrentHashJoin;
}

namespace DB::JoinBench
{

/// Non-partitioned hash join: the real ClickHouse `ConcurrentHashJoin` (`parallel_hash`),
/// used as-is through the `IJoin` interface: concurrent `addBlockToJoin` into per-slot
/// two-level maps, constant-time bucket merge in `onBuildPhaseFinish`, unpartitioned
/// shared-map probe via `joinBlock`.
class ConcurrentHashJoinBench : public IJoinBench
{
public:
    /// With a non-zero `stats_key`, hash table size statistics are collected into the
    /// process-global cache on destruction and used to preallocate the maps of subsequent
    /// instances built with the same key (the steady state of repeated real queries, driven
    /// by `collect_hash_table_stats_during_joins`). The first build with a given key is cold.
    ConcurrentHashJoinBench(WorkerPool & pool_, const Block & left_header, const Block & right_header, UInt64 stats_key = 0);
    ~ConcurrentHashJoinBench() override;

    std::string name() const override { return "ConcurrentHashJoin"; }
    void build(const std::vector<Block> & blocks) override;
    size_t probe(const std::vector<Block> & blocks, UInt64 * fingerprint) override;
    void teardown() override;

private:
    WorkerPool & pool;
    std::shared_ptr<TableJoin> table_join;
    std::shared_ptr<ConcurrentHashJoin> join;
};

}
