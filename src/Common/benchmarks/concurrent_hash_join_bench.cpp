#include "concurrent_hash_join_bench.h"

#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Interpreters/TableJoin.h>

namespace DB::JoinBench
{

ConcurrentHashJoinBench::ConcurrentHashJoinBench(WorkerPool & pool_, const Block & left_header, const Block & right_header, UInt64 stats_key)
    : pool(pool_)
    , table_join(makeTableJoin(left_header, right_header))
    , join(std::make_shared<ConcurrentHashJoin>(
          table_join, pool_.size(), std::make_shared<const Block>(right_header),
          /// Limits mirror the query defaults `max_entries_for_hash_table_stats` and
          /// `max_size_to_preallocate_for_joins`.
          StatsCollectingParams(stats_key, stats_key != 0, /*max_entries_for_hash_table_stats*/ 10'000, /*max_size_to_preallocate*/ 1'000'000'000'000ULL)))
{
}

ConcurrentHashJoinBench::~ConcurrentHashJoinBench() = default;

void ConcurrentHashJoinBench::build(const std::vector<Block> & blocks)
{
    const size_t threads = pool.size();
    pool.run([&](size_t tid)
    {
        for (size_t b = tid; b < blocks.size(); b += threads)
            join->addBlockToJoin(blocks[b], /*check_limits*/ false);
    });
    join->onBuildPhaseFinish();
}

size_t ConcurrentHashJoinBench::probe(const std::vector<Block> & blocks, UInt64 * fingerprint)
{
    const size_t threads = pool.size();
    std::atomic<size_t> rows{0};
    std::atomic<UInt64> digest{0};
    pool.run([&](size_t tid)
    {
        size_t local_rows = 0;
        UInt64 local_digest = 0;
        for (size_t b = tid; b < blocks.size(); b += threads)
            local_rows += drainJoinResult(join->joinBlock(blocks[b]), fingerprint ? &local_digest : nullptr);
        g_sink += local_rows;
        rows += local_rows;
        digest += local_digest;
    });
    if (fingerprint)
        *fingerprint += digest;
    return rows;
}

void ConcurrentHashJoinBench::teardown()
{
    /// The `ConcurrentHashJoin` destructor already records the size-hint statistics
    /// (`updateStatistics`, `ConcurrentHashJoin.cpp:253`) and then parallelizes hash-table
    /// teardown across the pool (`:255-280`). Teardown must run before the next same-key bench
    /// is constructed for the stats-warm pattern to work; the driver's
    /// build-probe-teardown-then-next-instance ordering already guarantees this.
    join.reset();
}

}
