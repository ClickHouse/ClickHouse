#pragma once

#include <memory>
#include <Analyzer/IQueryTreeNode.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashTablesStatistics.h>
#include <Interpreters/IJoin.h>
#include <base/defines.h>
#include <base/types.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool_fwd.h>

namespace DB
{

struct SelectQueryInfo;

/**
 * The default `HashJoin` is not thread-safe for inserting the right table's rows; thus, it is done on a single thread.
 * When the right table is large, the join process is too slow.
 *
 * `ConcurrentHashJoin` can run `addBlockToJoin()` concurrently to speed up the join process. On the test, it scales almost linearly.
 * For that, we create multiple `HashJoin` instances. In `addBlockToJoin()`, one input block is split into multiple blocks
 * corresponding to the `HashJoin` instances by hashing every row on the join keys. In particular, each `HashJoin` instance has its own hash map
 * that stores a unique set of keys. Also, `addBlockToJoin()` calls are done under mutex to guarantee
 * that every `HashJoin` instance is written only from one thread at a time.
 *
 * When matching the left table, the input blocks are also split by hash and routed to corresponding `HashJoin` instances.
 * This introduces some noticeable overhead compared to the `hash` join algorithm that doesn't have to split. Then,
 * we introduced the following optimization. On the probe stage, we want to have the same execution as for the `hash` join algorithm,
 * i.e., we want to have a single shared hash map that we will read from each thread. No splitting of blocks is required.
 * We should somehow divide this shared hash map between threads so that we can still execute the build stage concurrently.
 * The idea is to use a two-level hash map and distribute its buckets between threads. Namely, we will calculate the same hash
 * that the hash map calculates, map it to the bucket number, and then take this number modulo the number of threads. This way,
 * upon build phase completion, we will have thread #0 having a hash map with only buckets {#0, #threads_num, #threads_num*2, ...},
 * thread #1 with only buckets {#1, #threads_num+1, #threads_num*2+1, ...} and so on. To form the resulting hash map,
 * we will merge all these sub-maps in the method `onBuildPhaseFinish`. Please note that this merge could be done in constant time because,
 * for each bucket, only one `HashJoin` instance has it non-empty.
 */
class ConcurrentHashJoin : public IJoin
{

public:
    explicit ConcurrentHashJoin(
        std::shared_ptr<TableJoin> table_join_,
        size_t slots_,
        const Block & right_sample_block,
        const StatsCollectingParams & stats_collecting_params_,
        bool any_take_last_row_ = false);

    ~ConcurrentHashJoin() override;

    std::string getName() const override { return "ConcurrentHashJoin"; }
    const TableJoin & getTableJoin() const override { return *table_join; }
    bool addBlockToJoin(const Block & right_block_, bool check_limits) override;
    void checkTypesOfKeys(const Block & block) const override;
    void joinBlock(Block & block, std::shared_ptr<ExtraBlock> & not_processed) override;
    void setTotals(const Block & block) override;
    const Block & getTotals() const override;
    size_t getTotalRowCount() const override;
    size_t getTotalByteCount() const override;
    bool alwaysReturnsEmptySet() const override;
    bool supportParallelJoin() const override { return true; }

    bool isScatteredJoin() const override { return true; }
    void joinBlock(Block & block, ExtraScatteredBlocks & extra_blocks, std::vector<Block> & res) override;

    IBlocksStreamPtr
    getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;

    bool isCloneSupported() const override
    {
        return !getTotals() && getTotalRowCount() == 0;
    }

    std::shared_ptr<IJoin> clone(const std::shared_ptr<TableJoin> & table_join_, const Block &, const Block & right_sample_block_) const override
    {
        return std::make_shared<ConcurrentHashJoin>(table_join_, slots, right_sample_block_, stats_collecting_params);
    }

    std::shared_ptr<IJoin> cloneNoParallel(const std::shared_ptr<TableJoin> & table_join_, const Block &, const Block & right_sample_block_) const override
    {
        return std::make_shared<HashJoin>(table_join_, right_sample_block_, any_take_last_row);
    }

    void onBuildPhaseFinish() override;

    struct InternalHashJoin
    {
        std::mutex mutex;
        std::unique_ptr<HashJoin> data;
        bool space_was_preallocated = false;
    };

private:
    std::shared_ptr<TableJoin> table_join;
    size_t slots;
    bool any_take_last_row;
    std::unique_ptr<ThreadPool> pool;
    std::vector<std::shared_ptr<InternalHashJoin>> hash_joins;

    StatsCollectingParams stats_collecting_params;

    std::mutex totals_mutex;
    Block totals;

    ScatteredBlocks dispatchBlock(const Strings & key_columns_names, Block && from_block);
};

// The following two methods are deprecated and hopefully will be removed in the future.
IQueryTreeNode::HashState preCalculateCacheKey(const QueryTreeNodePtr & right_table_expression, const SelectQueryInfo & select_query_info);
UInt64 calculateCacheKey(std::shared_ptr<TableJoin> & table_join, IQueryTreeNode::HashState hash);
}
