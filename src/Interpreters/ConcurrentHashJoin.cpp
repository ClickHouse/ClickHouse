#include <Columns/ColumnSparse.h>
#include <Columns/FilterDescription.h>
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/createBlockSelector.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/parseQuery.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>
#include <Common/WeakHash.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>

namespace ProfileEvents
{
extern const Event HashJoinPreallocatedElementsInHashTables;
}

namespace CurrentMetrics
{
extern const Metric ConcurrentHashJoinPoolThreads;
extern const Metric ConcurrentHashJoinPoolThreadsActive;
extern const Metric ConcurrentHashJoinPoolThreadsScheduled;
}

namespace
{

void updateStatistics(const auto & hash_joins, const DB::StatsCollectingParams & params)
{
    if (!params.isCollectionAndUseEnabled())
        return;

    std::vector<size_t> sizes(hash_joins.size());
    for (size_t i = 0; i < hash_joins.size(); ++i)
        sizes[i] = hash_joins[i]->data->getTotalRowCount();
    const auto median_size = sizes.begin() + sizes.size() / 2; // not precisely though...
    std::nth_element(sizes.begin(), median_size, sizes.end());
    if (auto sum_of_sizes = std::accumulate(sizes.begin(), sizes.end(), 0ull))
        DB::getHashTablesStatistics().update(sum_of_sizes, *median_size, params);
}

}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

static UInt32 toPowerOfTwo(UInt32 x)
{
    if (x <= 1)
        return 1;
    return static_cast<UInt32>(1) << (32 - std::countl_zero(x - 1));
}

ConcurrentHashJoin::ConcurrentHashJoin(
    ContextPtr context_,
    std::shared_ptr<TableJoin> table_join_,
    size_t slots_,
    const Block & right_sample_block,
    const StatsCollectingParams & stats_collecting_params_,
    bool any_take_last_row_)
    : context(context_)
    , table_join(table_join_)
    , slots(toPowerOfTwo(std::min<UInt32>(static_cast<UInt32>(slots_), 256)))
    , pool(std::make_unique<ThreadPool>(
          CurrentMetrics::ConcurrentHashJoinPoolThreads,
          CurrentMetrics::ConcurrentHashJoinPoolThreadsActive,
          CurrentMetrics::ConcurrentHashJoinPoolThreadsScheduled,
          /*max_threads_*/ slots,
          /*max_free_threads_*/ 0,
          /*queue_size_*/ slots))
    , stats_collecting_params(stats_collecting_params_)
{
    hash_joins.resize(slots);

    try
    {
        for (size_t i = 0; i < slots; ++i)
        {
            pool->scheduleOrThrow(
                [&, idx = i, thread_group = CurrentThread::getGroup()]()
                {
                    SCOPE_EXIT_SAFE({
                        if (thread_group)
                            CurrentThread::detachFromGroupIfNotDetached();
                    });

                    if (thread_group)
                        CurrentThread::attachToGroupIfDetached(thread_group);
                    setThreadName("ConcurrentJoin");

                    size_t reserve_size = 0;
                    if (auto hint = getSizeHint(stats_collecting_params, slots))
                        reserve_size = hint->median_size;
                    ProfileEvents::increment(ProfileEvents::HashJoinPreallocatedElementsInHashTables, reserve_size);

                    auto inner_hash_join = std::make_shared<InternalHashJoin>();
                    inner_hash_join->data = std::make_unique<HashJoin>(
                        table_join_, right_sample_block, any_take_last_row_, reserve_size, fmt::format("concurrent{}", idx));
                    /// Non zero `max_joined_block_rows` allows to process block partially and return not processed part.
                    /// TODO: It's not handled properly in ConcurrentHashJoin case, so we set it to 0 to disable this feature.
                    inner_hash_join->data->setMaxJoinedBlockRows(0);
                    hash_joins[idx] = std::move(inner_hash_join);
                });
        }
        pool->wait();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        pool->wait();
        throw;
    }
}

ConcurrentHashJoin::~ConcurrentHashJoin()
{
    try
    {
        updateStatistics(hash_joins, stats_collecting_params);

        for (size_t i = 0; i < slots; ++i)
        {
            // Hash tables destruction may be very time-consuming.
            // Without the following code, they would be destroyed in the current thread (i.e. sequentially).
            // `InternalHashJoin` is moved here and will be destroyed in the destructor of the lambda function.
            pool->scheduleOrThrow(
                [join = std::move(hash_joins[i]), thread_group = CurrentThread::getGroup()]()
                {
                    SCOPE_EXIT_SAFE({
                        if (thread_group)
                            CurrentThread::detachFromGroupIfNotDetached();
                    });

                    if (thread_group)
                        CurrentThread::attachToGroupIfDetached(thread_group);
                    setThreadName("ConcurrentJoin");
                });
        }
        pool->wait();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        pool->wait();
    }
}

bool ConcurrentHashJoin::addBlockToJoin(const Block & right_block, bool check_limits)
{
    Blocks dispatched_blocks = dispatchBlock(table_join->getOnlyClause().key_names_right, right_block);

    size_t blocks_left = 0;
    for (const auto & block : dispatched_blocks)
    {
        if (block)
        {
            ++blocks_left;
        }
    }

    while (blocks_left > 0)
    {
        /// insert blocks into corresponding HashJoin instances
        for (size_t i = 0; i < dispatched_blocks.size(); ++i)
        {
            auto & hash_join = hash_joins[i];
            auto & dispatched_block = dispatched_blocks[i];

            if (dispatched_block)
            {
                /// if current hash_join is already processed by another thread, skip it and try later
                std::unique_lock<std::mutex> lock(hash_join->mutex, std::try_to_lock);
                if (!lock.owns_lock())
                    continue;

                bool limit_exceeded = !hash_join->data->addBlockToJoin(dispatched_block, check_limits);

                dispatched_block = {};
                blocks_left--;

                if (limit_exceeded)
                    return false;
            }
        }
    }

    if (check_limits)
        return table_join->sizeLimits().check(getTotalRowCount(), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    return true;
}

void ConcurrentHashJoin::joinBlock(Block & block, std::shared_ptr<ExtraBlock> & /*not_processed*/)
{
    Blocks dispatched_blocks = dispatchBlock(table_join->getOnlyClause().key_names_left, block);
    block = {};
    for (size_t i = 0; i < dispatched_blocks.size(); ++i)
    {
        std::shared_ptr<ExtraBlock> none_extra_block;
        auto & hash_join = hash_joins[i];
        auto & dispatched_block = dispatched_blocks[i];
        hash_join->data->joinBlock(dispatched_block, none_extra_block);
        if (none_extra_block && !none_extra_block->empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "not_processed should be empty");
    }

    block = concatenateBlocks(dispatched_blocks);
}

void ConcurrentHashJoin::checkTypesOfKeys(const Block & block) const
{
    hash_joins[0]->data->checkTypesOfKeys(block);
}

void ConcurrentHashJoin::setTotals(const Block & block)
{
    if (block)
    {
        std::lock_guard lock(totals_mutex);
        totals = block;
    }
}

const Block & ConcurrentHashJoin::getTotals() const
{
    return totals;
}

size_t ConcurrentHashJoin::getTotalRowCount() const
{
    size_t res = 0;
    for (const auto & hash_join : hash_joins)
    {
        std::lock_guard lock(hash_join->mutex);
        res += hash_join->data->getTotalRowCount();
    }
    return res;
}

size_t ConcurrentHashJoin::getTotalByteCount() const
{
    size_t res = 0;
    for (const auto & hash_join : hash_joins)
    {
        std::lock_guard lock(hash_join->mutex);
        res += hash_join->data->getTotalByteCount();
    }
    return res;
}

bool ConcurrentHashJoin::alwaysReturnsEmptySet() const
{
    for (const auto & hash_join : hash_joins)
    {
        std::lock_guard lock(hash_join->mutex);
        if (!hash_join->data->alwaysReturnsEmptySet())
            return false;
    }
    return true;
}

IBlocksStreamPtr ConcurrentHashJoin::getNonJoinedBlocks(
        const Block & /*left_sample_block*/, const Block & /*result_sample_block*/, UInt64 /*max_block_size*/) const
{
    if (!JoinCommon::hasNonJoinedBlocks(*table_join))
        return {};

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid join type. join kind: {}, strictness: {}",
                    table_join->kind(), table_join->strictness());
}

static ALWAYS_INLINE IColumn::Selector hashToSelector(const WeakHash32 & hash, size_t num_shards)
{
    assert(num_shards > 0 && (num_shards & (num_shards - 1)) == 0);
    const auto & data = hash.getData();
    size_t num_rows = data.size();

    IColumn::Selector selector(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
        /// Apply intHash64 to mix bits in data.
        /// HashTable internally uses WeakHash32, and we need to get different lower bits not to cause collisions.
        selector[i] = intHash64(data[i]) & (num_shards - 1);
    return selector;
}

IColumn::Selector ConcurrentHashJoin::selectDispatchBlock(const Strings & key_columns_names, const Block & from_block)
{
    size_t num_rows = from_block.rows();
    size_t num_shards = hash_joins.size();

    WeakHash32 hash(num_rows);
    for (const auto & key_name : key_columns_names)
    {
        const auto & key_col = from_block.getByName(key_name).column->convertToFullColumnIfConst();
        const auto & key_col_no_lc = recursiveRemoveLowCardinality(recursiveRemoveSparse(key_col));
        hash.update(key_col_no_lc->getWeakHash32());
    }
    return hashToSelector(hash, num_shards);
}

Blocks ConcurrentHashJoin::dispatchBlock(const Strings & key_columns_names, const Block & from_block)
{
    /// TODO: use JoinCommon::scatterBlockByHash
    size_t num_shards = hash_joins.size();
    size_t num_cols = from_block.columns();

    IColumn::Selector selector = selectDispatchBlock(key_columns_names, from_block);

    Blocks result(num_shards);
    for (size_t i = 0; i < num_shards; ++i)
        result[i] = from_block.cloneEmpty();

    for (size_t i = 0; i < num_cols; ++i)
    {
        auto dispatched_columns = from_block.getByPosition(i).column->scatter(num_shards, selector);
        assert(result.size() == dispatched_columns.size());
        for (size_t block_index = 0; block_index < num_shards; ++block_index)
        {
            result[block_index].getByPosition(i).column = std::move(dispatched_columns[block_index]);
        }
    }
    return result;
}

UInt64 calculateCacheKey(std::shared_ptr<TableJoin> & table_join, const QueryTreeNodePtr & right_table_expression)
{
    IQueryTreeNode::HashState hash;
    chassert(right_table_expression);
    hash.update(right_table_expression->getTreeHash());
    chassert(table_join && table_join->oneDisjunct());
    const auto keys
        = NameOrderedSet{table_join->getClauses().at(0).key_names_right.begin(), table_join->getClauses().at(0).key_names_right.end()};
    for (const auto & name : keys)
        hash.update(name);
    return hash.get64();
}
}
