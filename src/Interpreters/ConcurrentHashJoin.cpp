#include <Columns/ColumnSparse.h>
#include <Columns/FilterDescription.h>
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/createBlockSelector.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/parseQuery.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/BitHelpers.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>
#include <Common/WeakHash.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>

#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/KeyGetter.h>
#include <DataTypes/NullableUtils.h>
#include <base/defines.h>
#include <base/types.h>

#include <algorithm>
#include <numeric>

using namespace DB;

#define INVOKE_WITH_MAP(TYPE, maps, f) \
    case HashJoin::Type::TYPE:         \
        return f(*(maps).TYPE);

#define INVOKE_WITH_MAPS(TYPE, lhs_maps, rhs_maps, f) \
    case HashJoin::Type::TYPE:                        \
        return f(*(lhs_maps).TYPE, *(rhs_maps).TYPE);

#define APPLY_TO_MAP(M, type, ...)                        \
    switch (type)                                         \
    {                                                     \
        APPLY_FOR_TWO_LEVEL_JOIN_VARIANTS(M, __VA_ARGS__) \
                                                          \
        default:                                          \
            UNREACHABLE();                                \
    }

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

using BlockHashes = std::vector<UInt64>;

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

UInt32 toPowerOfTwo(UInt32 x)
{
    if (x <= 1)
        return 1;
    return static_cast<UInt32>(1) << (32 - std::countl_zero(x - 1));
}

HashJoin::RightTableDataPtr getData(const std::shared_ptr<ConcurrentHashJoin::InternalHashJoin> & join)
{
    return join->data->getJoinedData();
}

void reserveSpaceInHashMaps(HashJoin & hash_join, size_t ind, const StatsCollectingParams & stats_collecting_params, size_t slots)
{
    if (auto hint = getSizeHint(stats_collecting_params, slots))
    {
        /// Hash map is shared between all `HashJoin` instances, so the `median_size` is actually the total size
        /// we need to preallocate in all buckets of all hash maps.
        const size_t reserve_size = hint->median_size;

        /// Each `HashJoin` instance will "own" a subset of buckets during the build phase. Because of that
        /// we preallocate space only in the specific buckets of each `HashJoin` instance.
        auto reserve_space_in_buckets = [&](auto & maps, HashJoin::Type type, size_t idx)
        {
            APPLY_TO_MAP(
                INVOKE_WITH_MAP,
                type,
                maps,
                [&](auto & map)
                {
                    for (size_t j = idx; j < map.NUM_BUCKETS; j += slots)
                        map.impls[j].reserve(reserve_size / map.NUM_BUCKETS);
                })
        };

        const auto & right_data = hash_join.getJoinedData();
        std::visit([&](auto & maps) { return reserve_space_in_buckets(maps, right_data->type, ind); }, right_data->maps.at(0));
        ProfileEvents::increment(ProfileEvents::HashJoinPreallocatedElementsInHashTables, reserve_size / slots);
    }
}

template <typename HashTable>
concept HasGetBucketFromHashMemberFunc = requires {
    { std::declval<HashTable>().getBucketFromHash(static_cast<size_t>(0)) };
};
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}


ConcurrentHashJoin::ConcurrentHashJoin(
    std::shared_ptr<TableJoin> table_join_,
    size_t slots_,
    const Block & right_sample_block,
    const StatsCollectingParams & stats_collecting_params_,
    bool any_take_last_row_)
    : table_join(table_join_)
    , slots(toPowerOfTwo(std::min<UInt32>(static_cast<UInt32>(slots_), 256)))
    , any_take_last_row(any_take_last_row_)
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
                [&, i, thread_group = CurrentThread::getGroup()]()
                {
                    ThreadGroupSwitcher switcher(thread_group, "ConcurrentJoin");

                    /// reserve is not needed anyway - either we will use fixed-size hash map or shared two-level map (then reserve will be done in a special way below)
                    const size_t reserve_size = 0;

                    auto inner_hash_join = std::make_shared<InternalHashJoin>();
                    inner_hash_join->data = std::make_unique<HashJoin>(
                        table_join_,
                        right_sample_block,
                        any_take_last_row_,
                        reserve_size,
                        fmt::format("concurrent{}", i),
                        /*use_two_level_maps*/ true);
                    inner_hash_join->data->setMaxJoinedBlockRows(table_join->maxJoinedBlockRows());
                    hash_joins[i] = std::move(inner_hash_join);
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
        if (!hash_joins[0]->data->twoLevelMapIsUsed())
            return;

        updateStatistics(hash_joins, stats_collecting_params);

        for (size_t i = 0; i < slots; ++i)
        {
            // Hash tables destruction may be very time-consuming.
            // Without the following code, they would be destroyed in the current thread (i.e. sequentially).
            pool->scheduleOrThrow(
                [join = hash_joins[0], i, this, thread_group = CurrentThread::getGroup()]()
                {
                    ThreadGroupSwitcher switcher(thread_group, "ConcurrentJoin");

                    auto clear_space_in_buckets = [&](auto & maps, HashJoin::Type type, size_t idx)
                    {
                        APPLY_TO_MAP(
                            INVOKE_WITH_MAP,
                            type,
                            maps,
                            [&](auto & map)
                            {
                                for (size_t j = idx; j < map.NUM_BUCKETS; j += slots)
                                    map.impls[j].clearAndShrink();
                            })
                    };
                    const auto & right_data = getData(join);
                    std::visit([&](auto & maps) { return clear_space_in_buckets(maps, right_data->type, i); }, right_data->maps.at(0));
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

bool ConcurrentHashJoin::addBlockToJoin(const Block & right_block_, bool check_limits)
{
    /// We materialize columns here to avoid materializing them multiple times on different threads
    /// (inside different `hash_join`-s) because the block will be shared.
    Block right_block = hash_joins[0]->data->materializeColumnsFromRightBlock(right_block_);

    auto dispatched_blocks = dispatchBlock(table_join->getOnlyClause().key_names_right, std::move(right_block));
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

                if (!hash_join->space_was_preallocated && hash_join->data->twoLevelMapIsUsed())
                {
                    reserveSpaceInHashMaps(*hash_join->data, i, stats_collecting_params, slots);
                    hash_join->space_was_preallocated = true;
                }

                bool limit_exceeded = !hash_join->data->addBlockToJoin(dispatched_block, check_limits);

                dispatched_block = {};
                blocks_left--;

                if (limit_exceeded)
                    return false;
            }
        }
    }

    if (check_limits && table_join->sizeLimits().hasLimits())
        return table_join->sizeLimits().check(getTotalRowCount(), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    return true;
}

void ConcurrentHashJoin::joinBlock(Block & block, std::shared_ptr<ExtraBlock> & /*not_processed*/)
{
    Blocks res;
    ExtraScatteredBlocks extra_blocks;
    joinBlock(block, extra_blocks, res);
    chassert(!extra_blocks.rows());
    block = concatenateBlocks(res);
}

void ConcurrentHashJoin::joinBlock(Block & block, ExtraScatteredBlocks & extra_blocks, std::vector<Block> & res)
{
    ScatteredBlocks dispatched_blocks;
    auto & remaining_blocks = extra_blocks.remaining_blocks;
    if (extra_blocks.rows())
    {
        dispatched_blocks.swap(remaining_blocks);
    }
    else
    {
        hash_joins[0]->data->materializeColumnsFromLeftBlock(block);
        if (hash_joins[0]->data->twoLevelMapIsUsed())
            dispatched_blocks.emplace_back(std::move(block));
        else
            dispatched_blocks = dispatchBlock(table_join->getOnlyClause().key_names_left, std::move(block));
    }

    chassert(dispatched_blocks.size() == (hash_joins[0]->data->twoLevelMapIsUsed() ? 1 : slots));

    block = {};

    /// Just in case, should be no-op always
    remaining_blocks.resize(dispatched_blocks.size());

    chassert(res.empty());
    res.clear();
    res.reserve(dispatched_blocks.size());

    /// Might be zero, which means unlimited
    size_t remaining_rows_before_limit = table_join->maxJoinedBlockRows();

    for (size_t i = 0; i < dispatched_blocks.size(); ++i)
    {
        if (table_join->maxJoinedBlockRows() && remaining_rows_before_limit == 0)
        {
            /// Joining previous blocks produced enough rows already, skipping the rest of the blocks until the next call
            remaining_blocks[i] = std::move(dispatched_blocks[i]);
            continue;
        }
        auto & hash_join = hash_joins[i];
        auto & current_block = dispatched_blocks[i];
        if (current_block && (i == 0 || current_block.rows()))
            hash_join->data->joinBlock(current_block, remaining_blocks[i]);
        remaining_rows_before_limit -= std::min(current_block.rows(), remaining_rows_before_limit);
    }
    for (size_t i = 0; i < dispatched_blocks.size(); ++i)
    {
        auto & dispatched_block = dispatched_blocks[i];
        if (dispatched_block && (i == 0 || dispatched_block.rows()))
            res.emplace_back(std::move(dispatched_block).getSourceBlock());
    }
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

template <typename HashTable>
static IColumn::Selector hashToSelector(const HashTable & hash_table, const BlockHashes & hashes, size_t num_shards)
{
    assert(isPowerOf2(num_shards));
    const size_t num_rows = hashes.size();
    IColumn::Selector selector(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
    {
        if constexpr (HasGetBucketFromHashMemberFunc<HashTable>)
            selector[i] = hash_table.getBucketFromHash(hashes[i]) & (num_shards - 1);
        else
            selector[i] = hashes[i] & (num_shards - 1);
    }
    return selector;
}

template <typename KeyGetter, typename HashTable>
BlockHashes calculateHashes(const HashTable & hash_table, const ColumnRawPtrs & key_columns, const Sizes & key_sizes)
{
    const size_t num_rows = key_columns[0]->size();
    Arena pool;
    auto key_getter = KeyGetter(key_columns, key_sizes, nullptr);
    BlockHashes hash(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
        hash[i] = key_getter.getHash(hash_table, i, pool);
    return hash;
}

IColumn::Selector selectDispatchBlock(const HashJoin & join, size_t num_shards, const Strings & key_columns_names, const Block & from_block)
{
    std::vector<ColumnPtr> key_column_holders;
    ColumnRawPtrs key_columns;
    key_columns.reserve(key_columns_names.size());
    for (const auto & key_name : key_columns_names)
    {
        const auto & key_col = from_block.getByName(key_name).column->convertToFullColumnIfConst();
        const auto & key_col_no_lc = recursiveRemoveLowCardinality(recursiveRemoveSparse(key_col));
        key_column_holders.push_back(key_col_no_lc);
        key_columns.push_back(key_col_no_lc.get());
    }
    ConstNullMapPtr null_map{};
    ColumnPtr null_map_holder = extractNestedColumnsAndNullMap(key_columns, null_map);

    auto calculate_selector = [&](auto & maps)
    {
        BlockHashes hash;

        switch (join.getJoinedData()->type)
        {
        #define M(TYPE)                                                                                                                       \
            case HashJoin::Type::TYPE:                                                                                                        \
                hash = calculateHashes<typename KeyGetterForType<HashJoin::Type::TYPE, std::remove_reference_t<decltype(*maps.TYPE)>>::Type>( \
                    *maps.TYPE, key_columns, join.getKeySizes().at(0));                                                                       \
                return hashToSelector(*maps.TYPE, hash, num_shards);

                APPLY_FOR_JOIN_VARIANTS(M)
        #undef M

            default:
                UNREACHABLE();
        }
    };

    /// CHJ supports only one join clause for now
    chassert(join.getJoinedData()->maps.size() == 1, "Expected to have only one join clause");

    return std::visit([&](auto & maps) { return calculate_selector(maps); }, join.getJoinedData()->maps.at(0));
}

ScatteredBlocks scatterBlocksByCopying(size_t num_shards, const IColumn::Selector & selector, const Block & from_block)
{
    Blocks blocks(num_shards);
    for (size_t i = 0; i < num_shards; ++i)
        blocks[i] = from_block.cloneEmpty();

    for (size_t i = 0; i < from_block.columns(); ++i)
    {
        auto dispatched_columns = from_block.getByPosition(i).column->scatter(num_shards, selector);
        chassert(blocks.size() == dispatched_columns.size());
        for (size_t block_index = 0; block_index < num_shards; ++block_index)
        {
            blocks[block_index].getByPosition(i).column = std::move(dispatched_columns[block_index]);
        }
    }

    ScatteredBlocks result;
    result.reserve(num_shards);
    for (size_t i = 0; i < num_shards; ++i)
        result.emplace_back(std::move(blocks[i]));
    return result;
}

ScatteredBlocks scatterBlocksWithSelector(size_t num_shards, const IColumn::Selector & selector, const Block & from_block)
{
    std::vector<ScatteredBlock::IndexesPtr> selectors(num_shards);
    for (size_t i = 0; i < num_shards; ++i)
    {
        selectors[i] = ScatteredBlock::Indexes::create();
        selectors[i]->reserve(selector.size() / num_shards + 1);
    }
    for (size_t i = 0; i < selector.size(); ++i)
    {
        const size_t shard = selector[i];
        selectors[shard]->getData().push_back(i);
    }
    ScatteredBlocks result;
    result.reserve(num_shards);
    for (size_t i = 0; i < num_shards; ++i)
        result.emplace_back(from_block, std::move(selectors[i]));
    return result;
}

ScatteredBlocks ConcurrentHashJoin::dispatchBlock(const Strings & key_columns_names, Block && from_block)
{
    const size_t num_shards = hash_joins.size();
    if (num_shards == 1)
    {
        ScatteredBlocks res;
        res.emplace_back(std::move(from_block));
        return res;
    }

    IColumn::Selector selector = selectDispatchBlock(*hash_joins[0]->data, num_shards, key_columns_names, from_block);

    /// With zero-copy approach we won't copy the source columns, but will create a new one with indices.
    /// This is not beneficial when the whole set of columns is e.g. a single small column.
    constexpr auto threshold = sizeof(IColumn::Selector::value_type);
    const auto & data_types = from_block.getDataTypes();
    const bool use_zero_copy_approach
        = std::accumulate(
              data_types.begin(),
              data_types.end(),
              0u,
              [](size_t sum, const DataTypePtr & type)
              { return sum + (type->haveMaximumSizeOfValue() ? type->getMaximumSizeOfValueInMemory() : threshold + 1); })
        > threshold;

    return use_zero_copy_approach ? scatterBlocksWithSelector(num_shards, selector, from_block)
                                  : scatterBlocksByCopying(num_shards, selector, from_block);
}

IQueryTreeNode::HashState preCalculateCacheKey(const QueryTreeNodePtr & right_table_expression, const SelectQueryInfo & select_query_info)
{
    IQueryTreeNode::HashState hash;

    const auto * select = select_query_info.query->as<DB::ASTSelectQuery>();
    if (!select)
        return hash;

    if (const auto prewhere = select->prewhere())
        hash.update(prewhere->getTreeHash(/*ignore_aliases=*/true));
    if (const auto where = select->where())
        hash.update(where->getTreeHash(/*ignore_aliases=*/true));

    chassert(right_table_expression);
    hash.update(right_table_expression->getTreeHash());
    return hash;
}

UInt64 calculateCacheKey(std::shared_ptr<TableJoin> & table_join, IQueryTreeNode::HashState hash)
{
    // This condition is always true for ConcurrentHashJoin (see `TableJoin::allowParallelHashJoin()`),
    // but this method is called from generic code.
    if (!table_join || !table_join->oneDisjunct())
        return 0;

    const auto keys
        = NameOrderedSet{table_join->getClauses().at(0).key_names_right.begin(), table_join->getClauses().at(0).key_names_right.end()};
    for (const auto & name : keys)
        hash.update(name);

    return hash.get64();
}

void ConcurrentHashJoin::onBuildPhaseFinish()
{
    if (hash_joins[0]->data->twoLevelMapIsUsed())
    {
        // At this point, the build phase is finished. We need to build a shared common hash map to be used in the probe phase.
        // It is done in two steps:
        //     1. Merge hash maps into a single one. For that, we iterate over all sub-maps and move buckets from the current `HashJoin` instance to the common map.
        for (size_t i = 1; i < slots; ++i)
        {
            auto move_buckets = [&](auto & lhs_maps, HashJoin::Type type, auto & rhs_maps, size_t idx)
            {
                APPLY_TO_MAP(
                    INVOKE_WITH_MAPS,
                    type,
                    lhs_maps,
                    rhs_maps,
                    [&](auto & lhs_map, auto & rhs_map)
                    {
                        for (size_t j = idx; j < lhs_map.NUM_BUCKETS; j += slots)
                        {
                            if (!lhs_map.impls[j].empty())
                                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected non-empty map");
                            lhs_map.impls[j] = std::move(rhs_map.impls[j]);
                        }
                    })
            };

            std::visit(
                [&](auto & lhs_map)
                {
                    using T = std::decay_t<decltype(lhs_map)>;
                    move_buckets(lhs_map, getData(hash_joins[0])->type, std::get<T>(getData(hash_joins[i])->maps.at(0)), i);
                },
                getData(hash_joins[0])->maps.at(0));
        }
    }

    // `onBuildPhaseFinish` cannot be called concurrently with other IJoin methods, so we don't need a lock to access internal joins.
    // The following calls must be done after the final common map is constructed, otherwise we will incorrectly initialize `used_flags`.
    for (const auto & hash_join : hash_joins)
        hash_join->data->onBuildPhaseFinish();

    if (hash_joins[0]->data->twoLevelMapIsUsed())
    {
        //     2. Copy this common map to all the `HashJoin` instances along with the `used_flags` data structure.
        for (size_t i = 1; i < slots; ++i)
        {
            getData(hash_joins[i])->maps = getData(hash_joins[0])->maps;
            hash_joins[i]->data->getUsedFlags() = hash_joins[0]->data->getUsedFlags();
        }
    }
}
}

#undef INVOKE_WITH_MAP
#undef INVOKE_WITH_MAPS
#undef APPLY_TO_MAP
