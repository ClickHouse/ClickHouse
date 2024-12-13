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
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
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
#include <Interpreters/NullableUtils.h>
#include <base/defines.h>
#include <base/types.h>

#include <algorithm>
#include <numeric>

using namespace DB;

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
        size_t reserve_size = 0;
        if (auto hint = getSizeHint(stats_collecting_params, slots))
            reserve_size = hint->median_size;
        ProfileEvents::increment(ProfileEvents::HashJoinPreallocatedElementsInHashTables, reserve_size);

        for (size_t i = 0; i < slots; ++i)
        {
            auto inner_hash_join = std::make_shared<InternalHashJoin>();
            inner_hash_join->data = std::make_unique<HashJoin>(
                table_join_, right_sample_block, any_take_last_row_, /*reserve_size*/ 0, fmt::format("concurrent{}", i));
            inner_hash_join->data->setMaxJoinedBlockRows(table_join->maxJoinedBlockRows());
            hash_joins[i] = std::move(inner_hash_join);
        }

        if (reserve_size >= 1'000'000)
        {
            for (size_t i = 0; i < slots; ++i)
            {
                auto & join = hash_joins[0]->data;

                auto f = [&](auto & map, size_t idx)
                {
                    if constexpr (HasGetBucketFromHashMemberFunc<decltype(map)>)
                    {
                        for (size_t j = idx; j < 256; j += slots)
                            map.impls[idx].reserve(reserve_size / 256);
                    }
                };

                auto calculate_selector = [&](auto & maps, size_t idx)
                {
                    switch (join->getJoinedData()->type)
                    {
                        case HashJoin::Type::EMPTY:
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected HashJoin::Type::EMPTY");
                        case HashJoin::Type::CROSS:
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected HashJoin::Type::CROSS");

#define M(TYPE) \
    case HashJoin::Type::TYPE: \
        return f(*maps.TYPE, idx);

                            APPLY_FOR_JOIN_VARIANTS(M)
#undef M
                    }

                    UNREACHABLE();
                };

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

                        std::visit(
                            [&](auto & maps)
                            {
                                using T = std::decay_t<decltype(maps)>;
                                if constexpr (std::is_same_v<T, HashJoin::MapsOne>)
                                    return calculate_selector(maps, idx);
                                else if constexpr (std::is_same_v<T, HashJoin::MapsAll>)
                                    return calculate_selector(maps, idx);
                                else if constexpr (std::is_same_v<T, HashJoin::MapsAsof>)
                                    return calculate_selector(maps, idx);
                                else
                                    static_assert(false);
                            },
                            join->getJoinedData()->maps.at(0));
                    });
            }
            pool->wait();
        }

        for (size_t i = 1; i < slots; ++i)
        {
            if (hash_joins[0] && hash_joins[0]->data && hash_joins[0]->data->getJoinedData())
            {
                hash_joins[i]->data->getJoinedData()->maps = hash_joins[0]->data->getJoinedData()->maps;
                hash_joins[i]->data->getUsedFlags() = hash_joins[0]->data->getUsedFlags();
            }
        }
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
    Blocks res;
    ExtraScatteredBlocks extra_blocks;
    joinBlock(block, extra_blocks, res);
    chassert(!extra_blocks.rows());
    block = concatenateBlocks(res);
}

void ConcurrentHashJoin::joinBlock(Block & block, ExtraScatteredBlocks & extra_blocks, std::vector<Block> & res)
{
    // TODO(nickitat): simplify code, since we don't scatter anymore
    ScatteredBlocks dispatched_blocks;
    auto & remaining_blocks = extra_blocks.remaining_blocks;
    if (extra_blocks.rows())
    {
        dispatched_blocks.swap(remaining_blocks);
    }
    else
    {
        hash_joins[0]->data->materializeColumnsFromLeftBlock(block);
        dispatched_blocks.emplace_back(std::move(block));
    }
    if (dispatched_blocks.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "dispatched_blocks.size() != 1");

    block = {};

    /// Just in case, should be no-op always
    remaining_blocks.resize(1);

    chassert(res.empty());
    res.clear();
    res.reserve(dispatched_blocks.size());

    for (size_t i = 0; i < dispatched_blocks.size(); ++i)
    {
        auto & hash_join = hash_joins[i];
        auto & dispatched_block = dispatched_blocks[i];
        if (dispatched_block && (i == 0 || dispatched_block.rows()))
            hash_join->data->joinBlock(dispatched_block, remaining_blocks[i]);
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
            case HashJoin::Type::EMPTY:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected HashJoin::Type::EMPTY");
            case HashJoin::Type::CROSS:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected HashJoin::Type::CROSS");

#define M(TYPE) \
    case HashJoin::Type::TYPE: \
        hash = calculateHashes<typename KeyGetterForType<HashJoin::Type::TYPE, std::remove_reference_t<decltype(*maps.TYPE)>>::Type>( \
            *maps.TYPE, key_columns, join.getKeySizes().at(0)); \
        return hashToSelector(*maps.TYPE, hash, num_shards);

                APPLY_FOR_JOIN_VARIANTS(M)
#undef M
        }

        UNREACHABLE();
    };

    /// CHJ supports only one join clause for now
    chassert(join.getJoinedData()->maps.size() == 1, "Expected to have only one join clause");

    return std::visit(
        [&](auto & maps)
        {
            using T = std::decay_t<decltype(maps)>;
            if constexpr (std::is_same_v<T, HashJoin::MapsOne>)
                return calculate_selector(maps);
            else if constexpr (std::is_same_v<T, HashJoin::MapsAll>)
                return calculate_selector(maps);
            else if constexpr (std::is_same_v<T, HashJoin::MapsAsof>)
                return calculate_selector(maps);
            else
                static_assert(false);
        },
        join.getJoinedData()->maps.at(0));
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

UInt64 calculateCacheKey(
    std::shared_ptr<TableJoin> & table_join, const QueryTreeNodePtr & right_table_expression, const SelectQueryInfo & select_query_info)
{
    const auto * select = select_query_info.query->as<DB::ASTSelectQuery>();
    if (!select)
        return 0;

    IQueryTreeNode::HashState hash;

    if (const auto prewhere = select->prewhere())
        hash.update(prewhere->getTreeHash(/*ignore_aliases=*/true));
    if (const auto where = select->where())
        hash.update(where->getTreeHash(/*ignore_aliases=*/true));

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
