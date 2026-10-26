#include <any>
#include <limits>
#include <memory>
#include <optional>
#include <vector>
#include <Columns/ColumnIndex.h>
#include <Core/Block.h>

#include <base/getL2CacheSize.h>
#include <base/scope_guard.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsCommon.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/StackTrace.h>
#include <Common/logger_useful.h>


#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/castTypeToEither.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/HashJoin/DeferredBuild.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/MatchedRowsStats.h>
#include <Interpreters/JoinUtils.h>
#include <DataTypes/NullableUtils.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/RowDataStore.h>
#include <Interpreters/RowRefs.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/joinDispatch.h>

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/assert_cast.h>
#include <Common/formatReadable.h>
#include <Common/typeid_cast.h>
#include <Interpreters/IJoin.h>

#include <Interpreters/HashJoin/HashJoinMethods.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Interpreters/HashJoin/gatherJoinOutputColumns.h>

#include <numeric>

#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <Processors/QueryPlan/Profiling/Metrics/StepAnalyzeInfo.h>

namespace ProfileEvents
{
    extern const Event HashJoinBuiltWithParallelLayout;
    extern const Event HashJoinBuiltWithSerialLayout;
    extern const Event HashJoinDeferredBuilds;
    extern const Event HashJoinDeferredBuildBucketsGrown;
    extern const Event JoinNonJoinedHashTableScans;
    extern const Event JoinNonJoinedHashTableScansSkipped;
}

namespace CurrentMetrics
{
extern const Metric HashJoinBuildThreads;
extern const Metric HashJoinBuildThreadsActive;
extern const Metric HashJoinBuildThreadsScheduled;
extern const Metric HashJoinDestroyThreads;
extern const Metric HashJoinDestroyThreadsActive;
extern const Metric HashJoinDestroyThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int NO_SUCH_COLUMN_IN_TABLE;
extern const int INCOMPATIBLE_TYPE_OF_JOIN;
extern const int LOGICAL_ERROR;
extern const int SET_SIZE_LIMIT_EXCEEDED;
extern const int TYPE_MISMATCH;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int INVALID_JOIN_ON_EXPRESSION;
extern const int FAULT_INJECTED;
}

namespace FailPoints
{
extern const char hash_join_throw_after_data_release[];
}

size_t getMinBytesForPrefetchInJoin()
{
    /// Enable prefetch once the hash table no longer fits in L2; below that it
    /// is cache resident and prefetching is pure overhead. Cached after first call.
    static const size_t result = getL2CacheSize();
    return result;
}

namespace
{

/// With zero-copy approach we won't copy the source columns, but will store them as they are.
/// This is not beneficial when the whole set of columns is e.g. a single small column.
/// The threshold is the size of one selector index. `ConcurrentHashJoin` chose it because its zero-copy approach stored an index per row.
bool useZeroCopyApproach(const Block & from_block)
{
    constexpr auto threshold = sizeof(IColumn::Selector::value_type);
    const auto & data_types = from_block.getDataTypes();
    return std::accumulate(
              data_types.begin(),
              data_types.end(),
              0u,
              [](size_t sum, const DataTypePtr & type)
              { return sum + (type->haveMaximumSizeOfValue() ? type->getMaximumSizeOfValueInMemory() : threshold + 1); })
        > threshold;
}

Block filterColumnsPresentInSampleBlock(const Block & block, const Block & sample_block)
{
    Block filtered_block;
    for (const auto & sample_column : sample_block.getColumnsWithTypeAndName())
        filtered_block.insert(block.getByName(sample_column.name));
    return filtered_block;
}

std::pair<Columns, Columns> extractRowStoreColumns(const Block & block, const ColumnAccessIndexes & access_indexes)
{
    Columns row_store_columns;
    Columns remaining_columns;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        const auto & column = block.getByPosition(i);
        if (access_indexes[i].type == ColumnAccessIndex::Type::RowStore)
            row_store_columns.push_back(column.column);
        else
            remaining_columns.push_back(column.column);
    }

    return {row_store_columns, remaining_columns};
}

Columns materializeStoredBlock(StoredBlock & stored_block, const ColumnAccessIndexes & access_indexes)
{
    const auto & stored_columns = stored_block.columns;
    const auto & selector = stored_block.selector;

    MutableColumns row_store_columns;
    if (stored_block.hasRowStore())
    {
        if (selector.isContinuousRange())
        {
            auto [start, end] = selector.getRange();
            row_store_columns = stored_block.row_store->scatterRows(start, end - start);
        }
        else
            row_store_columns = stored_block.row_store->scatterRows(selector.getIndexes().getData());
        stored_block.row_store.reset();
    }

    Columns columnar_columns;
    columnar_columns.reserve(stored_block.columns.size());
    if (selector.size() == stored_block.blockRows())
        columnar_columns = stored_block.columns;
    else if (selector.isContinuousRange())
    {
        auto [start, end] = selector.getRange();
        for (const auto & c : stored_columns)
            columnar_columns.push_back(c->cut(start, end - start));
    }
    else
    {
        const auto & indexes = selector.getIndexes();
        for (const auto & c : stored_columns)
            columnar_columns.push_back(c->index(indexes, /*limit*/ 0));
    }

    if (access_indexes.empty())
        return columnar_columns;

    Columns result(access_indexes.size());
    for (size_t i = 0; i < access_indexes.size(); ++i)
    {
        const auto & access_index = access_indexes[i];
        if (access_index.type == ColumnAccessIndex::Type::RowStore)
            result[i] = std::move(row_store_columns[access_index.index]);
        else
            result[i] = std::move(columnar_columns[access_index.index]);
    }
    return result;
}

/// Routes each row the way `map` places its key: by `map.hash` and `getBucketOfKey`.
template <HashJoin::Type type, typename Map>
std::vector<ScatteredBlock::Selector> scatterImpl(
    const Map & map,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const ScatteredBlock::Selector & selector,
    size_t num_slots,
    bool is_asof)
{
    using KeyGetter = KeyGetterForType<type, Map>::Type;

    if constexpr (requires { KeyGetter::has_pre_computed_hashes; })
        static_assert(!KeyGetter::has_pre_computed_hashes, "Bucket routing assumes the map computes the hash it places by");

    /// For ASOF the key getter leaves out the ASOF column.
    KeyGetter key_getter = is_asof
        ? KeyGetter(ColumnRawPtrs(key_columns.begin(), key_columns.end() - 1), Sizes(key_sizes.begin(), key_sizes.end() - 1), nullptr)
        : KeyGetter(key_columns, key_sizes, nullptr);

    /// Nothing here outlives the call: the key holders are read for their hash, never persisted.
    Arena scratch_pool;

    const size_t rows = selector.size();

    PODArray<UInt32> row_to_slot(rows);
    std::vector<size_t> counts(num_slots, 0);
    for (size_t i = 0; i < rows; ++i)
    {
        auto key_holder = key_getter.getKeyHolder(selector[i], scratch_pool);
        const auto & key = keyHolderGetKey(key_holder);

        const size_t bucket = getBucketOfKey<Map>(key, map.hash(key));
        /// The slot count is a power of two, so this is `bucket % num_slots`.
        const auto slot = static_cast<UInt32>(bucket & (num_slots - 1));
        row_to_slot[i] = slot;
        ++counts[slot];
    }

    std::vector<ScatteredBlock::Selector::IndexesPtr> indexes;
    indexes.reserve(num_slots);
    for (size_t slot = 0; slot < num_slots; ++slot)
    {
        auto column = ScatteredBlock::Selector::Indexes::create();
        column->getData().reserve(counts[slot]);
        indexes.push_back(std::move(column));
    }

    for (size_t i = 0; i < rows; ++i)
        indexes[row_to_slot[i]]->getData().push_back(selector[i]);

    std::vector<ScatteredBlock::Selector> result;
    result.reserve(num_slots);
    for (auto & column : indexes)
        result.emplace_back(std::move(column));

    return result;
}

template <typename Maps>
std::vector<ScatteredBlock::Selector> scatterBlockBySlot(
    HashJoin::Type type,
    const Maps & maps,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const ScatteredBlock::Selector & selector,
    size_t num_slots)
{
    /// `MapsAsof` is the map of every ASOF clause and of no other strictness.
    constexpr bool is_asof = std::is_same_v<Maps, HashJoin::MapsAsof>;
    switch (type)
    {
#define M(NAME) \
        case HashJoin::Type::NAME: \
            return scatterImpl<HashJoin::Type::NAME>(*maps.NAME, key_columns, key_sizes, selector, num_slots, is_asof);
            APPLY_FOR_TWO_LEVEL_JOIN_VARIANTS(M)
#undef M
        default:
            UNREACHABLE();
    }
}

/// The maps a deferred build can reserve. The fixed maps have all their cells from the start.
#define APPLY_FOR_DEFERRED_JOIN_VARIANTS(M) \
    M(key32)                                \
    M(key64)                                \
    M(key_string)                           \
    M(key_fixed_string)                     \
    M(keys32)                               \
    M(keys64)                               \
    M(keys128)                              \
    M(keys256)                              \
    M(hashed)                               \
    M(low_cardinality_key_string)           \
    M(low_cardinality_key_fixed_string)     \
    M(two_level_key32)                      \
    M(two_level_key64)                      \
    M(two_level_key_string)                 \
    M(two_level_key_fixed_string)           \
    M(two_level_keys32)                     \
    M(two_level_keys64)                     \
    M(two_level_keys128)                    \
    M(two_level_keys256)                    \
    M(two_level_hashed)

bool canDeferBuildOf(HashJoin::Type type)
{
    switch (type)
    {
#define M(NAME) \
        case HashJoin::Type::NAME: \
            return true;
        APPLY_FOR_DEFERRED_JOIN_VARIANTS(M)
#undef M
        default:
            return false;
    }
}

template <typename Map, typename Maps>
DeferredBuildMapShape deferredBuildMapShape()
{
    DeferredBuildMapShape shape;
    if constexpr (!is_partitioned_fixed_table<Map>)
        shape.bucket_buffer_bytes = [](size_t keys)
        {
            /// What `HashTable::reserve` allocates.
            using Bucket = typename Map::Impl;
            typename Bucket::grower_type grower;
            if (keys)
                grower.set(keys);
            return grower.bufSize() * sizeof(typename Bucket::cell_type);
        };
    shape.keys_in_arena = std::is_same_v<typename Map::key_type, std::string_view>;
    shape.row_lists = std::is_same_v<Maps, HashJoin::MapsAll>;
    return shape;
}

/// Whether some rows of a block have a NULL key or fail the ON condition. `insertFromBlockImpl` skips them.
struct SkippedRows
{
    bool null_key = false;
    bool filtered = false;
};

/// Counts the keys of the rows that `insertFromBlockImpl` inserts into `map`: in the sketch by the hash of the map,
/// and per bucket of the map.
template <HashJoin::Type type, typename Map>
SkippedRows countInsertableKeysImpl(
    const Map & map,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const ScatteredBlock::Selector & selector,
    ConstNullMapPtr null_map,
    const JoinCommon::JoinMask & join_mask,
    bool is_asof,
    DeferredBuildStreamCounts::Clause & counts)
{
    using KeyGetter = KeyGetterForType<type, Map>::Type;

    /// For ASOF the key getter leaves out the ASOF column.
    KeyGetter key_getter = is_asof
        ? KeyGetter(ColumnRawPtrs(key_columns.begin(), key_columns.end() - 1), Sizes(key_sizes.begin(), key_sizes.end() - 1), nullptr)
        : KeyGetter(key_columns, key_sizes, nullptr);

    /// The key holders are read for their hash, never kept.
    Arena scratch_pool;

    SkippedRows skipped;
    size_t rows = 0;
    size_t key_bytes = 0;
    size_t max_key_bytes = 0;
    for (const size_t row : selector)
    {
        if (null_map && (*null_map)[row])
        {
            skipped.null_key = true;
            continue;
        }
        if (join_mask.isRowFiltered(row))
        {
            skipped.filtered = true;
            continue;
        }

        auto key_holder = key_getter.getKeyHolder(row, scratch_pool);
        const auto & key = keyHolderGetKey(key_holder);
        const size_t hash = map.hash(key);
        const size_t bucket = getBucketOfKey<Map>(key, hash);
        counts.sketch.insert(static_cast<UInt32>(hash));
        ++counts.bucket_rows[bucket];
        ++rows;
        if constexpr (std::is_same_v<std::decay_t<decltype(key)>, std::string_view>)
        {
            counts.bucket_key_bytes[bucket] += key.size();
            key_bytes += key.size();
            max_key_bytes = std::max(max_key_bytes, key.size());
        }
    }

    counts.rows.fetch_add(rows, std::memory_order_relaxed);
    counts.key_bytes.fetch_add(key_bytes, std::memory_order_relaxed);
    if (max_key_bytes > counts.max_key_bytes.load(std::memory_order_relaxed))
        counts.max_key_bytes.store(max_key_bytes, std::memory_order_relaxed);
    return skipped;
}

template <typename Maps>
SkippedRows countInsertableKeys(
    HashJoin::Type type,
    const Maps & maps,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const ScatteredBlock::Selector & selector,
    ConstNullMapPtr null_map,
    const JoinCommon::JoinMask & join_mask,
    DeferredBuildStreamCounts::Clause & counts)
{
    constexpr bool is_asof = std::is_same_v<Maps, HashJoin::MapsAsof>;
    switch (type)
    {
#define M(NAME) \
        case HashJoin::Type::NAME: \
            return countInsertableKeysImpl<HashJoin::Type::NAME>( \
                *maps.NAME, key_columns, key_sizes, selector, null_map, join_mask, is_asof, counts);
        APPLY_FOR_DEFERRED_JOIN_VARIANTS(M)
#undef M
        default:
            UNREACHABLE();
    }
}

}

static void correctNullabilityInplace(ColumnWithTypeAndName & column, bool nullable)
{
    if (nullable)
    {
        JoinCommon::convertColumnToNullable(column);
    }
    else
    {
        /// We have to replace values masked by NULLs with defaults.
        if (column.column)
            if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(&*column.column))
                column.column = JoinCommon::filterWithBlanks(column.column, nullable_column->getNullMapColumn().getData(), true);

        JoinCommon::removeColumnNullability(column);
    }
}

static HashJoin::Type chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes);
static std::optional<HashJoin::Type> tryGetLowCardinalityMethod(const ColumnPtr & column);

/// A multi-disjunct (OR) join shares a single data->type across all disjuncts. When the disjuncts
/// pick different packed fixed-key maps (e.g. keys32 for a (UInt16, UInt16) clause and keys64 for a
/// (UInt32, UInt32) clause), use the widest packed map that can hold all of them instead of
/// downgrading the whole join to the generic `hashed` map: a narrower packing always fits into a
/// wider fixed-key map. Only genuinely different key kinds fall back to `hashed`.
static HashJoin::Type mergeJoinMethods(HashJoin::Type lhs, HashJoin::Type rhs)
{
    using Type = HashJoin::Type;

    /// Rank within a packing family (single-level and two-level are ranked separately); 0 = not a
    /// packed fixed-key map. Within one join all disjuncts are the same level, so two packed types
    /// being merged always belong to the same family.
    auto packed_rank = [](Type type) -> int
    {
        switch (type)
        {
            case Type::keys32: case Type::two_level_keys32:   return 1;
            case Type::keys64: case Type::two_level_keys64:   return 2;
            case Type::keys128: case Type::two_level_keys128: return 3;
            case Type::keys256: case Type::two_level_keys256: return 4;
            default:                                          return 0;
        }
    };

    const int lhs_rank = packed_rank(lhs);
    const int rhs_rank = packed_rank(rhs);
    if (lhs_rank != 0 && rhs_rank != 0)
        return lhs_rank >= rhs_rank ? lhs : rhs;

    return Type::hashed;
}

/// The right columns a join with several disjuncts adds to the result. Every right key is read to build
/// the maps, but only the keys the query asks for belong in the result; `requiredRightKeys` says which
/// those are - the planner fills it in `setUsedColumns`, `TreeRewriter` in `addJoinedColumn`.
static Block rightColumnsToAddWithSeveralDisjuncts(const TableJoin & table_join, const Block & right_columns)
{
    NameSet key_names;
    for (const auto & clause : table_join.getClauses())
        key_names.insert(clause.key_names_right.begin(), clause.key_names_right.end());

    const NameSet required_keys = table_join.requiredRightKeys();

    Block columns_to_add;
    for (const auto & column : right_columns)
    {
        if (!key_names.contains(column.name) || required_keys.contains(column.name))
            columns_to_add.insert(column);
    }
    return columns_to_add;
}

HashJoin::HashJoin(
    std::shared_ptr<TableJoin> table_join_,
    SharedHeader right_sample_block_,
    bool any_take_last_row_,
    size_t reserve_num_,
    const String & instance_id_,
    HashJoinBuildLayout build_layout_,
    const HashJoinStatsCollectingParams & stats_collecting_params_,
    bool allow_deferred_build_)
    : table_join(table_join_)
    , kind(table_join->kind())
    , strictness(table_join->strictness())
    , any_take_last_row(any_take_last_row_)
    , reserve_num(reserve_num_)
    , instance_id(instance_id_)
    , build_layout(build_layout_)
    , allow_deferred_build(allow_deferred_build_)
    , asof_inequality(table_join->getAsofInequality())
    , data(std::make_shared<RightTableData>(build_layout))
    , right_sample_block(*right_sample_block_)
    , max_joined_block_rows(table_join->maxJoinedBlockRows())
    , max_joined_block_bytes(table_join->maxJoinedBlockBytes())
    , joined_block_split_single_row(table_join->joinedBlockAllowSplitSingleRow())
    , enable_lazy_columns_replication(table_join->enableColumnsLazyReplication())
    , enable_prefetch(table_join->enableSoftwarePrefetchInJoin())
    , stats_collecting_params(stats_collecting_params_)
    , instance_log_id(!instance_id_.empty() ? "(" + instance_id_ + ") " : "")
    , log(getLogger("HashJoin"))
{
    if (isCrossOrComma(kind))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin cannot execute {}", kind);

    if (table_join->getClauses().empty() || table_join->isJoinWithConstant())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin cannot execute JOIN without keys or with constant keys");

    if (joined_block_split_single_row && max_joined_block_rows == 0)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Setting `joined_block_split_single_row` is set to true, but `max_joined_block_rows` is 0 (no limit). "
            "Set max_joined_block_rows > 0 or use `max_joined_block_bytes` with default `max_joined_block_rows` (by default equals to block size).");
    }

    for (auto & column : right_sample_block)
    {
        if (!column.column)
            column.column = column.type->createColumn();
    }

    validateAdditionalFilterExpression(table_join->getMixedJoinExpression());

    used_flags = std::make_unique<JoinStuff::JoinUsedFlags>();
    used_flags->setPendingFlagWorkers(data->workers.size());

    if (table_join->collectAnalyzeStats())
        matched_rows_stats = std::make_unique<MatchedRowsStats>(kind, strictness, table_join->analyzeMode());

    if (table_join->getClauses().empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin cannot execute JOIN without keys");

    if (table_join->oneDisjunct())
    {
        const auto & key_names_right = table_join->getOnlyClause().key_names_right;
        JoinCommon::splitAdditionalColumns(key_names_right, right_sample_block, right_table_keys, sample_block_with_columns_to_add);
        required_right_keys = table_join->getRequiredRightKeys(right_table_keys, required_right_keys_sources);
    }
    else
    {
        /// With several disjuncts a right key can differ from the left key it matched - the match may
        /// have come from another clause - so it cannot be restored from the left column the way
        /// `required_right_keys` does it, and a key the query asks for stays a column the join adds.
        /// The keys nobody asks for are needed to build the maps and for nothing else.
        right_table_keys = materializeBlock(right_sample_block);
        sample_block_with_columns_to_add = rightColumnsToAddWithSeveralDisjuncts(*table_join, right_table_keys);
    }

    /// Detect a single non-nullable LowCardinality key before the keys are materialized below, so it
    /// can use a dictionary-aware map. Restricted to a single disjunct and non-two-level maps for now.
    std::optional<Type> low_cardinality_method;
    if (table_join->oneDisjunct() && !build_layout.two_level && strictness != JoinStrictness::Asof)
    {
        const auto & only_clause_key_names = table_join->getOnlyClause().key_names_right;
        if (only_clause_key_names.size() == 1)
            low_cardinality_method = tryGetLowCardinalityMethod(right_table_keys.getByName(only_clause_key_names[0]).column);
    }

    materializeBlockInplace(right_table_keys);
    initRightBlockStructure(data->sample_block);
    data->sample_block = prepareRightBlock(data->sample_block);

    if (!table_join->isRowStoreEnabled() || !isRowStoreSupported() || data->sample_block.columns() == 0)
        data->row_store_state = RowStoreState::Disabled;

    JoinCommon::createMissedColumns(sample_block_with_columns_to_add);

    size_t disjuncts_num = table_join->getClauses().size();
    data->maps.resize(disjuncts_num);
    /// Publish the row store layout before any `FillingRightJoinSide` thread can call `addBlockToJoin`.
    initRowStore(data->sample_block);
    key_sizes.reserve(disjuncts_num);

    std::optional<Type> selected_join_method;
    auto set_join_method = [&](Type current_join_method)
    {
        if (!selected_join_method)
            selected_join_method = current_join_method;
        else if (*selected_join_method != current_join_method)
            selected_join_method = mergeJoinMethods(*selected_join_method, current_join_method);
    };

    for (const auto & clause : table_join->getClauses())
    {
        const auto & key_names_right = clause.key_names_right;
        ColumnRawPtrs key_columns = JoinCommon::extractKeysForJoin(right_table_keys, key_names_right);

        if (strictness == JoinStrictness::Asof)
        {
            chassert(disjuncts_num == 1);

            /// @note ASOF JOIN is not INNER. It's better avoid use of 'INNER ASOF' combination in messages.
            /// In fact INNER means 'LEFT SEMI ASOF' while LEFT means 'LEFT OUTER ASOF'.
            if (!isLeft(kind) && !isInner(kind))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Wrong ASOF JOIN type. Only ASOF and LEFT ASOF joins are supported");

            if (key_columns.size() <= 1)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ASOF join with hash algorithm needs at least one equi-join column");

            size_t asof_size = 0;
            asof_type = SortedLookupVectorBase::getTypeSize(*key_columns.back(), asof_size);
            key_columns.pop_back();

            /// this is going to set up the appropriate hash table for the direct lookup part of the join
            /// However, this does not depend on the size of the asof join key (as that goes into the BST)
            /// Therefore, add it back in such that it can be extracted appropriately from the full stored
            /// key_columns and key_sizes
            auto & asof_key_sizes = key_sizes.emplace_back();
            selected_join_method = chooseMethod(key_columns, asof_key_sizes);
            asof_key_sizes.push_back(asof_size);
        }
        else
        {
            /// Choose data structure to use for JOIN.
            auto current_join_method = chooseMethod(key_columns, key_sizes.emplace_back());
            if (low_cardinality_method)
            {
                current_join_method = *low_cardinality_method;
                LOG_TRACE(log, "Using a dictionary-aware hash map for the single LowCardinality join key");
            }
            set_join_method(current_join_method);
        }
    }

    if (!selected_join_method)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin cannot choose JOIN method without keys");

    data->type = build_layout.two_level ? toTwoLevelType(*selected_join_method) : *selected_join_method;

    LOG_TRACE(log, "{}Join hash table type: {}", instance_log_id, data->type);
    LOG_TEST(
        log,
        "{}Keys: {}, datatype: {}, kind: {}, strictness: {}, right header: {}",
        instance_log_id,
        TableJoin::formatClauses(table_join->getClauses(), true),
        data->type,
        kind,
        strictness,
        right_sample_block.dumpStructure());

    use_set_maps = canUseSetMaps();
    if (use_set_maps)
        LOG_TRACE(log, "Using key-only hash tables: the join never reads a right row");

    for (auto & maps : data->maps)
        dataMapInit(maps);

    /// A 256-bucket map is sized from the estimate, or every bucket rehashes its way up from 256 cells. A serial map
    /// is not: sized from the estimate, it lands on a worse load factor than the grower picks.
    size_t size_hint = reserve_num;
    if (!size_hint && build_layout.two_level)
        if (const auto hint = getSizeHint(stats_collecting_params.build))
            size_hint = hint->ht_size;

    /// Statistics of an earlier run, or `reserve_num`, give the size: such a build inserts each block as it arrives, for both layouts.
    bool size_known = size_hint != 0 || getSizeHint(stats_collecting_params.build).has_value();
    const bool may_defer_build = allow_deferred_build && table_join->enableHashJoinDeferredBuild();
    /// Otherwise the `uniq` statistic of the right key may give it, which is used the same way.
    if (may_defer_build && !size_known)
    {
        if (const auto distinct_keys = table_join->rightKeyDistinctValuesFromUniq())
        {
            LOG_TRACE(log, "{}The statistics of the right table give {} distinct keys", instance_log_id, *distinct_keys);
            size_known = true;
            if (build_layout.two_level)
                size_hint = *distinct_keys;
        }
    }
    data->slots.setReserve(data->maps.size(), size_hint, table_join->maxBytesBeforeExternalJoin());

    /// `break` stops at the block that crosses a limit, so the limits must be checked while the blocks arrive.
    const bool breaks_at_limits = table_join->sizeLimits().overflow_mode == OverflowMode::BREAK && table_join->sizeLimits().hasLimits();
    if (may_defer_build && !size_known && !breaks_at_limits && canDeferBuildOf(data->type) && deferredBuildHoldsNoMore())
    {
        std::vector<DeferredBuildMapShape> shapes(data->maps.size());
        for (size_t clause = 0; clause < data->maps.size(); ++clause)
            joinDispatch(
                kind,
                strictness,
                data->maps[clause],
                getMapsKind(),
                [&](auto, auto, auto & maps)
                {
                    maps.visit(
                        data->type,
                        [&]<typename Table>(std::shared_ptr<Table> &)
                        { shapes[clause] = deferredBuildMapShape<Table, std::decay_t<decltype(maps)>>(); });
                });
        deferred_build = std::make_unique<DeferredBuildState>(
            data->workers.size(), std::move(shapes), build_layout.two_level ? NUM_HASH_TABLE_BUCKETS : 1, data->slots.size());
        LOG_TRACE(log, "{}Deferring the build until the number of distinct keys is known", instance_log_id);
    }

    /// `bucket_bytes` only accumulates insert deltas, so seed it with what `create` allocated.
    recomputeBucketBytes();

    if (table_join->getMixedJoinExpression())
    {
        const auto & required_cols = table_join->getMixedJoinExpression()->getRequiredColumnsWithTypes();
        size_t pos = 0;
        for (const auto & input : required_cols)
        {
            if (data->sample_block.has(input.name))
            {
                /// `buildAdditionalFilter` creates the column for this input from `input.type` and fills
                /// it from the stored right blocks, so resolving the input by name alone is not enough:
                /// a same-named column of a different type would be read through a mismatched
                /// `IColumn` interface. Fail here instead, where both types are still known.
                const auto & stored = data->sample_block.getByName(input.name);
                if (!stored.type->equals(*input.type))
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Column {} required by the mixed JOIN ON condition has type {}, "
                        "but the stored right column of that name has type {}",
                        input.name,
                        input.type->getName(),
                        stored.type->getName());

                additional_filter_required_rhs_pos.emplace_back(
                    pos,
                    data->sample_block.getPositionByName(input.name));
            }
            ++pos;
        }
    }

    /// Each small bucket allocation is charged to the thread, which passes the total to the query only past
    /// `max_untracked_memory`, so a 256-bucket layout can leave a megabyte uncharged. Flush it so the query's limit applies now.
    CurrentThread::flushUntrackedMemory();
    CurrentMemoryTracker::check();
}

size_t HashJoin::NullMapHolder::allocatedBytes() const
{
    if (!column)
        return 0;
    size_t rows = column->size();
    if (rows == 0)
        return 0;
    if (rows < selector_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The column size is smaller than the cached size");
    return column->allocatedBytes() * selector_rows / rows;
}

static HashJoin::Type chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes)
{
    using Type = HashJoin::Type;

    size_t keys_size = key_columns.size();

    if (keys_size == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin cannot choose JOIN method without keys");

    bool all_fixed = true;
    size_t keys_bytes = 0;
    key_sizes.resize(keys_size);
    for (size_t j = 0; j < keys_size; ++j)
    {
        if (!key_columns[j]->isFixedAndContiguous())
        {
            all_fixed = false;
            break;
        }
        key_sizes[j] = key_columns[j]->sizeOfValueIfFixed();
        keys_bytes += key_sizes[j];
    }

    /// If there is one numeric key that fits in 64 bits
    if (keys_size == 1 && key_columns[0]->isNumeric())
    {
        size_t size_of_field = key_columns[0]->sizeOfValueIfFixed();
        /// The loop above bails out before assigning `key_sizes` for a `LowCardinality` column.
        key_sizes[0] = size_of_field;
        if (size_of_field == 1)
            return Type::key8;
        if (size_of_field == 2)
            return Type::key16;
        if (size_of_field == 4)
            return Type::key32;
        if (size_of_field == 8)
            return Type::key64;
        if (size_of_field == 16)
            return Type::keys128;
        if (size_of_field == 32)
            return Type::keys256;
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Numeric column has sizeOfField not in 1, 2, 4, 8, 16, 32.");
    }

    /// If the keys fit in N bits, we will use a hash table for N-bit-packed keys
    if (all_fixed && keys_bytes <= 4)
        return Type::keys32;
    if (all_fixed && keys_bytes <= 8)
        return Type::keys64;
    if (all_fixed && keys_bytes <= 16)
        return Type::keys128;
    if (all_fixed && keys_bytes <= HashJoin::max_packed_keys_bytes)
        return Type::keys256;

    /// If there is single string key, use hash table of it's values.
    if (keys_size == 1)
    {
        auto is_string_column = [](const IColumn * column_ptr) -> bool
        {
            if (const auto * lc_column_ptr = typeid_cast<const ColumnLowCardinality *>(column_ptr))
                return typeid_cast<const ColumnString *>(lc_column_ptr->getDictionary().getNestedColumn().get());
            return typeid_cast<const ColumnString *>(column_ptr);
        };

        const auto * key_column = key_columns[0];
        if (is_string_column(key_column)
            || (isColumnConst(*key_column) && is_string_column(assert_cast<const ColumnConst *>(key_column)->getDataColumnPtr().get())))
            return Type::key_string;
    }

    if (keys_size == 1 && typeid_cast<const ColumnFixedString *>(key_columns[0]))
        return Type::key_fixed_string;

    /// Otherwise, will use set of cryptographic hashes of unambiguously serialized values.
    return Type::hashed;
}

/// If the column is a single non-nullable LowCardinality key, return the dictionary-aware map type
/// to use for it. LowCardinality(Nullable(T)) and wide numeric dictionaries fall back to the regular
/// (materialized) path. Mirrors the single-LowCardinality branch of AggregatedDataVariants::chooseMethod.
static std::optional<HashJoin::Type> tryGetLowCardinalityMethod(const ColumnPtr & column)
{
    using Type = HashJoin::Type;

    const auto * low_cardinality_column = typeid_cast<const ColumnLowCardinality *>(column.get());
    if (!low_cardinality_column)
        return {};

    if (low_cardinality_column->getDictionary().nestedColumnIsNullable())
        return {};

    const auto * nested = low_cardinality_column->getDictionary().getNestedNotNullableColumn().get();

    /// Numeric keys are intentionally not routed here. A materialized numeric key uses the key* maps,
    /// which (with `enable_join_fixed_hash_table_conversion`) convert a dense small range to a
    /// `range*_key*` FixedHashMap after build and can publish the shared fixed-hash-table runtime
    /// filter; the dictionary-aware map skips both for no measurable gain. The benefit of the
    /// dictionary-aware map is concentrated on variable-length string keys.
    if (typeid_cast<const ColumnString *>(nested))
        return Type::low_cardinality_key_string;
    if (typeid_cast<const ColumnFixedString *>(nested))
        return Type::low_cardinality_key_fixed_string;

    return {};
}

HashJoin::Type HashJoin::toTwoLevelType(Type type)
{
    switch (type)
    {
        case Type::key8: return Type::two_level_key8;
        case Type::key16: return Type::two_level_key16;
        case Type::key32: return Type::two_level_key32;
        case Type::key64: return Type::two_level_key64;
        case Type::key_string: return Type::two_level_key_string;
        case Type::key_fixed_string: return Type::two_level_key_fixed_string;
        case Type::keys32: return Type::two_level_keys32;
        case Type::keys64: return Type::two_level_keys64;
        case Type::keys128: return Type::two_level_keys128;
        case Type::keys256: return Type::two_level_keys256;
        case Type::hashed: return Type::two_level_hashed;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Hash join map type {} has no two-level variant", type);
    }
}

void HashJoin::dataMapInit(MapsVariant & map)
{
    const auto maps_kind = getMapsKind();
    joinDispatchInit(kind, strictness, map, maps_kind);
    joinDispatch(kind, strictness, map, maps_kind, [&](auto, auto, auto & map_) { map_.create(data->type); });

    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin::dataMapInit called with empty data");
}


bool HashJoin::preferUseMapsAll() const
{
    return all_join_was_promoted_to_right_any // It means that we built hash tables for ALL strictness, but upon finishing found out that we can switch to RIGHT ANY.
                                              // In this case we still have to use ALL maps.
        || table_join->getMixedJoinExpression() != nullptr;
}

/// A set map answers whether a key is present and nothing else, so it fits exactly those joins whose
/// result can never contain a value taken from a right row.
bool HashJoin::canUseSetMaps() const
{
    if (!table_join->enableJoinKeyOnlyHashTables())
        return false;

    /// A mixed join expression is evaluated against the right rows themselves.
    if (preferUseMapsAll() || table_join->getMixedJoinExpression())
        return false;

    /// `StorageJoin` reads its rows back out of the maps, both to `SELECT` from the table and for
    /// `joinGet`.
    if (table_join->isSpecialStorage())
        return false;

    if (kind != JoinKind::Left)
        return false;

    /// LEFT ANTI emits a left row only when its key is missing, and fills the right columns with
    /// defaults, so no right row is ever read whatever is selected from the right side.
    if (strictness == JoinStrictness::Anti)
        return true;

    /// LEFT SEMI emits the matched left row alone, so it qualifies when nothing of the right side
    /// besides the join keys - which are taken from the left row - is selected.
    return strictness == JoinStrictness::Semi && sample_block_with_columns_to_add.columns() == 0;
}

/// A set map holds no reference into a right block, so the blocks it was built from are not needed - but
/// the algorithm wrapping this join may still take them back out with `releaseJoinedBlocks`, and then
/// they have to be there. Only the wrapper knows, and it says so; `join_algorithm` does not, because it
/// lists what may be chosen rather than what was.
bool HashJoin::mustKeepRightBlocks() const
{
    return right_blocks_may_be_taken;
}

/// The blocks a set map keeps only for another algorithm are needed while that algorithm can still
/// take them - which is until the build phase ends: `JoinSwitcher` and `SpillingHashJoin` both take
/// them out of `addBlockToJoin`, and `GraceHashJoin` while it rebuckets one. Once the wrapper has
/// settled on this join, they are dead weight for the whole probe phase, so it says so and they go.
void HashJoin::dropRightBlocksKeptForAnotherAlgorithm()
{
    if (!data || getMapsKind() != JoinMapsKind::Set)
        return;

    /// A nullmap holds a raw pointer into a stored block, and the non-joined stream reads the block
    /// through it. Both nullmaps are only stored for a RIGHT or FULL join, which never gets a set
    /// map, so this holds today - it is here so that the drop stays honest if that ever changes.
    if (!data->workers.front().nullmaps.empty())
        return;

    doDebugAsserts();
    for (auto & stored_columns : data->workers.front().columns)
    {
        data->subBytes(data->allocated_size, stored_columns.allocatedBytes());
        /// No cell refers to the block, but null the index entry anyway, so that a stale reference
        /// trips the chassert in `StoredColumnsIndex::at` rather than reading freed memory.
        data->stored_columns_index->clearEntry(stored_columns.block_no);
    }
    data->workers.front().columns.clear();
    doDebugAsserts();
}

JoinMapsKind HashJoin::getMapsKind() const
{
    if (preferUseMapsAll())
        return JoinMapsKind::All;
    if (use_set_maps)
        return JoinMapsKind::Set;
    return JoinMapsKind::Default;
}

bool HashJoin::alwaysReturnsEmptySet() const
{
    /// A left semi join keeps only the left rows with a match, so it is empty for an empty right side too.
    const bool empty_for_empty_right = isInnerOrRight(getKind()) || (isLeft(getKind()) && getStrictness() == JoinStrictness::Semi);
    return empty_for_empty_right && data->rows_to_join == 0;
}

size_t HashJoin::getTotalRowCount() const
{
    if (!data)
        return 0;

    return data->keys_to_join.load(std::memory_order_relaxed);
}

void HashJoin::recomputeBucketBytes()
{
    size_t res = data->slots.arenaBytes();

    const auto maps_kind = getMapsKind();
    for (const auto & map : data->maps)
    {
        joinDispatch(
            kind, strictness, map, maps_kind, [&](auto, auto, auto & map_) { res += map_.getTotalByteCountImpl(data->type); });
    }

    data->setBytes(data->bucket_bytes, res);
}

void HashJoin::doDebugAsserts() const
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    /// Build threads append to per-worker lists with no shared lock. Walking every
    /// worker here races with those pushes (TSan) and can disagree with the atomics.
    if (data->workers.size() > 1 && !build_phase_finished)
        return;

    size_t debug_allocated_size = 0;
    for (const auto & worker : data->workers)
        for (const auto & columns : worker.columns)
            debug_allocated_size += columns.allocatedBytes();

    if (data->allocated_size != debug_allocated_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "data->allocated_size != debug_allocated_size ({} != {})",
            data->allocated_size.load(std::memory_order_relaxed),
            debug_allocated_size);

    size_t debug_nullmaps_allocated_size = 0;
    for (const auto & worker : data->workers)
        for (const auto & nullmap : worker.nullmaps)
            debug_nullmaps_allocated_size += nullmap.allocatedBytes();

    if (data->nullmaps_allocated_size != debug_nullmaps_allocated_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "data->nullmaps_allocated_size != debug_nullmaps_allocated_size ({} != {})",
            data->nullmaps_allocated_size.load(std::memory_order_relaxed),
            debug_nullmaps_allocated_size);
#endif
}

size_t HashJoin::getTotalByteCount() const
{
    if (!data)
        return 0;

    doDebugAsserts();
    return data->total_bytes.load(std::memory_order_relaxed);
}

void HashJoin::setTotals(const Block & block)
{
    if (block.empty())
        return;

    std::lock_guard lock(totals_mutex);
    IJoin::setTotals(block);
}

const Block & HashJoin::getTotals() const
{
    std::lock_guard lock(totals_mutex);
    return IJoin::getTotals();
}

StepAnalysisReport HashJoin::getAnalysisReport() const
{
    StepAnalysisReport report;

    if (matched_rows_stats)
    {
        UInt64 right_rows_total = getRightTableRowCount();
        report = buildMatchedRowsReport({
            .left_rows = matched_rows_stats->getInputLeft(),
            .matched_left = matched_rows_stats->getMatchedLeft(),
            .right_rows = right_rows_total,
            .matched_right = matched_rows_stats->getMatchedRight(right_rows_total)});
    }
    else
    {
        MetricList right_metrics;
        right_metrics.emplace_back(MetricKey::Rows, getRightTableRowCount());
        report.push_back({MetricGroupKey::Right, std::move(right_metrics)});
    }

    MetricList hash_table_metrics;
    hash_table_metrics.emplace_back(MetricKey::UniqueKeys, getTotalRowCount());
    hash_table_metrics.emplace_back(MetricKey::Memory, getPeakBuildBytes());
    report.push_back({MetricGroupKey::HashTable, std::move(hash_table_metrics)});

    return report;
}

bool HashJoin::isUsedByAnotherAlgorithm(const TableJoin & table_join)
{
    return table_join.isEnabledAlgorithm(JoinAlgorithm::AUTO)
        || table_join.isEnabledAlgorithm(JoinAlgorithm::GRACE_HASH)
        || table_join.maxBytesBeforeExternalJoin() > 0;
}
bool HashJoin::canRemoveColumnsFromLeftBlock(const TableJoin & table_join)
{
    /// `auto` may switch HashJoin -> MergeJoin (which keeps all left columns), so pruning would
    /// desync block structure; keep `grace_hash` conservative too. A spill threshold alone only
    /// switches HashJoin -> GraceHashJoin (same result path), so it must not disable pruning.
    const bool keep_left_columns = table_join.isEnabledAlgorithm(JoinAlgorithm::AUTO)
        || table_join.isEnabledAlgorithm(JoinAlgorithm::GRACE_HASH);
    return !table_join.hasUsing() && !keep_left_columns && table_join.strictness() != JoinStrictness::RightAny;
}

bool HashJoin::isUsedByAnotherAlgorithm() const
{
    return isUsedByAnotherAlgorithm(*table_join);
}

bool HashJoin::canRemoveColumnsFromLeftBlock() const
{
    return canRemoveColumnsFromLeftBlock(*table_join);
}

void HashJoin::initRightBlockStructure(Block & saved_block_sample)
{
    bool multiple_disjuncts = !table_join->oneDisjunct();
    /// We could remove key columns for LEFT | INNER HashJoin but we should keep them for JoinSwitcher (if any).
    bool save_key_columns = isUsedByAnotherAlgorithm() ||
                            isRightOrFull(kind) ||
                            multiple_disjuncts ||
                            table_join->getMixedJoinExpression();

    if (save_key_columns)
    {
        saved_block_sample = right_table_keys.cloneEmpty();
    }
    else if (strictness == JoinStrictness::Asof)
    {
        /// Save ASOF key
        saved_block_sample.insert(right_table_keys.safeGetByPosition(right_table_keys.columns() - 1));
    }

    /// Save non key columns
    for (auto & column : sample_block_with_columns_to_add)
    {
        if (auto * col = saved_block_sample.findByName(column.name))
            *col = column;
        else
            saved_block_sample.insert(column);
    }
}

void HashJoin::materializeColumnsFromLeftBlock(Block & block) const
{
    /** If you use FULL or RIGHT JOIN, then the columns from the "left" table must be materialized.
      * Because if they are constants, then in the "not joined" rows, they may have different values
      *  - default values, which can differ from the values of these constants.
      */
    if (kind == JoinKind::Right || kind == JoinKind::Full)
    {
        materializeBlockInplace(block);
    }
}

Block HashJoin::materializeColumnsFromRightBlock(Block block) const
{
    return JoinCommon::materializeColumnsFromRightBlock(std::move(block), savedBlockSample());
}

/// A row store is worth building when reading one output row's payload from it takes at least this
/// many fewer cache misses than reading it column by column.
static constexpr size_t MIN_CACHE_MISSES_SAVED_BY_ROW_STORE = 2;

void HashJoin::initRowStore(const Block & block)
{
    /// Skip initializing if it's already initialized or disabled.
    if (data->row_store_state != RowStoreState::Enabled)
        return;

    /// Skip using row store when the right table rerange optimization could get triggered.
    /// TODO: allow row store when right table could get reranged and build the reranged table
    /// based on the row store instead.
    if (isRightTableRerangeEnabled())
    {
        data->row_store_state = RowStoreState::Disabled;
        return;
    }

    /// Extract columns suitable for row store.
    Block block_to_save = filterColumnsPresentInSampleBlock(block, savedBlockSample());
    const auto & columns = block_to_save.getColumns();
    const auto types = block_to_save.getDataTypes();
    const String asof_key_name = strictness == JoinStrictness::Asof ? rightAsofKeyColumn().name : String{};
    ColumnAccessIndexes access_indexes;
    access_indexes.reserve(columns.size());
    Columns row_store_columns;
    DataTypes row_store_types;
    size_t remaining_columns = 0;
    for (size_t i = 0; i < columns.size(); ++i)
    {
        /// Only add columns that will be later reconstructed in the output.
        const String & saved_name = block_to_save.getByPosition(i).name;
        if (isRowStorageUseful(columns[i]) && (sample_block_with_columns_to_add.has(saved_name) || saved_name == asof_key_name))
        {
            access_indexes.push_back({ColumnAccessIndex::Type::RowStore, row_store_columns.size()});
            row_store_columns.push_back(columns[i]);
            row_store_types.push_back(types[i]);
        }
        else
            access_indexes.push_back({ColumnAccessIndex::Type::Columns, remaining_columns++});
    }

    /// Disable row store if it would be built from a single column.
    if (row_store_columns.size() <= 1)
    {
        data->row_store_state = RowStoreState::Disabled;
        return;
    }

    /// Add each field's offset, size and nullability to the row store access indexes.
    RowDataStore::RowLayoutPtr layout = RowDataStore::computeLayout(row_store_columns, row_store_types);

    if (RowDataStore::cacheMissReduction(*layout) < MIN_CACHE_MISSES_SAVED_BY_ROW_STORE)
    {
        data->row_store_state = RowStoreState::Disabled;
        return;
    }

    for (auto & access_index : access_indexes)
    {
        if (access_index.type != ColumnAccessIndex::Type::RowStore)
            continue;
        const auto & field = (*layout)[access_index.index];
        access_index.field_offset = field.offset;
        access_index.field_size = field.size;
        access_index.is_nullable = field.is_nullable;
    }

    data->row_store_state = RowStoreState::Initialized;
    data->row_store_layout = std::move(layout);
    data->column_access_indexes = std::move(access_indexes);

    LOG_DEBUG(log, "{}Initialized Row store with {} columns", instance_log_id, row_store_columns.size());
}

Block HashJoin::prepareRightBlock(const Block & block, const Block & saved_block_sample_)
{
    Block prepared_block = JoinCommon::materializeColumnsFromRightBlock(block, saved_block_sample_);
    return filterColumnsPresentInSampleBlock(prepared_block, saved_block_sample_);
}

Block HashJoin::prepareRightBlock(const Block & block) const
{
    return prepareRightBlock(block, savedBlockSample());
}

bool HashJoin::addBlockToJoin(const Block & source_block, size_t /* num_rows */, JoinBuildContext context)
{
    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Join data was released");

    const Block block = materializeColumnsFromRightBlock(source_block);
    ScatteredBlock::Selector selector(block.rows());

    /// RowRef::row_no is UInt32 (not size_t) for hash table Cell memory efficiency.
    /// It's possible to split bigger blocks and insert them by parts here. But it would be a dead code.
    if (unlikely(selector.size() > std::numeric_limits<decltype(RowRef::row_no)>::max()))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Too many rows in right table block for HashJoin: {}", selector.size());

    /** We do not allocate memory for stored blocks inside HashJoin, only for hash table.
      * In case when we have all the blocks allocated before the first `addBlockToJoin` call, will already be quite high.
      * In that case memory consumed by stored blocks will be underestimated.
      * The first build thread takes the sample; a later one would include the blocks stored before it.
      */
    if (Int64 unset = 0; !memory_usage_before_adding_blocks.load(std::memory_order_relaxed))
        memory_usage_before_adding_blocks.compare_exchange_strong(unset, JoinCommon::getCurrentQueryMemoryUsage(), std::memory_order_relaxed);

    if (strictness == JoinStrictness::Asof)
    {
        chassert(kind == JoinKind::Left || kind == JoinKind::Inner);

        /// Filter out rows with NULLs in ASOF key, nulls are not joined with anything since they are not comparable
        /// We support only INNER/LEFT ASOF join, so rows with NULLs never return from the right joined table.
        /// So filter them out here not to handle in implementation.
        const auto & asof_key_name = table_join->getOnlyClause().key_names_right.back();
        const auto & asof_column = block.getByName(asof_key_name);

        if (asof_column.type->isNullable())
        {
            /// filter rows with nulls in asof key
            if (const auto * asof_const_column = typeid_cast<const ColumnConst *>(asof_column.column.get()))
            {
                if (asof_const_column->isNullAt(0))
                    return false;
            }
            else
            {
                const auto asof_key_column = prepareRightKeyColumn(asof_column.column);
                const auto & asof_column_nullable = assert_cast<const ColumnNullable &>(*asof_key_column).getNullMapData();

                auto new_selector = ScatteredBlock::Indexes::create();
                auto & new_selector_data = new_selector->getData();

                /// Intersect with the original selector to keep only rows that
                /// both belong to this partition and have a non-NULL ASOF key
                for (size_t r : selector)
                    if (!asof_column_nullable[r])
                        new_selector_data.push_back(r);

                selector = ScatteredBlock::Selector(std::move(new_selector));
            }
        }
    }

    if (context.getNumStreams() > data->workers.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Too many HashJoin build streams: {}, capacity {}", context.getNumStreams(), data->workers.size());
    auto & worker = data->workers[context.getStream()];

    if (storage_join_lock)
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "addBlockToJoin called when HashJoin locked to prevent updates");

    const auto stored = storeBlock(worker, block, std::move(selector));

    if (deferred_build)
    {
        addPendingInsert(worker, stored, block, context.getStream());
        if (!context.joinChecksLimits())
            return true;
        /// The limits are checked once, in `replayDeferredBuild`, against what the maps hold then.
        deferred_build_checks_limits.store(true, std::memory_order_relaxed);
        size_t total_bytes = getTotalByteCount();
        updatePeakBuildBytes(total_bytes);
        shrinkStoredBlocksToFit(total_bytes, /*force_optimize=*/false, context.getStream());
        return true;
    }

    size_t total_rows = 0;
    size_t total_bytes = 0;
    insertStoredBlock(worker, stored, block, context, total_rows, total_bytes);

    if (!context.joinChecksLimits())
        return true;
    shrinkStoredBlocksToFit(total_bytes, /*force_optimize=*/false, context.getStream());
    return table_join->sizeLimits().check(total_rows, total_bytes, "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

bool HashJoin::storesCopies(const Block & block) const
{
    /// A parallel build stores copies of narrow rows, made here on a build stream (`shrinkToFit` copies every column).
    /// The producer then reuses its memory instead of faulting in fresh memory for every block.
    return (data->slots.size() > 1 && !useZeroCopyApproach(block)) || shrink_blocks.load(std::memory_order_relaxed);
}

ColumnPtr HashJoin::prepareRightKeyColumn(const ColumnPtr & column) const
{
    auto prepared_key_column = removeSpecialRepresentations(column->convertToFullColumnIfConst());
    /// Keep the dictionary for the single-LowCardinality-column maps; their key getter needs it.
    if (!isLowCardinalityType(data->type))
        prepared_key_column = prepared_key_column->convertToFullColumnIfLowCardinality();
    return prepared_key_column;
}

ColumnPtrMap HashJoin::prepareRightKeyColumns(const Block & block) const
{
    const auto & right_key_names = table_join->getAllNames(JoinTableSide::Right);
    ColumnPtrMap all_key_columns(right_key_names.size());
    for (const auto & column_name : right_key_names)
        all_key_columns[column_name] = prepareRightKeyColumn(block.getByName(column_name).column);
    return all_key_columns;
}

std::optional<size_t> HashJoin::storedColumnIndex(const String & name) const
{
    if (!data->sample_block.has(name))
        return {};
    const size_t position = data->sample_block.getPositionByName(name);
    if (data->column_access_indexes.empty())
        return position;
    const auto & access_index = data->column_access_indexes[position];
    if (access_index.type == ColumnAccessIndex::Type::RowStore)
        return {};
    return access_index.index;
}

/// Until its inserts, a deferred build holds the stored blocks. It defers only when that is no more than the streaming
/// build holds. So the stored blocks must have the keys, which an `INNER` or `LEFT` join without a spill budget does not
/// store. And the map must keep every row: with one row per key, the streaming build drops a block that adds no new key.
/// Only such a map reads `any_take_last_row`, which the deferred inserts could not follow: they lose the block order.
bool HashJoin::deferredBuildHoldsNoMore() const
{
    for (const auto & name : table_join->getAllNames(JoinTableSide::Right))
        if (!storedColumnIndex(name))
            return false;

    bool keeps_every_row = false;
    joinDispatch(
        kind,
        strictness,
        data->maps[0],
        getMapsKind(),
        [&](auto, auto, const auto & maps)
        {
            using Maps = std::decay_t<decltype(maps)>;
            keeps_every_row = std::is_same_v<Maps, MapsAll> || std::is_same_v<Maps, MapsAsof>;
        });
    return keeps_every_row;
}

void HashJoin::addPendingInsert(WorkerStoredData & worker, StoredBlocksList::iterator stored, const Block & block, size_t stream)
{
    const auto all_key_columns = prepareRightKeyColumns(block);
    auto & counts = deferred_build->streamCounts(stream);
    const auto maps_kind = getMapsKind();
    const auto & onexprs = table_join->getClauses();
    for (size_t onexpr_idx = 0; onexpr_idx < onexprs.size(); ++onexpr_idx)
    {
        ColumnRawPtrs key_columns;
        for (const auto & name : onexprs[onexpr_idx].key_names_right)
            key_columns.push_back(all_key_columns.at(name).get());

        ConstNullMapPtr null_map{};
        ColumnPtr null_map_holder = extractNestedColumnsAndNullMap(key_columns, null_map);
        const auto join_mask_col = JoinCommon::getColumnAsMask(block, onexprs[onexpr_idx].condColumnNames().second);

        SkippedRows skipped;
        joinDispatch(
            kind,
            strictness,
            data->maps[onexpr_idx],
            maps_kind,
            [&](auto, auto, const auto & maps)
            {
                skipped = countInsertableKeys(
                    data->type,
                    maps,
                    key_columns,
                    key_sizes[onexpr_idx],
                    stored->selector,
                    null_map,
                    join_mask_col,
                    counts.clauses[onexpr_idx]);
            });

        /// `insertStoredBlock` keeps up to two maps, one byte per row, of the rows a `RIGHT` or `FULL` join emits unjoined:
        /// one for the NULL keys and one for the rows that fail the ON condition. Count each map the block needs.
        if (isRightOrFull(kind) && !needUsedFlagsForPerRightTableRow(table_join))
            counts.null_map_bytes.fetch_add(
                (skipped.null_key + skipped.filtered) * stored->selector.size(), std::memory_order_relaxed);
    }
    deferred_build->addStoredRows(stored->selector.size());

    /// The keys are in the stored block (`deferredBuildHoldsNoMore`); the ON condition columns are kept as they arrived.
    PendingInsert pending{.stored = stored, .columns = {}, .allocated_bytes = 0};
    const bool copy = storesCopies(block);
    auto keep = [&](const String & name)
    {
        if (name.empty() || pending.columns.has(name))
            return;
        auto column = block.getByName(name);
        /// The same copy as `storeBlock` makes, so that the producer's columns are not kept alive.
        if (copy)
            column.column = column.column->cloneResized(column.column->size());
        pending.columns.insert(std::move(column));
    };
    for (const auto & clause : onexprs)
        keep(clause.condColumnNames().second);

    pending.allocated_bytes = pending.columns.allocatedBytes();
    data->addBytes(data->pending_allocated_size, pending.allocated_bytes);
    worker.pending_inserts.push_back(std::move(pending));
}

Block HashJoin::pendingInsertBlock(const PendingInsert & pending) const
{
    Block block = pending.columns;
    for (const auto & name : table_join->getAllNames(JoinTableSide::Right))
    {
        if (block.has(name))
            continue;
        /// Read now, not when the block was stored: `shrinkStoredBlocksToFit` may have replaced the column since.
        auto column = data->sample_block.getByName(name);
        column.column = pending.stored->columns[*storedColumnIndex(name)];
        block.insert(std::move(column));
    }
    return block;
}

HashJoin::StoredBlocksList::iterator HashJoin::storeBlock(WorkerStoredData & worker, const Block & block, ScatteredBlock::Selector selector)
{
    Block block_to_save = filterColumnsPresentInSampleBlock(block, savedBlockSample());
    if (storesCopies(block))
        block_to_save = block_to_save.shrinkToFit();

    assertBlocksHaveEqualStructureAllowReplicated(data->sample_block, block_to_save, "joined block");

    Columns columns;
    RowDataStorePtr row_store;
    if (data->row_store_state == RowStoreState::Initialized)
    {
        auto [row_store_columns, remaining_columns] = extractRowStoreColumns(block_to_save, data->column_access_indexes);
        columns = std::move(remaining_columns);
        row_store = RowDataStore::create(data->row_store_layout, row_store_columns);
    }
    else
        columns = block_to_save.getColumns();

    doDebugAsserts();
    /// Register the block and account for it while a local list still owns it: `splice` cannot throw,
    /// so the worker's list, `data->allocated_size` and `data->rows_to_join` always describe the same set
    /// of stored blocks. Same ordering as `tryRerangeRightTableDataImpl`; a list node keeps its address.
    StoredBlocksList new_block;
    const size_t rows = selector.size();
    new_block.emplace_back(std::move(columns), std::move(selector), std::move(row_store));
    auto & stored_columns = new_block.back();
    stored_columns.block_no = data->stored_columns_index->add(&stored_columns);
    data->addBytes(data->allocated_size, stored_columns.allocatedBytes());
    data->rows_to_join.fetch_add(rows, std::memory_order_relaxed);
    const auto stored = new_block.begin();
    worker.columns.splice(worker.columns.end(), new_block);
    doDebugAsserts();
    return stored;
}

void HashJoin::insertStoredBlock(
    WorkerStoredData & worker,
    StoredBlocksList::iterator stored,
    const Block & block,
    JoinBuildContext context,
    size_t & total_rows,
    size_t & total_bytes)
{
    const auto all_key_columns = prepareRightKeyColumns(block);
    const auto maps_kind = getMapsKind();
    auto * stored_columns = &*stored;
    const size_t rows = stored_columns->selector.size();

    bool flag_per_row = needUsedFlagsForPerRightTableRow(table_join);
    const auto & onexprs = table_join->getClauses();

    /// NullMapHolder stores a raw pointer to stored_columns. If any clause stores a nullmap
    /// referencing this block we must not remove the block later
    bool nullmap_stored_for_block = false;

    /// The per-row used flags of the stored block are initialized on the first clause only:
    /// their content does not depend on the inserts, and JoinUsedFlags expects one entry per block.
    bool per_row_flags_initialized = false;

    for (size_t onexpr_idx = 0; onexpr_idx < onexprs.size(); ++onexpr_idx)
    {
        ColumnRawPtrs key_columns;
        for (const auto & name : onexprs[onexpr_idx].key_names_right)
            key_columns.push_back(all_key_columns.at(name).get());

        /// We will insert to the map only keys, where all components are not NULL.
        ConstNullMapPtr null_map{};
        ColumnPtr null_map_holder = extractNestedColumnsAndNullMap(key_columns, null_map);

        /// If RIGHT or FULL save blocks with nulls for NotJoinedBlocks
        UInt8 save_nullmap = 0;
        if (isRightOrFull(kind) && null_map)
        {
            /// Only check rows belonging to this partition's selector
            for (size_t r : stored_columns->selector)
            {
                if ((*null_map)[r])
                {
                    save_nullmap = 1;
                    break;
                }
            }
        }

        auto join_mask_col = JoinCommon::getColumnAsMask(block, onexprs[onexpr_idx].condColumnNames().second);
        /// Save blocks that do not hold conditions in ON section
        ColumnUInt8::MutablePtr not_joined_map = nullptr;
        bool has_right_not_joined = false;
        if (!flag_per_row && isRightOrFull(kind) && join_mask_col.getKind() != JoinCommon::JoinMask::Kind::AllTrue)
        {
            ///  - build mask in the source block row space
            ///  - set bits only for rows that belong to THIS slot (by selector)
            not_joined_map = ColumnUInt8::create(block.rows(), static_cast<UInt8>(0));
            const auto & sel = stored_columns->selector;

            auto mark_if_needed = [&](size_t row)
            {
                if (!join_mask_col.isRowFiltered(row))
                    return; // ON condition passed -> not "non-joined"
                if (save_nullmap && (*null_map)[row])
                    return; // already covered by null-keys map
                not_joined_map->getData()[row] = 1;
                has_right_not_joined = true;
            };

            for (size_t r : sel)
                mark_if_needed(r);
        }

        bool is_inserted = false;

        joinDispatch(
            kind,
            strictness,
            data->maps[onexpr_idx],
            maps_kind,
            [&](auto kind_, auto strictness_, auto & map)
            {
                std::vector<ScatteredBlock::Selector> scattered;
                std::span<const ScatteredBlock::Selector> per_slot(&stored_columns->selector, 1);
                if (data->slots.size() > 1)
                {
                    scattered = scatterBlockBySlot(
                        data->type, map, key_columns, key_sizes[onexpr_idx], stored_columns->selector, data->slots.size());
                    per_slot = scattered;
                }

                const BuildResult result = HashJoinMethods<kind_, strictness_, std::decay_t<decltype(map)>>::insertFromBlockImpl(
                    *this,
                    data->type,
                    map,
                    onexpr_idx,
                    per_slot,
                    key_columns,
                    key_sizes[onexpr_idx],
                    stored_columns->block_no,
                    null_map,
                    join_mask_col);
                is_inserted = result.is_inserted;
                if (!result.all_values_unique)
                    all_values_unique.store(false, std::memory_order_relaxed);

                if (flag_per_row && !per_row_flags_initialized)
                {
                    used_flags->reinit<kind_, strictness_, mapsKindOf<decltype(map)>()>(
                        context.getStream(), stored_columns->block_no, stored_columns->blockRows());
                    per_row_flags_initialized = true;
                }
            });

        if (!flag_per_row && save_nullmap && is_inserted)
        {
            auto & h = worker.nullmaps.emplace_back(stored_columns, null_map_holder);
            data->addBytes(data->nullmaps_allocated_size, h.allocatedBytes());
            nullmap_stored_for_block = true;
        }

        if (!flag_per_row && not_joined_map && has_right_not_joined)
        {
            auto & h = worker.nullmaps.emplace_back(stored_columns, std::move(not_joined_map));
            data->addBytes(data->nullmaps_allocated_size, h.allocatedBytes());
            nullmap_stored_for_block = true;
        }

        /// Whether anything that outlives the build phase still points into the block. Per-row used
        /// flags are keyed by the stored block, so they keep it alive - except on a set map, which is
        /// never `flagged` (see `MapGetter`), so there are no such flags to begin with.
        const bool block_is_referenced
            = is_inserted || nullmap_stored_for_block || (flag_per_row && maps_kind != JoinMapsKind::Set);
        /// Every clause reads its keys out of the block before it goes. Only a set map gets here with
        /// more than one clause: several clauses always mean `flag_per_row`.
        const bool last_clause = onexpr_idx + 1 == onexprs.size();

        if (!block_is_referenced && last_clause)
        {
            doDebugAsserts();
            LOG_TRACE(log, "Skipping inserting block with {} rows", rows);
            data->subBytes(data->allocated_size, stored_columns->allocatedBytes());
            /// A set map references no block at all, so every block is dropped here even though its
            /// rows did take part in the join and must stay counted.
            if (maps_kind != JoinMapsKind::Set)
                data->rows_to_join.fetch_sub(rows, std::memory_order_relaxed);
            /// Nothing was inserted, so no refs to this block exist; null the index entry so
            /// that a stale ref trips the chassert in `StoredColumnsIndex::at` in debug builds
            /// (and dereferences nullptr deterministically in release builds) instead of
            /// silently reading freed memory.
            data->stored_columns_index->clearEntry(stored_columns->block_no);
            worker.columns.erase(stored);
            doDebugAsserts();
        }

        if (context.joinChecksLimits())
        {
            total_rows = getTotalRowCount();
            total_bytes = getTotalByteCount();
            /// `total_bytes` is the size before the caller shrinks the stored blocks, so this captures the
            /// build high-water mark for free on the path where a shrink can lower it.
            updatePeakBuildBytes(total_bytes);
        }
    }
}

void HashJoin::shrinkStoredBlocksToFit(size_t & total_bytes_in_join, bool force_optimize, size_t worker_id)
{
    /// The CAS decides once for the whole join; each thread then rewrites only its own list.
    const auto max_total_bytes_in_join = table_join->sizeLimits().max_bytes;

    const Int64 current_memory_usage = JoinCommon::getCurrentQueryMemoryUsage();
    const Int64 memory_usage_before = memory_usage_before_adding_blocks.load(std::memory_order_relaxed);
    const Int64 query_memory_usage_delta = current_memory_usage - memory_usage_before;
    const Int64 max_total_bytes_for_query = memory_usage_before ? table_join->getMaxMemoryUsage() : 0;

    if (!force_optimize)
    {
        if (shrink_blocks.load(std::memory_order_relaxed))
        {
            /// Another thread already decided to shrink; catch this worker up.
            shrinkWorkerStoredBlocks(data->workers[worker_id]);
            total_bytes_in_join = getTotalByteCount();
            return;
        }

        /** If accounted data size is more than half of `max_bytes_in_join`
        * or query memory consumption growth from the beginning of adding blocks (estimation of memory consumed by join using memory tracker)
        * is bigger than half of all memory available for query,
        * then shrink stored blocks to fit.
        */
        const bool should_shrink = (max_total_bytes_in_join && total_bytes_in_join > max_total_bytes_in_join / 2)
            || (max_total_bytes_for_query && query_memory_usage_delta > max_total_bytes_for_query / 2);
        if (!should_shrink)
            return;

        bool expected = false;
        if (!shrink_blocks.compare_exchange_strong(expected, true, std::memory_order_relaxed))
        {
            shrinkWorkerStoredBlocks(data->workers[worker_id]);
            total_bytes_in_join = getTotalByteCount();
            return;
        }
    }
    else
        data->workers[worker_id].shrink_done = false; /// Only `StorageJoin` forces, and its join has one worker.

    LOG_DEBUG(
        log,
        "Shrinking stored blocks, memory consumption is {} {} calculated by join, {} {} by memory tracker",
        ReadableSize(total_bytes_in_join),
        max_total_bytes_in_join ? fmt::format("/ {}", ReadableSize(max_total_bytes_in_join)) : "",
        ReadableSize(query_memory_usage_delta),
        max_total_bytes_for_query ? fmt::format("/ {}", ReadableSize(max_total_bytes_for_query)) : "");

    /// The other workers catch up on their next call.
    shrinkWorkerStoredBlocks(data->workers[worker_id]);
    /// A forced shrink leaves the worker not done, so a shrink that the limits decide later still rewrites the blocks stored until then.
    if (force_optimize)
        data->workers[worker_id].shrink_done = false;

    auto new_total_bytes_in_join = getTotalByteCount();
    Int64 new_current_memory_usage = JoinCommon::getCurrentQueryMemoryUsage();

    LOG_DEBUG(
        log,
        "Shrunk stored blocks {} freed ({} by memory tracker), new memory consumption is {} ({} by memory tracker)",
        ReadableSize(total_bytes_in_join - new_total_bytes_in_join),
        ReadableSize(current_memory_usage - new_current_memory_usage),
        ReadableSize(new_total_bytes_in_join),
        ReadableSize(new_current_memory_usage));

    total_bytes_in_join = new_total_bytes_in_join;
}

void HashJoin::shrinkWorkerStoredBlocks(WorkerStoredData & worker)
{
    if (worker.shrink_done)
        return;

    /// Each cloneResized below replaces a stored column object in place, so any emit table built by a
    /// prior query (a persistent StorageJoin builds one per SELECT, then OPTIMIZE/insert runs this) is
    /// left with dangling `const IColumn *`. Bump the generation on every exit - including the exception
    /// paths, where some columns were already replaced - so the next probe rebuilds it against the new
    /// columns. invalidateEmitTable only takes a mutex and increments a counter, so it is unwind-safe.
    SCOPE_EXIT({ data->stored_columns_index->invalidateEmitTable(); });

    for (auto & stored_columns : worker.columns)
    {
        doDebugAsserts();

        size_t old_size = stored_columns.allocatedBytes();

        try
        {
            for (auto & column : stored_columns.columns)
                column = column->cloneResized(column->size());

            /// `cloneResized` replaces each column with a new object.
            /// The raw pointers in `replicated_columns` pointed at the old objects and are now dangling.
            stored_columns.rebuildReplicatedColumns();
        }
        catch (...)
        {
            /// If cloneResized throws (e.g., due to memory allocation failure or fault injection),
            /// some columns may have already been replaced with shrunk copies while
            /// data->allocated_size still reflects the old sizes. Recalculate to stay consistent.
            /// Also rebuild replicated_columns for columns that were already replaced, to avoid dangling pointers.
            stored_columns.rebuildReplicatedColumns();
            size_t partial_new_size = stored_columns.allocatedBytes();
            if (old_size >= partial_new_size)
                data->subBytes(data->allocated_size, old_size - partial_new_size);
            else
                data->addBytes(data->allocated_size, partial_new_size - old_size);
            throw;
        }

        size_t new_size = stored_columns.allocatedBytes();

        if (old_size >= new_size)
        {
            if (data->allocated_size.load(std::memory_order_relaxed) < old_size - new_size)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Blocks allocated size value is broken: "
                    "blocks_allocated_size = {}, old_size = {}, new_size = {}",
                    data->allocated_size.load(std::memory_order_relaxed),
                    old_size,
                    new_size);

            data->subBytes(data->allocated_size, old_size - new_size);
        }
        else
            /// Sometimes after clone resized block can be bigger than original
            data->addBytes(data->allocated_size, new_size - old_size);

        doDebugAsserts();
    }

    worker.shrink_done = true;
}

DataTypePtr HashJoin::joinGetCheckAndGetReturnType(const DataTypes & data_types, const String & column_name, bool or_null) const
{
    size_t num_keys = data_types.size();
    if (right_table_keys.columns() != num_keys)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of join_keys and number of right table key columns for function joinGet{} don't match: passed {}, should be equal to {}",
            toString(or_null ? "OrNull" : ""),
            toString(num_keys),
            toString(right_table_keys.columns()));

    for (size_t i = 0; i < num_keys; ++i)
    {
        const auto & left_type_origin = data_types[i];
        const auto & [c2, right_type_origin, right_name] = right_table_keys.safeGetByPosition(i);
        auto left_type = removeNullable(recursiveRemoveLowCardinality(left_type_origin));
        auto right_type = removeNullable(recursiveRemoveLowCardinality(right_type_origin));
        if (!left_type->equals(*right_type))
            throw Exception(
                ErrorCodes::TYPE_MISMATCH,
                "Type mismatch in joinGet key {}: "
                "found type {}, while the needed type is {}",
                i,
                left_type->getName(),
                right_type->getName());
    }

    if (!sample_block_with_columns_to_add.has(column_name))
        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "StorageJoin doesn't contain column {}", column_name);

    auto elem = sample_block_with_columns_to_add.getByName(column_name);
    if (or_null && JoinCommon::canBecomeNullable(elem.type))
        elem.type = makeNullable(elem.type);
    return elem.type;
}

/// TODO: return multiple columns as named tuple
/// TODO: return array of values when strictness == JoinStrictness::All
ColumnWithTypeAndName HashJoin::joinGet(const Block & block, const Block & block_with_columns_to_add) const
{
    bool is_valid = (strictness == JoinStrictness::Any || strictness == JoinStrictness::RightAny) && kind == JoinKind::Left;
    if (!is_valid)
        throw Exception(ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN, "joinGet only supports StorageJoin of type Left Any");
    const auto & key_names_right = table_join->getOnlyClause().key_names_right;

    /// Assemble the key block with correct names.
    Block keys;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto key = block.getByPosition(i);
        key.name = key_names_right[i];
        keys.insert(std::move(key));
    }

    static_assert(
        !MapGetter<JoinKind::Left, JoinStrictness::Any, JoinMapsKind::Default>::flagged,
        "joinGet are not protected from hash table changes between block processing");

    std::vector<const MapsOne *> maps_vector;
    maps_vector.push_back(&std::get<MapsOne>(data->maps[0]));
    auto res = HashJoinMethods<JoinKind::Left, JoinStrictness::Any, MapsOne>::joinBlockImpl(
        *this, std::move(keys), block_with_columns_to_add, maps_vector, /* is_join_get = */ true)->next();
    chassert(res.is_last);
    return res.block.getByPosition(res.block.columns() - 1);
}

void HashJoin::checkTypesOfKeys(const Block & block) const
{
    for (const auto & onexpr : table_join->getClauses())
    {
        JoinCommon::checkTypesOfKeys(block, onexpr.key_names_left, right_table_keys, onexpr.key_names_right);
    }
}

/// Keeps selector-backed probe continuations inside HashJoin so an unprocessed left suffix
/// is not materialized and resubmitted while preserving the joined-block size limit.
class HashJoin::ContinuationResult final : public IJoinResult
{
public:
    ContinuationResult(HashJoin & join_, JoinResultPtr current_result_)
        : join(join_)
        , current_result(std::move(current_result_))
    {
    }

    JoinResultBlock next() override
    {
        auto result = current_result->next();

        if (result.is_last)
        {
            addMatchedRightRows(matched_right_rows, current_result->getMatchedRightRows());

            if (result.next_block)
                current_result = join.runJoinDispatch(std::move(*result.next_block));
            else
                current_result.reset();
        }

        return {std::move(result.block), nullptr, !current_result};
    }

    std::optional<size_t> getMatchedRightRows() const override
    {
        return matched_right_rows;
    }

private:
    HashJoin & join;
    JoinResultPtr current_result;
    std::optional<size_t> matched_right_rows = 0;
};

JoinResultPtr HashJoin::joinBlock(Block block)
{
    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot join after data has been released");
    if (replay_pending)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot join before the keys of a deferred build are in the maps");

    for (const auto & onexpr : table_join->getClauses())
    {
        auto cond_column_name = onexpr.condColumnNames();
        JoinCommon::checkTypesOfKeys(
            block, onexpr.key_names_left, cond_column_name.first, right_sample_block, onexpr.key_names_right, cond_column_name.second);
    }

    materializeColumnsFromLeftBlock(block);

    auto result = runJoinDispatch(ScatteredBlock(std::move(block)));
    return std::make_unique<ContinuationResult>(*this, std::move(result));
}

JoinResultPtr HashJoin::runJoinDispatch(ScatteredBlock block)
{
    std::vector<const std::decay_t<decltype(data->maps[0])> *> maps_vector;
    maps_vector.reserve(table_join->getClauses().size());
    for (size_t i = 0; i < table_join->getClauses().size(); ++i)
        maps_vector.push_back(&data->maps[i]);

    const auto maps_kind = getMapsKind();
    JoinResultPtr res;
    const bool joined = joinDispatch(
        kind,
        strictness,
        maps_vector,
        maps_kind,
        [&](auto kind_, auto strictness_, auto & maps_vector_)
        {
            if constexpr (std::is_same_v<std::decay_t<decltype(maps_vector_)>, std::vector<const MapsAll *>>)
            {
                res = HashJoinMethods<kind_, strictness_, MapsAll>::joinBlockImpl(
                    *this, std::move(block), sample_block_with_columns_to_add, maps_vector_);
            }
            else if constexpr (std::is_same_v<std::decay_t<decltype(maps_vector_)>, std::vector<const MapsOne *>>)
            {
                res = HashJoinMethods<kind_, strictness_, MapsOne>::joinBlockImpl(
                    *this, std::move(block), sample_block_with_columns_to_add, maps_vector_);
            }
            else if constexpr (std::is_same_v<std::decay_t<decltype(maps_vector_)>, std::vector<const MapsAsof *>>)
            {
                res = HashJoinMethods<kind_, strictness_, MapsAsof>::joinBlockImpl(
                    *this, std::move(block), sample_block_with_columns_to_add, maps_vector_);
            }
            else if constexpr (std::is_same_v<std::decay_t<decltype(maps_vector_)>, std::vector<const MapsSet *>>)
            {
                res = HashJoinMethods<kind_, strictness_, MapsSet>::joinBlockImpl(
                    *this, std::move(block), sample_block_with_columns_to_add, maps_vector_);
            }
            else
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown maps type");
            }
        });

    if (!joined)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong JOIN combination: {} {}", strictness, kind);

    return res;
}

HashJoin::~HashJoin()
{
    if (!data)
    {
        LOG_TEST(log, "{}Join data has been already released", instance_log_id);
        return;
    }

    try
    {
        if (build_phase_finished)
        {
            /// Like `ConcurrentHashJoin`, a parallel build of a fixed-key map leaves no entry: it has nothing to preallocate.
            if (stats_collecting_params.build.isCollectionAndUseEnabled() && data->type != Type::two_level_key8
                && data->type != Type::two_level_key16)
            {
                if (const auto ht_size = getTotalRowCount())
                    getHashTablesStatistics<HashJoinEntry>().update(
                        {.ht_size = ht_size, .source_rows = data->rows_to_join}, stats_collecting_params.build);
            }

            if (stats_collecting_params.match.isCollectionAndUseEnabled() && probe_phase_finished && hash_table_matches.has_value())
                getHashTablesStatistics<HashJoinMatchEntry>().update({.matches = *hash_table_matches}, stats_collecting_params.match);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    LOG_TEST(
        log,
        "{}Join data is being destroyed, {} bytes and {} rows in hash table",
        instance_log_id,
        getTotalByteCount(),
        getTotalRowCount());
}

HashJoin::RightTableData::~RightTableData()
{
    /// Freeing takes time per heap allocation more than per byte, and starting the pool takes a fixed time. So the pool
    /// frees only a table of many objects: stored columns, null maps, `ASOF` lookups. Arena chunks double, so arenas are not counted.
    static constexpr size_t PARALLEL_DESTROY_THRESHOLD_OBJECTS = 2000;

    try
    {
        /// An `ASOF` cell owns a heap lookup, so clearing the buckets of a large `ASOF` map is work worth spreading.
        /// A `PartitionedFixedHashTable` has one buffer for all its buckets, and a map with one bucket has nothing to split.
        auto for_each_asof_map_with_bucket_buffers = [this](auto && f)
        {
            for (auto & maps_variant : maps)
                if (auto * asof_maps = std::get_if<MapsAsof>(&maps_variant))
                    asof_maps->visit(type, [&]<typename Table>(std::shared_ptr<Table> & map)
                    {
                        if constexpr (!is_partitioned_fixed_table<Table> && Table::NUM_BUCKETS > 1)
                            if (map)
                                f(*map);
                    });
        };

        /// A cancelled build, or a deferred build whose inserts threw, never reached `finishBuildPhase`, so its lists may
        /// still be per worker.
        spliceWorkerLists();
        auto & stored = workers.front();
        const size_t num_blocks = stored.columns.size();
        size_t num_asof_buckets = 0;
        size_t num_asof_keys = 0;
        for_each_asof_map_with_bucket_buffers([&]<typename Table>(Table & map)
        {
            num_asof_buckets += Table::NUM_BUCKETS;
            num_asof_keys += map.size();
        });
        /// Every stored block has the columns of `sample_block`, though a row store keeps several of them in one buffer.
        const size_t num_objects = num_blocks * sample_block.columns() + stored.nullmaps.size() + num_asof_keys;
        /// The planner gives one worker to a join with the serial layout or with one thread, so its table is freed serially.
        const size_t num_threads = std::min(workers.size(), num_blocks + slots.numArenasWithMemory() + num_asof_buckets);
        if (num_threads <= 1 || num_objects < PARALLEL_DESTROY_THRESHOLD_OBJECTS)
            return;

        /// The jobs refer to these containers. The pool comes after them, so its destructor waits for the jobs first.
        std::vector<StoredBlocksList> sublists(std::min(num_threads, num_blocks));
        for (size_t block = 0; !stored.columns.empty(); ++block)
        {
            auto & sublist = sublists[block % sublists.size()];
            sublist.splice(sublist.end(), stored.columns, stored.columns.begin());
        }
        NullmapList nullmaps = std::move(stored.nullmaps);
        std::vector<std::unique_ptr<Arena>> arenas = slots.releaseArenas();

        ThreadPool pool(
            CurrentMetrics::HashJoinDestroyThreads,
            CurrentMetrics::HashJoinDestroyThreadsActive,
            CurrentMetrics::HashJoinDestroyThreadsScheduled,
            num_threads);

        auto schedule = [&pool, thread_group = CurrentThread::getGroup()](auto job)
        {
            pool.scheduleOrThrowOnError(
                [job, thread_group]
                {
                    ThreadGroupSwitcher switcher(thread_group, ThreadName::HASH_JOIN_DTOR);
                    job();
                });
        };
        auto schedule_destroy = [&schedule](auto & container) { schedule([&container] { auto destroyed = std::move(container); }); };

        for (auto & sublist : sublists)
            schedule_destroy(sublist);
        if (!nullmaps.empty())
            schedule_destroy(nullmaps);
        for (auto & arena : arenas)
            schedule_destroy(arena);
        for_each_asof_map_with_bucket_buffers([&]<typename Table>(Table & map)
        {
            for (size_t first_bucket = 0; first_bucket < num_threads; ++first_bucket)
                schedule([&map, first_bucket, num_threads]
                {
                    for (size_t bucket = first_bucket; bucket < Table::NUM_BUCKETS; bucket += num_threads)
                        map.impls[bucket].clearAndShrink();
                });
        });
        pool.wait();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void HashJoin::RightTableData::spliceWorkerLists()
{
    auto & stored = workers.front();
    for (auto & worker : workers | std::views::drop(1))
    {
        stored.columns.splice(stored.columns.end(), worker.columns);
        std::ranges::move(worker.nullmaps, std::back_inserter(stored.nullmaps));
        worker.nullmaps.clear();
    }
}

namespace
{

/// Whether every key in the cells that stream `bucket_idx` of `num_buckets` walks has its used flag set.
/// A two-level map numbers the cells of bucket `b` from `bucket_cells_prefix[b] + 1`, and its zero key has offset 0.
/// A one-bucket map is checked as a whole, and so is a `PartitionedFixedHashTable`, which keeps no count per bucket.
template <typename Map>
bool allKeysUsed(const Map & map, const JoinStuff::JoinUsedFlags & used_flags, size_t bucket_idx, size_t num_buckets)
{
    if (used_flags.per_offset_flags.size() < map.getBufferSizeInCells() + 1)
        return false;

    if constexpr (requires { map.bucket_cells_prefix; })
    {
        if constexpr (Map::NUM_BUCKETS > 1)
        {
            if (map.bucket_cells_prefix.size() != Map::NUM_BUCKETS)
                return false;
            for (size_t bucket = bucket_idx; bucket < Map::NUM_BUCKETS; bucket += num_buckets)
            {
                const auto & impl = map.impls[bucket];
                const size_t begin = map.bucket_cells_prefix[bucket] + 1;
                if (countBytesInFilter(used_flags.per_offset_flags.data(), begin, begin + impl.getBufferSizeInCells())
                    != impl.size() - impl.hasZero())
                    return false;
                if (impl.hasZero() && !used_flags.getUsedSafe(0))
                    return false;
            }
            return true;
        }
    }
    return countBytesInFilter(used_flags.per_offset_flags.data(), 0, map.getBufferSizeInCells() + 1) == map.size();
}

}

/// Appends one hash map cell's not-joined rows as a flat run of encoded ref words. Returns the rows
/// appended.
template <typename Mapped>
size_t collectNonJoinedRefWords([[maybe_unused]] const Mapped & mapped, [[maybe_unused]] PaddedPODArray<UInt64> & words)
{
    if constexpr (std::is_same_v<Mapped, AsofRowRefs>)
    {
        return 0;
    }
    else if constexpr (std::is_same_v<Mapped, RowRef>)
    {
        words.push_back(mapped.encode());
        return 1;
    }
    else
    {
        size_t rows = 0;
        for (auto it = mapped.begin(); it.ok(); ++it, ++rows)
            words.push_back(*it);
        return rows;
    }
}

/// Stream from not joined earlier rows of the right table.
/// Based on:
///   - map offsetInternal saved in used_flags for single disjuncts
///   - flags in BlockWithFlags for multiple disjuncts
///
/// For parallel iteration over two-level hash maps, bucket_idx and num_buckets
/// can be specified to process only buckets where (bucket % num_buckets == bucket_idx)
class NotJoinedHash final : public NotJoinedBlocks::RightColumnsFiller
{
public:
    NotJoinedHash(const HashJoin & parent_, UInt64 max_block_size_, bool flag_per_row_)
        : NotJoinedHash(parent_, max_block_size_, flag_per_row_, 0, 1)
    {
    }

    NotJoinedHash(const HashJoin & parent_, UInt64 max_block_size_, bool flag_per_row_,
                  size_t bucket_idx_, size_t num_buckets_)
        : parent(parent_)
        , max_block_size(max_block_size_)
        , flag_per_row(flag_per_row_)
        , bucket_idx(bucket_idx_)
        , num_buckets(num_buckets_)
    {
        if (parent.data == nullptr)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot join after data has been released");

        const Block & saved_block_sample = parent.savedBlockSample();

        NamesAndTypes type_name;
        type_name.reserve(saved_block_sample.columns());
        for (const auto & column : saved_block_sample)
            type_name.emplace_back(column.name, column.type);

        std::vector<size_t> positions(saved_block_sample.columns());
        std::iota(positions.begin(), positions.end(), 0);
        EmitPlan plan = planJoinEmit(*parent.data, positions, type_name, /*with_gather=*/true);
        emit_gather = std::move(plan.gather);
        emit_row_store_row_length = plan.row_store_row_length;
    }

    Block getEmptyBlock() override { return parent.savedBlockSample().cloneEmpty(); }

    size_t fillColumns(MutableColumns & columns_right) override
    {
        size_t rows_added = 0;
        auto fill_callback = [&](auto, auto, auto & map)
        {
            /// Only RIGHT and FULL joins have non-joined rows, and those never run on a set map.
            if constexpr (SetJoinMaps<decltype(map)>)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Non-joined right rows cannot be produced from a set map");
            else
                rows_added = fillColumnsFromMap(map, columns_right);
        };

        const auto maps_kind = parent.getMapsKind();
        if (!joinDispatch(parent.kind, parent.strictness, parent.data->maps.front(), maps_kind, fill_callback))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Unknown JOIN strictness '{}' (must be one of: ANY, ALL, ASOF)", parent.strictness);

        if (!flag_per_row)
            fillNullsFromBlocks(columns_right, rows_added);

        if (auto * stats = parent.matched_rows_stats.get())
            stats->collectNonJoined(rows_added);

        return rows_added;
    }

private:
    const HashJoin & parent;
    UInt64 max_block_size;
    bool flag_per_row;
    size_t bucket_idx;
    size_t num_buckets;

    std::any position;
    std::optional<HashJoin::NullmapList::const_iterator> nulls_position;
    std::optional<HashJoin::StoredBlocksList::const_iterator> used_position;

    std::vector<GatherColumn> emit_gather;
    std::optional<size_t> emit_row_store_row_length;

    bool isBucketInRange(size_t bucket) const
    {
        return num_buckets <= 1 || (bucket % num_buckets) == bucket_idx;
    }

    bool isBlockInRange(size_t block_no) const
    {
        return num_buckets <= 1 || (block_no % num_buckets) == bucket_idx;
    }

    template <typename Maps>
    size_t fillColumnsFromMap(const Maps & maps, MutableColumns & columns_right)
    {
        switch (parent.data->type)
        {
#define M(TYPE) \
    case HashJoin::Type::TYPE: return fillColumns(*maps.TYPE, columns_right);
            APPLY_FOR_JOIN_VARIANTS(M)
#undef M
        }
        UNREACHABLE();
    }

    /// Flat here: a not-joined row is always one inline ref, never a list and never a default.
    void emitOutputs(MutableColumns & columns_right, const PaddedPODArray<UInt64> & words) const
    {
        const RefWordSelection selection{
            .begin = words.data(), .end = words.data() + words.size(), .rows = words.size(), .shape = RefWordShape::Flat};
        gatherJoinOutputColumns(columns_right, emit_gather, selection, emit_row_store_row_length);
    }

    template <typename Map>
    size_t fillColumns(const Map & map, MutableColumns & columns_right)
    {
        size_t rows_added = 0;

        PaddedPODArray<UInt64> words;
        words.reserve(max_block_size);

        if (flag_per_row)
        {
            /// parent.data->columns is not partitioned by hash bucket, so distribute the stored
            /// right blocks across streams by their globally unique block_no instead
            if (!used_position.has_value())
                used_position = parent.data->workers.front().columns.begin();

            auto end = parent.data->workers.front().columns.end();

            for (auto & it = *used_position; it != end && rows_added < max_block_size; ++it)
            {
                const auto & mapped_block = *it;
                if (!isBlockInRange(mapped_block.block_no))
                    continue;

                size_t rows = mapped_block.blockRows();

                for (size_t row = 0; row < rows; ++row)
                {
                    if (!parent.isUsed(mapped_block.block_no, row))
                    {
                        ++rows_added;
                        words.push_back(RowRef(mapped_block.block_no, row).encode());
                    }
                }
            }
        }
        else
        {
            using Mapped = typename Map::mapped_type;
            using Iterator = typename Map::const_iterator;


            if (!position.has_value())
            {
                const bool all_keys_used = allKeysUsed(map, *parent.used_flags, bucket_idx, num_buckets);
                ProfileEvents::increment(
                    all_keys_used ? ProfileEvents::JoinNonJoinedHashTableScansSkipped : ProfileEvents::JoinNonJoinedHashTableScans);
                position = std::make_any<Iterator>(all_keys_used ? map.end() : map.begin());
            }

            Iterator & it = std::any_cast<Iterator &>(position);
            auto end = map.end();

            /// case: two-level hash tables with parallel iteration
            if constexpr (requires { map.iteratorAt(0); })
            {
                auto skipToNextOwnedBucket = [&]() -> bool
                {
                    while (it != end && !isBucketInRange(it.getBucket()))
                    {
                        /// smallest bucket > current that satisfies: bucket ≡ bucket_idx (mod num_buckets)
                        size_t cur = it.getBucket();
                        size_t next = cur - (cur % num_buckets) + bucket_idx;
                        if (next <= cur)
                            next += num_buckets;
                        it = map.iteratorAt(next);
                    }
                    return it != end;
                };

                /// position at the first bucket owned by this stream
                if (!skipToNextOwnedBucket())
                    return 0;

                while (it != end && rows_added < max_block_size)
                {
                    size_t offset = map.offsetInternal(it.getPtr());
                    if (!parent.isUsed(offset))
                        rows_added += collectNonJoinedRefWords<Mapped>(it->getMapped(), words);

                    ++it;

                    /// if we crossed into a bucket not owned by this stream, skip ahead
                    if (it != end && !isBucketInRange(it.getBucket()) && !skipToNextOwnedBucket())
                        break;
                }
            }
            else
            {
                /// A `PartitionedFixedHashTable` has no `iteratorAt`, so the walk tests the bucket of every cell.
                for (; it != end; ++it)
                {
                    if (!isBucketInRange(it.getBucket()))
                        continue;

                    size_t offset = map.offsetInternal(it.getPtr());
                    if (parent.isUsed(offset))
                        continue;

                    rows_added += collectNonJoinedRefWords<Mapped>(it->getMapped(), words);

                    if (rows_added >= max_block_size)
                    {
                        ++it;
                        break;
                    }
                }
            }
        }

        emitOutputs(columns_right, words);
        return rows_added;
    }

    void fillNullsFromBlocks(MutableColumns & columns_right, size_t & rows_added)
    {
        /// for parallel iteration, only stream 0 handles nullmaps to avoid duplicates
        if (bucket_idx != 0)
            return;

        if (!nulls_position.has_value())
            nulls_position = parent.data->workers.front().nullmaps.begin();

        auto end = parent.data->workers.front().nullmaps.end();

        size_t nulls_added = 0;

        PaddedPODArray<UInt64> words;
        words.reserve(max_block_size);

        for (auto & it = *nulls_position; it != end && rows_added + nulls_added < max_block_size; ++it)
        {
            const auto * columns = it->columns;
            ConstNullMapPtr nullmap = nullptr;
            if (it->column)
                nullmap = &assert_cast<const ColumnUInt8 &>(*it->column).getData();

            /// Iterate only the selector's rows to avoid emitting rows outside this partition.
            for (size_t row : columns->selector)
            {
                if (nullmap && (*nullmap)[row])
                {
                    ++nulls_added;
                    words.push_back(RowRef(columns->block_no, row).encode());
                }
            }
        }

        emitOutputs(columns_right, words);
        rows_added += nulls_added;
    }
};

IBlocksStreamPtr
HashJoin::getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const
{
    return getNonJoinedBlocks(left_sample_block, result_sample_block, max_block_size, 0, 1);
}

bool HashJoin::supportParallelNonJoinedBlocksProcessing() const
{
    /// Without equi keys nothing reaches a map, so no right row is ever marked used and the scan cannot be split by bucket.
    return supportParallelJoin() && table_join->allowParallelNonJoinedRowsProcessing() && JoinCommon::hasNonJoinedBlocks(*table_join)
        && std::ranges::any_of(table_join->getClauses(), [](const auto & clause) { return !clause.key_names_right.empty(); });
}

IBlocksStreamPtr
HashJoin::getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size,
                             size_t bucket_idx, size_t num_buckets) const
{
    if (!JoinCommon::hasNonJoinedBlocks(*table_join) || !isRightOrFull(kind) || !data || data->rows_to_join == 0)
        return {};

    size_t left_columns_count = left_sample_block.columns();
    if (canRemoveColumnsFromLeftBlock())
        left_columns_count = table_join->getOutputColumns(JoinTableSide::Left).size();

    bool flag_per_row = needUsedFlagsForPerRightTableRow(table_join);
    if (!flag_per_row)
    {
        /// With multiple disjuncts, all keys are in sample_block_with_columns_to_add, so invariant is not held
        size_t expected_columns_count = left_columns_count + required_right_keys.columns() + sample_block_with_columns_to_add.columns();
        if (expected_columns_count != result_sample_block.columns())
        {
            Names left_block_names;
            if (canRemoveColumnsFromLeftBlock())
                std::ranges::copy(
                    table_join->getOutputColumns(JoinTableSide::Left) | std::views::transform([](const auto & column) { return column.name; }),
                    std::back_inserter(left_block_names));
            else
                left_block_names = left_sample_block.getNames();

            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Unexpected number of columns in result sample block: {} expected {} ([{}] = [{}] + [{}] + [{}])",
                            result_sample_block.columns(), expected_columns_count,
                            result_sample_block.dumpNames(), fmt::join(left_block_names, ", "),
                            required_right_keys.dumpNames(), sample_block_with_columns_to_add.dumpNames());
        }
    }

    auto non_joined = std::make_unique<NotJoinedHash>(*this, max_block_size, flag_per_row, bucket_idx, num_buckets);
    return std::make_unique<NotJoinedBlocks>(std::move(non_joined), result_sample_block, left_columns_count, *table_join);
}

void HashJoin::reuseJoinedData(const HashJoin & join)
{
    data = join.data;
    peak_build_bytes.store(join.peak_build_bytes.load(std::memory_order_relaxed), std::memory_order_relaxed);
    from_storage_join = true;

    bool flag_per_row = needUsedFlagsForPerRightTableRow(table_join);
    if (flag_per_row)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "StorageJoin with ORs is not supported");

    const auto maps_kind = getMapsKind();
    for (auto & map : data->maps)
    {
        joinDispatch(
            kind,
            strictness,
            map,
            maps_kind,
            [this](auto kind_, auto strictness_, auto & map_)
            {
                used_flags->reinit<kind_, strictness_, mapsKindOf<decltype(map_)>()>(
                    map_.getBufferSizeInCells(data->type) + 1);
            });
    }

    if (matched_rows_stats)
        matched_rows_stats->prepareRightFlagsIfNeeded(data->workers.front().columns);
}

BlocksList HashJoin::releaseJoinedBlocks(bool restructure [[maybe_unused]])
{
    /// A set map stores the right blocks only for the algorithm that says it may take them. Asking
    /// for them without having said so would hand back an empty list and silently lose the right
    /// side, so say plainly that the join was built for a different contract.
    if (getMapsKind() == JoinMapsKind::Set && !right_blocks_may_be_taken)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Right blocks of a key-only join were asked for, but no algorithm said it would take them");

    LOG_TRACE(
        log, "{}Join data is being released, {} bytes and {} rows in hash table", instance_log_id, getTotalByteCount(), getTotalRowCount());

    if (!data)
        return {};

    const auto column_access_indexes = data->column_access_indexes;

    auto extract_source_blocks = [&](StoredBlocksList && columns_list, const Block & sample_block)
    {
        BlocksList result;
        for (auto & columns : columns_list)
            result.emplace_back(sample_block.cloneWithColumns(materializeStoredBlock(columns, column_access_indexes)));
        return result;
    };

    data->spliceWorkerLists();
    StoredBlocksList right_columns = std::move(data->workers.front().columns);
    if (!restructure)
    {
        auto sample_block = std::move(data->sample_block);
        data.reset();
        /// `extract_source_blocks` allocates, so it can throw here with `data` already gone.
        fiu_do_on(FailPoints::hash_join_throw_after_data_release,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure after the join data was released");
        });
        return extract_source_blocks(std::move(right_columns), sample_block);
    }

    data->maps.clear();
    data->workers.front().nullmaps.clear();

    BlocksList restored_blocks;

    /// names to positions optimization
    std::vector<size_t> positions;
    std::vector<bool> is_nullable;
    if (!right_columns.empty())
    {
        positions.reserve(right_sample_block.columns());
        for (const auto & sample_column : right_sample_block)
        {
            positions.emplace_back(data->sample_block.getPositionByName(sample_column.name));
            is_nullable.emplace_back(isNullableOrLowCardinalityNullable(sample_column.type));
        }
    }

    for (auto & saved_columns : right_columns)
    {
        Columns all_columns = materializeStoredBlock(saved_columns, column_access_indexes);
        Block restored_block;
        for (size_t i = 0; i < positions.size(); ++i)
        {
            auto column = data->sample_block.getByPosition(positions[i]);
            column.column = all_columns[positions[i]];
            correctNullabilityInplace(column, is_nullable[i]);
            restored_block.insert(column);
        }
        restored_blocks.emplace_back(std::move(restored_block));
    }

    data.reset();
    return restored_blocks;
}

BlocksList HashJoin::releaseWorkerBlocks(size_t worker_id)
{
    auto & worker = data->workers[worker_id];
    for (const auto & pending : worker.pending_inserts)
        data->subBytes(data->pending_allocated_size, pending.allocated_bytes);
    worker.pending_inserts.clear();
    StoredBlocksList right_columns = std::move(worker.columns);
    /// `data` outlives the last worker's list on purpose: other threads may still be draining theirs,
    /// and the sample block is shared. So its byte counts drop with the blocks.
    for (const auto & columns : right_columns)
        data->subBytes(data->allocated_size, columns.allocatedBytes());
    for (const auto & nullmap : worker.nullmaps)
        data->subBytes(data->nullmaps_allocated_size, nullmap.allocatedBytes());
    worker.nullmaps.clear();

    BlocksList result;
    for (auto & columns : right_columns)
        result.emplace_back(data->sample_block.cloneWithColumns(materializeStoredBlock(columns, data->column_access_indexes)));
    return result;
}

void HashJoin::releaseJoinMaps()
{
    data->maps.clear();
    data->slots.releaseArenas();
    data->setBytes(data->bucket_bytes, 0);
}

const ColumnWithTypeAndName & HashJoin::rightAsofKeyColumn() const
{
    /// It should be nullable when right side is nullable
    return savedBlockSample().getByName(table_join->getOnlyClause().key_names_right.back());
}

void HashJoin::validateAdditionalFilterExpression(ExpressionActionsPtr additional_filter_expression)
{
    if (!additional_filter_expression)
        return;

    Block expression_sample_block = additional_filter_expression->getSampleBlock();

    if (expression_sample_block.columns() != 1)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Unexpected expression in JOIN ON section. Expected single column, got '{}', expression:\n{}",
            expression_sample_block.dumpStructure(),
            additional_filter_expression->dumpActions());
    }

    auto type = removeNullable(expression_sample_block.getByPosition(0).type);
    if (!type->equals(*std::make_shared<DataTypeUInt8>()))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unexpected expression in JOIN ON section. Expected boolean (UInt8), got '{}'. expression:\n{}",
            expression_sample_block.getByPosition(0).type->getName(),
            additional_filter_expression->dumpActions());
    }

    bool is_supported = ((strictness == JoinStrictness::All) && (isInnerOrLeft(kind) || isRightOrFull(kind)))
        || ((strictness == JoinStrictness::Semi || strictness == JoinStrictness::Any || strictness == JoinStrictness::Anti)
            && (isLeft(kind) || isRight(kind)))
        || (strictness == JoinStrictness::Any && (isInner(kind)));

    if (!is_supported)
    {
        throw Exception(
            ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
            "Non equi condition '{}' from JOIN ON section is supported only for ALL INNER/LEFT/FULL/RIGHT JOINs",
            expression_sample_block.getByPosition(0).name);
    }

    /// `arrayJoin` changes the number of rows, but `buildAdditionalFilter` evaluates this expression
    /// per probe batch and `joinRightColumnsWithAdditionalFilter` indexes the result by row position,
    /// so the expression must preserve the number of rows.
    if (additional_filter_expression->hasArrayJoin())
    {
        throw Exception(
            ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
            "Non equi condition '{}' from JOIN ON section contains 'arrayJoin', which changes the number of rows. "
            "If the expansion depends on one side only, use ARRAY JOIN in a subquery before the JOIN",
            expression_sample_block.getByPosition(0).name);
    }
}

bool HashJoin::isUsed(size_t off) const
{
    return used_flags->getUsedSafe(off);
}

bool HashJoin::isUsed(UInt32 block_no, size_t row_idx) const
{
    return used_flags->getUsedSafe(block_no, row_idx);
}

bool HashJoin::needUsedFlagsForPerRightTableRow(std::shared_ptr<TableJoin> table_join_) const
{
    if (!table_join_->oneDisjunct())
        return true;
    /// If it'a a all right join with inequal conditions, we need to mark each row
    if (table_join_->getMixedJoinExpression() && isRightOrFull(table_join_->kind()))
        return true;
    return false;
}

template <JoinKind KIND, typename Map, JoinStrictness STRICTNESS>
void HashJoin::tryRerangeRightTableDataImpl(Map & map [[maybe_unused]])
{
    constexpr JoinFeatures<KIND, STRICTNESS, Map> join_features;
    if constexpr (!join_features.is_all_join || (!join_features.left && !join_features.inner))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Only left or inner join table can be reranged.");
    else
    {
        const StoredBlock * const * stored_columns = data->stored_columns_index->blocksData();

        auto merge_rows_into_one_block = [&](StoredBlocksList & columns_list, RowRefList & rows_ref)
        {
            auto it = rows_ref.begin();
            if (!it.ok())
                return;

            const StoredBlock * head_block = stored_columns[refWordBlockNo(*it)];

            if (columns_list.empty() || columns_list.back().columns.at(0)->size() >= DEFAULT_BLOCK_SIZE)
            {
                Columns columns;
                columns.reserve(head_block->columns.size());
                for (const auto & col : head_block->columns)
                    columns.push_back(col->cloneEmpty());
                columns_list.emplace_back(std::move(columns), ScatteredBlock::Selector());
                columns_list.back().block_no = data->stored_columns_index->add(&columns_list.back());
                /// The index storage might have been reallocated by the append.
                stored_columns = data->stored_columns_index->blocksData();
            }

            auto & merged = columns_list.back();
            size_t start_row = merged.columns.at(0)->size();

            /// Detach all destination columns once (COW-safe: clones only if shared) and append through the
            /// mutable handles, then move them back. This keeps the per-row append loop free of COW plumbing.
            MutableColumns mutable_columns;
            mutable_columns.reserve(merged.columns.size());
            for (auto & column : merged.columns)
                mutable_columns.push_back(IColumn::mutate(std::move(column)));

            for (; it.ok(); ++it)
            {
                const UInt64 ref_word = *it;
                const StoredBlock * src_block = stored_columns[refWordBlockNo(ref_word)];
                const size_t src_row = refWordRowNo(ref_word);
                for (size_t i = 0; i < mutable_columns.size(); ++i)
                {
                    auto & col = *mutable_columns[i];
                    /// Check if we insert into non replicated column from a replicated column.
                    if (!merged.replicated_columns[i] && src_block->replicated_columns[i])
                    {
                        const auto * src_replicated_column = src_block->replicated_columns[i];
                        col.insertFrom(*src_replicated_column->getNestedColumn(), src_replicated_column->getIndexes().getIndexAt(src_row));
                    }
                    else
                    {
                        col.insertFrom(*(src_block->columns[i]), src_row);
                    }
                }
            }

            for (size_t i = 0; i < mutable_columns.size(); ++i)
                merged.columns[i] = std::move(mutable_columns[i]);

            size_t new_rows = merged.columns.at(0)->size();
            if (new_rows > start_row)
            {
                const size_t merged_rows = new_rows - start_row;
                rows_ref.setRange(RowRef(merged.block_no, start_row).encode(), merged_rows, data->slots.arena(0));
            }
        };

        auto visit_rows_map = [&](StoredBlocksList & columns, MapsAll & rows_map)
        {
            switch (data->type)
            {
#define M(TYPE) \
    case Type::TYPE: { \
        rows_map.TYPE->forEachMapped([&](RowRefList & rows_ref) { merge_rows_into_one_block(columns, rows_ref); }); \
        break; \
    }
                APPLY_FOR_JOIN_VARIANTS(M)
#undef M
            }
        };
        StoredBlocksList sorted_columns;
        visit_rows_map(sorted_columns, map);
        doDebugAsserts();
        data->workers.front().columns.swap(sorted_columns);
        /// The replaced blocks are destroyed below; null their index entries so that any stale
        /// ref fails loudly instead of reading freed memory. All live cells were rewritten above.
        for (const auto & old_columns : sorted_columns)
            data->stored_columns_index->clearEntry(old_columns.block_no);
        size_t new_blocks_allocated_size = 0;
        for (auto & columns : data->workers.front().columns)
        {
            columns.selector = ScatteredBlock::Selector(columns.columns.at(0)->size());
            new_blocks_allocated_size += columns.allocatedBytes();
        }
        data->setBytes(data->allocated_size, new_blocks_allocated_size);

        /// Every stored block was replaced by a merged one with a fresh block_no, so the flags
        /// keyed by the old numbers are stale. Nothing has been marked yet - the probe runs later.
        if (matched_rows_stats && matched_rows_stats->hasRightFlags())
            matched_rows_stats->prepareRightFlags(data->workers.front().columns);

        doDebugAsserts();
    }
}

bool HashJoin::isRightTableRerangeEnabled() const
{
    return table_join->allowJoinSorting() && !table_join->getMixedJoinExpression() && isInnerOrLeft(kind)
        && strictness == JoinStrictness::All && data && !data->sorted && data->maps.size() == 1;
}

/// We should not rerange the right table on such conditions:
/// 1. The right table is already reranged by key, or it is empty.
/// 2. The join clauses size is greater than 1, for example:
///    `...join on a.key1=b.key1 or a.key2=b.key2`.
///    We cannot rerange the right table on different sets of keys.
/// 3. The number of right table rows exceeds the threshold, which may
///    results in a significant cost for reranging and performance degradation.
/// 4. The keys of the right table are very sparse, which may result in
///    insignificant performance improvement after reranging by key.
bool HashJoin::rightTableCanBeReranged() const
{
    return isRightTableRerangeEnabled() && !data->workers.front().columns.empty()
        && data->rows_to_join <= table_join->sortRightMaximumTableRows()
        && data->avgPerKeyRows() >= table_join->sortRightMinimumPerkeyRows();
}

void HashJoin::tryRerangeRightTableData()
{
    if (!rightTableCanBeReranged())
        return;

    /// If the there is no columns to add, means no columns to output, then the rerange would not improve performance by using column's `insertRangeFrom`
    /// to replace column's `insertFrom` to make the output.
    if (sample_block_with_columns_to_add.columns() == 0)
    {
        LOG_DEBUG(
            log,
            "The joined right table total rows :{}, total keys :{}",
            data->rows_to_join.load(std::memory_order_relaxed),
            data->keys_to_join.load(std::memory_order_relaxed));
        return;
    }
    [[maybe_unused]] bool result = joinDispatch(
        kind,
        strictness,
        data->maps.front(),
        getMapsKind(),
        [&](auto kind_, auto strictness_, auto & map_) { tryRerangeRightTableDataImpl<kind_, decltype(map_), strictness_>(map_); });
    chassert(result);
    data->sorted = true;
}

template <bool is_signed, typename Key, typename MapsTemplate>
void HashJoin::tryConvertToFixedHashMapImpl(MapsTemplate & maps)
{
    using SignedKey = std::make_signed_t<Key>;

    static constexpr size_t MAX_RANGE = (1ULL << 18);
    /// Limits conversion to cases where FixedHashMaps larger than 2^16 have at least 25% fill factor,
    /// ensuring they use at most around twice the memory of the source HashMap.
    static constexpr size_t MAX_RANGE_SPARSITY_FACTOR = 4;

    auto & source_map = [&]() -> auto &
    {
        if constexpr (std::is_same_v<Key, UInt32>)
            return *maps.key32;
        else
            return *maps.key64;
    }();

    if (source_map.empty() || source_map.size() > MAX_RANGE)
        return;

    size_t key_count = source_map.size();
    auto it = source_map.begin();
    Key min_key = it->getKey();
    Key max_key = it->getKey();
    ++it;

    /// Keys are stored as unsigned (UInt32/UInt64) in the hash map, but the original column
    /// may be signed (Int32/Int64). We must compare using signed arithmetic to find the true
    /// min/max
    for (; it != source_map.end(); ++it)
    {
        Key k = it->getKey();
        if constexpr (is_signed)
        {
            SignedKey signed_key = static_cast<SignedKey>(k);
            if (signed_key < static_cast<SignedKey>(min_key))
                min_key = k;
            if (signed_key > static_cast<SignedKey>(max_key))
                max_key = k;
        }
        else
        {
            if (k < min_key)
                min_key = k;
            if (k > max_key)
                max_key = k;
        }

        if (static_cast<size_t>(max_key - min_key) >= MAX_RANGE)
            return;
    }

    size_t range = static_cast<size_t>(max_key - min_key) + 1;

    using Mapped = typename MapsTemplate::MappedType;
    auto convert_to_fixed_hash_map = [&]<size_t size_bits>(auto & dst_map, Type type)
    {
        using RangeMap = JoinFixedHashMapWithSizeBits<Key, Mapped, size_bits>;
        auto range_map = std::make_shared<RangeMap>();
        for (auto source_map_it = source_map.begin(); source_map_it != source_map.end(); ++source_map_it)
        {
            typename RangeMap::LookupResult res;
            bool inserted = false;
            range_map->emplace(source_map_it->getKey() - min_key, res, inserted);
            if constexpr (MapsTemplate::has_mapped)
            {
                if (inserted)
                    res->getMapped() = source_map_it->getMapped();
            }
        }
        dst_map = std::move(range_map);
        data->key_range = {min_key, range};
        data->type = type;
    };

    auto dispatch_conversion =
        [&](auto & range8, Type type8, auto & range16, Type type16, auto & range17, Type type17, auto & range18, Type type18, auto & source)
    {
        if (range <= (1ULL << 8))
            convert_to_fixed_hash_map.template operator()<8>(range8, type8);
        else if (range <= (1ULL << 16))
            convert_to_fixed_hash_map.template operator()<16>(range16, type16);
        else if (range <= (1ULL << 17))
        {
            if ((1ULL << 17) > key_count * MAX_RANGE_SPARSITY_FACTOR)
                return false;
            convert_to_fixed_hash_map.template operator()<17>(range17, type17);
        }
        else
        {
            if ((1ULL << 18) > key_count * MAX_RANGE_SPARSITY_FACTOR)
                return false;
            convert_to_fixed_hash_map.template operator()<18>(range18, type18);
        }
        source.reset();
        return true;
    };

    bool result = false;
    if constexpr (std::is_same_v<Key, UInt32>)
        result = dispatch_conversion(
            maps.range8_key32,
            Type::range8_key32,
            maps.range16_key32,
            Type::range16_key32,
            maps.range17_key32,
            Type::range17_key32,
            maps.range18_key32,
            Type::range18_key32,
            maps.key32);
    else
        result = dispatch_conversion(
            maps.range8_key64,
            Type::range8_key64,
            maps.range16_key64,
            Type::range16_key64,
            maps.range17_key64,
            Type::range17_key64,
            maps.range18_key64,
            Type::range18_key64,
            maps.key64);

    if (result)
        LOG_DEBUG(
            log,
            "{}Converted join hash map to fixed hash map (range: {}, keys: {}, type: {})",
            instance_log_id,
            range,
            key_count,
            data->type);
}

bool HashJoin::canConvertToFixedHashMap() const
{
    return !conversion_to_fixed_hash_map_attempted && data && data->rows_to_join && table_join->enableJoinFixedHashTableConversion()
        && (data->type == Type::key32 || data->type == Type::key64) && data->maps.size() == 1 && strictness != JoinStrictness::Asof;
}

/// The build leaves two things unset that probing reads: cell prefix sums, and the min/max
/// bounds a direct-addressed map keeps off while concurrent inserts could race. Call after the last insert.
void HashJoin::freezeMapsForProbing()
{
    const auto maps_kind = getMapsKind();
    auto finish_fill = []<typename Table>(std::shared_ptr<Table> & table)
    {
        if (!table)
            return;
        table->computeBucketPrefix();
        if constexpr (is_partitioned_fixed_table<Table>)
            table->restoreMinMaxOptimization();
    };
    for (auto & map : data->maps)
    {
        joinDispatch(kind, strictness, map, maps_kind, [&](auto, auto, auto & map_) { map_.visit(data->type, finish_fill); });
    }
}

void HashJoin::reinitUsedFlags()
{
    if (needUsedFlagsForPerRightTableRow(table_join))
        return;

    const auto maps_kind = getMapsKind();
    for (auto & map : data->maps)
    {
        joinDispatch(
            kind,
            strictness,
            map,
            maps_kind,
            [this](auto kind_, auto strictness_, auto & map_)
            {
                used_flags->reinitAllowShrinking<kind_, strictness_, mapsKindOf<decltype(map_)>()>(
                    map_.getBufferSizeInCells(data->type) + 1);
            });
    }
}

namespace
{

/// Defensive check that T (= common_type) can hold every value of BuildKey.
/// `getLeastSupertype` guarantees this in normal flow; this catches contract violations
/// from future code changes and falls through to pass-all instead of misfiring bounds checks.
template <typename BuildKey, typename T>
constexpr bool canLosslesslyHold()
{
    if constexpr (std::is_unsigned_v<T> && std::is_signed_v<BuildKey>)
        return false;
    else if constexpr (std::is_signed_v<T> == std::is_signed_v<BuildKey>)
        return sizeof(T) >= sizeof(BuildKey);
    else /* T signed, BuildKey unsigned */
        return sizeof(T) > sizeof(BuildKey);
}

/// Inner loop: for each probe value v of type T, check it falls within BuildKey's value range,
/// then narrow to BuildKey and reinterpret to the FixedHashMap's unsigned key. The null-mask
/// merge is split out of the loop body into two specialized paths so each loop stays branchless
/// and vectorizable.
template <typename BuildKey, typename HashMapT, typename T>
void probeFixedHashMapLoop(
    const HashMapT & ht,
    std::make_unsigned_t<BuildKey> min_key,
    size_t range_size,
    const T * src,
    const UInt8 * null_map,
    UInt8 * result,
    size_t n)
{
    using UnsignedBK = std::make_unsigned_t<BuildKey>;
    static_assert(canLosslesslyHold<BuildKey, T>(),
                  "probeFixedHashMapLoop instantiated with a probe type that cannot hold BuildKey's full range");

    constexpr T t_lo = static_cast<T>(std::numeric_limits<BuildKey>::min());
    constexpr T t_hi = static_cast<T>(std::numeric_limits<BuildKey>::max());

    auto probe_one = [&](size_t i) -> UInt8
    {
        const T v = src[i];
        if (v < t_lo || v > t_hi)
            return 0;
        const UnsignedBK slot = static_cast<UnsignedBK>(static_cast<BuildKey>(v));
        const UnsignedBK idx = slot - min_key;
        return (idx < range_size && ht.has(idx)) ? 1 : 0;
    };

    if (null_map)
    {
        for (size_t i = 0; i < n; ++i)
            result[i] = probe_one(i) & static_cast<UInt8>(!null_map[i]);
    }
    else
    {
        for (size_t i = 0; i < n; ++i)
            result[i] = probe_one(i);
    }
}

/// Dispatch over the probe column's element type to call probeFixedHashMapLoop.
/// Probe types whose value range can't hold BuildKey are filtered out at compile time;
/// at runtime a non-ColumnVector or excluded type is conservatively passed through (all 1).
template <typename BuildKey, typename HashMapT>
ColumnPtr probeFixedHashMap(
    const HashMapT & ht,
    std::make_unsigned_t<BuildKey> min_key,
    size_t range_size,
    const ColumnWithTypeAndName & values)
{
    const IColumn * col = values.column.get();
    const ColumnUInt8 * nm_col = nullptr;
    if (const auto * nullable = checkAndGetColumn<ColumnNullable>(col))
    {
        col = &nullable->getNestedColumn();
        nm_col = &nullable->getNullMapColumn();
    }

    const size_t n = col->size();
    auto result_col = ColumnUInt8::create(n);
    UInt8 * result = result_col->getData().data();
    const UInt8 * null_map = nm_col ? nm_col->getData().data() : nullptr;

    const bool dispatched = castTypeToEither<
        ColumnVector<UInt8>, ColumnVector<UInt16>, ColumnVector<UInt32>, ColumnVector<UInt64>,
        ColumnVector<Int8>, ColumnVector<Int16>, ColumnVector<Int32>, ColumnVector<Int64>>(
        col,
        [&](const auto & typed_col) -> bool
        {
            using T = typename std::decay_t<decltype(typed_col)>::ValueType;
            if constexpr (canLosslesslyHold<BuildKey, T>())
            {
                probeFixedHashMapLoop<BuildKey, HashMapT, T>(
                    ht, min_key, range_size,
                    typed_col.getData().data(), null_map, result, n);
                return true;
            }
            else
            {
                return false;
            }
        });

    if (!dispatched)
        std::fill_n(result, n, UInt8(1));

    return result_col;
}

/// Wrap a FixedHashMap as a ProbeFn for the runtime filter to invoke on the probe side.
template <typename BuildKey, typename HashMapT>
SharedFixedHashTableRuntimeFilter::ProbeFn buildSharedFilterProbeFn(
    std::shared_ptr<HashMapT> range_map_arg,
    std::make_unsigned_t<BuildKey> min_key,
    size_t range_size)
{
    return [range_map = std::move(range_map_arg), min_key, range_size]
        (const ColumnWithTypeAndName & values) -> ColumnPtr
    {
        return probeFixedHashMap<BuildKey, HashMapT>(*range_map, min_key, range_size, values);
    };
}

} // anonymous namespace

void HashJoin::publishSharedRuntimeFilters()
{
    if (shared_runtime_filters_publish_attempted)
        return;
    shared_runtime_filters_publish_attempted = true;

    if (!table_join->joinRuntimeFilterFromFixedHashTable())
        return;

    const auto & descriptors = table_join->getSharedRuntimeFilterDescriptors();
    if (descriptors.empty())
        return;

    if (data->maps.size() != 1)
        return;

    const bool is_fixed_hash_table =
        data->type == Type::key8 || data->type == Type::key16 ||
        data->type == Type::range8_key32 || data->type == Type::range16_key32 ||
        data->type == Type::range17_key32 || data->type == Type::range18_key32 ||
        data->type == Type::range8_key64 || data->type == Type::range16_key64 ||
        data->type == Type::range17_key64 || data->type == Type::range18_key64;
    if (!is_fixed_hash_table)
        return;

    /// For a single distinct build key, the existing `== const` runtime filter specialization
    /// is faster than any hash table probe; leave it active.
    if (data->keys_to_join <= 1)
        return;

    auto query_context = CurrentThread::get().tryGetQueryContext();
    if (!query_context)
        return;
    auto lookup = query_context->getRuntimeFilterLookup();
    if (!lookup)
        return;

    if (right_table_keys.columns() != 1)
        return;
    const String build_key_name = right_table_keys.getByPosition(0).name;
    const auto & filter_column_type = right_table_keys.getByPosition(0).type;

    /// Only integer-backed build types have meaningful min/max for the bounds check.
    /// Float, Decimal and DateTime64 (scale) drop out via isValueRepresentedByInteger.
    const auto build_type = removeNullable(filter_column_type);
    if (!build_type->isValueRepresentedByInteger())
        return;
    const bool build_signed = !build_type->isValueRepresentedByUnsignedInteger();

    auto build_probe_fn = [&]() -> SharedFixedHashTableRuntimeFilter::ProbeFn
    {
        SharedFixedHashTableRuntimeFilter::ProbeFn probe_fn;
        std::visit(
            [&](auto & map)
            {
                using MapType = std::decay_t<decltype(map)>;
                if constexpr (std::is_same_v<MapType, MapsOne> || std::is_same_v<MapType, MapsAll> || std::is_same_v<MapType, MapsSet>)
                {
                    auto dispatch = [&]<typename BuildKey>(
                        auto & range_ptr,
                        std::make_unsigned_t<BuildKey> min_key,
                        size_t range_size)
                    {
                        if (!range_ptr)
                            return;
                        probe_fn = buildSharedFilterProbeFn<BuildKey>(range_ptr, min_key, range_size);
                    };

                    /// For range_*_key_* the min/range come from `tryConvertToFixedHashMap`'s
                    /// computed key_range; for key8/key16 the whole key space is the table so
                    /// min=0 and range = 2^(Key bits).
                    switch (data->type)
                    {
                        case Type::key8:
                            if (build_signed)
                                dispatch.template operator()<Int8>(map.key8, UInt8(0), 1ULL << 8);
                            else
                                dispatch.template operator()<UInt8>(map.key8, UInt8(0), 1ULL << 8);
                            break;
                        case Type::key16:
                            if (build_signed)
                                dispatch.template operator()<Int16>(map.key16, UInt16(0), 1ULL << 16);
                            else
                                dispatch.template operator()<UInt16>(map.key16, UInt16(0), 1ULL << 16);
                            break;
                        case Type::range8_key32:
                            if (build_signed)
                                dispatch.template operator()<Int32>(map.range8_key32,
                                    static_cast<UInt32>(data->key_range.min_key), data->key_range.size);
                            else
                                dispatch.template operator()<UInt32>(map.range8_key32,
                                    static_cast<UInt32>(data->key_range.min_key), data->key_range.size);
                            break;
                        case Type::range16_key32:
                            if (build_signed)
                                dispatch.template operator()<Int32>(map.range16_key32,
                                    static_cast<UInt32>(data->key_range.min_key), data->key_range.size);
                            else
                                dispatch.template operator()<UInt32>(map.range16_key32,
                                    static_cast<UInt32>(data->key_range.min_key), data->key_range.size);
                            break;
                        case Type::range17_key32:
                            if (build_signed)
                                dispatch.template operator()<Int32>(map.range17_key32,
                                    static_cast<UInt32>(data->key_range.min_key), data->key_range.size);
                            else
                                dispatch.template operator()<UInt32>(map.range17_key32,
                                    static_cast<UInt32>(data->key_range.min_key), data->key_range.size);
                            break;
                        case Type::range18_key32:
                            if (build_signed)
                                dispatch.template operator()<Int32>(map.range18_key32,
                                    static_cast<UInt32>(data->key_range.min_key), data->key_range.size);
                            else
                                dispatch.template operator()<UInt32>(map.range18_key32,
                                    static_cast<UInt32>(data->key_range.min_key), data->key_range.size);
                            break;
                        case Type::range8_key64:
                            if (build_signed)
                                dispatch.template operator()<Int64>(map.range8_key64,
                                    data->key_range.min_key, data->key_range.size);
                            else
                                dispatch.template operator()<UInt64>(map.range8_key64,
                                    data->key_range.min_key, data->key_range.size);
                            break;
                        case Type::range16_key64:
                            if (build_signed)
                                dispatch.template operator()<Int64>(map.range16_key64,
                                    data->key_range.min_key, data->key_range.size);
                            else
                                dispatch.template operator()<UInt64>(map.range16_key64,
                                    data->key_range.min_key, data->key_range.size);
                            break;
                        case Type::range17_key64:
                            if (build_signed)
                                dispatch.template operator()<Int64>(map.range17_key64,
                                    data->key_range.min_key, data->key_range.size);
                            else
                                dispatch.template operator()<UInt64>(map.range17_key64,
                                    data->key_range.min_key, data->key_range.size);
                            break;
                        case Type::range18_key64:
                            if (build_signed)
                                dispatch.template operator()<Int64>(map.range18_key64,
                                    data->key_range.min_key, data->key_range.size);
                            else
                                dispatch.template operator()<UInt64>(map.range18_key64,
                                    data->key_range.min_key, data->key_range.size);
                            break;
                        default: break;
                    }
                }
            },
            data->maps.front());
        return probe_fn;
    };

    auto probe_fn = build_probe_fn();
    if (!probe_fn)
        return;

    /// Replace any Set/BloomFilter that BuildRuntimeFilterStep installed earlier. The descriptor's
    /// `filter_key` is the rendezvous key (the same key `BuildRuntimeFilterTransform` registered the
    /// filter under and the probe-side `__applyFilter` looks it up by), not the stable display name.
    for (const auto & descriptor : descriptors)
    {
        if (descriptor.build_key_name != build_key_name)
            continue;

        auto existing = lookup->find(descriptor.filter_key);
        if (!existing)
            continue;

        /// When common_type is wide (e.g. Int64 = UInt64 promotes to Int128), per-row wide-integer
        /// arithmetic on the probe side can be slower than the existing BloomFilter; skip.
        const auto target_type = removeNullable(descriptor.common_type);
        WhichDataType target_which(target_type);
        if (!target_type->isValueRepresentedByInteger()
            || target_which.isInt128() || target_which.isUInt128()
            || target_which.isInt256() || target_which.isUInt256()
            || target_which.isIPv4()
            || target_which.isLowCardinality())
            continue;

        /// Metadata accessors expose data only after all stream-local filters have merged. If publication races
        /// a late registration, they return no partial metadata; copied metadata is therefore complete or absent.
        auto filter = std::make_unique<RuntimeFilter>(
            /*filters_to_merge_=*/0,
            existing->getConfig(),
            RuntimeFilter::SharedFixedHashTable(
                existing->getFilterColumnTargetType(),
                probe_fn,
                existing->getRecordedKeyRanges(),
                existing->getRecordedKeyValues()));
        /// `replace` keeps the original registration's display name in the lookup, so stats stay legible.
        LOG_TRACE(getLogger("HashJoin"), "Published shared fixed-hash-table runtime filter under key '{}'", descriptor.filter_key);
        lookup->replace(descriptor.filter_key, std::move(filter));
    }
}

void HashJoin::tryConvertToFixedHashMap()
{
    if (!canConvertToFixedHashMap())
        return;

    conversion_to_fixed_hash_map_attempted = true;
    const Type old_type = data->type;
    std::visit(
        [&](auto & map)
        {
            using MapType = std::decay_t<decltype(map)>;
            if constexpr (std::is_same_v<MapType, MapsOne> || std::is_same_v<MapType, MapsAll> || std::is_same_v<MapType, MapsSet>)
            {
                bool is_signed = !right_table_keys.getByPosition(0).type->isValueRepresentedByUnsignedInteger();
                if (data->type == Type::key32)
                {
                    if (is_signed)
                        tryConvertToFixedHashMapImpl<true, UInt32>(map);
                    else
                        tryConvertToFixedHashMapImpl<false, UInt32>(map);
                }
                else
                {
                    if (is_signed)
                        tryConvertToFixedHashMapImpl<true, UInt64>(map);
                    else
                        tryConvertToFixedHashMapImpl<false, UInt64>(map);
                }
            }
        },
        data->maps.front());

    if (data->type != old_type)
    {
        reinitUsedFlags();
        recomputeBucketBytes();
    }
}

bool HashJoin::isRowStoreSupported() const
{
    /// ANY joins materialize eagerly and doesn't run the batched fill the row store accelerates.
    return kind != JoinKind::Cross
        && strictness != JoinStrictness::Any
        && !table_join->getClauses().empty()
        && !table_join->getMixedJoinExpression();
}

bool HashJoin::recordsRowRefsForStats() const
{
    return table_join->collectExactMatches() && table_join->getMixedJoinExpression() == nullptr;
}

void HashJoin::onBuildPhaseFinish()
{
    /// `FillingRightJoinSideTransform` calls this from `prepare`, so the keys go into the maps in the post-build phase.
    if (deferred_build)
    {
        replay_pending = true;
        return;
    }
    finishBuildPhase();
}

size_t HashJoin::getProjectedBuildBytes(bool all_blocks_stored) const
{
    return getTotalByteCount() + (all_blocks_stored ? deferred_build->projectedBytes() : deferred_build->liveProjectedBytes());
}

void HashJoin::replayDeferredBuild()
{
    ProfileEvents::increment(ProfileEvents::HashJoinDeferredBuilds);
    const size_t projected_bytes = getProjectedBuildBytes(/*all_blocks_stored=*/true);

    std::vector<std::vector<size_t>> bucket_reserves(data->maps.size());
    for (size_t clause = 0; clause < data->maps.size(); ++clause)
    {
        const auto totals = deferred_build->totals(clause);
        bucket_reserves[clause] = deferredBuildBucketReserves(totals.estimate, totals.bucket_rows, totals.rows);
        LOG_DEBUG(
            log,
            "{}Deferred build: {} rows to insert into the map of clause {}, about {} distinct keys",
            instance_log_id,
            totals.rows,
            clause,
            totals.estimate);
    }
    data->slots.setBucketReserves(std::move(bucket_reserves));

    std::vector<std::pair<size_t, PendingInsert>> pending_inserts;
    for (size_t worker_id = 0; worker_id < data->workers.size(); ++worker_id)
    {
        for (auto & pending : data->workers[worker_id].pending_inserts)
            pending_inserts.emplace_back(worker_id, std::move(pending));
        data->workers[worker_id].pending_inserts.clear();
    }

    QueryStatusPtr query_status;
    if (auto query_context = CurrentThread::tryGetQueryContext())
        query_status = query_context->getProcessListElement();

    /// `worker_id` names the lists and the used flags that the insert appends to, so no two threads share one.
    auto insert_pending = [&](size_t worker_id, PendingInsert & pending)
    {
        if (query_status)
            query_status->checkTimeLimit();
        size_t total_rows = 0;
        size_t total_bytes = 0;
        insertStoredBlock(
            data->workers[worker_id],
            pending.stored,
            pendingInsertBlock(pending),
            JoinBuildContext::forStream(worker_id, data->workers.size()).callerChecksLimits(),
            total_rows,
            total_bytes);
        pending.columns.clear();
        data->subBytes(data->pending_allocated_size, pending.allocated_bytes);
    };

    const size_t num_threads = std::min(data->workers.size(), pending_inserts.size());
    if (num_threads <= 1)
    {
        for (auto & [worker_id, pending] : pending_inserts)
            insert_pending(worker_id, pending);
    }
    else
    {
        /// A thread takes the next stored block of any stream and inserts it with the lists of its own worker. A block that
        /// nothing refers to is then removed from that worker's list, so it must be in it: each stored block moves to a
        /// list of its own first, and from there to the list of the thread that inserts it.
        std::vector<StoredBlocksList> blocks(pending_inserts.size());
        for (size_t i = 0; i < pending_inserts.size(); ++i)
        {
            auto & [worker_id, pending] = pending_inserts[i];
            blocks[i].splice(blocks[i].end(), data->workers[worker_id].columns, pending.stored);
        }
        /// After an exception the blocks not inserted yet go back to a list of the join, which owns them.
        SCOPE_EXIT({
            for (auto & block : blocks)
                data->workers.front().columns.splice(data->workers.front().columns.end(), block);
        });

        std::atomic<size_t> next_pending = 0;
        std::atomic<bool> failed = false;
        ThreadPool pool(
            CurrentMetrics::HashJoinBuildThreads,
            CurrentMetrics::HashJoinBuildThreadsActive,
            CurrentMetrics::HashJoinBuildThreadsScheduled,
            num_threads);
        for (size_t thread = 0; thread < num_threads; ++thread)
        {
            pool.scheduleOrThrowOnError(
                [&, thread, thread_group = CurrentThread::getGroup()]
                {
                    ThreadGroupSwitcher switcher(thread_group, ThreadName::HASH_JOIN_BUILD);
                    auto & worker = data->workers[thread];
                    try
                    {
                        for (size_t i = next_pending++; i < pending_inserts.size() && !failed; i = next_pending++)
                        {
                            worker.columns.splice(worker.columns.end(), blocks[i]);
                            insert_pending(thread, pending_inserts[i].second);
                        }
                    }
                    catch (...)
                    {
                        failed = true;
                        throw;
                    }
                });
        }
        pool.wait();
    }

    size_t buckets_grown = 0;
    for (size_t clause = 0; clause < data->maps.size(); ++clause)
        joinDispatch(
            kind,
            strictness,
            data->maps[clause],
            getMapsKind(),
            [&](auto, auto, const auto & maps)
            {
                maps.visit(data->type, [&](const auto & map) { buckets_grown += data->slots.countGrownBuckets(*map, clause); });
            });
    ProfileEvents::increment(ProfileEvents::HashJoinDeferredBuildBucketsGrown, buckets_grown);
    LOG_DEBUG(log, "{}Deferred build: projected {} bytes, built {} bytes", instance_log_id, projected_bytes, getTotalByteCount());

    /// The check that a streaming build makes after each block, made once, against what the maps hold.
    if (deferred_build_checks_limits.load(std::memory_order_relaxed))
    {
        size_t total_bytes = getTotalByteCount();
        updatePeakBuildBytes(total_bytes);
        for (size_t worker_id = 0; worker_id < data->workers.size(); ++worker_id)
            shrinkStoredBlocksToFit(total_bytes, /*force_optimize=*/false, worker_id);
        table_join->sizeLimits().check(getTotalRowCount(), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    }
}

void HashJoin::finishBuildPhase()
{
    freezeMapsForProbing();
    reinitUsedFlags();

    used_flags->finalizePerRowFlags(data->stored_columns_index->size());
    data->spliceWorkerLists();

    if (all_values_unique && strictness == JoinStrictness::All && isInnerOrLeft(kind) && data->maps.size() == 1)
    {
        strictness = JoinStrictness::RightAny;
        all_join_was_promoted_to_right_any = true;
        LOG_DEBUG(log, "Promoting join strictness to RightAny, because all values in the right table are unique");
    }

    build_phase_finished = true;

    /// The plan prints `HashJoin` for both layouts, so record which one this join built on.
    ProfileEvents::increment(
        build_layout.two_level ? ProfileEvents::HashJoinBuiltWithParallelLayout : ProfileEvents::HashJoinBuiltWithSerialLayout);

    /// In case addBlockToJoin is returning early
    /// we take a peak snapshot
    size_t total_bytes = getTotalByteCount();
    updatePeakBuildBytes(total_bytes);

    recomputeBucketBytes();

    if (matched_rows_stats)
        matched_rows_stats->prepareRightFlagsIfNeeded(data->workers.front().columns);
    LOG_TRACE(
        log,
        "{}Join data is built, {} and {} rows in hash table",
        instance_log_id,
        ReadableSize(getTotalByteCount()),
        getTotalRowCount());
}

bool HashJoin::hasPostBuildPhase() const
{
    if (replay_pending)
        return true;

    /// key8/key16 are already FixedHashMap, so they don't go through tryConvertToFixedHashMap,
    /// but publishSharedRuntimeFilters still needs to run for them when the feature is on.
    const bool needs_shared_filter_publish =
        data && data->rows_to_join && data->maps.size() == 1
        && (data->type == Type::key8 || data->type == Type::key16)
        && table_join->joinRuntimeFilterFromFixedHashTable()
        && !table_join->getSharedRuntimeFilterDescriptors().empty();

    return rightTableCanBeReranged() || canConvertToFixedHashMap() || needs_shared_filter_publish;
}

void HashJoin::runPostBuildPhase()
{
    if (replay_pending)
    {
        replayDeferredBuild();
        replay_pending = false;
        finishBuildPhase();
    }
    tryRerangeRightTableData();
    tryConvertToFixedHashMap();
    publishSharedRuntimeFilters();
}
}
