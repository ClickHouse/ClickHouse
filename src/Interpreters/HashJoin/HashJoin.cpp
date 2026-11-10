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
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/StackTrace.h>
#include <Common/logger_useful.h>


#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/MatchedRowsStats.h>
#include <Interpreters/JoinUtils.h>
#include <DataTypes/NullableUtils.h>
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
#include <Interpreters/HashJoin/HashJoinTable.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Interpreters/HashJoin/gatherJoinOutputColumns.h>

#include <numeric>

#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <Processors/QueryPlan/Profiling/Metrics/StepAnalyzeInfo.h>

namespace ProfileEvents
{
    extern const Event HashJoinBuiltWithParallelLayout;
    extern const Event HashJoinBuiltWithSerialLayout;
}

namespace CurrentMetrics
{
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

/// A reserve is an estimate, so cap it at the spill budget. The maps of all disjuncts have one type, so any of them gives `cell_size`.
size_t capReserveAtSpillBudget(size_t keys, size_t max_reserve_bytes, size_t cell_size)
{
    /// `HashTableGrowerWithPrecalculation::set` keeps a buffer at most half full and rounds its size up to a power of two.
    static constexpr size_t max_cells_per_key = 4;
    /// `SpillingHashJoin` switches to `GraceHashJoin` at half of the budget, so the reserve stays under that half.
    static constexpr size_t budget_divisor = 2;

    if (!keys || !max_reserve_bytes)
        return keys;
    return std::min(keys, max_reserve_bytes / (budget_divisor * max_cells_per_key * cell_size));
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

/// A multi-disjunct (OR) join shares a single key type across all disjuncts. When the disjuncts
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
    const HashJoinStatsCollectingParams & stats_collecting_params_)
    : table_join(table_join_)
    , kind(table_join->kind())
    , strictness(table_join->strictness())
    , any_take_last_row(any_take_last_row_)
    , reserve_num(reserve_num_)
    , instance_id(instance_id_)
    , build_layout(build_layout_)
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
    used_flags->setNumBuildStreams(data->build_streams.size());

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

    if (!table_join->isRowStoreEnabled() || data->sample_block.columns() == 0)
        data->row_store_state = RowStoreState::Disabled;

    JoinCommon::createMissedColumns(sample_block_with_columns_to_add);

    size_t disjuncts_num = table_join->getClauses().size();
    /// Publish the row store layout before any `FillingRightJoinSide` thread can call `addBlockToJoin`.
    initRowStore(data->sample_block);
    std::vector<Sizes> key_sizes;
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

    use_set_maps = canUseSetMaps();
    if (table_join->enablePackedStringKeysInHashJoin() && *selected_join_method == Type::key_string)
    {
        selected_join_method = Type::key_packed_string;
        LOG_TRACE(log, "{}Using packed keys for the single String join key", instance_log_id);
    }

    const auto maps_kind = getMapsKind();
    data->tables.reserve(disjuncts_num);
    for (auto & clause_key_sizes : key_sizes)
        data->tables.push_back(
            std::make_unique<HashJoinTable>(
                *selected_join_method,
                std::move(clause_key_sizes),
                kind,
                strictness,
                maps_kind,
                build_layout,
                table_join->enableRobinHoodHashForJoins()));
    chassert(std::ranges::all_of(data->tables, [&](const auto & table) { return table->type() == keyType(); }));

    LOG_TRACE(log, "{}Join hash table type: {}", instance_log_id, keyType());
    LOG_TEST(
        log,
        "{}Keys: {}, datatype: {}, kind: {}, strictness: {}, right header: {}",
        instance_log_id,
        TableJoin::formatClauses(table_join->getClauses(), true),
        keyType(),
        kind,
        strictness,
        right_sample_block.dumpStructure());

    if (use_set_maps)
        LOG_TRACE(log, "Using key-only hash tables: the join never reads a right row");

    /// A 256-bucket map is sized from the estimate, or every bucket rehashes its way up from 256 cells. A serial map
    /// is not: sized from the estimate, it lands on a worse load factor than the grower picks. The cached size counts
    /// the keys of all maps, while each map reserves the whole hint, so a join with several disjuncts does not use it.
    size_t size_hint = reserve_num;
    if (!size_hint && build_layout.two_level && table_join->oneDisjunct())
        if (const auto hint = getSizeHint(stats_collecting_params.build))
            size_hint = hint->ht_size;
    const size_t keys_to_reserve
        = capReserveAtSpillBudget(size_hint, table_join->maxBytesBeforeExternalJoin(), data->tables.front()->cellSize());
    for (auto & table : data->tables)
        table->reserve(keys_to_reserve);

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
    if (!data->build_streams.front().nullmaps.empty())
        return;

    doDebugAsserts();
    for (auto & stored_columns : data->build_streams.front().columns)
    {
        data->subBytes(data->allocated_size, stored_columns.allocatedBytes());
        /// No cell refers to the block, but null the index entry anyway, so that a stale reference
        /// trips the chassert in `StoredColumnsIndex::at` rather than reading freed memory.
        data->stored_columns_index->clearEntry(stored_columns.block_no);
    }
    data->build_streams.front().columns.clear();
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

HashJoin::Type HashJoin::keyType() const
{
    return data->tables.front()->type();
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

    return data->keys();
}

void HashJoin::recomputeBucketBytes()
{
    size_t res = 0;
    for (const auto & table : data->tables)
        res += table->arenaBytes() + table->bufferBytes();

    data->setBytes(data->bucket_bytes, res);
}

void HashJoin::doDebugAsserts() const
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    /// Build streams append to their own lists with no shared lock. Walking every
    /// build stream here races with those pushes (TSan) and can disagree with the atomics.
    if (data->build_streams.size() > 1 && !build_phase_finished)
        return;

    size_t debug_allocated_size = 0;
    for (const auto & build_stream : data->build_streams)
        for (const auto & columns : build_stream.columns)
            debug_allocated_size += columns.allocatedBytes();

    if (data->allocated_size != debug_allocated_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "data->allocated_size != debug_allocated_size ({} != {})",
            data->allocated_size.load(std::memory_order_relaxed),
            debug_allocated_size);

    size_t debug_nullmaps_allocated_size = 0;
    for (const auto & build_stream : data->build_streams)
        for (const auto & nullmap : build_stream.nullmaps)
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

    /// The `FillingRightJoinSideTransform`s of a parallel build have no totals port, and each calls this concurrently
    /// with a zero-row block.
    std::lock_guard lock(totals_mutex);
    IJoin::setTotals(block);
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

    /// A candidate chosen at planning goes to the row store if it arrives as a plain column
    /// and is later reconstructed in the output.
    auto can_be_in_row_store = [&](const String & name, const ColumnPtr & column)
    {
        return table_join->getRowStoreCandidateColumns().contains(name)
            && !typeid_cast<const ColumnReplicated *>(column.get()) && !column->isSparse()
            && (sample_block_with_columns_to_add.has(name) || name == asof_key_name);
    };

    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (can_be_in_row_store(block_to_save.getByPosition(i).name, columns[i]))
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
        access_index.is_string = field.is_string;
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

    const auto & right_key_names = table_join->getAllNames(JoinTableSide::Right);
    ColumnPtrMap all_key_columns(right_key_names.size());
    for (const auto & column_name : right_key_names)
    {
        const auto & column = block.getByName(column_name).column;
        auto prepared_key_column = removeSpecialRepresentations(column->convertToFullColumnIfConst());
        /// Keep the dictionary for the single-LowCardinality-column maps; their key getter needs it.
        if (!isLowCardinalityType(keyType()))
            prepared_key_column = prepared_key_column->convertToFullColumnIfLowCardinality();
        all_key_columns[column_name] = prepared_key_column;
    }

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
                const auto & asof_column_nullable = assert_cast<const ColumnNullable &>(*all_key_columns.at(asof_key_name)).getNullMapData();

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

    const size_t rows = selector.size();

    Block block_to_save = filterColumnsPresentInSampleBlock(block, savedBlockSample());
    /// A parallel build stores copies of narrow rows, made here on a build stream (`shrinkToFit` copies every column).
    /// The producer then reuses its memory instead of faulting in fresh memory for every block.
    if ((build_layout.num_slots > 1 && !useZeroCopyApproach(block)) || shrink_blocks.load(std::memory_order_relaxed))
        block_to_save = block_to_save.shrinkToFit();

    const auto maps_kind = getMapsKind();
    if (context.getNumStreams() > data->build_streams.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Too many HashJoin build streams: {}, capacity {}",
            context.getNumStreams(),
            data->build_streams.size());
    auto & build_stream = data->build_streams[context.getStream()];

    size_t total_rows = 0;
    size_t total_bytes = 0;
    {
        if (storage_join_lock)
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "addBlockToJoin called when HashJoin locked to prevent updates");

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
        /// so the build stream's list, `data->allocated_size` and `data->rows_to_join` always describe the same set
        /// of stored blocks. Same ordering as `tryRerangeRightTableDataImpl`; a list node keeps its address.
        StoredBlocksList new_block;
        new_block.emplace_back(std::move(columns), std::move(selector), std::move(row_store));
        auto * stored_columns = &new_block.back();
        size_t data_allocated_bytes = stored_columns->allocatedBytes();
        stored_columns->block_no = data->stored_columns_index->add(stored_columns);
        data->addBytes(data->allocated_size, data_allocated_bytes);
        data->rows_to_join.fetch_add(rows, std::memory_order_relaxed);
        build_stream.columns.splice(build_stream.columns.end(), new_block);
        doDebugAsserts();

        bool flag_per_row = needUsedFlagsForPerRightTableRow(table_join);
        const auto & onexprs = table_join->getClauses();

        /// NullMapHolder stores a raw pointer to stored_columns. If any clause stores a nullmap
        /// referencing this block we must not pop the block later
        bool nullmap_stored_for_block = false;

        /// The per-row used flags of the stored block are initialized on the first clause only:
        /// their content does not depend on the inserts, and JoinUsedFlags expects one entry per block.
        bool per_row_flags_initialized = false;

        for (size_t onexpr_idx = 0; onexpr_idx < onexprs.size(); ++onexpr_idx)
        {
            ColumnRawPtrs key_columns;
            for (const auto & name : onexprs[onexpr_idx].key_names_right)
                key_columns.push_back(all_key_columns[name].get());

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

            dispatchJoinShape(
                kind,
                strictness,
                maps_kind,
                [&]<JoinKind kind_, JoinStrictness strictness_, typename Maps>()
                {
                    static_assert((strictness_ == JoinStrictness::Asof) == std::is_same_v<Maps, HashJoin::MapsAsof>);
                    const HashJoinTable::InsertSettings settings{
                        .any_take_last_row = anyTakeLastRow(),
                        .enable_software_prefetch = enableSoftwarePrefetch(),
                        .must_keep_right_blocks = mustKeepRightBlocks(),
                        .asof_type = getAsofType(),
                        .asof_inequality = getAsofInequality()};
                    const BuildResult result = data->tables[onexpr_idx]->insert<Maps>(
                        settings,
                        {key_columns, stored_columns->selector, stored_columns->block_no, null_map, join_mask_col},
                        context.getStream(),
                        [&](size_t bytes) { data->addBytes(data->bucket_bytes, bytes); });
                    is_inserted = result.is_inserted;
                    if (!result.all_values_unique)
                        all_values_unique.store(false, std::memory_order_relaxed);

                    if (flag_per_row && !per_row_flags_initialized)
                    {
                        used_flags->reinit<kind_, strictness_, mapsKindOf<Maps>()>(
                            context.getStream(), stored_columns->block_no, stored_columns->blockRows());
                        per_row_flags_initialized = true;
                    }
                });

            if (!flag_per_row && save_nullmap && is_inserted)
            {
                auto & h = build_stream.nullmaps.emplace_back(stored_columns, null_map_holder);
                data->addBytes(data->nullmaps_allocated_size, h.allocatedBytes());
                nullmap_stored_for_block = true;
            }

            if (!flag_per_row && not_joined_map && has_right_not_joined)
            {
                auto & h = build_stream.nullmaps.emplace_back(stored_columns, std::move(not_joined_map));
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
                data->subBytes(data->allocated_size, data_allocated_bytes);
                /// A set map references no block at all, so every block is dropped here even though its
                /// rows did take part in the join and must stay counted.
                if (maps_kind != JoinMapsKind::Set)
                    data->rows_to_join.fetch_sub(rows, std::memory_order_relaxed);
                /// Nothing was inserted, so no refs to this block exist; null the index entry so
                /// that a stale ref trips the chassert in `StoredColumnsIndex::at` in debug builds
                /// (and dereferences nullptr deterministically in release builds) instead of
                /// silently reading freed memory.
                data->stored_columns_index->clearEntry(stored_columns->block_no);
                build_stream.columns.pop_back();
                doDebugAsserts();
            }

            if (context.joinChecksLimits())
            {
                total_rows = getTotalRowCount();
                total_bytes = getTotalByteCount();
                /// total_bytes here is the pre-shrink size (shrink happens below), so this captures the
                /// build high-water mark for free on the path where a shrink can lower it.
                updatePeakBuildBytes(total_bytes);
            }
        }
    }
    if (!context.joinChecksLimits())
        return true;
    shrinkStoredBlocksToFit(total_bytes, /*force_optimize=*/false, context.getStream());
    return table_join->sizeLimits().check(total_rows, total_bytes, "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

void HashJoin::shrinkStoredBlocksToFit(size_t & total_bytes_in_join, bool force_optimize, size_t stream)
{
    /// The CAS decides once for the whole join; each build stream then rewrites only its own list.
    const auto max_total_bytes_in_join = table_join->sizeLimits().max_bytes;

    const Int64 current_memory_usage = JoinCommon::getCurrentQueryMemoryUsage();
    const Int64 memory_usage_before = memory_usage_before_adding_blocks.load(std::memory_order_relaxed);
    const Int64 query_memory_usage_delta = current_memory_usage - memory_usage_before;
    const Int64 max_total_bytes_for_query = memory_usage_before ? table_join->getMaxMemoryUsage() : 0;

    if (!force_optimize)
    {
        if (shrink_blocks.load(std::memory_order_relaxed))
        {
            /// Another thread already decided to shrink; catch this build stream up.
            shrinkBuildStreamBlocks(data->build_streams[stream]);
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
            shrinkBuildStreamBlocks(data->build_streams[stream]);
            total_bytes_in_join = getTotalByteCount();
            return;
        }
    }
    else
        data->build_streams[stream].shrink_done = false; /// Only `StorageJoin` forces, and its join has one build stream.

    LOG_DEBUG(
        log,
        "Shrinking stored blocks, memory consumption is {} {} calculated by join, {} {} by memory tracker",
        ReadableSize(total_bytes_in_join),
        max_total_bytes_in_join ? fmt::format("/ {}", ReadableSize(max_total_bytes_in_join)) : "",
        ReadableSize(query_memory_usage_delta),
        max_total_bytes_for_query ? fmt::format("/ {}", ReadableSize(max_total_bytes_for_query)) : "");

    /// The other build streams catch up on their next call.
    shrinkBuildStreamBlocks(data->build_streams[stream]);
    /// A forced shrink leaves the build stream not done, so a shrink that the limits decide later still rewrites
    /// the blocks stored until then.
    if (force_optimize)
        data->build_streams[stream].shrink_done = false;

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

void HashJoin::shrinkBuildStreamBlocks(BuildStreamData & build_stream)
{
    if (build_stream.shrink_done)
        return;

    /// Each cloneResized below replaces a stored column object in place, so any emit table built by a
    /// prior query (a persistent StorageJoin builds one per SELECT, then OPTIMIZE/insert runs this) is
    /// left with dangling `const IColumn *`. Bump the generation on every exit - including the exception
    /// paths, where some columns were already replaced - so the next probe rebuilds it against the new
    /// columns. invalidateEmitTable only takes a mutex and increments a counter, so it is unwind-safe.
    SCOPE_EXIT({ data->stored_columns_index->invalidateEmitTable(); });

    for (auto & stored_columns : build_stream.columns)
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

    build_stream.shrink_done = true;
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
        elem.type = JoinCommon::convertTypeToNullable(elem.type);
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

    auto res = HashJoinMethods<JoinKind::Left, JoinStrictness::Any, MapsOne>::joinBlockImpl(
        *this, std::move(keys), block_with_columns_to_add, /* is_join_get = */ true)->next();
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
    JoinResultPtr res;
    const bool joined = dispatchJoinShape(
        kind,
        strictness,
        getMapsKind(),
        [&]<JoinKind kind_, JoinStrictness strictness_, typename Maps>()
        { res = HashJoinMethods<kind_, strictness_, Maps>::joinBlockImpl(*this, std::move(block), sample_block_with_columns_to_add); });

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
            if (stats_collecting_params.build.isCollectionAndUseEnabled() && keyType() != Type::two_level_key8
                && keyType() != Type::two_level_key16)
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

HashJoin::RightTableData::RightTableData(const HashJoinBuildLayout & layout)
    : build_streams(layout.num_build_streams)
{
}

size_t HashJoin::RightTableData::keys() const
{
    size_t res = 0;
    for (const auto & table : tables)
        res += table->keys();
    return res;
}

HashJoin::RightTableData::~RightTableData()
{
    /// Freeing takes time per heap allocation more than per byte, and starting the pool takes a fixed time. So the pool
    /// frees only a table of many objects: stored columns, null maps, `ASOF` lookups. Arena chunks double, so arenas are not counted.
    static constexpr size_t PARALLEL_DESTROY_THRESHOLD_OBJECTS = 2000;

    try
    {
        /// A cancelled build never reached `onBuildPhaseFinish`, so its lists may still be per build stream.
        spliceBuildStreamLists();
        auto & stored = build_streams.front();
        const size_t num_blocks = stored.columns.size();
        size_t num_asof_buckets = 0;
        size_t num_asof_keys = 0;
        for (const auto & table : tables)
        {
            const auto work = table->parallelClearWork();
            num_asof_buckets += work.buckets;
            num_asof_keys += work.keys;
        }
        /// Every stored block has the columns of `sample_block`, though a row store keeps several of them in one buffer.
        const size_t num_objects = num_blocks * sample_block.columns() + stored.nullmaps.size() + num_asof_keys;
        /// The planner gives one build stream to a join with the serial layout or with one thread, so its table is freed serially.
        const size_t num_threads = std::min(build_streams.size(), num_blocks + num_asof_buckets);
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
        std::vector<std::unique_ptr<Arena>> released_arenas;
        for (auto & table : tables)
            std::ranges::move(table->releaseArenas(), std::back_inserter(released_arenas));

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
        for (auto & arena : released_arenas)
            schedule_destroy(arena);
        for (auto & table : tables)
            table->clearInParallel(num_threads, schedule);
        pool.wait();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void HashJoin::RightTableData::spliceBuildStreamLists()
{
    auto & stored = build_streams.front();
    for (auto & build_stream : build_streams | std::views::drop(1))
    {
        stored.columns.splice(stored.columns.end(), build_stream.columns);
        std::ranges::move(build_stream.nullmaps, std::back_inserter(stored.nullmaps));
        build_stream.nullmaps.clear();
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
        auto fill_callback = [&]<JoinKind, JoinStrictness, typename Maps>()
        {
            /// Only RIGHT and FULL joins have non-joined rows, and those never run on a set map.
            if constexpr (SetJoinMaps<Maps>)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Non-joined right rows cannot be produced from a set map");
            else
                rows_added = fillColumns<Maps>(columns_right);
        };

        const auto maps_kind = parent.getMapsKind();
        if (!dispatchJoinShape(parent.kind, parent.strictness, maps_kind, fill_callback))
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

    HashJoinTable::Cursor position;
    std::optional<HashJoin::NullmapList::const_iterator> nulls_position;
    std::optional<HashJoin::StoredBlocksList::const_iterator> used_position;

    std::vector<GatherColumn> emit_gather;
    std::optional<size_t> emit_row_store_row_length;

    bool isBlockInRange(size_t block_no) const
    {
        return num_buckets <= 1 || (block_no % num_buckets) == bucket_idx;
    }

    /// Flat here: a not-joined row is always one inline ref, never a list and never a default.
    void emitOutputs(MutableColumns & columns_right, const PaddedPODArray<UInt64> & words) const
    {
        const RefWordSelection selection{
            .begin = words.data(), .end = words.data() + words.size(), .rows = words.size(), .shape = RefWordShape::Flat};
        gatherJoinOutputColumns(columns_right, emit_gather, selection, emit_row_store_row_length);
    }

    template <typename Maps>
    size_t fillColumns(MutableColumns & columns_right)
    {
        size_t rows_added = 0;

        PaddedPODArray<UInt64> words;
        words.reserve(max_block_size);

        if (flag_per_row)
        {
            /// parent.data->columns is not partitioned by hash bucket, so distribute the stored
            /// right blocks across streams by their globally unique block_no instead
            if (!used_position.has_value())
                used_position = parent.data->build_streams.front().columns.begin();

            auto end = parent.data->build_streams.front().columns.end();

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
            rows_added = parent.data->tables.front()->scanNotJoined<Maps>(
                position, bucket_idx, num_buckets, *parent.used_flags, max_block_size, words);

        emitOutputs(columns_right, words);
        return rows_added;
    }

    void fillNullsFromBlocks(MutableColumns & columns_right, size_t & rows_added)
    {
        /// for parallel iteration, only stream 0 handles nullmaps to avoid duplicates
        if (bucket_idx != 0)
            return;

        if (!nulls_position.has_value())
            nulls_position = parent.data->build_streams.front().nullmaps.begin();

        auto end = parent.data->build_streams.front().nullmaps.end();

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

    dispatchJoinShape(
        kind,
        strictness,
        getMapsKind(),
        [this]<JoinKind kind_, JoinStrictness strictness_, typename Maps>()
        { used_flags->reinit<kind_, strictness_, mapsKindOf<Maps>()>(data->tables.front()->keyFlagsSize()); });

    if (matched_rows_stats)
        matched_rows_stats->prepareRightFlagsIfNeeded(data->build_streams.front().columns);
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

    data->spliceBuildStreamLists();
    StoredBlocksList right_columns = std::move(data->build_streams.front().columns);
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

    for (auto & table : data->tables)
        table->releaseMaps();
    data->build_streams.front().nullmaps.clear();

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

BlocksList HashJoin::releaseBuildStreamBlocks(size_t stream)
{
    auto & build_stream = data->build_streams[stream];
    StoredBlocksList right_columns = std::move(build_stream.columns);
    /// Other threads may still be draining the lists of other build streams, and the sample block is shared, so `data`
    /// stays alive. Its byte counts drop with the released blocks.
    for (const auto & columns : right_columns)
        data->subBytes(data->allocated_size, columns.allocatedBytes());
    for (const auto & nullmap : build_stream.nullmaps)
        data->subBytes(data->nullmaps_allocated_size, nullmap.allocatedBytes());
    build_stream.nullmaps.clear();

    BlocksList result;
    for (auto & columns : right_columns)
        result.emplace_back(data->sample_block.cloneWithColumns(materializeStoredBlock(columns, data->column_access_indexes)));
    return result;
}

void HashJoin::releaseJoinMaps()
{
    for (auto & table : data->tables)
    {
        table->releaseMaps();
        table->releaseArenas();
    }
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
    /// per probe batch and `JoinProbe::addMatchesWithAdditionalFilter` indexes the result by row position,
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

template <JoinKind KIND, typename Maps, JoinStrictness STRICTNESS>
void HashJoin::tryRerangeRightTableDataImpl()
{
    constexpr JoinFeatures<KIND, STRICTNESS, Maps> join_features;
    if constexpr (!join_features.is_all_join || (!join_features.left && !join_features.inner))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Only left or inner join table can be reranged.");
    else
    {
        const StoredBlock * const * stored_columns = data->stored_columns_index->blocksData();
        HashJoinTable & table = *data->tables.front();

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
                /// The rerange runs on one thread after the build, so no other call uses `arena(0)`.
                rows_ref.setRange(RowRef(merged.block_no, start_row).encode(), merged_rows, table.arena(0));
            }
        };

        StoredBlocksList sorted_columns;
        table.forEachMapped<MapsAll>([&](RowRefList & rows_ref) { merge_rows_into_one_block(sorted_columns, rows_ref); });
        doDebugAsserts();
        data->build_streams.front().columns.swap(sorted_columns);
        /// The replaced blocks are destroyed below; null their index entries so that any stale
        /// ref fails loudly instead of reading freed memory. All live cells were rewritten above.
        for (const auto & old_columns : sorted_columns)
            data->stored_columns_index->clearEntry(old_columns.block_no);
        size_t new_blocks_allocated_size = 0;
        for (auto & columns : data->build_streams.front().columns)
        {
            columns.selector = ScatteredBlock::Selector(columns.columns.at(0)->size());
            new_blocks_allocated_size += columns.allocatedBytes();
        }
        data->setBytes(data->allocated_size, new_blocks_allocated_size);

        /// Every stored block was replaced by a merged one with a fresh block_no, so the flags
        /// keyed by the old numbers are stale. Nothing has been marked yet - the probe runs later.
        if (matched_rows_stats && matched_rows_stats->hasRightFlags())
            matched_rows_stats->prepareRightFlags(data->build_streams.front().columns);

        doDebugAsserts();
    }
}

bool HashJoin::isRightTableRerangeEnabled() const
{
    return table_join->allowJoinSorting() && !table_join->getMixedJoinExpression() && isInnerOrLeft(kind)
        && strictness == JoinStrictness::All && data && !data->sorted && table_join->oneDisjunct();
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
    return isRightTableRerangeEnabled() && !data->build_streams.front().columns.empty()
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
            getTotalRowCount());
        return;
    }
    [[maybe_unused]] bool result = dispatchJoinShape(
        kind,
        strictness,
        getMapsKind(),
        [&]<JoinKind kind_, JoinStrictness strictness_, typename Maps>() { tryRerangeRightTableDataImpl<kind_, Maps, strictness_>(); });
    chassert(result);
    data->sorted = true;
}

bool HashJoin::canConvertToFixedHashMap() const
{
    return !conversion_to_fixed_hash_map_attempted && data && data->rows_to_join && table_join->enableJoinFixedHashTableConversion()
        && (keyType() == Type::key32 || keyType() == Type::key64) && table_join->oneDisjunct() && strictness != JoinStrictness::Asof;
}

/// The build leaves two things unset that probing reads: cell prefix sums, and the min/max
/// bounds a direct-addressed map keeps off while concurrent inserts could race. Call after the last insert.
void HashJoin::freezeMapsForProbing()
{
    for (auto & table : data->tables)
        table->freezeForProbing();
}

void HashJoin::reinitUsedFlags()
{
    if (needUsedFlagsForPerRightTableRow(table_join))
        return;

    dispatchJoinShape(
        kind,
        strictness,
        getMapsKind(),
        [this]<JoinKind kind_, JoinStrictness strictness_, typename Maps>()
        { used_flags->reinitAllowShrinking<kind_, strictness_, mapsKindOf<Maps>()>(data->tables.front()->keyFlagsSize()); });
}

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

    if (!table_join->oneDisjunct())
        return;

    const Type key_type = keyType();
    const bool is_fixed_hash_table =
        key_type == Type::key8 || key_type == Type::key16 ||
        key_type == Type::range8_key32 || key_type == Type::range16_key32 ||
        key_type == Type::range17_key32 || key_type == Type::range18_key32 ||
        key_type == Type::range8_key64 || key_type == Type::range16_key64 ||
        key_type == Type::range17_key64 || key_type == Type::range18_key64;
    if (!is_fixed_hash_table)
        return;

    /// For a single distinct build key, the existing `== const` runtime filter specialization
    /// is faster than any hash table probe; leave it active.
    if (getTotalRowCount() <= 1)
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

    auto probe_fn = data->tables.front()->makeSharedRuntimeFilter(build_signed);
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
    const bool is_signed = !right_table_keys.getByPosition(0).type->isValueRepresentedByUnsignedInteger();
    HashJoinTable & table = *data->tables.front();
    if (!table.tryConvertToFixedHashMap(is_signed))
        return;

    LOG_DEBUG(
        log,
        "{}Converted join hash map to fixed hash map (range: {}, keys: {}, type: {})",
        instance_log_id,
        table.keyRange().size,
        table.keys(),
        keyType());

    reinitUsedFlags();
    recomputeBucketBytes();
}

bool HashJoin::recordsRowRefsForStats() const
{
    return table_join->collectExactMatches() && table_join->getMixedJoinExpression() == nullptr;
}

void HashJoin::onBuildPhaseFinish()
{
    freezeMapsForProbing();
    reinitUsedFlags();

    used_flags->finalizePerRowFlags(data->stored_columns_index->size());
    data->spliceBuildStreamLists();

    if (all_values_unique && strictness == JoinStrictness::All && isInnerOrLeft(kind) && table_join->oneDisjunct())
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
        matched_rows_stats->prepareRightFlagsIfNeeded(data->build_streams.front().columns);
    LOG_TRACE(
        log,
        "{}Join data is built, {} and {} rows in hash table",
        instance_log_id,
        ReadableSize(getTotalByteCount()),
        getTotalRowCount());
}

bool HashJoin::hasPostBuildPhase() const
{
    /// key8/key16 are already FixedHashMap, so they don't go through tryConvertToFixedHashMap,
    /// but publishSharedRuntimeFilters still needs to run for them when the feature is on.
    const bool needs_shared_filter_publish =
        data && data->rows_to_join && table_join->oneDisjunct()
        && (keyType() == Type::key8 || keyType() == Type::key16)
        && table_join->joinRuntimeFilterFromFixedHashTable()
        && !table_join->getSharedRuntimeFilterDescriptors().empty();

    return rightTableCanBeReranged() || canConvertToFixedHashMap() || needs_shared_filter_publish;
}

void HashJoin::runPostBuildPhase()
{
    tryRerangeRightTableData();
    tryConvertToFixedHashMap();
    publishSharedRuntimeFilters();
}
}
