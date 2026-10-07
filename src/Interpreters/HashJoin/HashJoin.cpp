#include <algorithm>
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
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/HashTable/FixedHashMap.h>
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
#include <IO/WriteHelpers.h>

#include <Interpreters/IJoin.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/assert_cast.h>
#include <Common/formatReadable.h>
#include <Common/typeid_cast.h>

#include <Interpreters/HashJoin/HashJoinMethods.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Interpreters/HashJoin/fillRowStoreOutputColumns.h>
#include <Interpreters/HashJoin/gatherJoinOutputColumns.h>

#include <numeric>

#include <Processors/QueryPlan/StepAnalyzeInfo.h>

namespace ProfileEvents
{
extern const Event HashJoinBuildMicroseconds;
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

void correctNullabilityInplace(ColumnWithTypeAndName & column, bool nullable)
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

}

Columns HashJoin::materializeStoredBlock(StoredBlock & stored_block, const ColumnAccessIndexes & access_indexes)
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

    auto packed_rank = [](Type type) -> int
    {
        switch (type)
        {
            case Type::keys32: return 1;
            case Type::keys64: return 2;
            case Type::keys128: return 3;
            case Type::keys256: return 4;
            default: return 0;
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

HashJoin::HashJoin(std::shared_ptr<TableJoin> table_join_, SharedHeader right_sample_block_, bool any_take_last_row_, bool allow_set_maps_)
    : table_join(table_join_)
    , kind(table_join->kind())
    , strictness(table_join->strictness())
    , any_take_last_row(any_take_last_row_)
    , asof_inequality(table_join->getAsofInequality())
    , data(std::make_shared<RightTableData>())
    , right_sample_block(*right_sample_block_)
    , max_joined_block_rows(table_join->maxJoinedBlockRows())
    , max_joined_block_bytes(table_join->maxJoinedBlockBytes())
    , joined_block_split_single_row(table_join->joinedBlockAllowSplitSingleRow())
    , enable_lazy_columns_replication(table_join->enableColumnsLazyReplication())
    , enable_prefetch(table_join->enableSoftwarePrefetchInJoin())
    , allow_set_maps(allow_set_maps_)
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
    /// Published before the build: a stored block whose flags are not there yet then reads as not used.
    if (needUsedFlagsForPerRightTableRow(table_join))
        used_flags->need_flags = true;

    if (table_join->collectAnalyzeStats())
        matched_rows_stats = std::make_unique<MatchedRowsStats>(kind, strictness, table_join->analyzeMode());

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
    /// can use a dictionary-aware map. Restricted to one disjunct and non-ASOF for now.
    std::optional<Type> low_cardinality_method;
    if (table_join->oneDisjunct() && strictness != JoinStrictness::Asof)
    {
        const auto & only_clause_key_names = table_join->getOnlyClause().key_names_right;
        if (only_clause_key_names.size() == 1)
            low_cardinality_method = tryGetLowCardinalityMethod(right_table_keys.getByName(only_clause_key_names[0]).column);
    }

    materializeBlockInplace(right_table_keys);
    initRightBlockStructure(data->sample_block);
    data->sample_block = prepareRightBlock(data->sample_block);

    size_t disjuncts_num = table_join->getClauses().size();
    /// maps must exist so `initRowStore` can see the one-disjunct layout via `isRightTableRerangeEnabled`.
    data->maps.resize(disjuncts_num);

    if (!table_join->isRowStoreEnabled() || !isRowStoreSupported() || data->sample_block.columns() == 0)
        data->row_store_state = RowStoreState::Disabled;
    else
        /// Publish the layout before any `FillingRightJoinSide` thread can call `addBlockToJoin`.
        initRowStore(data->sample_block);

    JoinCommon::createMissedColumns(sample_block_with_columns_to_add);

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

    data->type = *selected_join_method;

    LOG_TRACE(log, "Join hash table type: {}", data->type);
    LOG_TEST(
        log,
        "Keys: {}, datatype: {}, kind: {}, strictness: {}, right header: {}",
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

    recomputeMapsBytes();

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
    if (all_fixed && keys_bytes <= 32)
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

void HashJoin::dataMapInit(MapsVariant & map)
{
    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin::dataMapInit called with empty data");

    const auto maps_kind = getMapsKind();
    joinDispatchInit(kind, strictness, map, maps_kind);
    joinDispatch(kind, strictness, map, maps_kind, [&](auto, auto, auto & map_) { map_.create(data->type); });
}


bool HashJoin::preferUseMapsAll() const
{
    return all_join_was_promoted_to_right_any // It means that we built hash tables for ALL strictness, but upon finishing found out that we can switch to RIGHT ANY.
                                              // In this case we still have to use ALL maps.
        || table_join->getMixedJoinExpression() != nullptr;
}

/// A set map answers whether a key is present and nothing else.
/// It fits joins whose result can never contain a value taken from a right row.
bool HashJoin::canUseSetMaps() const
{
    if (!allow_set_maps || !table_join->enableJoinKeyOnlyHashTables())
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
    return empty_for_empty_right && data->rows_to_join.load(std::memory_order_relaxed) == 0;
}

size_t HashJoin::getTotalRowCount() const
{
    if (!data)
        return 0;

    /// A running total: `PartitionedHashJoin` sets it from its own table, and summing the maps would cost a pass per block.
    return data->keys_to_join.load(std::memory_order_relaxed);
}

void HashJoin::recomputeMapsBytes()
{
    if (!data)
        return;

    size_t res = data->pool.allocatedBytes();

    const auto maps_kind = getMapsKind();
    for (const auto & map : data->maps)
    {
        joinDispatch(
            kind, strictness, map, maps_kind, [&](auto, auto, auto & map_) { res += map_.getTotalByteCountImpl(data->type); });
    }

    data->setBytes(data->maps_bytes, res);
}

void HashJoin::doDebugAsserts() const
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    size_t debug_allocated_size = 0;
    size_t debug_nullmaps_allocated_size = 0;
    for (const auto & columns : data->columns)
        debug_allocated_size += columns.allocatedBytes();
    for (const auto & nullmap : data->nullmaps)
        debug_nullmaps_allocated_size += nullmap.allocatedBytes();

    if (data->allocated_size != debug_allocated_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "data->allocated_size != debug_allocated_size ({} != {})",
            data->allocated_size.load(std::memory_order_relaxed),
            debug_allocated_size);

    if (data->nullmaps_allocated_size != debug_nullmaps_allocated_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "data->nullmaps_allocated_size != debug_nullmaps_allocated_size ({} != {})",
            data->nullmaps_allocated_size.load(std::memory_order_relaxed),
            debug_nullmaps_allocated_size);

    const size_t accounted = data->allocated_size.load(std::memory_order_relaxed)
        + data->nullmaps_allocated_size.load(std::memory_order_relaxed) + data->maps_bytes.load(std::memory_order_relaxed);
    if (data->total_bytes.load(std::memory_order_relaxed) != accounted)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "data->total_bytes != allocated + nullmaps + maps_bytes ({} != {})",
            data->total_bytes.load(std::memory_order_relaxed),
            accounted);
#endif
}

size_t HashJoin::getTotalByteCount() const
{
    if (!data)
        return 0;

    doDebugAsserts();
    return getTotalByteCountUnchecked();
}

size_t HashJoin::getTotalByteCountUnchecked() const
{
    if (!data)
        return 0;

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
    return !table_join.hasUsing() && !isUsedByAnotherAlgorithm(table_join) && table_join.strictness() != JoinStrictness::RightAny;
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
        saved_block_sample.insert(right_table_keys.safeGetByPosition(right_table_keys.columns() - 1));
    }

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

void HashJoin::initRowStore(const Block & block, bool may_rerange)
{
    /// Skip initializing if it's already initialized or disabled.
    if (data->row_store_state != RowStoreState::Enabled)
        return;

    /// Skip using row store when the right table rerange optimization could get triggered.
    /// TODO: allow row store when right table could get reranged and build the reranged table
    /// based on the row store instead.
    if (may_rerange && isRightTableRerangeEnabled())
    {
        data->row_store_state = RowStoreState::Disabled;
        return;
    }

    /// Extract columns suitable for row store.
    Block block_to_save = filterColumnsPresentInSampleBlock(block, savedBlockSample());
    const auto & columns = block_to_save.getColumns();
    const auto types = block_to_save.getDataTypes();
    ColumnAccessIndexes access_indexes;
    access_indexes.reserve(columns.size());
    Columns row_store_columns;
    DataTypes row_store_types;
    size_t remaining_columns = 0;
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (isRowStorageUseful(columns[i]))
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
    for (auto & access_index : access_indexes)
    {
        if (access_index.type != ColumnAccessIndex::Type::RowStore)
            continue;
        const auto & field = (*layout)[access_index.index];
        access_index.field_offset = field.offset;
        access_index.field_size = field.size;
        access_index.is_nullable = field.is_nullable;
    }

    data->row_store_layout = std::move(layout);
    data->column_access_indexes = std::move(access_indexes);
    data->row_store_state = RowStoreState::Initialized;

    LOG_DEBUG(log, "Initialized Row store with {} columns", row_store_columns.size());
}

RowDataStorePtr HashJoin::createRowStoreForBlock(const Block & block) const
{
    if (data->row_store_state != RowStoreState::Initialized)
        return nullptr;
    Block block_to_save = filterColumnsPresentInSampleBlock(block, savedBlockSample());
    auto [columns, _] = extractRowStoreColumns(block_to_save, data->column_access_indexes);
    return RowDataStore::create(data->row_store_layout, columns);
}

StoredBlock HashJoin::createStoredBlock(const Block & block_to_save, ScatteredBlock::Selector selector, RowDataStorePtr row_store) const
{
    if (data->row_store_state != RowStoreState::Initialized)
        return StoredBlock(block_to_save.getColumns(), std::move(selector), std::move(row_store));

    auto [row_store_columns, remaining_columns] = extractRowStoreColumns(block_to_save, data->column_access_indexes);
    if (!row_store)
        row_store = RowDataStore::create(data->row_store_layout, row_store_columns);
    return StoredBlock(std::move(remaining_columns), std::move(selector), std::move(row_store));
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

bool HashJoin::addBlockToJoin(const Block & source_block, size_t /* num_rows */, size_t /* worker_id */, bool check_limits)
{
    /// `materializeColumnsFromRightBlock` dereferences `data`, so the identical check in the
    /// overload below is reached too late to guard it.
    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Join data was released");

    auto materialized = materializeColumnsFromRightBlock(source_block);
    return addBlockToJoin(materialized, ScatteredBlock::Selector(materialized.rows()), check_limits);
}

bool HashJoin::addBlockToJoin(const Block & block, ScatteredBlock::Selector selector, bool check_limits, RowDataStorePtr row_store)
{
    ProfileEventTimeIncrement<Microseconds> build_watch(ProfileEvents::HashJoinBuildMicroseconds);

    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Join data was released");

    /// RowRef::row_no is UInt32 (not size_t) for hash table Cell memory efficiency.
    /// It's possible to split bigger blocks and insert them by parts here. But it would be a dead code.
    if (unlikely(selector.size() > std::numeric_limits<decltype(RowRef::row_no)>::max()))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Too many rows in right table block for HashJoin: {}", selector.size());

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
                const auto & asof_column_nullable = assert_cast<const ColumnNullable &>(*asof_column.column).getNullMapData();

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
    const auto & right_key_names = table_join->getAllNames(JoinTableSide::Right);
    ColumnPtrMap all_key_columns(right_key_names.size());
    for (const auto & column_name : right_key_names)
    {
        const auto & column = block.getByName(column_name).column;
        auto prepared_key_column = removeSpecialRepresentations(column->convertToFullColumnIfConst());
        /// Keep the dictionary for the single-LowCardinality-column maps; their key getter needs it.
        if (!isLowCardinalityType(data->type))
            prepared_key_column = prepared_key_column->convertToFullColumnIfLowCardinality();
        all_key_columns[column_name] = prepared_key_column;
    }

    Block block_to_save = filterColumnsPresentInSampleBlock(block, savedBlockSample());
    if (shrink_blocks)
        block_to_save = block_to_save.shrinkToFit();

    const auto maps_kind = getMapsKind();

    size_t total_rows = 0;
    size_t total_bytes = 0;
    {
        /** We do not allocate memory for stored blocks inside HashJoin, only for hash table.
          * In case when we have all the blocks allocated before the first `addBlockToJoin` call, will already be quite high.
          * In that case memory consumed by stored blocks will be underestimated.
          */
        if (!memory_usage_before_adding_blocks)
            memory_usage_before_adding_blocks = JoinCommon::getCurrentQueryMemoryUsage();

        assertBlocksHaveEqualStructureAllowReplicated(data->sample_block, block_to_save, "joined block");

        if (storage_join_lock)
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "addBlockToJoin called when HashJoin locked to prevent updates");

        StoredBlock new_stored_columns = createStoredBlock(block_to_save, std::move(selector), std::move(row_store));
        const size_t data_allocated_bytes = new_stored_columns.allocatedBytes();
        doDebugAsserts();
        /// Register the block and account for it while a local list still owns it: `splice` cannot throw,
        /// so `data->columns`, `data->allocated_size` and `data->rows_to_join` always describe the same
        /// set of stored blocks. Same ordering as `tryRerangeRightTableDataImpl`; a list node keeps its
        /// address across the splice.
        StoredBlocksList new_block;
        new_block.push_back(std::move(new_stored_columns));
        StoredBlock * stored_columns = &new_block.back();
        stored_columns->block_no = data->stored_columns_index->add(stored_columns);
        data->addBytes(data->allocated_size, data_allocated_bytes);
        data->rows_to_join.fetch_add(rows, std::memory_order_relaxed);
        data->columns.splice(data->columns.end(), new_block);
        auto stored_columns_it = std::prev(data->columns.end());
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
            if (!flag_per_row && isRightOrFull(kind) && join_mask_col.hasData())
            {
                ///  - build mask in the source block row space
                ///  - set bits only for rows that belong to THIS selector's partition
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
                    using Methods = HashJoinMethods<kind_, strictness_, std::decay_t<decltype(map)>>;

                    BuildResult result;
                    Methods::insertFromBlockImpl(
                        *this,
                        data->type,
                        map,
                        key_columns,
                        key_sizes[onexpr_idx],
                        stored_columns->block_no,
                        stored_columns->selector,
                        null_map,
                        join_mask_col,
                        data->pool,
                        result);
                    data->keys_to_join.fetch_add(result.new_keys, std::memory_order_relaxed);

                    is_inserted = result.is_inserted;
                    if (!result.all_values_unique)
                        all_values_unique.store(false, std::memory_order_relaxed);

                    if (flag_per_row && !per_row_flags_initialized)
                    {
                        used_flags->reinit<kind_, strictness_, mapsKindOf<decltype(map)>()>(
                            stored_columns->block_no, stored_columns->blockRows(), stored_columns->selector);
                        per_row_flags_initialized = true;
                    }
                });
            recomputeMapsBytes();

            if (!flag_per_row && save_nullmap && is_inserted)
            {
                auto & h = data->nullmaps.emplace_back(stored_columns, null_map_holder);
                data->addBytes(data->nullmaps_allocated_size, h.allocatedBytes());
                nullmap_stored_for_block = true;
            }

            if (!flag_per_row && not_joined_map && (is_inserted || has_right_not_joined))
            {
                auto & h = data->nullmaps.emplace_back(stored_columns, std::move(not_joined_map));
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
                data->columns.erase(stored_columns_it);
                stored_columns = nullptr;
                doDebugAsserts();
            }

            if (check_limits)
            {
                total_rows = getTotalRowCount();
                total_bytes = getTotalByteCountUnchecked();
                /// total_bytes here is the pre-shrink size (shrink happens below), so this captures the
                /// build high-water mark for free on the path where a shrink can lower it.
                updatePeakBuildBytes(total_bytes);
            }
        }
    }
    if (!check_limits)
        return true;
    shrinkStoredBlocksToFit(total_bytes);
    return table_join->sizeLimits().check(total_rows, total_bytes, "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

void HashJoin::shrinkStoredBlocksToFit(size_t & total_bytes_in_join, bool force_optimize)
{
    const auto max_total_bytes_in_join = table_join->sizeLimits().max_bytes;

    const Int64 current_memory_usage = JoinCommon::getCurrentQueryMemoryUsage();
    const Int64 query_memory_usage_delta = current_memory_usage - memory_usage_before_adding_blocks;
    const Int64 max_total_bytes_for_query = memory_usage_before_adding_blocks ? table_join->getMaxMemoryUsage() : 0;

    if (!force_optimize)
    {
        /// Already shrunk, and every block stored since arrived shrunk.
        if (shrink_blocks)
            return;

        /** If accounted data size is more than half of `max_bytes_in_join`
        * or query memory consumption growth from the beginning of adding blocks (estimation of memory consumed by join using memory tracker)
        * is bigger than half of all memory available for query,
        * then shrink stored blocks to fit.
        */
        shrink_blocks = (max_total_bytes_in_join && total_bytes_in_join > max_total_bytes_in_join / 2)
            || (max_total_bytes_for_query && query_memory_usage_delta > max_total_bytes_for_query / 2);
        if (!shrink_blocks)
            return;
    }
    else
        shrink_blocks = true;

    LOG_DEBUG(
        log,
        "Shrinking stored blocks, memory consumption is {} {} calculated by join, {} {} by memory tracker",
        ReadableSize(total_bytes_in_join),
        max_total_bytes_in_join ? fmt::format("/ {}", ReadableSize(max_total_bytes_in_join)) : "",
        ReadableSize(query_memory_usage_delta),
        max_total_bytes_for_query ? fmt::format("/ {}", ReadableSize(max_total_bytes_for_query)) : "");

    /// `cloneResized` replaces the column objects, so an emit table's raw pointers and those in
    /// `replicated_columns` go stale. A throw part-way through still leaves some replaced.
    bool any_column_replaced = false;
    SCOPE_EXIT({
        if (any_column_replaced)
            data->stored_columns_index->invalidateEmitTable();
    });

    for (auto & stored_columns : data->columns)
    {
        size_t old_size = stored_columns.allocatedBytes();

        try
        {
            for (auto & column : stored_columns.columns)
            {
                column = column->cloneResized(column->size());
                any_column_replaced = true;
            }

            stored_columns.rebuildReplicatedColumns();
        }
        catch (...)
        {
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
    }

    auto new_total_bytes_in_join = getTotalByteCountUnchecked();
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

    return runJoinDispatch(ScatteredBlock(std::move(block)));
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
        LOG_TEST(log, "Join data has been already released");
        return;
    }

    LOG_TEST(log, "Join data is being destroyed, {} bytes and {} rows in hash table", getTotalByteCountUnchecked(), getTotalRowCount());
}

/// Appends one hash map cell's not-joined rows: as a flat run of encoded ref words for the
/// columnar columns, and resolved to row pointers for the row store. Returns the rows appended.
/// Always inlined: it runs once per map key in the `fillColumns` scan. The run and chain tags grew
/// `RowRefList::ForwardIterator` past the inlining threshold. An out-of-line call per key cost the
/// RIGHT ALL non-joined emission 10-14 %.
template <typename Mapped>
struct CollectorNonJoined
{
    template <bool with_row_store, bool with_columns>
    static ALWAYS_INLINE size_t collect(
        const Mapped & mapped,
        [[maybe_unused]] const RowDataStore * const * block_row_stores,
        [[maybe_unused]] PaddedPODArray<UInt64> & words,
        [[maybe_unused]] RowStorePointers & row_store_ptrs,
        [[maybe_unused]] std::optional<size_t> & row_store_batch_size)
    {
        constexpr bool mapped_asof = std::is_same_v<Mapped, AsofRowRefs>;
        [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<Mapped, RowRef>;

        [[maybe_unused]] auto collect_word = [&](UInt64 ref_word)
        {
            if constexpr (with_columns)
                words.push_back(ref_word);
            if constexpr (with_row_store)
            {
                const auto * row_store = block_row_stores[refWordBlockNo(ref_word)];
                row_store_ptrs.ptrs.emplace_back(row_store->getRowAt(refWordRowNo(ref_word)));
                if (!row_store_batch_size)
                    row_store_batch_size = row_store->getBatchSize();
            }
        };

        if constexpr (mapped_asof)
        {
            return 0;
        }
        else if constexpr (mapped_one)
        {
            collect_word(mapped.encode());
            return 1;
        }
        else
        {
            size_t rows = 0;
            for (auto it = mapped.begin(); it.ok(); ++it, ++rows)
                collect_word(*it);
            return rows;
        }
    }
};

/// Stream from not joined earlier rows of the right table.
/// Based on:
///   - map offsetInternal saved in used_flags for single disjuncts
///   - flags in BlockWithFlags for multiple disjuncts
class NotJoinedHash final : public NotJoinedBlocks::RightColumnsFiller
{
public:
    NotJoinedHash(const HashJoin & parent_, UInt64 max_block_size_, bool flag_per_row_)
        : parent(parent_)
        , max_block_size(max_block_size_)
        , flag_per_row(flag_per_row_)
    {
        if (parent.data == nullptr)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot join after data has been released");

        const Block & saved_block_sample = parent.savedBlockSample();

        type_name.reserve(saved_block_sample.columns());
        for (const auto & column : saved_block_sample)
            type_name.emplace_back(column.name, column.type);

        std::vector<size_t> positions(saved_block_sample.columns());
        std::iota(positions.begin(), positions.end(), 0);
        EmitPlan plan = planJoinEmit(*parent.data, positions, type_name, /*with_gather=*/true);
        output_access_indexes = std::move(plan.access_indexes);
        emit_gather = std::move(plan.gather);
        has_row_store = plan.has_row_store;
        has_columns = plan.has_columns;
    }

    Block getEmptyBlock() override { return parent.savedBlockSample().cloneEmpty(); }

    size_t fillColumns(MutableColumns & columns_right) override
    {
        size_t rows_added = 0;
        dispatchOutputs(
            [&]<bool with_row_store, bool with_columns>()
            {
                auto fill_callback = [&](auto, auto, auto & map)
                {
                    /// Only RIGHT and FULL joins have non-joined rows, and those never run on a set map.
                    if constexpr (SetJoinMaps<decltype(map)>)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Non-joined right rows cannot be produced from a set map");
                    else
                        rows_added = fillColumnsFromMap<with_row_store, with_columns>(map, columns_right);
                };

                const auto maps_kind = parent.getMapsKind();
                if (!joinDispatch(parent.kind, parent.strictness, parent.data->maps.front(), maps_kind, fill_callback))
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR, "Unknown JOIN strictness '{}' (must be on of: ANY, ALL, ASOF)", parent.strictness);
            });

        if (!flag_per_row)
        {
            dispatchOutputs([&]<bool with_row_store, bool with_columns>()
                            { fillNullsFromBlocks<with_row_store, with_columns>(columns_right, rows_added); });
        }

        if (auto * stats = parent.matched_rows_stats.get())
            stats->collectNonJoined(rows_added);

        return rows_added;
    }

private:
    const HashJoin & parent;
    UInt64 max_block_size;
    bool flag_per_row;

    std::any position;
    std::optional<HashJoin::StoredBlocksList::const_iterator> used_position;
    std::optional<HashJoin::NullmapList::const_iterator> nulls_position;

    ColumnAccessIndexes output_access_indexes;
    NamesAndTypes type_name;
    bool has_row_store = false;
    bool has_columns = false;

    std::vector<GatherColumn> emit_gather;

    /// The row store needs its rows resolved to pointers and the columnar columns need the ref
    /// words. A scan collects only what its columns read.
    template <typename F>
    void dispatchOutputs(F && f) const
    {
        if (!has_row_store)
            f.template operator()<false, true>();
        else if (!has_columns)
            f.template operator()<true, false>();
        else
            f.template operator()<true, true>();
    }

    template <bool with_row_store, bool with_columns, typename Maps>
    size_t fillColumnsFromMap(const Maps & maps, MutableColumns & columns_right)
    {
        switch (parent.data->type)
        {
#define M(TYPE) \
    case HashJoin::Type::TYPE: return fillColumns<with_row_store, with_columns>(*maps.TYPE, columns_right);
            APPLY_FOR_JOIN_VARIANTS(M)
#undef M
        }
        UNREACHABLE();
    }

    /// Flat here: a not-joined row is always one inline ref, never a list and never a default.
    void emitColumnarOutputs(MutableColumns & columns_right, const PaddedPODArray<UInt64> & words) const
    {
        const RefWordSelection selection{
            .begin = words.data(), .end = words.data() + words.size(), .rows = words.size(), .shape = RefWordShape::Flat};
        EmitScratch scratch;

        for (size_t dst_idx = 0; dst_idx < output_access_indexes.size(); ++dst_idx)
            if (output_access_indexes[dst_idx].type == ColumnAccessIndex::Type::Columns)
                gatherColumn(*columns_right[dst_idx], emit_gather[dst_idx], selection, scratch);
    }

    template <bool with_row_store, bool with_columns, typename Map>
    size_t fillColumns(const Map & map, MutableColumns & columns_right)
    {
        size_t rows_added = 0;

        [[maybe_unused]] PaddedPODArray<UInt64> words;
        if constexpr (with_columns)
            words.reserve(max_block_size);

        [[maybe_unused]] RowStorePointers row_store_ptrs;
        [[maybe_unused]] std::optional<size_t> row_store_batch_size;
        if constexpr (with_row_store)
            row_store_ptrs.ptrs.reserve(max_block_size);

        if (flag_per_row)
        {
            if (!used_position.has_value())
                used_position = parent.data->columns.begin();

            const auto end = parent.data->columns.end();
            for (auto & it = *used_position; it != end && rows_added < max_block_size; ++it)
            {
                const auto & mapped_block = *it;
                const size_t rows = mapped_block.blockRows();

                for (size_t row = 0; row < rows; ++row)
                {
                    if (!parent.isUsed(mapped_block.block_no, row))
                    {
                        ++rows_added;
                        if constexpr (with_columns)
                            words.push_back(RowRef(mapped_block.block_no, row).encode());
                        if constexpr (with_row_store)
                        {
                            const auto & row_store = mapped_block.row_store;
                            row_store_ptrs.ptrs.emplace_back(row_store->getRowAt(row));
                            if (!row_store_batch_size)
                                row_store_batch_size = row_store->getBatchSize();
                        }
                    }
                }
            }
        }
        else
        {
            using Mapped = typename Map::mapped_type;
            using Iterator = typename Map::const_iterator;

            if (!position.has_value())
                position = std::make_any<Iterator>(map.begin());

            Iterator & it = std::any_cast<Iterator &>(position);
            auto end = map.end();
            const RowDataStore * const * block_row_stores = parent.data->stored_columns_index->rowStoresData();

            for (; it != end; ++it)
            {
                if (parent.isUsed(map.offsetInternal(it.getPtr())))
                    continue;

                const Mapped & mapped = it->getMapped();
                rows_added += CollectorNonJoined<Mapped>::template collect<with_row_store, with_columns>(
                    mapped, block_row_stores, words, row_store_ptrs, row_store_batch_size);

                if (rows_added >= max_block_size)
                {
                    ++it;
                    break;
                }
            }
        }

        if constexpr (with_columns)
            emitColumnarOutputs(columns_right, words);
        if constexpr (with_row_store)
            fillRowStoreOutputColumns(columns_right, output_access_indexes, row_store_ptrs, row_store_batch_size, type_name);
        return rows_added;
    }

    template <bool with_row_store, bool with_columns>
    void fillNullsFromBlocks(MutableColumns & columns_right, size_t & rows_added)
    {
        if (!nulls_position.has_value())
            nulls_position = parent.data->nullmaps.begin();

        const auto end = parent.data->nullmaps.end();
        size_t nulls_added = 0;

        [[maybe_unused]] PaddedPODArray<UInt64> words;
        if constexpr (with_columns)
            words.reserve(max_block_size);

        [[maybe_unused]] RowStorePointers row_store_ptrs;
        [[maybe_unused]] std::optional<size_t> row_store_batch_size;
        if constexpr (with_row_store)
            row_store_ptrs.ptrs.reserve(max_block_size);

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
                    if constexpr (with_columns)
                        words.push_back(RowRef(columns->block_no, row).encode());
                    if constexpr (with_row_store)
                    {
                        const auto & row_store = columns->row_store;
                        row_store_ptrs.ptrs.emplace_back(row_store->getRowAt(row));
                        if (!row_store_batch_size)
                            row_store_batch_size = row_store->getBatchSize();
                    }
                }
            }
        }

        if constexpr (with_columns)
            emitColumnarOutputs(columns_right, words);
        if constexpr (with_row_store)
            fillRowStoreOutputColumns(columns_right, output_access_indexes, row_store_ptrs, row_store_batch_size, type_name);
        rows_added += nulls_added;
    }
};

bool HashJoin::hasNonJoinedRows() const
{
    if (!isRightOrFull(kind) || !data || data->rows_to_join.load(std::memory_order_relaxed) == 0)
        return false;

    if (!data->nullmaps.empty() || needUsedFlagsForPerRightTableRow(table_join))
        return true;

    return used_flags && !used_flags->allOffsetFlagsSet();
}

IBlocksStreamPtr
HashJoin::getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const
{
    if (!JoinCommon::hasNonJoinedBlocks(*table_join) || !hasNonJoinedRows())
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

    auto non_joined = std::make_unique<NotJoinedHash>(*this, max_block_size, flag_per_row);
    return std::make_unique<NotJoinedBlocks>(std::move(non_joined), result_sample_block, left_columns_count, *table_join);
}

void HashJoin::reuseJoinedData(const HashJoin & join)
{
    data = join.data;
    peak_build_bytes = join.peak_build_bytes;
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

    used_flags->setUnsetOffsetCount(data->keys_to_join.load(std::memory_order_relaxed));
    if (matched_rows_stats)
        matched_rows_stats->prepareRightFlagsIfNeeded(data->columns);
}

BlocksList HashJoin::releaseJoinedBlocks(bool restructure)
{
    /// A set map keeps no right block, so handing back an empty list would silently lose the right side.
    if (getMapsKind() == JoinMapsKind::Set)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Right blocks of a key-only join were asked for; a key-only join keeps none");

    LOG_TRACE(log, "Join data is being released, {} bytes and {} rows in hash table", getTotalByteCountUnchecked(), getTotalRowCount());

    if (!data)
        return {};

    if (restructure)
    {
        data->maps.clear();
        data->nullmaps.clear();

        BlocksList restored_blocks;
        for (auto & saved_columns : data->columns)
            restored_blocks.emplace_back(restoreRightBlock(
                data->sample_block.cloneWithColumns(materializeStoredBlock(saved_columns, data->column_access_indexes)),
                right_sample_block));
        data->columns.clear();

        data.reset();
        return restored_blocks;
    }

    Block sample_block = std::move(data->sample_block);
    const auto column_access_indexes = data->column_access_indexes;
    StoredBlocksList right_columns = std::move(data->columns);
    data.reset();
    /// Materializing allocates, so it can throw here with `data` already gone.
    fiu_do_on(FailPoints::hash_join_throw_after_data_release, {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure after the join data was released");
    });

    BlocksList result;
    for (auto & columns : right_columns)
        result.emplace_back(sample_block.cloneWithColumns(materializeStoredBlock(columns, column_access_indexes)));
    return result;
}

Block HashJoin::restoreRightBlock(const Block & saved_block, const Block & right_sample_block)
{
    Block restored;
    for (const auto & sample_column : right_sample_block)
    {
        auto column = saved_block.getByName(sample_column.name);
        correctNullabilityInplace(column, isNullableOrLowCardinalityNullable(sample_column.type));
        restored.insert(std::move(column));
    }
    return restored;
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
                rows_ref.setRange(RowRef(merged.block_no, start_row).encode(), merged_rows, data->pool);
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

        StoredBlocksList old_all = std::move(data->columns);
        data->nullmaps.clear();
        data->columns = std::move(sorted_columns);

        /// The replaced blocks are destroyed below; null their index entries so that any stale
        /// ref fails loudly instead of reading freed memory. All live cells were rewritten above.
        for (const auto & old_columns : old_all)
            data->stored_columns_index->clearEntry(old_columns.block_no);
        size_t new_blocks_allocated_size = 0;
        for (auto & columns : data->columns)
        {
            columns.selector = ScatteredBlock::Selector(columns.columns.at(0)->size());
            new_blocks_allocated_size += columns.allocatedBytes();
        }
        data->setBytes(data->allocated_size, new_blocks_allocated_size);

        /// Every stored block was replaced by a merged one with a fresh block_no, so the flags
        /// keyed by the old numbers are stale. Nothing has been marked yet - the probe runs later.
        if (matched_rows_stats && matched_rows_stats->hasRightFlags())
            matched_rows_stats->prepareRightFlags(data->columns);

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
    return isRightTableRerangeEnabled() && data->hasStoredColumns()
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

    used_flags->setUnsetOffsetCount(data->keys_to_join.load(std::memory_order_relaxed));
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
    ProfileEventTimeIncrement<Microseconds> build_watch(ProfileEvents::HashJoinBuildMicroseconds);

    reinitUsedFlags();

    if (all_values_unique && strictness == JoinStrictness::All && isInnerOrLeft(kind) && data->maps.size() == 1)
    {
        strictness = JoinStrictness::RightAny;
        all_join_was_promoted_to_right_any = true;
        LOG_DEBUG(log, "Promoting join strictness to RightAny, because all values in the right table are unique");
    }

    /// In case addBlockToJoin is returning early
    /// we take a peak snapshot
    size_t total_bytes = getTotalByteCount();
    updatePeakBuildBytes(total_bytes);

    if (matched_rows_stats)
        matched_rows_stats->prepareRightFlagsIfNeeded(data->columns);
    LOG_TRACE(log, "Join data is built, {} and {} rows in hash table", ReadableSize(getTotalByteCountUnchecked()), getTotalRowCount());
}

bool HashJoin::hasPostBuildPhase() const
{
    return rightTableCanBeReranged();
}

void HashJoin::runPostBuildPhase()
{
    tryRerangeRightTableData();
    recomputeMapsBytes();
}
}
