#include <algorithm>
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
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/assert_cast.h>
#include <Common/formatReadable.h>
#include <Common/typeid_cast.h>

#include <Interpreters/HashJoin/HashJoinMethods.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>

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
extern const int LOGICAL_ERROR;
extern const int TYPE_MISMATCH;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int INVALID_JOIN_ON_EXPRESSION;
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

HashJoin::HashJoin(std::shared_ptr<TableJoin> table_join_, SharedHeader right_sample_block_, bool allow_set_maps_)
    : table_join(table_join_)
    , kind(table_join->kind())
    , strictness(table_join->strictness())
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

size_t HashJoin::getTotalByteCountUnchecked() const
{
    if (!data)
        return 0;

    return data->total_bytes.load(std::memory_order_relaxed);
}

bool HashJoin::isUsedByAnotherAlgorithm() const
{
    return JoinCommon::isUsedByAnotherAlgorithm(*table_join);
}

bool HashJoin::canRemoveColumnsFromLeftBlock() const
{
    return JoinCommon::canRemoveColumnsFromLeftBlock(*table_join);
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

void HashJoin::initRowStore(const Block & block)
{
    /// Skip initializing if it's already initialized or disabled.
    if (data->row_store_state != RowStoreState::Enabled)
        return;

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

StoredBlock HashJoin::createStoredBlock(const Block & block_to_save, ScatteredBlock::Selector selector, RowDataStorePtr row_store) const
{
    if (data->row_store_state != RowStoreState::Initialized)
        return StoredBlock(block_to_save.getColumns(), std::move(selector), std::move(row_store));

    auto [row_store_columns, remaining_columns] = extractRowStoreColumns(block_to_save, data->column_access_indexes);
    if (!row_store)
        row_store = RowDataStore::create(data->row_store_layout, row_store_columns);
    return StoredBlock(std::move(remaining_columns), std::move(selector), std::move(row_store));
}

Block HashJoinTypes::prepareRightBlock(const Block & block, const Block & saved_block_sample_)
{
    Block prepared_block = JoinCommon::materializeColumnsFromRightBlock(block, saved_block_sample_);
    return filterColumnsPresentInSampleBlock(prepared_block, saved_block_sample_);
}

Block HashJoin::prepareRightBlock(const Block & block) const
{
    return prepareRightBlock(block, savedBlockSample());
}

void HashJoin::shrinkStoredBlocksToFit(size_t & total_bytes_in_join)
{
    const auto max_total_bytes_in_join = table_join->sizeLimits().max_bytes;

    const Int64 current_memory_usage = JoinCommon::getCurrentQueryMemoryUsage();

    LOG_DEBUG(
        log,
        "Shrinking stored blocks, memory consumption is {} {} calculated by join, {} by memory tracker",
        ReadableSize(total_bytes_in_join),
        max_total_bytes_in_join ? fmt::format("/ {}", ReadableSize(max_total_bytes_in_join)) : "",
        ReadableSize(current_memory_usage));

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

void HashJoin::checkTypesOfKeys(const Block & block) const
{
    for (const auto & onexpr : table_join->getClauses())
    {
        JoinCommon::checkTypesOfKeys(block, onexpr.key_names_left, right_table_keys, onexpr.key_names_right);
    }
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

void HashJoin::reuseJoinedData(const HashJoin & join)
{
    data = join.data;

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

    doDebugAsserts();
    LOG_TRACE(log, "Join data is built, {} and {} rows in hash table", ReadableSize(getTotalByteCountUnchecked()), getTotalRowCount());
}
}
