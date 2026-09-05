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
#include <Functions/castTypeToEither.h>

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
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Interpreters/HashJoin/fillJoinOutputColumns.h>

#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <Processors/QueryPlan/StepAnalyzeInfo.h>

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

static HashJoin::Type chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes, bool use_two_level_maps);
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
    bool is_concurrent_hash_join_,
    const HashJoinStatsCollectingParams & stats_collecting_params_)
    : table_join(table_join_)
    , kind(table_join->kind())
    , strictness(table_join->strictness())
    , any_take_last_row(any_take_last_row_)
    , reserve_num(reserve_num_)
    , instance_id(instance_id_)
    , asof_inequality(table_join->getAsofInequality())
    , data(std::make_shared<RightTableData>())
    , right_sample_block(*right_sample_block_)
    , max_joined_block_rows(table_join->maxJoinedBlockRows())
    , max_joined_block_bytes(table_join->maxJoinedBlockBytes())
    , joined_block_split_single_row(table_join->joinedBlockAllowSplitSingleRow())
    , enable_lazy_columns_replication(table_join->enableColumnsLazyReplication())
    , enable_prefetch(table_join->enableSoftwarePrefetchInJoin())
    , is_concurrent_hash_join(is_concurrent_hash_join_)
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
    if (table_join->oneDisjunct() && !is_concurrent_hash_join && strictness != JoinStrictness::Asof)
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
            selected_join_method = chooseMethod(key_columns, asof_key_sizes, /*use_two_level_maps=*/ is_concurrent_hash_join);
            asof_key_sizes.push_back(asof_size);
        }
        else
        {
            /// Choose data structure to use for JOIN.
            auto current_join_method = chooseMethod(key_columns, key_sizes.emplace_back(), /*use_two_level_maps=*/ is_concurrent_hash_join);
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

static HashJoin::Type chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes, bool use_two_level_maps)
{
    using Type = HashJoin::Type;

    if (!use_two_level_maps)
        return chooseMethod(key_columns, key_sizes);

    // if `use_two_level_maps == true` returns two-level version of the map
    switch (auto type = chooseMethod(key_columns, key_sizes))
    {
        case Type::key32:
            return Type::two_level_key32;
        case Type::key64:
            return Type::two_level_key64;
        case Type::keys32:
            return Type::two_level_keys32;
        case Type::keys64:
            return Type::two_level_keys64;
        case Type::keys128:
            return Type::two_level_keys128;
        case Type::keys256:
            return Type::two_level_keys256;
        case Type::key_string:
            return Type::two_level_key_string;
        case Type::key_fixed_string:
            return Type::two_level_key_fixed_string;
        case Type::hashed:
            return Type::two_level_hashed;
        default:
            return type;
    }
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

template <typename KeyGetter, bool is_asof_join>
static KeyGetter createKeyGetter(const ColumnRawPtrs & key_columns, const Sizes & key_sizes)
{
    if constexpr (is_asof_join)
    {
        auto key_column_copy = key_columns;
        auto key_size_copy = key_sizes;
        key_column_copy.pop_back();
        key_size_copy.pop_back();
        return KeyGetter(key_column_copy, key_size_copy, nullptr);
    }
    else
        return KeyGetter(key_columns, key_sizes, nullptr);
}

void HashJoin::dataMapInit(MapsVariant & map)
{
    const auto maps_kind = getMapsKind();
    joinDispatchInit(kind, strictness, map, maps_kind);
    joinDispatch(kind, strictness, map, maps_kind, [&](auto, auto, auto & map_) { map_.create(data->type, reserve_num); });

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
    if (!data->nullmaps.empty())
        return;

    doDebugAsserts();
    for (auto & stored_columns : data->columns)
    {
        data->allocated_size -= stored_columns.allocatedBytes();
        /// No cell refers to the block, but null the index entry anyway, so that a stale reference
        /// trips the chassert in `StoredColumnsIndex::at` rather than reading freed memory.
        data->stored_columns_index->clearEntry(stored_columns.block_no);
    }
    data->columns.clear();
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
    return isInnerOrRight(getKind()) && data->rows_to_join == 0;
}

size_t HashJoin::getTotalRowCount() const
{
    if (!data)
        return 0;

    size_t res = 0;

    const auto maps_kind = getMapsKind();
    for (const auto & map : data->maps)
    {
        joinDispatch(
            kind, strictness, map, maps_kind, [&](auto, auto, auto & map_) { res += map_.getTotalRowCount(data->type); });
    }
    return res;
}

void HashJoin::doDebugAsserts() const
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    size_t debug_allocated_size = 0;
    for (const auto & columns : data->columns)
        debug_allocated_size += columns.allocatedBytes();

    if (data->allocated_size != debug_allocated_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "data->allocated_size != debug_allocated_size ({} != {})",
            data->allocated_size,
            debug_allocated_size);

    size_t debug_nullmaps_allocated_size = 0;
    for (const auto & nullmap : data->nullmaps)
        debug_nullmaps_allocated_size += nullmap.allocatedBytes();

    if (data->nullmaps_allocated_size != debug_nullmaps_allocated_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "data->nullmaps_allocated_size != debug_nullmaps_allocated_size ({} != {})",
            data->nullmaps_allocated_size,
            debug_nullmaps_allocated_size);
#endif
}

size_t HashJoin::getTotalByteCount() const
{
    if (!data)
        return 0;

    doDebugAsserts();

    size_t res = 0;

    res += data->allocated_size;
    res += data->nullmaps_allocated_size;
    res += data->pool.allocatedBytes();

    const auto maps_kind = getMapsKind();
    for (const auto & map : data->maps)
    {
        joinDispatch(
            kind,
            strictness,
            map,
            maps_kind,
            [&](auto, auto, auto & map_) { res += map_.getTotalByteCountImpl(data->type); });
    }
    return res;
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
    return table_join.enableAnalyzer() && !table_join.hasUsing() && !isUsedByAnotherAlgorithm(table_join) && table_join.strictness() != JoinStrictness::RightAny;
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

std::optional<HashJoin::RowStoreLayoutWithAccessIndexes> HashJoin::initRowStore(const Block & block)
{
    /// Skip initializing if it's already initialized or disabled.
    if (data->row_store_state != RowStoreState::Enabled)
        return {};

    /// Skip using row store when the right table rerange optimization could get triggered.
    /// TODO: allow row store when right table could get reranged and build the reranged table
    /// based on the row store instead.
    if (isRightTableRerangeEnabled())
    {
        data->row_store_state = RowStoreState::Disabled;
        return {};
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
        return {};
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

    data->row_store_state = RowStoreState::Initialized;
    data->row_store_layout = std::move(layout);
    data->column_access_indexes = std::move(access_indexes);

    LOG_DEBUG(log, "{}Initialized Row store with {} columns", instance_log_id, row_store_columns.size());
    return {RowStoreLayoutWithAccessIndexes{data->row_store_layout, data->column_access_indexes}};
}

void HashJoin::initRowStore(const std::optional<HashJoin::RowStoreLayoutWithAccessIndexes> & layout_with_access_indexes)
{
    /// Skip initializing if it's already initialized or disabled.
    if (data->row_store_state != RowStoreState::Enabled)
        return;

    if (!layout_with_access_indexes)
    {
        data->row_store_state = RowStoreState::Disabled;
        return;
    }

    data->row_store_state = RowStoreState::Initialized;
    data->row_store_layout = layout_with_access_indexes->layout;
    data->column_access_indexes = layout_with_access_indexes->access_indexes;
}

RowDataStorePtr HashJoin::createRowStoreForBlock(const Block & block) const
{
    if (data->row_store_state != RowStoreState::Initialized)
        return nullptr;
    Block block_to_save = filterColumnsPresentInSampleBlock(block, savedBlockSample());
    auto [columns, _] = extractRowStoreColumns(block_to_save, data->column_access_indexes);
    return RowDataStore::create(data->row_store_layout, columns);
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

bool HashJoin::addBlockToJoin(const Block & source_block, bool check_limits)
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
    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Join data was released");

    /// RowRef::row_no is UInt32 (not size_t) for hash table Cell memory efficiency.
    /// It's possible to split bigger blocks and insert them by parts here. But it would be a dead code.
    if (unlikely(selector.size() > std::numeric_limits<decltype(RowRef::row_no)>::max()))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Too many rows in right table block for HashJoin: {}", selector.size());

    /// Initialize the row store layout based on the first block.
    initRowStore(block);

    /** We do not allocate memory for stored blocks inside HashJoin, only for hash table.
      * In case when we have all the blocks allocated before the first `addBlockToJoin` call, will already be quite high.
      * In that case memory consumed by stored blocks will be underestimated.
      */
    if (!memory_usage_before_adding_blocks)
        memory_usage_before_adding_blocks = JoinCommon::getCurrentQueryMemoryUsage();

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
    data->rows_to_join += rows;
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
        if (storage_join_lock)
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "addBlockToJoin called when HashJoin locked to prevent updates");

        assertBlocksHaveEqualStructureAllowReplicated(data->sample_block, block_to_save, "joined block");

        Columns columns;
        if (data->row_store_state == RowStoreState::Initialized)
        {
            auto [row_store_columns, remaining_columns] = extractRowStoreColumns(block_to_save, data->column_access_indexes);
            columns = std::move(remaining_columns);
            if (!row_store)
                row_store = RowDataStore::create(data->row_store_layout, row_store_columns);
        }
        else
            columns = block_to_save.getColumns();

        doDebugAsserts();
        data->columns.emplace_back(std::move(columns), std::move(selector), std::move(row_store));
        auto * stored_columns = &data->columns.back();
        stored_columns->block_no = data->stored_columns_index->add(stored_columns);
        size_t data_allocated_bytes = stored_columns->allocatedBytes();
        data->allocated_size += data_allocated_bytes;
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
                    HashJoinMethods<kind_, strictness_, std::decay_t<decltype(map)>>::insertFromBlockImpl(
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
                        is_inserted,
                        all_values_unique);

                    if (flag_per_row && !per_row_flags_initialized)
                    {
                        used_flags->reinit<kind_, strictness_, mapsKindOf<decltype(map)>()>(
                            stored_columns->block_no, stored_columns->blockRows(), stored_columns->selector);
                        per_row_flags_initialized = true;
                    }
                });

            if (!flag_per_row && save_nullmap && is_inserted)
            {
                auto & h = data->nullmaps.emplace_back(stored_columns, null_map_holder);
                data->nullmaps_allocated_size += h.allocatedBytes();
                nullmap_stored_for_block = true;
            }

            if (!flag_per_row && not_joined_map && (is_inserted || has_right_not_joined))
            {
                auto & h = data->nullmaps.emplace_back(stored_columns, std::move(not_joined_map));
                data->nullmaps_allocated_size += h.allocatedBytes();
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
                data->allocated_size -= data_allocated_bytes;
                /// A set map references no block at all, so every block is dropped here even though its
                /// rows did take part in the join and must stay counted.
                if (maps_kind != JoinMapsKind::Set)
                    data->rows_to_join -= rows;
                /// Nothing was inserted, so no refs to this block exist; null the index entry so
                /// that a stale ref trips the chassert in `StoredColumnsIndex::at` in debug builds
                /// (and dereferences nullptr deterministically in release builds) instead of
                /// silently reading freed memory.
                data->stored_columns_index->clearEntry(stored_columns->block_no);
                data->columns.pop_back();
                doDebugAsserts();
            }

            if (!check_limits)
                return true;

            /// TODO: Do not calculate them every time
            total_rows = getTotalRowCount();
            total_bytes = getTotalByteCount();
            /// total_bytes here is the pre-shrink size (shrink happens below), so this captures the
            /// build high-water mark for free on the path where a shrink can lower it.
            peak_build_bytes = std::max(peak_build_bytes, total_bytes);
        }
    }
    data->keys_to_join = total_rows;
    shrinkStoredBlocksToFit(total_bytes);
    return table_join->sizeLimits().check(total_rows, total_bytes, "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

void HashJoin::shrinkStoredBlocksToFit(size_t & total_bytes_in_join, bool force_optimize)
{
    Int64 current_memory_usage = JoinCommon::getCurrentQueryMemoryUsage();
    Int64 query_memory_usage_delta = current_memory_usage - memory_usage_before_adding_blocks;
    Int64 max_total_bytes_for_query = memory_usage_before_adding_blocks ? table_join->getMaxMemoryUsage() : 0;

    auto max_total_bytes_in_join = table_join->sizeLimits().max_bytes;

    if (!force_optimize)
    {
        if (shrink_blocks)
            return; /// Already shrunk

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

    LOG_DEBUG(
        log,
        "Shrinking stored blocks, memory consumption is {} {} calculated by join, {} {} by memory tracker",
        ReadableSize(total_bytes_in_join),
        max_total_bytes_in_join ? fmt::format("/ {}", ReadableSize(max_total_bytes_in_join)) : "",
        ReadableSize(query_memory_usage_delta),
        max_total_bytes_for_query ? fmt::format("/ {}", ReadableSize(max_total_bytes_for_query)) : "");

    /// Each cloneResized below replaces a stored column object in place, so any emit table built by a
    /// prior query (a persistent StorageJoin builds one per SELECT, then OPTIMIZE/insert runs this) is
    /// left with dangling `const IColumn *`. Bump the generation on every exit - including the exception
    /// paths, where some columns were already replaced - so the next probe rebuilds it against the new
    /// columns. invalidateEmitTable only takes a mutex and increments a counter, so it is unwind-safe.
    SCOPE_EXIT({ data->stored_columns_index->invalidateEmitTable(); });

    for (auto & stored_columns : data->columns)
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
                data->allocated_size -= old_size - partial_new_size;
            else
                data->allocated_size += partial_new_size - old_size;
            throw;
        }

        size_t new_size = stored_columns.allocatedBytes();

        if (old_size >= new_size)
        {
            if (data->allocated_size < old_size - new_size)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Blocks allocated size value is broken: "
                    "blocks_allocated_size = {}, old_size = {}, new_size = {}",
                    data->allocated_size,
                    old_size,
                    new_size);

            data->allocated_size -= old_size - new_size;
        }
        else
            /// Sometimes after clone resized block can be bigger than original
            data->allocated_size += new_size - old_size;

        doDebugAsserts();
    }

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

JoinResultPtr HashJoin::joinScatteredBlock(ScatteredBlock block)
{
    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot join after data has been released");

    chassert(kind == JoinKind::Left || kind == JoinKind::Inner || kind == JoinKind::Right || kind == JoinKind::Full);
    for (const auto & onexpr : table_join->getClauses())
    {
        auto cond_column_name = onexpr.condColumnNames();
        JoinCommon::checkTypesOfKeys(
            block.getSourceBlock(),
            onexpr.key_names_left,
            cond_column_name.first,
            right_sample_block,
            onexpr.key_names_right,
            cond_column_name.second);
    }

    return runJoinDispatch(std::move(block));
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
            if (stats_collecting_params.build.isCollectionAndUseEnabled())
            {
                if (const auto ht_size = getTotalRowCount())
                    getHashTablesStatistics<HashJoinEntry>().update(
                        {.ht_size = ht_size, .source_rows = data->rows_to_join}, stats_collecting_params.build);
            }

            if (stats_collecting_params.match.isCollectionAndUseEnabled() && probe_phase_finished)
                getHashTablesStatistics<HashJoinMatchEntry>().update({.matches = hash_table_matches}, stats_collecting_params.match);
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

bool HashJoin::hasNonJoinedRows()
{
    if (has_non_joined_rows_checked)
        return has_non_joined_rows;

    if (!isRightOrFull(kind))
        return false;

    if (!needUsedFlagsForPerRightTableRow(table_join))
        return false;

    /// If the right table is empty, we have no non-joined rows.
    if (data->rows_to_join == 0)
        return false;

    updateNonJoinedRowsStatus();
    return has_non_joined_rows;
}

void HashJoin::updateNonJoinedRowsStatus()
{
    if (has_non_joined_rows_checked)
        return;

    bool found_non_joined = false;
    if (data->rows_to_join != 0)
    {
        // 1) There are masks for NULL-keys/ON? -> we have nonJoined rows
        if (!data->nullmaps.empty())
            found_non_joined = true;
        // 2) Used flags present:
        //    - If per-row flags are required (mixed ON / multiple disjuncts / RIGHT|FULL), conservatively assume non-joined rows exist
        //    - For single disjunct with per-offset flags, check allOffsetFlagsSet
        //    - Otherwise assume non-joined rows may exist
        else if (used_flags)
        {
            if (needUsedFlagsForPerRightTableRow(table_join))
                found_non_joined = true;
            else if (table_join->oneDisjunct())
                found_non_joined = !used_flags->allOffsetFlagsSet();
            else
                found_non_joined = true;
        }
    }

    has_non_joined_rows = found_non_joined;
    has_non_joined_rows_checked = true;
}

template <typename Mapped>
struct CollectorNonJoined
{
    template <bool with_row_store, bool with_columns>
    static void collect(
        const Mapped & mapped,
        [[maybe_unused]] const StoredBlock * const * stored_columns,
        [[maybe_unused]] const RowDataStore * const * block_row_stores,
        VectorWithMemoryTracking<const StoredBlock *> & blocks,
        VectorWithMemoryTracking<UInt32> & row_numbers,
        RowStorePointers & row_store_ptrs,
        std::optional<size_t> & row_store_batch_size)
    {
        constexpr bool mapped_asof = std::is_same_v<Mapped, AsofRowRefs>;
        [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<Mapped, RowRef>;

        [[maybe_unused]] auto collect_row = [&](UInt32 block_no, UInt32 row_no)
        {
            if constexpr (with_columns)
            {
                blocks.push_back(stored_columns[block_no]);
                row_numbers.push_back(row_no);
            }
            if constexpr (with_row_store)
            {
                const auto * row_store = block_row_stores[block_no];
                row_store_ptrs.ptrs.emplace_back(row_store->getRowAt(row_no));
                if (!row_store_batch_size)
                    row_store_batch_size = row_store->getBatchSize();
            }
        };

        if constexpr (mapped_asof)
        {
            /// Do nothing
        }
        else if constexpr (mapped_one)
        {
            collect_row(mapped.blockNo(), mapped.rowNo());
        }
        else
        {
            for (auto it = mapped.begin(); it.ok(); ++it)
            {
                const UInt64 ref_word = *it;
                collect_row(refWordBlockNo(ref_word), refWordRowNo(ref_word));
            }
        }
    }
};

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

        const auto & access_indexes = parent.data->column_access_indexes;
        const Block & saved_block_sample = parent.savedBlockSample();

        type_name.reserve(saved_block_sample.columns());
        for (const auto & column : saved_block_sample)
            type_name.emplace_back(column.name, column.type);

        output_access_indexes.reserve(saved_block_sample.columns());
        if (parent.data->row_store_state == HashJoin::RowStoreState::Initialized)
        {
            for (size_t i = 0; i < saved_block_sample.columns(); ++i)
            {
                const ColumnAccessIndex & access_index = access_indexes[i];
                if (access_index.type == ColumnAccessIndex::Type::RowStore)
                    has_row_store = true;
                else
                    has_columns = true;
                output_access_indexes.push_back(access_index);
            }
        }
        else
        {
            for (size_t i = 0; i < saved_block_sample.columns(); ++i)
                output_access_indexes.push_back({ColumnAccessIndex::Type::Columns, i});
        }
    }

    Block getEmptyBlock() override { return parent.savedBlockSample().cloneEmpty(); }

    size_t fillColumns(MutableColumns & columns_right) override
    {
        size_t rows_added = 0;
        dispatchOutputs([&]<bool with_row_store, bool with_columns>()
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
            {
                fillNullsFromBlocks<with_row_store, with_columns>(columns_right, rows_added);
            });
        }

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

    ColumnAccessIndexes output_access_indexes;
    NamesAndTypes type_name;
    bool has_row_store = false;
    bool has_columns = false;

    bool isBucketInRange(size_t bucket) const
    {
        return num_buckets <= 1 || (bucket % num_buckets) == bucket_idx;
    }

    bool isBlockInRange(size_t block_no) const
    {
        return num_buckets <= 1 || (block_no % num_buckets) == bucket_idx;
    }

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
    case HashJoin::Type::TYPE: \
        return fillColumns<with_row_store, with_columns>(*maps.TYPE, columns_right);
            APPLY_FOR_JOIN_VARIANTS(M)
#undef M
        }
        UNREACHABLE();
    }

    template <bool with_row_store, bool with_columns, typename Map>
    size_t fillColumns(const Map & map, MutableColumns & columns_right)
    {
        ColumnsWithRowNumbers columns_with_row_numbers;
        [[maybe_unused]] auto & many_columns = columns_with_row_numbers.columns;
        [[maybe_unused]] auto & row_nums = columns_with_row_numbers.row_numbers;
        if constexpr (with_columns)
        {
            many_columns.reserve(max_block_size);
            row_nums.reserve(max_block_size);
        }

        [[maybe_unused]] RowStorePointers row_store_ptrs;
        [[maybe_unused]] std::optional<size_t> row_store_batch_size;
        if constexpr (with_row_store)
            row_store_ptrs.ptrs.reserve(max_block_size);

        auto collected = [&]() -> size_t
        {
            if constexpr (with_columns)
                return row_nums.size();
            else
                return row_store_ptrs.ptrs.size();
        };

        if (flag_per_row)
        {
            /// parent.data->columns is not partitioned by hash bucket, so distribute the stored
            /// right blocks across streams by their globally unique block_no instead
            if (!used_position.has_value())
                used_position = parent.data->columns.begin();

            auto end = parent.data->columns.end();

            for (auto & it = *used_position; it != end && collected() < max_block_size; ++it)
            {
                const auto & mapped_block = *it;
                if (!isBlockInRange(mapped_block.block_no))
                    continue;

                size_t rows = mapped_block.blockRows();

                for (size_t row = 0; row < rows; ++row)
                {
                    if (!parent.isUsed(mapped_block.block_no, row))
                    {
                        if constexpr (with_columns)
                        {
                            many_columns.push_back(&mapped_block);
                            row_nums.push_back(static_cast<UInt32>(row));
                        }
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
            const StoredBlock * const * stored_columns = parent.data->stored_columns_index->blocksData();
            const RowDataStore * const * block_row_stores = parent.data->stored_columns_index->rowStoresData();

            /// case: two-level hash tables with parallel iteration
            if constexpr (requires { it.getBucket(); map.NUM_BUCKETS; })
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

                while (it != end && collected() < max_block_size)
                {
                    size_t offset = map.offsetInternal(it.getPtr());
                    if (!parent.isUsed(offset))
                    {
                        const Mapped & mapped = it->getMapped();
                        CollectorNonJoined<Mapped>::template collect<with_row_store, with_columns>(mapped, stored_columns, block_row_stores, many_columns, row_nums, row_store_ptrs, row_store_batch_size);
                    }

                    ++it;

                    /// if we crossed into a bucket not owned by this stream, skip ahead
                    if (it != end && !isBucketInRange(it.getBucket()) && !skipToNextOwnedBucket())
                        break;
                }
            }
            else
            {
                /// Single-level hash tables - no bucket filtering
                for (; it != end; ++it)
                {
                    size_t offset = map.offsetInternal(it.getPtr());
                    if (parent.isUsed(offset))
                        continue;

                    const Mapped & mapped = it->getMapped();
                    CollectorNonJoined<Mapped>::template collect<with_row_store, with_columns>(mapped, stored_columns, block_row_stores, many_columns, row_nums, row_store_ptrs, row_store_batch_size);

                    if (collected() >= max_block_size)
                    {
                        ++it;
                        break;
                    }
                }
            }
        }

        fillJoinOutputColumns(columns_right, output_access_indexes, row_store_ptrs, row_store_batch_size, columns_with_row_numbers, type_name);
        return collected();
    }

    template <bool with_row_store, bool with_columns>
    void fillNullsFromBlocks(MutableColumns & columns_right, size_t & rows_added)
    {
        /// for parallel iteration, only stream 0 handles nullmaps to avoid duplicates
        if (bucket_idx != 0)
            return;

        if (!nulls_position.has_value())
            nulls_position = parent.data->nullmaps.begin();

        auto end = parent.data->nullmaps.end();

        ColumnsWithRowNumbers columns_with_row_numbers;
        [[maybe_unused]] auto & many_columns = columns_with_row_numbers.columns;
        [[maybe_unused]] auto & row_nums = columns_with_row_numbers.row_numbers;
        if constexpr (with_columns)
        {
            many_columns.reserve(max_block_size);
            row_nums.reserve(max_block_size);
        }

        [[maybe_unused]] RowStorePointers row_store_ptrs;
        [[maybe_unused]] std::optional<size_t> row_store_batch_size;
        if constexpr (with_row_store)
            row_store_ptrs.ptrs.reserve(max_block_size);

        auto collected = [&]() -> size_t
        {
            if constexpr (with_columns)
                return row_nums.size();
            else
                return row_store_ptrs.ptrs.size();
        };

        for (auto & it = *nulls_position; it != end && rows_added + collected() < max_block_size; ++it)
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
                    if constexpr (with_columns)
                    {
                        many_columns.push_back(columns);
                        row_nums.push_back(static_cast<UInt32>(row));
                    }
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

        fillJoinOutputColumns(columns_right, output_access_indexes, row_store_ptrs, row_store_batch_size, columns_with_row_numbers, type_name);
        rows_added += collected();
    }
};

IBlocksStreamPtr
HashJoin::getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const
{
    return getNonJoinedBlocks(left_sample_block, result_sample_block, max_block_size, 0, 1);
}

IBlocksStreamPtr
HashJoin::getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size,
                             size_t bucket_idx, size_t num_buckets) const
{
    if (!JoinCommon::hasNonJoinedBlocks(*table_join))
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

    if (matched_rows_stats)
        matched_rows_stats->prepareRightFlagsIfNeeded(data->columns);
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

    const auto column_access_indexes = data->column_access_indexes;

    /// Reconstruct full column list from compact columns and row store
    /// using the access indexes to place each column back at its original position.
    /// TODO: make the row store spillable.
    auto materialize_columns = [&](StoredBlock & stored_block)
    {
        const auto & stored_columns = stored_block.columns;
        const auto & access_indexes = column_access_indexes;
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
    };

    auto extract_source_blocks = [&](StoredBlocksList && columns_list, const Block & sample_block)
    {
        BlocksList result;
        for (auto & columns : columns_list)
            result.emplace_back(sample_block.cloneWithColumns(materialize_columns(columns)));
        return result;
    };

    StoredBlocksList right_columns = std::move(data->columns);
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
    data->nullmaps.clear();

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
        Columns all_columns = materialize_columns(saved_columns);
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
        data->columns.swap(sorted_columns);
        /// The replaced blocks are destroyed below; null their index entries so that any stale
        /// ref fails loudly instead of reading freed memory. All live cells were rewritten above.
        for (const auto & old_columns : sorted_columns)
            data->stored_columns_index->clearEntry(old_columns.block_no);
        size_t new_blocks_allocated_size = 0;
        for (auto & columns : data->columns)
        {
            columns.selector = ScatteredBlock::Selector(columns.columns.at(0)->size());
            new_blocks_allocated_size += columns.allocatedBytes();
        }
        data->allocated_size = new_blocks_allocated_size;

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
    return isRightTableRerangeEnabled() && !data->columns.empty()
        && data->rows_to_join <= table_join->sortRightMaximumTableRows()
        && data->avgPerKeyRows() >= table_join->sortRightMinimumPerkeyRows();
}

size_t HashJoin::getAndSetRightTableKeys() const
{
    size_t total_rows = getTotalRowCount();
    if (data)
        data->keys_to_join = total_rows;
    return total_rows;
}

void HashJoin::tryRerangeRightTableData()
{
    if (!rightTableCanBeReranged())
        return;

    if (data->keys_to_join == 0)
        data->keys_to_join = getTotalRowCount();

    /// If the there is no columns to add, means no columns to output, then the rerange would not improve performance by using column's `insertRangeFrom`
    /// to replace column's `insertFrom` to make the output.
    if (sample_block_with_columns_to_add.columns() == 0)
    {
        LOG_DEBUG(log, "The joined right table total rows :{}, total keys :{}", data->rows_to_join, data->keys_to_join);
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
        LOG_DEBUG(log, "{}Converted join hash map to fixed hash map (range: {}, keys: {})", instance_log_id, range, key_count);
}

bool HashJoin::canConvertToFixedHashMap() const
{
    return !conversion_to_fixed_hash_map_attempted && data && data->rows_to_join && table_join->enableJoinFixedHashTableConversion()
        && (data->type == Type::key32 || data->type == Type::key64) && data->maps.size() == 1 && strictness != JoinStrictness::Asof;
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
    /// first element is the rendezvous key (the same key `BuildRuntimeFilterTransform` registered the
    /// filter under and the probe-side `__applyFilter` looks it up by), not the stable display name.
    for (const auto & [filter_key, descr_build_key] : descriptors)
    {
        if (descr_build_key != build_key_name)
            continue;

        auto existing = lookup->find(filter_key);
        if (!existing)
            continue;

        /// When common_type is wide (e.g. Int64 = UInt64 promotes to Int128), per-row wide-integer
        /// arithmetic on the probe side can be slower than the existing BloomFilter; skip.
        const auto target_type = removeNullable(existing->getFilterColumnTargetType());
        WhichDataType target_which(target_type);
        if (!target_type->isValueRepresentedByInteger()
            || target_which.isInt128() || target_which.isUInt128()
            || target_which.isInt256() || target_which.isUInt256()
            || target_which.isIPv4()
            || target_which.isLowCardinality())
            continue;

        auto filter = std::make_unique<SharedFixedHashTableRuntimeFilter>(
            existing->getFilterColumnTargetType(),
            existing->getPassRatioThresholdForDisabling(),
            existing->getBlocksToSkipBeforeReenabling(),
            probe_fn,
            existing->getRecordedKeyRanges(),
            existing->getRecordedKeyValues());
        /// `replace` keeps the original registration's display name in the lookup, so stats stay legible.
        LOG_TRACE(getLogger("HashJoin"), "Published shared fixed-hash-table runtime filter under key '{}'", filter_key);
        lookup->replace(filter_key, std::move(filter));
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
        reinitUsedFlags();
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
    reinitUsedFlags();

    /// Two-level maps per-row flags will be finalized by ConcurrentHashJoin.
    if (!twoLevelMapIsUsed())
        used_flags->finalizePerRowFlags(*used_flags, data->stored_columns_index->size());

    if (all_values_unique && strictness == JoinStrictness::All && isInnerOrLeft(kind) && data->maps.size() == 1)
    {
        strictness = JoinStrictness::RightAny;
        all_join_was_promoted_to_right_any = true;
        LOG_DEBUG(log, "Promoting join strictness to RightAny, because all values in the right table are unique");
    }
    updateNonJoinedRowsStatus();

    /// In case addBlockToJoin is returning early
    /// we take a peak snapshot
    size_t total_bytes = getTotalByteCount();
    peak_build_bytes = std::max(peak_build_bytes, total_bytes);

    if (matched_rows_stats)
        matched_rows_stats->prepareRightFlagsIfNeeded(data->columns);

    build_phase_finished = true;
    LOG_TRACE(log, "{}Join data is built, {} and {} rows in hash table", instance_log_id, ReadableSize(total_bytes), getTotalRowCount());
}

bool HashJoin::hasPostBuildPhase() const
{
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
    tryRerangeRightTableData();
    tryConvertToFixedHashMap();
    publishSharedRuntimeFilters();
}
}
