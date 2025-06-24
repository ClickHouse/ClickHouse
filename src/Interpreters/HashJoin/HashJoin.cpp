#include <any>
#include <limits>
#include <memory>
#include <vector>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Common/CurrentThread.h>
#include <Common/StackTrace.h>
#include <Common/logger_useful.h>


#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/JoinUtils.h>
#include <DataTypes/NullableUtils.h>
#include <Interpreters/RowRefs.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/joinDispatch.h>

#include <Interpreters/TemporaryDataOnDisk.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/formatReadable.h>
#include <Common/typeid_cast.h>

#include <Interpreters/HashJoin/HashJoinMethods.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int NO_SUCH_COLUMN_IN_TABLE;
extern const int INCOMPATIBLE_TYPE_OF_JOIN;
extern const int UNSUPPORTED_JOIN_KEYS;
extern const int LOGICAL_ERROR;
extern const int SYNTAX_ERROR;
extern const int SET_SIZE_LIMIT_EXCEEDED;
extern const int TYPE_MISMATCH;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int INVALID_JOIN_ON_EXPRESSION;
}

namespace
{

struct NotProcessedCrossJoin : public ExtraBlock
{
    size_t left_position;
    size_t right_block;
    std::optional<TemporaryBlockStreamReaderHolder> reader;
};


Int64 getCurrentQueryMemoryUsage()
{
    /// Use query-level memory tracker
    if (auto * memory_tracker_child = CurrentThread::getMemoryTracker())
        if (auto * memory_tracker = memory_tracker_child->getParent())
            return memory_tracker->get();
    return 0;
}

Block filterColumnsPresentInSampleBlock(const Block & block, const Block & sample_block)
{
    Block filtered_block;
    for (const auto & sample_column : sample_block.getColumnsWithTypeAndName())
        filtered_block.insert(block.getByName(sample_column.name));
    return filtered_block;
}

Block materializeColumnsFromRightBlock(Block block, const Block & sample_block, const Names &)
{
    std::unordered_map<std::string_view, std::vector<ColumnWithTypeAndName *>> block_index;
    for (auto & column : block)
        block_index[column.name].push_back(&column);

    for (const auto & sample_column : sample_block.getColumnsWithTypeAndName())
    {
        for (auto * column_ptr : block_index[sample_column.name])
        {
            auto & column = *column_ptr;

            /// There's no optimization for right side const columns. Remove constness if any.
            column.column = recursiveRemoveSparse(column.column->convertToFullColumnIfConst());

            if (column.column->lowCardinality() && !sample_column.column->lowCardinality())
            {
                column.column = column.column->convertToFullColumnIfLowCardinality();
                column.type = removeLowCardinality(column.type);
            }

            if (sample_column.column->isNullable())
                JoinCommon::convertColumnToNullable(column);
        }
    }

    return block;
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

static HashJoin::Type chooseMethod(JoinKind kind, const ColumnRawPtrs & key_columns, Sizes & key_sizes, bool use_two_level_maps);

HashJoin::HashJoin(
    std::shared_ptr<TableJoin> table_join_,
    const Block & right_sample_block_,
    bool any_take_last_row_,
    size_t reserve_num_,
    const String & instance_id_,
    bool use_two_level_maps)
    : table_join(table_join_)
    , kind(table_join->kind())
    , strictness(table_join->strictness())
    , any_take_last_row(any_take_last_row_)
    , reserve_num(reserve_num_)
    , instance_id(instance_id_)
    , asof_inequality(table_join->getAsofInequality())
    , data(std::make_shared<RightTableData>())
    , tmp_data(table_join_->getTempDataOnDisk())
    , right_sample_block(right_sample_block_)
    , max_joined_block_rows(table_join->maxJoinedBlockRows())
    , instance_log_id(!instance_id_.empty() ? "(" + instance_id_ + ") " : "")
    , log(getLogger("HashJoin"))
{
    for (auto & column : right_sample_block)
    {
        if (!column.column)
            column.column = column.type->createColumn();
    }

    LOG_TRACE(
        log,
        "{}Keys: {}, datatype: {}, kind: {}, strictness: {}, right header: {}",
        instance_log_id,
        TableJoin::formatClauses(table_join->getClauses(), true),
        data->type,
        kind,
        strictness,
        right_sample_block.dumpStructure());

    validateAdditionalFilterExpression(table_join->getMixedJoinExpression());

    used_flags = std::make_unique<JoinStuff::JoinUsedFlags>();

    if (isCrossOrComma(kind))
    {
        data->type = Type::CROSS;
        sample_block_with_columns_to_add = materializeBlock(right_sample_block);
    }
    else if (table_join->getClauses().empty())
    {
        data->type = Type::EMPTY;
        /// We might need to insert default values into the right columns, materialize them
        sample_block_with_columns_to_add = materializeBlock(right_sample_block);
    }
    else if (table_join->oneDisjunct())
    {
        const auto & key_names_right = table_join->getOnlyClause().key_names_right;
        JoinCommon::splitAdditionalColumns(key_names_right, right_sample_block, right_table_keys, sample_block_with_columns_to_add);
        required_right_keys = table_join->getRequiredRightKeys(right_table_keys, required_right_keys_sources);
    }
    else
    {
        /// required right keys concept does not work well if multiple disjuncts, we need all keys
        sample_block_with_columns_to_add = right_table_keys = materializeBlock(right_sample_block);
    }

    materializeBlockInplace(right_table_keys);
    initRightBlockStructure(data->sample_block);
    data->sample_block = prepareRightBlock(data->sample_block);

    JoinCommon::createMissedColumns(sample_block_with_columns_to_add);

    size_t disjuncts_num = table_join->getClauses().size();
    data->maps.resize(disjuncts_num);
    key_sizes.reserve(disjuncts_num);

    for (const auto & clause : table_join->getClauses())
    {
        const auto & key_names_right = clause.key_names_right;
        ColumnRawPtrs key_columns = JoinCommon::extractKeysForJoin(right_table_keys, key_names_right);

        if (strictness == JoinStrictness::Asof)
        {
            assert(disjuncts_num == 1);

            /// @note ASOF JOIN is not INNER. It's better avoid use of 'INNER ASOF' combination in messages.
            /// In fact INNER means 'LEFT SEMI ASOF' while LEFT means 'LEFT OUTER ASOF'.
            if (!isLeft(kind) && !isInner(kind))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Wrong ASOF JOIN type. Only ASOF and LEFT ASOF joins are supported");

            if (key_columns.size() <= 1)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "ASOF join needs at least one equi-join column");

            size_t asof_size;
            asof_type = SortedLookupVectorBase::getTypeSize(*key_columns.back(), asof_size);
            key_columns.pop_back();

            /// this is going to set up the appropriate hash table for the direct lookup part of the join
            /// However, this does not depend on the size of the asof join key (as that goes into the BST)
            /// Therefore, add it back in such that it can be extracted appropriately from the full stored
            /// key_columns and key_sizes
            auto & asof_key_sizes = key_sizes.emplace_back();
            data->type = chooseMethod(kind, key_columns, asof_key_sizes, use_two_level_maps);
            asof_key_sizes.push_back(asof_size);
        }
        else
        {
            /// Choose data structure to use for JOIN.
            auto current_join_method = chooseMethod(kind, key_columns, key_sizes.emplace_back(), use_two_level_maps);
            if (data->type == Type::EMPTY)
                data->type = current_join_method;
            else if (data->type != current_join_method)
                data->type = Type::hashed;
        }
    }

    for (auto & maps : data->maps)
        dataMapInit(maps);

    if (table_join->getMixedJoinExpression())
    {
        const auto & required_cols = table_join->getMixedJoinExpression()->getRequiredColumnsWithTypes();
        size_t pos = 0;
        for (const auto & input : required_cols)
        {
            if (data->sample_block.has(input.name))
                additional_filter_required_rhs_pos.emplace_back(
                    pos,
                    data->sample_block.getPositionByName(input.name));
            ++pos;
        }
    }
}

size_t HashJoin::ScatteredColumns::allocatedBytes() const
{
    if (columns.empty())
        return 0;

    size_t rows = columns.front()->size();
    if (rows == 0)
        return 0;

    size_t res = 0;
    for (const auto & column : columns)
        res += column->allocatedBytes();
    return res * selector.size() / rows;
}

size_t HashJoin::NullMapHolder::allocatedBytes() const
{
    size_t rows = column->size();
    if (rows == 0)
        return 0;
    return column->allocatedBytes() * columns->selector.size() / rows;
}

static HashJoin::Type chooseMethod(JoinKind kind, const ColumnRawPtrs & key_columns, Sizes & key_sizes)
{
    using Type = HashJoin::Type;

    size_t keys_size = key_columns.size();

    if (keys_size == 0)
    {
        if (isCrossOrComma(kind))
            return Type::CROSS;
        return Type::EMPTY;
    }

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

static HashJoin::Type chooseMethod(JoinKind kind, const ColumnRawPtrs & key_columns, Sizes & key_sizes, bool use_two_level_maps)
{
    using Type = HashJoin::Type;

    if (!use_two_level_maps)
        return chooseMethod(kind, key_columns, key_sizes);

    // if `use_two_level_maps == true` returns two-level version of the map
    switch (auto type = chooseMethod(kind, key_columns, key_sizes))
    {
        case Type::key32:
            return Type::two_level_key32;
        case Type::key64:
            return Type::two_level_key64;
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
    if (kind == JoinKind::Cross)
        return;
    auto prefer_use_maps_all = table_join->getMixedJoinExpression() != nullptr;
    joinDispatchInit(kind, strictness, map, prefer_use_maps_all);
    joinDispatch(kind, strictness, map, prefer_use_maps_all, [&](auto, auto, auto & map_) { map_.create(data->type, reserve_num); });

    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "HashJoin::dataMapInit called with empty data");
}

bool HashJoin::empty() const
{
    return data->type == Type::EMPTY;
}

bool HashJoin::alwaysReturnsEmptySet() const
{
    return isInnerOrRight(getKind()) && data->empty;
}

size_t HashJoin::getTotalRowCount() const
{
    if (!data)
        return 0;

    size_t res = 0;

    if (data->type == Type::CROSS)
    {
        for (const auto & columns : data->columns)
            res += columns.columns.at(0)->size();
    }
    else
    {
        auto prefer_use_maps_all = table_join->getMixedJoinExpression() != nullptr;
        for (const auto & map : data->maps)
        {
            joinDispatch(
                kind, strictness, map, prefer_use_maps_all, [&](auto, auto, auto & map_) { res += map_.getTotalRowCount(data->type); });
        }
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

    if (data->type != Type::CROSS)
    {
        auto prefer_use_maps_all = table_join->getMixedJoinExpression() != nullptr;
        for (const auto & map : data->maps)
        {
            joinDispatch(
                kind,
                strictness,
                map,
                prefer_use_maps_all,
                [&](auto, auto, auto & map_) { res += map_.getTotalByteCountImpl(data->type); });
        }
    }
    return res;
}

bool HashJoin::isUsedByAnotherAlgorithm() const
{
    return table_join->isEnabledAlgorithm(JoinAlgorithm::AUTO) || table_join->isEnabledAlgorithm(JoinAlgorithm::GRACE_HASH);
}

bool HashJoin::canRemoveColumnsFromLeftBlock() const
{
    return table_join->enableAnalyzer() && !table_join->hasUsing() && !isUsedByAnotherAlgorithm() && strictness != JoinStrictness::RightAny;
}

void HashJoin::initRightBlockStructure(Block & saved_block_sample)
{
    if (isCrossOrComma(kind))
    {
        /// cross join doesn't have keys, just add all columns
        saved_block_sample = sample_block_with_columns_to_add.cloneEmpty();
        return;
    }

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
    return DB::materializeColumnsFromRightBlock(std::move(block), savedBlockSample(), table_join->getAllNames(JoinTableSide::Right));
}

Block HashJoin::prepareRightBlock(const Block & block, const Block & saved_block_sample_)
{
    Block prepared_block = DB::materializeColumnsFromRightBlock(block, saved_block_sample_, {});
    return filterColumnsPresentInSampleBlock(prepared_block, saved_block_sample_);
}

Block HashJoin::prepareRightBlock(const Block & block) const
{
    return prepareRightBlock(block, savedBlockSample());
}

bool HashJoin::addBlockToJoin(const Block & source_block, bool check_limits)
{
    auto materialized = materializeColumnsFromRightBlock(source_block);
    auto scattered_block = ScatteredBlock{materialized};
    return addBlockToJoin(scattered_block, check_limits);
}

bool HashJoin::addBlockToJoin(ScatteredBlock & source_block, bool check_limits)
{
    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Join data was released");

    /// RowRef::SizeT is uint32_t (not size_t) for hash table Cell memory efficiency.
    /// It's possible to split bigger blocks and insert them by parts here. But it would be a dead code.
    if (unlikely(source_block.rows() > std::numeric_limits<RowRef::SizeT>::max()))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Too many rows in right table block for HashJoin: {}", source_block.rows());

    /** We do not allocate memory for stored blocks inside HashJoin, only for hash table.
      * In case when we have all the blocks allocated before the first `addBlockToJoin` call, will already be quite high.
      * In that case memory consumed by stored blocks will be underestimated.
      */
    if (!memory_usage_before_adding_blocks)
        memory_usage_before_adding_blocks = getCurrentQueryMemoryUsage();

    if (strictness == JoinStrictness::Asof)
    {
        chassert(kind == JoinKind::Left || kind == JoinKind::Inner);

        /// Filter out rows with NULLs in ASOF key, nulls are not joined with anything since they are not comparable
        /// We support only INNER/LEFT ASOF join, so rows with NULLs never return from the right joined table.
        /// So filter them out here not to handle in implementation.
        const auto & asof_key_name = table_join->getOnlyClause().key_names_right.back();
        const auto & asof_column = source_block.getByName(asof_key_name);

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

                NullMap negative_null_map(asof_column_nullable.size());
                for (size_t i = 0; i < asof_column_nullable.size(); ++i)
                    negative_null_map[i] = !asof_column_nullable[i];

                source_block.filter(negative_null_map);
            }
        }
    }

    const size_t rows = source_block.rows();
    data->rows_to_join += rows;
    const auto & right_key_names = table_join->getAllNames(JoinTableSide::Right);
    ColumnPtrMap all_key_columns(right_key_names.size());
    for (const auto & column_name : right_key_names)
    {
        const auto & column = source_block.getByName(column_name).column;
        all_key_columns[column_name] = recursiveRemoveSparse(column->convertToFullColumnIfConst())->convertToFullColumnIfLowCardinality();
    }

    Block block_to_save = filterColumnsPresentInSampleBlock(source_block.getSourceBlock(), savedBlockSample());
    if (shrink_blocks)
        block_to_save = block_to_save.shrinkToFit();

    size_t max_bytes_in_join = table_join->sizeLimits().max_bytes;
    size_t max_rows_in_join = table_join->sizeLimits().max_rows;

    if (kind == JoinKind::Cross && tmp_data
        && (tmp_stream || (max_bytes_in_join && getTotalByteCount() + block_to_save.allocatedBytes() >= max_bytes_in_join)
            || (max_rows_in_join && getTotalRowCount() + block_to_save.rows() >= max_rows_in_join)))
    {
        if (!tmp_stream)
            tmp_stream.emplace(right_sample_block, tmp_data.get());

        chassert(!source_block.wasScattered()); /// We don't run parallel_hash for cross join
        tmp_stream.value()->write(block_to_save);
        return true;
    }

    bool prefer_use_maps_all = table_join->getMixedJoinExpression() != nullptr;

    size_t total_rows = 0;
    size_t total_bytes = 0;
    {
        if (storage_join_lock)
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "addBlockToJoin called when HashJoin locked to prevent updates");

        assertBlocksHaveEqualStructure(data->sample_block, block_to_save, "joined block");

        size_t min_bytes_to_compress = table_join->crossJoinMinBytesToCompress();
        size_t min_rows_to_compress = table_join->crossJoinMinRowsToCompress();

        if (kind == JoinKind::Cross
            && ((min_bytes_to_compress && getTotalByteCount() >= min_bytes_to_compress)
                || (min_rows_to_compress && getTotalRowCount() >= min_rows_to_compress)))
        {
            chassert(!source_block.wasScattered()); /// We don't run parallel_hash for cross join
            block_to_save = block_to_save.compress();
            have_compressed = true;
        }

        doDebugAsserts();
        data->columns.emplace_back(block_to_save.getColumns(), source_block.detachSelector());
        const auto * stored_columns = &data->columns.back();
        size_t data_allocated_bytes = stored_columns->allocatedBytes();
        data->allocated_size += data_allocated_bytes;
        doDebugAsserts();

        if (rows)
            data->empty = false;

        bool flag_per_row = needUsedFlagsForPerRightTableRow(table_join);
        const auto & onexprs = table_join->getClauses();
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
                /// Save rows with NULL keys
                for (size_t i = 0; !save_nullmap && i < null_map->size(); ++i)
                    save_nullmap |= (*null_map)[i];
            }

            auto join_mask_col = JoinCommon::getColumnAsMask(source_block.getSourceBlock(), onexprs[onexpr_idx].condColumnNames().second);
            /// Save blocks that do not hold conditions in ON section
            ColumnUInt8::MutablePtr not_joined_map = nullptr;
            if (!flag_per_row && isRightOrFull(kind) && join_mask_col.hasData())
            {
                const auto & join_mask = join_mask_col.getData();
                /// Save rows that do not hold conditions
                not_joined_map = ColumnUInt8::create(rows, 0);
                for (size_t i = 0, sz = join_mask->size(); i < sz; ++i)
                {
                    /// Condition hold, do not save row
                    if ((*join_mask)[i])
                        continue;

                    /// NULL key will be saved anyway because, do not save twice
                    if (save_nullmap && (*null_map)[i])
                        continue;

                    not_joined_map->getData()[i] = 1;
                }
            }

            bool is_inserted = false;
            if (kind != JoinKind::Cross)
            {
                joinDispatch(
                    kind,
                    strictness,
                    data->maps[onexpr_idx],
                    prefer_use_maps_all,
                    [&](auto kind_, auto strictness_, auto & map)
                    {
                        HashJoinMethods<kind_, strictness_, std::decay_t<decltype(map)>>::insertFromBlockImpl(
                            *this,
                            data->type,
                            map,
                            key_columns,
                            key_sizes[onexpr_idx],
                            &stored_columns->columns,
                            stored_columns->selector,
                            null_map,
                            join_mask_col.getData(),
                            data->pool,
                            is_inserted);

                        if (flag_per_row)
                            used_flags->reinit<kind_, strictness_, std::is_same_v<std::decay_t<decltype(map)>, MapsAll>>(&stored_columns->columns);
                    });
            }

            if (!flag_per_row && save_nullmap && is_inserted)
            {
                data->nullmaps.emplace_back(stored_columns, null_map_holder);
                data->nullmaps_allocated_size += data->nullmaps.back().allocatedBytes();
            }

            if (!flag_per_row && not_joined_map && is_inserted)
            {
                data->nullmaps.emplace_back(stored_columns, std::move(not_joined_map));
                data->nullmaps_allocated_size += data->nullmaps.back().allocatedBytes();
            }

            if (!flag_per_row && !is_inserted)
            {
                doDebugAsserts();
                LOG_TRACE(log, "Skipping inserting block with {} rows", rows);
                data->allocated_size -= data_allocated_bytes;
                data->columns.pop_back();
                doDebugAsserts();
            }

            if (!check_limits)
                return true;

            /// TODO: Do not calculate them every time
            total_rows = getTotalRowCount();
            total_bytes = getTotalByteCount();
        }
    }
    data->keys_to_join = total_rows;
    shrinkStoredBlocksToFit(total_bytes);
    return table_join->sizeLimits().check(total_rows, total_bytes, "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

void HashJoin::shrinkStoredBlocksToFit(size_t & total_bytes_in_join, bool force_optimize)
{
    Int64 current_memory_usage = getCurrentQueryMemoryUsage();
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

    for (auto & stored_columns : data->columns)
    {
        doDebugAsserts();

        size_t old_size = stored_columns.allocatedBytes();

        for (auto & column : stored_columns.columns)
            column = column->cloneResized(column->size());

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

    Int64 new_current_memory_usage = getCurrentQueryMemoryUsage();

    LOG_DEBUG(
        log,
        "Shrunk stored blocks {} freed ({} by memory tracker), new memory consumption is {} ({} by memory tracker)",
        ReadableSize(total_bytes_in_join - new_total_bytes_in_join),
        ReadableSize(current_memory_usage - new_current_memory_usage),
        ReadableSize(new_total_bytes_in_join),
        ReadableSize(new_current_memory_usage));

    total_bytes_in_join = new_total_bytes_in_join;
}

void HashJoin::joinBlockImplCross(Block & block, ExtraBlockPtr & not_processed) const
{
    size_t start_left_row = 0;
    size_t start_right_block = 0;
    std::optional<TemporaryBlockStreamReaderHolder> reader;
    if (not_processed)
    {
        auto & continuation = static_cast<NotProcessedCrossJoin &>(*not_processed);
        start_left_row = continuation.left_position;
        start_right_block = continuation.right_block;
        if (continuation.reader)
            reader = std::move(*continuation.reader);
        not_processed.reset();
    }

    size_t num_existing_columns = block.columns();
    size_t num_columns_to_add = sample_block_with_columns_to_add.columns();

    ColumnRawPtrs src_left_columns;
    MutableColumns dst_columns;

    {
        src_left_columns.reserve(num_existing_columns);
        dst_columns.reserve(num_existing_columns + num_columns_to_add);

        for (const ColumnWithTypeAndName & left_column : block)
        {
            src_left_columns.push_back(left_column.column.get());
            dst_columns.emplace_back(src_left_columns.back()->cloneEmpty());
        }

        for (const ColumnWithTypeAndName & right_column : sample_block_with_columns_to_add)
            dst_columns.emplace_back(right_column.column->cloneEmpty());

        for (auto & dst : dst_columns)
            dst->reserve(max_joined_block_rows);
    }

    size_t rows_left = block.rows();
    size_t rows_added = 0;
    for (size_t left_row = start_left_row; left_row < rows_left; ++left_row)
    {
        size_t block_number = 0;

        auto process_right_block = [&](const Columns & columns)
        {
            size_t rows_right = columns.at(0)->size();
            rows_added += rows_right;

            for (size_t col_num = 0; col_num < num_existing_columns; ++col_num)
                dst_columns[col_num]->insertManyFrom(*src_left_columns[col_num], left_row, rows_right);

            for (size_t col_num = 0; col_num < num_columns_to_add; ++col_num)
            {
                const IColumn & column_right = *columns[col_num];
                dst_columns[num_existing_columns + col_num]->insertRangeFrom(column_right, 0, rows_right);
            }
        };

        for (const auto & scattered_columns : data->columns)
        {
            ++block_number;
            if (block_number < start_right_block)
                continue;
            /// The following statement cannot be substituted with `process_right_block(!have_compressed ? block_right : block_right.decompress())`
            /// because it will lead to copying of `block_right` even if its branch is taken (because common type of `block_right` and `block_right.decompress()` is `Block`).
            if (!have_compressed)
                process_right_block(scattered_columns.columns);
            else
            {
                chassert(scattered_columns.selector.size() == scattered_columns.columns.at(0)->size()); /// Compression only happens for cross join and scattering only for concurrent hash

                Columns new_columns;
                new_columns.reserve(scattered_columns.columns.size());
                for (const auto & column : scattered_columns.columns)
                    new_columns.emplace_back(column->decompress());

                process_right_block(new_columns);
            }

            if (rows_added > max_joined_block_rows)
            {
                break;
            }
        }

        if (tmp_stream && rows_added <= max_joined_block_rows)
        {
            if (!reader)
                reader = tmp_stream->getReadStream();

            while (auto block_right = reader.value()->read())
            {
                ++block_number;
                process_right_block(block_right.getColumns());
                if (rows_added > max_joined_block_rows)
                {
                    break;
                }
            }

            /// It means, that reader->read() returned {}
            if (rows_added <= max_joined_block_rows)
            {
                reader.reset();
            }
        }

        start_right_block = 0;

        if (rows_added > max_joined_block_rows)
        {
            not_processed = std::make_shared<NotProcessedCrossJoin>(
                NotProcessedCrossJoin{{block.cloneEmpty()}, left_row, block_number + 1, std::move(reader)});
            not_processed->block.swap(block);
            break;
        }
    }

    for (const ColumnWithTypeAndName & src_column : sample_block_with_columns_to_add)
        block.insert(src_column);

    block = block.cloneWithColumns(std::move(dst_columns));
}

DataTypePtr HashJoin::joinGetCheckAndGetReturnType(const DataTypes & data_types, const String & column_name, bool or_null) const
{
    size_t num_keys = data_types.size();
    if (right_table_keys.columns() != num_keys)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function joinGet{} doesn't match: passed, should be equal to {}",
            toString(or_null ? "OrNull" : ""),
            toString(num_keys));

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
        !MapGetter<JoinKind::Left, JoinStrictness::Any, false>::flagged,
        "joinGet are not protected from hash table changes between block processing");

    std::vector<const MapsOne *> maps_vector;
    maps_vector.push_back(&std::get<MapsOne>(data->maps[0]));
    HashJoinMethods<JoinKind::Left, JoinStrictness::Any, MapsOne>::joinBlockImpl(
        *this, keys, block_with_columns_to_add, maps_vector, /* is_join_get = */ true);
    return keys.getByPosition(keys.columns() - 1);
}

void HashJoin::checkTypesOfKeys(const Block & block) const
{
    for (const auto & onexpr : table_join->getClauses())
    {
        JoinCommon::checkTypesOfKeys(block, onexpr.key_names_left, right_table_keys, onexpr.key_names_right);
    }
}

void HashJoin::joinBlock(Block & block, ExtraBlockPtr & not_processed)
{
    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot join after data has been released");

    for (const auto & onexpr : table_join->getClauses())
    {
        auto cond_column_name = onexpr.condColumnNames();
        JoinCommon::checkTypesOfKeys(
            block, onexpr.key_names_left, cond_column_name.first, right_sample_block, onexpr.key_names_right, cond_column_name.second);
    }

    if (kind == JoinKind::Cross || kind == JoinKind::Comma)
    {
        joinBlockImplCross(block, not_processed);
        return;
    }

    materializeColumnsFromLeftBlock(block);

    bool prefer_use_maps_all = table_join->getMixedJoinExpression() != nullptr;
    {
        std::vector<const std::decay_t<decltype(data->maps[0])> *> maps_vector;
        maps_vector.reserve(table_join->getClauses().size());
        for (size_t i = 0; i < table_join->getClauses().size(); ++i)
            maps_vector.push_back(&data->maps[i]);

        if (joinDispatch(
                kind,
                strictness,
                maps_vector,
                prefer_use_maps_all,
                [&](auto kind_, auto strictness_, auto & maps_vector_)
                {
                    Block remaining_block;
                    if constexpr (std::is_same_v<std::decay_t<decltype(maps_vector_)>, std::vector<const MapsAll *>>)
                    {
                        remaining_block = HashJoinMethods<kind_, strictness_, MapsAll>::joinBlockImpl(
                            *this, block, sample_block_with_columns_to_add, maps_vector_);
                    }
                    else if constexpr (std::is_same_v<std::decay_t<decltype(maps_vector_)>, std::vector<const MapsOne *>>)
                    {
                        remaining_block = HashJoinMethods<kind_, strictness_, MapsOne>::joinBlockImpl(
                            *this, block, sample_block_with_columns_to_add, maps_vector_);
                    }
                    else if constexpr (std::is_same_v<std::decay_t<decltype(maps_vector_)>, std::vector<const MapsAsof *>>)
                    {
                        remaining_block = HashJoinMethods<kind_, strictness_, MapsAsof>::joinBlockImpl(
                            *this, block, sample_block_with_columns_to_add, maps_vector_);
                    }
                    else
                    {
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown maps type");
                    }
                    if (remaining_block.rows())
                        not_processed = std::make_shared<ExtraBlock>(ExtraBlock{std::move(remaining_block)});
                    else
                        not_processed.reset();
                }))
        {
            /// Joined
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong JOIN combination: {} {}", strictness, kind);
    }
}

void HashJoin::joinBlock(ScatteredBlock & block, ScatteredBlock & remaining_block)
{
    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot join after data has been released");

    chassert(kind == JoinKind::Left || kind == JoinKind::Inner);

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

    std::vector<const std::decay_t<decltype(data->maps[0])> *> maps_vector;
    maps_vector.reserve(table_join->getClauses().size());

    for (size_t i = 0; i < table_join->getClauses().size(); ++i)
        maps_vector.push_back(&data->maps[i]);

    bool prefer_use_maps_all = table_join->getMixedJoinExpression() != nullptr;
    [[maybe_unused]] const bool joined = joinDispatch(
        kind,
        strictness,
        maps_vector,
        prefer_use_maps_all,
        [&](auto kind_, auto strictness_, auto & maps_vector_)
        {
            if constexpr (std::is_same_v<std::decay_t<decltype(maps_vector_)>, std::vector<const MapsAll *>>)
            {
                remaining_block = HashJoinMethods<kind_, strictness_, MapsAll>::joinBlockImpl(
                    *this, block, sample_block_with_columns_to_add, maps_vector_);
            }
            else if constexpr (std::is_same_v<std::decay_t<decltype(maps_vector_)>, std::vector<const MapsOne *>>)
            {
                remaining_block = HashJoinMethods<kind_, strictness_, MapsOne>::joinBlockImpl(
                    *this, block, sample_block_with_columns_to_add, maps_vector_);
            }
            else if constexpr (std::is_same_v<std::decay_t<decltype(maps_vector_)>, std::vector<const MapsAsof *>>)
            {
                remaining_block = HashJoinMethods<kind_, strictness_, MapsAsof>::joinBlockImpl(
                    *this, block, sample_block_with_columns_to_add, maps_vector_);
            }
            else
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown maps type");
            }
        });

    chassert(joined);
}

HashJoin::~HashJoin()
{
    if (!data)
    {
        LOG_TEST(log, "{}Join data has been already released", instance_log_id);
        return;
    }
    LOG_TEST(
        log,
        "{}Join data is being destroyed, {} bytes and {} rows in hash table",
        instance_log_id,
        getTotalByteCount(),
        getTotalRowCount());
}

template <typename Mapped>
struct AdderNonJoined
{
    static void add(const Mapped & mapped, size_t & rows_added, MutableColumns & columns_right)
    {
        constexpr bool mapped_asof = std::is_same_v<Mapped, AsofRowRefs>;
        [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<Mapped, RowRef>;

        if constexpr (mapped_asof)
        {
            /// Do nothing
        }
        else if constexpr (mapped_one)
        {
            for (size_t j = 0; j < columns_right.size(); ++j)
            {
                const auto & mapped_column = (*mapped.columns)[j];
                columns_right[j]->insertFrom(*mapped_column, mapped.row_num);
            }

            ++rows_added;
        }
        else
        {
            for (auto it = mapped.begin(); it.ok(); ++it)
            {
                for (size_t j = 0; j < columns_right.size(); ++j)
                {
                    const auto & mapped_column = (*it->columns)[j];
                    columns_right[j]->insertFrom(*mapped_column, it->row_num);
                }

                ++rows_added;
            }
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
        : parent(parent_), max_block_size(max_block_size_), flag_per_row(flag_per_row_), current_block_start(0)
    {
        if (parent.data == nullptr)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot join after data has been released");
    }

    Block getEmptyBlock() override { return parent.savedBlockSample().cloneEmpty(); }

    size_t fillColumns(MutableColumns & columns_right) override
    {
        size_t rows_added = 0;
        if (unlikely(parent.data->type == HashJoin::Type::EMPTY))
        {
            rows_added = fillColumnsFromData(parent.data->columns, columns_right);
        }
        else
        {
            auto fill_callback = [&](auto, auto, auto & map) { rows_added = fillColumnsFromMap(map, columns_right); };

            bool prefer_use_maps_all = parent.table_join->getMixedJoinExpression() != nullptr;
            if (!joinDispatch(parent.kind, parent.strictness, parent.data->maps.front(), prefer_use_maps_all, fill_callback))
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "Unknown JOIN strictness '{}' (must be on of: ANY, ALL, ASOF)", parent.strictness);
        }

        if (!flag_per_row)
        {
            fillNullsFromBlocks(columns_right, rows_added);
        }

        return rows_added;
    }

private:
    const HashJoin & parent;
    UInt64 max_block_size;
    bool flag_per_row;

    size_t current_block_start;

    std::any position;
    std::optional<HashJoin::NullmapList::const_iterator> nulls_position;
    std::optional<HashJoin::ScatteredColumnsList::const_iterator> used_position;

    size_t fillColumnsFromData(const HashJoin::ScatteredColumnsList & columns, MutableColumns & columns_right)
    {
        if (!position.has_value())
            position = std::make_any<HashJoin::ScatteredColumnsList::const_iterator>(columns.begin());

        auto & block_it = std::any_cast<HashJoin::ScatteredColumnsList::const_iterator &>(position);
        auto end = columns.end();

        size_t rows_added = 0;
        for (; block_it != end; ++block_it)
        {
            size_t rows_from_block = std::min<size_t>(max_block_size - rows_added, block_it->columns.at(0)->size() - current_block_start);
            for (size_t j = 0; j < columns_right.size(); ++j)
            {
                const auto & col = block_it->columns[j];
                columns_right[j]->insertRangeFrom(*col, current_block_start, rows_from_block);
            }
            rows_added += rows_from_block;

            if (rows_added >= max_block_size)
            {
                /// How many rows have been read
                current_block_start += rows_from_block;
                if (block_it->columns.at(0)->size() <= current_block_start)
                {
                    /// current block was fully read
                    ++block_it;
                    current_block_start = 0;
                }
                break;
            }
            current_block_start = 0;
        }
        return rows_added;
    }

    template <typename Maps>
    size_t fillColumnsFromMap(const Maps & maps, MutableColumns & columns_keys_and_right)
    {
        switch (parent.data->type)
        {
#define M(TYPE) \
    case HashJoin::Type::TYPE: \
        return fillColumns(*maps.TYPE, columns_keys_and_right);
            APPLY_FOR_JOIN_VARIANTS(M)
#undef M
            default:
                throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unsupported JOIN keys (type: {})", parent.data->type);
        }
    }

    template <typename Map>
    size_t fillColumns(const Map & map, MutableColumns & columns_keys_and_right)
    {
        size_t rows_added = 0;

        if (flag_per_row)
        {
            if (!used_position.has_value())
                used_position = parent.data->columns.begin();

            auto end = parent.data->columns.end();

            for (auto & it = *used_position; it != end && rows_added < max_block_size; ++it)
            {
                const auto & mapped_block = *it;
                size_t rows = mapped_block.columns.at(0)->size();

                for (size_t row = 0; row < rows; ++row)
                {
                    if (!parent.isUsed(&mapped_block.columns, row))
                    {
                        for (size_t colnum = 0; colnum < columns_keys_and_right.size(); ++colnum)
                        {
                            columns_keys_and_right[colnum]->insertFrom(*mapped_block.columns[colnum], row);
                        }

                        ++rows_added;
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

            for (; it != end; ++it)
            {
                const Mapped & mapped = it->getMapped();

                size_t offset = map.offsetInternal(it.getPtr());
                if (parent.isUsed(offset))
                    continue;
                AdderNonJoined<Mapped>::add(mapped, rows_added, columns_keys_and_right);

                if (rows_added >= max_block_size)
                {
                    ++it;
                    break;
                }
            }
        }

        return rows_added;
    }

    void fillNullsFromBlocks(MutableColumns & columns_keys_and_right, size_t & rows_added)
    {
        if (!nulls_position.has_value())
            nulls_position = parent.data->nullmaps.begin();

        auto end = parent.data->nullmaps.end();

        for (auto & it = *nulls_position; it != end && rows_added < max_block_size; ++it)
        {
            const auto * columns = it->columns;
            ConstNullMapPtr nullmap = nullptr;
            if (it->column)
                nullmap = &assert_cast<const ColumnUInt8 &>(*it->column).getData();

            size_t rows = columns->columns.at(0)->size();
            for (size_t row = 0; row < rows; ++row)
            {
                if (nullmap && (*nullmap)[row])
                {
                    for (size_t col = 0; col < columns_keys_and_right.size(); ++col)
                        columns_keys_and_right[col]->insertFrom(*columns->columns[col], row);
                    ++rows_added;
                }
            }
        }
    }
};

IBlocksStreamPtr
HashJoin::getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const
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

    auto non_joined = std::make_unique<NotJoinedHash>(*this, max_block_size, flag_per_row);
    return std::make_unique<NotJoinedBlocks>(std::move(non_joined), result_sample_block, left_columns_count, *table_join);
}

void HashJoin::reuseJoinedData(const HashJoin & join)
{
    have_compressed = join.have_compressed;
    data = join.data;
    from_storage_join = true;

    bool flag_per_row = needUsedFlagsForPerRightTableRow(table_join);
    if (flag_per_row)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "StorageJoin with ORs is not supported");

    bool prefer_use_maps_all = join.table_join->getMixedJoinExpression() != nullptr;
    for (auto & map : data->maps)
    {
        joinDispatch(
            kind,
            strictness,
            map,
            prefer_use_maps_all,
            [this](auto kind_, auto strictness_, auto & map_)
            {
                used_flags->reinit<kind_, strictness_, std::is_same_v<std::decay_t<decltype(map_)>, MapsAll>>(
                    map_.getBufferSizeInCells(data->type) + 1);
            });
    }
}

BlocksList HashJoin::releaseJoinedBlocks(bool restructure [[maybe_unused]])
{
    LOG_TRACE(
        log, "{}Join data is being released, {} bytes and {} rows in hash table", instance_log_id, getTotalByteCount(), getTotalRowCount());

    auto extract_source_blocks = [](ScatteredColumnsList && columns_list, const Block & sample_block)
    {
        BlocksList result;
        for (auto & columns : columns_list)
            result.emplace_back(sample_block.cloneWithColumns(columns.columns));
        return result;
    };

    ScatteredColumnsList right_columns = std::move(data->columns);
    if (!restructure)
    {
        auto sample_block = std::move(data->sample_block);
        data.reset();
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
        Block restored_block;
        for (size_t i = 0; i < positions.size(); ++i)
        {
            auto column = data->sample_block.getByPosition(positions[i]);
            column.column = saved_columns.columns[positions[i]];
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
}

bool HashJoin::isUsed(size_t off) const
{
    return used_flags->getUsedSafe(off);
}

bool HashJoin::isUsed(const Columns * columns_ptr, size_t row_idx) const
{
    return used_flags->getUsedSafe(columns_ptr, row_idx);
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
        auto merge_rows_into_one_block = [&](ScatteredColumnsList & columns_list, RowRefList & rows_ref)
        {
            auto it = rows_ref.begin();
            if (!it.ok())
                return;

            if (columns_list.empty() || columns_list.back().columns.at(0)->size() >= DEFAULT_BLOCK_SIZE)
            {
                Columns columns;
                columns.reserve(it->columns->size());
                for (const auto & col : *(*it)->columns)
                    columns.push_back(col->cloneEmpty());
                columns_list.emplace_back(std::move(columns), ScatteredBlock::Selector());
            }

            auto & columns = columns_list.back().columns;
            size_t start_row = columns.at(0)->size();
            for (; it.ok(); ++it)
            {
                for (size_t i = 0; i < columns.size(); ++i)
                {
                    auto & col = columns[i]->assumeMutableRef();
                    col.insertFrom(*((*it->columns)[i]), it->row_num);
                }
            }
            size_t new_rows = columns.at(0)->size();
            if (new_rows > start_row)
            {
                RowRefList new_rows_ref(&columns, start_row, new_rows - start_row);
                rows_ref = std::move(new_rows_ref);
            }
        };

        auto visit_rows_map = [&](ScatteredColumnsList & columns, MapsAll & rows_map)
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
                default:
                    break;
            }
        };
        ScatteredColumnsList sorted_columns;
        visit_rows_map(sorted_columns, map);
        doDebugAsserts();
        data->columns.swap(sorted_columns);
        size_t new_blocks_allocated_size = 0;
        for (auto & columns : data->columns)
        {
            columns.selector = ScatteredBlock::Selector(columns.columns.at(0)->size());
            new_blocks_allocated_size += columns.allocatedBytes();
        }
        data->allocated_size = new_blocks_allocated_size;
        doDebugAsserts();
    }
}

bool HashJoin::rightTableCanBeReranged() const
{
    return table_join->allowJoinSorting()
            && !table_join->getMixedJoinExpression()
            && isInnerOrLeft(kind)
            && strictness == JoinStrictness::All;
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

    /// We should not rerange the right table on such conditions:
    /// 1. the right table is already reranged by key or it is empty.
    /// 2. the join clauses size is greater than 1, like `...join on a.key1=b.key1 or a.key2=b.key2`, we can not rerange the right table on different set of keys.
    /// 3. the number of right table rows exceed the threshold, which may result in a significant cost for reranging and lead to performance degradation.
    /// 4. the keys of right table is very sparse, which may result in insignificant performance improvement after reranging by key.
    if (!data || data->sorted || data->columns.empty() || data->maps.size() > 1 || data->rows_to_join > table_join->sortRightMaximumTableRows() ||  data->avgPerKeyRows() < table_join->sortRightMinimumPerkeyRows())
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
        /*prefer_use_maps_all*/ false,
        [&](auto kind_, auto strictness_, auto & map_) { tryRerangeRightTableDataImpl<kind_, decltype(map_), strictness_>(map_); });
    chassert(result);
    data->sorted = true;
}

void HashJoin::onBuildPhaseFinish()
{
    if (needUsedFlagsForPerRightTableRow(table_join))
        return;

    const bool prefer_use_maps_all = table_join->getMixedJoinExpression() != nullptr;
    for (auto & map : data->maps)
    {
        joinDispatch(
            kind,
            strictness,
            map,
            prefer_use_maps_all,
            [this](auto kind_, auto strictness_, auto & map_)
            {
                used_flags->reinit<kind_, strictness_, std::is_same_v<std::decay_t<decltype(map_)>, MapsAll>>(
                    map_.getBufferSizeInCells(data->type) + 1);
            });
    }
}
}
