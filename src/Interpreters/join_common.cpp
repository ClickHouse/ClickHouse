#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <DataStreams/materializeBlock.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/join_common.h>
#include <Interpreters/castColumn.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int LOGICAL_ERROR;
}

namespace
{


void changeNullabilityInplace(ColumnPtr & column)
{
    if (const auto * nullable = checkAndGetColumn<ColumnNullable>(*column))
        column = nullable->getNestedColumnPtr();
    else
        column = makeNullable(column);
}

void changeNullability(MutableColumnPtr & mutable_column)
{
    ColumnPtr column = std::move(mutable_column);
    changeNullabilityInplace(column);
    mutable_column = IColumn::mutate(std::move(column));
}

ColumnPtr changeLowCardinality(const ColumnPtr & column, const ColumnPtr & dst_sample)
{
    if (dst_sample->lowCardinality())
    {
        MutableColumnPtr lc = dst_sample->cloneEmpty();
        typeid_cast<ColumnLowCardinality &>(*lc).insertRangeFromFullColumn(*column, 0, column->size());
        return lc;
    }

    return column->convertToFullColumnIfLowCardinality();
}

}

namespace JoinCommon
{

void convertColumnToNullable(ColumnWithTypeAndName & column, bool low_card_nullability)
{
    if (low_card_nullability && column.type->lowCardinality())
    {
        column.column = recursiveRemoveLowCardinality(column.column);
        column.type = recursiveRemoveLowCardinality(column.type);
    }

    if (column.type->isNullable() || !column.type->canBeInsideNullable())
        return;

    column.type = makeNullable(column.type);
    if (column.column)
        column.column = makeNullable(column.column);
}

void convertColumnsToNullable(Block & block, size_t starting_pos)
{
    for (size_t i = starting_pos; i < block.columns(); ++i)
        convertColumnToNullable(block.getByPosition(i));
}

/// @warning It assumes that every NULL has default value in nested column (or it does not matter)
void removeColumnNullability(ColumnPtr & column)
{
    if (column)
    {
        const auto * nullable_column = checkAndGetColumn<ColumnNullable>(*column);
        if (nullable_column == nullptr)
            return;
        ColumnPtr nested_column = nullable_column->getNestedColumnPtr();
        MutableColumnPtr mutable_column = IColumn::mutate(std::move(nested_column));
        column = std::move(mutable_column);
    }
}

void removeColumnNullability(ColumnWithTypeAndName & column)
{
    if (!column.type->isNullable())
        return;

    column.type = static_cast<const DataTypeNullable &>(*column.type).getNestedType();
    removeColumnNullability(column.column);
}

/// Change both column nullability and low cardinality
void changeColumnRepresentation(const ColumnPtr & src_column, ColumnPtr & dst_column)
{
    bool nullable_src = src_column->isNullable();
    bool nullable_dst = dst_column->isNullable();

    ColumnPtr dst_not_null = JoinCommon::emptyNotNullableClone(dst_column);
    bool lowcard_src = JoinCommon::emptyNotNullableClone(src_column)->lowCardinality();
    bool lowcard_dst = dst_not_null->lowCardinality();
    bool change_lowcard = lowcard_src != lowcard_dst;

    if (nullable_src && !nullable_dst)
    {
        const auto * nullable = checkAndGetColumn<ColumnNullable>(*src_column);
        if (change_lowcard)
            dst_column = changeLowCardinality(nullable->getNestedColumnPtr(), dst_column);
        else
            dst_column = nullable->getNestedColumnPtr();
    }
    else if (!nullable_src && nullable_dst)
    {
        if (change_lowcard)
            dst_column = makeNullable(changeLowCardinality(src_column, dst_not_null));
        else
            dst_column = makeNullable(src_column);
    }
    else /// same nullability
    {
        if (change_lowcard)
        {
            if (const auto * nullable = checkAndGetColumn<ColumnNullable>(*src_column))
            {
                dst_column = makeNullable(changeLowCardinality(nullable->getNestedColumnPtr(), dst_not_null));
                assert_cast<ColumnNullable &>(*dst_column->assumeMutable()).applyNullMap(nullable->getNullMapColumn());
            }
            else
                dst_column = changeLowCardinality(src_column, dst_not_null);
        }
        else
            dst_column = src_column;
    }
}

ColumnPtr emptyNotNullableClone(const ColumnPtr & column)
{
    if (column->isNullable())
        return checkAndGetColumn<ColumnNullable>(*column)->getNestedColumnPtr()->cloneEmpty();
    return column->cloneEmpty();
}

Columns materializeColumns(const Columns & columns)
{
    Columns materialized;
    materialized.reserve(columns.size());

    for (const auto & src_column : columns)
    {
        materialized.emplace_back(recursiveRemoveLowCardinality(src_column->convertToFullColumnIfConst()));
    }

    return materialized;
}


Columns materializeColumns(const Block & block, const Names & names)
{
    Columns materialized;
    materialized.reserve(names.size());

    for (const auto & column_name : names)
    {
        const auto & src_column = block.getByName(column_name).column;
        materialized.emplace_back(recursiveRemoveLowCardinality(src_column->convertToFullColumnIfConst()));
    }

    return materialized;
}

/// Join keys not casted in some cases:
/// - for tables with `engine = Join` (in this case left_block hasn't rows)
/// - if some keys are not nullable (this limitation comes from accurateCastOrNull)
/// - `FULL JOIN ... USING` (not implemented, key column will contain values from columns from both tables and should be supertype of them)
bool isCastJoinKeysAllowed(const Block & left_block, const Block & right_block, const TableJoin & table_join)
{
    bool key_names_match = table_join.keyNamesLeft().size() == table_join.keyNamesRight().size();
    bool using_forbidden = table_join.hasUsing()
        && !isLeft(table_join.kind())
        && !isInner(table_join.kind())
        && !isRight(table_join.kind());
    bool is_storage_join = table_join.hasJoinedStorage();
    if (!left_block || !right_block || !key_names_match || using_forbidden || is_storage_join)
        return false;

    auto check_type_of_keys = [](const Names & names, const Block & block)
    {
        for (const auto & name : names)
        {
            const auto typ = block.getByName(name).type;
            if (!typ->isNullable() && !typ->canBeInsideNullable())
                return false;
        }
        return true;
    };

    bool types_is_nullable = check_type_of_keys(table_join.keyNamesLeft(), left_block)
        && check_type_of_keys(table_join.keyNamesRight(), right_block);

    return types_is_nullable;
}

void addCastedJoinColumns(Block & block, std::unordered_map<std::string, NameAndTypePair> name_mapping)
{
    for (const auto & [col_name, target] : name_mapping)
    {
        ColumnWithTypeAndName col = block.getByName(col_name);
        if (!col.column->lowCardinality() && col.type->lowCardinality())
        {
            /// type can be wrong and we need to fix it for castColumnAccurateOrNull
            col.type = removeLowCardinality(col.type);
        }
        ColumnPtr key_col = castColumnAccurateOrNull(col, target.type);
        block.insert({std::move(key_col), makeNullable(target.type), target.name});
    }
}

void restoreCastedJoinColumns(Block & block, std::unordered_map<std::string, NameAndTypePair> name_mapping)
{
    std::set<size_t> cols_to_remove;
    for (const auto & nm : name_mapping)
        cols_to_remove.insert(block.getPositionByName(nm.second.name));
    block.erase(cols_to_remove);
}

void castColumnInplace(ColumnWithTypeAndName & col, const ColumnWithTypeAndName & dst_sample)
{
    DataTypePtr target_type = dst_sample.type;

    if (typesEqualUpToNullability(col.type, target_type))
        return;

    if (target_type->lowCardinality() || col.type->lowCardinality())
        throw DB::Exception("Casting LowCardinality columns in join is not valid", ErrorCodes::LOGICAL_ERROR);

    if (col.type->isNullable() != target_type->isNullable())
    {
        changeNullabilityInplace(col.column);
        if (target_type->isNullable())
            col.type = makeNullable(col.type);
        else
            col.type = removeNullable(col.type);
    }
    col.column = castColumnAccurate(col, target_type);
    col.type = target_type;
}

ColumnRawPtrs getRawPointers(const Columns & columns)
{
    ColumnRawPtrs ptrs;
    ptrs.reserve(columns.size());

    for (const auto & column : columns)
        ptrs.push_back(column.get());

    return ptrs;
}

void removeLowCardinalityInplace(Block & block)
{
    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto & col = block.getByPosition(i);
        col.column = recursiveRemoveLowCardinality(col.column);
        col.type = recursiveRemoveLowCardinality(col.type);
    }
}

void removeLowCardinalityInplace(Block & block, const Names & names, bool change_type)
{
    for (const String & column_name : names)
    {
        auto & col = block.getByName(column_name);
        col.column = recursiveRemoveLowCardinality(col.column);
        if (change_type)
            col.type = recursiveRemoveLowCardinality(col.type);
    }
}

void restoreLowCardinalityInplace(Block & block)
{
    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto & col = block.getByPosition(i);
        if (col.type->lowCardinality() && col.column && !col.column->lowCardinality())
            col.column = changeLowCardinality(col.column, col.type->createColumn());
    }
}

Columns extractKeysForJoin(const Block & block, const Names & key_names, const NameToTypeMap & cast_columns, bool remove_nullability)
{
    size_t keys_size = key_names.size();
    Columns key_columns(keys_size);

    for (size_t i = 0; i < keys_size; ++i)
    {
        const String & column_name = key_names[i];
        const auto & col = block.getByName(column_name);

        if (const auto type_map = cast_columns.find(column_name); type_map != cast_columns.end())
            key_columns[i] = castColumnAccurateOrNull(col, type_map->second);
        else
            key_columns[i] = col.column;

        const ColumnNullable * nullable;
        if (remove_nullability && (nullable = checkAndGetColumn<ColumnNullable>(*key_columns[i])))
            key_columns[i] = nullable->getNestedColumnPtr();
    }

    return key_columns;
}

bool typesEqualUpToNullability(DataTypePtr left_type, DataTypePtr right_type)
{
    DataTypePtr left_type_strict = removeNullable(recursiveRemoveLowCardinality(left_type));
    DataTypePtr right_type_strict = removeNullable(recursiveRemoveLowCardinality(right_type));
    return left_type_strict->equals(*right_type_strict);
}

void checkTypesOfKeys(const Block & block_left, const Names & key_names_left, const Block & block_right, const Names & key_names_right)
{
    size_t keys_size = key_names_left.size();

    for (size_t i = 0; i < keys_size; ++i)
    {
        DataTypePtr left_type = block_left.getByName(key_names_left[i]).type;
        DataTypePtr right_type = block_right.getByName(key_names_right[i]).type;

        if (!typesEqualUpToNullability(left_type, right_type))
            throw Exception("Type mismatch of columns to JOIN by: "
                + key_names_left[i] + " " + left_type->getName() + " at left, "
                + key_names_right[i] + " " + right_type->getName() + " at right",
                ErrorCodes::TYPE_MISMATCH);
    }
}

NameToTypeMap getJoinColumnsNeedCast(const Block & block_converted, const Names & key_names_converted,
                                     const Block & block_unchanged, const Names & key_names_unchanged)
{
    if (key_names_converted.size() != key_names_unchanged.size())
    {
        throw DB::Exception("Number of keys don't match", ErrorCodes::LOGICAL_ERROR);
    }

    NameToTypeMap need_conversion;
    for (size_t i = 0; i < key_names_converted.size(); ++i)
    {
        const auto & converted_col = block_converted.getByName(key_names_converted[i]);
        const auto & unchanged_col = block_unchanged.getByName(key_names_unchanged[i]);
        if (typesEqualUpToNullability(converted_col.type, unchanged_col.type))
            continue;

        need_conversion.emplace(converted_col.name, unchanged_col.type);
        try
        {
            castColumnAccurateOrNull(converted_col, unchanged_col.type);
        }
        catch (DB::Exception &)
        {
            throw Exception("Type mismatch of columns to JOIN by: "
                            + key_names_converted[i] + " " + converted_col.type->getName() + " at left, "
                            + key_names_unchanged[i] + " " + unchanged_col.type->getName() + " at right",
                            ErrorCodes::TYPE_MISMATCH);
        }
    }
    return need_conversion;
}

void createMissedColumns(Block & block)
{
    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto & column = block.getByPosition(i);
        if (!column.column)
            column.column = column.type->createColumn();
    }
}

void joinTotals(const Block & totals, const Block & columns_to_add, const Names & key_names_right, Block & block)
{
    if (Block totals_without_keys = totals)
    {
        for (const auto & name : key_names_right)
            totals_without_keys.erase(totals_without_keys.getPositionByName(name));

        for (size_t i = 0; i < totals_without_keys.columns(); ++i)
            block.insert(totals_without_keys.safeGetByPosition(i));
    }
    else
    {
        /// We will join empty `totals` - from one row with the default values.

        for (size_t i = 0; i < columns_to_add.columns(); ++i)
        {
            const auto & col = columns_to_add.getByPosition(i);
            block.insert({
                col.type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst(),
                col.type,
                col.name});
        }
    }
}

/// Return mapping from left columns that need to have type from right table to names from right
NameToNameMap getLeftKeysToRemap(const TableJoin & table_join)
{
    NameToNameMap mapping;
    if (table_join.hasUsing())
    {
        const auto & required_right_keys = table_join.requiredRightKeys();
        for (size_t i = 0; i < table_join.keyNamesLeft().size(); ++i)
        {
            const String & left_key_name = table_join.keyNamesLeft()[i];
            const String & right_key_name = table_join.keyNamesRight()[i];
            if (!required_right_keys.contains(right_key_name))
                mapping[left_key_name] = right_key_name;
        }
    }
    return mapping;
}

/// Return mapping from left column that need to have type from right table to corresponding types from right
NameToTypeMap getLeftKeysToRemapType(const TableJoin & table_join, NamesAndTypesList right_columns)
{
    NameToTypeMap right_name_to_type;
    for (const auto & col : right_columns)
        right_name_to_type[col.name] = col.type;

    NameToTypeMap left_name_to_remap_type;

    for (const auto & [left_name, right_name] : getLeftKeysToRemap(table_join))
    {
        const auto right_type = right_name_to_type.find(right_name);
        if (right_type == right_name_to_type.end())
            throw DB::Exception(
                "Column " + right_name + " not found in " + right_columns.toString(), ErrorCodes::LOGICAL_ERROR);
        left_name_to_remap_type[left_name] = right_type->second;
    }
    return left_name_to_remap_type;
}

void remapLeftKeysToRight(Block & block, const Block & right_table_keys, const TableJoin & table_join)
{
    if (isRight(table_join.kind()))
    {
        auto names_to_remap = JoinCommon::getLeftKeysToRemap(table_join);
        for (const auto & [left_name, right_name] : names_to_remap)
            JoinCommon::castColumnInplace(block.getByName(left_name), right_table_keys.getByName(right_name));
    }
}

}


NotJoined::NotJoined(const TableJoin & table_join, const Block & saved_block_sample_, const Block & right_sample_block,
                     const Block & result_sample_block_)
    : saved_block_sample(saved_block_sample_)
    , result_sample_block(materializeBlock(result_sample_block_))
{
    std::vector<String> tmp;
    Block right_table_keys;
    Block sample_block_with_columns_to_add;
    table_join.splitAdditionalColumns(right_sample_block, right_table_keys, sample_block_with_columns_to_add);
    Block required_right_keys = table_join.getRequiredRightKeys(right_table_keys, tmp);

    auto names_to_remap = JoinCommon::getLeftKeysToRemap(table_join);
    std::unordered_map<size_t, size_t> left_to_right_key_remap;

    for (const auto & [left_key_name, right_key_name] : names_to_remap)
    {
        size_t left_key_pos = result_sample_block.getPositionByName(left_key_name);
        size_t right_key_pos = saved_block_sample.getPositionByName(right_key_name);
        left_to_right_key_remap[left_key_pos] = right_key_pos;
    }

    /// result_sample_block: left_sample_block + left expressions, right not key columns, required right keys
    size_t left_columns_count = result_sample_block.columns() -
        sample_block_with_columns_to_add.columns() - required_right_keys.columns();

    for (size_t left_pos = 0; left_pos < left_columns_count; ++left_pos)
    {
        /// We need right 'x' for 'RIGHT JOIN ... USING(x)'.
        if (left_to_right_key_remap.count(left_pos))
        {
            size_t right_key_pos = left_to_right_key_remap[left_pos];
            setRightIndex(right_key_pos, left_pos);
        }
        else
            column_indices_left.emplace_back(left_pos);
    }

    for (size_t right_pos = 0; right_pos < saved_block_sample.columns(); ++right_pos)
    {
        const String & name = saved_block_sample.getByPosition(right_pos).name;
        if (!result_sample_block.has(name))
            continue;

        size_t result_position = result_sample_block.getPositionByName(name);

        /// Don't remap left keys twice. We need only qualified right keys here
        if (result_position < left_columns_count)
            continue;

        setRightIndex(right_pos, result_position);
    }

    if (column_indices_left.size() + column_indices_right.size() + same_result_keys.size() != result_sample_block.columns())
        throw Exception("Error in columns mapping in RIGHT|FULL JOIN. Left: " + toString(column_indices_left.size()) +
                        ", right: " + toString(column_indices_right.size()) +
                        ", same: " + toString(same_result_keys.size()) +
                        ", result: " + toString(result_sample_block.columns()),
                        ErrorCodes::LOGICAL_ERROR);
}

void NotJoined::setRightIndex(size_t right_pos, size_t result_position)
{
    if (!column_indices_right.count(right_pos))
    {
        column_indices_right[right_pos] = result_position;
        extractColumnChanges(right_pos, result_position);
    }
    else
        same_result_keys[result_position] = column_indices_right[right_pos];
}

void NotJoined::extractColumnChanges(size_t right_pos, size_t result_pos)
{
    const auto & src = saved_block_sample.getByPosition(right_pos);
    const auto & dst = result_sample_block.getByPosition(result_pos);

    #ifndef NDEBUG
        if (!JoinCommon::typesEqualUpToNullability(src.type, dst.type))
            throw Exception("Wrong columns assign: " + src.type->getName() +
                            " " + dst.type->getName(), ErrorCodes::LOGICAL_ERROR);
    #endif

    if (!src.column->isNullable() && dst.column->isNullable())
        right_nullability_adds.push_back(right_pos);

    if (src.column->isNullable() && !dst.column->isNullable())
        right_nullability_removes.push_back(right_pos);

    ColumnPtr src_not_null = JoinCommon::emptyNotNullableClone(src.column);
    ColumnPtr dst_not_null = JoinCommon::emptyNotNullableClone(dst.column);

    if (src_not_null->lowCardinality() != dst_not_null->lowCardinality())
        right_lowcard_changes.push_back({right_pos, dst_not_null});
}

void NotJoined::correctLowcardAndNullability(MutableColumns & columns_right)
{
    for (size_t pos : right_nullability_removes)
        changeNullability(columns_right[pos]);

    for (auto & [pos, dst_sample] : right_lowcard_changes)
        columns_right[pos] = changeLowCardinality(std::move(columns_right[pos]), dst_sample)->assumeMutable();

    for (size_t pos : right_nullability_adds)
        changeNullability(columns_right[pos]);
}

void NotJoined::addLeftColumns(Block & block, size_t rows_added) const
{
    /// @note it's possible to make ColumnConst here and materialize it later
    for (size_t pos : column_indices_left)
        block.getByPosition(pos).column = block.getByPosition(pos).column->cloneResized(rows_added);
}

void NotJoined::addRightColumns(Block & block, MutableColumns & columns_right) const
{
    for (const auto & pr : column_indices_right)
    {
        auto & right_column = columns_right[pr.first];
        auto & result_column = block.getByPosition(pr.second).column;
#ifndef NDEBUG
        if (result_column->getName() != right_column->getName())
            throw Exception("Wrong columns assign in RIGHT|FULL JOIN: " + result_column->getName() +
                            " " + right_column->getName(), ErrorCodes::LOGICAL_ERROR);
#endif
        result_column = std::move(right_column);
    }
}

void NotJoined::copySameKeys(Block & block) const
{
    for (const auto & pr : same_result_keys)
    {
        auto & src_column = block.getByPosition(pr.second).column;
        auto & dst_column = block.getByPosition(pr.first).column;
        JoinCommon::changeColumnRepresentation(src_column, dst_column);
    }
}

}
