#include <Interpreters/join_common.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/ActionsDAG.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataStreams/materializeBlock.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int LOGICAL_ERROR;
}

namespace
{

void changeNullability(MutableColumnPtr & mutable_column)
{
    ColumnPtr column = std::move(mutable_column);
    if (const auto * nullable = checkAndGetColumn<ColumnNullable>(*column))
        column = nullable->getNestedColumnPtr();
    else
        column = makeNullable(column);

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

void changeLowCardinalityInplace(ColumnWithTypeAndName & column)
{
    if (column.type->lowCardinality())
    {
        column.type = recursiveRemoveLowCardinality(column.type);
        column.column = column.column->convertToFullColumnIfLowCardinality();
    }
    else
    {
        column.type = std::make_shared<DataTypeLowCardinality>(column.type);
        MutableColumnPtr lc = column.type->createColumn();
        typeid_cast<ColumnLowCardinality &>(*lc).insertRangeFromFullColumn(*column.column, 0, column.column->size());
        column.column = std::move(lc);
    }
}

bool canBecomeNullable(const DataTypePtr & type)
{
    bool can_be_inside = type->canBeInsideNullable();
    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        can_be_inside |= low_cardinality_type->getDictionaryType()->canBeInsideNullable();
    return can_be_inside;
}

/// Add nullability to type.
/// Note: LowCardinality(T) transformed to LowCardinality(Nullable(T))
DataTypePtr convertTypeToNullable(const DataTypePtr & type)
{
    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
    {
        const auto & dict_type = low_cardinality_type->getDictionaryType();
        if (dict_type->canBeInsideNullable())
            return std::make_shared<DataTypeLowCardinality>(makeNullable(dict_type));
    }
    return makeNullable(type);
}

void convertColumnToNullable(ColumnWithTypeAndName & column, bool remove_low_card)
{
    if (remove_low_card && column.type->lowCardinality())
    {
        column.column = recursiveRemoveLowCardinality(column.column);
        column.type = recursiveRemoveLowCardinality(column.type);
    }

    if (column.type->isNullable() || !canBecomeNullable(column.type))
        return;

    column.type = convertTypeToNullable(column.type);

    if (column.column)
    {
        if (column.column->lowCardinality())
        {
            /// Convert nested to nullable, not LowCardinality itself
            auto mut_col = IColumn::mutate(std::move(column.column));
            ColumnLowCardinality * col_as_lc = assert_cast<ColumnLowCardinality *>(mut_col.get());
            if (!col_as_lc->nestedIsNullable())
                col_as_lc->nestedToNullable();
            column.column = std::move(mut_col);
        }
        else
            column.column = makeNullable(column.column);
    }
}

void convertColumnsToNullable(Block & block, size_t starting_pos)
{
    for (size_t i = starting_pos; i < block.columns(); ++i)
        convertColumnToNullable(block.getByPosition(i));
}

/// @warning It assumes that every NULL has default value in nested column (or it does not matter)
void removeColumnNullability(ColumnWithTypeAndName & column)
{
    if (column.type->lowCardinality())
    {
        /// LowCardinality(Nullable(T)) case
        const auto & dict_type = typeid_cast<const DataTypeLowCardinality *>(column.type.get())->getDictionaryType();
        column.type = std::make_shared<DataTypeLowCardinality>(removeNullable(dict_type));

        if (column.column && column.column->lowCardinality())
        {
            auto mut_col = IColumn::mutate(std::move(column.column));
            ColumnLowCardinality * col_as_lc = typeid_cast<ColumnLowCardinality *>(mut_col.get());
            if (col_as_lc && col_as_lc->nestedIsNullable())
                col_as_lc->nestedRemoveNullable();
            column.column = std::move(mut_col);
        }

        return;
    }

    if (!column.type->isNullable())
        return;

    column.type = static_cast<const DataTypeNullable &>(*column.type).getNestedType();
    if (column.column)
    {
        const auto * nullable_column = checkAndGetColumn<ColumnNullable>(*column.column);
        ColumnPtr nested_column = nullable_column->getNestedColumnPtr();
        MutableColumnPtr mutable_column = IColumn::mutate(std::move(nested_column));
        column.column = std::move(mutable_column);
    }
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

ColumnRawPtrs materializeColumnsInplace(Block & block, const Names & names)
{
    ColumnRawPtrs ptrs;
    ptrs.reserve(names.size());

    for (const auto & column_name : names)
    {
        auto & column = block.getByName(column_name).column;
        column = recursiveRemoveLowCardinality(column->convertToFullColumnIfConst());
        ptrs.push_back(column.get());
    }

    return ptrs;
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

ColumnRawPtrs extractKeysForJoin(const Block & block_keys, const Names & key_names)
{
    size_t keys_size = key_names.size();
    ColumnRawPtrs key_columns(keys_size);

    for (size_t i = 0; i < keys_size; ++i)
    {
        const String & column_name = key_names[i];
        key_columns[i] = block_keys.getByName(column_name).column.get();

        /// We will join only keys, where all components are not NULL.
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(*key_columns[i]))
            key_columns[i] = &nullable->getNestedColumn();
    }

    return key_columns;
}

void checkTypesOfKeys(const Block & block_left, const Names & key_names_left, const Block & block_right, const Names & key_names_right)
{
    size_t keys_size = key_names_left.size();

    for (size_t i = 0; i < keys_size; ++i)
    {
        DataTypePtr left_type = removeNullable(recursiveRemoveLowCardinality(block_left.getByName(key_names_left[i]).type));
        DataTypePtr right_type = removeNullable(recursiveRemoveLowCardinality(block_right.getByName(key_names_right[i]).type));

        if (!left_type->equals(*right_type))
            throw Exception("Type mismatch of columns to JOIN by: "
                + key_names_left[i] + " " + left_type->getName() + " at left, "
                + key_names_right[i] + " " + right_type->getName() + " at right",
                ErrorCodes::TYPE_MISMATCH);
    }
}

void createMissedColumns(Block & block)
{
    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto & column = block.getByPosition(i);
        if (!column.column) //-V1051
            column.column = column.type->createColumn();
    }
}

/// Append totals from right to left block, correct types if needed
void joinTotals(Block left_totals, Block right_totals, const TableJoin & table_join, Block & out_block)
{
    if (table_join.forceNullableLeft())
        JoinCommon::convertColumnsToNullable(left_totals);

    if (table_join.forceNullableRight())
        JoinCommon::convertColumnsToNullable(right_totals);

    for (auto & col : out_block)
    {
        if (const auto * left_col = left_totals.findByName(col.name))
            col = *left_col;
        else if (const auto * right_col = right_totals.findByName(col.name))
            col = *right_col;
        else
            col.column = col.type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst();

        /// In case of using `arrayJoin` we can get more or less rows than one
        if (col.column->size() != 1)
            col.column = col.column->cloneResized(1);
    }
}

void addDefaultValues(IColumn & column, const DataTypePtr & type, size_t count)
{
    column.reserve(column.size() + count);
    for (size_t i = 0; i < count; ++i)
        type->insertDefaultInto(column);
}

bool typesEqualUpToNullability(DataTypePtr left_type, DataTypePtr right_type)
{
    DataTypePtr left_type_strict = removeNullable(recursiveRemoveLowCardinality(left_type));
    DataTypePtr right_type_strict = removeNullable(recursiveRemoveLowCardinality(right_type));
    return left_type_strict->equals(*right_type_strict);
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

    std::unordered_map<size_t, size_t> left_to_right_key_remap;

    if (table_join.hasUsing())
    {
        for (size_t i = 0; i < table_join.keyNamesLeft().size(); ++i)
        {
            const String & left_key_name = table_join.keyNamesLeft()[i];
            const String & right_key_name = table_join.keyNamesRight()[i];

            size_t left_key_pos = result_sample_block.getPositionByName(left_key_name);
            size_t right_key_pos = saved_block_sample.getPositionByName(right_key_name);

            if (!required_right_keys.has(right_key_name))
                left_to_right_key_remap[left_key_pos] = right_key_pos;
        }
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
    const auto & src = saved_block_sample.getByPosition(right_pos).column;
    const auto & dst = result_sample_block.getByPosition(result_pos).column;

    if (!src->isNullable() && dst->isNullable())
        right_nullability_adds.push_back(right_pos);

    if (src->isNullable() && !dst->isNullable())
        right_nullability_removes.push_back(right_pos);

    ColumnPtr src_not_null = JoinCommon::emptyNotNullableClone(src);
    ColumnPtr dst_not_null = JoinCommon::emptyNotNullableClone(dst);

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
    for (size_t pos : column_indices_left)
    {
        auto & col = block.getByPosition(pos);

        auto mut_col = col.column->cloneEmpty();
        JoinCommon::addDefaultValues(*mut_col, col.type, rows_added);
        col.column = std::move(mut_col);
    }
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
