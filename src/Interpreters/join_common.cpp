#include <Interpreters/join_common.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataStreams/materializeBlock.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
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
void removeColumnNullability(ColumnWithTypeAndName & column)
{
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

void removeLowCardinalityInplace(Block & block, const Names & names)
{
    for (const String & column_name : names)
    {
        auto & col = block.getByName(column_name);
        col.column = recursiveRemoveLowCardinality(col.column);
        col.type = recursiveRemoveLowCardinality(col.type);
    }
}

void splitAdditionalColumns(const Block & sample_block, const Names & key_names, Block & block_keys, Block & block_others)
{
    block_others = materializeBlock(sample_block);

    for (const String & column_name : key_names)
    {
        /// Extract right keys with correct keys order. There could be the same key names.
        if (!block_keys.has(column_name))
        {
            auto & col = block_others.getByName(column_name);
            block_keys.insert(col);
            block_others.erase(column_name);
        }
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

}
}
