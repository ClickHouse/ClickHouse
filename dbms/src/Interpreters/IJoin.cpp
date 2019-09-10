#include <Interpreters/IJoin.h>
#include <Columns/ColumnNullable.h>
#include <DataStreams/materializeBlock.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{

ColumnRawPtrs extractKeysForJoin(const Names & key_names_right, const Block & right_sample_block,
                                 Block & sample_block_with_keys, Block & sample_block_with_columns_to_add)
{
    size_t keys_size = key_names_right.size();
    ColumnRawPtrs key_columns(keys_size);

    sample_block_with_columns_to_add = materializeBlock(right_sample_block);

    for (size_t i = 0; i < keys_size; ++i)
    {
        const String & column_name = key_names_right[i];

        /// there could be the same key names
        if (sample_block_with_keys.has(column_name))
        {
            key_columns[i] = sample_block_with_keys.getByName(column_name).column.get();
            continue;
        }

        auto & col = sample_block_with_columns_to_add.getByName(column_name);
        col.column = recursiveRemoveLowCardinality(col.column);
        col.type = recursiveRemoveLowCardinality(col.type);

        /// Extract right keys with correct keys order.
        sample_block_with_keys.insert(col);
        sample_block_with_columns_to_add.erase(column_name);

        key_columns[i] = sample_block_with_keys.getColumns().back().get();

        /// We will join only keys, where all components are not NULL.
        if (auto * nullable = checkAndGetColumn<ColumnNullable>(*key_columns[i]))
            key_columns[i] = &nullable->getNestedColumn();
    }

    return key_columns;
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

}
