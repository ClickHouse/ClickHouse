#include <Formats/insertNullAsDefaultIfNeeded.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{

void insertNullAsDefaultIfNeeded(ColumnWithTypeAndName & input_column, const ColumnWithTypeAndName & header_column, size_t column_i, BlockMissingValues * block_missing_values)
{
    if (!isNullableOrLowCardinalityNullable(input_column.type) || isNullableOrLowCardinalityNullable(header_column.type))
        return;

    if (block_missing_values)
    {
        for (size_t i = 0; i < input_column.column->size(); ++i)
        {
            if (input_column.column->isNullAt(i))
                block_missing_values->setBit(column_i, i);
        }
    }

    if (input_column.type->isNullable())
    {
        input_column.column = assert_cast<const ColumnNullable *>(input_column.column.get())->getNestedColumnWithDefaultOnNull();
        input_column.type = removeNullable(input_column.type);
    }
    else
    {
        input_column.column = assert_cast<const ColumnLowCardinality *>(input_column.column.get())->cloneWithDefaultOnNull();
        const auto * lc_type = assert_cast<const DataTypeLowCardinality *>(input_column.type.get());
        input_column.type = std::make_shared<DataTypeLowCardinality>(removeNullable(lc_type->getDictionaryType()));
    }
}

}
