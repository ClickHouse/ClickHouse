#include <Interpreters/addMissingDefaults.h>

#include <Common/typeid_cast.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Core/Block.h>
#include <Storages/ColumnDefault.h>


namespace DB
{

Block addMissingDefaults(const Block & block,
                         const NamesAndTypesList & required_columns,
                         const ColumnDefaults & column_defaults,
                         const Context & context)
{
    /// For missing columns of nested structure, you need to create not a column of empty arrays, but a column of arrays of correct lengths.
    /// First, remember the offset columns for all arrays in the block.
    std::map<String, ColumnPtr> offset_columns;

    for (size_t i = 0, size = block.columns(); i < size; ++i)
    {
        const auto & elem = block.getByPosition(i);

        if (const ColumnArray * array = typeid_cast<const ColumnArray *>(&*elem.column))
        {
            String offsets_name = Nested::extractTableName(elem.name);
            auto & offsets_column = offset_columns[offsets_name];

            /// If for some reason there are different offset columns for one nested structure, then we take nonempty.
            if (!offsets_column || offsets_column->empty())
                offsets_column = array->getOffsetsPtr();
        }
    }

    const size_t rows = block.rows();
    Block res;

    /// We take given columns from input block and missed columns without default value
    /// (default and materialized will be computed later).
    for (const auto & column : required_columns)
    {
        if (block.has(column.name))
        {
            res.insert(block.getByName(column.name));
            continue;
        }

        if (column_defaults.count(column.name))
            continue;

        String offsets_name = Nested::extractTableName(column.name);
        if (offset_columns.count(offsets_name))
        {
            ColumnPtr offsets_column = offset_columns[offsets_name];
            DataTypePtr nested_type = typeid_cast<const DataTypeArray &>(*column.type).getNestedType();
            UInt64 nested_rows = rows ? get<UInt64>((*offsets_column)[rows - 1]) : 0;

            ColumnPtr nested_column = nested_type->createColumnConstWithDefaultValue(nested_rows)->convertToFullColumnIfConst();
            auto new_column = ColumnArray::create(nested_column, offsets_column);
            res.insert(ColumnWithTypeAndName(std::move(new_column), column.type, column.name));
            continue;
        }

        /** It is necessary to turn a constant column into a full column, since in part of blocks (from other parts),
        *  it can be full (or the interpreter may decide that it is constant everywhere).
        */
        auto new_column = column.type->createColumnConstWithDefaultValue(rows)->convertToFullColumnIfConst();
        res.insert(ColumnWithTypeAndName(std::move(new_column), column.type, column.name));
    }

    /// Computes explicitly specified values (in column_defaults) by default and materialized columns.
    evaluateMissingDefaults(res, required_columns, column_defaults, context);
    return res;
}

}
