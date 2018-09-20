#include <DataStreams/AddingDefaultBlockOutputStream.h>

#include <Common/typeid_cast.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Interpreters/evaluateMissingDefaults.h>
#include <Core/Block.h>


namespace DB
{

void AddingDefaultBlockOutputStream::write(const Block & block)
{
    Block res;
    /// We take given columns from input block
    /// and missed columns without default value (default and meterialized will be computed later)
    for (const auto & column : output_block)
    {
        if (block.has(column.name))
            res.insert(block.getByName(column.name));
        else if (!column_defaults.count(column.name))
            res.insert(column);
    }

    /// Adds not specified default values.
    size_t rows = block.rows();

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

    /// In this loop we fill missed columns
    for (auto & column : res)
    {
        if (block.has(column.name))
            continue;

        String offsets_name = Nested::extractTableName(column.name);
        if (offset_columns.count(offsets_name))
        {
            ColumnPtr offsets_column = offset_columns[offsets_name];
            DataTypePtr nested_type = typeid_cast<const DataTypeArray &>(*column.type).getNestedType();
            UInt64 nested_rows = rows ? get<UInt64>((*offsets_column)[rows - 1]) : 0;

            ColumnPtr nested_column = nested_type->createColumnConstWithDefaultValue(nested_rows)->convertToFullColumnIfConst();
            column.column = ColumnArray::create(nested_column, offsets_column);
        }
        else
        {
            /** It is necessary to turn a constant column into a full column, since in part of blocks (from other parts),
            *  it can be full (or the interpreter may decide that it is constant everywhere).
            */
            column.column = column.type->createColumnConstWithDefaultValue(rows)->convertToFullColumnIfConst();
        }
    }

    /// Computes explicitly specified values (in column_defaults) by default and materialized columns.
    evaluateMissingDefaults(res, output_block.getNamesAndTypesList(), column_defaults, context);

    output->write(res);
}

void AddingDefaultBlockOutputStream::flush()
{
    output->flush();
}

void AddingDefaultBlockOutputStream::writePrefix()
{
    output->writePrefix();
}

void AddingDefaultBlockOutputStream::writeSuffix()
{
    output->writeSuffix();
}

}
