#include <DataStreams/AddingDefaultBlockOutputStream.h>

#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Core/Block.h>


namespace DB
{

void AddingDefaultBlockOutputStream::write(const DB::Block & block)
{
    Block res = block;

    /// Computes explicitly specified values (in column_defaults) by default.
    /** @todo if somehow block does not contain values for implicitly-defaulted columns that are prerequisites
         *    for explicitly-defaulted ones, exception will be thrown during evaluating such columns
         *    (implicitly-defaulted columns are evaluated on the line after following one. */
    evaluateMissingDefaults(res, *required_columns, column_defaults, context);

    /// Adds not specified default values.
    /// @todo this may be moved before `evaluateMissingDefaults` with passing {required_columns - explicitly-defaulted columns}
    if (!only_explicit_column_defaults)
    {
        size_t rows = res.rows();

        /// For missing columns of nested structure, you need to create not a column of empty arrays, but a column of arrays of correct lengths.
        /// First, remember the offset columns for all arrays in the block.
        std::map<String, ColumnPtr> offset_columns;

        for (size_t i = 0, size = res.columns(); i < size; ++i)
        {
            const auto & elem = res.getByPosition(i);

            if (const ColumnArray * array = typeid_cast<const ColumnArray *>(&*elem.column))
            {
                String offsets_name = DataTypeNested::extractNestedTableName(elem.name);
                auto & offsets_column = offset_columns[offsets_name];

                /// If for some reason there are different offset columns for one nested structure, then we take nonempty.
                if (!offsets_column || offsets_column->empty())
                    offsets_column = array->getOffsetsPtr();
            }
        }

        for (const auto & requested_column : *required_columns)
        {
            if (res.has(requested_column.name))
                continue;

            ColumnWithTypeAndName column_to_add;
            column_to_add.name = requested_column.name;
            column_to_add.type = requested_column.type;

            String offsets_name = DataTypeNested::extractNestedTableName(column_to_add.name);
            if (offset_columns.count(offsets_name))
            {
                ColumnPtr offsets_column = offset_columns[offsets_name];
                DataTypePtr nested_type = typeid_cast<const DataTypeArray &>(*column_to_add.type).getNestedType();
                UInt64 nested_rows = rows ? get<UInt64>((*offsets_column)[rows - 1]) : 0;

                ColumnPtr nested_column = nested_type->createColumnConstWithDefaultValue(nested_rows)->convertToFullColumnIfConst();
                column_to_add.column = ColumnArray::create(nested_column, offsets_column);
            }
            else
            {
                /** It is necessary to turn a constant column into a full column, since in part of blocks (from other parts),
                *  it can be full (or the interpreter may decide that it is constant everywhere).
                */
                column_to_add.column = column_to_add.type->createColumnConstWithDefaultValue(rows)->convertToFullColumnIfConst();
            }

            res.insert(std::move(column_to_add));
        }
    }

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
