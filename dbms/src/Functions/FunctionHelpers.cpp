#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/WriteHelpers.h>
#include "FunctionsArithmetic.h"


namespace DB
{

const ColumnConst * checkAndGetColumnConstStringOrFixedString(const IColumn * column)
{
    if (!column->isColumnConst())
        return {};

    const ColumnConst * res = static_cast<const ColumnConst *>(column);

    if (checkColumn<ColumnString>(&res->getDataColumn())
        || checkColumn<ColumnFixedString>(&res->getDataColumn()))
        return res;

    return {};
}


Columns convertConstTupleToConstantElements(const ColumnConst & column)
{
    const ColumnTuple & src_tuple = static_cast<const ColumnTuple &>(column.getDataColumn());
    const Columns & src_tuple_columns = src_tuple.getColumns();
    size_t tuple_size = src_tuple_columns.size();
    size_t rows = column.size();

    Columns res(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i)
        res[i] = ColumnConst::create(src_tuple_columns[i], rows);

    return res;
}


Block createBlockWithNestedColumns(const Block & block, ColumnNumbers args)
{
    std::sort(args.begin(), args.end());

    Block res;
    size_t rows = block.rows();
    size_t columns = block.columns();

    size_t j = 0;
    for (size_t i = 0; i < columns; ++i)
    {
        const auto & col = block.getByPosition(i);
        bool is_inserted = false;

        if ((j < args.size()) && (i == args[j]))
        {
            ++j;

            if (col.type->isNullable())
            {
                const DataTypePtr & nested_type = static_cast<const DataTypeNullable &>(*col.type).getNestedType();

                if (!col.column)
                {
                    res.insert(i, {nullptr, nested_type, col.name});
                    is_inserted = true;
                }
                else if (col.column->isColumnNullable())
                {
                    const auto & nested_col = static_cast<const ColumnNullable &>(*col.column).getNestedColumnPtr();

                    res.insert(i, {nested_col, nested_type, col.name});
                    is_inserted = true;
                }
                else if (col.column->isColumnConst())
                {
                    const auto & nested_col = static_cast<const ColumnNullable &>(
                        static_cast<const ColumnConst &>(*col.column).getDataColumn()).getNestedColumnPtr();

                    res.insert(i, { ColumnConst::create(nested_col, rows), nested_type, col.name});
                    is_inserted = true;
                }
            }
        }

        if (!is_inserted)
            res.insert(i, col);
    }

    return res;
}


Block createBlockWithNestedColumns(const Block & block, ColumnNumbers args, size_t result)
{
    args.push_back(result);
    return createBlockWithNestedColumns(block, args);
}

}
