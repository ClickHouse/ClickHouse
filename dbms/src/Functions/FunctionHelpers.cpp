#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNullable.h>


namespace DB
{

const ColumnConst * checkAndGetColumnConstStringOrFixedString(const IColumn * column)
{
    if (!column->isConst())
        return {};

    const ColumnConst * res = static_cast<const ColumnConst *>(column);

    if (checkColumn<ColumnString>(&res->getDataColumn())
        || checkColumn<ColumnFixedString>(&res->getDataColumn()))
        return res;

    return {};
}


ColumnPtr convertConstTupleToTupleOfConstants(const ColumnConst & column)
{
    Block res;
    const ColumnTuple & src_tuple = static_cast<const ColumnTuple &>(column.getDataColumn());
    const Block & src_tuple_block = src_tuple.getData();
    size_t rows = src_tuple_block.rows();

    for (size_t i = 0, size = src_tuple_block.columns(); i < size; ++i)
        res.insert({
            std::make_shared<ColumnConst>(src_tuple_block.getByPosition(i).column, rows),
            src_tuple_block.getByPosition(i).type,
            src_tuple_block.getByPosition(i).name});

    return std::make_shared<ColumnTuple>(res);
}


Block createBlockWithNestedColumns(const Block & block, ColumnNumbers args)
{
    std::sort(args.begin(), args.end());

    Block res;

    size_t j = 0;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        const auto & col = block.getByPosition(i);
        bool is_inserted = false;

        if ((j < args.size()) && (i == args[j]))
        {
            ++j;

            if (col.column->isNullable())
            {
                auto nullable_col = static_cast<const ColumnNullable *>(col.column.get());
                const ColumnPtr & nested_col = nullable_col->getNestedColumn();

                auto nullable_type = static_cast<const DataTypeNullable *>(col.type.get());
                const DataTypePtr & nested_type = nullable_type->getNestedType();

                res.insert(i, {nested_col, nested_type, col.name});

                is_inserted = true;
            }
        }

        if (!is_inserted)
            res.insert(i, col);
    }

    return res;
}


Block createBlockWithNestedColumns(const Block & block, ColumnNumbers args, size_t result)
{
    std::sort(args.begin(), args.end());

    Block res;

    size_t j = 0;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        const auto & col = block.getByPosition(i);
        bool is_inserted = false;

        if ((j < args.size()) && (i == args[j]))
        {
            ++j;

            if (col.column->isNullable())
            {
                auto nullable_col = static_cast<const ColumnNullable *>(col.column.get());
                const ColumnPtr & nested_col = nullable_col->getNestedColumn();

                auto nullable_type = static_cast<const DataTypeNullable *>(col.type.get());
                const DataTypePtr & nested_type = nullable_type->getNestedType();

                res.insert(i, {nested_col, nested_type, col.name});

                is_inserted = true;
            }
        }
        else if (i == result)
        {
            if (col.type->isNullable())
            {
                auto nullable_type = static_cast<const DataTypeNullable *>(col.type.get());
                const DataTypePtr & nested_type = nullable_type->getNestedType();

                res.insert(i, {nullptr, nested_type, col.name});
                is_inserted = true;
            }
        }

        if (!is_inserted)
            res.insert(i, col);
    }

    return res;
}

}
