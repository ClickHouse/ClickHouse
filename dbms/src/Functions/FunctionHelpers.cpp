#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>


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

}
