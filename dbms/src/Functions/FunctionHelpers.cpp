#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnTuple.h>


namespace DB
{

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
