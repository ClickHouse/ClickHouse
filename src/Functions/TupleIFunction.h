#include <Functions/IFunction.h>

namespace DB
{
class TupleIFunction : public IFunction
{
public:
    Columns getTupleElements(const IColumn & column) const
    {
        if (const auto * const_column = typeid_cast<const ColumnConst *>(&column))
            return convertConstTupleToConstantElements(*const_column);

        if (const auto * column_tuple = typeid_cast<const ColumnTuple *>(&column))
        {
            Columns columns(column_tuple->tupleSize());
            for (size_t i = 0; i < columns.size(); ++i)
                columns[i] = column_tuple->getColumnPtr(i);
            return columns;
        }

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument of function {} should be tuples, got {}",
                        getName(), column.getName());
    }
};
}
