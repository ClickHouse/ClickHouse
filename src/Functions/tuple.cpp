#include <Functions/tuple.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

DataTypePtr FunctionTuple::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least one argument.", getName());

    return std::make_shared<DataTypeTuple>(arguments);
}

ColumnPtr FunctionTuple::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const
{
    size_t tuple_size = arguments.size();
    Columns tuple_columns(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i)
    {
        /** If tuple is mixed of constant and not constant columns,
            *  convert all to non-constant columns,
            *  because many places in code expect all non-constant columns in non-constant tuple.
            */
        tuple_columns[i] = arguments[i].column->convertToFullColumnIfConst();
    }
    return ColumnTuple::create(tuple_columns);
}


REGISTER_FUNCTION(Tuple)
{
    factory.registerFunction<FunctionTuple>();
}

}
