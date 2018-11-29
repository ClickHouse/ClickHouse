#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnTuple.h>
#include <memory>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/** tuple(x, y, ...) is a function that allows you to group several columns
  * tupleElement(tuple, n) is a function that allows you to retrieve a column from tuple.
  */

class FunctionTuple : public IFunction
{
public:
    static constexpr auto name = "tuple";

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionTuple>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isInjective(const Block &) override
    {
        return true;
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 1)
            throw Exception("Function " + getName() + " requires at least one argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<DataTypeTuple>(arguments);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        size_t tuple_size = arguments.size();
        Columns tuple_columns(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
        {
            tuple_columns[i] = block.getByPosition(arguments[i]).column;

            /** If tuple is mixed of constant and not constant columns,
            *  convert all to non-constant columns,
            *  because many places in code expect all non-constant columns in non-constant tuple.
            */
            if (ColumnPtr converted = tuple_columns[i]->convertToFullColumnIfConst())
                tuple_columns[i] = converted;
        }
        block.getByPosition(result).column = ColumnTuple::create(tuple_columns);
    }
};

void registerFunctionTuple(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTuple>();
}

}
