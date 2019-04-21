#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Common/typeid_cast.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <iostream>

#include <Common/PODArray.h>
#include <Columns/ColumnArray.h>

namespace DB
{

    namespace ErrorCodes
    {
        extern const int ILLEGAL_COLUMN;
        extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    }


/** finalizeAggregation(agg_state) - get the result from the aggregation state.
* Takes state of aggregate function. Returns result of aggregation (finalized state).
*/
class FunctionEvalMLMethod : public IFunction
{
public:
    static constexpr auto name = "evalMLMethod";
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionEvalMLMethod>(context);
    }
    FunctionEvalMLMethod(const Context & context) : context(context)
    {}

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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments.size())
            throw Exception("Function " + getName() + " requires at least one argument", ErrorCodes::BAD_ARGUMENTS);

        const DataTypeAggregateFunction * type = checkAndGetDataType<DataTypeAggregateFunction>(arguments[0].get());
        if (!type)
            throw Exception("Argument for function " + getName() + " must have type AggregateFunction - state of aggregate function.",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return type->getReturnType();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        if (!arguments.size())
            throw Exception("Function " + getName() + " requires at least one argument", ErrorCodes::BAD_ARGUMENTS);

        const ColumnConst * column_with_states
                = typeid_cast<const ColumnConst *>(&*block.getByPosition(arguments[0]).column);


        if (!column_with_states)
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                            + " of first argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

        block.getByPosition(result).column =
                typeid_cast<const ColumnAggregateFunction *>(&*column_with_states->getDataColumnPtr())->predictValues(block, arguments, context);
    }

    const Context & context;
};

void registerFunctionEvalMLMethod(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEvalMLMethod>();
}

}
