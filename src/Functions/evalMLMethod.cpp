#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Common/typeid_cast.h>

#include <iostream>

#include <Common/PODArray.h>

namespace DB
{

    namespace ErrorCodes
    {
        extern const int BAD_ARGUMENTS;
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
    explicit FunctionEvalMLMethod(const Context & context_) : context(context_)
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
        if (arguments.empty())
            throw Exception("Function " + getName() + " requires at least one argument", ErrorCodes::BAD_ARGUMENTS);

        const auto * type = checkAndGetDataType<DataTypeAggregateFunction>(arguments[0].get());
        if (!type)
            throw Exception("Argument for function " + getName() + " must have type AggregateFunction - state of aggregate function.",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return type->getReturnTypeToPredict();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
    {
        if (arguments.empty())
            throw Exception("Function " + getName() + " requires at least one argument", ErrorCodes::BAD_ARGUMENTS);

        const auto * model = block.getByPosition(arguments[0]).column.get();

        if (const auto * column_with_states = typeid_cast<const ColumnConst *>(model))
            model = column_with_states->getDataColumnPtr().get();

        const auto * agg_function = typeid_cast<const ColumnAggregateFunction *>(model);

        if (!agg_function)
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                            + " of first argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

        block.getByPosition(result).column = agg_function->predictValues(block, arguments, context);
    }

    const Context & context;
};

void registerFunctionEvalMLMethod(FunctionFactory & factory)
{
    factory.registerFunction<FunctionEvalMLMethod>();
}

}
