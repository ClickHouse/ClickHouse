#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Common/typeid_cast.h>


#include <Common/PODArray.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** finalizeAggregation(agg_state) - get the result from the aggregation state.
* Takes state of aggregate function. Returns result of aggregation (finalized state).
*/
class FunctionEvalMLMethod : public IFunction
{
public:
    static constexpr auto name = "evalMLMethod";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionEvalMLMethod>(context);
    }
    explicit FunctionEvalMLMethod(ContextPtr context_) : context(context_)
    {}

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
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
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} requires at least one argument", getName());

        const auto * type = checkAndGetDataType<DataTypeAggregateFunction>(arguments[0].get());
        if (!type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Argument for function {} must have type AggregateFunction - state "
                            "of aggregate function.", getName());

        return type->getReturnTypeToPredict();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} requires at least one argument", getName());

        const auto * model = arguments[0].column.get();

        if (const auto * column_with_states = typeid_cast<const ColumnConst *>(model))
            model = column_with_states->getDataColumnPtr().get();

        const auto * agg_function = typeid_cast<const ColumnAggregateFunction *>(model);

        if (!agg_function)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}",
                            arguments[0].column->getName(), getName());

        return agg_function->predictValues(arguments, context);
    }

    ContextPtr context;
};

}

REGISTER_FUNCTION(EvalMLMethod)
{
    FunctionDocumentation::Description description = R"(
Applies a trained machine learning model to input features to generate predictions.
)";
    FunctionDocumentation::Syntax syntax = "evalMLMethod(model, x1[, x2, ...])";
    FunctionDocumentation::Arguments arguments = {
        {"model", "The trained machine learning model.", {"AggregateFunctionState"}},
        {"x1, x2, ...", "Feature values for prediction.", {"Float*", "(U)Int*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the predicted value based on the trained model.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Example usage",
        R"(
SELECT
evalMLMethod(model, trip_distance),
total_amount
FROM trips
LEFT JOIN models ON year = toYear(pickup_datetime)
LIMIT 5
        )",
        R"(
┌─evalMLMethod(model, trip_distance)─┬─total_amount─┐
│ 8.087692004204174                  │ 5.4          │
│ 7.861181608305352                  │ 4.6          │
│ 26.661544467907536                 │ 23.4         │
│ 8.767223191900637                  │ 5.8          │
│ 10.80581675499003                  │ 9            │
└────────────────────────────────────┴──────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::MachineLearning;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionEvalMLMethod>(documentation);
}

}
