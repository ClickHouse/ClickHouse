#include <Core/ColumnsWithTypeAndName.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Common/FunctionDocumentation.h>
#include <Common/typeid_cast.h>


#include <Common/PODArray.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

bool isAggregateFunctionState(const IDataType & type)
{
    return typeid_cast<const DataTypeAggregateFunction *>(&type) != nullptr;
}

}

namespace
{

/** finalizeAggregation(agg_state) - get the result from the aggregation state.
* Takes state of aggregate function. Returns result of aggregation (finalized state).
*/
class FunctionEvalMLMethod final : public IFunction
{
public:
    static constexpr auto name = "evalMLMethod";
    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionEvalMLMethod>(context_);
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

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"model", &isAggregateFunctionState, nullptr, "AggregateFunctionState"}
        };
        FunctionArgumentDescriptor optional_args{
            "xi", &isNumber, nullptr, "Float* or (U)Int*"
        };

        validateFunctionArgumentsWithVariadics(*this, arguments, mandatory_args, optional_args);

        const auto* agg_function = static_cast<const DataTypeAggregateFunction *>(arguments[0].type.get());
        return agg_function->getReturnTypeToPredict();
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

private:
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
CREATE TABLE trips (pickup_datetime DateTime('UTC'), trip_distance Float64, total_amount Float64) ENGINE = Memory;

-- A fare of 3, plus 2.5 for every unit of distance.
INSERT INTO trips
SELECT toDateTime('2020-01-01 00:00:00', 'UTC') + number * 60, number % 10 + 1, 2.5 * (number % 10 + 1) + 3
FROM numbers(1000);

-- One model per year of the data.
CREATE TABLE models ENGINE = Memory AS
SELECT
    toYear(pickup_datetime) AS year,
    stochasticLinearRegressionState(0.01, 0.0, 10, 'SGD')(total_amount, trip_distance) AS model
FROM trips
GROUP BY year;

SELECT
    trip_distance,
    round(evalMLMethod(model, trip_distance), 2) AS predicted,
    total_amount
FROM trips
LEFT JOIN models ON year = toYear(pickup_datetime)
ORDER BY pickup_datetime
LIMIT 5
        )",
        R"(
┌─trip_distance─┬─predicted─┬─total_amount─┐
│             1 │      4.05 │          5.5 │
│             2 │      6.79 │            8 │
│             3 │      9.53 │         10.5 │
│             4 │     12.28 │           13 │
│             5 │     15.02 │         15.5 │
└───────────────┴───────────┴──────────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::MachineLearning;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionEvalMLMethod>(documentation);
}

}
