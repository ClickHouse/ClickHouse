#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** finalizeAggregation(agg_state) - get the result from the aggregation state.
  * Takes state of aggregate function. Returns result of aggregation (finalized state).
  */
class FunctionFinalizeAggregation final : public IFunction
{
public:
    static constexpr auto name = "finalizeAggregation";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionFinalizeAggregation>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeAggregateFunction * type = checkAndGetDataType<DataTypeAggregateFunction>(arguments[0].get());
        if (!type)
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument for function '{}' must have type AggregateFunction - state of aggregate function."
                " Got '{}' instead", getName(), arguments[0]->getName());
        }

        return type->getReturnType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        auto column = arguments.at(0).column;
        const auto * column_aggregate_function = typeid_cast<const ColumnAggregateFunction *>(column.get());
        if (!column_aggregate_function)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}",
                    arguments.at(0).column->getName(), getName());

        /// The column can hold states of a *different* aggregate function that shares the same state
        /// representation (e.g. 'quantileState' vs 'quantilesState', combined via UNION or arrayReduce)
        /// but finalizes to a different type than declared. Report that rather than failing downstream.
        const auto & actual_result_type = column_aggregate_function->getAggregateFunction()->getResultType();
        if (!actual_result_type->equals(*result_type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Cannot finalize state of function '{}': the column holds a state finalizing to {}, "
                "but the declared result type of '{}' is {}. This happens when states of different "
                "aggregate functions that share a state representation are combined.",
                column_aggregate_function->getAggregateFunction()->getName(),
                actual_result_type->getName(), getName(), result_type->getName());

        /// Column is copied here, because there is no guarantee that we own it.
        auto mut_column = IColumn::mutate(std::move(column));
        return ColumnAggregateFunction::convertToValues(std::move(mut_column));
    }
};

}

REGISTER_FUNCTION(FinalizeAggregation)
{
    FunctionDocumentation::Description description = R"(
Given an aggregation state, this function returns the result of aggregation (or the finalized state when using a [-State](../../sql-reference/aggregate-functions/combinators.md#-state) combinator).
)";
    FunctionDocumentation::Syntax syntax = "finalizeAggregation(state)";
    FunctionDocumentation::Arguments arguments = {
        {"state", "State of aggregation.", {"AggregateFunction"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the finalized result of aggregation.", {"Any"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT finalizeAggregation(arrayReduce('maxState', [1, 2, 3]));
        )",
        R"(
┌─finalizeAggregation(arrayReduce('maxState', [1, 2, 3]))─┐
│                                                       3 │
└─────────────────────────────────────────────────────────┘
        )"
    },
    {
        "Combined with initializeAggregation",
        R"(
SET allow_deprecated_error_prone_window_functions = 1;
WITH initializeAggregation('sumState', number) AS one_row_sum_state
SELECT
    number,
    finalizeAggregation(one_row_sum_state) AS one_row_sum,
    runningAccumulate(one_row_sum_state) AS cumulative_sum
FROM numbers(5);
        )",
        R"(
┌─number─┬─one_row_sum─┬─cumulative_sum─┐
│      0 │           0 │              0 │
│      1 │           1 │              1 │
│      2 │           2 │              3 │
│      3 │           3 │              6 │
│      4 │           4 │             10 │
└────────┴─────────────┴────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionFinalizeAggregation>(documentation);
}

}
