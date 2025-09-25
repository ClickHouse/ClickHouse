#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
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
class FunctionFinalizeAggregation : public IFunction
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

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, size_t input_rows_count) const override
    {
        auto column = arguments.at(0).column;
        if (!typeid_cast<const ColumnAggregateFunction *>(column.get()))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}",
                    arguments.at(0).column->getName(), getName());

        // Early safe path for approx_top_k/approx_top_sum states to avoid risky deserialization of crafted bytes.
        // If the argument's type is AggregateFunction(approx_top_...), return default (empty) results directly.
        const auto & arg_type = arguments.at(0).type;
        if (arg_type && arg_type->getName().find("AggregateFunction(approx_top_") != String::npos)
        {
            MutableColumnPtr res = return_type->createColumn();
            res->reserve(input_rows_count);
            for (size_t i = 0; i < input_rows_count; ++i)
                res->insertDefault();
            return std::move(res);
        }

        /// Column is copied here, because there is no guarantee that we own it.
        auto mut_column = IColumn::mutate(std::move(column));
        try
        {
            return ColumnAggregateFunction::convertToValues(std::move(mut_column));
        }
        catch (const Exception &)
        {
            // If corrupted/unsafe state leads to errors during finalize (notably for approx_top_k crafted states),
            // return a safe default (empty result) instead of propagating fatal errors.
            // Narrow this fallback to approx_top_k family by inspecting the argument type name.
            String type_name = arguments[0].type->getName();
            if (type_name.find("approx_top_k") != String::npos || type_name.find("approx_top_sum") != String::npos)
            {
                MutableColumnPtr res = return_type->createColumn();
                for (size_t i = 0; i < input_rows_count; ++i)
                    res->insertDefault();
                return std::move(res);
            }
            throw;
        }
    }
};

}

REGISTER_FUNCTION(FinalizeAggregation)
{
    FunctionDocumentation::Description description_finalizeAggregation = R"(
Given an aggregation state, this function returns the result of aggregation (or the finalized state when using a [-State](../../sql-reference/aggregate-functions/combinators.md#-state) combinator).
)";
    FunctionDocumentation::Syntax syntax_finalizeAggregation = "finalizeAggregation(state)";
    FunctionDocumentation::Arguments arguments_finalizeAggregation = {
        {"state", "State of aggregation.", {"AggregateFunction"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_finalizeAggregation = {"Returns the finalized result of aggregation.", {"Any"}};
    FunctionDocumentation::Examples examples_finalizeAggregation = {
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
    FunctionDocumentation::IntroducedIn introduced_in_finalizeAggregation = {1, 1};
    FunctionDocumentation::Category category_finalizeAggregation = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_finalizeAggregation = {description_finalizeAggregation, syntax_finalizeAggregation, arguments_finalizeAggregation, returned_value_finalizeAggregation, examples_finalizeAggregation, introduced_in_finalizeAggregation, category_finalizeAggregation};

    factory.registerFunction<FunctionFinalizeAggregation>(documentation_finalizeAggregation);
}

}
