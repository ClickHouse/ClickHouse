#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/IColumn.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionState.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>
#include <Common/Arena.h>

#include <Common/scope_guard_safe.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

class FunctionInitializeAggregation : public IFunction, private WithContext
{
public:
    static constexpr auto name = "initializeAggregation";
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionInitializeAggregation>(context_); }
    explicit FunctionInitializeAggregation(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

private:
    /// TODO Rewrite with FunctionBuilder.
    mutable AggregateFunctionPtr aggregate_function;
};


DataTypePtr FunctionInitializeAggregation::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    FunctionArgumentDescriptors mandatory_args{
        {"aggregate_function_name", &isString, &isColumnConst, "const String"},
        {"argument", nullptr, nullptr, "Any"}
    };
    FunctionArgumentDescriptor variadic_args{"argument", nullptr, nullptr, "Any"};
    validateFunctionArgumentsWithVariadics(*this, arguments, mandatory_args, variadic_args);

    const ColumnConst * aggregate_function_name_column = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
    if (!aggregate_function_name_column)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be constant string: "
            "name of aggregate function.", getName());

    DataTypes argument_types(arguments.size() - 1);
    for (size_t i = 1, size = arguments.size(); i < size; ++i)
    {
        argument_types[i - 1] = arguments[i].type;
    }

    if (!aggregate_function)
    {
        String aggregate_function_name_with_params = aggregate_function_name_column->getValue<String>();

        if (aggregate_function_name_with_params.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "First argument for function {} (name of aggregate function) cannot be empty.", getName());

        String aggregate_function_name;
        Array params_row;
        getAggregateFunctionNameAndParametersArray(aggregate_function_name_with_params,
                                                   aggregate_function_name, params_row, "function " + getName(), getContext());

        auto action = NullsAction::EMPTY; /// It is already embedded in the function name itself
        AggregateFunctionProperties properties;
        aggregate_function
            = AggregateFunctionFactory::instance().get(aggregate_function_name, action, argument_types, params_row, properties);
    }

    return aggregate_function->getResultType();
}


ColumnPtr FunctionInitializeAggregation::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
{
    const IAggregateFunction & agg_func = *aggregate_function;
    std::unique_ptr<Arena> arena = std::make_unique<Arena>();

    const size_t num_arguments_columns = arguments.size() - 1;

    std::vector<ColumnPtr> materialized_columns(num_arguments_columns);
    std::vector<const IColumn *> aggregate_arguments_vec(num_arguments_columns);

    for (size_t i = 0; i < num_arguments_columns; ++i)
    {
        const IColumn * col = arguments[i + 1].column.get();
        materialized_columns.emplace_back(col->convertToFullColumnIfConst());
        aggregate_arguments_vec[i] = &(*materialized_columns.back());
    }

    const IColumn ** aggregate_arguments = aggregate_arguments_vec.data();

    MutableColumnPtr result_holder = result_type->createColumn();
    IColumn & res_col = *result_holder;

    PODArray<AggregateDataPtr> places(input_rows_count);
    for (size_t i = 0; i < input_rows_count; ++i)
    {
        places[i] = arena->alignedAlloc(agg_func.sizeOfData(), agg_func.alignOfData());
        try
        {
            agg_func.create(places[i]);
        }
        catch (...)
        {
            for (size_t j = 0; j < i; ++j)
                agg_func.destroy(places[j]);
            throw;
        }
    }

    SCOPE_EXIT_MEMORY_SAFE({
        for (size_t i = 0; i < input_rows_count; ++i)
            agg_func.destroy(places[i]);
    });

    {
        const auto * that = &agg_func;
        /// Unnest consecutive trailing -State combinators
        while (const auto * func = typeid_cast<const AggregateFunctionState *>(that))
            that = func->getNestedFunction().get();
        that->addBatch(0, input_rows_count, places.data(), 0, aggregate_arguments, arena.get());
    }

    if (agg_func.isState())
    {
        /// We should use insertMergeResultInto to insert result into ColumnAggregateFunction
        /// correctly if result contains AggregateFunction's states
        for (size_t i = 0; i < input_rows_count; ++i)
            agg_func.insertMergeResultInto(places[i], res_col, arena.get());
    }
    else
    {
        for (size_t i = 0; i < input_rows_count; ++i)
            agg_func.insertResultInto(places[i], res_col, arena.get());
    }

    return result_holder;
}

}

REGISTER_FUNCTION(InitializeAggregation)
{
    FunctionDocumentation::Description description = R"(
Calculates the result of an aggregate function based on a single value.
This function can be used to initialize aggregate functions with combinator [-State](../../sql-reference/aggregate-functions/combinators.md#-state).
You can create states of aggregate functions and insert them to columns of type [`AggregateFunction`](../../sql-reference/data-types/aggregatefunction.md) or use initialized aggregates as default values.
    )";
    FunctionDocumentation::Syntax syntax = "initializeAggregation(aggregate_function, arg1[, arg2, ...])";
    FunctionDocumentation::Arguments arguments = {
        {"aggregate_function", "Name of the aggregation function to initialize.", {"String"}},
        {"arg1[, arg2, ...]", "Arguments of the aggregate function.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the result of aggregation for every row passed to the function. The return type is the same as the return type of the function that `initializeAggregation` takes as a first argument.", {"Any"}};
    FunctionDocumentation::Examples examples = {
    {
        "Basic usage with uniqState",
        R"(
SELECT uniqMerge(state) FROM (SELECT initializeAggregation('uniqState', number % 3) AS state FROM numbers(10000));
        )",
        R"(
┌─uniqMerge(state)─┐
│                3 │
└──────────────────┘
        )"
    },
    {
        "Usage with sumState and finalizeAggregation",
        R"(
SELECT finalizeAggregation(state), toTypeName(state) FROM (SELECT initializeAggregation('sumState', number % 3) AS state FROM numbers(5));
        )",
        R"(
┌─finalizeAggregation(state)─┬─toTypeName(state)─────────────┐
│                          0 │ AggregateFunction(sum, UInt8) │
│                          1 │ AggregateFunction(sum, UInt8) │
│                          2 │ AggregateFunction(sum, UInt8) │
│                          0 │ AggregateFunction(sum, UInt8) │
│                          1 │ AggregateFunction(sum, UInt8) │
└────────────────────────────┴───────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionInitializeAggregation>(documentation);
}

}
