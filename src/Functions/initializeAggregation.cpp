#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionState.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>
#include <Common/Arena.h>

#include <Common/scope_guard_safe.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
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
    if (arguments.size() < 2)
        throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
            + toString(arguments.size()) + ", should be at least 2.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const ColumnConst * aggregate_function_name_column = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
    if (!aggregate_function_name_column)
        throw Exception("First argument for function " + getName() + " must be constant string: name of aggregate function.",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    DataTypes argument_types(arguments.size() - 1);
    for (size_t i = 1, size = arguments.size(); i < size; ++i)
    {
        argument_types[i - 1] = arguments[i].type;
    }

    if (!aggregate_function)
    {
        String aggregate_function_name_with_params = aggregate_function_name_column->getValue<String>();

        if (aggregate_function_name_with_params.empty())
            throw Exception("First argument for function " + getName() + " (name of aggregate function) cannot be empty.",
                ErrorCodes::BAD_ARGUMENTS);

        String aggregate_function_name;
        Array params_row;
        getAggregateFunctionNameAndParametersArray(aggregate_function_name_with_params,
                                                   aggregate_function_name, params_row, "function " + getName(), getContext());

        AggregateFunctionProperties properties;
        aggregate_function = AggregateFunctionFactory::instance().get(aggregate_function_name, argument_types, params_row, properties);
    }

    return aggregate_function->getReturnType();
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

    /// AggregateFunction's states should be inserted into column using specific way
    auto * res_col_aggregate_function = typeid_cast<ColumnAggregateFunction *>(&res_col);

    if (!res_col_aggregate_function && agg_func.isState())
        throw Exception("State function " + agg_func.getName() + " inserts results into non-state column "
                        + result_type->getName(), ErrorCodes::ILLEGAL_COLUMN);

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

    for (size_t i = 0; i < input_rows_count; ++i)
        if (!res_col_aggregate_function)
            agg_func.insertResultInto(places[i], res_col, arena.get());
        else
            res_col_aggregate_function->insertFrom(places[i]);
    return result_holder;
}

}

void registerFunctionInitializeAggregation(FunctionFactory & factory)
{
    factory.registerFunction<FunctionInitializeAggregation>();
}

}
