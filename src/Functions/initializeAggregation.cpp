#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionState.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>
#include <Common/Arena.h>

#include <ext/scope_guard.h>


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

class FunctionInitializeAggregation : public IFunction
{
public:
    static constexpr auto name = "initializeAggregation";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionInitializeAggregation>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;

    void executeImpl(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override;

private:
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
                                                   aggregate_function_name, params_row, "function " + getName());

        AggregateFunctionProperties properties;
        aggregate_function = AggregateFunctionFactory::instance().get(aggregate_function_name, argument_types, params_row, properties);
    }

    return aggregate_function->getReturnType();
}


void FunctionInitializeAggregation::executeImpl(ColumnsWithTypeAndName & columns, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const
{
    IAggregateFunction & agg_func = *aggregate_function;
    std::unique_ptr<Arena> arena = std::make_unique<Arena>();

    const size_t num_arguments_columns = arguments.size() - 1;

    std::vector<ColumnPtr> materialized_columns(num_arguments_columns);
    std::vector<const IColumn *> aggregate_arguments_vec(num_arguments_columns);

    for (size_t i = 0; i < num_arguments_columns; ++i)
    {
        const IColumn * col = columns[arguments[i + 1]].column.get();
        materialized_columns.emplace_back(col->convertToFullColumnIfConst());
        aggregate_arguments_vec[i] = &(*materialized_columns.back());
    }

    const IColumn ** aggregate_arguments = aggregate_arguments_vec.data();

    MutableColumnPtr result_holder = columns[result].type->createColumn();
    IColumn & res_col = *result_holder;

    /// AggregateFunction's states should be inserted into column using specific way
    auto * res_col_aggregate_function = typeid_cast<ColumnAggregateFunction *>(&res_col);

    if (!res_col_aggregate_function && agg_func.isState())
        throw Exception("State function " + agg_func.getName() + " inserts results into non-state column "
                        + columns[result].type->getName(), ErrorCodes::ILLEGAL_COLUMN);

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

    SCOPE_EXIT({
        for (size_t i = 0; i < input_rows_count; ++i)
            agg_func.destroy(places[i]);
    });

    {
        auto * that = &agg_func;
        /// Unnest consecutive trailing -State combinators
        while (auto * func = typeid_cast<AggregateFunctionState *>(that))
            that = func->getNestedFunction().get();
        that->addBatch(input_rows_count, places.data(), 0, aggregate_arguments, arena.get());
    }

    for (size_t i = 0; i < input_rows_count; ++i)
        if (!res_col_aggregate_function)
            agg_func.insertResultInto(places[i], res_col, arena.get());
        else
            res_col_aggregate_function->insertFrom(places[i]);
    columns[result].column = std::move(result_holder);
}

}

void registerFunctionInitializeAggregation(FunctionFactory & factory)
{
    factory.registerFunction<FunctionInitializeAggregation>();
}

}
