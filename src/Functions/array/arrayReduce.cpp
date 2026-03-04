#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionState.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/Arena.h>

#include <Common/scope_guard_safe.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}


/** Applies an aggregate function to array and returns its result.
  * If aggregate function has multiple arguments, then this function can be applied to multiple arrays of the same size.
  *
  * arrayReduce('agg', arr1, ...) - apply the aggregate function `agg` to arrays `arr1...`
  *  If multiple arrays passed, then elements on corresponding positions are passed as multiple arguments to the aggregate function.
  */
class FunctionArrayReduce : public IFunction, private WithContext
{
public:
    static constexpr auto name = "arrayReduce";
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionArrayReduce>(context_); }
    explicit FunctionArrayReduce(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }
    /// As we parse the function name and deal with arrays we don't want to default NULL handler, which will hide
    /// nullability from us (which also means hidden from the aggregate functions)
    bool useDefaultImplementationForNulls() const override { return false; }
    /// Same for low cardinality. We want to return exactly what the aggregate function returns, no meddling
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

private:
    /// lazy initialization in getReturnTypeImpl
    /// TODO: init in OverloadResolver
    mutable AggregateFunctionPtr aggregate_function;
};


DataTypePtr FunctionArrayReduce::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    /// The first argument is a constant string with the name of the aggregate function
    ///  (possibly with parameters in parentheses, for example: "quantile(0.99)").

    if (arguments.size() < 2)
        throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
            "Number of arguments for function {} doesn't match: passed {}, should be at least 2.",
            getName(), arguments.size());

    const ColumnConst * aggregate_function_name_column = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
    if (!aggregate_function_name_column)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be constant string: "
            "name of aggregate function.", getName());

    DataTypes argument_types(arguments.size() - 1);
    for (size_t i = 1, size = arguments.size(); i < size; ++i)
    {
        const DataTypeArray * arg = checkAndGetDataType<DataTypeArray>(arguments[i].type.get());
        if (!arg)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Argument {} for function {} must be an array but it has type {}.",
                            i, getName(), arguments[i].type->getName());

        argument_types[i - 1] = arg->getNestedType();
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

        auto action = NullsAction::EMPTY;
        AggregateFunctionProperties properties;
        aggregate_function
            = AggregateFunctionFactory::instance().get(aggregate_function_name, action, argument_types, params_row, properties);
    }

    return aggregate_function->getResultType();
}


ColumnPtr FunctionArrayReduce::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
{
    const IAggregateFunction & agg_func = *aggregate_function;
    std::unique_ptr<Arena> arena = std::make_unique<Arena>();

    /// Aggregate functions do not support constant or lowcardinality columns. Therefore, we materialize them and
    /// keep a reference so they are alive until we finish using their nested columns (array data/offset)
    std::vector<ColumnPtr> materialized_columns;

    const size_t num_arguments_columns = arguments.size() - 1;

    std::vector<const IColumn *> aggregate_arguments_vec(num_arguments_columns);
    const ColumnArray::Offsets * offsets = nullptr;

    for (size_t i = 0; i < num_arguments_columns; ++i)
    {
        const IColumn * col = arguments[i + 1].column.get();
        auto col_no_lowcardinality = recursiveRemoveLowCardinality(arguments[i + 1].column);
        if (col_no_lowcardinality != arguments[i + 1].column)
        {
            materialized_columns.emplace_back(col_no_lowcardinality);
            col = col_no_lowcardinality.get();
        }

        const ColumnArray::Offsets * offsets_i = nullptr;
        if (const ColumnArray * arr = checkAndGetColumn<ColumnArray>(col))
        {
            aggregate_arguments_vec[i] = &arr->getData();
            offsets_i = &arr->getOffsets();
        }
        else if (const ColumnConst * const_arr = checkAndGetColumnConst<ColumnArray>(col))
        {
            materialized_columns.emplace_back(const_arr->convertToFullColumn());
            const auto & materialized_arr = typeid_cast<const ColumnArray &>(*materialized_columns.back());
            aggregate_arguments_vec[i] = &materialized_arr.getData();
            offsets_i = &materialized_arr.getOffsets();
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} as argument of function {}", col->getName(), getName());

        if (i == 0)
            offsets = offsets_i;
        else if (*offsets_i != *offsets)
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "Lengths of all arrays passed to {} must be equal.",
                getName());
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

        that->addBatchArray(0, input_rows_count, places.data(), 0, aggregate_arguments, offsets->data(), arena.get());
    }

    if (agg_func.isState())
    {
        for (size_t i = 0; i < input_rows_count; ++i)
            /// We should use insertMergeResultInto to insert result into ColumnAggregateFunction
            /// correctly if result contains AggregateFunction's states
            agg_func.insertMergeResultInto(places[i], res_col, arena.get());
    }
    else
    {
        for (size_t i = 0; i < input_rows_count; ++i)
            agg_func.insertResultInto(places[i], res_col, arena.get());
    }

    return result_holder;
}


REGISTER_FUNCTION(ArrayReduce)
{
    factory.registerFunction<FunctionArrayReduce>();
}

}
