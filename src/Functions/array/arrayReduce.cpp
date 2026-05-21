#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionState.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Common/Arena.h>
#include <Common/assert_cast.h>

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
    extern const int LOGICAL_ERROR;
}


/** Applies an aggregate function to array and returns its result.
  * If aggregate function has multiple arguments, then this function can be applied to multiple arrays of the same size.
  *
  * arrayReduce('agg', arr1, ...) - apply the aggregate function `agg` to arrays `arr1...`
  *  If multiple arrays passed, then elements on corresponding positions are passed as multiple arguments to the aggregate function.
  */
class FunctionArrayReduce : public IFunction
{
public:
    static constexpr auto name = "arrayReduce";

    explicit FunctionArrayReduce(AggregateFunctionPtr aggregate_function_)
        : aggregate_function(std::move(aggregate_function_)) {}

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

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto result = aggregate_function->getResultType();
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            if (arguments[i].type->isNullable())
                return makeNullable(result);
        }
        return result;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

private:
    AggregateFunctionPtr aggregate_function;
};

namespace
{

ColumnPtr materializeNullMapToRowCount(const ColumnPtr & null_map_column, size_t num_rows)
{
    const auto & null_map = assert_cast<const ColumnUInt8 &>(*null_map_column);
    if (null_map.size() == num_rows)
        return null_map_column;

    if (null_map.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Null map size {} does not match row count {} and is not a single constant value",
            null_map.size(), num_rows);

    auto result = ColumnUInt8::create();
    result->getData().resize_fill(num_rows, null_map.getData()[0]);
    return result;
}

void mergeRowNullMap(ColumnPtr & accumulated, const ColumnPtr & new_null_map, size_t num_rows)
{
    ColumnPtr materialized_new = materializeNullMapToRowCount(new_null_map, num_rows);

    if (!accumulated)
    {
        accumulated = std::move(materialized_new);
        return;
    }

    accumulated = materializeNullMapToRowCount(accumulated, num_rows);
    auto mutable_accumulated = IColumn::mutate(accumulated);
    auto & acc_data = assert_cast<ColumnUInt8 &>(*mutable_accumulated).getData();
    const auto & new_data = assert_cast<const ColumnUInt8 &>(*materialized_new).getData();

    for (size_t i = 0; i < num_rows; ++i)
        acc_data[i] |= new_data[i];

    accumulated = std::move(mutable_accumulated);
}

ColumnPtr wrapNullableArrayReduceResult(ColumnPtr result, const ColumnPtr & array_null_map, size_t num_rows)
{
    ColumnPtr null_map = array_null_map;
    if (!null_map)
    {
        auto null_map_mut = ColumnUInt8::create();
        null_map_mut->getData().resize_fill(num_rows, 0);
        null_map = std::move(null_map_mut);
    }
    else
        null_map = materializeNullMapToRowCount(null_map, num_rows);

    ColumnPtr nested_result = result;
    if (const auto * nullable_result = checkAndGetColumn<ColumnNullable>(result.get()))
    {
        mergeRowNullMap(null_map, nullable_result->getNullMapColumnPtr(), num_rows);
        nested_result = nullable_result->getNestedColumnPtr();
    }

    return ColumnNullable::create(nested_result, std::move(null_map));
}

ColumnPtr unwrapNullableArrayColumn(
    const ColumnPtr & column,
    const DataTypePtr & type,
    std::vector<ColumnPtr> & materialized_columns,
    ColumnPtr & out_null_map,
    size_t num_rows)
{
    ColumnPtr unwrapped_column = column;
    auto col_no_lowcardinality = recursiveRemoveLowCardinality(unwrapped_column);
    if (col_no_lowcardinality != unwrapped_column)
    {
        materialized_columns.emplace_back(col_no_lowcardinality);
        unwrapped_column = col_no_lowcardinality;
    }

    const ColumnConst * col_const = checkAndGetColumnConst<ColumnConst>(unwrapped_column.get());
    const IColumn * data_column = col_const ? &col_const->getDataColumn() : unwrapped_column.get();

    if (const auto * nullable_array_column = checkAndGetColumn<ColumnNullable>(data_column))
    {
        mergeRowNullMap(out_null_map, nullable_array_column->getNullMapColumnPtr(), num_rows);

        if (checkAndGetColumn<ColumnArray>(&nullable_array_column->getNestedColumn()))
        {
            if (col_const)
                return ColumnConst::create(nullable_array_column->getNestedColumnPtr(), col_const->size());
            return nullable_array_column->getNestedColumnPtr();
        }
    }
    else if (type->isNullable())
    {
        if (!out_null_map)
        {
            auto null_map = ColumnUInt8::create();
            null_map->getData().resize_fill(unwrapped_column->size(), 0);
            out_null_map = std::move(null_map);
        }
    }

    return unwrapped_column;
}

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
    ColumnPtr array_null_map;

    for (size_t i = 0; i < num_arguments_columns; ++i)
    {
        ColumnPtr col_holder = unwrapNullableArrayColumn(
            arguments[i + 1].column, arguments[i + 1].type, materialized_columns, array_null_map, input_rows_count);
        const IColumn * col = col_holder.get();

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

    /// Use the aggregate function's own result type for the destination column (*OrNull combinators
    /// require ColumnNullable). Outer nullability from Nullable(Array) arguments is applied below.
    const auto & aggregate_result_type = aggregate_function->getResultType();
    MutableColumnPtr result_holder = aggregate_result_type->createColumn();
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

    if (result_type->isNullable())
        return wrapNullableArrayReduceResult(std::move(result_holder), array_null_map, input_rows_count);

    return result_holder;
}


namespace
{

class FunctionArrayReduceOverloadResolver : public IFunctionOverloadResolver, private WithContext
{
public:
    static constexpr auto name = "arrayReduce";
    static FunctionOverloadResolverPtr create(ContextPtr context_) { return std::make_unique<FunctionArrayReduceOverloadResolver>(context_); }
    explicit FunctionArrayReduceOverloadResolver(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto result = resolveAggregateFunction(arguments)->getResultType();
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            if (arguments[i].type->isNullable())
                return makeNullable(result);
        }
        return result;
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        /// useDefaultImplementationForNulls() is false, so buildImpl receives Nullable arguments
        /// just like getReturnTypeImpl does. Resolve the aggregate function with the original types
        /// so it matches what executeImpl will receive (also Nullable, since the IFunction also
        /// has useDefaultImplementationForNulls() = false).
        auto aggregate_function = resolveAggregateFunction(arguments);
        auto function = std::make_shared<FunctionArrayReduce>(std::move(aggregate_function));

        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        return std::make_unique<FunctionToFunctionBaseAdaptor>(function, data_types, return_type);
    }

private:
    AggregateFunctionPtr resolveAggregateFunction(const ColumnsWithTypeAndName & arguments) const
    {
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
            const DataTypeArray * arg = checkAndGetDataType<DataTypeArray>(removeNullable(arguments[i].type).get());
            if (!arg)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Argument {} for function {} must be an array but it has type {}.",
                                i, getName(), arguments[i].type->getName());

            argument_types[i - 1] = arg->getNestedType();
        }

        String aggregate_function_name_with_params = aggregate_function_name_column->getValue<String>();

        if (aggregate_function_name_with_params.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "First argument for function {} (name of aggregate function) cannot be empty.", getName());

        String aggregate_function_name;
        Array params_row;
        getAggregateFunctionNameAndParametersArray(aggregate_function_name_with_params,
                                                   aggregate_function_name, params_row, "function " + getName(), getContext());

        auto action = NullsAction::EMPTY;
        AggregateFunctionProperties properties;
        return AggregateFunctionFactory::instance().get(aggregate_function_name, action, argument_types, params_row, properties);
    }
};

}


REGISTER_FUNCTION(ArrayReduce)
{
    FunctionDocumentation::Description description = R"(
Applies an aggregate function to array elements and returns its result.
The name of the aggregation function is passed as a string in single quotes `'max'`, `'sum'`.
When using parametric aggregate functions, the parameter is indicated after the function name in parentheses `'uniqUpTo(6)'`.
)";
    FunctionDocumentation::Syntax syntax = "arrayReduce(agg_f, arr1[, arr2, ... , arrN])";
    FunctionDocumentation::Arguments arguments = {
        {"agg_f", "The name of an aggregate function which should be a constant.", {"String"}},
        {"arr1[, arr2, ... , arrN]", "N arrays corresponding to the arguments of `agg_f`.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the result of the aggregate function"};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT arrayReduce('max', [1, 2, 3]);", R"(
┌─arrayReduce('max', [1, 2, 3])─┐
│                             3 │
└───────────────────────────────┘
)"},{"Example with aggregate function using multiple arguments", R"(--If an aggregate function takes multiple arguments, then this function must be applied to multiple arrays of the same size.

SELECT arrayReduce('maxIf', [3, 5], [1, 0]);
)", R"(
┌─arrayReduce('maxIf', [3, 5], [1, 0])─┐
│                                    3 │
└──────────────────────────────────────┘
)"},{"Example with a parametric aggregate function", R"(
SELECT arrayReduce('uniqUpTo(3)', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
)", R"(
┌─arrayReduce('uniqUpTo(3)', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])─┐
│                                                           4 │
└─────────────────────────────────────────────────────────────┘
)"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayReduceOverloadResolver>(documentation);
}

}
