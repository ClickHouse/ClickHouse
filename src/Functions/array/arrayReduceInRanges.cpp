#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnAggregateFunction.h>
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
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}


/** Applies an aggregate function to value ranges in the array.
  * The function does what arrayReduce do on a structure similar to segment tree.
  * Space complexity: n * log(n)
  *
  * arrayReduceInRanges('agg', indices, lengths, arr1, ...)
  */
class FunctionArrayReduceInRanges : public IFunction, private WithContext
{
public:
    static const size_t minimum_step = 64;
    static constexpr auto name = "arrayReduceInRanges";
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionArrayReduceInRanges>(context_); }
    explicit FunctionArrayReduceInRanges(ContextPtr context_) : WithContext(context_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

private:
    /// lazy initialization in getReturnTypeImpl
    /// TODO: init in OverloadResolver
    mutable AggregateFunctionPtr aggregate_function;
};


DataTypePtr FunctionArrayReduceInRanges::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    /// The first argument is a constant string with the name of the aggregate function
    ///  (possibly with parameters in parentheses, for example: "quantile(0.99)").

    if (arguments.size() < 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function {} doesn't match: passed {}, should be at least 3.",
            getName(), arguments.size());

    const ColumnConst * aggregate_function_name_column = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
    if (!aggregate_function_name_column)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be constant string: "
            "name of aggregate function.", getName());

    const DataTypeArray * ranges_type_array = checkAndGetDataType<DataTypeArray>(arguments[1].type.get());
    if (!ranges_type_array)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument for function {} must be an array of ranges.",
            getName());
    const DataTypeTuple * ranges_type_tuple = checkAndGetDataType<DataTypeTuple>(ranges_type_array->getNestedType().get());
    if (!ranges_type_tuple || ranges_type_tuple->getElements().size() != 2)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Each array element in the second argument for function {} must be a tuple (index, length).",
                        getName());
    if (!isNativeInteger(ranges_type_tuple->getElements()[0]))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "First tuple member in the second argument for function {} must be ints or uints.", getName());
    if (!WhichDataType(ranges_type_tuple->getElements()[1]).isNativeUInt())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Second tuple member in the second argument for function {} must be uints.", getName());

    DataTypes argument_types(arguments.size() - 2);
    for (size_t i = 2, size = arguments.size(); i < size; ++i)
    {
        const DataTypeArray * arg = checkAndGetDataType<DataTypeArray>(arguments[i].type.get());
        if (!arg)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Argument {} for function {} must be an array but it has type {}.",
                            i, getName(), arguments[i].type->getName());

        argument_types[i - 2] = arg->getNestedType();
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

        AggregateFunctionProperties properties;
        auto action = NullsAction::EMPTY;
        aggregate_function
            = AggregateFunctionFactory::instance().get(aggregate_function_name, action, argument_types, params_row, properties);
    }

    return std::make_shared<DataTypeArray>(aggregate_function->getResultType());
}


ColumnPtr FunctionArrayReduceInRanges::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
{
    const IAggregateFunction & agg_func = *aggregate_function;
    std::unique_ptr<Arena> arena = std::make_unique<Arena>();

    /// Aggregate functions do not support constant columns. Therefore, we materialize them.
    std::vector<ColumnPtr> materialized_columns;

    /// Handling ranges

    const IColumn * ranges_col_array = arguments[1].column.get();
    const IColumn * ranges_col_tuple = nullptr;
    const ColumnArray::Offsets * ranges_offsets = nullptr;
    if (const ColumnArray * arr = checkAndGetColumn<ColumnArray>(ranges_col_array))
    {
        ranges_col_tuple = &arr->getData();
        ranges_offsets = &arr->getOffsets();
    }
    else if (const ColumnConst * const_arr = checkAndGetColumnConst<ColumnArray>(ranges_col_array))
    {
        materialized_columns.emplace_back(const_arr->convertToFullColumn());
        const auto & materialized_arr = typeid_cast<const ColumnArray &>(*materialized_columns.back());
        ranges_col_tuple = &materialized_arr.getData();
        ranges_offsets = &materialized_arr.getOffsets();
    }
    else
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} as argument of function {}", ranges_col_array->getName(), getName());

    const IColumn & indices_col = static_cast<const ColumnTuple *>(ranges_col_tuple)->getColumn(0);
    const IColumn & lengths_col = static_cast<const ColumnTuple *>(ranges_col_tuple)->getColumn(1);

    /// Handling arguments
    /// The code is mostly copied from `arrayReduce`. Maybe create a utility header?

    const size_t num_arguments_columns = arguments.size() - 2;

    std::vector<const IColumn *> aggregate_arguments_vec(num_arguments_columns);
    const ColumnArray::Offsets * offsets = nullptr;

    for (size_t i = 0; i < num_arguments_columns; ++i)
    {
        const IColumn * col = arguments[i + 2].column.get();

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

    /// Handling results

    MutableColumnPtr result_holder = result_type->createColumn();
    ColumnArray * result_arr = static_cast<ColumnArray *>(result_holder.get());
    IColumn & result_data = result_arr->getData();

    result_arr->getOffsets().insert(ranges_offsets->begin(), ranges_offsets->end());

    /// Perform the aggregation

    size_t begin = 0;
    size_t end = 0;
    size_t ranges_begin = 0;
    size_t ranges_end = 0;

    for (size_t i = 0; i < input_rows_count; ++i)
    {
        begin = end;
        end = (*offsets)[i];
        ranges_begin = ranges_end;
        ranges_end = (*ranges_offsets)[i];

        /// We will allocate pre-aggregation places for each `minimum_place << level` rows.
        /// The value of `level` starts from 0, and it will never exceed the number of bits in a `size_t`.
        /// We calculate the offset (and thus size) of those places in each level.
        size_t place_offsets[sizeof(size_t) * 8];
        size_t place_total = 0;
        {
            size_t place_in_level = (end - begin) / minimum_step;

            place_offsets[0] = place_in_level;
            for (size_t level = 0; place_in_level; ++level)
            {
                place_in_level >>= 1;
                place_total = place_offsets[level] + place_in_level;
                place_offsets[level + 1] = place_total;
            }
        }

        PODArray<AggregateDataPtr> places(place_total);
        for (size_t j = 0; j < place_total; ++j)
        {
            places[j] = arena->alignedAlloc(agg_func.sizeOfData(), agg_func.alignOfData());
            try
            {
                agg_func.create(places[j]);
            }
            catch (...)
            {
                for (size_t k = 0; k < j; ++k)
                    agg_func.destroy(places[k]);
                throw;
            }
        }

        SCOPE_EXIT_MEMORY_SAFE({
            for (size_t j = 0; j < place_total; ++j)
                agg_func.destroy(places[j]);
        });

        const auto * true_func = &agg_func;
        /// Unnest consecutive trailing -State combinators
        while (const auto * func = typeid_cast<const AggregateFunctionState *>(true_func))
            true_func = func->getNestedFunction().get();

        /// Pre-aggregate to the initial level
        for (size_t j = 0; j < place_offsets[0]; ++j)
        {
            size_t local_begin = j * minimum_step;
            size_t local_end = (j + 1) * minimum_step;

            for (size_t k = local_begin; k < local_end; ++k)
                true_func->add(places[j], aggregate_arguments, begin + k, arena.get());
        }

        /// Pre-aggregate to the higher levels by merging
        {
            size_t place_in_level = place_offsets[0] >> 1;
            size_t place_begin = 0;

            for (size_t level = 0; place_in_level; ++level)
            {
                size_t next_place_begin = place_offsets[level];

                for (size_t j = 0; j < place_in_level; ++j)
                {
                    true_func->merge(places[next_place_begin + j], places[place_begin + (j << 1)], arena.get());
                    true_func->merge(places[next_place_begin + j], places[place_begin + (j << 1) + 1], arena.get());
                }

                place_in_level >>= 1;
                place_begin = next_place_begin;
            }
        }

        for (size_t j = ranges_begin; j < ranges_end; ++j)
        {
            size_t local_begin = 0;
            size_t local_end = 0;

            {
                Int64 index = indices_col.getInt(j);
                UInt64 length = lengths_col.getUInt(j);

                /// Keep the same as in arraySlice

                if (index > 0)
                {
                    local_begin = index - 1;
                    if (local_begin + length < end - begin)
                        local_end = local_begin + length;
                    else
                        local_end = end - begin;
                }
                else if (index < 0)
                {
                    if (end - begin + index > 0)
                        local_begin = end - begin + index;
                    else
                        local_begin = 0;

                    if (local_begin + length < end - begin)
                        local_end = local_begin + length;
                    else
                        local_end = end - begin;
                }
            }

            size_t place_begin = (local_begin + minimum_step - 1) / minimum_step;
            size_t place_end = local_end / minimum_step;

            AggregateDataPtr place = arena->alignedAlloc(agg_func.sizeOfData(), agg_func.alignOfData());
            agg_func.create(place);

            SCOPE_EXIT_MEMORY_SAFE({
                agg_func.destroy(place);
            });

            if (place_begin < place_end)
            {
                /// In this case, we can use pre-aggregated data.

                /// Aggregate rows before
                for (size_t k = local_begin; k < place_begin * minimum_step; ++k)
                    true_func->add(place, aggregate_arguments, begin + k, arena.get());

                /// Aggregate using pre-aggretated data
                {
                    size_t level = 0;
                    size_t place_curr = place_begin;

                    while (place_curr < place_end)
                    {
                        while (((place_curr >> level) & 1) == 0 && place_curr + (2 << level) <= place_end)
                            level += 1;
                        while (place_curr + (1 << level) > place_end)
                            level -= 1;

                        size_t place_offset = 0;
                        if (level)
                            place_offset = place_offsets[level - 1];

                        true_func->merge(place, places[place_offset + (place_curr >> level)], arena.get());
                        place_curr += 1 << level;
                    }
                }

                /// Aggregate rows after
                for (size_t k = place_end * minimum_step; k < local_end; ++k)
                    true_func->add(place, aggregate_arguments, begin + k, arena.get());
            }
            else
            {
                /// In this case, we can not use pre-aggregated data.

                for (size_t k = local_begin; k < local_end; ++k)
                    true_func->add(place, aggregate_arguments, begin + k, arena.get());
            }
            /// We should use insertMergeResultInto to insert result into ColumnAggregateFunction
            /// correctly if result contains AggregateFunction's states
            agg_func.insertMergeResultInto(place, result_data, arena.get());
        }
    }

    return result_holder;
}


REGISTER_FUNCTION(ArrayReduceInRanges)
{
    factory.registerFunction<FunctionArrayReduceInRanges>();
}

}
