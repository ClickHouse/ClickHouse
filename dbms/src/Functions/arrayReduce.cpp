#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnAggregateFunction.h>
#include <IO/WriteHelpers.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/parseAggregateFunctionParameters.h>
#include <Common/AlignedBuffer.h>
#include <Common/Arena.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
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
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionArrayReduce>(); }

    String getName() const override { return name; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    String getSignature() const override { return "f(const agg_func_name String, Array(T1), ...) -> returnTypeOfAggregateFunction(agg_func_name, T1, ...)"; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;
};


void FunctionArrayReduce::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
{
    String aggregate_function_name_with_params = typeid_cast<const ColumnConst &>(*block.getByPosition(arguments[0]).column).getValue<String>();
    DataTypes argument_types;
    const size_t num_arguments_columns = arguments.size() - 1;
    for (size_t i = 0; i < num_arguments_columns; ++i)
        argument_types.emplace_back(typeid_cast<const DataTypeArray &>(*block.getByPosition(arguments[i + 1]).type).getNestedType());

    if (aggregate_function_name_with_params.empty())
        throw Exception("First argument for function " + getName() + " (name of aggregate function) cannot be empty.",
            ErrorCodes::BAD_ARGUMENTS);

    String aggregate_function_name;
    Array params_row;
    getAggregateFunctionNameAndParametersArray(aggregate_function_name_with_params,
                                                aggregate_function_name, params_row, "function " + getName());

    AggregateFunctionPtr aggregate_function = AggregateFunctionFactory::instance().get(aggregate_function_name, argument_types, params_row);

    AlignedBuffer place_holder(aggregate_function->sizeOfData(), aggregate_function->alignOfData());
    AggregateDataPtr place = place_holder.data();

    std::unique_ptr<Arena> arena = aggregate_function->allocatesMemoryInArena() ? std::make_unique<Arena>() : nullptr;

    size_t rows = input_rows_count;

    /// Aggregate functions do not support constant columns. Therefore, we materialize them.
    std::vector<ColumnPtr> materialized_columns;

    std::vector<const IColumn *> aggregate_arguments_vec(num_arguments_columns);
    const ColumnArray::Offsets * offsets = nullptr;

    for (size_t i = 0; i < num_arguments_columns; ++i)
    {
        const IColumn * col = block.getByPosition(arguments[i + 1]).column.get();
        const ColumnArray::Offsets * offsets_i = nullptr;
        if (const ColumnArray * arr = checkAndGetColumn<ColumnArray>(col))
        {
            aggregate_arguments_vec[i] = &arr->getData();
            offsets_i = &arr->getOffsets();
        }
        else if (const ColumnConst * const_arr = checkAndGetColumnConst<ColumnArray>(col))
        {
            materialized_columns.emplace_back(const_arr->convertToFullColumn());
            const auto & arr = typeid_cast<const ColumnArray &>(*materialized_columns.back().get());
            aggregate_arguments_vec[i] = &arr.getData();
            offsets_i = &arr.getOffsets();
        }
        else
            throw Exception("Illegal column " + col->getName() + " as argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN);

        if (i == 0)
            offsets = offsets_i;
        else if (*offsets_i != *offsets)
            throw Exception("Lengths of all arrays passed to " + getName() + " must be equal.",
                ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
    }
    const IColumn ** aggregate_arguments = aggregate_arguments_vec.data();

    MutableColumnPtr result_holder = block.getByPosition(result).type->createColumn();
    IColumn & res_col = *result_holder;

    /// AggregateFunction's states should be inserted into column using specific way
    auto res_col_aggregate_function = typeid_cast<ColumnAggregateFunction *>(&res_col);

    if (!res_col_aggregate_function && aggregate_function->isState())
        throw Exception("State function " + aggregate_function->getName() + " inserts results into non-state column "
                        + block.getByPosition(result).type->getName(), ErrorCodes::ILLEGAL_COLUMN);

    ColumnArray::Offset current_offset = 0;
    for (size_t i = 0; i < rows; ++i)
    {
        aggregate_function->create(place);
        ColumnArray::Offset next_offset = (*offsets)[i];

        try
        {
            for (size_t j = current_offset; j < next_offset; ++j)
                aggregate_function->add(place, aggregate_arguments, j, arena.get());

            if (!res_col_aggregate_function)
                aggregate_function->insertResultInto(place, res_col);
            else
                res_col_aggregate_function->insertFrom(place);
        }
        catch (...)
        {
            aggregate_function->destroy(place);
            throw;
        }

        aggregate_function->destroy(place);
        current_offset = next_offset;
    }

    block.getByPosition(result).column = std::move(result_holder);
}


void registerFunctionArrayReduce(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayReduce>();
}

}
