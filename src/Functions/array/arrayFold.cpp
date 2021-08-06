#include "FunctionArrayMapped.h"
#include <Functions/FunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int TYPE_MISMATCH;
}


/** arrayFold(x1,...,xn,accum -> expression, array1,...,arrayn, init_accum) - apply the expression to each element of the array (or set of parallel arrays).
  */
class FunctionArrayFold : public IFunction
{
public:
    static constexpr auto name = "arrayFold";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayFold>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    void getLambdaArgumentTypes(DataTypes & arguments) const override
    {
        if (arguments.size() < 3)
            throw Exception("Function " + getName() + " needs lambda function, at least one array argument and one accumulator argument.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        DataTypes nested_types(arguments.size() - 1);
        for (size_t i = 0; i < nested_types.size() - 1; ++i)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(&*arguments[i + 1]);
            if (!array_type)
                throw Exception("Argument " + toString(i + 2) + " of function " + getName() + " must be array. Found "
                                + arguments[i + 1]->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            nested_types[i] = recursiveRemoveLowCardinality(array_type->getNestedType());
        }
        nested_types[nested_types.size() - 1] = arguments[arguments.size() - 1];

        const DataTypeFunction * function_type = checkAndGetDataType<DataTypeFunction>(arguments[0].get());
        if (!function_type || function_type->getArgumentTypes().size() != nested_types.size())
            throw Exception("First argument for this overload of " + getName() + " must be a function with "
                            + toString(nested_types.size()) + " arguments. Found "
                            + arguments[0]->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        arguments[0] = std::make_shared<DataTypeFunction>(nested_types);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception("Function " + getName() + " needs at least 2 arguments; passed "
                            + toString(arguments.size()) + ".",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        const auto * data_type_function = checkAndGetDataType<DataTypeFunction>(arguments[0].type.get());
        if (!data_type_function)
            throw Exception("First argument for function " + getName() + " must be a function.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto const accumulator_type = arguments.back().type;
        auto const lambda_type = data_type_function->getReturnType();
        if (! accumulator_type->equals(*lambda_type))
            throw Exception("Return type of lambda function must be the same as the accumulator type. "
                            "Inferred type of lambda " + lambda_type->getName() + ", "
                            + "inferred type of accumulator " + accumulator_type->getName() + ".",
                ErrorCodes::TYPE_MISMATCH);

        return DataTypePtr(accumulator_type);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const auto & column_with_type_and_name = arguments[0];

        if (!column_with_type_and_name.column)
            throw Exception("First argument for function " + getName() + " must be a function.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const auto * column_function = typeid_cast<const ColumnFunction *>(column_with_type_and_name.column.get());

        if (!column_function)
            throw Exception("First argument for function " + getName() + " must be a function.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        ColumnPtr offsets_column;
        ColumnPtr column_first_array_ptr;
        const ColumnArray * column_first_array = nullptr;
        ColumnsWithTypeAndName arrays;
        arrays.reserve(arguments.size() - 1);

        for (size_t i = 1; i < arguments.size() - 1; ++i)
        {
            const auto & array_with_type_and_name = arguments[i];
            ColumnPtr column_array_ptr = array_with_type_and_name.column;
            const auto * column_array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());
            const DataTypePtr & array_type_ptr = array_with_type_and_name.type;
            const auto * array_type = checkAndGetDataType<DataTypeArray>(array_type_ptr.get());
            if (!column_array)
            {
                const ColumnConst * column_const_array = checkAndGetColumnConst<ColumnArray>(column_array_ptr.get());
                if (!column_const_array)
                    throw Exception("Expected array column, found " + column_array_ptr->getName(), ErrorCodes::ILLEGAL_COLUMN);
                column_array_ptr = recursiveRemoveLowCardinality(column_const_array->convertToFullColumn());
                column_array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());
            }
            if (!array_type)
                throw Exception("Expected array type, found " + array_type_ptr->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            if (!offsets_column)
            {
                offsets_column = column_array->getOffsetsPtr();
            }
            else
            {
                /// The first condition is optimization: do not compare data if the pointers are equal.
                if (column_array->getOffsetsPtr() != offsets_column
                    && column_array->getOffsets() != typeid_cast<const ColumnArray::ColumnOffsets &>(*offsets_column).getData())
                    throw Exception("Arrays passed to " + getName() + " must have equal size", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
            }
            if (i == 1)
            {
                column_first_array_ptr = column_array_ptr;
                column_first_array = column_array;
            }
            arrays.emplace_back(ColumnWithTypeAndName(column_array->getDataPtr(),
                                                      recursiveRemoveLowCardinality(array_type->getNestedType()),
                                                      array_with_type_and_name.name));
        }
        arrays.emplace_back(arguments.back());

        MutableColumnPtr result = arguments.back().column->convertToFullColumnIfConst()->cloneEmpty();
        size_t arr_cursor = 0;
        for (size_t irow = 0; irow < column_first_array->size(); ++irow) // for each row of result
        {
            // Make accumulator column for this row. We initialize it
            // with the starting value given as the last argument.
            ColumnWithTypeAndName accumulator_column = arguments.back();
            ColumnPtr acc(accumulator_column.column->cut(irow, 1));
            auto accumulator = ColumnWithTypeAndName(acc,
                                                     accumulator_column.type,
                                                     accumulator_column.name);
            ColumnPtr res(acc);
            size_t const arr_next = column_first_array->getOffsets()[irow]; // when we do folding
            for (size_t iter = 0; arr_cursor < arr_next; ++iter, ++arr_cursor)
            {
                // Make slice of input arrays and accumulator for lambda
                ColumnsWithTypeAndName iter_arrays;
                iter_arrays.reserve(arrays.size() + 1);
                for (size_t icolumn = 0; icolumn < arrays.size() - 1; ++icolumn)
                {
                    auto const & arr = arrays[icolumn];
                    iter_arrays.emplace_back(ColumnWithTypeAndName(arr.column->cut(arr_cursor, 1),
                                                                   arr.type,
                                                                   arr.name));
                }
                iter_arrays.emplace_back(accumulator);
                // Calculate function on arguments
                auto replicated_column_function_ptr = IColumn::mutate(column_function->replicate(ColumnArray::Offsets(column_first_array->getOffsets().size(), 1)));
                auto * replicated_column_function = typeid_cast<ColumnFunction *>(replicated_column_function_ptr.get());
                replicated_column_function->appendArguments(iter_arrays);
                auto lambda_result = replicated_column_function->reduce().column;
                if (lambda_result->lowCardinality())
                    lambda_result = lambda_result->convertToFullColumnIfLowCardinality();
                res = lambda_result->cut(0, 1);
                accumulator.column = res;
            }
            result->insert((*res)[0]);
        }
        return result;
    }
};


void registerFunctionArrayFold(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayFold>();
}


}

