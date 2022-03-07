#include <Functions/FunctionFactory.h>
#include "FunctionArrayMapped.h"


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

class FunctionArrayFoldMe : public IFunction
{
public:
    static constexpr auto name = "arayFoldMe";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayFoldMe>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }


    void getLambdaArgumentTypes(DataTypes & arguments) const override
    {
        if (arguments.size() < 3)
            throw Exception(
                "Function " + getName() + " needs lambda function, at least one array argument and one accumulator argument.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        DataTypes nested_types(arguments.size() - 1);
        for (size_t i = 0; i < nested_types.size() - 1; ++i)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(&*arguments[i + 1]);
            if (!array_type)
                throw Exception(
                    "Argument " + toString(i + 2) + " of function " + getName() + " must be array. Found " + arguments[i + 1]->getName()
                        + " instead.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            nested_types[i] = recursiveRemoveLowCardinality(array_type->getNestedType());
        }
        nested_types[nested_types.size() - 1] = arguments[arguments.size() - 1];

        const DataTypeFunction * function_type = checkAndGetDataType<DataTypeFunction>(arguments[0].get());
        if (!function_type || function_type->getArgumentTypes().size() != nested_types.size())
            throw Exception(
                "First argument for this overload of " + getName() + " must be a function with " + toString(nested_types.size())
                    + " arguments. Found " + arguments[0]->getName() + " instead.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        arguments[0] = std::make_shared<DataTypeFunction>(nested_types);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(
                "Function " + getName() + " needs at least 2 arguments; passed " + toString(arguments.size()) + ".",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        const auto * data_type_function = checkAndGetDataType<DataTypeFunction>(arguments[0].type.get());
        if (!data_type_function)
            throw Exception("First argument for function " + getName() + " must be a function.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto const accumulator_type = arguments.back().type;
        auto const lambda_type = data_type_function->getReturnType();
        if (!accumulator_type->equals(*lambda_type))
            throw Exception(
                "Return type of lambda function must be the same as the accumulator type. "
                "Inferred type of lambda "
                    + lambda_type->getName() + ", " + "inferred type of accumulator " + accumulator_type->getName() + ".",
                ErrorCodes::TYPE_MISMATCH);

        return DataTypePtr(accumulator_type);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const auto & column_function_with_type_and_name = arguments[0];

        if (!column_function_with_type_and_name.column)
            throw Exception("First argument for function " + getName() + " must be a function", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const auto * column_function = typeid_cast<const ColumnFunction *>(column_function_with_type_and_name.column.get());

        if (!column_function)
            throw Exception("First argument for function " + getName() + " must be a function", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        auto column_first_array_ptr = arguments[1].column;
        const auto * column_first_array = checkAndGetColumn<ColumnArray>(column_first_array_ptr.get());
        MutableColumnPtr result = arguments.back().column->convertToFullColumnIfConst()->cloneEmpty();
        result->reserve(column_first_array->size());
        std::vector<size_t> array_size_vec(column_first_array->size());
        ColumnVector<UInt8>::MutablePtr filter = ColumnVector<UInt8>::create(column_first_array->size());
        array_size_vec[0] = column_first_array->getOffsets()[0];
        size_t max_array_size = array_size_vec[0];

        for (size_t i = 1; i < array_size_vec.size(); ++i)
        {
            array_size_vec[i] = column_first_array->getOffsets()[i] - column_first_array->getOffsets()[i - 1];
            max_array_size = std::max(max_array_size, array_size_vec[i]);
        }

        ColumnWithTypeAndName column_accumulator = arguments.back().cloneEmpty();
        ColumnsWithTypeAndName columns_lambda_input(arguments.size() - 1);

        for (size_t array_size = 0; array_size < max_array_size; ++array_size)
        {
            filter.reset();
            size_t size_hint = 0;

            /// Construct a filter
            for (size_t irow = 0; irow < array_size_vec.size(); ++irow)
            {
                size_t remaining_array_size = array_size_vec[irow] - array_size;

                /// Arrays whose size is smaller than array_size will be filtered away.
                filter->insertValue(remaining_array_size > 0 ? 1 : 0);

                if (remaining_array_size == 0)
                {
                    /// We got the final accumulator of current row, final accumulator is exactly the result of current row.
                    /// 1. Insert it to row of result column
                    /// 2. cut this item from column_accumulator
                    (*result)[irow] = (*column_accumulator.column)[irow];
                    // column_accumulator.column->cut(irow, 1);
                    continue;
                }

                ++size_hint;
            }

            /// Traverse each input array column
            /// 1. Filter arrays whose size is larger than current array_size
            /// 2. Extract array_size-th element of each filtered array, construct a input array column
            for (size_t icol = 1; icol < arguments.size() - 1; ++icol)
            {
                const auto & array_with_type_and_name = arguments[icol];
                const ColumnPtr column_filtered_array_ptr = array_with_type_and_name.column->filter(filter->getData(), size_hint);
                const auto * column_filtered_array = checkAndGetColumn<ColumnArray>(column_filtered_array_ptr.get());
                auto column_input_array = ColumnArray::create(column_filtered_array->cloneEmpty());

                /// Extract array_size-th element from filtered_array
                for (size_t irow = 0; irow < column_filtered_array->size(); ++irow)
                {
                    size_t array_begin = column_filtered_array->getOffsets()[irow - 1];
                    size_t elem_idx = array_begin + array_size * sizeof(column_filtered_array->getDataType());
                    /// Copy element and create a new array with this element
                    column_input_array->insertRangeFrom(column_filtered_array->getData(), elem_idx, 1);
                }

                columns_lambda_input.emplace_back(
                    ColumnWithTypeAndName(column_input_array->getDataPtr(), array_with_type_and_name.type, array_with_type_and_name.name));
            }

            columns_lambda_input.emplace_back(column_accumulator);
            auto replicated_column_function_ptr
                = IColumn::mutate(column_function->replicate(ColumnArray::Offsets(column_first_array->getOffsets().size(), 1)));
            auto * replicated_column_function = typeid_cast<ColumnFunction *>(replicated_column_function_ptr.get());
            replicated_column_function->appendArguments(columns_lambda_input);
            auto lambda_result = replicated_column_function->reduce().column;

            if (lambda_result->lowCardinality())
                lambda_result = lambda_result->convertToFullColumnIfLowCardinality();

            auto res = lambda_result->cut(0, 1);
            column_accumulator.column = res;
            result->insert((*res)[0]);
        }

        return result;
    }
};

void registerFunctionArrayFoldMe(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayFoldMe>();
}
}
