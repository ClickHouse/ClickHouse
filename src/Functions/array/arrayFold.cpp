#include <cassert>
#include <memory>
#include <Functions/FunctionFactory.h>
#include <Poco/Logger.h>


#include "Common/Exception.h"
#include "Columns/ColumnArray.h"
#include "Columns/ColumnConst.h"
#include "Columns/ColumnFunction.h"
#include "Columns/IColumn.h"
#include "Core/ColumnWithTypeAndName.h"
#include "DataTypes/DataTypeArray.h"
#include "DataTypes/DataTypeLowCardinality.h"
#include "DataTypes/DataTypeFunction.h"
#include "FunctionArrayMapped.h"
#include "Functions/FunctionHelpers.h"
#include "Functions/IFunction.h"
#include "Interpreters/Context_fwd.h"
#include "base/types.h"

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

class FunctionArrayFold : public IFunction, private WithContext
{
public:
    static constexpr auto name = "arrayFold";
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionArrayFold>(context_); }
    explicit FunctionArrayFold(ContextPtr context_) : WithContext(context_) { }
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    void getLambdaArgumentTypes(DataTypes & arguments) const override;
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
};


void FunctionArrayFold::getLambdaArgumentTypes(DataTypes & arguments) const
{
    if (arguments.size() < 3)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} needs lambda function, at least one array argument and one accumulator argument.", getName());

    const auto lambda_arguments_size = arguments.size() - 1;
    DataTypes nested_types(lambda_arguments_size);
    /// Lambda functiong's input arguments should all be array, except last one.
    for (size_t i = 0; i < lambda_arguments_size - 1; ++i)
    {
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(&*arguments[i + 1]);
        if (!array_type)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument {} of function {} must be array. Found {} instead.", i + 1, getName(), arguments[i + 1]->getName());

        nested_types[i] = recursiveRemoveLowCardinality(array_type->getNestedType());
    }
    nested_types[lambda_arguments_size - 1] = arguments[lambda_arguments_size];

    const DataTypeFunction * function_type = checkAndGetDataType<DataTypeFunction>(arguments[0].get());
    if (!function_type || function_type->getArgumentTypes().size() != nested_types.size())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First argument for this overload of {}  must be a function with {} arguments, Found {} instead.", getName(), toString(nested_types.size()), arguments[0]->getName());

    arguments[0] = std::make_shared<DataTypeFunction>(nested_types);
}

DataTypePtr FunctionArrayFold::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    if (arguments.size() < 3) 
    {
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Function {} needs at least 3 arguments; passed {}.", getName(), toString(arguments.size()));
    }
        
    const auto * data_type_function = checkAndGetDataType<DataTypeFunction>(arguments[0].type.get());
    if (!data_type_function)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a function.", getName());

    auto const accumulator_type = arguments.back().type;
    auto const lambda_return_type = data_type_function->getReturnType();
    if (!accumulator_type->equals(*lambda_return_type))
        throw Exception(ErrorCodes::TYPE_MISMATCH,
            "Return type of lambda function must be the same as the accumulator type. "
            "Inferred type of lambda {}, inferred type of accumulator {}.", lambda_return_type->getName(), accumulator_type->getName());

    return DataTypePtr(accumulator_type);
}

ColumnPtr FunctionArrayFold::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t rows_count) const
{
    const auto & column_function_with_type_and_name = arguments[0];
    if (!column_function_with_type_and_name.column)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a function.", getName());

    const auto * column_function = typeid_cast<const ColumnFunction *>(column_function_with_type_and_name.column.get());
    if (!column_function)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a function.", getName());

    const size_t arguments_count = arguments.size();
    assert(arguments_count > 2);
    ColumnPtr column_offset;
    const ColumnArray * column_first_array = nullptr;
    ColumnsWithTypeAndName arrays;
    arrays.reserve(arguments_count - 1);

    for (size_t i = 1; i < arguments_count - 1; ++i) 
    {
        const auto & array_with_type_and_name = arguments[i];
        ColumnPtr column_array_ptr = array_with_type_and_name.column;
        const auto * column_array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());
        if (!column_array)
        {
            const ColumnConst * column_const_array = checkAndGetColumnConst<ColumnArray>(column_array_ptr.get());
            if (!column_const_array)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN, "Expected array column, found {}", column_array_ptr->getName());
            column_array_ptr = recursiveRemoveLowCardinality(column_const_array->convertToFullColumn());
            column_array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());
        }

        const DataTypePtr & type_array_ptr = array_with_type_and_name.type;
        const auto * type_array = checkAndGetDataType<DataTypeArray>(type_array_ptr.get());
        if (!type_array)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Expected array type, found {}.", type_array_ptr->getName());

        if (!column_offset)
        {
            column_offset = column_array->getOffsetsPtr();
        }
        else
        {
            /// The first condition is optimization: do not compare data if the pointers are equal.
            if (column_array->getOffsetsPtr() != column_offset
                && column_array->getOffsets() != typeid_cast<const ColumnArray::ColumnOffsets &>(*column_offset).getData())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Arrays passed to {} must have equal size.", getName());
        }
        if (i == 1)
        {
            column_first_array = column_array;
        }
        arrays.emplace_back(ColumnWithTypeAndName(column_array->getDataPtr(),
                                                  recursiveRemoveLowCardinality(type_array->getNestedType()),
                                                  array_with_type_and_name.name));
    }
    arrays.emplace_back(arguments.back());

    if (rows_count == 0)
            return arguments.back().column->convertToFullColumnIfConst()->cloneEmpty();

    ColumnWithTypeAndName column_accumulator = arguments.back();
    std::vector<size_t> array_size_vec(rows_count);
    const ColumnArray::Offsets& array_offsets = column_first_array->getOffsets();
    size_t max_array_size = rows_count ? array_offsets[0] : 0;
    std::map<size_t, ColumnPtr> row_res;

    for (size_t i = 0; i < rows_count; ++i)
    {
        auto size = array_offsets[i] - array_offsets[i - 1];
        array_size_vec[i] = size;
        max_array_size = std::max(max_array_size, array_size_vec[i]);
        if (size == 0) 
        {
            // use accumulator as its final result, cause lambda should not be executed on empty array
            row_res[i] = arguments.back().column;
        }
    }

    ColumnsWithTypeAndName columns_lambda_input;
    columns_lambda_input.reserve(arguments_count - 1);
    
    for (size_t array_size = 1; array_size <= max_array_size; ++array_size)
    {
        columns_lambda_input.clear();    

        for (size_t column_idx = 0; column_idx < arguments_count - 2; ++column_idx)
        {
            const auto & array_element_with_type_and_name = arrays[column_idx];
            const ColumnPtr& column_array_element_ptr = array_element_with_type_and_name.column;
            MutableColumnPtr column_lambda_input = array_element_with_type_and_name.column->cloneEmpty();
            
            for (size_t irow = 0; irow < rows_count; ++irow)
            {
                if (array_size_vec[irow] >= array_size) 
                {
                    const size_t array_begin = array_offsets[irow - 1];
                    const size_t array_element_idx = array_begin + array_size - 1;
                    column_lambda_input->insert((*column_array_element_ptr)[array_element_idx]);
                }
                else
                {
                    // When current array is smaller than array_size, we insert dummy data 
                    // to avoid heavy operation on array, otherwise we need to do scattor like operation.
                    // Dummy data has no effect on result of current row, cause result 
                    // of dummy data will not be added to final result.
                    column_lambda_input->insertDefault();
                }
            }

            columns_lambda_input.emplace_back(
                ColumnWithTypeAndName(std::move(column_lambda_input), array_element_with_type_and_name.type, array_element_with_type_and_name.name));
        }

        columns_lambda_input.emplace_back(column_accumulator);
        auto mutable_column_function_ptr = IColumn::mutate(column_function->getPtr());
        auto * mutable_column_function = typeid_cast<ColumnFunction *>(mutable_column_function_ptr.get());
        mutable_column_function->appendArguments(columns_lambda_input);
        auto lambda_result = mutable_column_function->reduce().column;
        
        if (lambda_result->lowCardinality())
            lambda_result = lambda_result->convertToFullColumnIfLowCardinality();

        column_accumulator.column = lambda_result;

        for (size_t irow = 0; irow < rows_count; ++irow)
        {
            if (array_size_vec[irow] - array_size == 0)
            {
                /// We got the final accumulator of current row, final accumulator is exactly the result of current row.
                row_res[irow] = ColumnPtr(lambda_result->cut(irow, 1));
            }
        }
    }
    
    MutableColumnPtr result = arguments.back().column->convertToFullColumnIfConst()->cloneEmpty();
    for (auto & row_re : row_res) {
        result->insert((*row_re.second)[0]);
    }
    return result;
}


REGISTER_FUNCTION(FunctionArrayFold)
{
    factory.registerFunction<FunctionArrayFold>(FunctionDocumentation{.description=R"(
        Function arrayFold(x1,...,xn,accum -> expression, array1,...,arrayn, init_accum) applies lambda function to a number of same sized array columns
        and collects result in accumulator. Accumulator can be either constant or column.
        )", .examples{{"sum", "SELECT arrayFold(x,acc -> acc + x, [1,2,3,4], toInt64(1));", "11"}}, .categories{"Array"}});
}

}
