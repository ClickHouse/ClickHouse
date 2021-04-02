#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Core/ColumnWithTypeAndName.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
}

class FunctionValidateNestedArraySizes : public IFunction
{
public:
    static constexpr auto name = "validateNestedArraySizes";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionValidateNestedArraySizes>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override;
};

DataTypePtr FunctionValidateNestedArraySizes::getReturnTypeImpl(const DataTypes & arguments) const
{
    size_t num_args = arguments.size();

    if (num_args < 3)
        throw Exception(
            "Function " + getName() + " needs one argument; passed " + toString(arguments.size()) + ".",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (!WhichDataType(arguments[0]).isUInt8())
        throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + " Must be UInt.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    for (size_t i = 1; i < num_args; ++i)
        if (!WhichDataType(arguments[i]).isArray())
            throw Exception(
                "Illegal type " + arguments[i]->getName() + " of " + toString(i) + " argument of function " + getName() + " Must be Array.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<DataTypeUInt8>();
}

ColumnPtr FunctionValidateNestedArraySizes::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const
{
    const ColumnUInt8 * condition_column = typeid_cast<const ColumnUInt8 *>(arguments[0].column.get());

    size_t args_num = arguments.size();
    
    for (size_t i = 0; i < input_rows_count; ++i)
    {
        if (!condition_column->getData()[i])
            continue;

        /// The condition is true, then check the row in subcolumns in Nested Type has the same array size
        size_t first_length = 0;
        size_t length = 0;
        for (size_t args_idx = 1; args_idx < args_num; ++args_idx)
        {
            ColumnWithTypeAndName current_arg = arguments[args_idx];
            const ColumnArray * current_column = nullptr;
            if (const auto *const_array = checkAndGetColumnConst<ColumnArray>(current_arg.column.get()))
            {
                current_column = checkAndGetColumn<ColumnArray>(&const_array->getDataColumn());
                length = current_column->getOffsets()[0];
            }
            else
            {
                current_column = typeid_cast<const ColumnArray *>(current_arg.column.get());
                auto & offsets = current_column->getOffsets();
                length = offsets[i] - offsets[i - 1];
            }
            
            if (args_idx == 1)
            {
                first_length = length;
            }
            else if (first_length != length)
            {
                throw Exception(
                    "Elements '" + arguments[1].column->getName() + "' and '" + arguments[i].column->getName()
                        + +"' of Nested data structure (Array columns) have different array sizes.",
                    ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
            }
        }
    }
    
    auto res = ColumnUInt8::create(input_rows_count);
    auto & vec_res = res->getData();
    for (size_t row_num = 0; row_num < input_rows_count; ++row_num)
        vec_res[row_num] = 1;

    return res; 
}

void registerFunctionValidateNestedArraySizes(FunctionFactory & factory)
{
    factory.registerFunction<FunctionValidateNestedArraySizes>();
}

}
