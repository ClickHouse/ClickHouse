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
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
}

/** Function validateNestedArraySizes is used to check the consistency of Nested DataType subcolumns's offsets when Update
 *  Arguments: num > 2
 *     The first argument is the condition of WHERE in UPDATE operation, only when this is true, we need to check
 *     The rest arguments are the subcolumns of Nested DataType.
 */
class FunctionValidateNestedArraySizes final : public IFunction
{
public:
    static constexpr auto name = "validateNestedArraySizes";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionValidateNestedArraySizes>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }

    String getSignatureString() const override
    {
        return "(UInt8, Array, Array, ...) -> UInt8";
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override;
};

ColumnPtr FunctionValidateNestedArraySizes::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const
{
    bool is_condition_const = false;
    bool condition = false;
    const ColumnUInt8 * condition_column = typeid_cast<const ColumnUInt8 *>(arguments[0].column.get());
    if (!condition_column)
    {
        if (checkAndGetColumnConst<ColumnUInt8>(arguments[0].column.get()))
        {
            is_condition_const = true;
            condition = arguments[0].column->getBool(0);
        }
    }

    size_t args_num = arguments.size();

    for (size_t i = 0; i < input_rows_count; ++i)
    {
        if (is_condition_const && !condition)
            break;

        if (!is_condition_const && !condition_column->getData()[i])
            continue;

        /// The condition is true, then check the row in subcolumns in Nested Type has the same array size
        size_t first_length = 0;
        size_t length = 0;
        for (size_t args_idx = 1; args_idx < args_num; ++args_idx)
        {
            const auto & current_arg = arguments[args_idx];
            const ColumnArray * current_column = nullptr;
            if (const auto * const_array = checkAndGetColumnConst<ColumnArray>(current_arg.column.get()))
            {
                current_column = checkAndGetColumn<ColumnArray>(&const_array->getDataColumn());
                length = current_column->getOffsets()[0];
            }
            else
            {
                current_column = checkAndGetColumn<ColumnArray>(current_arg.column.get());
                const auto & offsets = current_column->getOffsets();
                length = offsets[i] - offsets[i - 1];
            }

            if (args_idx == 1)
            {
                first_length = length;
            }
            else if (first_length != length)
            {
                throw Exception(
                    ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                    "Elements '{}' and '{}' of Nested data structure (Array columns) "
                    "have different array sizes ({} and {} respectively) on row {}",
                    arguments[1].name, arguments[args_idx].name, first_length, length, i);
            }
        }
    }

    return ColumnUInt8::create(input_rows_count, static_cast<UInt8>(1));
}

REGISTER_FUNCTION(ValidateNestedArraySizes)
{
    factory.registerFunction<FunctionValidateNestedArraySizes>(FunctionDocumentation::INTERNAL_FUNCTION_DOCS);
}

}
