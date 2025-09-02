#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>


namespace DB
{

namespace
{

/** byteSize() - get the value size in number of bytes for accounting purposes.
  */
class FunctionByteSize : public IFunction
{
public:
    static constexpr auto name = "byteSize";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionByteSize>();
    }

    String getName() const override { return name; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool useDefaultImplementationForSparseColumns() const override { return false; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        size_t num_args = arguments.size();

        /// If the resulting size is constant, return constant column.

        bool all_constant = true;
        UInt64 constant_size = 0;
        for (size_t arg_num = 0; arg_num < num_args; ++arg_num)
        {
            if (arguments[arg_num].type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
            {
                constant_size += arguments[arg_num].type->getSizeOfValueInMemory();
            }
            else
            {
                all_constant = false;
                break;
            }
        }

        if (all_constant)
            return result_type->createColumnConst(input_rows_count, constant_size);

        auto result_col = ColumnUInt64::create(input_rows_count);
        auto & vec_res = result_col->getData();
        for (size_t arg_num = 0; arg_num < num_args; ++arg_num)
        {
            const IColumn * column = arguments[arg_num].column.get();

            if (arg_num == 0)
                for (size_t row = 0; row < input_rows_count; ++row)
                    vec_res[row] = column->byteSizeAt(row);
            else
                for (size_t row = 0; row < input_rows_count; ++row)
                    vec_res[row] += column->byteSizeAt(row);
        }

        return result_col;
    }
};

}

REGISTER_FUNCTION(ByteSize)
{
    FunctionDocumentation::Description description = R"(
Returns an estimation of the uncompressed byte size of its arguments in memory.
For `String` arguments, the function returns the string length + 8 (length).
If the function has multiple arguments, the function accumulates their byte sizes.
    )";
    FunctionDocumentation::Syntax syntax = "byteSize(arg1[, arg2, ...])";
    FunctionDocumentation::Arguments arguments = {
        {"arg1[, arg2, ...]", "Values of any data type for which to estimate the uncompressed byte size.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an estimation of the byte size of the arguments in memory.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT byteSize('string')
        )",
        R"(
┌─byteSize('string')─┐
│                 15 │
└────────────────────┘
        )"
    },
    {
        "Multiple arguments",
        R"(
SELECT byteSize(NULL, 1, 0.3, '')
        )",
        R"(
┌─byteSize(NULL, 1, 0.3, '')─┐
│                         19 │
└────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionByteSize>(documentation);
}

}
