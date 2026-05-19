#pragma once

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromVector.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER;
}

/** formatReadableSize - prints the transferred size in bytes in form `123.45 GiB`.
  * formatReadableQuantity - prints the quantity in form of 123 million.
  */

class FunctionFormatReadable : public IFunction
{
public:
    static constexpr UInt8 DEFAULT_PRECISION = 2;

    using FormatFunc = void (*)(double, WriteBuffer &, int);

    FunctionFormatReadable(const char * name_, FormatFunc format_func_)
        : function_name(name_), format_func(format_func_) {}

    static FunctionPtr create(const char * name, FormatFunc format_func)
    {
        return std::make_shared<FunctionFormatReadable>(name, format_func);
    }

    String getName() const override
    {
        return function_name;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_arguments{
            {"x", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"},
        };

        FunctionArgumentDescriptors optional_arguments{
            {"precision", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt8), &isColumnConst, "const UInt8"},
        };

        validateFunctionArguments(getName(), arguments, mandatory_arguments, optional_arguments);

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        UInt8 precision = DEFAULT_PRECISION;
        if (arguments.size() == 2 && !arguments[1].column->empty())
        {
            precision = assert_cast<const ColumnConst &>(*arguments[1].column).getValue<UInt8>();

            /// 'ToFixed' in 'double-conversion' fails when requested_digits > 'kMaxFixedDigitsAfterPoint' (100);
            /// the formatter then silently falls back to 'ToShortest', ignoring the requested precision.
            if (precision > double_conversion::DoubleToStringConverter::kMaxFixedDigitsAfterPoint)
                throw Exception(
                    DB::ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER,
                    "Too high precision requested, must not be more than {}, got {}",
                     static_cast<int>(double_conversion::DoubleToStringConverter::kMaxFixedDigitsAfterPoint), static_cast<uint8_t>(precision));
        }

        auto col_to = ColumnString::create();

        ColumnString::Chars & data_to = col_to->getChars();
        ColumnString::Offsets & offsets_to = col_to->getOffsets();
        data_to.resize(input_rows_count * 2);
        offsets_to.resize(input_rows_count);

        WriteBufferFromVector<ColumnString::Chars> buf_to(data_to);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            /// The cost of the virtual call for getFloat64 is negligible compared with the format calls
            format_func(arguments[0].column->getFloat64(i), buf_to, precision);
            offsets_to[i] = buf_to.count();
        }

        buf_to.finalize();
        return col_to;
    }

private:
    const char * function_name;
    FormatFunc format_func;
};

}
