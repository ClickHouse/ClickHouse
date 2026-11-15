#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Core/Types.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER;
}

namespace
{

class FunctionToDecimalString : public IFunction
{
public:
    static constexpr auto name = "toDecimalString";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionToDecimalString>(); }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args = {
            {"Value", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"},
            {"precision", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeInteger), &isColumnConst, "const Integer"}
        };

        validateFunctionArguments(*this, arguments, mandatory_args, {});

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

private:
    /// For operations with Integer/Float
    template <typename FromVectorType>
    void vectorConstant(const FromVectorType & vec_from, UInt8 precision,
                        ColumnString::Chars & vec_to, ColumnString::Offsets & result_offsets,
                        size_t input_rows_count) const
    {
        result_offsets.resize(input_rows_count);

        /// Buffer is used here and in functions below because resulting size cannot be precisely anticipated,
        /// and buffer resizes on-the-go. Also, .count() provided by buffer is convenient in this case.
        WriteBufferFromVector<ColumnString::Chars> buf_to(vec_to);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            format(vec_from[i], buf_to, precision);
            result_offsets[i] = buf_to.count();
        }

        buf_to.finalize();
    }

    template <typename FirstArgVectorType>
    void vectorVector(const FirstArgVectorType & vec_from, const ColumnVector<UInt8>::Container & vec_precision,
                      ColumnString::Chars & vec_to, ColumnString::Offsets & result_offsets,
                      size_t input_rows_count) const
    {
        result_offsets.resize(input_rows_count);

        WriteBufferFromVector<ColumnString::Chars> buf_to(vec_to);

        constexpr size_t max_digits = std::numeric_limits<UInt256>::digits10;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if (vec_precision[i] > max_digits)
                throw DB::Exception(DB::ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER,
                                    "Too many fractional digits requested, shall not be more than {}", max_digits);
            format(vec_from[i], buf_to, vec_precision[i]);
            result_offsets[i] = buf_to.count();
        }

        buf_to.finalize();
    }

    /// For operations with Decimal
    template <typename FirstArgVectorType>
    void vectorConstant(const FirstArgVectorType & vec_from, UInt8 precision,
                        ColumnString::Chars & vec_to, ColumnString::Offsets & result_offsets, UInt8 from_scale,
                        size_t input_rows_count) const
    {
        /// There are no more than 77 meaning digits (as it is the max length of UInt256). So we can limit it with 77.
        constexpr size_t max_digits = std::numeric_limits<UInt256>::digits10;
        if (precision > max_digits)
            throw DB::Exception(DB::ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER,
                                "Too many fractional digits requested for Decimal, must not be more than {}", max_digits);

        WriteBufferFromVector<ColumnString::Chars> buf_to(vec_to);
        result_offsets.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            writeText(vec_from[i], from_scale, buf_to, true, true, precision);
            result_offsets[i] = buf_to.count();
        }
        buf_to.finalize();
    }

    template <typename FirstArgVectorType>
    void vectorVector(const FirstArgVectorType & vec_from, const ColumnVector<UInt8>::Container & vec_precision,
                      ColumnString::Chars & vec_to, ColumnString::Offsets & result_offsets, UInt8 from_scale,
                      size_t input_rows_count) const
    {
        result_offsets.resize(input_rows_count);

        WriteBufferFromVector<ColumnString::Chars> buf_to(vec_to);

        constexpr size_t max_digits = std::numeric_limits<UInt256>::digits10;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            if (vec_precision[i] > max_digits)
                throw DB::Exception(DB::ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER,
                                    "Too many fractional digits requested for Decimal, must not be more than {}", max_digits);
            writeText(vec_from[i], from_scale, buf_to, true, true, vec_precision[i]);
            result_offsets[i] = buf_to.count();
        }
        buf_to.finalize();
    }

    template <is_floating_point T>
    static void format(T value, DB::WriteBuffer & out, UInt8 precision)
    {
        /// Maximum of 60 is hard-coded in 'double-conversion/double-conversion.h' for floating point values,
        /// Catch this here to give user a more reasonable error.
        if (precision > 60)
            throw DB::Exception(DB::ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER,
                                "Too high precision requested for Float, must not be more than 60, got {}", Int8(precision));

        DB::DoubleConverter<false>::BufferType buffer;
        double_conversion::StringBuilder builder{buffer, sizeof(buffer)};

        const auto result = DB::DoubleConverter<false>::instance().ToFixed(value, precision, &builder);

        if (!result)
            throw DB::Exception(DB::ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER, "Error processing number: {}", value);

        out.write(buffer, builder.position());
    }

    template <is_integer T>
    static void format(T value, DB::WriteBuffer & out, UInt8 precision)
    {
        /// Fractional part for Integer is just trailing zeros. Let's limit it with 77 (like with Decimals).
        constexpr size_t max_digits = std::numeric_limits<UInt256>::digits10;
        if (precision > max_digits)
            throw DB::Exception(DB::ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER,
                                "Too many fractional digits requested, shall not be more than {}", max_digits);
        writeText(value, out);
        if (precision > 0) [[likely]]
        {
            writeChar('.', out);
            for (int i = 0; i < precision; ++i)
                writeChar('0', out);
        }
    }

public:
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        switch (arguments[0].type->getTypeId())
        {
            case TypeIndex::UInt8:      return executeType<UInt8>(arguments, input_rows_count);
            case TypeIndex::UInt16:     return executeType<UInt16>(arguments, input_rows_count);
            case TypeIndex::UInt32:     return executeType<UInt32>(arguments, input_rows_count);
            case TypeIndex::UInt64:     return executeType<UInt64>(arguments, input_rows_count);
            case TypeIndex::UInt128:    return executeType<UInt128>(arguments, input_rows_count);
            case TypeIndex::UInt256:    return executeType<UInt256>(arguments, input_rows_count);
            case TypeIndex::Int8:       return executeType<Int8>(arguments, input_rows_count);
            case TypeIndex::Int16:      return executeType<Int16>(arguments, input_rows_count);
            case TypeIndex::Int32:      return executeType<Int32>(arguments, input_rows_count);
            case TypeIndex::Int64:      return executeType<Int64>(arguments, input_rows_count);
            case TypeIndex::Int128:     return executeType<Int128>(arguments, input_rows_count);
            case TypeIndex::Int256:     return executeType<Int256>(arguments, input_rows_count);
            case TypeIndex::Float32:    return executeType<Float32>(arguments, input_rows_count);
            case TypeIndex::Float64:    return executeType<Float64>(arguments, input_rows_count);
            case TypeIndex::Decimal32:  return executeType<Decimal32>(arguments, input_rows_count);
            case TypeIndex::Decimal64:  return executeType<Decimal64>(arguments, input_rows_count);
            case TypeIndex::Decimal128: return executeType<Decimal128>(arguments, input_rows_count);
            case TypeIndex::Decimal256: return executeType<Decimal256>(arguments, input_rows_count);
            default:
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                                arguments[0].column->getName(), getName());
        }
    }

private:
    template <typename T>
    ColumnPtr executeType(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        const auto * precision_col = checkAndGetColumn<ColumnVector<UInt8>>(arguments[1].column.get());
        const auto * precision_col_const = checkAndGetColumnConst<ColumnVector<UInt8>>(arguments[1].column.get());

        auto result_col = ColumnString::create();
        auto * result_col_string = assert_cast<ColumnString *>(result_col.get());
        ColumnString::Chars & result_chars = result_col_string->getChars();
        ColumnString::Offsets & result_offsets = result_col_string->getOffsets();

        if constexpr (is_decimal<T>)
        {
            const auto * from_col = checkAndGetColumn<ColumnDecimal<T>>(arguments[0].column.get());

            if (from_col)
            {
                UInt8 from_scale = static_cast<UInt8>(from_col->getScale());
                if (precision_col_const)
                    vectorConstant(from_col->getData(), precision_col_const->template getValue<UInt8>(), result_chars, result_offsets, from_scale, input_rows_count);
                else if (precision_col)
                    vectorVector(from_col->getData(), precision_col->getData(), result_chars, result_offsets, from_scale, input_rows_count);
                else
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of second argument of function formatDecimal", arguments[1].column->getName());
            }
            else
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function formatDecimal", arguments[0].column->getName());
        }
        else
        {
            const auto * from_col = checkAndGetColumn<ColumnVector<T>>(arguments[0].column.get());
            if (from_col)
            {
                if (precision_col_const)
                    vectorConstant(from_col->getData(), precision_col_const->template getValue<UInt8>(), result_chars, result_offsets, input_rows_count);
                else if (precision_col)
                    vectorVector(from_col->getData(), precision_col->getData(), result_chars, result_offsets, input_rows_count);
                else
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of second argument of function formatDecimal", arguments[1].column->getName());

            }
            else
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function formatDecimal", arguments[0].column->getName());
        }

        return result_col;
    }
};

}

REGISTER_FUNCTION(ToDecimalString)
{
    FunctionDocumentation::Description description = R"(
Converts a numeric value to a String with specified number of fractional digits.

The function rounds the input value to the specified number of decimal places. If the input value has fewer fractional
digits than requested, the result is padded with zeros to achieve the exact number of fractional digits specified.
    )";
    FunctionDocumentation::Syntax syntax = R"(
toDecimalString(number, scale)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"number", "The numeric value to convert to a string. Can be any numeric type (Int, UInt, Float, Decimal).", {"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64", "Float32", "Float64", "Decimal"}},
        {"scale", "The number of digits to display in the fractional part. The result will be rounded if necessary.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a String representation of the number with exactly the specified number of fractional digits.", {"String"}};
    FunctionDocumentation::Examples examples = {
        {"Round and format a number", R"(
SELECT toDecimalString(2.1456, 2)
        )",
        R"(
┌─toDecimalString(2.1456, 2)─┐
│ 2.15                       │
└────────────────────────────┘
        )"},
        {"Pad with zeros", R"(
SELECT toDecimalString(5, 3)
        )",
        R"(
┌─toDecimalString(5, 3)─┐
│ 5.000                 │
└───────────────────────┘
        )"},
        {"Different numeric types", R"(
SELECT toDecimalString(CAST(123.456 AS Decimal(10,3)), 2) AS decimal_val,
       toDecimalString(CAST(42.7 AS Float32), 4) AS float_val
        )",
        R"(
┌─decimal_val─┬─float_val─┐
│ 123.46      │ 42.7000   │
└─────────────┴───────────┘
        )"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {23, 3};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TypeConversion;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionToDecimalString>(documentation, FunctionFactory::Case::Insensitive);
}

}
