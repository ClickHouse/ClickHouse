#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <IO/DoubleConverter.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/// Formats a floating-point value like Go's strconv.FormatFloat(value, 'f', -1, 64),
/// which is the representation Prometheus uses for sample values stored in labels.
class FunctionTimeSeriesPrometheusValueToString final : public IFunction
{
public:
    static constexpr auto name = "timeSeriesPrometheusValueToString";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesPrometheusValueToString>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /* arguments */) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const WhichDataType which(arguments[0]);
        if (!which.isFloat32() && !which.isFloat64())
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument #1 of function {} has wrong type {}, expected Float32 or Float64",
                name,
                arguments[0]->getName());
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto result = ColumnString::create();
        result->reserve(input_rows_count);

        const WhichDataType which(arguments[0].type);
        if (which.isFloat32())
            execute<ColumnFloat32>(*arguments[0].column, *result);
        else if (which.isFloat64())
            execute<ColumnFloat64>(*arguments[0].column, *result);
        else
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), name);

        return result;
    }

private:
    /// A fixed representation of any Float64 needs at most 327 bytes:
    /// "-0.", 323 leading zeroes, and the shortest significant digits.
    static constexpr size_t max_formatted_size = 512;

    template <typename ColumnType>
    static void execute(const IColumn & column, ColumnString & result)
    {
        const auto * values = checkAndGetColumn<ColumnType>(&column);
        if (!values)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", column.getName(), name);

        std::array<char, max_formatted_size> formatted{};
        for (const auto value : values->getData())
        {
            const size_t length = format(static_cast<Float64>(value), formatted.data());
            result.insertData(formatted.data(), length);
        }
    }

    static size_t format(Float64 value, char * output)
    {
        if (std::isnan(value))
        {
            std::memcpy(output, "NaN", 3);
            return 3;
        }

        if (std::isinf(value))
        {
            if (std::signbit(value))
            {
                std::memcpy(output, "-Inf", 4);
                return 4;
            }

            std::memcpy(output, "+Inf", 4);
            return 4;
        }

        using Converter = double_conversion::DoubleToStringConverter;
        std::array<char, Converter::kBase10MaximalLength + 1> digits{};
        bool negative = false;
        int digits_length = 0;
        int decimal_point = 0;
        Converter::DoubleToAscii(
            value, Converter::SHORTEST, 0, digits.data(), static_cast<int>(digits.size()), &negative, &digits_length, &decimal_point);

        size_t output_length = 0;
        if (negative)
            output[output_length++] = '-';

        if (decimal_point <= 0)
        {
            output[output_length++] = '0';
            output[output_length++] = '.';
            std::fill_n(output + output_length, static_cast<size_t>(-decimal_point), '0');
            output_length += static_cast<size_t>(-decimal_point);
            std::memcpy(output + output_length, digits.data(), static_cast<size_t>(digits_length));
            output_length += static_cast<size_t>(digits_length);
        }
        else if (decimal_point >= digits_length)
        {
            std::memcpy(output + output_length, digits.data(), static_cast<size_t>(digits_length));
            output_length += static_cast<size_t>(digits_length);
            std::fill_n(output + output_length, static_cast<size_t>(decimal_point - digits_length), '0');
            output_length += static_cast<size_t>(decimal_point - digits_length);
        }
        else
        {
            std::memcpy(output + output_length, digits.data(), static_cast<size_t>(decimal_point));
            output_length += static_cast<size_t>(decimal_point);
            output[output_length++] = '.';
            std::memcpy(output + output_length, digits.data() + decimal_point, static_cast<size_t>(digits_length - decimal_point));
            output_length += static_cast<size_t>(digits_length - decimal_point);
        }

        chassert(output_length <= max_formatted_size);
        return output_length;
    }
};


REGISTER_FUNCTION(TimeSeriesPrometheusValueToString)
{
    FunctionDocumentation::Description description = R"(
Formats a floating-point sample value using Prometheus label-value semantics.

Finite values use fixed notation with the shortest decimal representation that round-trips to the same `Float64`.
Negative zero is preserved. Special values are returned as `NaN`, `+Inf`, and `-Inf`.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesPrometheusValueToString(value)";
    FunctionDocumentation::Arguments arguments = {{"value", "A floating-point sample value.", {"Float32", "Float64"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"The Prometheus-compatible string representation of `value`.", {"String"}};
    FunctionDocumentation::Examples examples
        = {{"Fixed notation",
            R"(
SELECT timeSeriesPrometheusValueToString(toFloat64(1e-7))
        )",
            R"(
┌─timeSeriesPrometheusValueToString(toFloat64(1e-7))─┐
│ 0.0000001                                          │
└────────────────────────────────────────────────────┘
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesPrometheusValueToString>(documentation);
}

}
