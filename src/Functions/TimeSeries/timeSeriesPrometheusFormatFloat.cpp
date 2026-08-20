#include <Functions/FunctionFactory.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/WhichDataType.h>
#include <Functions/FunctionHelpers.h>
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
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
    /// A fixed representation of any Float64 needs at most 327 bytes:
    /// "-0.", 323 leading zeroes, and the shortest significant digits.
    constexpr size_t max_formatted_size = 512;

    size_t formatPrometheusFloat(Float64 value, char * output)
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

    template <typename T>
    bool executeNumber(const IColumn & column, ColumnString & result)
    {
        const auto * numbers = checkAndGetColumn<ColumnVector<T>>(&column);
        if (!numbers)
            return false;

        std::array<char, max_formatted_size> formatted{};
        for (const auto value : numbers->getData())
        {
            const size_t length = formatPrometheusFloat(static_cast<Float64>(value), formatted.data());
            result.insertData(formatted.data(), length);
        }
        return true;
    }
}

/// Function timeSeriesPrometheusFormatFloat(value) formats a floating-point value the way Prometheus count_values() formats labels.
class FunctionTimeSeriesPrometheusFormatFloat : public IFunction
{
public:
    static constexpr auto name = "timeSeriesPrometheusFormatFloat";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesPrometheusFormatFloat>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        checkArgumentTypes(arguments);
        return std::make_shared<DataTypeString>();
    }

    static void checkArgumentTypes(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.size() != 1)
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} must be called with one argument: {}(value)",
                name,
                name);
        }

        const WhichDataType which(arguments[0].type);
        if (!which.isFloat32() && !which.isFloat64())
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument #1 of function {} has wrong type {}, expected Float32 or Float64",
                name,
                arguments[0].type->getName());
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto column = arguments[0].column->convertToFullColumnIfConst();

        auto result = ColumnString::create();
        result->reserve(input_rows_count);

        if (executeNumber<Float64>(*column, *result) || executeNumber<Float32>(*column, *result))
            return result;

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of argument of function {}, it must be Float32 or Float64",
            column->getName(),
            getName());
    }
};

REGISTER_FUNCTION(TimeSeriesPrometheusFormatFloat)
{
    FunctionDocumentation::Description description = R"(
Formats a floating-point value using Prometheus label formatting for `count_values()`.
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesPrometheusFormatFloat(value)";
    FunctionDocumentation::Arguments arguments = {
        {"value", "A floating-point value.", {"Float32", "Float64"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the Prometheus label representation of the value.", {"String"}};
    FunctionDocumentation::Examples examples = {
        {
            "Example",
            "SELECT timeSeriesPrometheusFormatFloat(toFloat64(0.0000001)), timeSeriesPrometheusFormatFloat(toFloat64('inf'))",
            "0.0000001\t+Inf",
        },
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesPrometheusFormatFloat>(documentation);
}

}
