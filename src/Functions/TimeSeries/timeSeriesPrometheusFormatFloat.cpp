#include <Functions/FunctionFactory.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <array>
#include <charconv>
#include <cmath>
#include <system_error>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
    String formatPrometheusFloat(Float64 value)
    {
        if (std::isnan(value))
            return "NaN";
        if (std::isinf(value))
            return (value > 0) ? "+Inf" : "-Inf";

        std::array<char, 2048> buffer;
        auto [ptr, ec] = std::to_chars(buffer.data(), buffer.data() + buffer.size(), value, std::chars_format::fixed);
        if (ec != std::errc{})
            throw Exception(ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER, "Cannot print float or double number");

        return {buffer.data(), ptr};
    }

    template <typename T>
    bool executeNumber(const IColumn & column, ColumnString & result)
    {
        const auto * numbers = checkAndGetColumn<ColumnVector<T>>(&column);
        if (!numbers)
            return false;

        for (const auto value : numbers->getData())
        {
            String formatted = formatPrometheusFloat(static_cast<Float64>(value));
            result.insertData(formatted.data(), formatted.size());
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

        if (!isFloat(arguments[0].type))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument of function {} must be a floating-point number (passed: {})",
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
    FunctionDocumentation::IntroducedIn introduced_in = {26, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesPrometheusFormatFloat>(documentation);
}

}
