#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <span>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}


namespace
{

/// The suffixes each Prometheus metric type appends to its family name.
std::span<const std::string_view> suffixesForType(std::string_view type)
{
    /// The empty suffix for `counter` and `info` supports the classic exposition format, where the family name
    /// already includes the type suffix (e.g. a counter family `http_requests_total` with a series of the same
    /// name), while the OpenMetrics format appends the suffix to the family name (`http_requests` + `_total`).
    static constexpr std::string_view counter[] = {"", "_total"};
    static constexpr std::string_view histogram[] = {"_bucket", "_count", "_sum"};
    static constexpr std::string_view gauge_histogram[] = {"_bucket", "_gcount", "_gsum"};
    static constexpr std::string_view summary[] = {"", "_count", "_sum"};
    static constexpr std::string_view info[] = {"", "_info"};
    static constexpr std::string_view empty[] = {""};

    if (type == "counter")
        return counter;
    if (type == "histogram")
        return histogram;
    if (type == "gaugehistogram")
        return gauge_histogram;
    if (type == "summary")
        return summary;
    if (type == "info")
        return info;
    return empty;
}

/// Function timeSeriesMetricTypeToSuffixes(type) returns the metric-name suffixes
/// for a metric family of the given Prometheus type.
class FunctionTimeSeriesMetricTypeToSuffixes final : public IFunction
{
public:
    static constexpr auto name = "timeSeriesMetricTypeToSuffixes";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesMetricTypeToSuffixes>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"type", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}
        };
        validateFunctionArguments(*this, arguments, mandatory_args);
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        const auto * col_type = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!col_type)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of argument of function {}",
                arguments[0].column->getName(), getName());

        auto col_suffixes = ColumnString::create();
        auto col_offsets = ColumnArray::ColumnOffsets::create();
        col_offsets->reserve(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view type = col_type->getDataAt(i);
            for (auto suffix : suffixesForType(type))
                col_suffixes->insertData(suffix.data(), suffix.size());
            col_offsets->insertValue(col_suffixes->size());
        }

        return ColumnArray::create(std::move(col_suffixes), std::move(col_offsets));
    }
};

}


REGISTER_FUNCTION(TimeSeriesMetricTypeToSuffixes)
{
    FunctionDocumentation::Description description = R"(
Returns the metric-name suffixes for a metric family of the given Prometheus type.
)";
    FunctionDocumentation::Syntax syntax = "timeSeriesMetricTypeToSuffixes(type)";
    FunctionDocumentation::Arguments arguments = {
        {"type", "Prometheus metric type (`counter`, `gauge`, `histogram`, `summary`, ...).", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the suffixes for a family of that type.", {"Array(String)"}
    };
    FunctionDocumentation::Examples examples = {
        {
            "Example",
            R"(
SELECT timeSeriesMetricTypeToSuffixes('histogram') AS suffixes;
            )",
            R"(
┌─suffixes────────────────────┐
│ ['_bucket','_count','_sum'] │
└─────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesMetricTypeToSuffixes>(documentation);
}

}
