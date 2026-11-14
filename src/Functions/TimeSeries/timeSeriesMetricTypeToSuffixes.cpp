#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
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
std::span<const std::string_view> suffixesForType(std::string_view type, bool native_histograms)
{
    /// The empty suffix for `counter` and `info` supports the classic exposition format, where the family name
    /// already includes the type suffix (e.g. a counter family `http_requests_total` with a series of the same
    /// name), while the OpenMetrics format appends the suffix to the family name (`http_requests` + `_total`).
    static constexpr std::string_view counter[] = {"", "_total"};
    /// A native histogram is one series named like its family, so the empty suffix is included with `native_histograms`;
    /// a classic histogram has the suffixed members only.
    static constexpr std::string_view histogram[] = {"", "_bucket", "_count", "_sum"};
    static constexpr std::string_view gauge_histogram[] = {"", "_bucket", "_gcount", "_gsum"};
    static constexpr std::string_view summary[] = {"", "_count", "_sum"};
    static constexpr std::string_view info[] = {"", "_info"};
    static constexpr std::string_view empty[] = {""};

    const size_t histogram_offset = native_histograms ? 0 : 1;
    if (type == "counter")
        return counter;
    if (type == "histogram")
        return std::span{histogram}.subspan(histogram_offset);
    if (type == "gaugehistogram")
        return std::span{gauge_histogram}.subspan(histogram_offset);
    if (type == "summary")
        return summary;
    if (type == "info")
        return info;
    return empty;
}

/// Function timeSeriesMetricTypeToSuffixes(type[, native_histograms]) returns the metric-name suffixes for a metric family
/// of the given Prometheus type. With `native_histograms` the histogram types include the empty suffix (see suffixesForType).
class FunctionTimeSeriesMetricTypeToSuffixes final : public IFunction
{
public:
    static constexpr auto name = "timeSeriesMetricTypeToSuffixes";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesMetricTypeToSuffixes>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"type", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}
        };
        FunctionArgumentDescriptors optional_args{
            {"native_histograms", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUInt8), isColumnConst, "const Bool"}
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        const auto * col_type = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!col_type)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of argument of function {}",
                arguments[0].column->getName(), getName());

        bool native_histograms = false;
        if (arguments.size() > 1)
        {
            if (!isColumnConst(*arguments[1].column))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                    "Argument #2 (native_histograms) of function {} must be a constant, got {}",
                    getName(), arguments[1].column->getName());
            native_histograms = arguments[1].column->getBool(0);
        }

        auto col_suffixes = ColumnString::create();
        auto col_offsets = ColumnArray::ColumnOffsets::create();
        col_offsets->reserve(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view type = col_type->getDataAt(i);
            for (auto suffix : suffixesForType(type, native_histograms))
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
With `native_histograms` the types `histogram` and `gaugehistogram` also get the empty suffix: a native histogram is one series
named like its family, while the other suffixes match the members of a classic histogram.
)";
    FunctionDocumentation::Syntax syntax = "timeSeriesMetricTypeToSuffixes(type[, native_histograms])";
    FunctionDocumentation::Arguments arguments = {
        {"type", "Prometheus metric type (`counter`, `gauge`, `histogram`, `summary`, ...).", {"String"}},
        {"native_histograms", "Optional. Whether the histogram types include the empty suffix of a native histogram. `false` by default.", {"const Bool"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the suffixes for a family of that type.", {"Array(String)"}
    };
    FunctionDocumentation::Examples examples = {
        {
            "The members of a classic histogram",
            R"(
SELECT timeSeriesMetricTypeToSuffixes('histogram') AS suffixes;
            )",
            R"(
┌─suffixes────────────────────┐
│ ['_bucket','_count','_sum'] │
└─────────────────────────────┘
            )"
        },
        {
            "The members of a classic histogram and the native histogram",
            R"(
SELECT timeSeriesMetricTypeToSuffixes('histogram', true) AS suffixes;
            )",
            R"(
┌─suffixes───────────────────────┐
│ ['','_bucket','_count','_sum'] │
└────────────────────────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesMetricTypeToSuffixes>(documentation);
}

}
