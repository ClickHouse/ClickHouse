#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}


namespace
{

/// Function timeSeriesMetricNameToFamily(metric_name) returns the Prometheus metric family name.
/// The function strips the canonical suffix `_bucket`, `_count`, `_total`, or `_sum` if present;
/// or returns the unchanged input if no canonical suffix is found.
class FunctionTimeSeriesMetricNameToFamily final : public IFunction
{
public:
    static constexpr auto name = "timeSeriesMetricNameToFamily";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeSeriesMetricNameToFamily>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"metric_name", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"}
        };
        validateFunctionArguments(*this, arguments, mandatory_args);
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        const auto * col_input = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!col_input)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of argument of function {}",
                arguments[0].column->getName(), getName());

        /// Canonical Prometheus suffixes for histogram/summary/counter series.
        static constexpr std::string_view suffixes[] = {"_bucket", "_count", "_total", "_sum"};

        auto col_res = ColumnString::create();
        col_res->reserve(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            std::string_view metric_name = col_input->getDataAt(i);
            std::string_view metric_family = metric_name;

            for (auto suffix : suffixes)
            {
                /// Don't strip a suffix that would leave the metric family name empty.
                if (metric_name.size() > suffix.size() && metric_name.ends_with(suffix))
                {
                    metric_family = metric_name.substr(0, metric_name.size() - suffix.size());
                    break;
                }
            }
            col_res->insertData(metric_family.data(), metric_family.size());
        }

        return col_res;
    }
};

}


REGISTER_FUNCTION(TimeSeriesMetricNameToFamily)
{
    FunctionDocumentation::Description description = R"(
Returns the Prometheus metric family name.
The function strips the canonical suffix `_bucket`, `_count`, `_total`, or `_sum` if present;
or returns the unchanged input if no canonical suffix is found.

The function reflects the relationship between an individual time series (e.g. a histogram bucket
`http_request_duration_bucket`) and its containing metric family (`http_request_duration`).
)";
    FunctionDocumentation::Syntax syntax = "timeSeriesMetricNameToFamily(metric_name)";
    FunctionDocumentation::Arguments arguments = {
        {"metric_name", "Prometheus metric name.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the metric family name.", {"String"}
    };
    FunctionDocumentation::Examples examples = {
        {
            "Example",
            R"(
SELECT timeSeriesMetricNameToFamily('http_requests_total') AS family;
            )",
            R"(
┌─family────────┐
│ http_requests │
└───────────────┘
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionTimeSeriesMetricNameToFamily>(documentation);
}

}
