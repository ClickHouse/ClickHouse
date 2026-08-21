#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeSeriesHistogramOverGroup.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Core/Settings.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_time_series_aggregate_functions;
    extern const SettingsBool allow_experimental_time_series_table;
}

namespace
{
    /// timeSeriesHistogramSumOverGroup(histogram): the PromQL `sum` aggregation over one group's
    /// native-histogram samples (Kahan accumulation, compensation added at the end, Compact(0)).
    class AggregateFunctionTimeSeriesHistogramSumOverGroup final :
        public AggregateFunctionTimeSeriesHistogramOverGroupBase<AggregateFunctionTimeSeriesHistogramSumOverGroup, TimeSeriesHistogramOverGroupState>
    {
    public:
        static constexpr auto name = "timeSeriesHistogramSumOverGroup";
        static constexpr UInt16 FORMAT_VERSION = 1;

        AggregateFunctionTimeSeriesHistogramSumOverGroup(const DataTypes & argument_types_, const Array & parameters_)
            : AggregateFunctionTimeSeriesHistogramOverGroupBase(argument_types_, parameters_, name) {}

        String getName() const override { return name; }

        void doAdd(TimeSeriesHistogramOverGroupState & state, TimeSeriesFloatHistogram histogram) const
        {
            /// The first sample of the group is copied (upstream's group initialization).
            if (!state.has_value)
            {
                state.value = std::move(histogram);
                state.has_value = true;
                return;
            }
            /// Mirrors upstream: group.histogramKahanC = group.histogramValue.KahanAdd(h, group.histogramKahanC)
            /// with err == ErrHistogramsIncompatibleSchema -> group.incompatibleHistograms = true.
            if (state.value.usesCustomBuckets() != histogram.usesCustomBuckets())
            {
                state.incompatible = true;
                return;
            }
            state.kahan_c = state.value.kahanAdd(histogram, std::move(state.kahan_c)).updated_compensation;
        }

        std::optional<TimeSeriesFloatHistogram> computeResult(TimeSeriesHistogramOverGroupState & state) const
        {
            if (!state.has_value || state.incompatible)
                return std::nullopt;

            /// Mirrors upstream's finalization: add the compensation (cannot fail after the KahanAdds), then Compact(0).
            if (state.kahan_c)
                state.value.add(*state.kahan_c);
            state.value.compact(0);
            return std::move(state.value);
        }
    };

    AggregateFunctionPtr createAggregateFunctionTimeSeriesHistogramSumOverGroup(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
    {
        if (settings && (*settings)[Setting::allow_experimental_time_series_aggregate_functions] == 0 && (*settings)[Setting::allow_experimental_time_series_table] == 0)
            throw Exception(
                ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
                "Aggregate function {} is experimental and disabled by default. Enable it with setting allow_experimental_time_series_aggregate_functions",
                name);

        assertNoParameters(name, parameters);
        return std::make_shared<AggregateFunctionTimeSeriesHistogramSumOverGroup>(argument_types, parameters);
    }
}

void registerAggregateFunctionTimeSeriesHistogramSumOverGroup(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeSeriesHistogramSumOverGroup(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Aggregates the native-histogram samples of one group like the PromQL `sum` aggregation operator:
the histograms are accumulated with Kahan summation (see `FloatHistogram.KahanAdd` in Prometheus),
the compensation is added at the end and the result is `Compact(0)`-ed. Returns NULL when the group
contains no samples or when two samples have incompatible schemas (exponential vs custom buckets;
PromQL drops the group element there). A group mixing float and histogram samples is dropped by the
PromQL converter before this aggregate runs.

:::warning
This function is experimental, enable it by setting `allow_experimental_time_series_aggregate_functions=true`.
:::
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesHistogramSumOverGroup(histogram)";
    FunctionDocumentation::Arguments arguments = {
        {"histogram", "A native histogram sample: the payload tuple of the `histograms` target table of a `TimeSeries` table.", {"Tuple"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the payload tuple of the sum of the group's histograms, or NULL for an empty or schema-incompatible group.", {"Nullable(Tuple)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SET allow_experimental_time_series_aggregate_functions = 1;
SELECT timeSeriesHistogramSumOverGroup(h) FROM (SELECT arrayJoin([(0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], []), (0, 0, 0., 8., 21., 0., [(0, 2)], [2., 6.], [], [], [])]::Array(Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)))) AS h)
        )",
        R"(
┌─timeSeriesHistogramSumOverGroup(h)────────────────────────────────────────┐
│ (0,0,0,12,31,0,[(0,2)],[3,9],[],[],[])                                    │
└───────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction("timeSeriesHistogramSumOverGroup", {createAggregateFunctionTimeSeriesHistogramSumOverGroup, documentation});
}

}
