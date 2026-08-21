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
    /// timeSeriesHistogramAvgOverGroup(histogram): the PromQL `avg` aggregation over one group's
    /// native-histogram samples (Kahan sum over the group count, incremental mean on overflow).
    class AggregateFunctionTimeSeriesHistogramAvgOverGroup final :
        public AggregateFunctionTimeSeriesHistogramOverGroupBase<AggregateFunctionTimeSeriesHistogramAvgOverGroup, TimeSeriesHistogramAvgOverGroupState>
    {
    public:
        static constexpr auto name = "timeSeriesHistogramAvgOverGroup";
        static constexpr UInt16 FORMAT_VERSION = 1;

        AggregateFunctionTimeSeriesHistogramAvgOverGroup(const DataTypes & argument_types_, const Array & parameters_)
            : AggregateFunctionTimeSeriesHistogramOverGroupBase(argument_types_, parameters_, name) {}

        String getName() const override { return name; }

        void doAdd(TimeSeriesHistogramAvgOverGroupState & state, TimeSeriesFloatHistogram histogram) const
        {
            /// The first sample of the group is copied (upstream's group initialization).
            if (!state.has_value)
            {
                state.value = std::move(histogram);
                state.has_value = true;
                state.count = 1;
                return;
            }
            /// Mirrors upstream's err == ErrHistogramsIncompatibleSchema -> group.incompatibleHistograms = true.
            if (state.value.usesCustomBuckets() != histogram.usesCustomBuckets())
            {
                state.incompatible = true;
                return;
            }

            /// Upstream increments groupCount before the aggregation proper.
            ++state.count;
            if (!state.incremental_mean)
            {
                /// Trial addition on a copy: if the running sum would overflow, switch to the incremental mean.
                TimeSeriesFloatHistogram trial_value = state.value;
                auto outcome = trial_value.kahanAdd(histogram, state.kahan_c);
                if (!timeSeriesHistogramHasOverflow(trial_value))
                {
                    state.value = std::move(trial_value);
                    state.kahan_c = std::move(outcome.updated_compensation);
                    return;
                }
                state.incremental_mean = true;
                state.mean = state.value;
                state.mean.div(state.count - 1);
                if (state.kahan_c)
                    state.kahan_c->div(state.count - 1);
            }
            /// The incremental mean update (mirrors upstream):
            /// q = (groupCount - 1) / groupCount; kahanC.Mul(q); mean.Mul(q).KahanAdd(h.Div(groupCount), kahanC).
            const Float64 q = (state.count - 1) / state.count;
            if (state.kahan_c)
                state.kahan_c->mul(q);
            histogram.div(state.count);
            state.mean.mul(q);
            state.kahan_c = state.mean.kahanAdd(histogram, std::move(state.kahan_c)).updated_compensation;
        }

        std::optional<TimeSeriesFloatHistogram> computeResult(TimeSeriesHistogramAvgOverGroupState & state) const
        {
            if (!state.has_value || state.incompatible)
                return std::nullopt;

            /// Mirrors upstream's finalization: the incremental mean (plus compensation), or the
            /// running sum divided by the group count (plus the divided compensation); then Compact(0).
            if (state.incremental_mean)
            {
                if (state.kahan_c)
                    state.mean.add(*state.kahan_c);
                state.value = std::move(state.mean);
            }
            else
            {
                state.value.div(state.count);
                if (state.kahan_c)
                {
                    state.kahan_c->div(state.count);
                    state.value.add(*state.kahan_c);
                }
            }
            state.value.compact(0);
            return std::move(state.value);
        }
    };

    AggregateFunctionPtr createAggregateFunctionTimeSeriesHistogramAvgOverGroup(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
    {
        if (settings && (*settings)[Setting::allow_experimental_time_series_aggregate_functions] == 0 && (*settings)[Setting::allow_experimental_time_series_table] == 0)
            throw Exception(
                ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
                "Aggregate function {} is experimental and disabled by default. Enable it with setting allow_experimental_time_series_aggregate_functions",
                name);

        assertNoParameters(name, parameters);
        return std::make_shared<AggregateFunctionTimeSeriesHistogramAvgOverGroup>(argument_types, parameters);
    }
}

void registerAggregateFunctionTimeSeriesHistogramAvgOverGroup(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeSeriesHistogramAvgOverGroup(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Aggregates the native-histogram samples of one group like the PromQL `avg` aggregation operator:
the histograms are accumulated with Kahan summation (see `FloatHistogram.KahanAdd` in Prometheus)
and the sum is divided by the group count at the end; when the running sum would overflow, the
aggregation switches to an incremental mean instead. Returns NULL when the group contains no
samples or when two samples have incompatible schemas (exponential vs custom buckets; PromQL drops
the group element there). A group mixing float and histogram samples is dropped by the PromQL
converter before this aggregate runs.

:::warning
This function is experimental, enable it by setting `allow_experimental_time_series_aggregate_functions=true`.
:::
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesHistogramAvgOverGroup(histogram)";
    FunctionDocumentation::Arguments arguments = {
        {"histogram", "A native histogram sample: the payload tuple of the `histograms` target table of a `TimeSeries` table.", {"Tuple"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the payload tuple of the average of the group's histograms, or NULL for an empty or schema-incompatible group.", {"Nullable(Tuple)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Example",
        R"(
SET allow_experimental_time_series_aggregate_functions = 1;
SELECT timeSeriesHistogramAvgOverGroup(h) FROM (SELECT arrayJoin([(0, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], []), (0, 0, 0., 8., 21., 0., [(0, 2)], [2., 6.], [], [], [])]::Array(Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64)))) AS h)
        )",
        R"(
┌─timeSeriesHistogramAvgOverGroup(h)────────────────────────────────────────┐
│ (0,0,0,6,15.5,0,[(0,2)],[1.5,4.5],[],[],[])                               │
└───────────────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::TimeSeries;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction("timeSeriesHistogramAvgOverGroup", {createAggregateFunctionTimeSeriesHistogramAvgOverGroup, documentation});
}

}
