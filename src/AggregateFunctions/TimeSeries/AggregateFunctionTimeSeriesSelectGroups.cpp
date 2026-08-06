#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeSeriesSelectGroups.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_time_series_aggregate_functions;
    extern const SettingsBool allow_experimental_time_series_table;
}


namespace
{
    template <TimeSeriesSelectGroupsKind kind>
    AggregateFunctionPtr createAggregateFunctionTimeSeriesSelectGroups(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
    {
        if (settings && (*settings)[Setting::allow_experimental_time_series_aggregate_functions] == 0
            && (*settings)[Setting::allow_experimental_time_series_table] == 0)
            throw Exception(
                ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
                "Aggregate function {} is experimental and disabled by default. "
                "Enable it with setting allow_experimental_time_series_aggregate_functions",
                name);

        assertNoParameters(name, parameters);

        const size_t expected_num_arguments = (kind == TimeSeriesSelectGroupsKind::LimitK) ? 4 : 3;
        if (argument_types.size() != expected_num_arguments)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Aggregate function {} requires {} arguments: {}(group, values, k{})",
                            name, expected_num_arguments, name,
                            (kind == TimeSeriesSelectGroupsKind::LimitK) ? ", sampling_key" : "");

        if (!isUInt64(argument_types[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Illegal type {} of 1st argument (group) for aggregate function {}, expected UInt64",
                            argument_types[0]->getName(), name);

        const auto * values_type = typeid_cast<const DataTypeArray *>(argument_types[1].get());
        const DataTypePtr value_type = values_type ? removeNullable(values_type->getNestedType()) : nullptr;
        if (!value_type || !isNativeFloat(value_type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Illegal type {} of 2nd argument (values) for aggregate function {}, "
                            "expected an array of Float32, Float64, Nullable(Float32) or Nullable(Float64)",
                            argument_types[1]->getName(), name);

        const auto * k_array_type = typeid_cast<const DataTypeArray *>(argument_types[2].get());
        const DataTypePtr k_type = k_array_type ? k_array_type->getNestedType() : argument_types[2];
        if (!isNativeUInt(k_type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Illegal type {} of 3rd argument (k) for aggregate function {}, "
                            "expected an unsigned integer or an array of unsigned integers",
                            argument_types[2]->getName(), name);

        if constexpr (kind == TimeSeriesSelectGroupsKind::LimitK)
        {
            if (!isUInt64(argument_types[3]))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Illegal type {} of 4th argument (sampling_key) for aggregate function {}, expected UInt64",
                                argument_types[3]->getName(), name);
        }

        if (value_type->getTypeId() == TypeIndex::Float64)
            return std::make_shared<AggregateFunctionTimeSeriesSelectGroups<kind, Float64>>(argument_types);
        else
            return std::make_shared<AggregateFunctionTimeSeriesSelectGroups<kind, Float32>>(argument_types);
    }

    FunctionDocumentation::ReturnedValue getReturnedValueDocumentation()
    {
        return {"Returns the selected series in the order of ascending `group`, each together with its per-step mask: "
                "`steps_mask[t] = 1` if the series is selected at time step `t`. "
                "Series which are selected at no time step are not returned.",
                {"Array(Tuple(group UInt64, steps_mask Array(UInt8)))"}};
    }
}

void registerAggregateFunctionTimeSeriesSelectGroups(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeSeriesSelectGroups(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description_topk = R"(
Selects the time series with the k greatest values at each time step of a time grid.

Each input row is one time series: `group` identifies the series and `values` contains its values aligned to a common
time grid, so the `values` arrays of all rows must have the same size. At each time step, the series with the k greatest
non-NULL values at that step are selected (NaN is considered smaller than any other value). Value ties are broken by
preferring the series with the smaller `group`.

This function implements the `topk()` aggregation operator of PromQL and keeps only one bounded heap of size `k` per
time step, so its state size does not depend on the number of aggregated series.

:::note
This function is experimental, enable it by setting `allow_experimental_time_series_aggregate_functions = 1`.
:::
    )";
    FunctionDocumentation::Syntax syntax_topk = R"(
timeSeriesSelectTopKGroups(group, values, k)
    )";
    FunctionDocumentation::Arguments arguments_topk = {
        {"group", "Identifier of the time series.", {"UInt64"}},
        {"values", "Values of the time series aligned to the time grid, one element per time step.", {"Array(Nullable(Float32))", "Array(Nullable(Float64))", "Array(Float32)", "Array(Float64)"}},
        {"k", "How many series to select at each time step, either one value for all time steps or an array with one value per time step. Must be the same for all rows.", {"UInt*", "Array(UInt*)"}},
    };
    FunctionDocumentation::Parameters parameters = {};
    FunctionDocumentation::ReturnedValue returned_value = getReturnedValueDocumentation();
    FunctionDocumentation::Examples examples_topk = {
    {
        "Selecting the 2 greatest series per time step",
        R"(
SET allow_experimental_time_series_aggregate_functions = 1;
WITH [(1, [10., 1., NULL]), (2, [20., 2., 2.]), (3, [30., NULL, 1.])]::Array(Tuple(UInt64, Array(Nullable(Float64)))) AS series
SELECT timeSeriesSelectTopKGroups(s.1, s.2, 2)
FROM (SELECT arrayJoin(series) AS s);
        )",
        R"(
[(1,[0,1,0]),(2,[1,1,1]),(3,[1,0,1])]
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 8};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_topk = {description_topk, syntax_topk, arguments_topk, parameters, returned_value, examples_topk, introduced_in, category};

    factory.registerFunction("timeSeriesSelectTopKGroups",
        {createAggregateFunctionTimeSeriesSelectGroups<TimeSeriesSelectGroupsKind::TopK>, documentation_topk});

    FunctionDocumentation::Description description_bottomk = R"(
Selects the time series with the k smallest values at each time step of a time grid.

Each input row is one time series: `group` identifies the series and `values` contains its values aligned to a common
time grid, so the `values` arrays of all rows must have the same size. At each time step, the series with the k smallest
non-NULL values at that step are selected (NaN is considered greater than any other value). Value ties are broken by
preferring the series with the smaller `group`.

This function implements the `bottomk()` aggregation operator of PromQL and keeps only one bounded heap of size `k` per
time step, so its state size does not depend on the number of aggregated series.

:::note
This function is experimental, enable it by setting `allow_experimental_time_series_aggregate_functions = 1`.
:::
    )";
    FunctionDocumentation::Syntax syntax_bottomk = R"(
timeSeriesSelectBottomKGroups(group, values, k)
    )";
    FunctionDocumentation::Examples examples_bottomk = {
    {
        "Selecting the 2 smallest series per time step",
        R"(
SET allow_experimental_time_series_aggregate_functions = 1;
WITH [(1, [10., 1., NULL]), (2, [20., 2., 2.]), (3, [30., NULL, 1.])]::Array(Tuple(UInt64, Array(Nullable(Float64)))) AS series
SELECT timeSeriesSelectBottomKGroups(s.1, s.2, 2)
FROM (SELECT arrayJoin(series) AS s);
        )",
        R"(
[(1,[1,1,0]),(2,[1,1,1]),(3,[0,0,1])]
        )"
    }
    };
    FunctionDocumentation documentation_bottomk = {description_bottomk, syntax_bottomk, arguments_topk, parameters, returned_value, examples_bottomk, introduced_in, category};

    factory.registerFunction("timeSeriesSelectBottomKGroups",
        {createAggregateFunctionTimeSeriesSelectGroups<TimeSeriesSelectGroupsKind::BottomK>, documentation_bottomk});

    FunctionDocumentation::Description description_limitk = R"(
Selects up to k time series at each time step of a time grid, in a deterministic pseudo-random fashion.

Each input row is one time series: `group` identifies the series, `values` contains its values aligned to a common time
grid (so the `values` arrays of all rows must have the same size), and `sampling_key` is a per-series hash used as the
selection order. At each time step, the series with the k smallest sampling keys among the series with non-NULL values
at that step are selected. Sampling key ties are broken by preferring the series with the smaller `group`.

This function implements the `limitk()` aggregation operator of PromQL and keeps only one bounded heap of size `k` per
time step, so its state size does not depend on the number of aggregated series.

:::note
This function is experimental, enable it by setting `allow_experimental_time_series_aggregate_functions = 1`.
:::
    )";
    FunctionDocumentation::Syntax syntax_limitk = R"(
timeSeriesSelectLimitKGroups(group, values, k, sampling_key)
    )";
    FunctionDocumentation::Arguments arguments_limitk = {
        {"group", "Identifier of the time series.", {"UInt64"}},
        {"values", "Values of the time series aligned to the time grid, one element per time step.", {"Array(Nullable(Float32))", "Array(Nullable(Float64))", "Array(Float32)", "Array(Float64)"}},
        {"k", "How many series to select at each time step, either one value for all time steps or an array with one value per time step. Must be the same for all rows.", {"UInt*", "Array(UInt*)"}},
        {"sampling_key", "A per-series hash defining the selection order, e.g. `timeSeriesGroupToSamplingKey(group)`.", {"UInt64"}},
    };
    FunctionDocumentation::Examples examples_limitk = {
    {
        "Selecting 2 series per time step by the smallest sampling keys",
        R"(
SET allow_experimental_time_series_aggregate_functions = 1;
WITH [(1, [10., 1., NULL], 300), (2, [20., 2., 2.], 100), (3, [30., NULL, 1.], 200)]::Array(Tuple(UInt64, Array(Nullable(Float64)), UInt64)) AS series
SELECT timeSeriesSelectLimitKGroups(s.1, s.2, 2, s.3)
FROM (SELECT arrayJoin(series) AS s);
        )",
        R"(
[(1,[0,1,0]),(2,[1,1,1]),(3,[1,0,1])]
        )"
    }
    };
    FunctionDocumentation documentation_limitk = {description_limitk, syntax_limitk, arguments_limitk, parameters, returned_value, examples_limitk, introduced_in, category};

    factory.registerFunction("timeSeriesSelectLimitKGroups",
        {createAggregateFunctionTimeSeriesSelectGroups<TimeSeriesSelectGroupsKind::LimitK>, documentation_limitk});
}

}
