#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <cmath>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
}

namespace Setting
{
    extern const SettingsBool enable_time_series_aggregate_functions;
    extern const SettingsBool enable_time_series_table;
}

namespace
{

/// Kahan-Babuska-Neumaier summation step, the same as `kahansum.Inc` in Prometheus.
void kahanAdd(Float64 x, Float64 & sum, Float64 & compensation)
{
    const Float64 t = sum + x;
    if (std::isinf(t))
        compensation = 0;
    else if (std::abs(sum) >= std::abs(x))
        compensation += (sum - t) + x;
    else
        compensation += (x - t) + sum;
    sum = t;
}

/// The average as PromQL `avg` computes it: a compensated sum that turns into a compensated mean once the sum would overflow.
struct TimeSeriesAvgOverGroupData
{
    /// The sum of the values, or their mean once `is_mean` is set.
    Float64 value = 0;
    Float64 compensation = 0;
    UInt64 count = 0;
    bool is_mean = false;

    void add(Float64 x)
    {
        ++count;
        if (count == 1)
        {
            value = x;
            return;
        }

        if (!is_mean)
        {
            Float64 new_value = value;
            Float64 new_compensation = compensation;
            kahanAdd(x, new_value, new_compensation);
            if (!std::isinf(new_value))
            {
                value = new_value;
                compensation = new_compensation;
                return;
            }
            is_mean = true;
            value /= static_cast<Float64>(count - 1);
            compensation /= static_cast<Float64>(count - 1);
        }

        const Float64 n = static_cast<Float64>(count);
        value *= (n - 1) / n;
        compensation *= (n - 1) / n;
        kahanAdd(x / n, value, compensation);
    }

    void merge(const TimeSeriesAvgOverGroupData & rhs)
    {
        if (rhs.count == 0)
            return;
        if (count == 0)
        {
            *this = rhs;
            return;
        }

        if (!is_mean && !rhs.is_mean)
        {
            Float64 new_value = value;
            Float64 new_compensation = compensation;
            kahanAdd(rhs.value, new_value, new_compensation);
            if (!std::isinf(new_value))
            {
                value = new_value;
                compensation = new_compensation + rhs.compensation;
                count += rhs.count;
                return;
            }
        }

        /// Add the means weighted by their counts, which cannot overflow.
        const Float64 total = static_cast<Float64>(count + rhs.count);
        const Float64 weight = (is_mean ? static_cast<Float64>(count) : 1) / total;
        const Float64 rhs_weight = (rhs.is_mean ? static_cast<Float64>(rhs.count) : 1) / total;
        value *= weight;
        compensation *= weight;
        kahanAdd(rhs.value * rhs_weight, value, compensation);
        compensation += rhs.compensation * rhs_weight;
        count += rhs.count;
        is_mean = true;
    }

    Float64 get() const
    {
        if (is_mean)
            return value + compensation;
        const Float64 n = static_cast<Float64>(count);
        return value / n + compensation / n;
    }
};

class AggregateFunctionTimeSeriesAvgOverGroup final
    : public IAggregateFunctionDataHelper<TimeSeriesAvgOverGroupData, AggregateFunctionTimeSeriesAvgOverGroup>
{
public:
    explicit AggregateFunctionTimeSeriesAvgOverGroup(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<TimeSeriesAvgOverGroupData, AggregateFunctionTimeSeriesAvgOverGroup>(
            argument_types_, {}, std::make_shared<DataTypeFloat64>())
    {
    }

    String getName() const override { return "timeSeriesAvgOverGroup"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        data(place).add(assert_cast<const ColumnFloat64 &>(*columns[0]).getData()[row_num]);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).merge(data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & state = data(place);
        writeBinaryLittleEndian(state.value, buf);
        writeBinaryLittleEndian(state.compensation, buf);
        writeVarUInt(state.count, buf);
        writeBinary(state.is_mean, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        auto & state = data(place);
        readBinaryLittleEndian(state.value, buf);
        readBinaryLittleEndian(state.compensation, buf);
        readVarUInt(state.count, buf);
        readBinary(state.is_mean, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnFloat64 &>(to).getData().push_back(data(place).get());
    }
};

AggregateFunctionPtr createAggregateFunctionTimeSeriesAvgOverGroup(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    if (settings && (*settings)[Setting::enable_time_series_aggregate_functions] == 0
        && (*settings)[Setting::enable_time_series_table] == 0)
        throw Exception(
            ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
            "Aggregate function {} is in private preview and disabled by default. "
            "Enable it with setting enable_time_series_aggregate_functions",
            name);

    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    if (!WhichDataType(argument_types[0]).isFloat64())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}, expected Float64",
                        argument_types[0]->getName(), name);
    return std::make_shared<AggregateFunctionTimeSeriesAvgOverGroup>(argument_types);
}

}

void registerAggregateFunctionTimeSeriesAvgOverGroup(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeSeriesAvgOverGroup(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Calculates the arithmetic mean of the values.

The values are summed with the Kahan-Babuska-Neumaier compensated summation algorithm.
If the sum would overflow, the function continues with an incremental mean, so the average of large finite values stays finite
where [`avg`](/reference/functions/aggregate-functions/avg) returns `inf`. The function is slower than `avg`.

This function implements the `avg()` aggregation operator of PromQL.

<Note>
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions = 1`.
</Note>
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesAvgOverGroup(x)";
    FunctionDocumentation::Arguments arguments = {{"x", "Input values.", {"Float64"}}};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the arithmetic mean, or `NaN` if the input is empty.", {"Float64"}};
    FunctionDocumentation::Examples examples = {
    {
        "The sum overflows",
        R"(
SET enable_time_series_aggregate_functions = 1;
SELECT avg(x), timeSeriesAvgOverGroup(x) FROM values('x Float64', 1e308, 1e308);
        )",
        R"(
┌─avg(x)─┬─timeSeriesAvgOverGroup(x)─┐
│    inf │                     1e308 │
└────────┴───────────────────────────┘
        )"
    },
    {
        "Compensated summation",
        R"(
SET enable_time_series_aggregate_functions = 1;
SELECT avg(x), timeSeriesAvgOverGroup(x) FROM values('x Float64', 1, 1e100, 1, -1e100);
        )",
        R"(
┌─avg(x)─┬─timeSeriesAvgOverGroup(x)─┐
│      0 │                       0.5 │
└────────┴───────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction("timeSeriesAvgOverGroup", {createAggregateFunctionTimeSeriesAvgOverGroup, documentation});
}

}
