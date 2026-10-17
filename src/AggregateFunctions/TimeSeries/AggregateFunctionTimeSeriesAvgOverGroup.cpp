#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Common/PODArray.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <base/arithmeticOverflow.h>
#include <base/sort.h>

#include <cmath>
#include <cstring>
#include <numeric>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_DATA;
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

/// The average at one time step as the Prometheus engine computes PromQL `avg`: a compensated sum that turns into
/// a compensated mean once the sum would overflow. The result depends on the order of the values.
struct PrometheusAvg
{
    /// The sum of the values, or their mean once `is_mean` is set.
    Float64 value = 0;
    Float64 compensation = 0;
    Float64 count = 0;
    bool is_mean = false;

    void add(Float64 x)
    {
        count += 1;
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
            value /= count - 1;
            compensation /= count - 1;
        }

        const Float64 q = (count - 1) / count;
        value *= q;
        compensation *= q;
        kahanAdd(x / count, value, compensation);
    }

    Float64 get() const
    {
        if (is_mean)
            return value + compensation;
        return value / count + compensation / count;
    }
};

/// All rows added so far. The average is computed at finalization in the order of the keys,
/// so the result does not depend on the order in which rows and states arrive.
struct TimeSeriesAvgOverGroupData
{
    /// The keys in the binary format of their type, key `i` ends at `key_ends[i]`.
    PODArray<char, 64> keys;
    PODArray<UInt64, 32> key_ends;
    /// The values of row `i` are at [i * num_steps, (i + 1) * num_steps), a NULL is stored as 0 with `is_null` set.
    PODArray<Float64, 64> values;
    PODArray<UInt8, 64> is_null;
    size_t num_steps = 0;

    size_t size() const { return key_ends.size(); }
};

class AggregateFunctionTimeSeriesAvgOverGroup final
    : public IAggregateFunctionDataHelper<TimeSeriesAvgOverGroupData, AggregateFunctionTimeSeriesAvgOverGroup>
{
public:
    explicit AggregateFunctionTimeSeriesAvgOverGroup(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<TimeSeriesAvgOverGroupData, AggregateFunctionTimeSeriesAvgOverGroup>(
            argument_types_, {}, std::make_shared<DataTypeArray>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>())))
        , key_type(argument_types_[0])
        , key_serialization(key_type->getDefaultSerialization())
    {
    }

    String getName() const override { return "timeSeriesAvgOverGroup"; }

    bool allocatesMemoryInArena() const override { return false; }

    /// A NULL key sorts last instead of making the default adapter skip the row.
    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr &, const DataTypes & types, const Array &, const AggregateFunctionProperties &) const override
    {
        return std::make_shared<AggregateFunctionTimeSeriesAvgOverGroup>(types);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & array = assert_cast<const ColumnArray &>(*columns[1]);
        const size_t begin = array.getOffsets()[row_num - 1];
        const size_t end = array.getOffsets()[row_num];

        const IColumn * nested = &array.getData();
        const NullMap * null_map = nullptr;
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(nested))
        {
            null_map = &nullable->getNullMapData();
            nested = &nullable->getNestedColumn();
        }
        const auto & input_values = assert_cast<const ColumnFloat64 &>(*nested).getData();

        auto & state = data(place);
        checkNumSteps(state, end - begin);
        addKey(state, *columns[0], row_num);

        const size_t old_size = state.values.size();
        state.values.insert(input_values.begin() + begin, input_values.begin() + end);
        if (null_map)
        {
            state.is_null.insert(null_map->begin() + begin, null_map->begin() + end);
            for (size_t i = old_size; i != state.values.size(); ++i)
                if (state.is_null[i])
                    state.values[i] = 0;
        }
        else
            state.is_null.resize_fill(state.values.size(), 0);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        const auto & rhs_state = data(rhs);
        if (rhs_state.size() == 0)
            return;

        auto & state = data(place);
        checkNumSteps(state, rhs_state.num_steps);
        const size_t keys_size = state.keys.size();
        state.keys.insert(rhs_state.keys.begin(), rhs_state.keys.end());
        for (UInt64 key_end : rhs_state.key_ends)
            state.key_ends.push_back(keys_size + key_end);
        state.values.insert(rhs_state.values.begin(), rhs_state.values.end());
        state.is_null.insert(rhs_state.is_null.begin(), rhs_state.is_null.end());
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & state = data(place);
        writeVarUInt(state.size(), buf);
        writeVarUInt(state.num_steps, buf);
        buf.write(state.keys.data(), state.keys.size());
        for (size_t i = 0; i != state.values.size(); ++i)
        {
            writeBinary(state.is_null[i], buf);
            writeBinaryLittleEndian(state.values[i], buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        size_t num_rows = 0;
        size_t num_steps = 0;
        size_t num_values = 0;
        readVarUInt(num_rows, buf);
        readVarUInt(num_steps, buf);
        if (common::mulOverflow(num_rows, num_steps, num_values))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect size of a state of aggregate function {}", getName());

        auto & state = data(place);
        state.num_steps = num_steps;
        auto keys = key_type->createColumn();
        for (size_t row = 0; row != num_rows; ++row)
        {
            key_serialization->deserializeBinary(*keys, buf, {});
            addKey(state, *keys, row);
        }
        for (size_t i = 0; i != num_values; ++i)
        {
            UInt8 is_null = 0;
            Float64 value = 0;
            readBinary(is_null, buf);
            readBinaryLittleEndian(value, buf);
            state.is_null.push_back(is_null != 0);
            state.values.push_back(is_null ? 0 : value);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & state = data(place);
        const size_t num_rows = state.size();
        const size_t num_steps = num_rows ? state.num_steps : 0;

        auto keys = key_type->createColumn();
        keys->reserve(num_rows);
        for (size_t row = 0; row != num_rows; ++row)
        {
            const size_t key_begin = row ? state.key_ends[row - 1] : 0;
            ReadBufferFromMemory in(state.keys.data() + key_begin, state.key_ends[row] - key_begin);
            key_serialization->deserializeBinary(*keys, in, {});
        }

        VectorWithMemoryTracking<size_t> order(num_rows);
        std::iota(order.begin(), order.end(), 0);
        ::sort(order.begin(), order.end(), [&](size_t a, size_t b) { return lessRow(state, *keys, a, b); });

        VectorWithMemoryTracking<PrometheusAvg> averages(num_steps);
        for (size_t row : order)
        {
            const size_t offset = row * num_steps;
            for (size_t step = 0; step != num_steps; ++step)
                if (!state.is_null[offset + step])
                    averages[step].add(state.values[offset + step]);
        }

        auto & array = assert_cast<ColumnArray &>(to);
        auto & nullable = assert_cast<ColumnNullable &>(array.getData());
        auto & result = assert_cast<ColumnFloat64 &>(nullable.getNestedColumn()).getData();
        auto & result_null_map = nullable.getNullMapData();
        for (const auto & average : averages)
        {
            result.push_back(average.count != 0 ? average.get() : 0);
            result_null_map.push_back(average.count == 0);
        }
        array.getOffsets().push_back(result.size());
    }

private:
    void checkNumSteps(TimeSeriesAvgOverGroupData & state, size_t num_steps) const
    {
        if (state.size() == 0)
            state.num_steps = num_steps;
        else if (num_steps != state.num_steps)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "All arrays of values passed to aggregate function {} must have the same size, got {} and {}",
                            getName(), state.num_steps, num_steps);
    }

    void addKey(TimeSeriesAvgOverGroupData & state, const IColumn & column, size_t row_num) const
    {
        WriteBufferFromVector<PODArray<char, 64>> out(state.keys, AppendModeTag{});
        key_serialization->serializeBinary(column, row_num, out, {});
        out.finalize();
        state.key_ends.push_back(state.keys.size());
    }

    /// Rows with equal keys are ordered by their values, so the order is always the same.
    static bool lessRow(const TimeSeriesAvgOverGroupData & state, const IColumn & keys, size_t a, size_t b)
    {
        if (int res = keys.compareAt(a, b, keys, 1))
            return res < 0;
        const size_t n = state.num_steps;
        if (int res = memcmp(state.values.data() + a * n, state.values.data() + b * n, n * sizeof(Float64)))
            return res < 0;
        return memcmp(state.is_null.data() + a * n, state.is_null.data() + b * n, n) < 0;
    }

    DataTypePtr key_type;
    SerializationPtr key_serialization;
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
    assertBinary(name, argument_types);

    if (!argument_types[0]->isComparable())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of the first argument of aggregate function {}, it must be comparable",
                        argument_types[0]->getName(), name);

    const auto * array_type = typeid_cast<const DataTypeArray *>(argument_types[1].get());
    if (!array_type || !WhichDataType(removeNullable(array_type->getNestedType())).isFloat64())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of the second argument of aggregate function {}, expected Array(Float64) or Array(Nullable(Float64))",
                        argument_types[1]->getName(), name);

    return std::make_shared<AggregateFunctionTimeSeriesAvgOverGroup>(argument_types);
}

}

void registerAggregateFunctionTimeSeriesAvgOverGroup(AggregateFunctionFactory & factory);
void registerAggregateFunctionTimeSeriesAvgOverGroup(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Calculates the average of time series at each time step, the way PromQL `avg()` does.

Each row is one time series: a sort key and an array with one value per time step (`NULL` means no value).
At each time step the function averages the non-`NULL` values the same way as the Prometheus engine:
a Kahan-Babuska-Neumaier compensated sum that switches to an incremental mean if the sum would overflow,
so the average of large finite values stays finite where [`avg`](/reference/functions/aggregate-functions/avg) returns `inf`.
The rounding of that algorithm depends on the order of the values. The function processes the rows in the order of their sort keys,
like Prometheus processes series in the order of their labels, so the result does not depend on the order in which rows are read or merged.

The function keeps all values until the result is calculated, so it needs more memory than `avgForEach`.

This function implements the `avg()` aggregation operator of PromQL.

<Note>
This function is in private preview, enable it by setting `enable_time_series_aggregate_functions = 1`.
</Note>
    )";
    FunctionDocumentation::Syntax syntax = "timeSeriesAvgOverGroup(sort_key, values)";
    FunctionDocumentation::Arguments arguments = {
        {"sort_key", "Defines the order in which the rows are averaged, it must be of a comparable type. `NULL` keys go last. For PromQL it is the labels of the time series.", {"Any"}},
        {"values", "The values of a time series, one per time step.", {"Array(Float64)", "Array(Nullable(Float64))"}}};
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns the average at each time step, or `NULL` at the time steps where all values are `NULL`.", {"Array(Nullable(Float64))"}};
    FunctionDocumentation::Examples examples = {
    {
        "The sum overflows",
        R"(
SET enable_time_series_aggregate_functions = 1;
SELECT avgForEach(values), timeSeriesAvgOverGroup(series, values)
FROM values('series String, values Array(Nullable(Float64))', ('a', [1e308, 1, NULL]), ('b', [1e308, 2, NULL]));
        )",
        R"(
┌─avgForEach(values)─┬─timeSeriesAvgOverGroup(series, values)─┐
│ [inf,1.5,NULL]     │ [1e308,1.5,NULL]                       │
└────────────────────┴────────────────────────────────────────┘
        )"
    },
    {
        "The order of the sort keys defines the rounding",
        R"(
SET enable_time_series_aggregate_functions = 1;
SELECT timeSeriesAvgOverGroup(series, [value]), timeSeriesAvgOverGroup(-series, [value])
FROM values('series Int8, value Float64', (4, -1e308), (1, 1e308), (3, -9.988465674311579e307), (2, 9.988465674311579e307));
        )",
        R"(
┌─timeSeriesAvgOverGroup(series, [value])─┬─timeSeriesAvgOverGroup(negate(series), [value])─┐
│ [-4.9896007738368e291]                  │ [4.9896007738368e291]                           │
└─────────────────────────────────────────┴─────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction("timeSeriesAvgOverGroup", {createAggregateFunctionTimeSeriesAvgOverGroup, documentation});
}

}
