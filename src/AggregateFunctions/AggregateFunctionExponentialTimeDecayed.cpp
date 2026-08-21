#include <AggregateFunctions/AggregateFunctionExponentialTimeDecayed.h>

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeExponentialTimeDecayingFloat64.h>
#include <DataTypes/DataTypeNumberBase.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <cmath>
#include <limits>
#include <optional>
#include <utility>


namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_time_decay_aggregate_functions;
    extern const SettingsFloat exponential_time_decay_aggregate_function_calculation_budget;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
}

namespace
{

enum class ExponentialTimeDecayedResult
{
    Sum,
    Avg,
    Count,
};

struct ExponentialTimeDecayedState
{
    /// Every state is evaluated at max_time:
    /// weighted_sum = sum(value_i * exp((time_i - max_time) / decay_length))
    /// weight = sum(exp((time_i - max_time) / decay_length))
    /// This representation makes merging states the same operation as adding rows,
    /// independent of row order, batch distribution, and batch count.
    Float64 weighted_sum = 0;
    Float64 weight = 0;
    Float64 max_time = 0;
    /// Runtime-only cache. It is deliberately not serialized: persisted aggregate
    /// states and storage-engine merges always retain the exact representation.
    mutable Float64 calculation_index_time = std::numeric_limits<Float64>::quiet_NaN();
    bool empty() const { return weight == 0; }

    Float64 getCalculationIndexTime(Float64 decay_length) const
    {
        if (std::isnan(calculation_index_time))
            calculation_index_time = weighted_sum == 0
                ? -std::numeric_limits<Float64>::infinity()
                : max_time + decay_length * std::log(std::abs(weighted_sum));
        return calculation_index_time;
    }

    void add(
        Float64 value,
        Float64 time,
        Float64 decay_length,
        Float64 max_decay_distance,
        std::optional<Float64> input_calculation_index_time)
    {
        ExponentialTimeDecayedState rhs;
        rhs.weighted_sum = value;
        rhs.weight = 1;
        rhs.max_time = time;
        if (input_calculation_index_time)
            rhs.calculation_index_time = *input_calculation_index_time;

        merge(rhs, decay_length, max_decay_distance, input_calculation_index_time.has_value());
    }

    void merge(
        const ExponentialTimeDecayedState & rhs,
        Float64 decay_length,
        Float64 max_decay_distance,
        bool input_has_calculation_index)
    {
        /// Approximation is only allowed when the input already carries its
        /// calculation index. Query-local partial states preserve that index, so
        /// parallel aggregation has the same budget semantics as adding rows to a
        /// single state. Aggregate functions reconstructed for storage-engine
        /// merges have an infinite budget and therefore always take the exact path.
        if (input_has_calculation_index && std::isfinite(max_decay_distance) && !empty() && !rhs.empty())
        {
            const Float64 current_index_time = getCalculationIndexTime(decay_length);
            const Float64 rhs_index_time = rhs.getCalculationIndexTime(decay_length);
            if (rhs_index_time > current_index_time
                && rhs_index_time - current_index_time > max_decay_distance)
            {
                *this = rhs;
                return;
            }
            if (current_index_time > rhs_index_time
                && current_index_time - rhs_index_time > max_decay_distance)
                return;
        }

        mergeExact(rhs, decay_length);
    }

    void mergeExact(const ExponentialTimeDecayedState & rhs, Float64 decay_length)
    {
        if (rhs.empty())
            return;

        if (empty())
        {
            *this = rhs;
            return;
        }

        /// Re-anchor the older state at the shared greatest timestamp before adding them.
        if (rhs.max_time > max_time)
        {
            const Float64 distance = rhs.max_time - max_time;
            const Float64 decay = std::exp(-distance / decay_length);
            weighted_sum = weighted_sum * decay + rhs.weighted_sum;
            weight = weight * decay + rhs.weight;
            max_time = rhs.max_time;
        }
        else if (rhs.max_time < max_time)
        {
            const Float64 distance = max_time - rhs.max_time;
            const Float64 decay = std::exp(-distance / decay_length);
            weighted_sum += rhs.weighted_sum * decay;
            weight += rhs.weight * decay;
        }
        else
        {
            weighted_sum += rhs.weighted_sum;
            weight += rhs.weight;
        }

        calculation_index_time = std::numeric_limits<Float64>::quiet_NaN();
    }

    void write(WriteBuffer & buf) const
    {
        writeBinaryLittleEndian(weighted_sum, buf);
        writeBinaryLittleEndian(weight, buf);
        writeBinaryLittleEndian(max_time, buf);
    }

    void read(ReadBuffer & buf)
    {
        readBinaryLittleEndian(weighted_sum, buf);
        readBinaryLittleEndian(weight, buf);
        readBinaryLittleEndian(max_time, buf);
        calculation_index_time = std::numeric_limits<Float64>::quiet_NaN();
    }
};

template <ExponentialTimeDecayedResult result_kind>
class AggregateFunctionExponentialTimeDecayed final
    : public IAggregateFunctionDataHelper<
          ExponentialTimeDecayedState,
          AggregateFunctionExponentialTimeDecayed<result_kind>>
{
public:
    static DataTypePtr getResultDataType(Float64 decay_length)
    {
        if constexpr (result_kind == ExponentialTimeDecayedResult::Avg)
            return std::make_shared<DataTypeFloat64>();

        return createDataTypeExponentialTimeDecayingFloat64(decay_length);
    }

    AggregateFunctionExponentialTimeDecayed(
        String name_,
        const DataTypes & argument_types_,
        const Array & parameters_,
        Float64 decay_length_,
        Float64 max_decay_distance_,
        bool input_is_decaying_value_ = false)
        : IAggregateFunctionDataHelper<
              ExponentialTimeDecayedState,
              AggregateFunctionExponentialTimeDecayed<result_kind>>(
              argument_types_, parameters_, getResultDataType(decay_length_))
        , name(std::move(name_))
        , decay_length(decay_length_)
        , max_decay_distance(max_decay_distance_)
        , input_is_decaying_value(input_is_decaying_value_)
    {
    }

    String getName() const override { return name; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        Float64 value = 1;
        Float64 time = std::numeric_limits<Float64>::quiet_NaN();
        std::optional<Float64> input_calculation_index_time;

        if (input_is_decaying_value)
        {
            const auto & tuple = assert_cast<const ColumnTuple &>(*columns[0]);
            const Float64 sign = assert_cast<const ColumnFloat64 &>(tuple.getColumn(0)).getData()[row_num];
            const Float64 signed_unit_time = assert_cast<const ColumnFloat64 &>(tuple.getColumn(1)).getData()[row_num];
            const Float64 stored_decay_length
                = assert_cast<const ColumnFloat64 &>(tuple.getColumn(2)).getData()[row_num];
            if (!std::isfinite(stored_decay_length) || stored_decay_length != decay_length)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Stored decay length {} does not match decay length {} of aggregate function {}",
                    stored_decay_length,
                    decay_length,
                    getName());
            if (!isCanonicalExponentialTimeDecayingFloat64Value(sign, signed_unit_time))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Input of aggregate function {} is not a canonical ExponentialTimeDecayingFloat64 value",
                    getName());
            if (sign == 0)
                return;

            value = sign;
            time = getExponentialTimeDecayingUnitTime(sign, signed_unit_time);
            input_calculation_index_time = time;
        }
        else
        {
            const size_t time_argument = result_kind == ExponentialTimeDecayedResult::Count ? 0 : 1;
            time = columns[time_argument]->getFloat64(row_num);
            if constexpr (result_kind != ExponentialTimeDecayedResult::Count)
                value = columns[0]->getFloat64(row_num);
        }

        if (!std::isfinite(time))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Time of aggregate function {} must be finite", getName());
        if (!std::isfinite(value))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Value of aggregate function {} must be finite", getName());

        this->data(place).add(value, time, decay_length, max_decay_distance, input_calculation_index_time);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(
            this->data(rhs),
            decay_length,
            max_decay_distance,
            input_is_decaying_value);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & state = this->data(place);
        if constexpr (result_kind == ExponentialTimeDecayedResult::Avg)
        {
            const Float64 result = state.weight > 0
                ? state.weighted_sum / state.weight
                : std::numeric_limits<Float64>::quiet_NaN();
            assert_cast<ColumnFloat64 &>(to).getData().push_back(result);
        }
        else
        {
            const Float64 result = result_kind == ExponentialTimeDecayedResult::Sum
                ? state.weighted_sum
                : state.weight;
            const auto normalized = normalizeExponentialTimeDecayingFloat64(
                result,
                state.empty() ? 0 : state.max_time,
                decay_length);
            if (!std::isfinite(normalized.signed_unit_time))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Result of aggregate function {} cannot be represented by ExponentialTimeDecayingFloat64",
                    getName());

            Tuple decaying_value{
                Field(normalized.sign),
                Field(normalized.signed_unit_time),
                Field(decay_length)};
            to.insert(Field(decaying_value));
        }
    }

    bool allocatesMemoryInArena() const override { return false; }

private:
    const String name;
    const Float64 decay_length;
    const Float64 max_decay_distance;
    const bool input_is_decaying_value;
};

Float64 getMaxDecayDistance(const String & name, const Settings * settings, Float64 decay_length)
{
    /// Aggregate functions embedded in AggregateFunction/SimpleAggregateFunction
    /// data types are constructed without query settings. Keeping this path
    /// infinite makes all storage-engine merges exact by construction.
    if (!settings)
        return std::numeric_limits<Float64>::infinity();

    const Float64 calculation_budget
        = static_cast<Float64>((*settings)[Setting::exponential_time_decay_aggregate_function_calculation_budget]);
    if (!std::isfinite(calculation_budget) || calculation_budget < 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Setting exponential_time_decay_aggregate_function_calculation_budget must be finite and non-negative for aggregate function {}",
            name);

    if (calculation_budget == 0)
        return std::numeric_limits<Float64>::infinity();

    return calculation_budget * decay_length;
}

Float64 getDecayLength(const String & name, const Array & parameters)
{
    if (parameters.size() != 1)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} takes exactly one parameter, the decay length",
            name);

    const Float64 decay_length = applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters[0]);
    if (!std::isfinite(decay_length) || decay_length <= 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Decay length of aggregate function {} must be finite and positive", name);

    return decay_length;
}

void assertValueAndTimeArguments(const String & name, const DataTypes & argument_types)
{
    if (argument_types.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} takes exactly two arguments", name);

    if (!isNumber(argument_types[0]))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First argument of aggregate function {} must be a number, got {}",
            name,
            argument_types[0]->getName());

    if (!isNumber(argument_types[1]) && !isDateTime(argument_types[1]) && !isDateTime64(argument_types[1]))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Second argument of aggregate function {} must be a number, DateTime, or DateTime64, got {}",
            name,
            argument_types[1]->getName());
}

void assertTimeArgument(const String & name, const DataTypes & argument_types)
{
    if (argument_types.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} takes exactly one argument", name);

    if (!isNumber(argument_types[0]) && !isDateTime(argument_types[0]) && !isDateTime64(argument_types[0]))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Argument of aggregate function {} must be a number, DateTime, or DateTime64, got {}",
            name,
            argument_types[0]->getName());
}

}

void assertExperimentalTimeDecayAggregateFunctionEnabled(const String & name, const Settings * settings)
{
    if (settings && !(*settings)[Setting::allow_experimental_time_decay_aggregate_functions])
        throw Exception(
            ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
            "Aggregate function {} is experimental and disabled by default. Enable it with setting "
            "allow_experimental_time_decay_aggregate_functions",
            name);
}

AggregateFunctionPtr createAggregateFunctionExponentialTimeDecayedSum(
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    const Settings * settings)
{
    if (argument_types.size() == 1)
    {
        const auto type_decay_length = tryGetExponentialTimeDecayingFloat64DecayLength(argument_types[0]);
        if (type_decay_length)
        {
            const Float64 decay_length = parameters.empty()
                ? *type_decay_length
                : getDecayLength(name, parameters);
            if (decay_length != *type_decay_length)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Decay length parameter {} of aggregate function {} does not match input type {}",
                    decay_length,
                    name,
                    argument_types[0]->getName());

            return std::make_shared<AggregateFunctionExponentialTimeDecayed<ExponentialTimeDecayedResult::Sum>>(
                name,
                argument_types,
                parameters,
                decay_length,
                getMaxDecayDistance(name, settings, decay_length),
                true);
        }
    }

    assertValueAndTimeArguments(name, argument_types);
    const Float64 decay_length = getDecayLength(name, parameters);
    return std::make_shared<AggregateFunctionExponentialTimeDecayed<ExponentialTimeDecayedResult::Sum>>(
        name,
        argument_types,
        parameters,
        decay_length,
        getMaxDecayDistance(name, settings, decay_length));
}

AggregateFunctionPtr createAggregateFunctionExponentialTimeDecayingFloat64(
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    const Settings * settings)
{
    return createAggregateFunctionExponentialTimeDecayedSum(name, argument_types, parameters, settings);
}

AggregateFunctionPtr createAggregateFunctionExponentialTimeDecayedAvg(
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    const Settings * settings)
{
    assertValueAndTimeArguments(name, argument_types);
    const Float64 decay_length = getDecayLength(name, parameters);
    return std::make_shared<AggregateFunctionExponentialTimeDecayed<ExponentialTimeDecayedResult::Avg>>(
        name,
        argument_types,
        parameters,
        decay_length,
        getMaxDecayDistance(name, settings, decay_length));
}

AggregateFunctionPtr createAggregateFunctionExponentialTimeDecayedCount(
    const String & name,
    const DataTypes & argument_types,
    const Array & parameters,
    const Settings * settings)
{
    assertTimeArgument(name, argument_types);
    const Float64 decay_length = getDecayLength(name, parameters);
    return std::make_shared<AggregateFunctionExponentialTimeDecayed<ExponentialTimeDecayedResult::Count>>(
        name,
        argument_types,
        parameters,
        decay_length,
        getMaxDecayDistance(name, settings, decay_length));
}

}
