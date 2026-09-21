#include <AggregateFunctions/AggregateFunctionExponentialTimeDecayed.h>

#include <Columns/ColumnExponentialTimeDecaying.h>
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
    extern const SettingsFloat exponential_time_decay_significance_cutoff;
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
    Float64 weighted_sum = 0;
    Float64 weight = 0;
    Float64 max_time = 0;

    bool empty() const { return weight == 0; }

    void add(
        Float64 value,
        Float64 time,
        Float64 decay_length,
        Float64 max_decay_distance,
        bool input_has_calculation_index)
    {
        ExponentialTimeDecayedState rhs;
        rhs.weighted_sum = value;
        rhs.weight = 1;
        rhs.max_time = time;
        merge(rhs, decay_length, max_decay_distance, input_has_calculation_index);
    }

    void merge(
        const ExponentialTimeDecayedState & rhs,
        Float64 decay_length,
        Float64 max_decay_distance,
        bool input_has_calculation_index)
    {
        /// The significance comparison is formed from a relative timestamp difference
        /// plus a logarithmic magnitude difference. It deliberately avoids constructing
        /// absolute unit timestamps, which can lose the magnitude term for large times.
        if (input_has_calculation_index && std::isfinite(max_decay_distance) && !empty() && !rhs.empty())
        {
            if (weighted_sum == 0 && rhs.weighted_sum != 0)
            {
                *this = rhs;
                return;
            }
            if (weighted_sum != 0 && rhs.weighted_sum == 0)
                return;

            if (weighted_sum != 0 && rhs.weighted_sum != 0)
            {
                const long double index_distance
                    = static_cast<long double>(rhs.max_time) - static_cast<long double>(max_time)
                    + static_cast<long double>(decay_length)
                        * (std::log(std::abs(static_cast<long double>(rhs.weighted_sum)))
                            - std::log(std::abs(static_cast<long double>(weighted_sum))));

                if (index_distance > static_cast<long double>(max_decay_distance))
                {
                    *this = rhs;
                    return;
                }
                if (-index_distance > static_cast<long double>(max_decay_distance))
                    return;
            }
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
    }
};

template <ExponentialTimeDecayedResult result_kind>
class AggregateFunctionExponentialTimeDecayed final
    : public IAggregateFunctionDataHelper<
          ExponentialTimeDecayedState,
          AggregateFunctionExponentialTimeDecayed<result_kind>>
{
public:
    static DataTypePtr getResultDataType(Float64 decay_length, bool legacy_result_name)
    {
        if constexpr (result_kind == ExponentialTimeDecayedResult::Avg)
            return std::make_shared<DataTypeFloat64>();

        return std::make_shared<DataTypeExponentialTimeDecayingFloat64>(
            decay_length, legacy_result_name);
    }

    AggregateFunctionExponentialTimeDecayed(
        String name_,
        const DataTypes & argument_types_,
        const Array & parameters_,
        Float64 decay_length_,
        Float64 max_decay_distance_,
        bool input_is_decaying_value_ = false,
        bool legacy_result_name_ = false)
        : IAggregateFunctionDataHelper<
              ExponentialTimeDecayedState,
              AggregateFunctionExponentialTimeDecayed<result_kind>>(
              argument_types_, parameters_, getResultDataType(decay_length_, legacy_result_name_))
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
        if (input_is_decaying_value)
        {
            const auto & decaying = assert_cast<const ColumnExponentialTimeDecaying &>(*columns[0]);
            const auto & tuple = decaying.getStorageTuple();
            value = assert_cast<const ColumnFloat64 &>(tuple.getColumn(2)).getData()[row_num];
            time = assert_cast<const ColumnFloat64 &>(tuple.getColumn(3)).getData()[row_num];
            if (value == 0)
                return;
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

        this->data(place).add(
            value, time, decay_length, max_decay_distance, input_is_decaying_value);
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
                    "Result of aggregate function {} cannot be represented by ExponentialTimeDecaying",
                    getName());

            Tuple decaying_value{
                Field(normalized.sign),
                Field(normalized.signed_unit_time),
                Field(normalized.value_at_anchor),
                Field(normalized.anchor_time)};
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

    const Float64 max_distance_in_decay_lengths
        = static_cast<Float64>((*settings)[Setting::exponential_time_decay_significance_cutoff]);
    if (!std::isfinite(max_distance_in_decay_lengths) || max_distance_in_decay_lengths < 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Setting exponential_time_decay_significance_cutoff must be finite and non-negative for aggregate function {}",
            name);

    if (max_distance_in_decay_lengths == 0)
        return std::numeric_limits<Float64>::infinity();

    return max_distance_in_decay_lengths * decay_length;
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
    assertValueAndTimeArguments(name, argument_types);
    const Float64 decay_length = getDecayLength(name, parameters);
    return std::make_shared<AggregateFunctionExponentialTimeDecayed<ExponentialTimeDecayedResult::Sum>>(
        name,
        argument_types,
        parameters,
        decay_length,
        getMaxDecayDistance(name, settings, decay_length),
        false,
        true);
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
