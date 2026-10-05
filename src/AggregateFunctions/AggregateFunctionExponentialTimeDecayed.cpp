#include <AggregateFunctions/AggregateFunctionExponentialTimeDecayed.h>

#include <Columns/ColumnExponentialTimeDecaying.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeExponentialTimeDecaying.h>
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
        Float64 max_decay_lengths,
        std::optional<UInt128> input_ordering_key,
        std::optional<ExponentialTimeDecayingKeyWidth> key_width)
    {
        ExponentialTimeDecayedState rhs;
        rhs.weighted_sum = value;
        rhs.weight = 1;
        rhs.max_time = time;

        /// The significance cutoff is an input-row optimization for finalized
        /// `ExponentialTimeDecaying` values only. It must never affect state merges.
        if (input_ordering_key && key_width && std::isfinite(max_decay_lengths) && !empty())
        {
            if (weighted_sum == 0)
            {
                *this = rhs;
                return;
            }

            const UInt128 current_ordering_key = normalizeExponentialTimeDecaying(
                weighted_sum, max_time, decay_length, *key_width).ordering_key;
            const int distance = compareExponentialTimeDecayingOrderingKeyDistance(
                *input_ordering_key,
                current_ordering_key,
                *key_width,
                decay_length,
                max_decay_lengths);

            if (distance > 0)
            {
                *this = rhs;
                return;
            }
            if (distance < 0)
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
    AggregateFunctionExponentialTimeDecayed(
        String name_,
        const DataTypes & argument_types_,
        const Array & parameters_,
        Float64 decay_length_,
        Float64 max_decay_lengths_,
        DataTypePtr result_type_,
        std::optional<ExponentialTimeDecayingKeyWidth> key_width_ = std::nullopt)
        : IAggregateFunctionDataHelper<
              ExponentialTimeDecayedState,
              AggregateFunctionExponentialTimeDecayed<result_kind>>(
              argument_types_, parameters_, std::move(result_type_))
        , name(std::move(name_))
        , decay_length(decay_length_)
        , max_decay_lengths(max_decay_lengths_)
        , key_width(key_width_)
        , input_is_decaying_value(key_width.has_value())
    {
    }

    String getName() const override { return name; }

    bool haveSameStateRepresentationImpl(const IAggregateFunction & rhs) const override
    {
        const auto * other = typeid_cast<const AggregateFunctionExponentialTimeDecayed *>(&rhs);
        return other
            && this->getStateVariant() == other->getStateVariant()
            && name == other->name
            && decay_length == other->decay_length
            && this->haveEqualArgumentTypes(rhs);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        Float64 value = 1;
        Float64 time = std::numeric_limits<Float64>::quiet_NaN();
        std::optional<UInt128> input_ordering_key;
        if (input_is_decaying_value)
        {
            const auto & decaying = assert_cast<const ColumnExponentialTimeDecaying &>(*columns[0]);
            const auto canonical = decaying.getCanonicalValueAt(row_num);
            value = canonical.value_at_anchor;
            time = canonical.anchor_time;
            if (value == 0)
                return;

            chassert(key_width);
            if (*key_width == ExponentialTimeDecayingKeyWidth::Bits64)
            {
                const auto & keys
                    = assert_cast<const ColumnUInt64 &>(decaying.getOrderingKeyColumn()).getData();
                input_ordering_key = UInt128(keys[row_num]);
            }
            else
            {
                const auto & keys
                    = assert_cast<const ColumnUInt128 &>(decaying.getOrderingKeyColumn()).getData();
                input_ordering_key = keys[row_num];
            }
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
            value, time, decay_length, max_decay_lengths, input_ordering_key, key_width);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).mergeExact(this->data(rhs), decay_length);
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

        Float64 result = 0;
        if constexpr (result_kind == ExponentialTimeDecayedResult::Avg)
            result = state.weight > 0
                ? state.weighted_sum / state.weight
                : std::numeric_limits<Float64>::quiet_NaN();
        else if constexpr (result_kind == ExponentialTimeDecayedResult::Sum)
            result = state.weighted_sum;
        else
            result = state.weight;

        if (!input_is_decaying_value)
        {
            if constexpr (result_kind != ExponentialTimeDecayedResult::Avg)
            {
                if (!std::isfinite(result))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Result of aggregate function {} is not finite",
                        getName());
            }

            assert_cast<ColumnFloat64 &>(to).getData().push_back(result);
            return;
        }

        chassert(key_width);
        const Float64 result_time = state.empty() ? 0 : state.max_time;
        if (!isFiniteExponentialTimeDecayingCurve(result, result_time, decay_length))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Result of aggregate function {} does not define a finite decaying curve",
                getName());

        const auto & decaying_to = assert_cast<ColumnExponentialTimeDecaying &>(to);
        if (decaying_to.getMode() == ExponentialTimeDecayingMode::Direct)
        {
            to.insert(Field(Tuple{Field(result), Field(result_time), Field(decay_length)}));
        }
        else
        {
            const auto normalized = normalizeExponentialTimeDecaying(
                result, result_time, decay_length, *key_width);
            const auto canonical = *key_width == ExponentialTimeDecayingKeyWidth::Bits64
                ? getExponentialTimeDecayingCanonicalDirectValue(static_cast<UInt64>(normalized.ordering_key))
                : getExponentialTimeDecayingCanonicalDirectValue(normalized.ordering_key);
            to.insert(Field(Tuple{
                Field(canonical.value_at_anchor),
                Field(canonical.anchor_time),
                Field(decay_length)}));
        }
    }

    bool allocatesMemoryInArena() const override { return false; }

private:
    const String name;
    const Float64 decay_length;
    const Float64 max_decay_lengths;
    const std::optional<ExponentialTimeDecayingKeyWidth> key_width;
    const bool input_is_decaying_value;
};

Float64 getMaxDecayLengths(const String & name, const Settings * settings)
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

    return max_distance_in_decay_lengths;
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
        const auto type_decay_length = tryGetExponentialTimeDecayingDecayLength(argument_types[0]);
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
                getMaxDecayLengths(name, settings),
                argument_types[0],
                *tryGetExponentialTimeDecayingKeyWidth(argument_types[0]));
        }
    }

    assertValueAndTimeArguments(name, argument_types);
    const Float64 decay_length = getDecayLength(name, parameters);
    return std::make_shared<AggregateFunctionExponentialTimeDecayed<ExponentialTimeDecayedResult::Sum>>(
        name,
        argument_types,
        parameters,
        decay_length,
        getMaxDecayLengths(name, settings),
        std::make_shared<DataTypeFloat64>());
}

AggregateFunctionPtr createAggregateFunctionExponentialTimeDecaying(
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
        getMaxDecayLengths(name, settings),
        std::make_shared<DataTypeFloat64>());
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
        getMaxDecayLengths(name, settings),
        std::make_shared<DataTypeFloat64>());
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
        getMaxDecayLengths(name, settings),
        std::make_shared<DataTypeFloat64>());
}

}
