#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeFloat.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <cmath>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/**
 * State for exponential time decay aggregation.
 * Stores the sum, count, previous time, and previous value for incremental calculations.
 * Can be merged and serialized for use in AggregatingMergeTree.
 */
struct ExponentialDecayState
{
    Float64 sum = 0;
    Float64 count = 0;
    Float64 previous_time = 0;
    Float64 previous_value = 0;
    bool initialized = false;

    void write(WriteBuffer & buf) const
    {
        writeBinaryLittleEndian(sum, buf);
        writeBinaryLittleEndian(count, buf);
        writeBinaryLittleEndian(previous_time, buf);
        writeBinaryLittleEndian(previous_value, buf);
        writeBinaryLittleEndian(initialized, buf);
    }

    void read(ReadBuffer & buf)
    {
        readBinaryLittleEndian(sum, buf);
        readBinaryLittleEndian(count, buf);
        readBinaryLittleEndian(previous_time, buf);
        readBinaryLittleEndian(previous_value, buf);
        readBinaryLittleEndian(initialized, buf);
    }
};

/**
 * Exponential time decay sum aggregate function.
 * Computes: sum(v * exp((t - t_max) / decay_length))
 * 
 * Usage: exponentialTimeDecayedSum(decay_length)(value, time)
 * Example: SELECT exponentialTimeDecayedSum(10)(metric_value, timestamp) FROM metrics
 */
template <typename ValueType>
class AggregateFunctionExponentialTimeDecayedSum final
    : public IAggregateFunctionDataHelper<ExponentialDecayState, AggregateFunctionExponentialTimeDecayedSum<ValueType>>
{
private:
    Float64 decay_length;

public:
    AggregateFunctionExponentialTimeDecayedSum(
        const DataTypes & argument_types_,
        const Array & parameters_,
        Float64 decay_length_)
        : IAggregateFunctionDataHelper<ExponentialDecayState, AggregateFunctionExponentialTimeDecayedSum<ValueType>>(
            argument_types_, parameters_, std::make_shared<DataTypeFloat64>())
        , decay_length(decay_length_)
    {
    }

    String getName() const override { return "exponentialTimeDecayedSum"; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & state = this->data(place);

        Float64 value = static_cast<Float64>(columns[0]->getFloat64(row_num));
        Float64 time = static_cast<Float64>(columns[1]->getFloat64(row_num));

        if (!state.initialized)
        {
            state.previous_time = time;
            state.previous_value = value;
            state.sum = value;
            state.count = 1;
            state.initialized = true;
        }
        else
        {
            Float64 time_diff = time - state.previous_time;
            Float64 decay = std::exp(-time_diff / decay_length);
            state.sum = state.sum * decay + value;
            state.previous_time = time;
            state.previous_value = value;
            state.count++;
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & state_lhs = this->data(place);
        const auto & state_rhs = this->data(rhs);

        if (!state_rhs.initialized)
            return;

        if (!state_lhs.initialized)
        {
            state_lhs = state_rhs;
        }
        else
        {
            Float64 time_diff = state_rhs.previous_time - state_lhs.previous_time;
            Float64 decay = std::exp(-time_diff / decay_length);
            state_lhs.sum = state_lhs.sum * decay + state_rhs.sum;
            state_lhs.previous_time = std::max(state_lhs.previous_time, state_rhs.previous_time);
            state_lhs.previous_value = state_rhs.previous_value;
            state_lhs.count += state_rhs.count;
        }
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
        auto & state = this->data(place);
        assert_cast<ColumnFloat64 &>(to).getData().push_back(state.sum);
    }

    bool allocatesMemoryInArena() const override { return false; }

    bool isState() const override { return true; }

    static Float64 getDecayLength(const Array & parameters_)
    {
        if (parameters_.size() != 1)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "exponentialTimeDecayedSum takes exactly one parameter (decay_length)");
        }
        return applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters_[0]);
    }
};

/**
 * Exponential time decay average aggregate function.
 * Computes: sum(v * decay) / sum(decay)
 * 
 * Usage: exponentialTimeDecayedAvg(decay_length)(value, time)
 */
template <typename ValueType>
class AggregateFunctionExponentialTimeDecayedAvg final
    : public IAggregateFunctionDataHelper<ExponentialDecayState, AggregateFunctionExponentialTimeDecayedAvg<ValueType>>
{
private:
    Float64 decay_length;

public:
    AggregateFunctionExponentialTimeDecayedAvg(
        const DataTypes & argument_types_,
        const Array & parameters_,
        Float64 decay_length_)
        : IAggregateFunctionDataHelper<ExponentialDecayState, AggregateFunctionExponentialTimeDecayedAvg<ValueType>>(
            argument_types_, parameters_, std::make_shared<DataTypeFloat64>())
        , decay_length(decay_length_)
    {
    }

    String getName() const override { return "exponentialTimeDecayedAvg"; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & state = this->data(place);

        Float64 value = static_cast<Float64>(columns[0]->getFloat64(row_num));
        Float64 time = static_cast<Float64>(columns[1]->getFloat64(row_num));

        if (!state.initialized)
        {
            state.previous_time = time;
            state.previous_value = value;
            state.sum = value;
            state.count = 1.0;
            state.initialized = true;
        }
        else
        {
            Float64 time_diff = time - state.previous_time;
            Float64 decay = std::exp(-time_diff / decay_length);
            state.sum = state.sum * decay + value;
            state.count = state.count * decay + 1.0;
            state.previous_time = time;
            state.previous_value = value;
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & state_lhs = this->data(place);
        const auto & state_rhs = this->data(rhs);

        if (!state_rhs.initialized)
            return;

        if (!state_lhs.initialized)
        {
            state_lhs = state_rhs;
        }
        else
        {
            Float64 time_diff = state_rhs.previous_time - state_lhs.previous_time;
            Float64 decay = std::exp(-std::abs(time_diff) / decay_length);

            if (time_diff >= 0)
            {
                state_lhs.sum = state_lhs.sum * decay + state_rhs.sum;
                state_lhs.count = state_lhs.count * decay + state_rhs.count;
            }
            else
            {
                state_lhs.sum = state_rhs.sum * decay + state_lhs.sum;
                state_lhs.count = state_rhs.count * decay + state_lhs.count;
            }
            state_lhs.previous_time = std::max(state_lhs.previous_time, state_rhs.previous_time);
            state_lhs.previous_value = state_rhs.previous_value;
        }
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
        auto & state = this->data(place);
        if (state.count > 0)
        {
            assert_cast<ColumnFloat64 &>(to).getData().push_back(state.sum / state.count);
        }
        else
        {
            assert_cast<ColumnFloat64 &>(to).getData().push_back(std::numeric_limits<Float64>::quiet_NaN());
        }
    }

    bool allocatesMemoryInArena() const override { return false; }

    bool isState() const override { return true; }

    static Float64 getDecayLength(const Array & parameters_)
    {
        if (parameters_.size() != 1)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "exponentialTimeDecayedAvg takes exactly one parameter (decay_length)");
        }
        return applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters_[0]);
    }
};

/**
 * Exponential time decay count aggregate function.
 * Computes: sum(decay)
 * 
 * Usage: exponentialTimeDecayedCount(decay_length)(time)
 */
template <typename TimeType>
class AggregateFunctionExponentialTimeDecayedCount final
    : public IAggregateFunctionDataHelper<ExponentialDecayState, AggregateFunctionExponentialTimeDecayedCount<TimeType>>
{
private:
    Float64 decay_length;

public:
    AggregateFunctionExponentialTimeDecayedCount(
        const DataTypes & argument_types_,
        const Array & parameters_,
        Float64 decay_length_)
        : IAggregateFunctionDataHelper<ExponentialDecayState, AggregateFunctionExponentialTimeDecayedCount<TimeType>>(
            argument_types_, parameters_, std::make_shared<DataTypeFloat64>())
        , decay_length(decay_length_)
    {
    }

    String getName() const override { return "exponentialTimeDecayedCount"; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & state = this->data(place);

        Float64 time = static_cast<Float64>(columns[0]->getFloat64(row_num));

        if (!state.initialized)
        {
            state.previous_time = time;
            state.sum = 1.0;
            state.initialized = true;
        }
        else
        {
            Float64 time_diff = time - state.previous_time;
            state.sum = state.sum * std::exp(-time_diff / decay_length) + 1.0;
            state.previous_time = time;
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & state_lhs = this->data(place);
        const auto & state_rhs = this->data(rhs);

        if (!state_rhs.initialized)
            return;

        if (!state_lhs.initialized)
        {
            state_lhs = state_rhs;
        }
        else
        {
            Float64 time_diff = state_rhs.previous_time - state_lhs.previous_time;
            Float64 decay = std::exp(-std::abs(time_diff) / decay_length);

            if (time_diff >= 0)
            {
                state_lhs.sum = state_lhs.sum * decay + state_rhs.sum;
            }
            else
            {
                state_lhs.sum = state_rhs.sum * decay + state_lhs.sum;
            }
            state_lhs.previous_time = std::max(state_lhs.previous_time, state_rhs.previous_time);
        }
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
        auto & state = this->data(place);
        assert_cast<ColumnFloat64 &>(to).getData().push_back(state.sum);
    }

    bool allocatesMemoryInArena() const override { return false; }

    bool isState() const override { return true; }

    static Float64 getDecayLength(const Array & parameters_)
    {
        if (parameters_.size() != 1)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "exponentialTimeDecayedCount takes exactly one parameter (decay_length)");
        }
        return applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters_[0]);
    }
};

}
