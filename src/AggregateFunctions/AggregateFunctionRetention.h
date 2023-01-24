#pragma once

#include <unordered_set>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/ArenaAllocator.h>
#include <base/range.h>
#include <bitset>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

struct AggregateFunctionRetentionData
{
    static constexpr auto max_events = 32;

    using Events = std::bitset<max_events>;

    Events events;

    void add(UInt8 event)
    {
        events.set(event);
    }

    void merge(const AggregateFunctionRetentionData & other)
    {
        events |= other.events;
    }

    void serialize(WriteBuffer & buf) const
    {
        UInt32 event_value = events.to_ulong();
        writeBinary(event_value, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        UInt32 event_value;
        readBinary(event_value, buf);
        events = event_value;
    }
};

/**
  * The max size of events is 32, that's enough for retention analytics
  *
  * Usage:
  * - retention(cond1, cond2, cond3, ....)
  * - returns [cond1_flag, cond1_flag && cond2_flag, cond1_flag && cond3_flag, ...]
  */
class AggregateFunctionRetention final
        : public IAggregateFunctionDataHelper<AggregateFunctionRetentionData, AggregateFunctionRetention>
{
private:
    UInt8 events_size;

public:
    String getName() const override
    {
        return "retention";
    }

    explicit AggregateFunctionRetention(const DataTypes & arguments)
        : IAggregateFunctionDataHelper<AggregateFunctionRetentionData, AggregateFunctionRetention>(arguments, {})
    {
        for (const auto i : collections::range(0, arguments.size()))
        {
            const auto * cond_arg = arguments[i].get();
            if (!isUInt8(cond_arg))
                throw Exception{"Illegal type " + cond_arg->getName() + " of argument " + toString(i) + " of aggregate function "
                        + getName() + ", must be UInt8",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        events_size = static_cast<UInt8>(arguments.size());
    }


    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt8>());
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, const size_t row_num, Arena *) const override
    {
        for (const auto i : collections::range(0, events_size))
        {
            auto event = assert_cast<const ColumnVector<UInt8> *>(columns[i])->getData()[row_num];
            if (event)
            {
                this->data(place).add(i);
            }
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & data_to = assert_cast<ColumnUInt8 &>(assert_cast<ColumnArray &>(to).getData()).getData();
        auto & offsets_to = assert_cast<ColumnArray &>(to).getOffsets();

        ColumnArray::Offset current_offset = data_to.size();
        data_to.resize(current_offset + events_size);

        const bool first_flag = this->data(place).events.test(0);
        data_to[current_offset] = first_flag;
        ++current_offset;

        for (size_t i = 1; i < events_size; ++i)
        {
            data_to[current_offset] = (first_flag && this->data(place).events.test(i));
            ++current_offset;
        }

        offsets_to.push_back(current_offset);
    }
};

}
