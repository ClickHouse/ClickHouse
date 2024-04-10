#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Columns/ColumnArray.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/range.h>

#include <bitset>
#include <unordered_set>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

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
        UInt32 event_value = static_cast<UInt32>(events.to_ulong());
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
        : IAggregateFunctionDataHelper<AggregateFunctionRetentionData, AggregateFunctionRetention>(arguments, {}, std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt8>()))
    {
        for (const auto i : collections::range(0, arguments.size()))
        {
            const auto * cond_arg = arguments[i].get();
            if (!isUInt8(cond_arg))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Illegal type {} of argument {} of aggregate function {}, must be UInt8",
                                cond_arg->getName(), i, getName());
        }

        events_size = static_cast<UInt8>(arguments.size());
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, const size_t row_num, Arena *) const override
    {
        for (const auto i : collections::range(0, events_size))
        {
            auto event = assert_cast<const ColumnVector<UInt8> *>(columns[i])->getData()[row_num];
            if (event)
            {
                data(place).add(i);
            }
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).merge(data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & data_to = assert_cast<ColumnUInt8 &>(assert_cast<ColumnArray &>(to).getData()).getData();
        auto & offsets_to = assert_cast<ColumnArray &>(to).getOffsets();

        ColumnArray::Offset current_offset = data_to.size();
        data_to.resize(current_offset + events_size);

        const bool first_flag = data(place).events.test(0);
        data_to[current_offset] = first_flag;
        ++current_offset;

        for (size_t i = 1; i < events_size; ++i)
        {
            data_to[current_offset] = (first_flag && data(place).events.test(i));
            ++current_offset;
        }

        offsets_to.push_back(current_offset);
    }
};


AggregateFunctionPtr createAggregateFunctionRetention(const std::string & name, const DataTypes & arguments, const Array & params, const Settings *)
{
    assertNoParameters(name, params);

    if (arguments.size() < 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Not enough event arguments for aggregate function {}", name);

    if (arguments.size() > AggregateFunctionRetentionData::max_events)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Too many event arguments for aggregate function {}", name);

    return std::make_shared<AggregateFunctionRetention>(arguments);
}

}

void registerAggregateFunctionRetention(AggregateFunctionFactory & factory)
{
    factory.registerFunction("retention", createAggregateFunctionRetention);
}

}
