#pragma once

#include <iostream>
#include <sstream>
#include <unordered_set>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNumberBase.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/ArenaAllocator.h>
#include <Common/typeid_cast.h>
#include <ext/range.h>

#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{

struct AggregateFunctionBoundingRatioData
{
    using TimestampEvent = std::pair<UInt32, Float64>;

    bool is_first = false;

    TimestampEvent first_event;
    TimestampEvent last_event;

    void add(UInt32 timestamp, Float64 f)
    {
        if (is_first)
        {
            first_event = TimestampEvent{timestamp, f};
            is_first = true;
        }
        else
        {
            last_event = TimestampEvent{timestamp, f};
        }
    }

    void merge(const AggregateFunctionBoundingRatioData & other)
    {
        // if the arg is earlier than us, replace us with them
        if (other.first_event.first < first_event.first)
        {
            first_event = other.first_event;
        }
        // if the arg is _later_ than us, replace us with them
        if (other.last_event.first > last_event.second)
        {
            last_event = other.last_event;
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(is_first, buf);
        writeBinary(first_event.first, buf);
        writeBinary(first_event.second, buf);

        writeBinary(last_event.first, buf);
        writeBinary(last_event.second, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(is_first, buf);

        readBinary(first_event.first, buf);
        readBinary(first_event.second, buf);

        readBinary(last_event.first, buf);
        readBinary(last_event.second, buf);
    }
};


class AggregateFunctionBoundingRatio final : public IAggregateFunctionDataHelper<AggregateFunctionBoundingRatioData, AggregateFunctionBoundingRatio>
{
private:
    /* implements a basic derivative function
     *
     * (y2 - y1) / (x2 - x1)
     */
    Float64 getBoundingRatio(const AggregateFunctionBoundingRatioData & data) const
    {
        if (data.first_event.first == 0)
            return 0;
        if (data.last_event.first == 0)
            return 0;
        // void divide by zero in denominator
        if (data.last_event.first == data.first_event.first)
            return 0;

        return (data.last_event.second - data.first_event.second) / (data.last_event.first - data.first_event.first);
    }

public:
    String getName() const override
    {
        return "boundingRatio";
    }

    AggregateFunctionBoundingRatio(const DataTypes & arguments)
    {


        const auto time_arg = arguments.at(0).get();
        if (!typeid_cast<const DataTypeDateTime *>(time_arg) && !typeid_cast<const DataTypeUInt32 *>(time_arg))
            throw Exception {"Illegal type " + time_arg->getName() + " of first argument of aggregate function " + getName()
                + ", must be DateTime or UInt32"};

        const auto number_arg = arguments.at(1).get();
        if (!number_arg->isNumber())
            throw Exception {"Illegal type " + number_arg->getName() + " of argument " + toString(1) + " of aggregate function " + getName()
                    + ", must be a Number",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    }


    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, const size_t row_num, Arena *) const override
    {
        const auto timestamp = static_cast<const ColumnVector<UInt32> *>(columns[0])->getData()[row_num];
        const auto value = static_cast<const ColumnVector<Float64> *>(columns[1])->getData()[row_num];
        data(place).add(timestamp, value);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).merge(data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        data(place).deserialize(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        static_cast<ColumnFloat64 &>(to).getData().push_back(getBoundingRatio(data(place)));
    }

    const char * getHeaderFilePath() const override
    {
        return __FILE__;
    }
};

}
