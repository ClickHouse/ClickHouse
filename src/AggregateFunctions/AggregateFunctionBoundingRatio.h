#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Common/FieldVisitors.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/** Tracks the leftmost and rightmost (x, y) data points.
  */
struct AggregateFunctionBoundingRatioData
{
    struct Point
    {
        Float64 x;
        Float64 y;
    };

    bool empty = true;
    Point left;
    Point right;

    void add(Float64 x, Float64 y)
    {
        Point point{x, y};

        if (empty)
        {
            left = point;
            right = point;
            empty = false;
        }
        else if (point.x < left.x)
        {
            left = point;
        }
        else if (point.x > right.x)
        {
            right = point;
        }
    }

    void merge(const AggregateFunctionBoundingRatioData & other)
    {
        if (empty)
        {
            *this = other;
        }
        else
        {
            if (other.left.x < left.x)
                left = other.left;
            if (other.right.x > right.x)
                right = other.right;
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(empty, buf);

        if (!empty)
        {
            writePODBinary(left, buf);
            writePODBinary(right, buf);
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(empty, buf);

        if (!empty)
        {
            readPODBinary(left, buf);
            readPODBinary(right, buf);
        }
    }
};


class AggregateFunctionBoundingRatio final : public IAggregateFunctionDataHelper<AggregateFunctionBoundingRatioData, AggregateFunctionBoundingRatio>
{
private:
    /** Calculates the slope of a line between leftmost and rightmost data points.
      * (y2 - y1) / (x2 - x1)
      */
    Float64 NO_SANITIZE_UNDEFINED getBoundingRatio(const AggregateFunctionBoundingRatioData & data) const
    {
        if (data.empty)
            return std::numeric_limits<Float64>::quiet_NaN();

        return (data.right.y - data.left.y) / (data.right.x - data.left.x);
    }

public:
    String getName() const override
    {
        return "boundingRatio";
    }

    AggregateFunctionBoundingRatio(const DataTypes & arguments)
        : IAggregateFunctionDataHelper<AggregateFunctionBoundingRatioData, AggregateFunctionBoundingRatio>(arguments, {})
    {
        const auto x_arg = arguments.at(0).get();
        const auto y_arg = arguments.at(0).get();

        if (!x_arg->isValueRepresentedByNumber() || !y_arg->isValueRepresentedByNumber())
            throw Exception("Illegal types of arguments of aggregate function " + getName() + ", must have number representation.",
                ErrorCodes::BAD_ARGUMENTS);
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, const size_t row_num, Arena *) const override
    {
        /// NOTE Slightly inefficient.
        const auto x = columns[0]->getFloat64(row_num);
        const auto y = columns[1]->getFloat64(row_num);
        data(place).add(x, y);
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

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnFloat64 &>(to).getData().push_back(getBoundingRatio(data(place)));
    }
};

}
