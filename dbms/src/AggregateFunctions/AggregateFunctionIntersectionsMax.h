#pragma once

#include <common/logger_useful.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnNullable.h>

#include <Common/Allocator.h>

#include <AggregateFunctions/IAggregateFunction.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename T>
class Intersections final
{
    using PointsMap = std::map<T, T>;

    PointsMap points;
    T max_weight;
    T max_weight_pos;

    typename PointsMap::iterator insert_point(const T & v);

public:
    Intersections() : max_weight(0) {}

    void add(const T & start, const T & end, T weight = 1);
    void merge(const Intersections & other);

    void serialize(WriteBuffer & buf) const;
    void deserialize(ReadBuffer & buf);

    T max() const
    {
        return max_weight;
    }
    T max_pos() const
    {
        return max_weight_pos;
    }
};

class AggregateFunctionIntersectionsMax final
    : public IAggregateFunctionDataHelper<Intersections<UInt64>, AggregateFunctionIntersectionsMax>
{
    using PointType = UInt64;

    bool return_position;
    void _add(AggregateDataPtr place, const IColumn & column_start, const IColumn & column_end, size_t row_num) const;

public:
    AggregateFunctionIntersectionsMax(const DataTypes & arguments, const Array & params, bool return_position)
        : return_position(return_position)
    {
        if (!params.empty())
        {
            throw Exception(
                "Aggregate function " + getName() + " does not allow paremeters.", ErrorCodes::AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS);
        }

        if (arguments.size() != 2)
            throw Exception("Aggregate function " + getName() + " requires two arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!arguments[0]->isValueRepresentedByInteger())
            throw Exception{getName() + ": first argument must be represented by integer", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if (!arguments[1]->isValueRepresentedByInteger())
            throw Exception{getName() + ": second argument must be represented by integer", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if (!arguments[0]->equals(*arguments[1]))
            throw Exception{getName() + ": arguments must have the same type", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    }

    String getName() const override
    {
        return "IntersectionsMax";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        _add(place, *columns[0], *columns[1], row_num);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        auto & ret = static_cast<ColumnUInt64 &>(to).getData();
        ret.push_back(data(place).max());
        if (return_position)
            ret.push_back(data(place).max_pos());
    }

    const char * getHeaderFilePath() const override
    {
        return __FILE__;
    }
};
}
