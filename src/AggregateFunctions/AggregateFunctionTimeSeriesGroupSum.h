#pragma once

#include <bitset>
#include <iostream>
#include <map>
#include <queue>
#include <sstream>
#include <unordered_set>
#include <utility>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>
#include <ext/range.h>
#include "IAggregateFunction.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <bool rate>
struct AggregateFunctionTimeSeriesGroupSumData
{
    using DataPoint = std::pair<Int64, Float64>;
    struct Points
    {
        using Dps = std::queue<DataPoint>;
        Dps dps;
        void add(Int64 t, Float64 v)
        {
            dps.push(std::make_pair(t, v));
            if (dps.size() > 2)
                dps.pop();
        }
        Float64 getval(Int64 t)
        {
            Int64 t1, t2;
            Float64 v1, v2;
            if (rate)
            {
                if (dps.size() < 2)
                    return 0;
                t1 = dps.back().first;
                t2 = dps.front().first;
                v1 = dps.back().second;
                v2 = dps.front().second;
                return (v1 - v2) / Float64(t1 - t2);
            }
            else
            {
                if (dps.size() == 1 && t == dps.front().first)
                    return dps.front().second;
                t1 = dps.back().first;
                t2 = dps.front().first;
                v1 = dps.back().second;
                v2 = dps.front().second;
                return v2 + ((v1 - v2) * Float64(t - t2)) / Float64(t1 - t2);
            }
        }
    };

    typedef std::map<UInt64, Points> Series;
    typedef PODArrayWithStackMemory<DataPoint, 128> AggSeries;
    Series ss;
    AggSeries result;

    void add(UInt64 uid, Int64 t, Float64 v)
    { //suppose t is coming asc
        typename Series::iterator it_ss;
        if (ss.count(uid) == 0)
        { //time series not exist, insert new one
            Points tmp;
            tmp.add(t, v);
            ss.emplace(uid, tmp);
            it_ss = ss.find(uid);
        }
        else
        {
            it_ss = ss.find(uid);
            it_ss->second.add(t, v);
        }
        if (result.size() > 0 && t < result.back().first)
            throw Exception{"timeSeriesGroupSum or timeSeriesGroupRateSum must order by timestamp asc!!!", ErrorCodes::LOGICAL_ERROR};
        if (result.size() > 0 && t == result.back().first)
        {
            //do not add new point
            if (rate)
                result.back().second += it_ss->second.getval(t);
            else
                result.back().second += v;
        }
        else
        {
            if (rate)
                result.emplace_back(std::make_pair(t, it_ss->second.getval(t)));
            else
                result.emplace_back(std::make_pair(t, v));
        }
        size_t i = result.size() - 1;
        //reverse find out the index of timestamp that more than previous timestamp of t
        while (result[i].first > it_ss->second.dps.front().first && i >= 0)
            i--;

        i++;
        while (i < result.size() - 1)
        {
            result[i].second += it_ss->second.getval(result[i].first);
            i++;
        }
    }

    void merge(const AggregateFunctionTimeSeriesGroupSumData & other)
    {
        //if ts has overlap, then aggregate two series by interpolation;
        AggSeries tmp;
        tmp.reserve(other.result.size() + result.size());
        size_t i = 0, j = 0;
        Int64 t1, t2;
        Float64 v1, v2;
        while (i < result.size() && j < other.result.size())
        {
            if (result[i].first < other.result[j].first)
            {
                if (j == 0)
                {
                    tmp.emplace_back(result[i]);
                }
                else
                {
                    t1 = other.result[j].first;
                    t2 = other.result[j - 1].first;
                    v1 = other.result[j].second;
                    v2 = other.result[j - 1].second;
                    Float64 value = result[i].second + v2 + (v1 - v2) * (Float64(result[i].first - t2)) / Float64(t1 - t2);
                    tmp.emplace_back(std::make_pair(result[i].first, value));
                }
                i++;
            }
            else if (result[i].first > other.result[j].first)
            {
                if (i == 0)
                {
                    tmp.emplace_back(other.result[j]);
                }
                else
                {
                    t1 = result[i].first;
                    t2 = result[i - 1].first;
                    v1 = result[i].second;
                    v2 = result[i - 1].second;
                    Float64 value = other.result[j].second + v2 + (v1 - v2) * (Float64(other.result[j].first - t2)) / Float64(t1 - t2);
                    tmp.emplace_back(std::make_pair(other.result[j].first, value));
                }
                j++;
            }
            else
            {
                tmp.emplace_back(std::make_pair(result[i].first, result[i].second + other.result[j].second));
                i++;
                j++;
            }
        }
        while (i < result.size())
        {
            tmp.emplace_back(result[i]);
            i++;
        }
        while (j < other.result.size())
        {
            tmp.push_back(other.result[j]);
            j++;
        }
        swap(result, tmp);
    }

    void serialize(WriteBuffer & buf) const
    {
        size_t size = result.size();
        writeVarUInt(size, buf);
        buf.write(reinterpret_cast<const char *>(result.data()), sizeof(result[0]));
    }

    void deserialize(ReadBuffer & buf)
    {
        size_t size = 0;
        readVarUInt(size, buf);
        result.resize(size);
        buf.read(reinterpret_cast<char *>(result.data()), size * sizeof(result[0]));
    }
};
template <bool rate>
class AggregateFunctionTimeSeriesGroupSum final
    : public IAggregateFunctionDataHelper<AggregateFunctionTimeSeriesGroupSumData<rate>, AggregateFunctionTimeSeriesGroupSum<rate>>
{
private:
public:
    String getName() const override { return rate ? "timeSeriesGroupRateSum" : "timeSeriesGroupSum"; }

    AggregateFunctionTimeSeriesGroupSum(const DataTypes & arguments)
        : IAggregateFunctionDataHelper<AggregateFunctionTimeSeriesGroupSumData<rate>, AggregateFunctionTimeSeriesGroupSum<rate>>(arguments, {})
    {
        if (!WhichDataType(arguments[0].get()).isUInt64())
            throw Exception{"Illegal type " + arguments[0].get()->getName() + " of argument 1 of aggregate function " + getName()
                                + ", must be UInt64",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if (!WhichDataType(arguments[1].get()).isInt64())
            throw Exception{"Illegal type " + arguments[1].get()->getName() + " of argument 2 of aggregate function " + getName()
                                + ", must be Int64",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if (!WhichDataType(arguments[2].get()).isFloat64())
            throw Exception{"Illegal type " + arguments[2].get()->getName() + " of argument 3 of aggregate function " + getName()
                                + ", must be Float64",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    }

    DataTypePtr getReturnType() const override
    {
        auto datatypes = std::vector<DataTypePtr>();
        datatypes.push_back(std::make_shared<DataTypeInt64>());
        datatypes.push_back(std::make_shared<DataTypeFloat64>());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(datatypes));
    }

    void add(AggregateDataPtr place, const IColumn ** columns, const size_t row_num, Arena *) const override
    {
        auto uid = assert_cast<const ColumnVector<UInt64> *>(columns[0])->getData()[row_num];
        auto ts = assert_cast<const ColumnVector<Int64> *>(columns[1])->getData()[row_num];
        auto val = assert_cast<const ColumnVector<Float64> *>(columns[2])->getData()[row_num];
        if (uid && ts && val)
        {
            this->data(place).add(uid, ts, val);
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override { this->data(place).merge(this->data(rhs)); }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override { this->data(place).serialize(buf); }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override { this->data(place).deserialize(buf); }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        const auto & value = this->data(place).result;
        size_t size = value.size();

        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        size_t old_size = offsets_to.back();

        offsets_to.push_back(offsets_to.back() + size);

        if (size)
        {
            typename ColumnInt64::Container & ts_to
                = assert_cast<ColumnInt64 &>(assert_cast<ColumnTuple &>(arr_to.getData()).getColumn(0)).getData();
            typename ColumnFloat64::Container & val_to
                = assert_cast<ColumnFloat64 &>(assert_cast<ColumnTuple &>(arr_to.getData()).getColumn(1)).getData();
            ts_to.reserve(old_size + size);
            val_to.reserve(old_size + size);
            size_t i = 0;
            while (i < this->data(place).result.size())
            {
                ts_to.push_back(this->data(place).result[i].first);
                val_to.push_back(this->data(place).result[i].second);
                i++;
            }
        }
    }

    bool allocatesMemoryInArena() const override { return true; }
};
}
