//
// Created by sinsinan on 04/08/23.
//

#pragma once

#include <iostream>
#include <limits>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/StatCommon.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsDateTime.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/types.h>
#include <Common/PODArray_fwd.h>
#include <Common/assert_cast.h>

#include <boost/math/distributions/normal.hpp>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

/// helper type for comparing `std::pair`s using solely the .first member
template <template <typename> class Comparator>
struct ComparePairFirst final
{
    template <typename T1, typename T2>
    bool operator()(const std::pair<T1, T2> & lhs, const std::pair<T1, T2> & rhs) const
    {
        return Comparator<T1>{}(lhs.first, rhs.first);
    }
};

struct LTTBDataDateTime64Float64WithSort final
{
    using DataPair = std::pair<DateTime64, Float64>;
    using ComparatorLess = ComparePairFirst<std::less>;
    using ComparatorGreater = ComparePairFirst<std::greater>;
    using AllocatorSample = MixedAlignedArenaAllocator<alignof(DataPair), 4096>;
    using DataList = PODArray<DataPair, 32, AllocatorSample>;

    bool sorted = true;

    DataList dataList{};

    void add(const DateTime64 x, const Float64 y, Arena * arena)
    {
        dataList.push_back(std::make_pair(x, y), arena);
        sorted = false;
    }

    void merge(const LTTBDataDateTime64Float64WithSort & other, Arena * arena)
    {
        if (other.dataList.empty())
            return;

        dataList.insert(std::begin(other.dataList), std::end(other.dataList), arena);
        sorted = false;
    }

    void sort(bool ascending)
    {
        if (sorted)
            return;

        if (ascending)
            ::sort(std::begin(dataList), std::end(dataList), ComparatorLess{});
        else
            ::sort(std::begin(dataList), std::end(dataList), ComparatorGreater{});

        sorted = true;
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(sorted, buf);
        writeBinary(dataList.size(), buf);

        for (const auto & events : dataList)
        {
            writeBinary(events.first, buf);
            writeBinary(events.second, buf);
        }
    }

    void deserialize(ReadBuffer & buf, Arena * arena)
    {
        readBinary(sorted, buf);

        size_t size;
        readBinary(size, buf);

        dataList.clear();
        dataList.reserve(size, arena);

        for (size_t i = 0; i < size; ++i)
        {
            DateTime64 x;
            readBinary(x, buf);

            Float64 y;
            readBinary(y, buf);

            dataList.push_back(std::make_pair(x, y), arena);
        }
    }

    PODArray<std::pair<DateTime64, Float64>> getResult(UInt64 total_buckets)
    {
        sort(true);

        unsigned long int_total_buckets = total_buckets;
        PODArray<std::pair<DateTime64, Float64>> result;

        if (dataList.size() <= int_total_buckets)
        {
            for (unsigned long i = 0; i < dataList.size(); ++i)
            {
                result.emplace_back(std::make_pair(dataList[i].first, dataList[i].second));
            }
            return result;
        }

        // find the size of each bucket
        unsigned long single_bucket_size = dataList.size() / total_buckets;

        for (unsigned long i = 0; i < int_total_buckets; ++i)
        {
            unsigned long start_index = i * single_bucket_size;
            unsigned long end_index = (i + 1) * single_bucket_size;
            if (i == int_total_buckets - 1)
            {
                end_index = dataList.size();
            }

            //            int center_index = (start_index + end_index) / 2;
            unsigned long max_index = start_index;
            Float64 max_value = dataList[start_index].second;
            for (unsigned long j = start_index; j < end_index; ++j)
            {
                if (dataList[j].second > max_value)
                {
                    max_value = dataList[j].second;
                    max_index = j;
                }
            }
            result.emplace_back(std::make_pair(dataList[max_index].first, dataList[max_index].second));

            unsigned long last_index = max_index;
            Float64 area = 0.0;
            for (unsigned long j = max_index + 1; j < end_index; ++j)
            {
                Float64 this_area
                    = 0.5 * (dataList[j].first - dataList[last_index].first) * (dataList[j].second - dataList[last_index].second);
                if (this_area > area)
                {
                    result.emplace_back(std::make_pair(dataList[j].first, dataList[j].second));
                    last_index = j;
                    area = this_area;
                }
            }
        }

        return result;
    }

    PODArray<std::pair<DateTime64, Float64>> getResult2(UInt64 total_buckets)
    {
        sort(true);
        unsigned long int_total_buckets = total_buckets;
        PODArray<std::pair<DateTime64, Float64>> result;

        // Handle special cases for small dataList
        if (dataList.size() <= int_total_buckets)
        {
            for (unsigned long i = 0; i < dataList.size(); ++i)
            {
                result.emplace_back(std::make_pair(dataList[i].first, dataList[i].second));
            }
            return result;
        }

        // Find the size of each bucket
        unsigned long single_bucket_size = dataList.size() / total_buckets;

        // Include the first data point
        result.emplace_back(dataList[0]);

        for (unsigned long i = 1; i < int_total_buckets - 1; ++i) // Skip the first and last bucket
        {
            unsigned long start_index = i * single_bucket_size;
            unsigned long end_index = (i + 1) * single_bucket_size;

            // Compute the average point in the next bucket
            DateTime64 avg_x = 0;
            Float64 avg_y = 0;
            for (unsigned long j = end_index; j < (i + 2) * single_bucket_size; ++j)
            {
                avg_x += dataList[j].first;
                avg_y += dataList[j].second;
            }
            avg_x /= single_bucket_size;
            avg_y /= single_bucket_size;

            // Find the point in the current bucket that forms the largest triangle
            unsigned long max_index = start_index;
            Float64 max_area = 0.0;
            for (unsigned long j = start_index; j < end_index; ++j)
            {
                Float64 area = std::abs(0.5 * (result.back().first * dataList[j].second +
                                               dataList[j].first * avg_y +
                                               avg_x * result.back().second -
                                               result.back().first * avg_y -
                                               dataList[j].first * result.back().second -
                                               avg_x * dataList[j].second));
                if (area > max_area)
                {
                    max_area = area;
                    max_index = j;
                }
            }

            // Include the selected point
            result.emplace_back(dataList[max_index]);
        }

        // Include the last data point
        result.emplace_back(dataList.back());

        return result;
    }
};

class AggregateFunctionLTTBDateTime64Float64 final
    : public IAggregateFunctionDataHelper<LTTBDataDateTime64Float64WithSort, AggregateFunctionLTTBDateTime64Float64>
{
private:
    UInt64 total_buckets{0};

public:
    explicit AggregateFunctionLTTBDateTime64Float64(const DataTypes & arguments, const Array & params, int scale)
        : IAggregateFunctionDataHelper<LTTBDataDateTime64Float64WithSort, AggregateFunctionLTTBDateTime64Float64>(
            {arguments}, {}, createResultType(scale))
    {
        if (params.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} require one parameter", getName());

        if (params[0].getType() != Field::Types::UInt64)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function {} require first parameter to be a UInt64", getName());

        total_buckets = params[0].get<UInt64>();
    }

    String getName() const override { return "lttb"; }

    bool allocatesMemoryInArena() const override { return true; }

    static DataTypePtr createResultType(int scale)
    {
        DataTypes types{
            std::make_shared<DataTypeDateTime64>(scale),
            std::make_shared<DataTypeNumber<Float64>>(),
        };

        Strings names{"x", "y"};

        auto tuple = std::make_shared<DataTypeTuple>(std::move(types), std::move(names));

        return std::make_shared<DataTypeArray>(tuple);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const auto x = static_cast<const ColumnDateTime64 &>(*columns[0]).getData()[row_num];
        const auto y = columns[1]->getFloat64(row_num);
        this->data(place).add(x, y, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & a = this->data(place);
        const auto & b = this->data(rhs);

        a.merge(b, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).deserialize(buf, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto res = this->data(place).getResult2(total_buckets);
        auto & col = assert_cast<ColumnArray &>(to);
        auto & col_offsets = assert_cast<ColumnArray::ColumnOffsets &>(col.getOffsetsColumn());

        for (size_t i = 0; i < res.size(); ++i)
        {
            auto & column_tuple = assert_cast<ColumnTuple &>(col.getData());
            auto & column_x = assert_cast<ColumnDateTime64 &>(column_tuple.getColumn(0));
            auto & column_y = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(1));

            column_x.getData().push_back(res[i].first);
            column_y.getData().push_back(res[i].second);
        }

        col_offsets.getData().push_back(col.getData().size());
    }
};

}
