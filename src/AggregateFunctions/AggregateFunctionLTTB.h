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

struct LTTBData final
{
    using DataPair = std::pair<Float64, Float64>;
    using ComparatorLess = ComparePairFirst<std::less>;
    using ComparatorGreater = ComparePairFirst<std::greater>;
    using AllocatorSample = MixedAlignedArenaAllocator<alignof(DataPair), 4096>;
    using DataList = PODArray<DataPair, 32, AllocatorSample>;

//    using ResultType = PODArray<std::pair<Float64, Float64>, 32, MixedAlignedArenaAllocator<alignof(std::pair<Float64, Float64>), 4096>>;

    bool sorted = true;

    DataList dataList{};

    void add(const Float64 x, const Float64 y, Arena * arena)
    {
        dataList.push_back(std::make_pair(x, y), arena);
        sorted = false;
    }

    void merge(const LTTBData & other, Arena * arena)
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
            Float64 x;
            readBinary(x, buf);

            Float64 y;
            readBinary(y, buf);

            dataList.push_back(std::make_pair(x, y), arena);
        }
    }

    PODArray<std::pair<Float64,Float64>> getResult(UInt64 total_buckets)
    {
        sort(true);
        unsigned long int_total_buckets = total_buckets;
        PODArray<std::pair<Float64,Float64>> result{};

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
            Float64 avg_x = 0;
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

class AggregateFunctionLTTB final
    : public IAggregateFunctionDataHelper<LTTBData, AggregateFunctionLTTB>
{
private:
    UInt64 total_buckets{0};
    TypeIndex x_type;
public:
    explicit AggregateFunctionLTTB(const DataTypes & arguments, const Array & params, int xScale ,TypeIndex xType)
        : IAggregateFunctionDataHelper<LTTBData, AggregateFunctionLTTB>(
            {arguments},
            {},
            createResultType(xScale, xType)
            )
    {
        if (params.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} require one parameter", getName());

        if (params[0].getType() != Field::Types::UInt64)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function {} require first parameter to be a UInt64", getName());

        total_buckets = params[0].get<UInt64>();

        this->x_type = xType;
    }

    String getName() const override { return "lttb2"; }

    bool allocatesMemoryInArena() const override { return true; }

    static DataTypePtr createResultType(int x_scale, TypeIndex xType)
    {
        DataTypes types;
        if (xType == TypeIndex::DateTime64)
        {
            types = {
                std::make_shared<DataTypeDateTime64>(x_scale),
                std::make_shared<DataTypeNumber<Float64>>(),
            };
        } else if (xType == TypeIndex::DateTime)
        {
            types = {
                std::make_shared<DataTypeDateTime>(),
                std::make_shared<DataTypeNumber<Float64>>(),
            };
        }
        else
        {
             types = {
                std::make_shared<DataTypeNumber<Float64>>(),
                std::make_shared<DataTypeNumber<Float64>>(),
            };
        }

        Strings names{"x", "y"};

        auto tuple = std::make_shared<DataTypeTuple>(std::move(types), std::move(names));

        return std::make_shared<DataTypeArray>(tuple);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        Float64 x = 0;
        if (this->x_type == TypeIndex::DateTime64)
        {
            x = static_cast<const ColumnDateTime64 &>(*columns[0]).getData()[row_num];
        }
        else if (this->x_type == TypeIndex::DateTime)
        {
            x = static_cast<const ColumnDateTime &>(*columns[0]).getData()[row_num];
        }
        else
        {
            x = columns[0]->getFloat64(row_num);
        }
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
        auto res = this->data(place).getResult(total_buckets);
        auto & col = assert_cast<ColumnArray &>(to);
        auto & col_offsets = assert_cast<ColumnArray::ColumnOffsets &>(col.getOffsetsColumn());

        for (size_t i = 0; i < res.size(); ++i)
        {
            auto & column_tuple = assert_cast<ColumnTuple &>(col.getData());

            if (x_type == TypeIndex::DateTime64)
            {
                auto & column_x = assert_cast<ColumnDateTime64 &>(column_tuple.getColumn(0));
                DateTime64 val = DateTime64(static_cast<long long>(res[i].first));
                column_x.getData().push_back(val);
            }
            else if (x_type == TypeIndex::DateTime)
            {
                auto & column_x = assert_cast<ColumnDateTime &>(column_tuple.getColumn(0));
                unsigned int val =static_cast<unsigned int>(res[i].first);
                column_x.getData().push_back(val);
            }
            else
            {
                auto & column_x = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(0));
                column_x.getData().push_back(res[i].first);
            }

            auto & column_y = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(1));
            column_y.getData().push_back(res[i].second);
        }

        col_offsets.getData().push_back(col.getData().size());
    }
};

}
