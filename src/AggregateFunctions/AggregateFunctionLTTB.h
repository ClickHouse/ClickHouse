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
#include <numeric>

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


struct LTTBData final
{
    using AllocatorSample = MixedAlignedArenaAllocator<alignof(Float64), 4096>;
    using DataList = PODArray<Float64, 32, AllocatorSample>;

    DataList dataList_x{};
    DataList dataList_y{};

    void add(const Float64 x, const Float64 y, Arena * arena)
    {
        dataList_x.push_back(x, arena);
        dataList_y.push_back(y, arena);
    }

    void merge(const LTTBData & other, Arena * arena)
    {
        if (other.dataList_x.empty() || other.dataList_y.empty())
            return;

        dataList_x.insert(std::begin(other.dataList_x), std::end(other.dataList_x), arena);
        dataList_y.insert(std::begin(other.dataList_y), std::end(other.dataList_y), arena);
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(dataList_x.size(), buf);

        for (const auto & x : dataList_x)
            writeBinary(x, buf);

        for (const auto & y : dataList_y)
            writeBinary(y, buf);
    }

    void deserialize(ReadBuffer & buf, Arena * arena)
    {
        size_t size;
        readBinary(size, buf);

        dataList_x.clear();
        dataList_y.clear();
        dataList_x.reserve(size, arena);
        dataList_y.reserve(size, arena);

        for (size_t i = 0; i < size; ++i)
        {
            Float64 x;
            readBinary(x, buf);
            dataList_x.push_back(x, arena);
        }

        for (size_t i = 0; i < size; ++i)
        {
            Float64 y;
            readBinary(y, buf);
            dataList_y.push_back(y, arena);
        }
    }

    PODArray<std::pair<Float64, Float64>> getResult(UInt64 total_buckets)
    {
        unsigned long int_total_buckets = total_buckets;
        PODArray<std::pair<Float64, Float64>> result;

        // Handle special cases for small dataList
        if (dataList_x.size() <= int_total_buckets)
        {
            for (unsigned long i = 0; i < dataList_x.size(); ++i)
            {
                result.emplace_back(std::make_pair(dataList_x[i], dataList_y[i]));
            }
            return result;
        }

        // Find the size of each bucket
        unsigned long single_bucket_size = dataList_x.size() / total_buckets;

        // Include the first data point
        result.emplace_back(std::make_pair(dataList_x[0], dataList_y[0]));

        for (unsigned long i = 1; i < int_total_buckets - 1; ++i) // Skip the first and last bucket
        {
            unsigned long start_index = i * single_bucket_size;
            unsigned long end_index = (i + 1) * single_bucket_size;

            // Compute the average point in the next bucket
            Float64 avg_x = 0;
            Float64 avg_y = 0;
            for (unsigned long j = end_index; j < (i + 2) * single_bucket_size; ++j)
            {
                avg_x += dataList_x[j];
                avg_y += dataList_y[j];
            }
            avg_x /= single_bucket_size;
            avg_y /= single_bucket_size;

            // Find the point in the current bucket that forms the largest triangle
            unsigned long max_index = start_index;
            Float64 max_area = 0.0;
            for (unsigned long j = start_index; j < end_index; ++j)
            {
                Float64 area = std::abs(
                    0.5
                    * (result.back().first * dataList_y[j] + dataList_x[j] * avg_y + avg_x * result.back().second
                       - result.back().first * avg_y - dataList_x[j] * result.back().second - avg_x * dataList_y[j]));
                if (area > max_area)
                {
                    max_area = area;
                    max_index = j;
                }
            }

            // Include the selected point
            result.emplace_back(std::make_pair(dataList_x[max_index], dataList_y[max_index]));
        }

        // Include the last data point
        result.emplace_back(std::make_pair(dataList_x.back(), dataList_y.back()));

        return result;
    }
};

class AggregateFunctionLTTB final
    : public IAggregateFunctionDataHelper<LTTBData, AggregateFunctionLTTB>
{
private:
    UInt64 total_buckets{0};
    TypeIndex x_type;
    TypeIndex y_type;

public:
    explicit AggregateFunctionLTTB(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<LTTBData, AggregateFunctionLTTB>(
            {arguments},
            {},
            createResultType(arguments)
            )
    {
        if (params.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} require one parameter", getName());

        if (params[0].getType() != Field::Types::UInt64)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function {} require first parameter to be a UInt64", getName());

        total_buckets = params[0].get<UInt64>();

        this->x_type = WhichDataType(arguments[0]).idx;
        this->y_type = WhichDataType(arguments[1]).idx;

    }

    String getName() const override { return "lttb"; }

    bool allocatesMemoryInArena() const override { return true; }

    static DataTypePtr createResultType(const DataTypes & arguments)
    {
        TypeIndex xType = arguments[0]->getTypeId();
        TypeIndex yType = arguments[1]->getTypeId();

        UInt32 xScale = 0;
        UInt32 yScale = 0;

        if (const auto * datetime64_type = typeid_cast<const DataTypeDateTime64 *>(arguments[0].get()))
        {
            xScale = datetime64_type->getScale();
        }

        if (const auto * datetime64_type = typeid_cast<const DataTypeDateTime64 *>(arguments[1].get()))
        {
            yScale = datetime64_type->getScale();
        }


        DataTypes types = {getDataTypeFromTypeIndex(xType, xScale), getDataTypeFromTypeIndex(yType, yScale)};

        auto tuple = std::make_shared<DataTypeTuple>(std::move(types));

        return std::make_shared<DataTypeArray>(tuple);
    }

    static DataTypePtr getDataTypeFromTypeIndex(TypeIndex typeIndex, UInt32 scale) {
        DataTypePtr data_type;
        switch (typeIndex) {
            case TypeIndex::Date:
                data_type = std::make_shared<DataTypeDate>();
                break;
            case TypeIndex::Date32:
                data_type =  std::make_shared<DataTypeDate32>();
                break;
            case TypeIndex::DateTime:
                data_type =  std::make_shared<DataTypeDateTime>();
                break;
            case TypeIndex::DateTime64:
                data_type =  std::make_shared<DataTypeDateTime64>(scale);
                break;
            default:
                data_type =  std::make_shared<DataTypeNumber<Float64>>();
        }
        return data_type;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        Float64 x = getFloat64DataFromColumn(columns[0], row_num, this->x_type);
        Float64 y = getFloat64DataFromColumn(columns[1], row_num, this->y_type);
        this->data(place).add(x, y, arena);
    }

    Float64 getFloat64DataFromColumn(const IColumn * column, size_t row_num, TypeIndex typeIndex) const {
        switch (typeIndex) {
            case TypeIndex::Date:
                return static_cast<const ColumnDate &>(*column).getData()[row_num];
            case TypeIndex::Date32:
                return static_cast<const ColumnDate32 &>(*column).getData()[row_num];
            case TypeIndex::DateTime:
                return static_cast<const ColumnDateTime &>(*column).getData()[row_num];
            case TypeIndex::DateTime64:
                return static_cast<const ColumnDateTime64 &>(*column).getData()[row_num];
            default:
                return column->getFloat64(row_num);
        }
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

        auto column_x_adder_func = getColumnAdderFunc(x_type);
        auto column_y_adder_func = getColumnAdderFunc(y_type);

        for (size_t i = 0; i < res.size(); ++i)
        {
            auto & column_tuple = assert_cast<ColumnTuple &>(col.getData());
            column_x_adder_func(column_tuple.getColumn(0), res[i].first);
            column_y_adder_func(column_tuple.getColumn(1), res[i].second);
        }

        col_offsets.getData().push_back(col.getData().size());
    }

    std::function<void(IColumn &, Float64)> getColumnAdderFunc(TypeIndex typeIndex) const {
        switch (typeIndex) {
            case TypeIndex::Date:
                return [](IColumn & column, Float64 value) {
                    auto & col = assert_cast<ColumnDate &>(column);
                    col.getData().push_back(static_cast<UInt16>(value));
                };
            case TypeIndex::Date32:
                return [](IColumn & column, Float64 value) {
                    auto & col = assert_cast<ColumnDate32 &>(column);
                    col.getData().push_back(static_cast<UInt32>(value));
                };
            case TypeIndex::DateTime:
                return [](IColumn & column, Float64 value) {
                    auto & col = assert_cast<ColumnDateTime &>(column);
                    col.getData().push_back(static_cast<UInt32>(value));
                };
            case TypeIndex::DateTime64:
                return [](IColumn & column, Float64 value) {
                    auto & col = assert_cast<ColumnDateTime64 &>(column);
                    col.getData().push_back(static_cast<UInt64>(value));
                };
            default:
                return [](IColumn & column, Float64 value) {
                    auto & col = assert_cast<ColumnFloat64 &>(column);
                    col.getData().push_back(value);
                };
        }
    }
};

}
