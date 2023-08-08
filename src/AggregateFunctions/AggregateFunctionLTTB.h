#pragma once

#include <iostream>
#include <limits>
#include <numeric>
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


struct LTTBData final
{
    using AllocatorSample = MixedAlignedArenaAllocator<alignof(Float64), 4096>;
    using DataList = PODArray<Float64, 32, AllocatorSample>;

    DataList data_list_x{};
    DataList data_list_y{};

    bool sorted = true;

    void add(const Float64 x, const Float64 y, Arena * arena)
    {
        data_list_x.push_back(x, arena);
        data_list_y.push_back(y, arena);
        sorted = false;
    }

    void merge(const LTTBData & other, Arena * arena)
    {
        if (other.data_list_x.empty() || other.data_list_y.empty())
            return;

        data_list_x.insert(std::begin(other.data_list_x), std::end(other.data_list_x), arena);
        data_list_y.insert(std::begin(other.data_list_y), std::end(other.data_list_y), arena);
        sorted = false;
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(sorted, buf);
        writeBinary(data_list_x.size(), buf);

        for (const auto & x : data_list_x)
            writeBinary(x, buf);

        for (const auto & y : data_list_y)
            writeBinary(y, buf);
    }

    void deserialize(ReadBuffer & buf, Arena * arena)
    {
        readBinary(sorted, buf);
        size_t size;
        readBinary(size, buf);

        data_list_x.clear();
        data_list_y.clear();
        data_list_x.reserve(size, arena);
        data_list_y.reserve(size, arena);

        for (size_t i = 0; i < size; ++i)
        {
            Float64 x;
            readBinary(x, buf);
            data_list_x.push_back(x, arena);
        }

        for (size_t i = 0; i < size; ++i)
        {
            Float64 y;
            readBinary(y, buf);
            data_list_y.push_back(y, arena);
        }
    }

    void sort()
    {
        if (sorted)
            return;

        // sort the data_list_x and data_list_y in ascending order of data_list_x using index
        std::vector<size_t> index(data_list_x.size());
        std::iota(index.begin(), index.end(), 0);
        std::sort(index.begin(), index.end(), [&](size_t i1, size_t i2) { return data_list_x[i1] < data_list_x[i2]; });

        PODArray<Float64> data_list_x_temp;
        PODArray<Float64> data_list_y_temp;

        for (size_t i = 0; i < data_list_x.size(); ++i)
        {
            data_list_x_temp.push_back(data_list_x[index[i]]);
            data_list_y_temp.push_back(data_list_y[index[i]]);
        }

        for (size_t i = 0; i < data_list_x.size(); ++i)
        {
            data_list_x[i] = data_list_x_temp[i];
            data_list_y[i] = data_list_y_temp[i];
        }

        sorted = true;
    }

    PODArray<std::pair<Float64, Float64>> getResult(unsigned long total_buckets)
    {
        sort();

        PODArray<std::pair<Float64, Float64>> result;

        // Handle special cases for small data list
        if (data_list_x.size() <= total_buckets)
        {
            for (unsigned long i = 0; i < data_list_x.size(); ++i)
            {
                result.emplace_back(std::make_pair(data_list_x[i], data_list_y[i]));
            }
            return result;
        }

        // Handle special cases for 0 or 1 or 2 buckets
        if (total_buckets == 0)
            return result;
        if (total_buckets == 1)
        {
            result.emplace_back(std::make_pair(data_list_x.front(), data_list_y.front()));
            return result;
        }
        if (total_buckets == 2)
        {
            result.emplace_back(std::make_pair(data_list_x.front(), data_list_y.front()));
            result.emplace_back(std::make_pair(data_list_x.back(), data_list_y.back()));
            return result;
        }


        // Find the size of each bucket
        unsigned long single_bucket_size = data_list_x.size() / total_buckets;

        // Include the first data point
        result.emplace_back(std::make_pair(data_list_x[0], data_list_y[0]));

        for (unsigned long i = 1; i < total_buckets - 1; ++i) // Skip the first and last bucket
        {
            unsigned long start_index = i * single_bucket_size;
            unsigned long end_index = (i + 1) * single_bucket_size;

            // Compute the average point in the next bucket
            Float64 avg_x = 0;
            Float64 avg_y = 0;
            for (unsigned long j = end_index; j < (i + 2) * single_bucket_size; ++j)
            {
                avg_x += data_list_x[j];
                avg_y += data_list_y[j];
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
                    * (result.back().first * data_list_y[j] + data_list_x[j] * avg_y + avg_x * result.back().second
                       - result.back().first * avg_y - data_list_x[j] * result.back().second - avg_x * data_list_y[j]));
                if (area > max_area)
                {
                    max_area = area;
                    max_index = j;
                }
            }

            // Include the selected point
            result.emplace_back(std::make_pair(data_list_x[max_index], data_list_y[max_index]));
        }

        // Include the last data point
        result.emplace_back(std::make_pair(data_list_x.back(), data_list_y.back()));

        return result;
    }
};

class AggregateFunctionLTTB final : public IAggregateFunctionDataHelper<LTTBData, AggregateFunctionLTTB>
{
private:
    UInt64 total_buckets{0};
    TypeIndex x_type;
    TypeIndex y_type;

public:
    explicit AggregateFunctionLTTB(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<LTTBData, AggregateFunctionLTTB>({arguments}, {}, createResultType(arguments))
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
        TypeIndex x_type = arguments[0]->getTypeId();
        TypeIndex y_type = arguments[1]->getTypeId();

        UInt32 x_scale = 0;
        UInt32 y_scale = 0;

        if (const auto * datetime64_type = typeid_cast<const DataTypeDateTime64 *>(arguments[0].get()))
        {
            x_scale = datetime64_type->getScale();
        }

        if (const auto * datetime64_type = typeid_cast<const DataTypeDateTime64 *>(arguments[1].get()))
        {
            y_scale = datetime64_type->getScale();
        }


        DataTypes types = {getDataTypeFromTypeIndex(x_type, x_scale), getDataTypeFromTypeIndex(y_type, y_scale)};

        auto tuple = std::make_shared<DataTypeTuple>(std::move(types));

        return std::make_shared<DataTypeArray>(tuple);
    }

    static DataTypePtr getDataTypeFromTypeIndex(TypeIndex type_index, UInt32 scale)
    {
        DataTypePtr data_type;
        switch (type_index)
        {
            case TypeIndex::Date:
                data_type = std::make_shared<DataTypeDate>();
                break;
            case TypeIndex::Date32:
                data_type = std::make_shared<DataTypeDate32>();
                break;
            case TypeIndex::DateTime:
                data_type = std::make_shared<DataTypeDateTime>();
                break;
            case TypeIndex::DateTime64:
                data_type = std::make_shared<DataTypeDateTime64>(scale);
                break;
            default:
                data_type = std::make_shared<DataTypeNumber<Float64>>();
        }
        return data_type;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        Float64 x = getFloat64DataFromColumn(columns[0], row_num, this->x_type);
        Float64 y = getFloat64DataFromColumn(columns[1], row_num, this->y_type);
        this->data(place).add(x, y, arena);
    }

    Float64 getFloat64DataFromColumn(const IColumn * column, size_t row_num, TypeIndex type_index) const
    {
        switch (type_index)
        {
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

    std::function<void(IColumn &, Float64)> getColumnAdderFunc(TypeIndex type_index) const
    {
        switch (type_index)
        {
            case TypeIndex::Date:
                return [](IColumn & column, Float64 value)
                {
                    auto & col = assert_cast<ColumnDate &>(column);
                    col.getData().push_back(static_cast<UInt16>(value));
                };
            case TypeIndex::Date32:
                return [](IColumn & column, Float64 value)
                {
                    auto & col = assert_cast<ColumnDate32 &>(column);
                    col.getData().push_back(static_cast<UInt32>(value));
                };
            case TypeIndex::DateTime:
                return [](IColumn & column, Float64 value)
                {
                    auto & col = assert_cast<ColumnDateTime &>(column);
                    col.getData().push_back(static_cast<UInt32>(value));
                };
            case TypeIndex::DateTime64:
                return [](IColumn & column, Float64 value)
                {
                    auto & col = assert_cast<ColumnDateTime64 &>(column);
                    col.getData().push_back(static_cast<UInt64>(value));
                };
            default:
                return [](IColumn & column, Float64 value)
                {
                    auto & col = assert_cast<ColumnFloat64 &>(column);
                    col.getData().push_back(value);
                };
        }
    }
};

}
