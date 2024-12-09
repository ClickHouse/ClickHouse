#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>

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
#include <Common/assert_cast.h>
#include <Common/PODArray.h>
#include <Common/iota.h>
#include <base/types.h>

#include <boost/math/distributions/normal.hpp>


namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace DB
{
struct Settings;

namespace
{

struct LargestTriangleThreeBucketsData : public StatisticalSample<Float64, Float64>
{
    void add(const Float64 xval, const Float64 yval, Arena * arena)
    {
        /// We need to ensure either both or neither coordinates are saved (StatisticalSample ignores NaNs)
        if (isNaN(xval) || isNaN(yval))
            return;
        this->addX(xval, arena);
        this->addY(yval, arena);
    }

    void sort(Arena * arena)
    {
        chassert(this->x.size() == this->y.size());
        // sort the this->x and this->y in ascending order of this->x using index
        std::vector<size_t> index(this->x.size());

        iota(index.data(), index.size(), size_t(0));
        ::sort(index.begin(), index.end(), [&](size_t i1, size_t i2) { return this->x[i1] < this->x[i2]; });

        SampleX temp_x{};
        SampleY temp_y{};

        for (size_t i = 0; i < this->x.size(); ++i)
        {
            temp_x.push_back(this->x[index[i]], arena);
            temp_y.push_back(this->y[index[i]], arena);
        }

        for (size_t i = 0; i < this->x.size(); ++i)
        {
            this->x[i] = temp_x[i];
            this->y[i] = temp_y[i];
        }
    }

    PODArray<std::pair<Float64, Float64>> getResult(size_t total_buckets, Arena * arena)
    {
        // Sort the data
        this->sort(arena);

        PODArray<std::pair<Float64, Float64>> result;

        // Handle special cases for small data list
        if (this->x.size() <= total_buckets)
        {
            for (size_t i = 0; i < this->x.size(); ++i)
            {
                result.emplace_back(std::make_pair(this->x[i], this->y[i]));
            }
            return result;
        }

        // Handle special cases for 0 or 1 or 2 buckets
        if (total_buckets == 0)
            return result;
        if (total_buckets == 1)
        {
            result.emplace_back(std::make_pair(this->x.front(), this->y.front()));
            return result;
        }
        if (total_buckets == 2)
        {
            result.emplace_back(std::make_pair(this->x.front(), this->y.front()));
            result.emplace_back(std::make_pair(this->x.back(), this->y.back()));
            return result;
        }

        // Find the size of each bucket
        Float64 single_bucket_size = static_cast<Float64>(this->x.size() - 2) / static_cast<Float64>(total_buckets - 2);

        // Include the first data point
        result.emplace_back(std::make_pair(this->x[0], this->y[0]));

        // the start index of current bucket
        size_t start_index = 1;
        // the end index of current bucket, also is the start index of next bucket
        size_t center_index = start_index + static_cast<int>(floor(single_bucket_size));

        for (size_t i = 0; i < total_buckets - 2; ++i) // Skip the first and last bucket
        {
            // the end index of next bucket
            size_t end_index = 1 + static_cast<int>(floor(single_bucket_size * (i + 2)));
            // current bucket is the last bucket
            end_index = std::min(end_index, this->x.size());

            // Compute the average point in the next bucket
            Float64 avg_x = 0;
            Float64 avg_y = 0;
            for (size_t j = center_index; j < end_index; ++j)
            {
                avg_x += this->x[j];
                avg_y += this->y[j];
            }
            avg_x /= static_cast<Float64>(end_index - center_index);
            avg_y /= static_cast<Float64>(end_index - center_index);

            // Find the point in the current bucket that forms the largest triangle
            size_t max_index = start_index;
            Float64 max_area = 0.0;
            for (size_t j = start_index; j < center_index; ++j)
            {
                Float64 area = std::abs(
                    0.5
                    * (result.back().first * this->y[j] + this->x[j] * avg_y + avg_x * result.back().second - result.back().first * avg_y
                       - this->x[j] * result.back().second - avg_x * this->y[j]));
                if (area > max_area)
                {
                    max_area = area;
                    max_index = j;
                }
            }

            // Include the selected point
            result.emplace_back(std::make_pair(this->x[max_index], this->y[max_index]));

            start_index = center_index;
            center_index = end_index;
        }

        // Include the last data point
        result.emplace_back(std::make_pair(this->x.back(), this->y.back()));

        return result;
    }
};

class AggregateFunctionLargestTriangleThreeBuckets final : public IAggregateFunctionDataHelper<LargestTriangleThreeBucketsData, AggregateFunctionLargestTriangleThreeBuckets>
{
private:
    UInt64 total_buckets{0};
    TypeIndex x_type;
    TypeIndex y_type;

public:
    explicit AggregateFunctionLargestTriangleThreeBuckets(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<LargestTriangleThreeBucketsData, AggregateFunctionLargestTriangleThreeBuckets>({arguments}, {}, createResultType(arguments))
    {
        if (params.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} require one parameter", getName());

        if (params[0].getType() != Field::Types::UInt64)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function {} require first parameter to be a UInt64", getName());

        total_buckets = params[0].safeGet<UInt64>();

        this->x_type = WhichDataType(arguments[0]).idx;
        this->y_type = WhichDataType(arguments[1]).idx;
    }

    static constexpr auto name = "largestTriangleThreeBuckets";

    String getName() const override { return name; }

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
        data(place).add(x, y, arena);
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
        auto & a = data(place);
        const auto & b = data(rhs);

        a.merge(b, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        data(place).read(buf, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        auto res = data(place).getResult(total_buckets, arena);

        auto & col = assert_cast<ColumnArray &>(to);
        auto & col_offsets = assert_cast<ColumnArray::ColumnOffsets &>(col.getOffsetsColumn());

        auto column_x_adder_func = getColumnAdderFunc(x_type);
        auto column_y_adder_func = getColumnAdderFunc(y_type);

        for (const auto & elem : res)
        {
            auto & column_tuple = assert_cast<ColumnTuple &>(col.getData());
            column_x_adder_func(column_tuple.getColumn(0), elem.first);
            column_y_adder_func(column_tuple.getColumn(1), elem.second);
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


AggregateFunctionPtr
createAggregateFunctionLargestTriangleThreeBuckets(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertBinary(name, argument_types);

    if (!(isNumber(argument_types[0]) || isDateOrDate32(argument_types[0]) || isDateTime(argument_types[0])
          || isDateTime64(argument_types[0])))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Aggregate function {} only supports Date, Date32, DateTime, DateTime64 and Number as the first argument",
            name);

    if (!(isNumber(argument_types[1]) || isDateOrDate32(argument_types[1]) || isDateTime(argument_types[1])
          || isDateTime64(argument_types[1])))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Aggregate function {} only supports Date, Date32, DateTime, DateTime64 and Number as the second argument",
            name);

    return std::make_shared<AggregateFunctionLargestTriangleThreeBuckets>(argument_types, parameters);
}

}


void registerAggregateFunctionLargestTriangleThreeBuckets(AggregateFunctionFactory & factory)
{
    factory.registerFunction(AggregateFunctionLargestTriangleThreeBuckets::name, createAggregateFunctionLargestTriangleThreeBuckets);
    factory.registerAlias("lttb", AggregateFunctionLargestTriangleThreeBuckets::name);
}


}
