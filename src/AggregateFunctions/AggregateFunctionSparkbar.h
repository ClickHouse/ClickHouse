#pragma once

#include <DataTypes/DataTypeString.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <common/range.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Columns/ColumnString.h>
#include <common/logger_useful.h>
#include <IO/ReadBufferFromString.h>

namespace DB
{

template<typename X, typename Y>
struct AggregateFunctionSparkbarData
{
    using Points = std::map<X, Y>;

    Points points;

    Y min_y = std::numeric_limits<Y>::max();
    Y max_y = std::numeric_limits<Y>::min();

    void add(X x, Y y)
    {
        auto it = points.find(x);
        if (it != points.end())
            it->second += y;
        else
            points.emplace(x, y);
        if (y < min_y)
            min_y = y;
        if (y > max_y)
            max_y = y;
    }

    void merge(const AggregateFunctionSparkbarData & other)
    {
        if (other.points.empty())
            return;

        for (auto & point : other.points)
        {
            auto it = points.find(point.first);
            if (it != points.end())
                it->second += point.second;
            else
                points.emplace(point.first, point.second);
        }

        if (other.min_y < min_y)
            min_y = other.min_y;

        if (other.max_y > max_y)
            max_y = other.max_y;
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(min_y, buf);
        writeBinary(max_y, buf);
        writeVarUInt(points.size(), buf);

        for (const auto & elem : points)
        {
            writeBinary(elem.first, buf);
            writeBinary(elem.second, buf);
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        readPODBinary(min_y, buf);
        readPODBinary(max_y, buf);
        size_t size;
        readVarUInt(size, buf);

        /// TODO Protection against huge size

        points.clear();

        X x;
        Y y;
        for (size_t i = 0; i < size; ++i)
        {
            readBinary(x, buf);
            readBinary(y, buf);
            points.emplace(x, y);
        }
    }

};

template<typename X, typename Y>
class AggregateFunctionSparkbar final
    : public IAggregateFunctionDataHelper<AggregateFunctionSparkbarData<X, Y>, AggregateFunctionSparkbar<X, Y>>
{

private:
    size_t width;
    X min_x;
    X max_x;

    String getBar(const UInt8 value) const
    {
        // ▁▂▃▄▅▆▇█
        switch (value)
        {
            case 1: return "▁";
            case 2: return "▂";
            case 3: return "▃";
            case 4: return "▄";
            case 5: return "▅";
            case 6: return "▆";
            case 7: return "▇";
            case 8: return "█";
        }
        return " ";
    }

    /**
     *  The minimum value of y is rendered as the lowest height "▁",
     *  the maximum value of y is rendered as the highest height "█", and the middle value will be rendered proportionally.
     *  If a bucket has no y value, it will be rendered as " ".
     *  If the actual number of buckets is greater than the specified bucket, it will be compressed by width.
     *  For example, there are actually 11 buckets, specify 10 buckets, and divide the 11 buckets as follows (11/10):
     *  0.0-1.1, 1.1-2.2, 2.2-3.3, 3.3-4.4, 4.4-5.5, 5.5-6.6, 6.6-7.7, 7.7-8.8, 8.8-9.9, 9.9-11.
     *  The y value of the first bucket will be calculated as follows:
     *  the actual y value of the first position + the actual second position y*0.1, and the remaining y*0.9 is reserved for the next bucket.
     *  The next bucket will use the last y*0.9 + the actual third position y*0.2, and the remaining y*0.8 will be reserved for the next bucket. And so on.
     */
    String render(const AggregateFunctionSparkbarData<X, Y> & data) const
    {
        String value;
        X local_min_x = data.points.begin()->first;
        X local_max_x = data.points.rbegin()->first;
        size_t diff_x = local_max_x - local_min_x;
        if ((diff_x + 1) <= width)
        {
            Y min_y = data.min_y;
            Y max_y = data.max_y;
            Float64 diff_y = max_y - min_y;
            for (size_t i = 0; i <= diff_x; ++i)
            {
                auto it = data.points.find(local_min_x + i);
                bool found = it != data.points.end();
                value += getBar(found ? std::round(((it->second - min_y) / diff_y) * 7) + 1 : 0);
            }
        }
        else
        {
            // begin reshapes to width buckets
            Float64 multiple_d = (diff_x + 1) / static_cast<Float64>(width);

            // init bounds
            std::map<size_t, Float64> bounds;
            for (size_t i = 1; i <= width; ++i)
            {
                Float64 bound = i * multiple_d;
                bounds.template emplace(std::floor(bound), bound);
            }

            std::optional<Float64> min_y;
            std::optional<Float64> max_y;

            std::optional<Float64> new_y;
            std::map<size_t, std::optional<Float64>> newPoints;
            for (size_t i = 0; i <= (diff_x + 1); ++i)
            {
                auto bound = bounds.find(i);
                if (bound != bounds.end()) // is bound
                {
                    Float64 proportion = bound->second - bound->first;
                    auto it = data.points.find(local_min_x + i);
                    bool found = (it != data.points.end());
                    if (found)
                        new_y ? new_y = new_y.value() + it->second * proportion : new_y = it->second * proportion;

                    // find bucket_num
                    size_t bucket_num = 0;
                    for (auto & b : bounds)
                    {
                        if (i <= b.second)
                            break;
                        else
                            ++bucket_num;
                    }

                    if (new_y)
                    {
                        Float64 avg_y = new_y.value() / multiple_d;

                        newPoints.template emplace(bucket_num, avg_y);
                        if (!min_y || avg_y < min_y)
                            min_y = avg_y;
                        if (!max_y || avg_y > max_y)
                            max_y = avg_y;
                    }
                    else
                    {
                        newPoints.template emplace(bucket_num, std::optional<Float64>());
                    }

                    // next bucket
                    new_y = found ? ((1 - proportion) * it->second) : std::optional<Float64>();
                }
                else
                {
                    auto it = data.points.find(local_min_x + i);
                    if (it != data.points.end())
                        new_y ? new_y = new_y.value() + it->second : new_y = it->second;
                }
            }

            if (!min_y || !max_y)
                return {};

            Float64 diff_y = max_y.value() - min_y.value();

            auto getBars = [&] (std::pair<size_t, std::optional<Float64>> point)
            {
                value += getBar(point.second ? std::round(((point.second.value() - min_y.value()) / diff_y) * 7) + 1 : 0);
            };
            auto getBarsForConstant = [&] (std::pair<size_t, std::optional<Float64>> point)
            {
                value += getBar(point.second ?  1 : 0);
            };

            if (diff_y)
                std::for_each(newPoints.begin(), newPoints.end(), getBars);
            else
                std::for_each(newPoints.begin(), newPoints.end(), getBarsForConstant);
        }
        return value;
    }


public:
    AggregateFunctionSparkbar(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionSparkbarData<X, Y>, AggregateFunctionSparkbar>(
        arguments, params)
    {
        width = params.at(0).safeGet<UInt64>();
        if (params.size() == 3)
        {
            min_x = params.at(1).safeGet<X>();
            max_x = params.at(2).safeGet<X>();
        }
        else
        {
            min_x = std::numeric_limits<X>::min();
            max_x = std::numeric_limits<X>::max();
        }
    }

    String getName() const override
    {
        return "sparkbar";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeString>();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * /*arena*/) const override
    {
        X x = assert_cast<const ColumnVector<X> *>(columns[0])->getData()[row_num];
        if (min_x <= x && x <= max_x)
        {
            Y y = assert_cast<const ColumnVector<Y> *>(columns[1])->getData()[row_num];
            this->data(place).add(x, y);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * /*arena*/) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    bool allocatesMemoryInArena() const override { return false; }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * /*arena*/) const override
    {
        auto & to_column = assert_cast<ColumnString &>(to);
        const auto & data = this->data(place);
        const String & value = render(data);
        to_column.insertData(value.data(), value.size());
    }
};

}
