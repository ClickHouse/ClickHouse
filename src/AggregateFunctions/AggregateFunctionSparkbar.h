#pragma once

#include <DataTypes/DataTypeString.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <base/range.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Columns/ColumnString.h>
#include <Common/logger_useful.h>
#include <IO/ReadBufferFromString.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{

template<typename X, typename Y>
struct AggregateFunctionSparkbarData
{

    using Points = HashMap<X, Y>;
    Points points;

    X min_x = std::numeric_limits<X>::max();
    X max_x = std::numeric_limits<X>::lowest();

    Y min_y = std::numeric_limits<Y>::max();
    Y max_y = std::numeric_limits<Y>::lowest();

    void insert(const X & x, const Y & y)
    {
        auto result = points.insert({x, y});
        if (!result.second)
            result.first->getMapped() += y;
    }

    void add(X x, Y y)
    {
        insert(x, y);
        min_x = std::min(x, min_x);
        max_x = std::max(x, max_x);
        min_y = std::min(y, min_y);
        max_y = std::max(y, max_y);
    }

    void merge(const AggregateFunctionSparkbarData & other)
    {
        if (other.points.empty())
            return;

        for (auto & point : other.points)
            insert(point.getKey(), point.getMapped());

        min_x = std::min(other.min_x, min_x);
        max_x = std::max(other.max_x, max_x);
        min_y = std::min(other.min_y, min_y);
        max_y = std::max(other.max_y, max_y);
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(min_x, buf);
        writeBinary(max_x, buf);
        writeBinary(min_y, buf);
        writeBinary(max_y, buf);
        writeVarUInt(points.size(), buf);

        for (const auto & elem : points)
        {
            writeBinary(elem.getKey(), buf);
            writeBinary(elem.getMapped(), buf);
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(min_x, buf);
        readBinary(max_x, buf);
        readBinary(min_y, buf);
        readBinary(max_y, buf);
        size_t size;
        readVarUInt(size, buf);

        /// TODO Protection against huge size
        X x;
        Y y;
        for (size_t i = 0; i < size; ++i)
        {
            readBinary(x, buf);
            readBinary(y, buf);
            insert(x, y);
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
    bool specified_min_max_x;

    template <class T>
    String getBar(const T value) const
    {
        if (isNaN(value) || value > 8 || value < 1)
            return " ";

        // ▁▂▃▄▅▆▇█
        switch (static_cast<UInt8>(value))
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
        if (data.points.empty() || !width)
            return value;

        size_t diff_x;
        X min_x_local;
        if (specified_min_max_x)
        {
            diff_x = max_x - min_x;
            min_x_local = min_x;
        }
        else
        {
            diff_x = data.max_x - data.min_x;
            min_x_local = data.min_x;
        }

        if ((diff_x + 1) <= width)
        {
            Y min_y = data.min_y;
            Y max_y = data.max_y;
            Float64 diff_y = max_y - min_y;

            if (diff_y)
            {
                for (size_t i = 0; i <= diff_x; ++i)
                {
                    auto it = data.points.find(min_x_local + i);
                    bool found = it != data.points.end();
                    value += getBar(found ? std::round(((it->getMapped() - min_y) / diff_y) * 7) + 1 : 0.0);
                }
            }
            else
            {
                for (size_t i = 0; i <= diff_x; ++i)
                    value += getBar(data.points.has(min_x_local + i) ? 1 : 0);
            }
        }
        else
        {
            // begin reshapes to width buckets
            Float64 multiple_d = (diff_x + 1) / static_cast<Float64>(width);

            std::optional<Float64> min_y;
            std::optional<Float64> max_y;

            std::optional<Float64> new_y;
            std::vector<std::optional<Float64>> new_points;
            new_points.reserve(width);

            std::pair<size_t, Float64> bound{0, 0.0};
            size_t cur_bucket_num = 0;
            // upper bound for bucket
            auto upper_bound = [&](size_t bucket_num)
            {
                bound.second = (bucket_num + 1) * multiple_d;
                bound.first = std::floor(bound.second);
            };
            upper_bound(cur_bucket_num);
            for (size_t i = 0; i <= (diff_x + 1); ++i)
            {
                if (i == bound.first) // is bound
                {
                    Float64 proportion = bound.second - bound.first;
                    auto it = data.points.find(min_x_local + i);
                    bool found = (it != data.points.end());
                    if (found && proportion > 0)
                        new_y = new_y.value_or(0) + it->getMapped() * proportion;

                    if (new_y)
                    {
                        Float64 avg_y = new_y.value() / multiple_d;

                        new_points.emplace_back(avg_y);
                        // If min_y has no value, or if the avg_y of the current bucket is less than min_y, update it.
                        if (!min_y || avg_y < min_y)
                            min_y = avg_y;
                        if (!max_y || avg_y > max_y)
                            max_y = avg_y;
                    }
                    else
                    {
                        new_points.emplace_back();
                    }

                    // next bucket
                    new_y = found ? ((1 - proportion) * it->getMapped()) : std::optional<Float64>();
                    upper_bound(++cur_bucket_num);
                }
                else
                {
                    auto it = data.points.find(min_x_local + i);
                    if (it != data.points.end())
                        new_y = new_y.value_or(0) + it->getMapped();
                }
            }

            if (!min_y || !max_y) // No value is set
                return {};

            Float64 diff_y = max_y.value() - min_y.value();

            auto get_bars = [&] (const std::optional<Float64> & point_y)
            {
                value += getBar(point_y ? std::round(((point_y.value() - min_y.value()) / diff_y) * 7) + 1 : 0);
            };
            auto get_bars_for_constant = [&] (const std::optional<Float64> & point_y)
            {
                value += getBar(point_y ? 1 : 0);
            };

            if (diff_y)
                std::for_each(new_points.begin(), new_points.end(), get_bars);
            else
                std::for_each(new_points.begin(), new_points.end(), get_bars_for_constant);
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
            specified_min_max_x = true;
            min_x = params.at(1).safeGet<X>();
            max_x = params.at(2).safeGet<X>();
        }
        else
        {
            specified_min_max_x = false;
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

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
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
