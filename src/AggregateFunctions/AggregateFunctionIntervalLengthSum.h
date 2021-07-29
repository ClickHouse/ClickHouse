#pragma once

#include <AggregateFunctions/AggregateFunctionNull.h>

#include <Columns/ColumnsNumber.h>

#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>

#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
}

/**
 * Calculate total length of intervals without intersections. Each interval is the pair of numbers [begin, end];
 * Return UInt64 for integral types (UInt/Int*, Date/DateTime) and return Float64 for Float*.
 *
 * Implementation simply stores intervals sorted by beginning and sums lengths at final.
 */
template <typename T>
struct AggregateFunctionIntervalLengthSumData
{
    constexpr static size_t MAX_ARRAY_SIZE = 0xFFFFFF;

    using Segment = std::pair<T, T>;
    using Segments = PODArrayWithStackMemory<Segment, 64>;

    bool sorted = false;

    Segments segments;

    void add(T begin, T end)
    {
        if (sorted && !segments.empty())
        {
            sorted = segments.back().first <= begin;
        }
        segments.emplace_back(begin, end);
    }

    void merge(const AggregateFunctionIntervalLengthSumData & other)
    {
        if (other.segments.empty())
            return;

        const auto size = segments.size();

        segments.insert(std::begin(other.segments), std::end(other.segments));

        /// either sort whole container or do so partially merging ranges afterwards
        if (!sorted && !other.sorted)
        {
            std::sort(std::begin(segments), std::end(segments));
        }
        else
        {
            const auto begin = std::begin(segments);
            const auto middle = std::next(begin, size);
            const auto end = std::end(segments);

            if (!sorted)
                std::sort(begin, middle);

            if (!other.sorted)
                std::sort(middle, end);

            std::inplace_merge(begin, middle, end);
        }

        sorted = true;
    }

    void sort()
    {
        if (!sorted)
        {
            std::sort(std::begin(segments), std::end(segments));
            sorted = true;
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(sorted, buf);
        writeBinary(segments.size(), buf);

        for (const auto & time_gap : segments)
        {
            writeBinary(time_gap.first, buf);
            writeBinary(time_gap.second, buf);
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(sorted, buf);

        size_t size;
        readBinary(size, buf);

        if (unlikely(size > MAX_ARRAY_SIZE))
            throw Exception("Too large array size", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        segments.clear();
        segments.reserve(size);

        Segment segment;
        for (size_t i = 0; i < size; ++i)
        {
            readBinary(segment.first, buf);
            readBinary(segment.second, buf);
            segments.emplace_back(segment);
        }
    }
};

template <typename T, typename Data>
class AggregateFunctionIntervalLengthSum final : public IAggregateFunctionDataHelper<Data, AggregateFunctionIntervalLengthSum<T, Data>>
{
private:
    template <typename TResult>
    TResult getIntervalLengthSum(Data & data) const
    {
        if (data.segments.empty())
            return 0;

        data.sort();

        TResult res = 0;

        typename Data::Segment cur_segment = data.segments[0];

        for (size_t i = 1, sz = data.segments.size(); i < sz; ++i)
        {
            /// Check if current interval intersect with next one then add length, otherwise advance interval end
            if (cur_segment.second < data.segments[i].first)
            {
                res += cur_segment.second - cur_segment.first;
                cur_segment = data.segments[i];
            }
            else
                cur_segment.second = std::max(cur_segment.second, data.segments[i].second);
        }

        res += cur_segment.second - cur_segment.first;

        return res;
    }

public:
    String getName() const override { return "intervalLengthSum"; }

    explicit AggregateFunctionIntervalLengthSum(const DataTypes & arguments)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionIntervalLengthSum<T, Data>>(arguments, {})
    {
    }

    DataTypePtr getReturnType() const override
    {
        if constexpr (std::is_floating_point_v<T>)
            return std::make_shared<DataTypeFloat64>();
        return std::make_shared<DataTypeUInt64>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function,
        const DataTypes & arguments,
        const Array & params,
        const AggregateFunctionProperties & /*properties*/) const override
    {
        return std::make_shared<AggregateFunctionNullVariadic<false, false, false>>(nested_function, arguments, params);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, const size_t row_num, Arena *) const override
    {
        auto begin = assert_cast<const ColumnVector<T> *>(columns[0])->getData()[row_num];
        auto end = assert_cast<const ColumnVector<T> *>(columns[1])->getData()[row_num];
        this->data(place).add(begin, end);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
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

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        if constexpr (std::is_floating_point_v<T>)
            assert_cast<ColumnFloat64 &>(to).getData().push_back(getIntervalLengthSum<Float64>(this->data(place)));
        else
            assert_cast<ColumnUInt64 &>(to).getData().push_back(getIntervalLengthSum<UInt64>(this->data(place)));
    }
};

}
