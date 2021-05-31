#pragma once

#include <unordered_set>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/AggregateFunctionNull.h>

namespace DB
{

template <typename T>
struct AggregateFunctionSegmentLengthSumData
{
    using Segment = std::pair<T, T>;
    using Segments = PODArrayWithStackMemory<Segment, 64>;

    bool sorted = false;

    Segments segments;

    size_t size() const { return segments.size(); }

    void add(T start, T end)
    {
        if (sorted && segments.size() > 0)
        {
            sorted = segments.back().first <= start;
        }
        segments.emplace_back(start, end);
    }

    void merge(const AggregateFunctionSegmentLengthSumData & other)
    {
        if (other.segments.empty())
            return;

        const auto size = segments.size();

        segments.insert(std::begin(other.segments), std::end(other.segments));

        /// either sort whole container or do so partially merging ranges afterwards
        if (!sorted && !other.sorted)
            std::stable_sort(std::begin(segments), std::end(segments));
        else
        {
            const auto begin = std::begin(segments);
            const auto middle = std::next(begin, size);
            const auto end = std::end(segments);

            if (!sorted)
                std::stable_sort(begin, middle);

            if (!other.sorted)
                std::stable_sort(middle, end);

            std::inplace_merge(begin, middle, end);
        }

        sorted = true;
    }

    void sort()
    {
        if (!sorted)
        {
            std::stable_sort(std::begin(segments), std::end(segments));
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

        segments.clear();
        segments.reserve(size);

        T start, end;

        for (size_t i = 0; i < size; ++i)
        {
            readBinary(start, buf);
            readBinary(end, buf);
            segments.emplace_back(start, end);
        }
    }
};

template <typename T, typename Data>
class AggregateFunctionSegmentLengthSum final : public IAggregateFunctionDataHelper<Data, AggregateFunctionSegmentLengthSum<T, Data>>
{
private:
    template <typename TResult>
    TResult getSegmentLengthSum(Data & data) const
    {
        if (data.size() == 0)
            return 0;

        data.sort();

        TResult res = 0;

        typename Data::Segment cur_segment = data.segments[0];

        for (size_t i = 1; i < data.segments.size(); ++i)
        {
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
    String getName() const override { return "segmentLengthSum"; }

    explicit AggregateFunctionSegmentLengthSum(const DataTypes & arguments)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionSegmentLengthSum<T, Data>>(arguments, {})
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
        auto start = assert_cast<const ColumnVector<T> *>(columns[0])->getData()[row_num];
        auto end = assert_cast<const ColumnVector<T> *>(columns[1])->getData()[row_num];
        this->data(place).add(start, end);
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
            assert_cast<ColumnFloat64 &>(to).getData().push_back(getSegmentLengthSum<Float64>(this->data(place)));
        else
            assert_cast<ColumnUInt64 &>(to).getData().push_back(getSegmentLengthSum<UInt64>(this->data(place)));
    }
};

}
