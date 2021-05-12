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

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

template <typename T>
struct AggregateFunctionRangeSumData
{
    using TimeGap = std::pair<T, T>;
    using TimeGaps = PODArrayWithStackMemory<TimeGap, 64>;

    bool sorted = false;

    TimeGaps gap_list;

    size_t size() const { return gap_list.size(); }

    void add(T start, T end)
    {
        if (sorted && gap_list.size() > 0)
        {
            sorted = gap_list.back().first <= start;
        }
        gap_list.emplace_back(start, end);
    }

    void merge(const AggregateFunctionRangeSumData & other)
    {
        if (other.gap_list.empty())
            return;

        const auto size = gap_list.size();

        gap_list.insert(std::begin(other.gap_list), std::end(other.gap_list));

        /// either sort whole container or do so partially merging ranges afterwards
        if (!sorted && !other.sorted)
            std::stable_sort(std::begin(gap_list), std::end(gap_list));
        else
        {
            const auto begin = std::begin(gap_list);
            const auto middle = std::next(begin, size);
            const auto end = std::end(gap_list);

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
            std::stable_sort(std::begin(gap_list), std::end(gap_list));
            sorted = true;
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(sorted, buf);
        writeBinary(gap_list.size(), buf);

        for (const auto & time_gap : gap_list)
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

        gap_list.clear();
        gap_list.reserve(size);

        T start, end;

        for (size_t i = 0; i < size; ++i)
        {
            readBinary(start, buf);
            readBinary(end, buf);
            gap_list.emplace_back(start, end);
        }
    }
};

template <typename T, typename Data>
class AggregateFunctionRangeSum final : public IAggregateFunctionDataHelper<Data, AggregateFunctionRangeSum<T, Data>>
{
private:
    UInt64 getRangeSum(Data & data) const
    {
        if (data.size() == 0)
            return 0;

        data.sort();

        typename Data::TimeGaps merged;

        for (const auto & gap : data.gap_list)
        {
            if (!merged.size() || merged.back().second < gap.first)
                merged.emplace_back(gap.first, gap.second);
            else
                merged.back().second = std::max(merged.back().second, gap.second);
        }

        UInt64 res = 0;
        for (const auto & gap : merged)
        {
            res += gap.second - gap.first;
        }

        return res;
    }

public:
    String getName() const override { return "rangeSum"; }

    explicit AggregateFunctionRangeSum(const DataTypes & arguments)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionRangeSum<T, Data>>(arguments, {})
    {
    }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

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
        assert_cast<ColumnUInt64 &>(to).getData().push_back(getRangeSum(this->data(place)));
    }
};

}
