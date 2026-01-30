#pragma once

#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/ISource.h>
#include <Common/Exception.h>
#include <Common/PrimeGenerator.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

using Interval = std::pair<UInt64, UInt64>; /// inclusive [l..r]

class PrimesSource final : public ISource
{
public:
    PrimesSource(UInt64 block_size_, UInt64 offset_, std::optional<UInt64> limit_, UInt64 step_, const std::string & column_name)
        : ISource(createHeader(column_name))
        , block_size(block_size_)
        , limit(limit_)
        , step(step_)
        , next_selected_index(offset_)
    {
        if (step_ == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Step size in PrimesSource cannot be zero");
    }

    String getName() const override { return "Primes"; }

    static SharedHeader createHeader(const std::string & column_name)
    {
        return std::make_shared<const Block>(
            Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), column_name)});
    }

protected:
    Chunk generate() override
    {
        if (limit && produced_rows >= *limit)
            return {};

        UInt64 target = block_size;
        if (limit)
            target = std::min<UInt64>(target, *limit - produced_rows);

        /// In case block_size is zero
        if (target == 0)
            return {};

        auto column = ColumnUInt64::create(target);
        auto & vec = column->getData();

        UInt64 written = 0;

        while (written < target)
        {
            auto prime = prime_generator.next();

            const UInt64 cur_prime_index = prime_index;
            ++prime_index;

            if (cur_prime_index < next_selected_index)
                continue;

            chassert(cur_prime_index == next_selected_index);

            /// Avoid overflow
            /// This can be caused by this query: SELECT * FROM primes(1, 2, 18446744073709551615);
            if (next_selected_index > std::numeric_limits<UInt64>::max() - step)
                next_selected_index = std::numeric_limits<UInt64>::max();
            else
                next_selected_index += step;

            vec[written] = prime;
            ++written;
        }

        if (written == 0)
            return {};

        vec.resize(written);

        produced_rows += written;
        progress(written, column->byteSize());
        return {Columns{std::move(column)}, written};
    }

private:
    UInt64 block_size;
    std::optional<UInt64> limit;
    UInt64 step;

    SegmentedSievePrimeGenerator prime_generator;

    UInt64 prime_index = 0;
    UInt64 produced_rows = 0;

    UInt64 next_selected_index = 0;
};


class PrimesRangedSource final : public ISource
{
public:
    PrimesRangedSource(
        UInt64 block_size_,
        std::vector<Interval> intervals_,
        UInt64 offset_,
        std::optional<UInt64> limit_,
        UInt64 step_,
        const std::string & column_name)
        : ISource(PrimesSource::createHeader(column_name))
        , block_size(block_size_)
        , intervals(std::move(intervals_))
        , limit(limit_)
        , step(step_)
        , next_selected_index(offset_)
    {
        if (step_ == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Step size in PrimesRangedSource cannot be zero");

        if (intervals.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Intervals in PrimesRangedSource cannot be empty");

        for (auto & interval : intervals)
        {
            if (interval.first > interval.second)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Intervals in PrimesRangedSource must be valid (l <= r)");
        }

        for (size_t i = 1; i < intervals.size(); ++i)
        {
            if (intervals[i].first <= intervals[i - 1].second)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Intervals in PrimesRangedSource must be sorted and non-overlapping");
        }
    }

    String getName() const override { return "PrimesRange"; }

protected:
    Chunk generate() override
    {
        if (interval_idx >= intervals.size())
            return {};

        if (limit && produced_rows >= *limit)
            return {};

        UInt64 target = block_size;
        if (limit)
            target = std::min<UInt64>(target, *limit - produced_rows);

        /// In case block_size is zero
        if (target == 0)
            return {};

        auto column = ColumnUInt64::create(target);
        auto & vec = column->getData();

        UInt64 written = 0;

        while (written < target)
        {
            auto prime = prime_generator.next();

            const UInt64 cur_prime_index = prime_index;
            ++prime_index;

            if (cur_prime_index < next_selected_index)
                continue;

            chassert(cur_prime_index == next_selected_index);

            /// Avoid overflow
            /// This can be caused by this query: SELECT * FROM primes(1, 2, 18446744073709551615);
            if (next_selected_index > std::numeric_limits<UInt64>::max() - step)
                next_selected_index = std::numeric_limits<UInt64>::max();
            else
                next_selected_index += step;

            /// Now apply interval filter
            while (interval_idx < intervals.size() && prime > intervals[interval_idx].second)
                ++interval_idx;

            if (interval_idx >= intervals.size())
                break;

            if (prime < intervals[interval_idx].first)
                continue;

            vec[written] = prime;
            ++written;
        }

        if (written == 0)
            return {};

        vec.resize(written);

        produced_rows += written;
        progress(written, column->byteSize());
        return {Columns{std::move(column)}, written};
    }

private:
    UInt64 block_size;

    std::vector<Interval> intervals;
    size_t interval_idx = 0;

    std::optional<UInt64> limit;
    UInt64 step;

    SegmentedSievePrimeGenerator prime_generator;

    UInt64 prime_index = 0;
    UInt64 produced_rows = 0;

    UInt64 next_selected_index = 0;
};


class PrimesSimpleRangedSource final : public ISource
{
public:
    PrimesSimpleRangedSource(
        UInt64 block_size_, std::vector<Interval> intervals_, std::optional<UInt64> limit_, const std::string & column_name)
        : ISource(PrimesSource::createHeader(column_name))
        , block_size(block_size_)
        , intervals(std::move(intervals_))
        , limit(limit_)
    {
        if (intervals.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Intervals in PrimesSimpleRangedSource cannot be empty");

        for (auto & interval : intervals)
        {
            if (interval.first > interval.second)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Intervals in PrimesRangedSource must be valid (l <= r)");
        }

        for (size_t i = 1; i < intervals.size(); ++i)
        {
            if (intervals[i].first <= intervals[i - 1].second)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Intervals in PrimesSimpleRangedSource must be sorted and non-overlapping");
        }

        prime_generator.setRange(intervals[0].first, intervals[0].second);
    }

    String getName() const override { return "PrimesSimpleRange"; }

protected:
    Chunk generate() override
    {
        if (interval_idx >= intervals.size())
            return {};

        if (limit && produced_rows >= *limit)
            return {};

        UInt64 target = block_size;
        if (limit)
            target = std::min<UInt64>(target, *limit - produced_rows);

        /// In case block_size is zero
        if (target == 0)
            return {};

        auto column = ColumnUInt64::create(target);
        auto & vec = column->getData();

        UInt64 written = 0;

        while (written < target)
        {
            if (interval_idx >= intervals.size())
                break;

            auto prime_opt = prime_generator.next();
            if (!prime_opt)
            {
                ++interval_idx;
                if (interval_idx < intervals.size())
                    prime_generator.setRange(intervals[interval_idx].first, intervals[interval_idx].second);
                continue;
            }

            vec[written] = *prime_opt;
            ++written;
        }

        if (written == 0)
            return {};

        vec.resize(written);

        produced_rows += written;
        progress(written, column->byteSize());
        return {Columns{std::move(column)}, written};
    }

private:
    UInt64 block_size;

    std::vector<Interval> intervals;
    size_t interval_idx = 0;

    std::optional<UInt64> limit;

    RangeSegmentedSievePrimeGenerator prime_generator;

    UInt64 produced_rows = 0;
};

}
