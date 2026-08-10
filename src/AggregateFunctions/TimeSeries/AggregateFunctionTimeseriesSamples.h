#pragma once

#include <algorithm>
#include <iterator>
#include <utility>

#include <base/sort.h>

#include <Common/Exception.h>
#include <Common/NaNUtils.h>
#include <Common/VectorWithMemoryTracking.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

/// Per-bucket storage of timeseries samples: a flat array of (timestamp, value) pairs kept sorted by timestamp, where duplicate timestamps keep the largest real value (a NaN survives only when every sample at the timestamp is NaN).
template <typename TimestampType, typename ValueType>
class AggregateFunctionTimeseriesSamples
{
public:
    /// The bucket map (`HashMap`) relocates cells with `memcpy` and abandons the source, which is safe here: the vector holds only pointers to its heap buffer, no pointers into itself.
    static constexpr bool is_position_independent = true;

    void add(TimestampType timestamp, ValueType value)
    {
        /// Out-of-order and duplicate timestamps are rare (measured ~1 per 1.5 billion adds on production-shaped multithreaded reads), hence `[[unlikely]]`.
        if (!buffer.empty() && timestamp <= buffer.back().first) [[unlikely]]
        {
            auto & last = buffer.back();
            if (timestamp == last.first)
            {
                last.second = getMax(last.second, value);
                return;
            }
            sorted = false;
        }
        buffer.emplace_back(timestamp, value);
    }

    void merge(const AggregateFunctionTimeseriesSamples & other)
    {
        if (other.buffer.empty())
        {
            /// Nothing to merge: the state is left as is (a rare unsorted state stays unsorted until a later operation sorts it).
            return;
        }

        if (buffer.empty())
        {
            buffer = other.buffer;
            sorted = other.sorted;
            sort();
            return;
        }

        sort();

        /// A rare unsorted argument is sorted into a copy: `other` belongs to another state and is kept intact.
        const Buffer * rhs = &other.buffer;
        Buffer sorted_other_buffer;
        if (!other.sorted)
        {
            sorted_other_buffer = other.buffer;
            sortBuffer(sorted_other_buffer);
            rhs = &sorted_other_buffer;
        }

        /// Partial states often cover disjoint timestamp ranges - then the merge is a plain append or prepend.
        if (buffer.back().first < rhs->front().first)
        {
            buffer.insert(buffer.end(), rhs->begin(), rhs->end());
            return;
        }
        if (rhs->back().first < buffer.front().first)
        {
            buffer.insert(buffer.begin(), rhs->begin(), rhs->end());
            return;
        }

        Buffer merged;
        merged.reserve(buffer.size() + rhs->size());
        std::merge(buffer.begin(), buffer.end(), rhs->begin(), rhs->end(), std::back_inserter(merged), lessByTimestamp);
        deduplicateSorted(merged);
        buffer = std::move(merged);
    }

    void serialize(WriteBuffer & buf) const
    {
        /// A rare unsorted state is serialized from a sorted copy, so the state is not mutated behind `const`.
        if (!sorted) [[unlikely]]
        {
            Buffer sorted_buffer = buffer;
            sortBuffer(sorted_buffer);
            writeSamples(sorted_buffer, buf);
            return;
        }
        writeSamples(buffer, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        /// Deserialize replaces any previous contents.
        buffer.clear();
        sorted = true;

        size_t sample_count = 0;
        readBinaryLittleEndian(sample_count, buf);
        buffer.reserve(sample_count);
        /// No order is assumed on the wire (older peers serialize hash-map iteration order): `add` detects disorder while reading and `sort` restores the invariant if it was violated.
        for (size_t s = 0; s < sample_count; ++s)
        {
            TimestampType timestamp;
            readBinaryLittleEndian(timestamp, buf);
            ValueType value;
            readBinaryLittleEndian(value, buf);
            add(timestamp, value);
        }
        sort();
    }

    /// Throws if any sample's timestamp is outside the range.
    template <typename RangeType>
    void checkTimestampsInRange(const RangeType & range) const
    {
        forEachSample([&range](TimestampType timestamp, ValueType)
        {
            if (!range.contains(timestamp))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Cannot deserialize data: timestamp {} is outside its bucket's range",
                    static_cast<Int64>(timestamp));
        });
    }

    /// Invokes `f(timestamp, value)` for every sample, in ascending timestamp order with duplicates collapsed.
    template <typename F>
    void forEachSample(F && f) const
    {
        /// A rare unsorted state is iterated via a sorted copy, so the state is not mutated behind `const`.
        if (!sorted) [[unlikely]]
        {
            Buffer sorted_buffer = buffer;
            sortBuffer(sorted_buffer);
            for (const auto & [timestamp, value] : sorted_buffer)
                f(timestamp, value);
            return;
        }
        for (const auto & [timestamp, value] : buffer)
            f(timestamp, value);
    }

private:
    /// `VectorWithMemoryTracking` counts the samples' memory in the `MemoryTracker`, like the rest of the aggregate state.
    using Buffer = VectorWithMemoryTracking<std::pair<TimestampType, ValueType>>;

    static void writeSamples(const Buffer & samples, WriteBuffer & buf)
    {
        writeBinaryLittleEndian(samples.size(), buf);
        for (const auto & [timestamp, value] : samples)
        {
            writeBinaryLittleEndian(timestamp, buf);
            writeBinaryLittleEndian(value, buf);
        }
    }

    static bool lessByTimestamp(const std::pair<TimestampType, ValueType> & lhs, const std::pair<TimestampType, ValueType> & rhs)
    {
        return lhs.first < rhs.first;
    }

    /// Returns the larger of two values sharing a timestamp; a NaN loses to any real value.
    /// The operation is associative and commutative, so the result does not depend on arrival or merge order.
    static ValueType getMax(ValueType lhs, ValueType rhs)
    {
        if (isNaN(lhs))
            return rhs;
        if (isNaN(rhs))
            return lhs;
        return std::max(lhs, rhs);
    }

    /// Collapses each equal-timestamp run of a sorted buffer into one sample with `getMax`.
    static void deduplicateSorted(Buffer & buf)
    {
        size_t last_unique = 0;
        for (size_t i = 1; i < buf.size(); ++i)
        {
            if (buf[i].first == buf[last_unique].first)
                buf[last_unique].second = getMax(buf[last_unique].second, buf[i].second);
            else
                buf[++last_unique] = buf[i];
        }
        if (!buf.empty())
            buf.resize(last_unique + 1);
    }

    static void sortBuffer(Buffer & buf)
    {
        ::sort(buf.begin(), buf.end(), lessByTimestamp);
        deduplicateSorted(buf);
    }

    /// Restores the invariant in place after out-of-order `add`s; no-op in the common (already sorted) case.
    void sort()
    {
        if (sorted)
            return;
        sortBuffer(buffer);
        sorted = true;
    }

    /// The samples, sorted by timestamp and deduplicated whenever `sorted` is true.
    Buffer buffer;
    /// Cleared by an out-of-order `add`; while set, timestamps in `buffer` are strictly increasing.
    bool sorted = true;
};

}
