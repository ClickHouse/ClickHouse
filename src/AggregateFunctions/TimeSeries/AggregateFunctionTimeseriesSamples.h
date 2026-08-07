#pragma once

#include <algorithm>
#include <utility>

#include <base/sort.h>

#include <Common/Exception.h>
#include <Common/VectorWithMemoryTracking.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

/// Per-bucket storage of timeseries samples: a flat array of (timestamp, value) pairs kept sorted by timestamp, where duplicate timestamps keep the larger value (and the first arrival against a NaN).
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
                last.second = std::max(last.second, value);
                return;
            }
            sorted = false;
        }
        buffer.emplace_back(timestamp, value);
    }

    void merge(const AggregateFunctionTimeseriesSamples & other)
    {
        if (other.buffer.empty())
            return;

        if (buffer.empty())
        {
            buffer = other.buffer;
            if (!other.sorted)
                sortAndDeduplicate(buffer);
            sorted = true;
            return;
        }

        normalize();

        /// A rare unsorted argument is normalized into a copy: `other` belongs to another state and is kept intact.
        Buffer other_normalized;
        const Buffer * rhs = &other.buffer;
        if (!other.sorted)
        {
            other_normalized = other.buffer;
            sortAndDeduplicate(other_normalized);
            rhs = &other_normalized;
        }

        /// Partial states often cover disjoint timestamp ranges - then the merge is a plain append.
        if (buffer.back().first < rhs->front().first)
        {
            buffer.insert(buffer.end(), rhs->begin(), rhs->end());
            return;
        }

        /// Linear merge of two sorted arrays; equal timestamps collapse into one sample keeping the larger value.
        Buffer merged;
        merged.reserve(buffer.size() + rhs->size());
        auto lhs_it = buffer.begin();
        auto rhs_it = rhs->begin();
        while (lhs_it != buffer.end() && rhs_it != rhs->end())
        {
            if (lhs_it->first < rhs_it->first)
                merged.push_back(*lhs_it++);
            else if (rhs_it->first < lhs_it->first)
                merged.push_back(*rhs_it++);
            else
            {
                merged.emplace_back(lhs_it->first, std::max(lhs_it->second, rhs_it->second));
                ++lhs_it;
                ++rhs_it;
            }
        }
        merged.insert(merged.end(), lhs_it, buffer.end());
        merged.insert(merged.end(), rhs_it, rhs->end());
        buffer = std::move(merged);
    }

    void serialize(WriteBuffer & buf) const
    {
        /// A rare unsorted state is serialized from a normalized copy, so the state is not mutated behind `const`.
        if (!sorted) [[unlikely]]
        {
            Buffer normalized = buffer;
            sortAndDeduplicate(normalized);
            writeSamples(normalized, buf);
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
        /// No order is assumed on the wire (older peers serialize hash-map iteration order): `add` detects disorder while reading and `normalize` restores the invariant if it was violated.
        for (size_t s = 0; s < sample_count; ++s)
        {
            TimestampType timestamp;
            readBinaryLittleEndian(timestamp, buf);
            ValueType value;
            readBinaryLittleEndian(value, buf);
            add(timestamp, value);
        }
        normalize();
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
        /// A rare unsorted state is iterated via a normalized copy, so the state is not mutated behind `const`.
        if (!sorted) [[unlikely]]
        {
            Buffer normalized = buffer;
            sortAndDeduplicate(normalized);
            for (const auto & [timestamp, value] : normalized)
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

    /// Sorts by timestamp (stably, keeping equal-timestamp samples in arrival order), then folds each equal-timestamp run with `std::max` like the former hash map did: the larger value wins, the first arrival wins against a NaN (`std::max` returns its first argument when the comparison is false).
    static void sortAndDeduplicate(Buffer & buf)
    {
        /// The comparator looks at timestamps only: comparing whole pairs would compare values, and `ValueType` can hold NaNs, which break the strict weak ordering the sort requires.
        ::stableSort(buf.begin(), buf.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

        size_t last_unique = 0;
        for (size_t i = 1; i < buf.size(); ++i)
        {
            if (buf[i].first == buf[last_unique].first)
                buf[last_unique].second = std::max(buf[last_unique].second, buf[i].second);
            else
                buf[++last_unique] = buf[i];
        }
        if (!buf.empty())
            buf.resize(last_unique + 1);
    }

    /// Restores the invariant in place after out-of-order `add`s; no-op in the common (already sorted) case.
    void normalize()
    {
        if (sorted)
            return;
        sortAndDeduplicate(buffer);
        sorted = true;
    }

    /// The samples, sorted by timestamp and deduplicated whenever `sorted` is true.
    Buffer buffer;
    /// Cleared by an out-of-order `add`; while set, timestamps in `buffer` are strictly increasing.
    bool sorted = true;
};

}
