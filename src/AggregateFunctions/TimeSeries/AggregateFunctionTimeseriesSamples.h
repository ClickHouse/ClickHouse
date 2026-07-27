#pragma once

#include <algorithm>
#include <functional>
#include <utility>

#include <base/sort.h>

#include <Common/AllocatorWithMemoryTracking.h>
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

/// Per-bucket storage of timeseries samples.
/// When two samples share a timestamp the larger value is kept.
///
/// Samples are stored in an append-only vector and canonicalized (sorted by timestamp, duplicates collapsed
/// to the larger value) lazily, on first ordered access. Inputs arrive as ascending-timestamp runs (the
/// samples table is sorted by timestamp within each series), so the canonicalization sort runs on
/// almost-sorted data and is cheap, while appends stay O(1) without any per-sample hashing.
template <typename TimestampType, typename ValueType>
class AggregateFunctionTimeseriesSamples
{
public:
    /// The bucket map (`HashMap`) relocates cells with `memcpy` and abandons the source, which is safe for
    /// any type without pointers into itself. A vector qualifies: its buffer is on the heap. No standard
    /// trait expresses this (weaker than `std::is_trivially_copyable`) property, hence the explicit declaration.
    static constexpr bool is_position_independent = true;

    void add(TimestampType timestamp, ValueType value)
    {
        buffer.emplace_back(timestamp, value);
        compacted = false;
    }

    /// Appends `count` samples at once. Semantically `add` in a loop, but with a single size bump and
    /// raw writes - this is the hot path when whole runs of samples fall into one bucket.
    void addRun(const TimestampType * timestamps, const ValueType * values, size_t count)
    {
        const size_t old_size = buffer.size();
        buffer.resize(old_size + count);
        auto * out = buffer.data() + old_size;
        for (size_t i = 0; i < count; ++i)
        {
            out[i].first = timestamps[i];
            out[i].second = values[i];
        }
        compacted = false;
    }

    void merge(const AggregateFunctionTimeseriesSamples & other)
    {
        buffer.insert(buffer.end(), other.buffer.begin(), other.buffer.end());
        compacted = false;
    }

    /// Merge that may leave `other` empty. During the merge of partial aggregation states each
    /// (series, bucket) usually exists in exactly one source state, so the destination bucket is
    /// empty and the samples are stolen instead of copied.
    void mergeDestructive(AggregateFunctionTimeseriesSamples & other)
    {
        if (buffer.empty())
        {
            buffer.swap(other.buffer);
            compacted = other.compacted;
            return;
        }
        merge(other);
    }

    void serialize(WriteBuffer & buf) const
    {
        /// Canonicalize first so duplicates don't inflate the serialized state.
        compact();

        writeBinaryLittleEndian(buffer.size(), buf);
        for (const auto & [timestamp, value] : buffer)
        {
            writeBinaryLittleEndian(timestamp, buf);
            writeBinaryLittleEndian(value, buf);
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        /// Deserialize replaces any previous contents.
        buffer.clear();
        compacted = false;

        size_t sample_count = 0;
        readBinaryLittleEndian(sample_count, buf);
        buffer.reserve(sample_count);
        for (size_t s = 0; s < sample_count; ++s)
        {
            TimestampType timestamp;
            readBinaryLittleEndian(timestamp, buf);
            ValueType value;
            readBinaryLittleEndian(value, buf);
            buffer.emplace_back(timestamp, value);
        }
    }

    /// Throws if any sample's timestamp is outside the range.
    template <typename RangeType>
    void checkTimestampsInRange(const RangeType & range) const
    {
        for (const auto & [timestamp, value] : buffer)
        {
            if (!range.contains(timestamp))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Cannot deserialize data: timestamp {} is outside its bucket's range",
                    static_cast<Int64>(timestamp));
        }
    }

    /// Invokes `f(timestamp, value)` for every distinct-timestamp sample, in arbitrary order. Used by the
    /// per-function sliding aggregators for order-independent aggregates (e.g. linear regression moments).
    /// Canonicalizes first: duplicate-timestamp semantics (keep the larger value) must hold here too.
    template <typename F>
    void forEachSample(F && f) const
    {
        compact();
        for (const auto & [timestamp, value] : buffer)
            f(timestamp, value);
    }

    /// Invokes `f(timestamp, value)` for every distinct-timestamp sample in ascending timestamp order.
    /// Used by the order-dependent aggregators (rate reset accounting, counting transitions).
    template <typename F>
    void forEachSampleSorted(F && f) const
    {
        compact();
        for (const auto & [timestamp, value] : buffer)
            f(timestamp, value);
    }

private:
    /// Sorts samples by timestamp and collapses samples sharing a timestamp to the one with the larger value.
    /// Logically const: canonicalization only changes the representation, not the sample set, and states are
    /// never accessed concurrently.
    void compact() const
    {
        if (compacted)
            return;

        /// Sorting by (timestamp, value) makes the last sample of each equal-timestamp run the one with
        /// the larger value. Samples arrive as ascending runs, so the buffer is usually sorted already -
        /// checking is much cheaper than re-sorting.
        if (!std::is_sorted(buffer.begin(), buffer.end()))
            ::sort(buffer.begin(), buffer.end());

        auto out = buffer.begin();
        for (auto it = buffer.begin(); it != buffer.end();)
        {
            auto run_end = it + 1;
            while (run_end != buffer.end() && run_end->first == it->first)
                ++run_end;
            *out++ = *(run_end - 1);
            it = run_end;
        }
        buffer.erase(out, buffer.end());

        compacted = true;
    }

    /// Samples in insertion order until `compact` canonicalizes them. Uses `AllocatorWithMemoryTracking` so
    /// per-bucket sample memory is counted by the `MemoryTracker`, like the rest of the aggregate state.
    mutable VectorWithMemoryTracking<std::pair<TimestampType, ValueType>> buffer;
    mutable bool compacted = false;
};

}
