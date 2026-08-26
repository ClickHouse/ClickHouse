#pragma once

#include <algorithm>
#include <functional>
#include <utility>

#include <absl/container/flat_hash_map.h>

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

/// Per-bucket storage of timeseries samples keyed by timestamp.
/// When two samples share a timestamp the larger value is kept.
template <typename TimestampType, typename ValueType>
class AggregateFunctionTimeseriesSamples
{
public:
    /// The bucket map (`HashMap`) relocates cells with `memcpy` and abandons the source, which is safe for
    /// any type without pointers into itself. `absl::flat_hash_map` qualifies: the address of its inline
    /// single-element storage (small object optimization) is computed from `this`, the rest is on the heap.
    /// No standard trait expresses this (weaker than `std::is_trivially_copyable`) property, hence the
    /// explicit declaration.
    static constexpr bool is_position_independent = true;

    void add(TimestampType timestamp, ValueType value)
    {
        auto [it, inserted] = buffer.emplace(timestamp, value);
        if (!inserted)
            it->second = std::max(it->second, value);
    }

    void merge(const AggregateFunctionTimeseriesSamples & other)
    {
        buffer.reserve(buffer.size() + other.buffer.size());
        for (const auto & [timestamp, value] : other.buffer)
            add(timestamp, value);
    }

    void serialize(WriteBuffer & buf) const
    {
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

        size_t sample_count = 0;
        readBinaryLittleEndian(sample_count, buf);
        buffer.reserve(sample_count);
        for (size_t s = 0; s < sample_count; ++s)
        {
            TimestampType timestamp;
            readBinaryLittleEndian(timestamp, buf);
            ValueType value;
            readBinaryLittleEndian(value, buf);
            add(timestamp, value);
        }
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

    /// Invokes `f(timestamp, value)` for every sample, in arbitrary order. Used by the per-function sliding
    /// aggregators for order-independent aggregates (e.g. linear regression moments).
    template <typename F>
    void forEachSample(F && f) const
    {
        for (const auto & [timestamp, value] : buffer)
            f(timestamp, value);
    }

    /// Invokes `f(timestamp, value)` for every sample in ascending timestamp order, using `temp_buffer` as a
    /// reused sort buffer. Used by the order-dependent aggregators (rate reset accounting, counting transitions).
    template <typename F>
    void forEachSampleSorted(F && f, VectorWithMemoryTracking<std::pair<TimestampType, ValueType>> & temp_buffer) const
    {
        temp_buffer.clear();
        temp_buffer.reserve(buffer.size());
        for (const auto & [timestamp, value] : buffer)
            temp_buffer.emplace_back(timestamp, value);
        ::sort(temp_buffer.begin(), temp_buffer.end());
        for (const auto & [timestamp, value] : temp_buffer)
            f(timestamp, value);
    }

private:
    /// Samples keyed by timestamp. Uses `AllocatorWithMemoryTracking` so per-bucket sample memory is counted by
    /// the `MemoryTracker`, like the rest of the aggregate state.
    absl::flat_hash_map<
        TimestampType,
        ValueType,
        absl::container_internal::hash_default_hash<TimestampType>,
        std::equal_to<>,
        AllocatorWithMemoryTracking<std::pair<const TimestampType, ValueType>>> buffer;
};

}
