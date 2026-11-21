#pragma once

#include <algorithm>
#include <utility>

#include <base/sort.h>

#include <Common/ArenaAllocator.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <AggregateFunctions/TimeSeries/timeseriesMaxValueForDuplicateTimestamp.h>


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
    /// The bucket map (`HashMap`) relocates cells with `memcpy` and abandons the source, which is safe here:
    /// the buffer is reached only through pointers to arena or heap memory, never through pointers into itself.
    static constexpr bool is_position_independent = true;

    static constexpr bool allocates_memory_in_arena = true;

    void add(TimestampType timestamp, ValueType value, Arena * arena)
    {
        /// Out-of-order and duplicate timestamps are rare (measured ~1 per 1.5 billion adds on production-shaped multithreaded reads), hence `[[unlikely]]`.
        if (!buffer.empty() && timestamp <= buffer.back().first) [[unlikely]]
        {
            auto & last = buffer.back();
            if (timestamp == last.first)
            {
                last.second = timeseriesMaxValueForDuplicateTimestamp(last.second, value);
                return;
            }
            sorted = false;
        }
        buffer.push_back(Sample{timestamp, value}, arena);
    }

    ALWAYS_INLINE void addMany(const TimestampType * __restrict timestamps, const ValueType * __restrict values, size_t count, Arena * arena)
    {
        if (count == 0)
            return;

        const size_t old_size = buffer.size();
        buffer.resize(old_size + count, arena);
        auto * __restrict appended = buffer.data() + old_size;
        for (size_t i = 0; i < count; ++i)
            appended[i] = {timestamps[i], values[i]};

        UInt8 in_order = old_size == 0 || appended[-1].first < timestamps[0];
        for (size_t i = 1; i < count; ++i)
            in_order &= static_cast<UInt8>(timestamps[i - 1] < timestamps[i]);
        sorted = sorted && in_order;
    }

    void merge(const AggregateFunctionTimeseriesSamples & other, Arena * arena)
    {
        if (other.buffer.empty())
        {
            /// Nothing to merge: the state is left as is (a rare unsorted state stays unsorted until a later operation sorts it).
            return;
        }

        if (buffer.empty())
        {
            buffer.assign(other.buffer.begin(), other.buffer.end(), arena);
            sorted = other.sorted;
            sort();
            return;
        }

        sort();

        /// A rare unsorted argument is sorted into a copy: `other` belongs to another state and is kept intact.
        const Sample * rhs_begin = other.buffer.begin();
        const Sample * rhs_end = other.buffer.end();
        TemporaryBuffer sorted_other_buffer;
        if (!other.sorted)
        {
            sorted_other_buffer.assign(other.buffer.begin(), other.buffer.end());
            sortBuffer(sorted_other_buffer);
            rhs_begin = sorted_other_buffer.begin();
            rhs_end = sorted_other_buffer.end();
        }
        /// Partial states often cover disjoint timestamp ranges - then the merge is a plain append.
        if (buffer.back().first < rhs_begin->first)
        {
            buffer.insert(rhs_begin, rhs_end, arena);
            return;
        }

        size_t lhs_pos = buffer.size();
        size_t rhs_pos = rhs_end - rhs_begin;
        buffer.resize(lhs_pos + rhs_pos, arena);

        /// Disjoint the other way round: the own samples move up as one block and `rhs` goes in front of them.
        if (rhs_begin[rhs_pos - 1].first < buffer.front().first)
        {
            std::copy_backward(buffer.begin(), buffer.begin() + lhs_pos, buffer.end());
            std::copy(rhs_begin, rhs_end, buffer.begin());
            return;
        }

        /// Merges in place from the back, so no unread sample of `buffer` is overwritten.
        Sample * out = buffer.end();
        while (lhs_pos > 0 && rhs_pos > 0)
        {
            if (rhs_begin[rhs_pos - 1].first < buffer[lhs_pos - 1].first)
                *--out = buffer[--lhs_pos];
            else
                *--out = rhs_begin[--rhs_pos];
        }
        std::copy(rhs_begin, rhs_begin + rhs_pos, buffer.begin());
        deduplicateSorted(buffer);
    }

    /// Merges `other`, which the caller gives up: the bigger of the two buffers is kept and the smaller one is
    /// merged into it, so a source merged into an empty bucket hands its buffer over instead of being copied.
    void merge(AggregateFunctionTimeseriesSamples && other, Arena * arena)
    {
        if (buffer.size() < other.buffer.size())
        {
            /// For this allocator `swap` only exchanges the pointers; the arena is needed by the signature.
            buffer.swap(other.buffer, arena);
            std::swap(sorted, other.sorted);
        }
        /// `other` is ours to change, so it is sorted in place instead of into a copy.
        other.sort();
        /// A named rvalue reference is an lvalue: this calls the `const &` overload.
        merge(other, arena);
    }

    void serialize(WriteBuffer & buf) const
    {
        /// A rare unsorted state is serialized from a sorted copy, so the state is not mutated behind `const`.
        if (!sorted) [[unlikely]]
        {
            TemporaryBuffer sorted_buffer(buffer.begin(), buffer.end());
            sortBuffer(sorted_buffer);
            writeSamples(sorted_buffer, buf);
            return;
        }
        writeSamples(buffer, buf);
    }

    void deserialize(ReadBuffer & buf, Arena * arena)
    {
        /// Deserialize replaces any previous contents.
        buffer.clear();
        sorted = true;

        size_t sample_count = 0;
        readBinaryLittleEndian(sample_count, buf);
        /// The sample count is read from the state and cannot be trusted, so only a bounded amount is reserved
        /// upfront and `add` grows the buffer while the samples are read. That way a corrupted count fails with
        /// an end-of-buffer error instead of allocating memory for the claimed number of samples.
        buffer.reserve(std::min(sample_count, MAX_SAMPLES_TO_RESERVE), arena);
        /// No order is assumed on the wire (older peers serialize hash-map iteration order): `add` detects disorder while reading and `sort` restores the invariant if it was violated.
        for (size_t s = 0; s < sample_count; ++s)
        {
            TimestampType timestamp;
            readBinaryLittleEndian(timestamp, buf);
            ValueType value;
            readBinaryLittleEndian(value, buf);
            add(timestamp, value, arena);
        }
        sort();
    }

    /// Throws if any sample's timestamp is outside the closed range.
    /// For a sorted buffer, checking the first and last timestamps is sufficient.
    template <typename RangeType>
    void checkTimestampsInRange(const RangeType & range) const
    {
        if (sorted && !buffer.empty() && range.contains(buffer.front().first)
            && (buffer.size() == 1 || range.contains(buffer.back().first)))
            return;

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
            TemporaryBuffer sorted_buffer(buffer.begin(), buffer.end());
            sortBuffer(sorted_buffer);
            for (const auto & [timestamp, value] : sorted_buffer)
                f(timestamp, value);
            return;
        }
        for (const auto & [timestamp, value] : buffer)
            f(timestamp, value);
    }

private:
    /// How many samples `deserialize` reserves before reading the data. Bigger buckets grow while they are read.
    static constexpr size_t MAX_SAMPLES_TO_RESERVE = 4096;

    using Sample = std::pair<TimestampType, ValueType>;
    /// Buffers of 4096 bytes and more are allocated with the ordinary allocator, to avoid fragmentation and trash in the arena.
    using Allocator = MixedAlignedArenaAllocator<alignof(Sample), 4096>;
    using Buffer = PODArray<Sample, /* initial_bytes = */ sizeof(Sample), Allocator>;
    /// For the sorted copies made where no arena is available.
    using TemporaryBuffer = PODArray<Sample, 32>;

    template <typename Container>
    static void writeSamples(const Container & samples, WriteBuffer & buf)
    {
        writeBinaryLittleEndian(samples.size(), buf);
        for (const auto & [timestamp, value] : samples)
        {
            writeBinaryLittleEndian(timestamp, buf);
            writeBinaryLittleEndian(value, buf);
        }
    }

    static bool lessByTimestamp(const Sample & lhs, const Sample & rhs)
    {
        return lhs.first < rhs.first;
    }

    /// Collapses each equal-timestamp run of a sorted buffer into one sample with `timeseriesMaxValueForDuplicateTimestamp`.
    template <typename Container>
    static void deduplicateSorted(Container & buf)
    {
        size_t last_unique = 0;
        for (size_t i = 1; i < buf.size(); ++i)
        {
            if (buf[i].first == buf[last_unique].first)
                buf[last_unique].second = timeseriesMaxValueForDuplicateTimestamp(buf[last_unique].second, buf[i].second);
            else
                buf[++last_unique] = buf[i];
        }
        if (!buf.empty())
            buf.resize_assume_reserved(last_unique + 1);
    }

    template <typename Container>
    static void sortBuffer(Container & buf)
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
