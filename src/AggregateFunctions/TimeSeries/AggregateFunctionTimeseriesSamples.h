#pragma once

#include <algorithm>
#include <limits>
#include <utility>

#include <absl/container/inlined_vector.h>

#include <base/sort.h>

#include <Common/AllocatorWithMemoryTracking.h>
#include <Common/Exception.h>
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
/// Out-of-order samples are appended to an unsorted tail of the array; the tail is sorted and merged into the sorted prefix when the state is read.
template <typename TimestampType, typename ValueType>
class AggregateFunctionTimeseriesSamples
{
public:
    /// The bucket map (`HashMap`) relocates cells with `memcpy` and abandons the source,
    /// which is safe here: the buffer's inline single-sample storage is addressed relative to `this` (and the samples are trivially copyable),
    /// an allocated buffer is reached only through a pointer to the heap - no pointers into itself either way.
    static constexpr bool is_position_independent = true;

    void add(TimestampType timestamp, ValueType value)
    {
        compactBeforeGrowth(1);
        /// Out-of-order and duplicate timestamps are rare (measured ~1 per 1.5 billion adds on production-shaped multithreaded reads), hence `[[unlikely]]`.
        if (!buffer.empty() && timestamp <= buffer.back().first) [[unlikely]]
        {
            auto & last = buffer.back();
            if (timestamp == last.first)
            {
                last.second = timeseriesMaxValueForDuplicateTimestamp(last.second, value);
                return;
            }
            if (isSorted())
                unsorted_begin = buffer.size();
        }
        buffer.emplace_back(timestamp, value);
    }

    ALWAYS_INLINE void addMany(const TimestampType * __restrict timestamps, const ValueType * __restrict values, size_t count)
    {
        if (count == 0)
            return;

        compactBeforeGrowth(count);
        const size_t old_size = buffer.size();
        buffer.resize(old_size + count);
        auto * __restrict appended = buffer.data() + old_size;
        for (size_t i = 0; i < count; ++i)
            appended[i] = {timestamps[i], values[i]};

        if (!isSorted())
            return;

        if (old_size && appended[-1].first >= timestamps[0])
        {
            unsorted_begin = old_size;
            return;
        }

        for (size_t i = 1; i < count; ++i)
        {
            if (timestamps[i - 1] >= timestamps[i])
            {
                unsorted_begin = old_size + i;
                return;
            }
        }
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
            unsorted_begin = other.unsorted_begin;
            sort();
            return;
        }

        /// A rare unsorted argument joins the unsorted tail: `other` belongs to another state and is kept intact.
        if (!other.isSorted())
        {
            if (isSorted())
                unsorted_begin = buffer.size();
            buffer.insert(buffer.end(), other.buffer.begin(), other.buffer.end());
            sort();
            return;
        }

        sort();

        /// Partial states often cover disjoint timestamp ranges - then the merge is a plain append or prepend.
        if (buffer.back().first < other.buffer.front().first)
        {
            buffer.insert(buffer.end(), other.buffer.begin(), other.buffer.end());
            return;
        }
        if (other.buffer.back().first < buffer.front().first)
        {
            buffer.insert(buffer.begin(), other.buffer.begin(), other.buffer.end());
            return;
        }

        /// Overlapping ranges: the other samples are appended and merged into the prefix in place.
        const size_t tail_begin = buffer.size();
        buffer.insert(buffer.end(), other.buffer.begin(), other.buffer.end());
        mergeSortedTail(buffer, tail_begin);
    }

    void serialize(WriteBuffer & buf) const
    {
        /// A rare unsorted state is serialized from a sorted copy, so the state is not mutated behind `const`.
        if (!isSorted()) [[unlikely]]
        {
            Buffer sorted_buffer = buffer;
            sortBuffer(sorted_buffer, unsorted_begin);
            writeSamples(sorted_buffer, buf);
            return;
        }
        writeSamples(buffer, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        /// Deserialize replaces any previous contents.
        buffer.clear();
        unsorted_begin = NO_UNSORTED;

        size_t sample_count = 0;
        readBinaryLittleEndian(sample_count, buf);
        /// The sample count is read from the state and cannot be trusted, so only a bounded amount is reserved
        /// upfront and `add` grows the buffer while the samples are read. That way a corrupted count fails with
        /// an end-of-buffer error instead of allocating memory for the claimed number of samples.
        buffer.reserve(std::min(sample_count, MAX_SAMPLES_TO_RESERVE));
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

    /// Throws if any sample's timestamp is outside the closed range.
    /// For a sorted buffer, checking the first and last timestamps is sufficient.
    template <typename RangeType>
    void checkTimestampsInRange(const RangeType & range) const
    {
        if (isSorted() && !buffer.empty() && range.contains(buffer.front().first)
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
        if (!isSorted()) [[unlikely]]
        {
            Buffer sorted_buffer = buffer;
            sortBuffer(sorted_buffer, unsorted_begin);
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

    /// Some buckets hold a single sample - the inline capacity of 1 keeps it in the state itself with no heap allocation.
    using Buffer = absl::InlinedVector<
        std::pair<TimestampType, ValueType>,
        /* N = */ 1,
        AllocatorWithMemoryTracking<std::pair<TimestampType, ValueType>>>;

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

    /// Collapses each equal-timestamp run of a sorted buffer into one sample with `timeseriesMaxValueForDuplicateTimestamp`.
    /// Samples before `from` are already unique.
    static void deduplicateSorted(Buffer & buf, size_t from = 0)
    {
        size_t last_unique = from == 0 ? 0 : from - 1;
        for (size_t i = last_unique + 1; i < buf.size(); ++i)
        {
            if (buf[i].first == buf[last_unique].first)
                buf[last_unique].second = timeseriesMaxValueForDuplicateTimestamp(buf[last_unique].second, buf[i].second);
            else
                buf[++last_unique] = buf[i];
        }
        if (!buf.empty())
            buf.resize(last_unique + 1);
    }

    /// Sorts the tail `[unsorted_begin, size)` of a buffer whose prefix `[0, unsorted_begin)` is sorted and unique, and merges the tail into the prefix.
    static void sortBuffer(Buffer & buf, size_t unsorted_begin)
    {
        ::sort(buf.begin() + unsorted_begin, buf.end(), lessByTimestamp);

        /// A tail that lies entirely before the prefix, such as an older part read after a newer one, is moved in front of it.
        if ((buf.end() - 1)->first < buf.begin()->first)
        {
            deduplicateSorted(buf, unsorted_begin);
            std::rotate(buf.begin(), buf.begin() + unsorted_begin, buf.end());
            return;
        }

        mergeSortedTail(buf, unsorted_begin);
    }

    /// Merges the sorted tail `[tail_begin, size)` into the sorted and unique prefix `[0, tail_begin)` in place.
    static void mergeSortedTail(Buffer & buf, size_t tail_begin)
    {
        std::inplace_merge(buf.begin(), buf.begin() + tail_begin, buf.end(), lessByTimestamp);
        deduplicateSorted(buf);
    }

    /// Restores the invariant in place after out-of-order `add`s; no-op in the common (already sorted) case.
    void sort()
    {
        if (isSorted())
            return;
        sortBuffer(buffer, unsorted_begin);
        unsorted_begin = NO_UNSORTED;
    }

    [[nodiscard]] bool isSorted() const { return unsorted_begin == NO_UNSORTED; }

    /// Duplicates (the same timestamps coming from many series) can make the unsorted tail much longer than the number of distinct timestamps.
    /// So before the buffer would be reallocated, the unsorted tail is sorted and deduplicated, which can leave enough room to avoid growing.
    /// If more than a half of the capacity is still used, the buffer is grown geometrically right away, so a nearly full buffer is not compacted again by every following add.
    ALWAYS_INLINE void compactBeforeGrowth(size_t extra)
    {
        if (buffer.size() + extra <= buffer.capacity() || isSorted())
            return;
        sort();
        if (buffer.size() > buffer.capacity() / 2)
            buffer.reserve(std::max(buffer.size() + extra, 2 * buffer.capacity()));
    }

    /// The value of `unsorted_begin` for a buffer with no unsorted tail.
    static constexpr size_t NO_UNSORTED = std::numeric_limits<size_t>::max();

    /// The samples. The prefix `[0, unsorted_begin)` is sorted by timestamp and unique, the rest is in arbitrary order;
    /// `NO_UNSORTED` means the whole buffer is sorted and unique.
    Buffer buffer;
    size_t unsorted_begin = NO_UNSORTED;
};

}
