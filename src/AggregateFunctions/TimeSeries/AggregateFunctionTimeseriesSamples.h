#pragma once

#include <algorithm>
#include <iterator>
#include <span>
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

/// Storage of the timeseries samples of a bucket or of a whole state: a flat array of (timestamp, value) pairs kept sorted by timestamp, where duplicate timestamps keep the largest real value (a NaN survives only when every sample at the timestamp is NaN).
/// With `compacting`, used for a whole state, an unsorted tail is sorted and merged into the buffer before the buffer grows.
template <typename TimestampType, typename ValueType, bool compacting = false>
class AggregateFunctionTimeseriesSamples
{
public:
    /// The bucket map (`HashMap`) relocates cells with `memcpy` and abandons the source,
    /// which is safe here: the buffer's inline single-sample storage is addressed relative to `this` (and the samples are trivially copyable),
    /// an allocated buffer is reached only through a pointer to the heap - no pointers into itself either way.
    static constexpr bool is_position_independent = true;

    void add(TimestampType timestamp, ValueType value)
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
            if constexpr (compacting)
                compactIfFull(1);
            if (!compacting || isSorted())
                unsorted_begin = buffer.size();
        }
        buffer.emplace_back(timestamp, value);
    }

    ALWAYS_INLINE void addMany(const TimestampType * __restrict timestamps, const ValueType * __restrict values, size_t count)
    {
        if (count == 0)
            return;

        if constexpr (compacting)
            compactIfFull(count);
        const size_t old_size = buffer.size();
        buffer.resize(old_size + count);
        auto * __restrict appended = buffer.data() + old_size;
        for (size_t i = 0; i < count; ++i)
            appended[i] = {timestamps[i], values[i]};

        UInt8 in_order = old_size == 0 || appended[-1].first < timestamps[0];
        for (size_t i = 1; i < count; ++i)
            in_order &= static_cast<UInt8>(timestamps[i - 1] < timestamps[i]);
        if constexpr (compacting)
        {
            /// The first sample alone is sorted, so an unsorted buffer always has a sorted prefix.
            if (isSorted() && !in_order)
                unsorted_begin = std::max<size_t>(old_size, 1);
        }
        else
        {
            /// Without `compacting` only zero or non-zero matters, and a branchless update is faster on shuffled input.
            unsorted_begin |= !in_order;
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

        /// A whole state merges at once only when the buffer is not much bigger than the incoming samples.
        /// Smaller inputs are appended and sorted later, so many small merges do not copy the whole buffer each time.
        if (compacting && buffer.size() > MAX_EAGER_MERGE_RATIO * other.buffer.size())
        {
            compactIfFull(other.buffer.size());
            const size_t old_size = buffer.size();
            const bool in_order = other.isSorted() && buffer.back().first < other.buffer.front().first;
            buffer.insert(buffer.end(), other.buffer.begin(), other.buffer.end());
            if (isSorted() && !in_order)
                unsorted_begin = old_size;
            return;
        }

        sort();

        /// A rare unsorted argument is sorted into a copy: `other` belongs to another state and is kept intact.
        const Buffer * rhs = &other.buffer;
        Buffer sorted_other_buffer;
        if (!other.isSorted())
        {
            sorted_other_buffer = other.buffer;
            sortBuffer(sorted_other_buffer, other.unsorted_begin);
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
        unsorted_begin = 0;

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

    using Sample = std::pair<TimestampType, ValueType>;

    /// Invokes `f` with a span of all samples, in ascending timestamp order with duplicates collapsed.
    template <typename F>
    void withSortedSamples(F && f) const
    {
        /// A rare unsorted state is passed as a sorted copy, so the state is not mutated behind `const`.
        if (!isSorted()) [[unlikely]]
        {
            Buffer sorted_buffer = buffer;
            sortBuffer(sorted_buffer, unsorted_begin);
            f(std::span<const Sample>(sorted_buffer.data(), sorted_buffer.size()));
            return;
        }
        f(std::span<const Sample>(buffer.data(), buffer.size()));
    }

private:
    /// How many samples `deserialize` reserves before reading the data. Bigger buffers grow while they are read.
    static constexpr size_t MAX_SAMPLES_TO_RESERVE = compacting ? 65536 : 4096;
    /// A whole state merges eagerly while its buffer is at most this many times bigger than the incoming samples.
    static constexpr size_t MAX_EAGER_MERGE_RATIO = 16;

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
    static void deduplicateSorted(Buffer & buf)
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
            buf.resize(last_unique + 1);
    }

    /// Sorts the buffer and collapses duplicates; with `compacting` only the tail from `unsorted_begin` is sorted and then merged.
    static void sortBuffer(Buffer & buf, size_t unsorted_begin)
    {
        if constexpr (compacting)
        {
            ::sort(buf.begin() + unsorted_begin, buf.end(), lessByTimestamp);
            std::inplace_merge(buf.begin(), buf.begin() + unsorted_begin, buf.end(), lessByTimestamp);
        }
        else
            ::sort(buf.begin(), buf.end(), lessByTimestamp);
        deduplicateSorted(buf);
    }

    bool isSorted() const { return unsorted_begin == 0; }

    /// Restores the invariant in place after out-of-order `add`s; no-op in the common (already sorted) case.
    void sort()
    {
        if (isSorted())
            return;
        sortBuffer(buffer, unsorted_begin);
        unsorted_begin = 0;
    }

    /// An unsorted buffer is sorted before it grows, so duplicate timestamps collapse instead of taking memory.
    void compactIfFull(size_t count)
    {
        if (!isSorted() && buffer.size() + count > buffer.capacity())
            sort();
    }

    /// The samples: sorted by timestamp and deduplicated, except for an unsorted tail from `unsorted_begin`.
    Buffer buffer;
    /// 0 while the whole buffer is sorted, else the start of its unsorted tail.
    size_t unsorted_begin = 0;
};

}
