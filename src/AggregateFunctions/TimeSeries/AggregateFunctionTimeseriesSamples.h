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

        sort();

        /// Both the prefix and the tail of `other` are inserted; one reservation saves a second reallocation.
        buffer.reserve(buffer.size() + other.buffer.size());

        /// The sorted prefix of `other` is merged first, so its order is kept; the unsorted tail of `other`, if any, then becomes the tail of this buffer.
        /// `other` belongs to another state and is kept intact.
        const auto other_prefix_begin = other.buffer.begin();
        const auto other_prefix_end = other.isSorted() ? other.buffer.end() : other.buffer.begin() + other.unsorted_begin;

        /// Partial states often cover disjoint timestamp ranges - then the merge is a plain append or prepend.
        if (buffer.back().first < other_prefix_begin->first)
        {
            buffer.insert(buffer.end(), other_prefix_begin, other_prefix_end);
        }
        else if ((other_prefix_end - 1)->first < buffer.front().first)
        {
            buffer.insert(buffer.begin(), other_prefix_begin, other_prefix_end);
        }
        else
        {
            /// Overlapping ranges: the prefix samples are appended and merged into this buffer in place.
            const size_t tail_begin = buffer.size();
            buffer.insert(buffer.end(), other_prefix_begin, other_prefix_end);
            mergeSortedTail(buffer, tail_begin);
        }

        if (other_prefix_end != other.buffer.end())
        {
            unsorted_begin = buffer.size();
            buffer.insert(buffer.end(), other_prefix_end, other.buffer.end());
            sort();
        }
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

    using Sample = std::pair<TimestampType, ValueType>;

    /// Some buckets hold a single sample - the inline capacity of 1 keeps it in the state itself with no heap allocation.
    using Buffer = absl::InlinedVector<Sample, /* N = */ 1, AllocatorWithMemoryTracking<Sample>>;

    static void writeSamples(const Buffer & samples, WriteBuffer & buf)
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

    /// Collapses each run of adjacent samples with equal timestamps into one sample with `timeseriesMaxValueForDuplicateTimestamp`.
    /// Samples before `start_pos` are already unique.
    static void deduplicateSorted(Buffer & buf, size_t start_pos = 0)
    {
        chassert(start_pos <= buf.size());
        size_t last_unique = start_pos == 0 ? 0 : start_pos - 1;
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

    /// Sorts an unsorted tail. It usually consists of a few ascending runs (the samples of one series or of one part each),
    /// so the runs are found and merged; a tail with too many runs for its size is sorted the usual way.
    static void sortTail(Sample * samples, size_t size)
    {
        /// Merging beats `::sort` while `num_runs <= sqrt(size) / 2`, which is checked as `4 * num_runs * num_runs <= size`.
        Sample * const end = samples + size;
        Sample * run_ends[MAX_RUNS_TO_MERGE];
        size_t num_runs = 0;
        for (Sample * pos = samples; pos != end;)
        {
            if (num_runs == MAX_RUNS_TO_MERGE || 4 * (num_runs + 1) * (num_runs + 1) > size)
            {
                ::sort(samples, end, lessByTimestamp);
                return;
            }
            pos = std::is_sorted_until(pos, end, lessByTimestamp);
            run_ends[num_runs++] = pos;
        }

        /// Adjacent runs are merged pairwise until one run is left.
        while (num_runs > 1)
        {
            size_t num_merged_runs = 0;
            Sample * run_begin = samples;
            for (size_t i = 0; i + 1 < num_runs; i += 2)
            {
                std::inplace_merge(run_begin, run_ends[i], run_ends[i + 1], lessByTimestamp);
                run_begin = run_ends[i + 1];
                run_ends[num_merged_runs++] = run_ends[i + 1];
            }
            if (num_runs % 2 != 0)
                run_ends[num_merged_runs++] = run_ends[num_runs - 1];
            num_runs = num_merged_runs;
        }
    }

    /// Sorts the tail `[start_pos, size)` of a buffer whose prefix `[0, start_pos)` is sorted and unique, and merges the tail into the prefix.
    static void sortBuffer(Buffer & buf, size_t start_pos = 0)
    {
        chassert(start_pos <= buf.size());
        sortTail(buf.data() + start_pos, buf.size() - start_pos);
        /// Many series with equal timestamps in one bucket collapse to the distinct timestamps here; only those are merged into the prefix.
        deduplicateSorted(buf, start_pos);
        mergeSortedTail(buf, start_pos);
    }

    /// Merges the sorted and unique tail `[tail_begin, size)` into the sorted and unique prefix `[0, tail_begin)` in place.
    static void mergeSortedTail(Buffer & buf, size_t tail_begin)
    {
        chassert(tail_begin <= buf.size());
        if (tail_begin == buf.size())
            return;

        /// A tail that lies entirely before the prefix, such as an older part read after a newer one, is moved in front of it.
        if ((buf.end() - 1)->first < buf.begin()->first)
        {
            std::rotate(buf.begin(), buf.begin() + tail_begin, buf.end());
            return;
        }

        /// The prefix samples before `merge_begin` are less than every sample of the tail, so they keep their positions,
        /// and the merge and the deduplication start at `merge_begin`.
        auto tail = buf.begin() + tail_begin;
        auto merge_begin = std::lower_bound(buf.begin(), tail, *tail, lessByTimestamp);
        /// With the tail after the prefix there is nothing to merge, and a unique tail leaves nothing to deduplicate.
        if (merge_begin == tail)
            return;
        std::inplace_merge(merge_begin, tail, buf.end(), lessByTimestamp);
        deduplicateSorted(buf, merge_begin - buf.begin());
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
        /// A small buffer is cheaper to grow than to sort at every growth.
        if (buffer.capacity() < MIN_CAPACITY_TO_COMPACT)
            return;
        sort();
        if (buffer.size() > buffer.capacity() / 2)
            buffer.reserve(std::max(buffer.size() + extra, 2 * buffer.capacity()));
    }

    /// The most runs of an unsorted tail that are merged, whatever its size.
    static constexpr size_t MAX_RUNS_TO_MERGE = 64;

    /// Buffers with a smaller capacity grow without sorting the unsorted tail first.
    static constexpr size_t MIN_CAPACITY_TO_COMPACT = 64;

    /// The value of `unsorted_begin` for a buffer with no unsorted tail.
    static constexpr size_t NO_UNSORTED = std::numeric_limits<size_t>::max();

    /// The samples. The prefix `[0, unsorted_begin)` is sorted by timestamp and unique, the rest is in arbitrary order;
    /// `NO_UNSORTED` means the whole buffer is sorted and unique.
    Buffer buffer;
    size_t unsorted_begin = NO_UNSORTED;
};

}
