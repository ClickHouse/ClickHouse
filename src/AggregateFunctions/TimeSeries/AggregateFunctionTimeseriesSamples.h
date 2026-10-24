#pragma once

#include <algorithm>
#include <bit>
#include <iterator>
#include <limits>
#include <span>
#include <utility>

#include <absl/container/inlined_vector.h>

#include <base/sort.h>

#include <Common/AllocatorWithMemoryTracking.h>
#include <Common/Exception.h>
#include <Common/VectorWithMemoryTracking.h>
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
        /// An unsorted buffer is compacted before any append can grow it, not only before an out-of-order one.
        if constexpr (compacting)
            compactIfFull(1);
        /// Out-of-order and duplicate timestamps are rare (measured ~1 per 1.5 billion adds on production-shaped multithreaded reads), hence `[[unlikely]]`.
        if (!buffer.empty() && timestamp <= buffer.back().first) [[unlikely]]
        {
            auto & last = buffer.back();
            if (timestamp == last.first)
            {
                last.second = timeseriesMaxValueForDuplicateTimestamp(last.second, value);
                return;
            }
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

        /// A whole state appends the samples and sorts them later, when the buffer grows or is read.
        if constexpr (compacting)
        {
            compactIfFull(other.buffer.size());
            append(other);
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

        /// A rare unsorted argument is sorted into a copy: `other` belongs to another state and is kept intact.
        const Buffer * rhs = &other.buffer;
        Buffer sorted_other_buffer;
        if (!other.isSorted())
        {
            sorted_other_buffer = other.sortedCopy();
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
            const Buffer sorted_buffer = sortedCopy();
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
            const Buffer sorted_buffer = sortedCopy();
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
            const Buffer sorted_buffer = sortedCopy();
            f(std::span<const Sample>(sorted_buffer.data(), sorted_buffer.size()));
            return;
        }
        f(std::span<const Sample>(buffer.data(), buffer.size()));
    }

private:
    /// How many samples `deserialize` reserves before reading the data. Bigger buffers grow while they are read.
    static constexpr size_t MAX_SAMPLES_TO_RESERVE = compacting ? 65536 : 4096;
    /// A buffer of at most this many sorted runs is sorted by merging them into another buffer, a buffer of more runs in place.
    static constexpr size_t MAX_RUNS_TO_MERGE_INTO = 16;
    /// Runs that alternate in stretches shorter than this on average are merged in pairs; checked every `STRETCHES_PER_CHECK` stretches.
    static constexpr size_t MIN_AVERAGE_STRETCH_LENGTH = 4;
    static constexpr size_t STRETCHES_PER_CHECK = 64;
    /// An unsorted tail whose sorted runs are shorter than this on average is sorted instead of merged run by run.
    static constexpr size_t MIN_AVERAGE_RUN_LENGTH = 32;
    /// A tail shorter than this is comparison-sorted at once; a longer one is first partitioned into groups of this many samples on average.
    static constexpr size_t MIN_PARTITION_SORT_SIZE = 2048;
    static constexpr size_t PARTITION_SORT_GROUP_SIZE = 8;

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
            if (!mergeTailRuns(buf, unsorted_begin))
            {
                sortSamples(buf.data() + unsorted_begin, buf.data() + buf.size());
                std::inplace_merge(buf.begin(), buf.begin() + unsorted_begin, buf.end(), lessByTimestamp);
            }
        }
        else
            ::sort(buf.begin(), buf.end(), lessByTimestamp);
        deduplicateSorted(buf);
    }

    /// Sorts the samples in [first, last) by timestamp. A long range is first split into groups of consecutive timestamps
    /// in one counting pass, and the small groups are sorted on their own, which costs much less per sample
    /// than comparison-sorting the whole range.
    static void sortSamples(Sample * first, Sample * last)
    {
        const size_t size = last - first;
        /// The group offsets below are 32-bit, so a tail of 2^32 samples or more is comparison-sorted.
        if (size < MIN_PARTITION_SORT_SIZE || size > std::numeric_limits<UInt32>::max())
        {
            ::sort(first, last, lessByTimestamp);
            return;
        }

        const auto minmax = std::minmax_element(first, last, lessByTimestamp);
        const UInt64 min_timestamp = static_cast<UInt64>(static_cast<Int64>(minmax.first->first));
        auto offset = [&](TimestampType timestamp) { return static_cast<UInt64>(static_cast<Int64>(timestamp)) - min_timestamp; };

        /// The offsets are scaled down to 32 bits, so that `(offset >> shift) * scale` fits 64 bits and a group index is below `groups`.
        const UInt64 span = offset(minmax.second->first);
        const unsigned shift = (span >> 32) ? std::bit_width(span >> 32) : 0;
        const UInt64 scaled_span = (span >> shift) + 1;
        const size_t groups = size / PARTITION_SORT_GROUP_SIZE;
        const UInt64 scale = (static_cast<UInt64>(groups) << 32) / scaled_span;
        auto group_of = [&](TimestampType timestamp) { return static_cast<size_t>(((offset(timestamp) >> shift) * scale) >> 32); };

        /// `group_ends[g]` first holds the start of group `g` and ends up holding its end.
        VectorWithMemoryTracking<UInt32> group_ends(groups + 1);
        for (const Sample * sample = first; sample != last; ++sample)
            ++group_ends[group_of(sample->first) + 1];
        for (size_t g = 0; g < groups; ++g)
            group_ends[g + 1] += group_ends[g];

        Buffer partitioned;
        partitioned.resize(size);
        for (const Sample * sample = first; sample != last; ++sample)
            partitioned[group_ends[group_of(sample->first)]++] = *sample;

        size_t group_begin = 0;
        for (size_t g = 0; g < groups; ++g)
        {
            ::sort(partitioned.data() + group_begin, partitioned.data() + group_ends[g], lessByTimestamp);
            group_begin = group_ends[g];
        }
        std::copy(partitioned.begin(), partitioned.end(), first);
    }

    /// Merges a tail made of long sorted runs (one per part read) into the buffer; returns false if the runs are short.
    static bool mergeTailRuns(Buffer & buf, size_t unsorted_begin)
    {
        const size_t max_runs = (buf.size() - unsorted_begin) / MIN_AVERAGE_RUN_LENGTH + 1;
        absl::InlinedVector<size_t, 16> bounds{unsorted_begin};
        for (size_t i = unsorted_begin + 1; i < buf.size(); ++i)
        {
            if (buf[i].first >= buf[i - 1].first)
                continue;
            if (bounds.size() == max_runs)
                return false;
            bounds.push_back(i);
        }
        bounds.push_back(buf.size());

        Buffer tmp;
        mergeRuns(buf.data(), bounds.data(), bounds.size() - 1, tmp);
        gallopMerge(buf.data(), buf.data() + unsorted_begin, buf.data() + buf.size(), tmp);
        return true;
    }

    /// Merges the `count` adjacent sorted runs starting at `bounds[0..count)`, the last one ending at `bounds[count]`.
    static void mergeRuns(Sample * data, const size_t * bounds, size_t count, Buffer & tmp)
    {
        if (count < 2)
            return;
        const size_t half = count / 2;
        mergeRuns(data, bounds, half, tmp);
        mergeRuns(data, bounds + half, count - half, tmp);
        gallopMerge(data + bounds[0], data + bounds[half], data + bounds[count], tmp);
    }

    struct Run
    {
        const Sample * begin;
        const Sample * end;
    };
    using Runs = absl::InlinedVector<Run, MAX_RUNS_TO_MERGE_INTO>;

    /// Collects the sorted part and the sorted runs of the tail of `buf`; returns false if there are more than `MAX_RUNS_TO_MERGE_INTO` runs.
    static bool findRuns(const Buffer & buf, size_t unsorted_begin, Runs & runs, bool & has_duplicates)
    {
        const Sample * data = buf.data();
        runs.assign({{data, data + unsorted_begin}});
        has_duplicates = false;
        size_t run_begin = unsorted_begin;
        for (size_t i = unsorted_begin + 1; i < buf.size(); ++i)
        {
            if (data[i].first > data[i - 1].first)
                continue;
            if (data[i].first == data[i - 1].first)
            {
                has_duplicates = true;
                continue;
            }
            if (runs.size() == MAX_RUNS_TO_MERGE_INTO)
                return false;
            runs.push_back({data + run_begin, data + i});
            run_begin = i;
        }
        runs.push_back({data + run_begin, data + buf.size()});
        return true;
    }

    /// Writes the samples of the runs to the empty `dst` sorted and deduplicated. The run with the smallest next timestamp gives its samples
    /// up to the next timestamp of the other runs at once; runs that alternate in short stretches (e.g. with shared timestamps) are merged in pairs.
    static void mergeRunsInto(Runs & runs, Buffer & dst, bool has_duplicates)
    {
        size_t total = 0;
        for (const auto & run : runs)
            total += run.end - run.begin;
        dst.resize(total);
        Sample * const out_begin = dst.data();
        Sample * out = out_begin;

        auto earlier = [](const Run & lhs, const Run & rhs) { return lhs.begin->first < rhs.begin->first; };
        ::sort(runs.begin(), runs.end(), earlier);
        size_t stretches = 0;
        const Sample * window_begin = out;
        while (runs.size() > 2)
        {
            Run run = runs.front();
            const TimestampType next = runs[1].begin->first;
            const Sample * stop = run.begin + 1;
            if (stop != run.end && stop->first <= next)
                stop = gallop(stop, run.end, [&](const Sample & sample) { return sample.first <= next; });
            /// The first sample may share its timestamp with the last one written, which comes from another run.
            if (out != out_begin && run.begin->first == out[-1].first)
            {
                out[-1].second = timeseriesMaxValueForDuplicateTimestamp(out[-1].second, run.begin->second);
                ++run.begin;
            }
            out = std::copy(run.begin, stop, out);
            run.begin = stop;
            if (run.begin == run.end)
                runs.erase(runs.begin());
            else
            {
                size_t pos = 0;
                for (; pos + 1 < runs.size() && !earlier(run, runs[pos + 1]); ++pos)
                    runs[pos] = runs[pos + 1];
                runs[pos] = run;
            }
            if (++stretches % STRETCHES_PER_CHECK == 0)
            {
                if (static_cast<size_t>(out - window_begin) < STRETCHES_PER_CHECK * MIN_AVERAGE_STRETCH_LENGTH)
                    break;
                window_begin = out;
            }
        }

        /// The rest is merged without looking at the samples written so far, so its first sample may repeat the last one.
        Sample * const rest_begin = out;
        out = runs.size() == 1 ? std::copy(runs.front().begin, runs.front().end, out) : mergeInPairs(runs, out);
        if (rest_begin != out_begin && rest_begin != out && rest_begin[-1].first == rest_begin->first)
            has_duplicates = true;
        dst.resize(out - out_begin);
        if (has_duplicates)
            deduplicateSorted(dst);
    }

    /// Writes the samples of the runs to `out` sorted, merging adjacent runs in pairs level by level between `out` and a temporary buffer.
    /// The last level writes to `out`; returns the end of the written samples.
    static Sample * mergeInPairs(Runs & runs, Sample * out)
    {
        size_t total = 0;
        for (const auto & run : runs)
            total += run.end - run.begin;
        size_t levels = 0;
        while ((size_t{1} << levels) < runs.size())
            ++levels;
        Buffer tmp;
        if (levels > 1)
            tmp.resize(total);

        Runs merged_runs;
        Sample * level_out = out;
        for (size_t level = 0; level < levels; ++level)
        {
            level_out = (levels - 1 - level) % 2 == 0 ? out : tmp.data();
            merged_runs.clear();
            for (size_t i = 0; i < runs.size(); i += 2)
            {
                Sample * begin = level_out;
                if (i + 1 < runs.size())
                    level_out = mergeTwo(runs[i], runs[i + 1], level_out);
                else
                    level_out = std::copy(runs[i].begin, runs[i].end, level_out);
                merged_runs.push_back({begin, level_out});
            }
            runs.swap(merged_runs);
        }
        return level_out;
    }

    /// Writes the samples of two sorted runs to `out` in timestamp order; samples of both runs with the same timestamp become one.
    static Sample * mergeTwo(Run lhs, Run rhs, Sample * out)
    {
        while (lhs.begin != lhs.end && rhs.begin != rhs.end)
        {
            if (lhs.begin->first < rhs.begin->first)
            {
                const TimestampType next = rhs.begin->first;
                const Sample * stop = gallop(lhs.begin, lhs.end, [&](const Sample & sample) { return sample.first < next; });
                out = std::copy(lhs.begin, stop, out);
                lhs.begin = stop;
            }
            else if (rhs.begin->first < lhs.begin->first)
            {
                const TimestampType next = lhs.begin->first;
                const Sample * stop = gallop(rhs.begin, rhs.end, [&](const Sample & sample) { return sample.first < next; });
                out = std::copy(rhs.begin, stop, out);
                rhs.begin = stop;
            }
            else
            {
                *out++ = {lhs.begin->first, timeseriesMaxValueForDuplicateTimestamp(lhs.begin->second, rhs.begin->second)};
                ++lhs.begin;
                ++rhs.begin;
            }
        }
        out = std::copy(lhs.begin, lhs.end, out);
        return std::copy(rhs.begin, rhs.end, out);
    }

    /// Merges the sorted ranges [first, middle) and [middle, last), moving each stretch of one side
    /// that comes before the other side's next sample at once. Equal timestamps keep the left side first.
    static void gallopMerge(Sample * first, Sample * middle, Sample * last, Buffer & tmp)
    {
        /// Samples that are already in place at either end are not moved.
        first = std::upper_bound(first, middle, *middle, lessByTimestamp);
        last = std::lower_bound(middle, last, middle[-1], lessByTimestamp);
        if (first == middle || middle == last)
            return;

        tmp.assign(first, middle);
        const Sample * left = tmp.data();
        const Sample * left_end = left + tmp.size();
        Sample * right = middle;
        Sample * out = first;
        while (true)
        {
            Sample * right_stop = gallop(right, last, [&](const Sample & sample) { return sample.first < left->first; });
            out = std::copy(right, right_stop, out);
            right = right_stop;
            if (right == last)
                break;
            const Sample * left_stop = gallop(left, left_end, [&](const Sample & sample) { return sample.first <= right->first; });
            out = std::copy(left, left_stop, out);
            left = left_stop;
            if (left == left_end)
                return;
        }
        std::copy(left, left_end, out);
    }

    /// Returns the end of the prefix of [begin, end) where `pred` holds, knowing it holds for `*begin`.
    /// Probes positions 1, 3, 7, 15, ... and then bisects, so a long stretch costs few comparisons.
    template <typename Iterator, typename Pred>
    static Iterator gallop(Iterator begin, Iterator end, Pred pred)
    {
        const size_t size = end - begin;
        size_t low = 1;
        size_t high = 2;
        while (high <= size && pred(begin[high - 1]))
        {
            low = high;
            high *= 2;
        }
        return std::partition_point(begin + low, begin + std::min(high - 1, size), pred);
    }

    bool isSorted() const { return unsorted_begin == 0; }

    /// Returns the samples of an unsorted state sorted and deduplicated, leaving the state as is.
    Buffer sortedCopy() const
    {
        Buffer sorted_buffer;
        if constexpr (compacting)
        {
            Runs runs;
            bool has_duplicates = false;
            if (findRuns(buffer, unsorted_begin, runs, has_duplicates))
            {
                sorted_buffer.reserve(buffer.size());
                mergeRunsInto(runs, sorted_buffer, has_duplicates);
                return sorted_buffer;
            }
        }
        sorted_buffer = buffer;
        sortBuffer(sorted_buffer, unsorted_begin);
        return sorted_buffer;
    }

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
        if (isSorted() || buffer.size() + count <= buffer.capacity())
            return;
        compact(count);
    }

    void NO_INLINE compact(size_t count)
    {
        /// When the sorted part alone needs a bigger buffer, a few runs are merged right into it, which costs about as much as growing.
        Runs runs;
        bool has_duplicates = false;
        if (unsorted_begin + count > buffer.capacity() / 2 && findRuns(buffer, unsorted_begin, runs, has_duplicates))
        {
            Buffer merged;
            merged.reserve(std::max(buffer.capacity() * 2, buffer.size() + count));
            mergeRunsInto(runs, merged, has_duplicates);
            buffer = std::move(merged);
            unsorted_begin = 0;
            return;
        }

        sort();
        /// Grow when sorting freed less than half of the buffer, so a full sort happens at most once per half-buffer of new samples.
        if (buffer.size() + count > buffer.capacity() / 2)
            buffer.reserve(std::max(buffer.capacity() * 2, buffer.size() + count));
    }

    /// Appends the samples of another state; its unsorted tail is sorted where it lands, so the buffer's tail stays made of long sorted runs.
    void append(const AggregateFunctionTimeseriesSamples & other)
    {
        const size_t old_size = buffer.size();
        buffer.insert(buffer.end(), other.buffer.begin(), other.buffer.end());
        const bool in_order = other.isSorted() && (old_size == 0 || buffer[old_size - 1].first < buffer[old_size].first);
        if (isSorted() && !in_order)
            unsorted_begin = old_size ? old_size : other.unsorted_begin;
        if (!other.isSorted())
            sortSamples(buffer.data() + old_size + other.unsorted_begin, buffer.data() + buffer.size());
    }

    /// The samples: sorted by timestamp and deduplicated, except for an unsorted tail from `unsorted_begin`.
    Buffer buffer;
    /// 0 while the whole buffer is sorted, else the start of its unsorted tail.
    size_t unsorted_begin = 0;
};

}
