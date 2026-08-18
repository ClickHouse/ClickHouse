#pragma once

#include <algorithm>
#include <iterator>
#include <memory>
#include <utility>
#include <vector>

#include <absl/container/inlined_vector.h>

#include <base/sort.h>

#include <Common/AllocatorWithMemoryTracking.h>
#include <Common/Exception.h>
#include <Common/NaNUtils.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

/// Per-bucket storage of timeseries samples: a flat array of (timestamp, value) pairs kept sorted by timestamp, where duplicate timestamps keep the largest real value (a NaN survives only when every sample at the timestamp is NaN).
///
/// Besides its own buffer, a state can hold buffers ADOPTED from other states by `adopt` - the
/// destructive merge used when the source state is destroyed right after merging (the two-level
/// aggregation merge, see `IAggregateFunction::mergeAndDestroyBatch`). Adoption steals the source
/// buffer in O(1) instead of copying every sample; iteration then k-way merges the buffers.
/// Partial aggregation states almost always cover disjoint timestamp ranges (parallel streams read
/// disjoint ranges of a part), so the k-way merge nearly always degenerates into iterating the
/// buffers one after another.
template <typename TimestampType, typename ValueType>
class AggregateFunctionTimeseriesSamples
{
public:
    /// The bucket map (`HashMap`) relocates cells with `memcpy` and abandons the source,
    /// which is safe here: the buffer's inline single-sample storage is addressed relative to `this` (and the samples are trivially copyable),
    /// an allocated buffer and the list of adopted buffers are reached only through pointers to the heap - no pointers into itself either way.
    static constexpr bool is_position_independent = true;

    AggregateFunctionTimeseriesSamples() = default;
    /// Not copyable: the state owns the adopted chunk list through a raw tagged pointer.
    /// (Nothing copies or moves whole states: the bucket maps relocate them with `memcpy`
    /// and abandon the source, and merging works on references.)
    AggregateFunctionTimeseriesSamples(const AggregateFunctionTimeseriesSamples &) = delete;
    AggregateFunctionTimeseriesSamples & operator=(const AggregateFunctionTimeseriesSamples &) = delete;
    ~AggregateFunctionTimeseriesSamples() { delete adoptedChunks(); }

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
            setSorted(false);
        }
        buffer.emplace_back(timestamp, value);
    }

    ALWAYS_INLINE void addMany(const TimestampType * __restrict timestamps, const ValueType * __restrict values, size_t count)
    {
        if (count == 0)
            return;

        const size_t old_size = buffer.size();
        buffer.resize(old_size + count);
        auto * __restrict appended = buffer.data() + old_size;
        for (size_t i = 0; i < count; ++i)
            appended[i] = {timestamps[i], values[i]};

        UInt8 in_order = old_size == 0 || appended[-1].first < timestamps[0];
        for (size_t i = 1; i < count; ++i)
            in_order &= static_cast<UInt8>(timestamps[i - 1] < timestamps[i]);
        if (!in_order)
            setSorted(false);
    }

    /// The destructive merge: steals the other state's buffers in O(1) instead of copying the
    /// samples. The other state is left empty; the caller destroys it right after.
    void adopt(AggregateFunctionTimeseriesSamples && other)
    {
        /// A small buffer is cheaper to copy than to keep as a separate chunk: an adopted chunk
        /// costs a heap allocation for the chunk list plus per-chunk cursor work on every
        /// iteration, while copying N samples is ~N ns. Range queries with narrow windows keep
        /// just a few samples per bucket, and this keeps their merge identical to the copy path.
        /// (An empty own state still steals the buffer wholesale below, which is free.)
        if (!other.adoptedChunks() && (other.buffer.size() <= ADOPT_MIN_SAMPLES) && !(buffer.empty() && !adoptedChunks()))
        {
            merge(other);
            other.buffer = {};
            other.setSorted(true);
            return;
        }

        if (!other.buffer.empty())
        {
            /// A rare unsorted buffer is sorted at adoption (the other state is ours to mutate),
            /// so every adopted chunk is sorted and deduplicated - the iteration relies on that.
            if (!other.isSorted())
                sortBuffer(other.buffer);

            if (buffer.empty() && !adoptedChunks())
            {
                buffer = std::move(other.buffer);
                setSorted(true);
            }
            else
            {
                ensureAdopted().push_back(std::move(other.buffer));
            }
        }

        if (auto * other_chunks = other.adoptedChunks())
        {
            auto & chunks = ensureAdopted();
            for (auto & chunk : *other_chunks)
                chunks.push_back(std::move(chunk));
            delete other_chunks;
            other.setAdoptedChunks(nullptr);
        }

        other.buffer = {};
        other.setSorted(true);
    }

    void merge(const AggregateFunctionTimeseriesSamples & other)
    {
        if (other.empty())
        {
            /// Nothing to merge: the state is left as is (a rare unsorted state stays unsorted until a later operation sorts it).
            return;
        }

        /// The non-destructive merge flattens both sides into one buffer (it must copy the other
        /// state's samples anyway). The hot merge path (the two-level aggregation merge) uses
        /// `adopt` instead and never gets here with adopted chunks.
        if (adoptedChunks() || other.adoptedChunks())
        {
            consolidate();
            Buffer other_samples = other.flattenedCopy();
            mergeSortedBuffer(other_samples);
            return;
        }

        if (buffer.empty())
        {
            buffer = other.buffer;
            setSorted(other.isSorted());
            sort();
            return;
        }

        sort();

        /// A rare unsorted argument is sorted into a copy: `other` belongs to another state and is kept intact.
        Buffer sorted_other_buffer = other.buffer;
        if (!other.isSorted())
            sortBuffer(sorted_other_buffer);
        mergeSortedBuffer(sorted_other_buffer);
    }

    void serialize(WriteBuffer & buf) const
    {
        if (adoptedChunks()) [[unlikely]]
        {
            writeSamples(flattenedCopy(), buf);
            return;
        }
        /// A rare unsorted state is serialized from a sorted copy, so the state is not mutated behind `const`.
        if (!isSorted()) [[unlikely]]
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
        delete adoptedChunks();
        adopted_and_unsorted = 0;

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
        const AdoptedChunks * chunks = adoptedChunks();
        if (!chunks) [[likely]]
        {
            /// A rare unsorted state is iterated via a sorted copy, so the state is not mutated behind `const`.
            if (!isSorted()) [[unlikely]]
            {
                Buffer sorted_buffer = buffer;
                sortBuffer(sorted_buffer);
                for (const auto & [timestamp, value] : sorted_buffer)
                    f(timestamp, value);
                return;
            }
            for (const auto & [timestamp, value] : buffer)
                f(timestamp, value);
            return;
        }

        /// A rare unsorted own buffer is iterated via a sorted copy (adopted chunks are always sorted).
        Buffer sorted_own;
        const Buffer * own = &buffer;
        if (!isSorted()) [[unlikely]]
        {
            sorted_own = buffer;
            sortBuffer(sorted_own);
            own = &sorted_own;
        }

        /// The cursors over the sorted chunks, ordered by the first timestamp.
        absl::InlinedVector<Cursor, 4, AllocatorWithMemoryTracking<Cursor>> cursors;
        if (!own->empty())
            cursors.push_back({own->data(), own->data() + own->size()});
        for (const auto & chunk : *chunks)
            if (!chunk.empty())
                cursors.push_back({chunk.data(), chunk.data() + chunk.size()});
        std::sort(cursors.begin(), cursors.end(), [](const Cursor & lhs, const Cursor & rhs) { return lhs.it->first < rhs.it->first; });

        /// Partial states almost always cover disjoint timestamp ranges: then the chunks are just
        /// iterated one after another with no per-sample work.
        bool disjoint = true;
        for (size_t i = 1; i < cursors.size(); ++i)
            disjoint &= (cursors[i - 1].end[-1].first < cursors[i].it->first);
        if (disjoint) [[likely]]
        {
            for (const auto & cursor : cursors)
                for (const auto * sample = cursor.it; sample != cursor.end; ++sample)
                    f(sample->first, sample->second);
            return;
        }

        /// Overlapping chunks (out-of-order writes which ended up in overlapping parts): a k-way
        /// merge with the same duplicate collapsing as `deduplicateSorted`.
        while (!cursors.empty())
        {
            size_t min_index = 0;
            for (size_t i = 1; i < cursors.size(); ++i)
                if (cursors[i].it->first < cursors[min_index].it->first)
                    min_index = i;

            const TimestampType timestamp = cursors[min_index].it->first;
            ValueType value = cursors[min_index].it->second;
            ++cursors[min_index].it;

            for (size_t i = 0; i < cursors.size();)
            {
                auto & cursor = cursors[i];
                while (cursor.it != cursor.end && cursor.it->first == timestamp)
                {
                    value = getMax(value, cursor.it->second);
                    ++cursor.it;
                }
                if (cursor.it == cursor.end)
                    cursors.erase(cursors.begin() + i);
                else
                    ++i;
            }

            f(timestamp, value);
        }
    }

private:
    /// Buffers at most this big are copied by `adopt` instead of being kept as separate chunks.
    static constexpr size_t ADOPT_MIN_SAMPLES = 64;

    /// Some buckets hold a single sample - the inline capacity of 1 keeps it in the state itself with no heap allocation.
    using Buffer = absl::InlinedVector<
        std::pair<TimestampType, ValueType>,
        /* N = */ 1,
        AllocatorWithMemoryTracking<std::pair<TimestampType, ValueType>>>;

    /// Buffers stolen from other states by `adopt`; every chunk is sorted and deduplicated.
    using AdoptedChunks = std::vector<Buffer, AllocatorWithMemoryTracking<Buffer>>;

    struct Cursor
    {
        const std::pair<TimestampType, ValueType> * it;
        const std::pair<TimestampType, ValueType> * end;
    };

    bool isSorted() const { return (adopted_and_unsorted & 1) == 0; }
    void setSorted(bool sorted) { adopted_and_unsorted = sorted ? (adopted_and_unsorted & ~uintptr_t{1}) : (adopted_and_unsorted | 1); }
    AdoptedChunks * adoptedChunks() const { return reinterpret_cast<AdoptedChunks *>(adopted_and_unsorted & ~uintptr_t{1}); }
    void setAdoptedChunks(AdoptedChunks * chunks) { adopted_and_unsorted = reinterpret_cast<uintptr_t>(chunks) | (adopted_and_unsorted & 1); }

    bool empty() const { return buffer.empty() && !adoptedChunks(); }

    AdoptedChunks & ensureAdopted()
    {
        if (!adoptedChunks())
            setAdoptedChunks(new AdoptedChunks());
        return *adoptedChunks();
    }

    /// Returns all the samples flattened into one sorted deduplicated buffer.
    Buffer flattenedCopy() const
    {
        Buffer res;
        size_t total = buffer.size();
        if (const auto * chunks = adoptedChunks())
            for (const auto & chunk : *chunks)
                total += chunk.size();
        res.reserve(total);
        forEachSample([&res](TimestampType timestamp, ValueType value) { res.emplace_back(timestamp, value); });
        return res;
    }

    /// Consolidates the adopted chunks into the own buffer.
    void consolidate()
    {
        if (!adoptedChunks())
        {
            sort();
            return;
        }
        Buffer flattened = flattenedCopy();
        buffer = std::move(flattened);
        delete adoptedChunks();
        adopted_and_unsorted = 0;
    }

    /// Merges a sorted deduplicated buffer into the own sorted buffer (the pre-`adopt` merge logic).
    void mergeSortedBuffer(const Buffer & rhs)
    {
        if (rhs.empty())
            return;
        if (buffer.empty())
        {
            buffer = rhs;
            return;
        }

        /// Partial states often cover disjoint timestamp ranges - then the merge is a plain append or prepend.
        if (buffer.back().first < rhs.front().first)
        {
            buffer.insert(buffer.end(), rhs.begin(), rhs.end());
            return;
        }
        if (rhs.back().first < buffer.front().first)
        {
            buffer.insert(buffer.begin(), rhs.begin(), rhs.end());
            return;
        }

        Buffer merged;
        merged.reserve(buffer.size() + rhs.size());
        std::merge(buffer.begin(), buffer.end(), rhs.begin(), rhs.end(), std::back_inserter(merged), lessByTimestamp);
        deduplicateSorted(merged);
        buffer = std::move(merged);
    }

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
        if (isSorted())
            return;
        sortBuffer(buffer);
        setSorted(true);
    }

    /// The samples, sorted by timestamp and deduplicated whenever the unsorted bit is clear.
    Buffer buffer;
    /// A tagged word: the upper bits hold the `AdoptedChunks *` with buffers adopted from
    /// destructively merged states (see `adopt`), bit 0 is set while `buffer` is NOT sorted
    /// (an out-of-order `add` sets it; while clear, timestamps in `buffer` are strictly
    /// increasing). Packed into one word because this struct is the per-bucket aggregation
    /// state: one extra pointer per bucket is measurable on wide grids.
    /// Zero-initialized = sorted, no adopted chunks.
    uintptr_t adopted_and_unsorted = 0;
};

}
