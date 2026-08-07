#pragma once

#include <algorithm>
#include <bit>
#include <new>
#include <type_traits>
#include <utility>

#include <base/defines.h>
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

/// Per-bucket storage of timeseries samples sorted by timestamp with duplicates keeping the larger value under the IEEE-754 total order (see `Representation::maxValue`).
/// Two representations, each a `Representation` subclass with its own `add`, `serialize` and `deserialize`:
///  - `RawRepresentation`: a sorted-append vector, the bucket's form while it receives its in-order stream;
///  - `PackedRepresentation`: a compact blob (varint delta-of-delta timestamps, integer-delta or verbatim values), the form of a bucket sealed by the write cursor leaving it or grown past `SELF_PACK_THRESHOLD`.
/// This class is a tagged union of the two: `storage_type` selects which subclass occupies the union, and the class itself only orchestrates the transitions between them (sealing, self-packing, unpack-repairs) and the cross-representation merge.
///
/// The packed encoding is bit-exact: the encoder emits a compact token only after verifying that decoding it
/// reproduces the sample's exact bit pattern (and falls back to a verbatim token otherwise), so packing and unpacking
/// round-trip the sample sequence losslessly and every operation on a packed bucket is equivalent to the same
/// operation on the raw vector. It is NOT a lossy per-bucket summary: a summary cannot absorb a late interior sample
/// (whether its timestamp collides with a discarded sample, and how it shifts the counter-reset correction, depend on
/// discarded neighbors), and late samples and overlapping merges are legal at any time, so the raw sequence must
/// remain recoverable. The blob never leaves the process: the wire format below stays plain (count + pairs).
///
/// Operations on a packed bucket:
///  - append (timestamp above the packed range) extends the blob in O(1);
///  - a duplicate of the last timestamp with a value that loses the max-dedup is dropped in O(1);
///  - anything else (an interior or preceding timestamp, or a duplicate of the last timestamp winning the dedup) is
///    rare - the bucket unpacks back to the raw vector, replays the proven raw-path logic, and is packed again when
///    the write cursor leaves it. Each such unpack-repair is counted in `repair_unpacks`; at `MAX_REPAIR_UNPACKS`
///    the bucket stays raw for good, so sustained out-of-order traffic degrades memory to the old raw behavior
///    instead of burning CPU on a decode+encode cycle per repair.
template <typename TimestampType, typename ValueType>
class AggregateFunctionTimeseriesSamples
{
public:
    /// The bucket map (`HashMap`) relocates cells with `memcpy` and abandons the source, which is safe here: both union members hold only heap pointers (the sample vector / the token blob) and scalars, no pointers into the cell itself.
    static constexpr bool is_position_independent = true;

    AggregateFunctionTimeseriesSamples() : raw() {}

    AggregateFunctionTimeseriesSamples(const AggregateFunctionTimeseriesSamples &) = delete;
    AggregateFunctionTimeseriesSamples & operator=(const AggregateFunctionTimeseriesSamples &) = delete;

    AggregateFunctionTimeseriesSamples(AggregateFunctionTimeseriesSamples && other) noexcept
        : storage_type(other.storage_type)
        , repair_unpacks(other.repair_unpacks)
    {
        if (storage_type == StorageType::Packed)
            new (&packed) PackedRepresentation(std::move(other.packed));
        else
            new (&raw) RawRepresentation(std::move(other.raw));
        /// Leave the source a valid empty state: a moved-from packed representation would keep its sample count with no tokens.
        other.emplaceRaw();
        other.repair_unpacks = 0;
    }

    AggregateFunctionTimeseriesSamples & operator=(AggregateFunctionTimeseriesSamples && other) noexcept
    {
        if (this == &other)
            return *this;
        if (other.storage_type == StorageType::Packed)
            emplacePacked(std::move(other.packed));
        else
            emplaceRaw(std::move(other.raw));
        repair_unpacks = other.repair_unpacks;
        other.emplaceRaw();
        other.repair_unpacks = 0;
        return *this;
    }

    ~AggregateFunctionTimeseriesSamples()
    {
        destroyRepresentation();
    }

    void add(TimestampType timestamp, ValueType value)
    {
        if (storage_type == StorageType::Packed)
        {
            if (packed.add(timestamp, value))
                return;
            /// A modification inside the packed range: unpack and replay the proven raw-path logic on the reconstructed vector.
            unpackToRaw();
            raw.add(timestamp, value);
            return;
        }
        raw.add(timestamp, value);
        /// A still-growing raw bucket packs itself at `SELF_PACK_THRESHOLD`, which bounds the raw footprint of grids whose write cursor never leaves the bucket (e.g. a single-point grid holding the whole series); later in-order samples append to the blob in O(1).
        /// Not after a repair-unpack: an out-of-order source would immediately unpack again, wasting the encode.
        if (repair_unpacks == 0 && raw.isSorted() && raw.size() >= SELF_PACK_THRESHOLD) [[unlikely]]
            packRaw();
    }

    /// Called by the aggregate function when its in-order write cursor moves to another bucket: the bucket is likely done growing, so trade its raw vector for the packed blob.
    void seal()
    {
        if (storage_type == StorageType::Packed || repair_unpacks >= MAX_REPAIR_UNPACKS)
            return;
        raw.normalize();
        if (raw.size() >= PACK_MIN_SAMPLES)
            packRaw();
    }

    /// Whether a `seal()` call would do anything, so the caller can skip re-locating the bucket when it would not (the common case for small buckets).
    bool worthSealing() const
    {
        return storage_type == StorageType::Raw && repair_unpacks < MAX_REPAIR_UNPACKS && raw.size() >= PACK_MIN_SAMPLES;
    }

    void merge(const AggregateFunctionTimeseriesSamples & other)
    {
        if (other.empty())
            return;

        if (empty())
        {
            copyFrom(other);
            return;
        }

        if (storage_type == StorageType::Raw)
            raw.normalize();

        /// Both raw: the pre-packing merge (a bulk append for disjoint ranges, a linear merge otherwise) and the result stays raw - packing here would tax the in-memory partial-state merges whose peak memory is set by the raw partials anyway.
        if (storage_type == StorageType::Raw && other.storage_type == StorageType::Raw)
        {
            raw.merge(other.raw);
            return;
        }

        /// Both packed with `other` entirely later: extend our blob from a decoding cursor over `other`, without unpacking either side - the common case when partial states cover disjoint time ranges.
        if (storage_type == StorageType::Packed && other.storage_type == StorageType::Packed
            && packed.lastTimestamp() < other.packed.firstTimestamp())
        {
            packed.append(other.packed);
            return;
        }

        /// General case, exactly one side raw (`this` is normalized above, a rare unsorted `other` is normalized into `scratch` by its cursor): merge the two sorted sample streams into a new raw buffer.
        Buffer scratch;
        const size_t reserve_hint = sampleCount() + other.sampleCount();
        Buffer merged;
        if (storage_type == StorageType::Packed && other.storage_type == StorageType::Packed)
            merged = Representation::mergeSortedStreams(packed.cursor(), other.packed.cursor(), reserve_hint);
        else if (storage_type == StorageType::Packed)
            merged = Representation::mergeSortedStreams(packed.cursor(), other.raw.cursor(scratch), reserve_hint);
        else
            merged = Representation::mergeSortedStreams(raw.cursor(scratch), other.packed.cursor(), reserve_hint);

        /// The cursors read from the current representation, so it is replaced only after the merge consumed them.
        emplaceRaw(RawRepresentation(std::move(merged)));

        /// A packed side must not inflate back to raw for good: the merged bucket is long-lived, so re-pack it.
        if (repair_unpacks < MAX_REPAIR_UNPACKS && raw.size() >= PACK_MIN_SAMPLES)
            packRaw();
    }

    void serialize(WriteBuffer & buf) const
    {
        /// The wire format is the plain (count + sorted pairs) one regardless of the in-memory representation, so peers running older versions interoperate transparently.
        if (storage_type == StorageType::Packed)
            packed.serialize(buf);
        else
            raw.serialize(buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        /// Deserialize replaces any previous contents.
        repair_unpacks = 0;
        size_t sample_count = 0;
        readBinaryLittleEndian(sample_count, buf);
        /// Large states are read straight into the packed representation, so states read back from disk or the network (e.g. an `AggregatingMergeTree` or a distributed query) do not re-inflate to the raw representation.
        /// The count on the wire may still overcount duplicates, in which case a rare bucket slightly under `PACK_MIN_SAMPLES` gets packed, which is harmless.
        if (sample_count >= PACK_MIN_SAMPLES)
            emplacePacked().deserialize(buf, sample_count);
        else
            emplaceRaw().deserialize(buf, sample_count);
    }

    /// Throws if any sample's timestamp is outside the range.
    template <typename RangeType>
    void checkTimestampsInRange(const RangeType & range) const
    {
        if (storage_type == StorageType::Packed)
            packed.checkTimestampsInRange(range);
        else
            raw.checkTimestampsInRange(range);
    }

    /// Invokes `f(timestamp, value)` for every sample, in ascending timestamp order with duplicates collapsed.
    template <typename F>
    void forEachSample(F && f) const
    {
        if (storage_type == StorageType::Packed)
            packed.forEachSample(std::forward<F>(f));
        else
            raw.forEachSample(std::forward<F>(f));
    }

private:
    using Sample = std::pair<TimestampType, ValueType>;
    /// `VectorWithMemoryTracking` counts the samples' memory in the `MemoryTracker`, like the rest of the aggregate state.
    using Buffer = VectorWithMemoryTracking<Sample>;
    using Blob = VectorWithMemoryTracking<UInt8>;

    /// Do not pack buckets smaller than this: below it the blob saves little over the 16-byte raw samples, while every read of a packed bucket pays the cursor decode.
    static constexpr size_t PACK_MIN_SAMPLES = 12;
    /// A still-growing raw bucket packs itself at this size (see `add`).
    static constexpr size_t SELF_PACK_THRESHOLD = 1024;
    /// After this many unpack-repairs the bucket stays raw for good (see the class comment).
    static constexpr UInt8 MAX_REPAIR_UNPACKS = 3;

    /// Which subclass of `Representation` occupies the union.
    enum class StorageType : UInt8
    {
        Raw,
        Packed,
    };

    /// What the two representations share: the sample semantics (the total-order dedup rule), the bit-pattern helpers and the wire reading.
    /// Stateless by design - the subclasses hold the actual storage.
    class Representation
    {
    public:
        /// The unsigned integer type with the value's bit pattern (`ValueType` is `Float64` or `Float32`), used for the exact bit-equality checks the packed codec relies on.
        using ValueBits = std::conditional_t<sizeof(ValueType) == 8, UInt64, UInt32>;
        static_assert(sizeof(ValueBits) == sizeof(ValueType));

        /// Timestamps are encoded via their 64-bit pattern; all delta arithmetic is wraparound `UInt64`, which is a bijection, so any timestamp sequence round-trips exactly.
        static UInt64 timestampBits(TimestampType timestamp)
        {
            return static_cast<UInt64>(static_cast<Int64>(timestamp));
        }

        static TimestampType timestampFromBits(UInt64 bits)
        {
            return static_cast<TimestampType>(static_cast<Int64>(bits));
        }

        static ValueBits valueBits(ValueType value)
        {
            return std::bit_cast<ValueBits>(value);
        }

        /// Maps a value's bits to a key whose unsigned comparison implements the IEEE-754 totalOrder predicate: negative bit patterns (sign bit set) are flipped entirely, non-negative ones just get the sign bit set.
        static ValueBits totalOrderKey(ValueType value)
        {
            const ValueBits bits = valueBits(value);
            constexpr ValueBits sign_bit = static_cast<ValueBits>(1) << (sizeof(ValueBits) * 8 - 1);
            return (bits & sign_bit) ? static_cast<ValueBits>(~bits) : static_cast<ValueBits>(bits | sign_bit);
        }

        /// The equal-timestamp survivor: the larger value under the IEEE-754 total order (-0.0 < +0.0, NaNs ordered by sign bit), which unlike `std::max` is commutative and associative, so the survivor never depends on arrival order, sort stability, or how many stages collapse the run.
        static ValueType maxValue(ValueType lhs, ValueType rhs)
        {
            return totalOrderKey(lhs) < totalOrderKey(rhs) ? rhs : lhs;
        }

        /// Sorts by timestamp, then collapses each equal-timestamp run into one sample keeping the larger value; the stable sort keeps equal-timestamp runs in arrival order, though `maxValue` is commutative and associative, so the survivor would not depend on that order anyway.
        static void sortAndDeduplicate(Buffer & buf)
        {
            /// The comparator looks at timestamps only: comparing whole pairs would compare values, and `ValueType` can hold NaNs, which break the strict weak ordering the sort requires.
            ::stableSort(buf.begin(), buf.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

            size_t last_unique = 0;
            for (size_t i = 1; i < buf.size(); ++i)
            {
                if (buf[i].first == buf[last_unique].first)
                    buf[last_unique].second = maxValue(buf[last_unique].second, buf[i].second);
                else
                    buf[++last_unique] = buf[i];
            }
            if (!buf.empty())
                buf.resize(last_unique + 1);
        }

        /// Replaces `out` with `sample_count` wire samples, sorted and deduplicated.
        /// No order is assumed on the wire (older peers serialize hash-map iteration order): disorder is detected while reading and repaired at the end if it occurred.
        static void readSamplesFromWire(ReadBuffer & buf, size_t sample_count, Buffer & out)
        {
            out.clear();
            out.reserve(sample_count);
            bool sorted = true;
            for (size_t s = 0; s < sample_count; ++s)
            {
                TimestampType timestamp;
                readBinaryLittleEndian(timestamp, buf);
                ValueType value;
                readBinaryLittleEndian(value, buf);
                if (!out.empty() && timestamp <= out.back().first) [[unlikely]]
                {
                    auto & last = out.back();
                    if (timestamp == last.first)
                    {
                        last.second = maxValue(last.second, value);
                        continue;
                    }
                    sorted = false;
                }
                out.emplace_back(timestamp, value);
            }
            if (!sorted)
                sortAndDeduplicate(out);
        }

        /// Linear merge of two sorted sample streams into a new buffer; equal timestamps collapse into one sample keeping the larger value.
        /// Each representation exposes an interface-compatible `Cursor` (`valid`/`timestamp`/`value`/`advance`), so every combination of the two compiles into its own loop with no per-sample dispatch.
        template <typename LhsCursor, typename RhsCursor>
        static Buffer mergeSortedStreams(LhsCursor lhs, RhsCursor rhs, size_t reserve_hint)
        {
            Buffer merged;
            merged.reserve(reserve_hint);
            while (lhs.valid() && rhs.valid())
            {
                if (lhs.timestamp() < rhs.timestamp())
                {
                    merged.emplace_back(lhs.timestamp(), lhs.value());
                    lhs.advance();
                }
                else if (rhs.timestamp() < lhs.timestamp())
                {
                    merged.emplace_back(rhs.timestamp(), rhs.value());
                    rhs.advance();
                }
                else
                {
                    merged.emplace_back(lhs.timestamp(), maxValue(lhs.value(), rhs.value()));
                    lhs.advance();
                    rhs.advance();
                }
            }
            for (; lhs.valid(); lhs.advance())
                merged.emplace_back(lhs.timestamp(), lhs.value());
            for (; rhs.valid(); rhs.advance())
                merged.emplace_back(rhs.timestamp(), rhs.value());
            return merged;
        }
    };

    /// The raw representation: a vector of (timestamp, value) pairs, append-only on the common in-order stream, sorted and deduplicated whenever `sorted` is set.
    class RawRepresentation : public Representation
    {
    public:
        RawRepresentation() = default;

        /// Adopts an already sorted and deduplicated sample vector (a decoded blob or a finished merge).
        explicit RawRepresentation(Buffer && samples)
            : buffer(std::move(samples))
        {
        }

        void add(TimestampType timestamp, ValueType value)
        {
            /// Out-of-order and duplicate timestamps are rare (measured ~1 per 1.5 billion adds on production-shaped multithreaded reads), hence `[[unlikely]]`.
            if (!buffer.empty() && timestamp <= buffer.back().first) [[unlikely]]
            {
                auto & last = buffer.back();
                if (timestamp == last.first)
                {
                    last.second = Representation::maxValue(last.second, value);
                    return;
                }
                sorted = false;
            }
            buffer.emplace_back(timestamp, value);
        }

        /// A read position over the samples in ascending timestamp order; interface-compatible with `PackedRepresentation::Cursor`.
        struct Cursor
        {
            const Sample * position = nullptr;
            const Sample * end = nullptr;

            bool valid() const { return position != end; }
            TimestampType timestamp() const { return position->first; }
            ValueType value() const { return position->second; }
            void advance() { ++position; }
        };

        /// A cursor over the samples; the rare unsorted state is normalized into the caller-provided `scratch`, which must then outlive the cursor.
        Cursor cursor(Buffer & scratch) const
        {
            const Buffer * source = &buffer;
            if (!sorted) [[unlikely]]
            {
                scratch = buffer;
                Representation::sortAndDeduplicate(scratch);
                source = &scratch;
            }
            return {source->data(), source->data() + source->size()};
        }

        /// Merges another raw representation into this one: a bulk append when the ranges are disjoint, a linear merge otherwise; `this` must be normalized and non-empty, the result stays raw.
        void merge(const RawRepresentation & other)
        {
            chassert(sorted && !buffer.empty() && !other.buffer.empty());

            /// A rare unsorted argument is normalized into a scratch copy by its cursor: `other` belongs to another state and is kept intact.
            Buffer scratch;
            const Cursor rhs = other.cursor(scratch);

            /// Partial states often cover disjoint timestamp ranges - then the merge is a plain append.
            if (buffer.back().first < rhs.timestamp())
            {
                buffer.insert(buffer.end(), rhs.position, rhs.end);
                return;
            }

            buffer = Representation::mergeSortedStreams(
                Cursor{buffer.data(), buffer.data() + buffer.size()}, rhs, buffer.size() + (rhs.end - rhs.position));
        }

        void serialize(WriteBuffer & buf) const
        {
            /// A rare unsorted state is serialized from a normalized copy, so the state is not mutated behind `const`.
            if (!sorted) [[unlikely]]
            {
                Buffer normalized = buffer;
                Representation::sortAndDeduplicate(normalized);
                writeSamples(normalized, buf);
                return;
            }
            writeSamples(buffer, buf);
        }

        /// Replaces the contents with `sample_count` samples read from the wire (the count itself was consumed by the caller to pick the representation).
        void deserialize(ReadBuffer & buf, size_t sample_count)
        {
            Representation::readSamplesFromWire(buf, sample_count, buffer);
            sorted = true;
        }

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

        template <typename F>
        void forEachSample(F && f) const
        {
            /// A rare unsorted state is iterated via a normalized copy, so the state is not mutated behind `const`.
            if (!sorted) [[unlikely]]
            {
                Buffer normalized = buffer;
                Representation::sortAndDeduplicate(normalized);
                for (const auto & [timestamp, value] : normalized)
                    f(timestamp, value);
                return;
            }
            for (const auto & [timestamp, value] : buffer)
                f(timestamp, value);
        }

        /// Restores the invariant in place after out-of-order `add`s; no-op in the common (already sorted) case.
        void normalize()
        {
            if (sorted)
                return;
            Representation::sortAndDeduplicate(buffer);
            sorted = true;
        }

        const Buffer & samples() const { return buffer; }
        bool isSorted() const { return sorted; }
        bool empty() const { return buffer.empty(); }

        /// Number of stored samples; while not normalized this may still count duplicates, which is fine for its only use as a `reserve` hint and packing thresholds.
        size_t size() const { return buffer.size(); }

    private:
        static void writeSamples(const Buffer & samples, WriteBuffer & buf)
        {
            writeBinaryLittleEndian(samples.size(), buf);
            for (const auto & [timestamp, value] : samples)
            {
                writeBinaryLittleEndian(timestamp, buf);
                writeBinaryLittleEndian(value, buf);
            }
        }

        Buffer buffer;
        /// Cleared by an out-of-order `add`; while set, timestamps in `buffer` are strictly increasing.
        bool sorted = true;
    };

    /// The packed representation: the first sample verbatim plus one variable-length token pair per further sample (a zigzag-varint delta-of-delta of the timestamp followed by a value token).
    class PackedRepresentation : public Representation
    {
    public:
        using typename Representation::ValueBits;

        PackedRepresentation() = default;

        /// Encodes a sorted, deduplicated, non-empty sample vector; `shrink_to_fit` drops the growth slop so the blob's allocation is exact.
        explicit PackedRepresentation(const Buffer & samples)
        {
            chassert(!samples.empty());
            count = samples.size();
            first_timestamp_bits = Representation::timestampBits(samples.front().first);
            first_value = samples.front().second;
            tokens.reserve(samples.size() * 2);
            UInt64 previous_timestamp_bits = first_timestamp_bits;
            UInt64 previous_delta = 0;
            ValueType previous_value = first_value;
            for (size_t i = 1; i < samples.size(); ++i)
            {
                const UInt64 current_timestamp_bits = Representation::timestampBits(samples[i].first);
                const UInt64 delta = current_timestamp_bits - previous_timestamp_bits;
                appendVarint(tokens, zigzagEncode(delta - previous_delta));
                appendValueToken(tokens, samples[i].second, previous_value);
                previous_timestamp_bits = current_timestamp_bits;
                previous_delta = delta;
                previous_value = samples[i].second;
            }
            last_timestamp_bits = previous_timestamp_bits;
            last_delta = previous_delta;
            last_value = previous_value;
            tokens.shrink_to_fit();
        }

        /// A decoding position inside the blob, holding the current sample and the delta chain state; interface-compatible with `RawRepresentation::Cursor`.
        struct Cursor
        {
            const UInt8 * position = nullptr;
            UInt64 remaining_tokens = 0;
            UInt64 timestamp_bits = 0;
            UInt64 delta = 0;
            ValueType current_value{};
            bool has_sample = false;

            bool valid() const { return has_sample; }
            TimestampType timestamp() const { return Representation::timestampFromBits(timestamp_bits); }
            ValueType value() const { return current_value; }

            void advance()
            {
                if (remaining_tokens == 0)
                {
                    has_sample = false;
                    return;
                }
                --remaining_tokens;
                delta += zigzagDecode(readVarint(position));
                timestamp_bits += delta;
                current_value = readValueToken(position, current_value);
            }
        };

        Cursor cursor() const
        {
            Cursor result;
            result.position = tokens.data();
            result.remaining_tokens = count ? count - 1 : 0;
            result.timestamp_bits = first_timestamp_bits;
            result.current_value = first_value;
            result.has_sample = count != 0;
            return result;
        }

        /// Handles the O(1) cases: a timestamp past the packed range extends the blob, and a duplicate of the last timestamp whose value loses the dedup (the same `maxValue` rule as the raw path, checked bit-for-bit) changes nothing.
        /// Returns false for anything else - a modification inside the packed range, which the caller repairs by unpacking and replaying the add on the raw representation.
        bool add(TimestampType timestamp, ValueType value)
        {
            const TimestampType last_timestamp = Representation::timestampFromBits(last_timestamp_bits);
            if (timestamp > last_timestamp)
            {
                appendTail(timestamp, value);
                return true;
            }
            return timestamp == last_timestamp
                && Representation::valueBits(Representation::maxValue(last_value, value)) == Representation::valueBits(last_value);
        }

        /// Appends every sample of `other`, which must lie entirely past this blob's last timestamp, without unpacking either side.
        void append(const PackedRepresentation & other)
        {
            chassert(Representation::timestampFromBits(last_timestamp_bits) < Representation::timestampFromBits(other.first_timestamp_bits));
            for (Cursor other_cursor = other.cursor(); other_cursor.valid(); other_cursor.advance())
                appendTail(other_cursor.timestamp(), other_cursor.value());
        }

        void serialize(WriteBuffer & buf) const
        {
            writeBinaryLittleEndian(static_cast<size_t>(count), buf);
            for (Cursor read_cursor = cursor(); read_cursor.valid(); read_cursor.advance())
            {
                writeBinaryLittleEndian(read_cursor.timestamp(), buf);
                writeBinaryLittleEndian(read_cursor.value(), buf);
            }
        }

        /// Replaces the contents with `sample_count` samples read from the wire (the count itself was consumed by the caller to pick the representation), normalized and encoded.
        void deserialize(ReadBuffer & buf, size_t sample_count)
        {
            Buffer samples;
            Representation::readSamplesFromWire(buf, sample_count, samples);
            *this = PackedRepresentation(samples);
        }

        /// Decodes the blob back into a sorted, deduplicated sample vector.
        Buffer unpack() const
        {
            Buffer samples;
            samples.reserve(count);
            for (Cursor read_cursor = cursor(); read_cursor.valid(); read_cursor.advance())
                samples.emplace_back(read_cursor.timestamp(), read_cursor.value());
            return samples;
        }

        template <typename RangeType>
        void checkTimestampsInRange(const RangeType & range) const
        {
            /// Packed timestamps are strictly ascending, so the two extreme samples being in range implies every sample is.
            for (const UInt64 extreme_bits : {first_timestamp_bits, last_timestamp_bits})
                if (!range.contains(Representation::timestampFromBits(extreme_bits)))
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "Cannot deserialize data: timestamp {} is outside its bucket's range",
                        static_cast<Int64>(extreme_bits));
        }

        template <typename F>
        void forEachSample(F && f) const
        {
            for (Cursor read_cursor = cursor(); read_cursor.valid(); read_cursor.advance())
                f(read_cursor.timestamp(), read_cursor.value());
        }

        bool empty() const { return count == 0; }
        size_t sampleCount() const { return count; }
        TimestampType firstTimestamp() const { return Representation::timestampFromBits(first_timestamp_bits); }
        TimestampType lastTimestamp() const { return Representation::timestampFromBits(last_timestamp_bits); }

    private:
        /// Value tokens: `SAME` repeats the previous value bit-for-bit, small integer deltas fit the token byte itself (or the token byte plus one, covering the typical counter increment), larger ones follow as a zigzag varint, and everything else (verified at encode time) is stored verbatim.
        static constexpr UInt8 VALUE_TOKEN_SAME = 0x00;
        static constexpr UInt8 VALUE_TOKEN_SMALL_BASE = 0x01;
        static constexpr UInt8 VALUE_TOKEN_SMALL_MAX = 0xE7;
        static constexpr UInt8 VALUE_TOKEN_MEDIUM_BASE = 0xE8;
        static constexpr UInt8 VALUE_TOKEN_MEDIUM_MAX = 0xF7;
        static constexpr UInt8 VALUE_TOKEN_INT_DELTA = 0xF8;
        static constexpr UInt8 VALUE_TOKEN_RAW = 0xF9;
        /// Zigzag deltas up to this fit the single token byte.
        static constexpr UInt64 VALUE_SMALL_LIMIT = VALUE_TOKEN_SMALL_MAX - VALUE_TOKEN_SMALL_BASE;
        /// Zigzag deltas up to this fit two bytes: 4 bits of the token byte plus one payload byte, biased past the single-byte range.
        static constexpr UInt64 VALUE_MEDIUM_LIMIT = VALUE_SMALL_LIMIT + ((static_cast<UInt64>(VALUE_TOKEN_MEDIUM_MAX - VALUE_TOKEN_MEDIUM_BASE) + 1) << 8);

        /// Bounds within which casting a `ValueType` to `Int64` is defined; slightly inside +-2^63 so the truncated value always fits.
        static constexpr ValueType MIN_CASTABLE_TO_INT64 = static_cast<ValueType>(-9.2e18);
        static constexpr ValueType MAX_CASTABLE_TO_INT64 = static_cast<ValueType>(9.2e18);

        static UInt64 zigzagEncode(UInt64 bits)
        {
            const Int64 signed_bits = static_cast<Int64>(bits);
            return (static_cast<UInt64>(signed_bits) << 1) ^ static_cast<UInt64>(signed_bits >> 63);
        }

        static UInt64 zigzagDecode(UInt64 encoded)
        {
            return (encoded >> 1) ^ (0 - (encoded & 1));
        }

        static void appendVarint(Blob & out, UInt64 value)
        {
            while (value >= 0x80)
            {
                out.push_back(static_cast<UInt8>(value) | 0x80);
                value >>= 7;
            }
            out.push_back(static_cast<UInt8>(value));
        }

        static UInt64 readVarint(const UInt8 *& position)
        {
            UInt64 result = 0;
            UInt8 shift = 0;
            UInt8 byte = 0;
            do
            {
                chassert(shift < 64);
                byte = *position++;
                result |= static_cast<UInt64>(byte & 0x7F) << shift;
                shift += 7;
            } while (byte & 0x80);
            return result;
        }

        /// Encodes `value` relative to `previous_value`; a compact token is used only if decoding it reproduces the exact bit pattern of `value`, otherwise the bits are stored verbatim - the codec is lossless by construction, not by reasoning about floating-point.
        static void appendValueToken(Blob & out, ValueType value, ValueType previous_value)
        {
            if (Representation::valueBits(value) == Representation::valueBits(previous_value))
            {
                out.push_back(VALUE_TOKEN_SAME);
                return;
            }

            const ValueType difference = value - previous_value;
            if (difference >= MIN_CASTABLE_TO_INT64 && difference <= MAX_CASTABLE_TO_INT64)
            {
                const Int64 int_difference = static_cast<Int64>(difference);
                /// The exact expression the decoder computes: token usable only if it reproduces the value bit-for-bit.
                if (Representation::valueBits(previous_value + static_cast<ValueType>(int_difference)) == Representation::valueBits(value))
                {
                    const UInt64 encoded = zigzagEncode(static_cast<UInt64>(int_difference));
                    if (encoded <= VALUE_SMALL_LIMIT)
                    {
                        out.push_back(static_cast<UInt8>(VALUE_TOKEN_SMALL_BASE + encoded));
                        return;
                    }
                    if (encoded <= VALUE_MEDIUM_LIMIT)
                    {
                        const UInt64 biased = encoded - VALUE_SMALL_LIMIT - 1;
                        out.push_back(static_cast<UInt8>(VALUE_TOKEN_MEDIUM_BASE + (biased >> 8)));
                        out.push_back(static_cast<UInt8>(biased));
                        return;
                    }
                    out.push_back(VALUE_TOKEN_INT_DELTA);
                    appendVarint(out, encoded);
                    return;
                }
            }

            out.push_back(VALUE_TOKEN_RAW);
            ValueBits bits = Representation::valueBits(value);
            for (size_t i = 0; i < sizeof(ValueBits); ++i)
            {
                out.push_back(static_cast<UInt8>(bits));
                bits >>= 8;
            }
        }

        static ValueType readValueToken(const UInt8 *& position, ValueType previous_value)
        {
            const UInt8 token = *position++;
            if (token == VALUE_TOKEN_SAME)
                return previous_value;
            if (token <= VALUE_TOKEN_SMALL_MAX)
                return previous_value + static_cast<ValueType>(static_cast<Int64>(zigzagDecode(token - VALUE_TOKEN_SMALL_BASE)));
            if (token <= VALUE_TOKEN_MEDIUM_MAX)
            {
                const UInt64 biased = (static_cast<UInt64>(token - VALUE_TOKEN_MEDIUM_BASE) << 8) | *position++;
                return previous_value + static_cast<ValueType>(static_cast<Int64>(zigzagDecode(biased + VALUE_SMALL_LIMIT + 1)));
            }
            if (token == VALUE_TOKEN_INT_DELTA)
                return previous_value + static_cast<ValueType>(static_cast<Int64>(zigzagDecode(readVarint(position))));
            chassert(token == VALUE_TOKEN_RAW);
            ValueBits bits = 0;
            for (size_t i = 0; i < sizeof(ValueBits); ++i)
                bits |= static_cast<ValueBits>(*position++) << (i * 8);
            return std::bit_cast<ValueType>(bits);
        }

        /// Appends one sample past the packed range: encodes the delta-of-delta and value tokens and advances the blob's tail state in O(1).
        void appendTail(TimestampType timestamp, ValueType value)
        {
            const UInt64 new_timestamp_bits = Representation::timestampBits(timestamp);
            const UInt64 delta = new_timestamp_bits - last_timestamp_bits;
            appendVarint(tokens, zigzagEncode(delta - last_delta));
            appendValueToken(tokens, value, last_value);
            last_timestamp_bits = new_timestamp_bits;
            last_delta = delta;
            last_value = value;
            ++count;
        }

        /// Encoded samples 2..count: a zigzag-varint delta-of-delta of the timestamp followed by a value token.
        Blob tokens;
        UInt64 first_timestamp_bits = 0;
        UInt64 last_timestamp_bits = 0;
        /// Timestamp delta between the last two samples (0 when count == 1), the seed of the delta-of-delta chain for O(1) appends.
        UInt64 last_delta = 0;
        ValueType first_value{};
        ValueType last_value{};
        UInt64 count = 0;
    };

    /// Destroys the active union member; an `emplace*` must follow before the state is used again.
    void destroyRepresentation() noexcept
    {
        if (storage_type == StorageType::Packed)
            packed.~PackedRepresentation();
        else
            raw.~RawRepresentation();
    }

    /// The representation switches construct the replacement before destroying the current member (the replacement is built from it), then move it into the union, which is noexcept, so a mid-switch exception never leaves the union half-formed.
    RawRepresentation & emplaceRaw(RawRepresentation && value = RawRepresentation{})
    {
        destroyRepresentation();
        new (&raw) RawRepresentation(std::move(value));
        storage_type = StorageType::Raw;
        return raw;
    }

    PackedRepresentation & emplacePacked(PackedRepresentation && value = PackedRepresentation{})
    {
        destroyRepresentation();
        new (&packed) PackedRepresentation(std::move(value));
        storage_type = StorageType::Packed;
        return packed;
    }

    /// Trades the raw vector for the packed blob; `raw` must be normalized and non-empty.
    void packRaw()
    {
        chassert(storage_type == StorageType::Raw && raw.isSorted() && !raw.empty());
        emplacePacked(PackedRepresentation(raw.samples()));
    }

    /// Trades the packed blob for the decoded raw vector, counting the repair towards `MAX_REPAIR_UNPACKS`.
    void unpackToRaw()
    {
        chassert(storage_type == StorageType::Packed);
        emplaceRaw(RawRepresentation(packed.unpack()));
        if (repair_unpacks < MAX_REPAIR_UNPACKS)
            ++repair_unpacks;
    }

    /// Replaces the empty `this` with a copy of `other`, normalized (and packed when large enough), mirroring the lhs-empty merge path.
    void copyFrom(const AggregateFunctionTimeseriesSamples & other)
    {
        chassert(empty());
        if (other.storage_type == StorageType::Packed)
        {
            emplacePacked(PackedRepresentation(other.packed));
            return;
        }
        emplaceRaw(RawRepresentation(other.raw));
        raw.normalize();
        if (repair_unpacks < MAX_REPAIR_UNPACKS && raw.size() >= PACK_MIN_SAMPLES)
            packRaw();
    }

    bool empty() const
    {
        return storage_type == StorageType::Packed ? packed.empty() : raw.empty();
    }

    /// Number of stored samples; for a not-yet-normalized raw buffer this may still count duplicates, which is fine for its only use as a `reserve` hint.
    size_t sampleCount() const
    {
        return storage_type == StorageType::Packed ? packed.sampleCount() : raw.size();
    }

    /// The active representation, selected by `storage_type`; the transitions run through `emplaceRaw`/`emplacePacked`.
    union
    {
        RawRepresentation raw;
        PackedRepresentation packed;
    };
    StorageType storage_type = StorageType::Raw;
    /// Saturating count of unpack-repairs; at `MAX_REPAIR_UNPACKS` the bucket stops packing itself.
    UInt8 repair_unpacks = 0;
};

}
