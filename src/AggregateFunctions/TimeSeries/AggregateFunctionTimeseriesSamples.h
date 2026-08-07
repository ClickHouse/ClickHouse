#pragma once

#include <algorithm>
#include <bit>
#include <memory>
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

/// Per-bucket storage of timeseries samples sorted by timestamp with duplicates keeping the larger value under the IEEE-754 total order (see `maxValue`).
/// Two representations: a raw sorted-append vector while the bucket receives its in-order stream, and a compact packed blob (varint delta-of-delta timestamps, integer-delta or verbatim values) once sealed by the write cursor leaving the bucket or by growing past `SELF_PACK_THRESHOLD`.
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
///    the write cursor leaves it. `MAX_REPAIR_UNPACKS` bounds the repacking work: a bucket that keeps attracting
///    such repairs stays raw (degrading memory to the old raw behavior instead of burning CPU on repacking).
template <typename TimestampType, typename ValueType>
class AggregateFunctionTimeseriesSamples
{
public:
    /// The bucket map (`HashMap`) relocates cells with `memcpy` and abandons the source, which is safe here: the vector and the `Packed` pointer hold no pointers into the cell itself.
    static constexpr bool is_position_independent = true;

    void add(TimestampType timestamp, ValueType value)
    {
        if (packed)
        {
            addToPacked(timestamp, value);
            return;
        }
        addToRaw(timestamp, value);
    }

    /// Called by the aggregate function when its in-order write cursor moves to another bucket: the bucket is likely done growing, so trade its raw vector for the packed blob.
    void seal()
    {
        if (packed || repair_unpacks >= MAX_REPAIR_UNPACKS)
            return;
        normalize();
        if (buffer.size() >= PACK_MIN_SAMPLES)
            pack();
    }

    /// Whether a `seal()` call would do anything, so the caller can skip re-locating the bucket when it would not (the common case for small buckets).
    bool worthSealing() const
    {
        return !packed && repair_unpacks < MAX_REPAIR_UNPACKS && buffer.size() >= PACK_MIN_SAMPLES;
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

        normalize();

        /// Both raw: the pre-packing merge (a bulk append for disjoint ranges, a linear merge otherwise) and the result stays raw - packing here would tax the in-memory partial-state merges whose peak memory is set by the raw partials anyway.
        if (!packed && !other.packed)
        {
            mergeRawWithRaw(other);
            return;
        }

        /// Both packed with `other` entirely later: extend our blob from a decoding cursor over `other`, without unpacking either side - the common case when partial states cover disjoint time ranges.
        if (packed && other.packed && timestampFromBits(packed->last_timestamp_bits) < timestampFromBits(other.packed->first_timestamp_bits))
        {
            for (PackedCursor cursor = packedCursor(*other.packed); cursor.valid; advancePackedCursor(cursor))
                appendToPackedTail(timestampFromBits(cursor.timestamp_bits), cursor.value);
            return;
        }

        /// General case, at least one side packed: linear merge of two sorted sample streams; equal timestamps collapse into one sample keeping the larger value.
        Buffer other_normalized;
        MergeCursor rhs;
        if (other.packed)
        {
            rhs.from_packed = true;
            rhs.packed_cursor = packedCursor(*other.packed);
        }
        else if (!other.sorted)
        {
            /// A rare unsorted argument is normalized into a copy: `other` belongs to another state and is kept intact.
            other_normalized = other.buffer;
            sortAndDeduplicate(other_normalized);
            rhs.raw_position = other_normalized.data();
            rhs.raw_end = other_normalized.data() + other_normalized.size();
        }
        else
        {
            rhs.raw_position = other.buffer.data();
            rhs.raw_end = other.buffer.data() + other.buffer.size();
        }

        MergeCursor lhs;
        if (packed)
        {
            lhs.from_packed = true;
            lhs.packed_cursor = packedCursor(*packed);
        }
        else
        {
            lhs.raw_position = buffer.data();
            lhs.raw_end = buffer.data() + buffer.size();
        }

        Buffer merged;
        merged.reserve(sampleCount() + other.sampleCount());
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

        packed.reset();
        buffer = std::move(merged);
        sorted = true;

        /// A packed side must not inflate back to raw for good: the merged bucket is long-lived, so re-pack it.
        if (repair_unpacks < MAX_REPAIR_UNPACKS && buffer.size() >= PACK_MIN_SAMPLES)
            pack();
    }

    void serialize(WriteBuffer & buf) const
    {
        /// The wire format is the plain (count + sorted pairs) one regardless of the in-memory representation, so peers running older versions interoperate transparently.
        if (packed)
        {
            writeBinaryLittleEndian(static_cast<size_t>(packed->count), buf);
            for (PackedCursor cursor = packedCursor(*packed); cursor.valid; advancePackedCursor(cursor))
            {
                writeBinaryLittleEndian(timestampFromBits(cursor.timestamp_bits), buf);
                writeBinaryLittleEndian(cursor.value, buf);
            }
            return;
        }

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
        packed.reset();
        buffer.clear();
        sorted = true;
        repair_unpacks = 0;

        size_t sample_count = 0;
        readBinaryLittleEndian(sample_count, buf);
        buffer.reserve(sample_count);
        /// No order is assumed on the wire (older peers serialize hash-map iteration order): disorder is detected while reading and `normalize` restores the invariant if it was violated.
        for (size_t s = 0; s < sample_count; ++s)
        {
            TimestampType timestamp;
            readBinaryLittleEndian(timestamp, buf);
            ValueType value;
            readBinaryLittleEndian(value, buf);
            /// Raw append without the self-pack trigger: the bucket is packed once, below, after all samples are read and normalized.
            if (!buffer.empty() && timestamp <= buffer.back().first) [[unlikely]]
            {
                auto & last = buffer.back();
                if (timestamp == last.first)
                {
                    last.second = maxValue(last.second, value);
                    continue;
                }
                sorted = false;
            }
            buffer.emplace_back(timestamp, value);
        }
        normalize();
        /// Pack right away so that states read back from disk or the network (e.g. an `AggregatingMergeTree` or a distributed query) do not re-inflate to the raw representation.
        if (buffer.size() >= PACK_MIN_SAMPLES)
            pack();
    }

    /// Throws if any sample's timestamp is outside the range.
    template <typename RangeType>
    void checkTimestampsInRange(const RangeType & range) const
    {
        /// Packed timestamps are strictly ascending, so the two extreme samples being in range implies every sample is.
        if (packed)
        {
            for (const UInt64 extreme_bits : {packed->first_timestamp_bits, packed->last_timestamp_bits})
                if (!range.contains(timestampFromBits(extreme_bits)))
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "Cannot deserialize data: timestamp {} is outside its bucket's range",
                        static_cast<Int64>(extreme_bits));
            return;
        }
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
        if (packed)
        {
            for (PackedCursor cursor = packedCursor(*packed); cursor.valid; advancePackedCursor(cursor))
                f(timestampFromBits(cursor.timestamp_bits), cursor.value);
            return;
        }

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
    using Blob = VectorWithMemoryTracking<UInt8>;
    /// The unsigned integer type with the value's bit pattern (`ValueType` is `Float64` or `Float32`), used for the exact bit-equality checks the codec relies on.
    using ValueBits = std::conditional_t<sizeof(ValueType) == 8, UInt64, UInt32>;
    static_assert(sizeof(ValueBits) == sizeof(ValueType));

    /// Do not pack buckets smaller than this: the `Packed` header plus a separate heap allocation would eat the savings (the break-even against 16-byte raw samples is at 8-10 samples).
    static constexpr size_t PACK_MIN_SAMPLES = 12;
    /// A still-growing raw bucket packs itself at this size, which bounds the raw footprint of grids whose write cursor never leaves the bucket (e.g. a single-point grid holding the whole series); later in-order samples append to the blob in O(1).
    static constexpr size_t SELF_PACK_THRESHOLD = 1024;
    /// After this many unpack-repairs the bucket stays raw: sustained out-of-order traffic into one bucket would otherwise pay a decode+encode cycle per repair, and raw is exactly the pre-packing behavior.
    static constexpr UInt8 MAX_REPAIR_UNPACKS = 3;

    /// The packed representation: the first sample verbatim plus one variable-length token pair per further sample.
    struct Packed
    {
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

    /// Bounds within which casting a `ValueType` to `Int64` is defined; slightly inside +-2^63 so the truncated value always fits.
    static constexpr ValueType MIN_CASTABLE_TO_INT64 = static_cast<ValueType>(-9.2e18);
    static constexpr ValueType MAX_CASTABLE_TO_INT64 = static_cast<ValueType>(9.2e18);

    /// Encodes `value` relative to `previous_value`; a compact token is used only if decoding it reproduces the exact bit pattern of `value`, otherwise the bits are stored verbatim - the codec is lossless by construction, not by reasoning about floating-point.
    static void appendValueToken(Blob & out, ValueType value, ValueType previous_value)
    {
        if (valueBits(value) == valueBits(previous_value))
        {
            out.push_back(VALUE_TOKEN_SAME);
            return;
        }

        const ValueType difference = value - previous_value;
        if (difference >= MIN_CASTABLE_TO_INT64 && difference <= MAX_CASTABLE_TO_INT64)
        {
            const Int64 int_difference = static_cast<Int64>(difference);
            /// The exact expression the decoder computes: token usable only if it reproduces the value bit-for-bit.
            if (valueBits(previous_value + static_cast<ValueType>(int_difference)) == valueBits(value))
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
        ValueBits bits = valueBits(value);
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

    /// A decoding position inside a packed blob; holds the current sample and the delta chain state.
    struct PackedCursor
    {
        const UInt8 * position = nullptr;
        UInt64 remaining_tokens = 0;
        UInt64 timestamp_bits = 0;
        UInt64 delta = 0;
        ValueType value{};
        bool valid = false;
    };

    static PackedCursor packedCursor(const Packed & packed_data)
    {
        PackedCursor cursor;
        cursor.position = packed_data.tokens.data();
        cursor.remaining_tokens = packed_data.count ? packed_data.count - 1 : 0;
        cursor.timestamp_bits = packed_data.first_timestamp_bits;
        cursor.value = packed_data.first_value;
        cursor.valid = packed_data.count != 0;
        return cursor;
    }

    static void advancePackedCursor(PackedCursor & cursor)
    {
        if (cursor.remaining_tokens == 0)
        {
            cursor.valid = false;
            return;
        }
        --cursor.remaining_tokens;
        cursor.delta += zigzagDecode(readVarint(cursor.position));
        cursor.timestamp_bits += cursor.delta;
        cursor.value = readValueToken(cursor.position, cursor.value);
    }

    /// A merge-time read cursor over either representation, yielding samples in ascending timestamp order.
    struct MergeCursor
    {
        PackedCursor packed_cursor;
        const std::pair<TimestampType, ValueType> * raw_position = nullptr;
        const std::pair<TimestampType, ValueType> * raw_end = nullptr;
        bool from_packed = false;

        bool valid() const { return from_packed ? packed_cursor.valid : raw_position != raw_end; }
        TimestampType timestamp() const { return from_packed ? timestampFromBits(packed_cursor.timestamp_bits) : raw_position->first; }
        ValueType value() const { return from_packed ? packed_cursor.value : raw_position->second; }
        void advance()
        {
            if (from_packed)
                advancePackedCursor(packed_cursor);
            else
                ++raw_position;
        }
    };

    void addToRaw(TimestampType timestamp, ValueType value)
    {
        /// Out-of-order and duplicate timestamps are rare (measured ~1 per 1.5 billion adds on production-shaped multithreaded reads), hence `[[unlikely]]`.
        if (!buffer.empty() && timestamp <= buffer.back().first) [[unlikely]]
        {
            auto & last = buffer.back();
            if (timestamp == last.first)
            {
                last.second = maxValue(last.second, value);
                return;
            }
            sorted = false;
        }
        buffer.emplace_back(timestamp, value);
        /// Not after a repair-unpack: an out-of-order source would immediately unpack again, wasting the encode.
        if (sorted && repair_unpacks == 0 && buffer.size() >= SELF_PACK_THRESHOLD) [[unlikely]]
            pack();
    }

    /// The pre-packing merge of two raw sorted buffers: a bulk append when the ranges are disjoint, a linear merge otherwise; `this` must be normalized, the result stays raw.
    void mergeRawWithRaw(const AggregateFunctionTimeseriesSamples & other)
    {
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
                merged.emplace_back(lhs_it->first, maxValue(lhs_it->second, rhs_it->second));
                ++lhs_it;
                ++rhs_it;
            }
        }
        merged.insert(merged.end(), lhs_it, buffer.end());
        merged.insert(merged.end(), rhs_it, rhs->end());
        buffer = std::move(merged);
    }

    /// Appends one sample past the packed range: encodes the delta-of-delta and value tokens and advances the blob's tail state in O(1).
    void appendToPackedTail(TimestampType timestamp, ValueType value)
    {
        Packed & packed_data = *packed;
        const UInt64 new_timestamp_bits = timestampBits(timestamp);
        const UInt64 delta = new_timestamp_bits - packed_data.last_timestamp_bits;
        appendVarint(packed_data.tokens, zigzagEncode(delta - packed_data.last_delta));
        appendValueToken(packed_data.tokens, value, packed_data.last_value);
        packed_data.last_timestamp_bits = new_timestamp_bits;
        packed_data.last_delta = delta;
        packed_data.last_value = value;
        ++packed_data.count;
    }

    void addToPacked(TimestampType timestamp, ValueType value)
    {
        Packed & packed_data = *packed;
        const TimestampType last_timestamp = timestampFromBits(packed_data.last_timestamp_bits);
        /// The common late-add case, a timestamp past the packed range, extends the blob in O(1).
        if (timestamp > last_timestamp)
        {
            appendToPackedTail(timestamp, value);
            return;
        }
        /// A duplicate of the last timestamp whose value loses the dedup (the same `maxValue` rule as the raw path, checked bit-for-bit) changes nothing.
        if (timestamp == last_timestamp && valueBits(maxValue(packed_data.last_value, value)) == valueBits(packed_data.last_value))
            return;
        /// Everything else is a modification inside the packed range: unpack and replay the proven raw-path logic on the reconstructed vector.
        unpackToRaw();
        addToRaw(timestamp, value);
    }

    void unpackToRaw()
    {
        chassert(packed && buffer.empty());
        buffer.reserve(packed->count);
        for (PackedCursor cursor = packedCursor(*packed); cursor.valid; advancePackedCursor(cursor))
            buffer.emplace_back(timestampFromBits(cursor.timestamp_bits), cursor.value);
        packed.reset();
        sorted = true;
        if (repair_unpacks < MAX_REPAIR_UNPACKS)
            ++repair_unpacks;
    }

    /// Encodes the normalized raw vector into a packed blob and frees the vector; `shrink_to_fit` drops the growth slop so the blob's allocation is exact.
    void pack()
    {
        chassert(sorted && !packed && !buffer.empty());
        auto packed_data = std::make_unique<Packed>();
        packed_data->count = buffer.size();
        packed_data->first_timestamp_bits = timestampBits(buffer.front().first);
        packed_data->first_value = buffer.front().second;
        packed_data->tokens.reserve(buffer.size() * 2);
        UInt64 previous_timestamp_bits = packed_data->first_timestamp_bits;
        UInt64 previous_delta = 0;
        ValueType previous_value = packed_data->first_value;
        for (size_t i = 1; i < buffer.size(); ++i)
        {
            const UInt64 current_timestamp_bits = timestampBits(buffer[i].first);
            const UInt64 delta = current_timestamp_bits - previous_timestamp_bits;
            appendVarint(packed_data->tokens, zigzagEncode(delta - previous_delta));
            appendValueToken(packed_data->tokens, buffer[i].second, previous_value);
            previous_timestamp_bits = current_timestamp_bits;
            previous_delta = delta;
            previous_value = buffer[i].second;
        }
        packed_data->last_timestamp_bits = previous_timestamp_bits;
        packed_data->last_delta = previous_delta;
        packed_data->last_value = previous_value;
        packed_data->tokens.shrink_to_fit();
        packed = std::move(packed_data);
        Buffer{}.swap(buffer);
    }

    /// Replaces the empty `this` with a copy of `other`, normalized (and packed when large enough), mirroring the old lhs-empty merge path.
    void copyFrom(const AggregateFunctionTimeseriesSamples & other)
    {
        chassert(empty());
        if (other.packed)
        {
            packed = std::make_unique<Packed>(*other.packed);
            return;
        }
        buffer = other.buffer;
        sorted = true;
        if (!other.sorted)
            sortAndDeduplicate(buffer);
        if (repair_unpacks < MAX_REPAIR_UNPACKS && buffer.size() >= PACK_MIN_SAMPLES)
            pack();
    }

    bool empty() const
    {
        return !packed && buffer.empty();
    }

    /// Number of stored samples; for a not-yet-normalized raw buffer this may still count duplicates, which is fine for its only use as a `reserve` hint.
    size_t sampleCount() const
    {
        return packed ? packed->count : buffer.size();
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

    /// Sorts by timestamp, then collapses each equal-timestamp run into one sample keeping the larger value; `maxValue` is commutative and associative, so the sort's unspecified equal-key order cannot influence the survivor.
    static void sortAndDeduplicate(Buffer & buf)
    {
        /// The comparator looks at timestamps only: comparing whole pairs would compare values, and `ValueType` can hold NaNs, which break the strict weak ordering `::sort` requires.
        ::sort(buf.begin(), buf.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

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

    /// Restores the invariant in place after out-of-order `add`s; no-op in the common (already sorted) case.
    void normalize()
    {
        if (sorted)
            return;
        sortAndDeduplicate(buffer);
        sorted = true;
    }

    /// The raw samples, sorted by timestamp and deduplicated whenever `sorted` is true; empty while `packed` is set.
    Buffer buffer;
    /// The packed representation; the bucket is packed if and only if this is set.
    std::unique_ptr<Packed> packed;
    /// Cleared by an out-of-order `add`; while set, timestamps in `buffer` are strictly increasing.
    bool sorted = true;
    /// Saturating count of unpack-repairs; at `MAX_REPAIR_UNPACKS` the bucket stops packing itself.
    UInt8 repair_unpacks = 0;
};

}
