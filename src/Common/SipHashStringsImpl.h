#pragma once

#include <Common/SipHash.h>
#include <base/types.h>

/// The vector code of `sipHash128PaddedStrings`, for callers that inline it into their own loop over a column. It exists
/// on aarch64 with NEON and on x86 with AVX2. A caller splits the rows into chunks of at most `chunk_rows` rows, gets the
/// buckets of a chunk from `lengthBuckets`, and hashes the chunk with `hashRows`.
/// Strings of one length need none of that: a caller hashes them `group_size` at a time with `hashGroup`.

#if (defined(__aarch64__) && defined(__ARM_NEON)) || defined(__AVX2__)

#    include <base/unaligned.h>

#    include <algorithm>
#    include <array>
#    include <bit>
#    include <cstring>
#    include <limits>
#    include <type_traits>
#    include <utility>

#    if defined(__aarch64__)
#        include <arm_neon.h>
#    endif

namespace SipHashStrings
{

static_assert(std::endian::native == std::endian::little);

/// A chunk has at most `chunk_rows` rows, so that the row numbers and the buffers of `hashRows` fit on the stack.
constexpr size_t chunk_rows = 256;

/// Rows are sorted by their number of 8-byte words: one bucket per word count below `exact_buckets`, then 8 buckets per
/// doubling of the word count, up to `length_buckets - 1`.
constexpr size_t exact_buckets = 64;
constexpr size_t length_buckets = 256;
/// Buckets are stored as `uint8_t`, and `bucketStats` loads them as `UInt8Vec`.
static_assert(length_buckets - 1 <= std::numeric_limits<uint8_t>::max());

/// The initial state of the unkeyed `sipHash128Padded`.
constexpr UInt64 sip_v0 = 0x736f6d6570736575ULL;
constexpr UInt64 sip_v1 = 0x646f72616e646f6dULL;
constexpr UInt64 sip_v2 = 0x6c7967656e657261ULL;
constexpr UInt64 sip_v3 = 0x7465646279746573ULL;

/// A group is `group_vectors` vectors of `lanes` strings, one string per 64-bit lane,
/// plus `group_scalars` strings in general registers.
#    if defined(__AVX2__)
/// An AVX2 register holds 4 strings. No strings go in general registers: on Ice Lake the vector operations already fill
/// ports 0, 1 and 5, and the integer operations need the same ports.
constexpr size_t lanes = 4;
constexpr size_t group_scalars = 0;
#    else
/// A NEON register holds 2 strings. The vector code leaves the integer pipes idle, and 4 scalar states use them.
constexpr size_t lanes = 2;
constexpr size_t group_scalars = 4;
#    endif
/// The operations of one vector form a chain, so two vectors hide each other's latency.
constexpr size_t group_vectors = 2;
constexpr size_t group_size = lanes * group_vectors + group_scalars;

/// Clang vector extensions: an operation on a whole vector compiles to one NEON or AVX2 instruction.
using UInt64Vec = UInt64 __attribute__((vector_size(8 * lanes)));
using Int64Vec = Int64 __attribute__((vector_size(8 * lanes)));
using UInt32Vec = UInt32 __attribute__((vector_size(8 * lanes)));
using UInt8Vec = UInt8 __attribute__((vector_size(8 * lanes)));
using UInt64x2 = UInt64 __attribute__((vector_size(16)));

/// Calls `f(i)` for each `i` in [0, count), with `i` a compile-time constant.
template <size_t count, typename F>
ALWAYS_INLINE void unrolledFor(F && f)
{
    /// With constant indices the arrays of states can stay in registers, and `i` can be a shuffle index.
    [&]<size_t... i>(std::index_sequence<i...>) ALWAYS_INLINE
    { (f(std::integral_constant<size_t, i>{}), ...); }(std::make_index_sequence<count>{});
}

/// Lane `i` = `f(i)`.
template <typename F>
ALWAYS_INLINE UInt64Vec makeLanes(F && f)
{
    return [&]<size_t... i>(std::index_sequence<i...>) ALWAYS_INLINE { return UInt64Vec{f(i)...}; }(std::make_index_sequence<lanes>{});
}

/// Lane `i` = the 8 bytes at `address(i)`.
template <typename F>
ALWAYS_INLINE UInt64Vec loadLanes(F && address)
{
    /// Each lane is loaded as a vector of one lane, and the vectors are joined as `vcombine_u64` joins them. Building the
    /// vector from 64-bit integers would load them into general registers first.
    using UInt64x1 = UInt64 __attribute__((vector_size(8)));
    const auto pair = [&](size_t i) ALWAYS_INLINE
    { return __builtin_shufflevector(unalignedLoad<UInt64x1>(address(i)), unalignedLoad<UInt64x1>(address(i + 1)), 0, 1); };
    static_assert(lanes == 2 || lanes == 4);
    if constexpr (lanes == 2)
        return pair(0);
    else
        return __builtin_shufflevector(pair(0), pair(2), 0, 1, 2, 3);
}

/// Element `i` = element `index(i)` of `x`.
template <auto index, typename Vec>
ALWAYS_INLINE Vec permute(Vec x)
{
    return [&]<size_t... i>(std::index_sequence<i...>) ALWAYS_INLINE
    { return __builtin_shufflevector(x, x, index(i)...); }(std::make_index_sequence<sizeof(Vec) / sizeof(x[0])>{});
}

template <int bits>
ALWAYS_INLINE UInt64Vec rotl(UInt64Vec x)
{
#    if defined(__aarch64__)
    /// SHL and SRI. Clang lowers the portable rotate to SHL and USRA, and USRA makes the chain of a round a cycle longer.
    return std::bit_cast<UInt64Vec>(vsriq_n_u64(std::bit_cast<uint64x2_t>(x << bits), std::bit_cast<uint64x2_t>(x), 64 - bits));
#    else
    return (x << bits) | (x >> (64 - bits));
#    endif
}

inline ALWAYS_INLINE UInt64Vec rotl32(UInt64Vec x)
{
    /// A rotation by 32 swaps the halves of each lane: one REV64 or VPSHUFD.
    return std::bit_cast<UInt64Vec>(permute<[](size_t i) { return i ^ 1; }>(std::bit_cast<UInt32Vec>(x)));
}

inline ALWAYS_INLINE UInt64Vec rotl16(UInt64Vec x)
{
    /// One TBL or VPSHUFB instead of two shifts that depend on each other: byte i of a lane gets byte (i - 2) mod 8.
    return std::bit_cast<UInt64Vec>(permute<[](size_t i) { return (i & ~size_t(7)) | ((i + 6) & 7); }>(std::bit_cast<UInt8Vec>(x)));
}

/// The SipHash states of `lanes` strings, one per lane.
struct LaneStates
{
    UInt64Vec v0;
    UInt64Vec v1;
    UInt64Vec v2;
    UInt64Vec v3;
    const char * data[lanes];
    size_t size[lanes];
    /// The number of full words in the shortest and in the longest string.
    size_t common_words;
    size_t longer_words;

    /// `string_at(i)` gives the data and the size of the string of lane `i`.
    template <typename StringAt>
    ALWAYS_INLINE void init(StringAt && string_at)
    {
        v0 = UInt64Vec{} + sip_v0;
        v1 = UInt64Vec{} + sip_v1;
        v2 = UInt64Vec{} + sip_v2;
        v3 = UInt64Vec{} + sip_v3;
        common_words = std::numeric_limits<size_t>::max();
        longer_words = 0;
        unrolledFor<lanes>(
            [&](auto i) ALWAYS_INLINE
            {
                const auto [string_data, string_size] = string_at(i);
                data[i] = string_data;
                size[i] = string_size;
                common_words = std::min(common_words, size[i] / 8);
                longer_words = std::max(longer_words, size[i] / 8);
            });
    }

    /// `SIPROUND` on all lanes.
    ALWAYS_INLINE void round()
    {
        const UInt64Vec a0 = v0 + v1;
        const UInt64Vec b1 = rotl<13>(v1) ^ a0;
        const UInt64Vec a2 = v2 + v3;
        const UInt64Vec b3 = rotl16(v3) ^ a2;
        v0 = rotl32(a0) + b3;
        v3 = rotl<21>(b3) ^ v0;
        const UInt64Vec d2 = a2 + b1;
        v1 = rotl<17>(b1) ^ d2;
        v2 = rotl32(d2);
    }

    ALWAYS_INLINE void compress(UInt64Vec words)
    {
        v3 ^= words;
        round();
        round();
        v0 ^= words;
    }

    [[nodiscard]] ALWAYS_INLINE UInt64Vec word(size_t k) const
    {
        return loadLanes([&](size_t i) ALWAYS_INLINE { return data[i] + 8 * k; });
    }

    [[nodiscard]] ALWAYS_INLINE UInt64Vec sizes() const
    {
        return makeLanes([&](size_t i) ALWAYS_INLINE { return UInt64(size[i]); });
    }

    /// The lanes where `active` is 0 get the state `saved` back.
    ALWAYS_INLINE void keepState(Int64Vec active, const std::array<UInt64Vec, 4> & saved)
    {
        v0 = active ? v0 : saved[0];
        v1 = active ? v1 : saved[1];
        v2 = active ? v2 : saved[2];
        v3 = active ? v3 : saved[3];
    }

    /// Compresses the full words that the longer strings have past `common_words`.
    ALWAYS_INLINE void compressLongerWords()
    {
        if (longer_words == common_words)
            return;
        /// A vector operation cannot skip a lane, so a lane past the end of its string runs on the words of another string
        /// and then gets its state back.
        if constexpr (lanes == 2)
        {
            /// Only the longer string has such words. They are loaded once into both lanes, and the other lane gets its
            /// state back at the end.
            const std::array saved{v0, v1, v2, v3};
            const char * longer = size[0] > size[1] ? data[0] : data[1];
            for (size_t k = common_words; k < longer_words; ++k)
                compress(UInt64Vec{} + unalignedLoad<UInt64>(longer + 8 * k));
            const Int64Vec lane_sizes = std::bit_cast<Int64Vec>(sizes());
            keepState(lane_sizes > permute<[](size_t i) { return i ^ 1; }>(lane_sizes), saved);
        }
        else
        {
            /// Several strings can have words past `common_words`. A lane loads its own string while it has words. Past
            /// its end it loads the longest string and gets its state back after each word.
            const char * longest = data[0];
            unrolledFor<lanes>(
                [&](auto i) ALWAYS_INLINE
                {
                    if (size[i] / 8 == longer_words)
                        longest = data[i];
                });
            const Int64Vec words = std::bit_cast<Int64Vec>(sizes() >> 3);
            for (size_t k = common_words; k < longer_words; ++k)
            {
                const std::array saved{v0, v1, v2, v3};
                compress(loadLanes([&](size_t i) ALWAYS_INLINE { return (k < size[i] / 8 ? data[i] : longest) + 8 * k; }));
                keepState(words > static_cast<Int64>(k), saved);
            }
        }
    }

    /// The last partial word with the size modulo 256 in the high byte.
    [[nodiscard]] ALWAYS_INLINE UInt64Vec lastWord() const
    {
        const UInt64Vec lane_sizes = sizes();
        /// The 8 bytes that end at the end of the string. They start before the string when it is shorter than 8 bytes, so
        /// each lane takes one load and no branch on its size.
        const UInt64Vec words = loadLanes([&](size_t i) ALWAYS_INLINE { return data[i] + size[i] - 8; });
        /// Shifting right by 64 - 8 * (size % 8) keeps the last `size % 8` bytes, in the low bytes. When no bytes remain,
        /// the shift is by 64 and gives 0. A vector shift by 64 is undefined, so the code shifts twice.
        return ((words >> 8) >> (56 - ((lane_sizes & 7) << 3))) | (lane_sizes << 56);
    }

    /// Stores the hash of lane `i` to `*hash_at(i)`, with `high_bits` OR-ed into `items[1]`.
    template <UInt64 high_bits, typename HashAt>
    ALWAYS_INLINE void store(HashAt && hash_at) const
    {
        const UInt64Vec low = v0 ^ v1;
        const UInt64Vec high = (v2 ^ v3) | high_bits;
        /// `even` holds the hashes of the even lanes and `odd` of the odd lanes, one per 16 bytes, as ZIP1 and ZIP2 or
        /// VPUNPCKLQDQ and VPUNPCKHQDQ do.
        const auto [even, odd] = [&]<size_t... i>(std::index_sequence<i...>) ALWAYS_INLINE
        {
            return std::pair{
                __builtin_shufflevector(low, high, (i % 2 ? lanes + i - 1 : i)...),
                __builtin_shufflevector(low, high, (i % 2 ? lanes + i : i + 1)...)};
        }(std::make_index_sequence<lanes>{});
        unrolledFor<lanes>(
            [&](auto i) ALWAYS_INLINE
            {
                const UInt64Vec & pairs = i % 2 ? odd : even;
                unalignedStore<UInt64x2>(hash_at(i), __builtin_shufflevector(pairs, pairs, i / 2 * 2, i / 2 * 2 + 1));
            });
    }
};

/// The SipHash state of one string in general registers. With AVX2 there are no scalar states, and the functions are
/// unused.
struct ScalarState
{
    UInt64 v0;
    UInt64 v1;
    UInt64 v2;
    UInt64 v3;
    const char * data;
    size_t size;
    size_t words;

    [[maybe_unused]] ALWAYS_INLINE void init(const char * data_, size_t size_)
    {
        v0 = sip_v0;
        v1 = sip_v1;
        v2 = sip_v2;
        v3 = sip_v3;
        data = data_;
        size = size_;
        words = size / 8;
    }

    [[maybe_unused]] ALWAYS_INLINE void round() { SIPROUND; }

    [[maybe_unused]] ALWAYS_INLINE void compressWord(size_t k)
    {
        const UInt64 word = unalignedLoad<UInt64>(data + 8 * k);
        v3 ^= word;
        SIPROUND;
        SIPROUND;
        v0 ^= word;
    }

    [[nodiscard]] [[maybe_unused]] ALWAYS_INLINE UInt64 lastWord() const
    {
        const size_t remaining = size - 8 * words;
        const UInt64 last = remaining ? unalignedLoad<UInt64>(data + 8 * words) & ((1ULL << (remaining * 8)) - 1) : 0;
        return last | (static_cast<UInt64>(static_cast<uint8_t>(size)) << 56);
    }
};

/// Sets `*hash_at(i)` to `sipHash128Padded` of string `i`, with `high_bits` OR-ed into `items[1]`, for i in
/// [0, `group_size`). `string_at(i)` gives the data and the size of string `i`. `hashGroup` reads up to 8 bytes before a
/// string that is shorter than 8 bytes, and up to 7 bytes past the end of any string.
template <UInt64 high_bits, typename StringAt, typename HashAt>
ALWAYS_INLINE void hashGroup(StringAt && string_at, HashAt && hash_at)
{
    /// The rounds of one string depend on each other, so the strings of a group run interleaved.
    LaneStates vectors[group_vectors];
    std::array<ScalarState, group_scalars> scalars; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - set by `init`
    size_t common_words = std::numeric_limits<size_t>::max();
    unrolledFor<group_vectors>(
        [&](auto j) ALWAYS_INLINE
        {
            vectors[j].init([&](size_t i) ALWAYS_INLINE { return string_at(lanes * j + i); });
            common_words = std::min(common_words, vectors[j].common_words);
        });
    unrolledFor<group_scalars>(
        [&](auto t) ALWAYS_INLINE
        {
            const auto [data, size] = string_at(lanes * group_vectors + t);
            scalars[t].init(data, size);
            common_words = std::min(common_words, scalars[t].words);
        });

    /// One loop compresses the first full words of all the strings, as many as the shortest string has.
    for (size_t k = 0; k < common_words; ++k)
    {
        unrolledFor<group_vectors>([&](auto j) ALWAYS_INLINE { vectors[j].compress(vectors[j].word(k)); });
        unrolledFor<group_scalars>([&](auto t) ALWAYS_INLINE { scalars[t].compressWord(k); });
    }
    /// Then each string compresses the rest of its full words.
    unrolledFor<group_vectors>(
        [&](auto j) ALWAYS_INLINE
        {
            for (size_t k = common_words; k < vectors[j].common_words; ++k)
                vectors[j].compress(vectors[j].word(k));
            vectors[j].compressLongerWords();
        });
    unrolledFor<group_scalars>(
        [&](auto t) ALWAYS_INLINE
        {
            for (size_t k = common_words; k < scalars[t].words; ++k)
                scalars[t].compressWord(k);
        });

    /// The last words and the final rounds of all the strings are interleaved again, one round at a time.
    UInt64Vec vector_last[group_vectors];
    std::array<UInt64, group_scalars> scalar_last; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - set below
    unrolledFor<group_vectors>(
        [&](auto j) ALWAYS_INLINE
        {
            vector_last[j] = vectors[j].lastWord();
            vectors[j].v3 ^= vector_last[j];
        });
    unrolledFor<group_scalars>(
        [&](auto t) ALWAYS_INLINE
        {
            scalar_last[t] = scalars[t].lastWord();
            scalars[t].v3 ^= scalar_last[t];
        });
    for (int i = 0; i < 2; ++i)
    {
        unrolledFor<group_vectors>([&](auto j) ALWAYS_INLINE { vectors[j].round(); });
        unrolledFor<group_scalars>([&](auto t) ALWAYS_INLINE { scalars[t].round(); });
    }
    unrolledFor<group_vectors>(
        [&](auto j) ALWAYS_INLINE
        {
            vectors[j].v0 ^= vector_last[j];
            vectors[j].v2 ^= 0xff;
        });
    unrolledFor<group_scalars>(
        [&](auto t) ALWAYS_INLINE
        {
            scalars[t].v0 ^= scalar_last[t];
            scalars[t].v2 ^= 0xff;
        });
    for (int i = 0; i < 4; ++i)
    {
        unrolledFor<group_vectors>([&](auto j) ALWAYS_INLINE { vectors[j].round(); });
        unrolledFor<group_scalars>([&](auto t) ALWAYS_INLINE { scalars[t].round(); });
    }

    unrolledFor<group_vectors>([&](auto j) ALWAYS_INLINE
                               { vectors[j].template store<high_bits>([&](size_t i) ALWAYS_INLINE { return hash_at(lanes * j + i); }); });
    unrolledFor<group_scalars>(
        [&](auto t) ALWAYS_INLINE
        {
            UInt128 & hash = *hash_at(lanes * group_vectors + t);
            hash.items[0] = scalars[t].v0 ^ scalars[t].v1;
            hash.items[1] = (scalars[t].v2 ^ scalars[t].v3) | high_bits;
        });
}

/// `buckets[r]` = min(words of row r, `exact_buckets`) for the rows [0, num_rows) of a chunk.
inline ALWAYS_INLINE void lengthBuckets(const UInt64 * offsets, size_t num_rows, uint8_t * buckets)
{
    size_t r = 0;
    /// A step of 16 rows gives one 16-byte vector of buckets.
    for (; r + 16 <= num_rows; r += 16)
    {
        /// A step fills a local array: `buckets` may alias `offsets`, so a store per row would keep the step from vectorizing.
        uint8_t step_buckets[16];
        for (size_t i = 0; i < 16; ++i)
        {
            const UInt64 words = (offsets[r + i + 1] - offsets[r + i]) / 8;
            /// Saturating the word count to 32 bits does not change the bucket. It lets the SLP vectorizer narrow with
            /// UQSHRN on NEON.
            const UInt32 words32 = static_cast<UInt32>(std::min<UInt64>(words, std::numeric_limits<UInt32>::max()));
            step_buckets[i] = static_cast<uint8_t>(std::min<UInt32>(words32, exact_buckets));
        }
        memcpy(buckets + r, step_buckets, sizeof(step_buckets));
    }
    for (; r < num_rows; ++r)
        buckets[r] = static_cast<uint8_t>(std::min<UInt64>((offsets[r + 1] - offsets[r]) / 8, exact_buckets));
}

/// The smallest and the largest bucket of a chunk, the number of rows above the smallest, and the number at the largest.
struct BucketStats
{
    unsigned min_bucket = 0;
    unsigned max_bucket = 0;
    size_t rows_above_min = 0;
    size_t rows_at_max = 0;
};

/// The `BucketStats` of the first `num_rows` buckets. `num_rows` must be at least 1. `buckets` has room for `num_rows`
/// rounded up to a multiple of 32, and `bucketStats` overwrites the buckets past `num_rows`.
inline ALWAYS_INLINE BucketStats bucketStats(uint8_t * buckets, size_t num_rows)
{
    const size_t padded_rows = (num_rows + 31) & ~size_t(31);
    static_assert(32 % sizeof(UInt8Vec) == 0);
    /// Padding with the last bucket changes neither the smallest nor the largest bucket.
    for (size_t r = num_rows; r < padded_rows; ++r)
        buckets[r] = buckets[num_rows - 1];
    UInt8Vec smallest = ~UInt8Vec{};
    UInt8Vec largest{};
    for (size_t r = 0; r < padded_rows; r += sizeof(UInt8Vec))
    {
        const UInt8Vec x = unalignedLoad<UInt8Vec>(buckets + r);
        smallest = __builtin_elementwise_min(smallest, x);
        largest = __builtin_elementwise_max(largest, x);
    }
    const uint8_t min_bucket = __builtin_reduce_min(smallest);
    const uint8_t max_bucket = __builtin_reduce_max(largest);
    if (min_bucket == max_bucket)
        return {min_bucket, max_bucket, 0, num_rows};

    /// Padding with the smallest bucket adds no row above it or at the largest.
    for (size_t r = num_rows; r < padded_rows; ++r)
        buckets[r] = min_bucket;
    /// A comparison gives -1 in the lanes where it holds. A lane counts at most `chunk_rows / sizeof(UInt8Vec)` rows.
    static_assert(chunk_rows / sizeof(UInt8Vec) <= std::numeric_limits<UInt8>::max());
    UInt8Vec above_min{};
    UInt8Vec at_max{};
    for (size_t r = 0; r < padded_rows; r += sizeof(UInt8Vec))
    {
        const UInt8Vec x = unalignedLoad<UInt8Vec>(buckets + r);
        above_min -= x != min_bucket;
        at_max -= x == max_bucket;
    }
    using UInt16Widened = UInt16 __attribute__((vector_size(2 * sizeof(UInt8Vec))));
    return {
        min_bucket,
        max_bucket,
        __builtin_reduce_add(__builtin_convertvector(above_min, UInt16Widened)),
        __builtin_reduce_add(__builtin_convertvector(at_max, UInt16Widened))};
}

/// The bucket of a row of at least `exact_buckets` words: 8 buckets per doubling, by the 3 bits after the leading one.
inline ALWAYS_INLINE uint8_t longBucket(UInt64 words)
{
    const unsigned log = std::bit_width(words) - 1;
    const UInt64 bucket = exact_buckets + 8 * (log - std::countr_zero(exact_buckets)) + ((words >> (log - 3)) & 7);
    return static_cast<uint8_t>(std::min<UInt64>(bucket, length_buckets - 1));
}

/// A stable counting sort of `rows` by bucket into `sorted`. `min_bucket` and `max_bucket` bound the buckets.
inline ALWAYS_INLINE void
sortByBucket(const uint8_t * buckets, const UInt32 * rows, size_t num_rows, unsigned min_bucket, unsigned max_bucket, UInt32 * sorted)
{
    const size_t quarter = num_rows / 4;
    /// Each quarter of the rows counts in its own histogram, so that a run of equal buckets does not make each
    /// increment wait for the previous one.
    UInt16 counts[4][length_buckets];
    /// Only the buckets from `min_bucket` to `max_bucket` are cleared and scanned: a chunk usually uses few of the buckets.
    for (auto & count : counts)
        for (unsigned b = min_bucket; b <= max_bucket; ++b)
            count[b] = 0;
    for (size_t i = 0; i < quarter; ++i)
        for (size_t q = 0; q < 4; ++q)
            ++counts[q][buckets[q * quarter + i]];
    for (size_t j = 4 * quarter; j < num_rows; ++j)
        ++counts[3][buckets[j]];
    UInt16 position = 0;
    /// The rows of a bucket keep their order, so that a group reads neighbouring strings.
    for (unsigned b = min_bucket; b <= max_bucket; ++b)
        for (auto & count : counts)
            position += std::exchange(count[b], position);
    for (size_t i = 0; i < quarter; ++i)
        for (size_t q = 0; q < 4; ++q)
            sorted[counts[q][buckets[q * quarter + i]]++] = rows[q * quarter + i];
    for (size_t j = 4 * quarter; j < num_rows; ++j)
        sorted[counts[3][buckets[j]]++] = rows[j];
}

/// Sets `hashes[rows[i]]` to `sipHash128Padded` of row `rows[i]`, with `high_bits` OR-ed into `items[1]`, for i in
/// [0, num_rows). `num_rows` is at most `chunk_rows`. Row `r` is the bytes [chars + offsets[r], chars + offsets[r + 1]):
/// for the rows of a `ColumnString` from row `begin` on, pass its offsets plus `begin - 1`. `buckets[i]` is the bucket
/// that `lengthBuckets` gives row `rows[i]`. `buckets` has room for `num_rows` rounded up to a multiple of 32, and
/// `hashRows` overwrites it. `hashRows` reads up to 8 bytes before a string that is shorter than 8 bytes, and up to 7
/// bytes past the end of any string.
template <UInt64 high_bits = 0>
ALWAYS_INLINE void
hashRows(const char * chars, const UInt64 * offsets, const UInt32 * rows, uint8_t * buckets, size_t num_rows, UInt128 * __restrict hashes)
{
    UInt32 sorted_rows[chunk_rows];
    const UInt32 * order = rows;
    /// A sort by bucket puts rows of similar lengths into the same groups, so fewer words are compressed outside the
    /// interleaved loop of `hashGroup`. It costs about as much as hashing one or two words of every row, so it runs only
    /// where it can repay that. With fewer than 2 groups of rows, it cannot.
    if (num_rows >= 2 * group_size)
    {
        BucketStats stats = bucketStats(buckets, num_rows);
        /// The buckets cap the lengths at `exact_buckets` words. When at least `group_size` rows have the cap, they get
        /// buckets by length ratio (`longBucket`), unless all the rows have one length and would get one bucket again.
        const auto size = [&](UInt32 row) { return offsets[row + 1] - offsets[row]; };
        const bool one_length = stats.min_bucket == exact_buckets
            && std::all_of(rows, rows + num_rows, [&](UInt32 row) { return size(row) == size(rows[0]); });
        if (stats.max_bucket == exact_buckets && stats.rows_at_max >= group_size && !one_length)
        {
            for (size_t j = 0; j < num_rows; ++j)
            {
                const uint8_t long_bucket = longBucket(std::max<UInt64>(size(rows[j]) / 8, exact_buckets));
                buckets[j] = buckets[j] == exact_buckets ? long_bucket : buckets[j];
            }
            stats = bucketStats(buckets, num_rows);
        }
        /// The longer rows need a group of their own: fewer than `group_size` of them still share groups with shorter
        /// rows. So rows of one word count keep their order, also with a few longer rows among them. With fewer than 4
        /// groups of rows, the lengths must also differ by at least 4 words to repay the sort.
        if (stats.rows_above_min >= group_size && (num_rows >= 4 * group_size || stats.max_bucket - stats.min_bucket >= 4))
        {
            sortByBucket(buckets, rows, num_rows, stats.min_bucket, stats.max_bucket, sorted_rows);
            order = sorted_rows;
        }
    }

    size_t j = 0;
    for (; j + group_size <= num_rows; j += group_size)
        hashGroup<high_bits>(
            [&](size_t i) ALWAYS_INLINE
            {
                const UInt32 row = order[j + i];
                return std::pair{chars + offsets[row], offsets[row + 1] - offsets[row]};
            },
            [&](size_t i) ALWAYS_INLINE { return hashes + order[j + i]; });
    for (; j < num_rows; ++j)
    {
        UInt128 & hash = hashes[order[j]];
        hash = sipHash128Padded(chars + offsets[order[j]], offsets[order[j] + 1] - offsets[order[j]]);
        hash.items[1] |= high_bits;
    }
}

}

#endif
