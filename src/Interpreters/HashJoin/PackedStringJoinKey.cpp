#include <cstddef>
#include <Interpreters/HashJoin/KeyGetter.h>

#if (defined(__aarch64__) && defined(__ARM_NEON)) || defined(__AVX2__)
#    include <Common/SipHashStringsImpl.h>

#    include <algorithm>
#    include <cstring>
#    include <span>
#endif


namespace DB
{

#if (defined(__aarch64__) && defined(__ARM_NEON)) || defined(__AVX2__)

namespace
{

using SipHashStrings::chunk_rows;

/// The bucket of a row is its number of 8-byte words, up to `SipHashStrings::exact_buckets`. A row is hashed when its
/// bucket is at least `first_long_bucket`.
constexpr uint8_t first_long_bucket = 2;
static_assert(PackedStringJoinKey::max_inline_size == first_long_bucket * sizeof(UInt64) - 1);

/// Writes `UnhashedPackedStringRef::buildShort` of the string into `key`. `size` must be at most 15. It reads 16 bytes from `data`.
ALWAYS_INLINE void shortKey(const char * data, size_t size, UInt128 * key)
{
    const auto ref = UnhashedPackedStringRef::buildShort(data, size);
    memcpy(key, &ref, sizeof(ref));
}

/// Builds the keys of 1 to `chunk_rows` rows. Row `r` is the chars [offsets[r], offsets[r + 1]): for the rows
/// of a `ColumnString` from row `begin` on, `offsets` is the offsets of the column plus `begin - 1`, and `skip` is the
/// skip bytes of the column plus `begin`, see `buildRange`.
void buildChunk(const char * chars, const UInt64 * offsets, const UInt8 * skip, size_t num_rows, UInt128 * __restrict keys)
{
    uint8_t buckets[chunk_rows + 32];
    UInt32 long_rows[chunk_rows];

    SipHashStrings::lengthBuckets(offsets, num_rows, buckets);
    const auto [min_bucket, max_bucket] = std::ranges::minmax(std::span(buckets, num_rows));

    if (max_bucket < first_long_bucket)
    {
        /// Clang does not unroll this loop on aarch64 by itself. Unrolled, it loads the offsets and stores the keys in
        /// pairs, and it updates the loop counter once per 4 rows.
#pragma clang loop unroll_count(4)
        for (size_t r = 0; r < num_rows; ++r)
            shortKey(chars + offsets[r], offsets[r + 1] - offsets[r], keys + r);
        return;
    }

    /// A short key costs less than checking whether its row is skipped, so only a chunk with long rows reads the skip
    /// bytes. Clang vectorizes this OR, so it costs less than the loop below.
    UInt8 any_skipped = 0;
    for (size_t r = 0; r < num_rows; ++r)
        any_skipped |= skip[r];

    /// Some rows are skipped, and all the other rows are long.
    bool only_long_unskipped = false;
    if (any_skipped)
    {
        uint8_t min_unskipped = 0xFF;
        for (size_t r = 0; r < num_rows; ++r)
        {
            const uint8_t skipped = skip[r] ? 0xFF : 0;
            min_unskipped = std::min<uint8_t>(min_unskipped, buckets[r] | skipped);
            /// A skipped row gets bucket 0, so it is not hashed.
            buckets[r] &= ~skipped;
        }
        only_long_unskipped = min_unskipped >= first_long_bucket;
    }

    size_t num_long = num_rows;
    if (only_long_unskipped)
    {
        /// No short keys are built: the long rows go to the front of `long_rows` and `buckets`, and the skipped rows
        /// get no key.
        num_long = 0;
#pragma clang loop unroll_count(4)
        for (size_t r = 0; r < num_rows; ++r)
        {
            const uint8_t bucket = buckets[r];
            long_rows[num_long] = static_cast<UInt32>(r);
            buckets[num_long] = bucket;
            num_long += bucket >= first_long_bucket;
        }
    }
    /// `hashRows` sets the keys of the long rows, so when all the rows are long no short keys are built.
    else if (min_bucket >= first_long_bucket)
    {
        for (size_t r = 0; r < num_rows; ++r)
            long_rows[r] = static_cast<UInt32>(r);
    }
    else
    {
        /// Every row gets a short key, a wrong one for a skipped long row. The long rows go to `long_rows`, and their
        /// buckets go to the front of `buckets`.
        num_long = 0;
        /// Unrolled for the same reason as the loop above.
#pragma clang loop unroll_count(4)
        for (size_t r = 0; r < num_rows; ++r)
        {
            const uint8_t bucket = buckets[r];
            /// Every row is written to the next place in both arrays, and `num_long` moves past the long rows only. So
            /// the loop has no branch on the length: with mixed lengths such a branch would often be mispredicted.
            long_rows[num_long] = static_cast<UInt32>(r);
            buckets[num_long] = bucket;
            num_long += bucket >= first_long_bucket;
            /// `hashRows` overwrites the keys of the long rows, so for them `% 16` only has to keep the index into
            /// the mask table of `buildShort` in range. `% 16` is one AND, while `std::min` would need a compare and a select.
            shortKey(chars + offsets[r], (offsets[r + 1] - offsets[r]) % 16, keys + r);
        }
    }

    SipHashStrings::hashRows<PackedStringJoinKey::hashed_flag>(chars, offsets, long_rows, buckets, num_long, keys);
}

}

#endif

void PackedStringJoinKey::buildRange(
    const char * chars, const IColumn::Offset * offsets, size_t begin, size_t end, UInt128 * __restrict keys, const UInt8 * skip)
{
#if (defined(__aarch64__) && defined(__ARM_NEON)) || defined(__AVX2__)
    for (size_t chunk_begin = begin; chunk_begin < end; chunk_begin += chunk_rows)
        buildChunk(
            chars, offsets + chunk_begin - 1, skip + chunk_begin, std::min(chunk_rows, end - chunk_begin), keys + (chunk_begin - begin));

#    ifndef NDEBUG
    for (size_t i = begin; i < end; ++i)
        chassert(skip[i] || keys[i - begin] == build(chars + offsets[ssize_t(i) - 1], offsets[i] - offsets[ssize_t(i) - 1]));
#    endif
#else
    for (size_t i = begin; i < end; ++i)
        if (!skip[i])
            keys[i - begin] = build(chars + offsets[ssize_t(i) - 1], offsets[i] - offsets[ssize_t(i) - 1]);
#endif
}

}
