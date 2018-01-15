#include <string.h>
#include <iostream>

#include <Common/LZ4_decompress_faster.h>
#include <Core/Defines.h>

#include <common/likely.h>
#include <common/Types.h>
#include <common/unaligned.h>

#if __SSE2__
#include <emmintrin.h>
#endif

#if __SSSE3__
#include <tmmintrin.h>
#endif


/** for i in *.bin; do ./decompress_perf < $i > /dev/null; done
  */

namespace LZ4
{

namespace
{

template <size_t N> [[maybe_unused]] void copy(UInt8 * dst, const UInt8 * src);
template <size_t N> [[maybe_unused]] void wildCopy(UInt8 * dst, const UInt8 * src, UInt8 * dst_end);
template <size_t N, bool USE_SHUFFLE> [[maybe_unused]] void copyOverlap(UInt8 * op, const UInt8 *& match, const size_t offset);


inline void copy8(UInt8 * dst, const UInt8 * src)
{
    memcpy(dst, src, 8);
}

inline void wildCopy8(UInt8 * dst, const UInt8 * src, UInt8 * dst_end)
{
    do
    {
        copy8(dst, src);
        dst += 8;
        src += 8;
    } while (dst < dst_end);
}

inline void copyOverlap8(UInt8 * op, const UInt8 *& match, const size_t offset)
{
    /// 4 % n.
    static constexpr int shift1[] = { 0, 1, 2, 1, 4, 4, 4, 4 };

    /// 8 % n - 4 % n
    static constexpr int shift2[] = { 0, 0, 0, 1, 0, -1, -2, -3 };

    op[0] = match[0];
    op[1] = match[1];
    op[2] = match[2];
    op[3] = match[3];

    match += shift1[offset];
    memcpy(op + 4, match, 4);
    match += shift2[offset];
}

inline void copyOverlap8Shuffle(UInt8 * op, const UInt8 *& match, const size_t offset)
{
#ifdef __SSSE3__

    static constexpr UInt8 __attribute__((__aligned__(8))) masks[] =
    {
        0, 1, 2, 2, 4, 3, 2, 1, /* offset = 0, not used as mask, but for shift amount instead */
        0, 0, 0, 0, 0, 0, 0, 0, /* offset = 1 */
        0, 1, 0, 1, 0, 1, 0, 1,
        0, 1, 2, 0, 1, 2, 0, 1,
        0, 1, 2, 3, 0, 1, 2, 3,
        0, 1, 2, 3, 4, 0, 1, 2,
        0, 1, 2, 3, 4, 5, 0, 1,
        0, 1, 2, 3, 4, 5, 6, 0,
    };

    unalignedStore(op, _mm_shuffle_pi8(
        unalignedLoad<__m64>(match),
        unalignedLoad<__m64>(masks + 8 * offset)));

    match += masks[offset];

#else
    copyOverlap8(op, match, offset);
#endif
}

template <> void inline copy<8>(UInt8 * dst, const UInt8 * src) { copy8(dst, src); };
template <> void inline wildCopy<8>(UInt8 * dst, const UInt8 * src, UInt8 * dst_end) { wildCopy8(dst, src, dst_end); };
template <> void inline copyOverlap<8, false>(UInt8 * op, const UInt8 *& match, const size_t offset) { copyOverlap8(op, match, offset); };
template <> void inline copyOverlap<8, true>(UInt8 * op, const UInt8 *& match, const size_t offset) { copyOverlap8Shuffle(op, match, offset); };


#if __SSE2__

inline void copy16(UInt8 * dst, const UInt8 * src)
{
    _mm_storeu_si128(reinterpret_cast<__m128i *>(dst),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(src)));
}

inline void wildCopy16(UInt8 * dst, const UInt8 * src, UInt8 * dst_end)
{
    do
    {
        copy16(dst, src);
        dst += 16;
        src += 16;
    } while (dst < dst_end);
}

inline void copyOverlap16(UInt8 * op, const UInt8 *& match, const size_t offset)
{
    /// 4 % n.
    static constexpr int shift1[]
        = { 0,  1,  2,  1,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4 };

    /// 8 % n - 4 % n
    static constexpr int shift2[]
        = { 0,  0,  0,  1,  0, -1, -2, -3, -4,  4,  4,  4,  4,  4,  4,  4 };

    /// 16 % n - 8 % n
    static constexpr int shift3[]
        = { 0,  0,  0, -1,  0, -2,  2,  1,  8, -1, -2, -3, -4, -5, -6, -7 };

    op[0] = match[0];
    op[1] = match[1];
    op[2] = match[2];
    op[3] = match[3];

    match += shift1[offset];
    memcpy(op + 4, match, 4);
    match += shift2[offset];
    memcpy(op + 8, match, 8);
    match += shift3[offset];
}

inline void copyOverlap16Shuffle(UInt8 * op, const UInt8 *& match, const size_t offset)
{
#ifdef __SSSE3__

    static constexpr UInt8 __attribute__((__aligned__(16))) masks[] =
    {
        0,  1,  2,  1,  4,  1,  4,  2,  8,  7,  6,  5,  4,  3,  2,  1, /* offset = 0, not used as mask, but for shift amount instead */
        0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, /* offset = 1 */
        0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,
        0,  1,  2,  0,  1,  2,  0,  1,  2,  0,  1,  2,  0,  1,  2,  0,
        0,  1,  2,  3,  0,  1,  2,  3,  0,  1,  2,  3,  0,  1,  2,  3,
        0,  1,  2,  3,  4,  0,  1,  2,  3,  4,  0,  1,  2,  3,  4,  0,
        0,  1,  2,  3,  4,  5,  0,  1,  2,  3,  4,  5,  0,  1,  2,  3,
        0,  1,  2,  3,  4,  5,  6,  0,  1,  2,  3,  4,  5,  6,  0,  1,
        0,  1,  2,  3,  4,  5,  6,  7,  0,  1,  2,  3,  4,  5,  6,  7,
        0,  1,  2,  3,  4,  5,  6,  7,  8,  0,  1,  2,  3,  4,  5,  6,
        0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  0,  1,  2,  3,  4,  5,
        0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10,  0,  1,  2,  3,  4,
        0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11,  0,  1,  2,  3,
        0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12,  0,  1,  2,
        0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13,  0,  1,
        0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,  0,
    };

    _mm_storeu_si128(reinterpret_cast<__m128i *>(op),
        _mm_shuffle_epi8(
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(match)),
            _mm_load_si128(reinterpret_cast<const __m128i *>(masks) + offset)));

    match += masks[offset];

#else
    copyOverlap16(op, match, offset);
#endif
}

template <> void inline copy<16>(UInt8 * dst, const UInt8 * src) { copy16(dst, src); };
template <> void inline wildCopy<16>(UInt8 * dst, const UInt8 * src, UInt8 * dst_end) { wildCopy16(dst, src, dst_end); };
template <> void inline copyOverlap<16, false>(UInt8 * op, const UInt8 *& match, const size_t offset) { copyOverlap16(op, match, offset); };
template <> void inline copyOverlap<16, true>(UInt8 * op, const UInt8 *& match, const size_t offset) { copyOverlap16Shuffle(op, match, offset); };

#endif

/// See also https://stackoverflow.com/a/30669632


template <size_t copy_amount, bool use_shuffle>
void NO_INLINE decompressImpl(
     const char * const source,
     char * const dest,
     size_t dest_size)
{
    const UInt8 * ip = (UInt8 *)source;
    UInt8 * op = (UInt8 *)dest;
    UInt8 * const output_end = op + dest_size;

    while (1)
    {
        size_t length;

        auto continue_read_length = [&]
        {
            unsigned s;
            do
            {
                s = *ip++;
                length += s;
            } while (unlikely(s == 255));
        };

        /// Get literal length.

        const unsigned token = *ip++;
        length = token >> 4;
        if (length == 0x0F)
            continue_read_length();

        /// Copy literals.

        UInt8 * copy_end = op + length;

        wildCopy<copy_amount>(op, ip, copy_end);    /// Here we can write up to copy_amount - 1 bytes after buffer.

        ip += length;
        op = copy_end;

        if (copy_end >= output_end)
            return;

        /// Get match offset.

        size_t offset = unalignedLoad<UInt16>(ip);
        ip += 2;
        const UInt8 * match = op - offset;

        /// Get match length.

        length = token & 0x0F;
        if (length == 0x0F)
            continue_read_length();
        length += 4;

        /// Copy match within block, that produce overlapping pattern. Match may replicate itself.

        copy_end = op + length;

        /** Here we can write up to copy_amount - 1 - 4 * 2 bytes after buffer.
          * The worst case when offset = 1 and length = 4
          */

        if (unlikely(offset < copy_amount))
        {
            copyOverlap<copy_amount, use_shuffle>(op, match, offset);
        }
        else
        {
            copy<copy_amount>(op, match);
            match += copy_amount;
        }

        op += copy_amount;

        copy<copy_amount>(op, match);   /// copy_amount + copy_amount - 1 - 4 * 2 bytes after buffer.
        if (length > copy_amount * 2)
            wildCopy<copy_amount>(op + copy_amount, match + copy_amount, copy_end);

        op = copy_end;
    }
}

}


void decompress(
    const char * const source,
    char * const dest,
    size_t source_size,
    size_t dest_size)
{
    decompressImpl<16, true>(source, dest, dest_size);
    (void) source_size;

/*    if (dest_size / source_size >= 16)
        decompressImpl<16, true>(source, dest, dest_size);
    else
        decompressImpl<8, true>(source, dest, dest_size);*/
}


void Stat::literal(size_t length)
{
    ++num_tokens;
    sum_literal_lengths += length;
}

void Stat::match(size_t length, size_t offset)
{
    ++num_tokens;
    sum_match_lengths += length;
    sum_match_offsets += offset;
    count_match_offset_less_8 += offset < 8;
    count_match_offset_less_16 += offset < 16;
    count_match_replicate_itself += offset < length;
}

void Stat::print() const
{
    std::cerr
        << "Num tokens: " << num_tokens
        << ", Avg literal length: " << double(sum_literal_lengths) / num_tokens
        << ", Avg match length: " << double(sum_match_lengths) / num_tokens
        << ", Avg match offset: " << double(sum_match_offsets) / num_tokens
        << ", Offset < 8 ratio: " << double(count_match_offset_less_8) / num_tokens
        << ", Offset < 16 ratio: " << double(count_match_offset_less_16) / num_tokens
        << ", Match replicate itself: " << double(count_match_replicate_itself) / num_tokens
        << "\n";
}

Stat statistics(
    const char * const source,
    char * const dest,
    size_t dest_size,
    Stat & stat)
{
    const UInt8 * ip = (UInt8 *)source;
    UInt8 * op = (UInt8 *)dest;
    UInt8 * const output_end = op + dest_size;

    while (1)
    {
        size_t length;

        auto continue_read_length = [&]
        {
            unsigned s;
            do
            {
                s = *ip++;
                length += s;
            } while (unlikely(s == 255));
        };

        auto token = *ip++;
        length = token >> 4;
        if (length == 0x0F)
            continue_read_length();

        stat.literal(length);

        ip += length;
        op += length;

        if (op > output_end)
            return stat;

        size_t offset = unalignedLoad<UInt16>(ip);
        ip += 2;

        length = token & 0x0F;
        if (length == 0x0F)
            continue_read_length();
        length += 4;

        stat.match(length, offset);

        op += length;
    }
}

}
