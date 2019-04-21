#include "LZ4_decompress_faster.h"

#include <string.h>
#include <iostream>
#include <random>
#include <algorithm>
#include <Core/Defines.h>
#include <Common/Stopwatch.h>
#include <common/likely.h>
#include <common/Types.h>
#include <common/unaligned.h>

#ifdef __SSE2__
#include <emmintrin.h>
#endif

#ifdef __AVX__
#include <immintrin.h>
#endif

#ifdef __SSSE3__
#include <tmmintrin.h>
#endif

#ifdef __aarch64__
#include <arm_neon.h>
#endif


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
    /// Or if 4 % n is zero, we use n.
    /// It gives equivalent result, but is better CPU friendly for unknown reason.
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


#if defined(__x86_64__) || defined(__PPC__)

/** We use 'xmm' (128bit SSE) registers here to shuffle 16 bytes.
  *
  * It is possible to use 'mm' (64bit MMX) registers to shuffle just 8 bytes as we need.
  *
  * There is corresponding version of 'pshufb' instruction that operates on 'mm' registers,
  *  (it operates on MMX registers although it is available in SSSE3)
  *  and compiler library has the corresponding intrinsic: '_mm_shuffle_pi8'.
  *
  * It can be done like this:
  *
  *  unalignedStore(op, _mm_shuffle_pi8(
  *      unalignedLoad<__m64>(match),
  *      unalignedLoad<__m64>(masks + 8 * offset)));
  *
  * This is perfectly correct and this code have the same or even better performance.
  *
  * But if we write code this way, it will lead to
  *  extremely weird and extremely non obvious
  *  effects in completely unrelated parts of code.
  *
  * Because using MMX registers alters the mode of operation of x87 FPU,
  *  and then operations with FPU become broken.
  *
  * Example 1.
  * Compile this code without optimizations:
  *
    #include <vector>
    #include <unordered_set>
    #include <iostream>
    #include <tmmintrin.h>

    int main(int, char **)
    {
        [[maybe_unused]] __m64 shuffled = _mm_shuffle_pi8(__m64{}, __m64{});

        std::vector<int> vec;
        std::unordered_set<int> set(vec.begin(), vec.end());

        std::cerr << set.size() << "\n";
        return 0;
    }

    $ g++ -g -O0 -mssse3 -std=c++17 mmx_bug1.cpp && ./a.out
    terminate called after throwing an instance of 'std::bad_alloc'
    what():  std::bad_alloc

    Also reproduced with clang. But only with libstdc++, not with libc++.

  * Example 2.

    #include <math.h>
    #include <iostream>
    #include <tmmintrin.h>

    int main(int, char **)
    {
        double max_fill = 1;

        std::cerr << (long double)max_fill << "\n";
        [[maybe_unused]] __m64 shuffled = _mm_shuffle_pi8(__m64{}, __m64{});
        std::cerr << (long double)max_fill << "\n";

        return 0;
    }

    $ g++ -g -O0 -mssse3 -std=c++17 mmx_bug2.cpp && ./a.out
    1
    -nan

  * Explanation:
  *
  * https://stackoverflow.com/questions/33692969/assembler-mmx-errors
  * https://software.intel.com/en-us/node/524274
  *
  * Actually it's possible to use 'emms' instruction after decompression routine.
  * But it's more easy to just use 'xmm' registers and avoid using 'mm' registers.
  */
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
        0, 0, 0, 0, 0, 0, 0, 0, /* this row is not used: padding to allow read 16 bytes starting at previous row */
    };

    _mm_storeu_si128(reinterpret_cast<__m128i *>(op),
        _mm_shuffle_epi8(
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(match)),
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(masks + 8 * offset))));

    match += masks[offset];

#else
    copyOverlap8(op, match, offset);
#endif
}

#endif


#ifdef __aarch64__

inline void copyOverlap8Shuffle(UInt8 * op, const UInt8 *& match, const size_t offset)
{
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

    unalignedStore(op, vtbl1_u8(unalignedLoad<uint8x8_t>(match), unalignedLoad<uint8x8_t>(masks + 8 * offset)));
    match += masks[offset];
}

#endif



template <> void inline copy<8>(UInt8 * dst, const UInt8 * src) { copy8(dst, src); }
template <> void inline wildCopy<8>(UInt8 * dst, const UInt8 * src, UInt8 * dst_end) { wildCopy8(dst, src, dst_end); }
template <> void inline copyOverlap<8, false>(UInt8 * op, const UInt8 *& match, const size_t offset) { copyOverlap8(op, match, offset); }
template <> void inline copyOverlap<8, true>(UInt8 * op, const UInt8 *& match, const size_t offset) { copyOverlap8Shuffle(op, match, offset); }


inline void copy16(UInt8 * dst, const UInt8 * src)
{
#ifdef __SSE2__
    _mm_storeu_si128(reinterpret_cast<__m128i *>(dst),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(src)));
#else
    memcpy(dst, src, 16);
#endif
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


#if defined(__x86_64__) || defined(__PPC__)

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

#endif

#ifdef __aarch64__

inline void copyOverlap16Shuffle(UInt8 * op, const UInt8 *& match, const size_t offset)
{
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

    unalignedStore(op,
        vtbl2_u8(unalignedLoad<uint8x8x2_t>(match), unalignedLoad<uint8x8_t>(masks + 16 * offset)));

    unalignedStore(op + 8,
        vtbl2_u8(unalignedLoad<uint8x8x2_t>(match), unalignedLoad<uint8x8_t>(masks + 16 * offset + 8)));

    match += masks[offset];
}

#endif


template <> void inline copy<16>(UInt8 * dst, const UInt8 * src) { copy16(dst, src); }
template <> void inline wildCopy<16>(UInt8 * dst, const UInt8 * src, UInt8 * dst_end) { wildCopy16(dst, src, dst_end); }
template <> void inline copyOverlap<16, false>(UInt8 * op, const UInt8 *& match, const size_t offset) { copyOverlap16(op, match, offset); }
template <> void inline copyOverlap<16, true>(UInt8 * op, const UInt8 *& match, const size_t offset) { copyOverlap16Shuffle(op, match, offset); }

inline void copy32(UInt8 * dst, const UInt8 * src)
{
#if defined(__AVX__)
    _mm256_storeu_si256(reinterpret_cast<__m256i *>(dst),
        _mm256_loadu_si256(reinterpret_cast<const __m256i *>(src)));
#elif defined(__SSE2__)
    _mm_storeu_si128(reinterpret_cast<__m128i *>(dst),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(src)));
    _mm_storeu_si128(reinterpret_cast<__m128i *>(dst + 16),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(src + 16)));
#else
    memcpy(dst, src, 16);
#endif
}

inline void wildCopy32(UInt8 * dst, const UInt8 * src, UInt8 * dst_end)
{
    do
    {
        copy32(dst, src);
        dst += 32;
        src += 32;
    } while (dst < dst_end);
}

template <> void inline copy<32>(UInt8 * dst, const UInt8 * src) { copy32(dst, src); }
template <> void inline wildCopy<32>(UInt8 * dst, const UInt8 * src, UInt8 * dst_end) { wildCopy32(dst, src, dst_end); }

inline void copyUsingOffset(UInt8 * dst, const UInt8 * src, UInt8 * dst_end, const size_t offset)
{
    UInt8 v[8];
    switch (offset)
    {
        case 1:
            memset(v, *src, 8);
            goto copy_loop;
        case 2:
            memcpy(v, src, 2);
            memcpy(&v[2], src, 2);
            memcpy(&v[4], &v[0], 4);
            goto copy_loop;
        case 4:
            memcpy(v, src, 4);
            memcpy(&v[4], src, 4);
            goto copy_loop;
        default:
            if (offset < 8)
            {
                copyOverlap<8, false>(dst, src, offset);
                dst += 8;
            }
            else
            {
                memcpy(dst, src, 8);
                dst += 8;
                src += 8;
            }
            wildCopy<8>(dst, src, dst_end);
            return;
    }
copy_loop:
    memcpy(dst, v, 8);
    dst += 8;
    while (dst < dst_end)
    {
        memcpy(dst, v, 8);
        dst += 8;
    }
}

/// See also https://stackoverflow.com/a/30669632


/// use_optimized_new_lz4_version is for the new optimized LZ4 decompression that was introduced in LZ4 1.9.0
/// With experiments it turned out to be faster for big columns, so we decided to add it to the statistics choice.

template <size_t copy_amount, size_t guaranteed_minimal_dest_size, bool use_shuffle, bool use_optimized_new_lz4_version>
void NO_INLINE decompressImpl(
     const char * const source,
     char * const dest,
     [[maybe_unused]] size_t source_size,
     size_t dest_size)
{
    const UInt8 * ip = reinterpret_cast<const UInt8 *>(source);
    [[maybe_unused]] const UInt8 * const input_end = reinterpret_cast<const UInt8 *>(source) + source_size;
    UInt8 * op = reinterpret_cast<UInt8 *>(dest);
    UInt8 * const output_end = op + dest_size;
    UInt8 * copy_end;
    unsigned token;
    size_t length;
    size_t offset;
    const UInt8 * match;

    auto continue_read_length = [&]
    {
        unsigned s;
        do
        {
            s = *ip++;
            length += s;
        } while (unlikely(s == 255));
    };

    if (use_optimized_new_lz4_version)
    {
        if (guaranteed_minimal_dest_size >= 64 || dest_size >= 64)
        {
            while (1)
            {
                /// Get literal length.
                token = *ip++;
                length = token >> 4;
                if (length == 0x0F)
                {
                    continue_read_length();
                    copy_end = op + length;
                    if (copy_end > output_end - 32 || ip + length > input_end - 32)
                        goto literal_copy;
                    wildCopy<32>(op, ip, copy_end);
                    ip += length;
                    op = copy_end;
                }
                else
                {
                    copy_end = op + length;
                    if (ip > input_end - 17)
                        goto literal_copy;
                    copy<16>(op, ip);
                    ip += length;
                    op = copy_end;
                }

                offset = unalignedLoad<UInt16>(ip);
                ip += 2;
                match = op - offset;

                /// Get match length.
                length = token & 0x0F;
                if (length == 0x0F)
                {
                    continue_read_length();
                    length += 4;
                    if (op + length >= output_end - 64)
                        goto match_copy;
                }
                else
                {
                    length += 4;
                    if (op + length >= output_end - 64)
                        goto match_copy;
                    if (offset >= 8)
                    {
                        memcpy(op, match, 8);
                        memcpy(op + 8, match + 8, 8);
                        memcpy(op + 16, match + 16, 2);
                        op += length;
                        continue;
                    }
                }

                /// Copy match within block, that produce overlapping pattern. Match may replicate itself.
                copy_end = op + length;
                if (unlikely(offset < 16))
                    copyUsingOffset(op, match, copy_end, offset);
                else
                    wildCopy<32>(op, match, copy_end);

                op = copy_end;
            }
        }
    }

    while (1)
    {
        /// Get literal length.
        token = *ip++;
        length = token >> 4;
        if (length == 0x0F)
            continue_read_length();
        /// Copy literals.
        copy_end = op + length;
literal_copy:
        /// input: Hello, world
        ///        ^-ip
        /// output: xyz
        ///            ^-op  ^-copy_end
        /// output: xyzHello, w
        ///                   ^- excessive copied bytes due to "wildCopy"
        /// input: Hello, world
        ///              ^-ip
        /// output: xyzHello, w
        ///                  ^-op (we will overwrite excessive bytes on next iteration)
        wildCopy<copy_amount>(op, ip, copy_end);

        ip += length;
        op = copy_end;

        if (copy_end >= output_end)
            return;

        /// Get match offset.
        offset = unalignedLoad<UInt16>(ip);
        ip += 2;
        match = op - offset;

        /// Get match length.
        length = token & 0x0F;
        if (length == 0x0F)
            continue_read_length();
        length += 4;

match_copy:
        /// Copy match within block, that produce overlapping pattern. Match may replicate itself.
        copy_end = op + length;

        /** Here we can write up to copy_amount - 1 - 4 * 2 bytes after buffer.
          * The worst case when offset = 1 and length = 4
          */

        if (unlikely(offset < copy_amount))
        {
            /// output: Hello
            ///              ^-op
            ///         ^-match; offset = 5
            ///
            /// output: Hello
            ///         [------] - copy_amount bytes
            ///              [------] - copy them here
            ///
            /// output: HelloHelloHel
            ///            ^-match   ^-op
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
    size_t dest_size,
    PerformanceStatistics & statistics [[maybe_unused]])
{
    if (source_size == 0 || dest_size == 0)
        return;
    static constexpr size_t small_block_threshold = 32768;
    /// Don't run timer if the block is too small.
    if (dest_size >= small_block_threshold)
    {
        size_t best_variant = statistics.select();

        /// Run the selected method and measure time.

        Stopwatch watch;

        if (best_variant == 0)
            decompressImpl<16, small_block_threshold, true, false>(source, dest, source_size, dest_size);
        if (best_variant == 1)
            decompressImpl<16, small_block_threshold, false, false>(source, dest, source_size, dest_size);
        if (best_variant == 2)
            decompressImpl<8, small_block_threshold, true, false>(source, dest, source_size, dest_size);
        if (best_variant == 3)
            decompressImpl<8, small_block_threshold, false, true>(source, dest, source_size, dest_size);

        watch.stop();

        /// Update performance statistics.

        statistics.data[best_variant].update(watch.elapsedSeconds(), dest_size);
    }
    else
    {
        decompressImpl<8, 0, false, false>(source, dest, source_size, dest_size);
    }
}


void StreamStatistics::literal(size_t length)
{
    ++num_tokens;
    sum_literal_lengths += length;
}

void StreamStatistics::match(size_t length, size_t offset)
{
    ++num_tokens;
    sum_match_lengths += length;
    sum_match_offsets += offset;
    count_match_offset_less_8 += offset < 8;
    count_match_offset_less_16 += offset < 16;
    count_match_replicate_itself += offset < length;
}

void StreamStatistics::print() const
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

void statistics(
    const char * const source,
    char * const dest,
    size_t dest_size,
    StreamStatistics & stat)
{
    const UInt8 * ip = reinterpret_cast<const UInt8 *>(source);
    UInt8 * op = reinterpret_cast<UInt8 *>(dest);
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
            return;

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
