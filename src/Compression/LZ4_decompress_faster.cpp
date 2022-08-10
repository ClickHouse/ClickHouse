#include "LZ4_decompress_faster.h"

#include <cstring>
#include <iostream>
#include <Core/Defines.h>
#include <Common/Stopwatch.h>
#include <base/types.h>
#include <base/unaligned.h>

#ifdef __SSE2__
#include <emmintrin.h>
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
template <size_t N, bool USE_SHUFFLE> [[maybe_unused]] void copyOverlap(UInt8 * op, const UInt8 *& match, size_t offset);


inline void copy8(UInt8 * dst, const UInt8 * src)
{
    memcpy(dst, src, 8);
}

inline void wildCopy8(UInt8 * dst, const UInt8 * src, const UInt8 * dst_end)
{
    /// Unrolling with clang is doing >10% performance degrade.
#if defined(__clang__)
    #pragma nounroll
#endif
    do
    {
        copy8(dst, src);
        dst += 8;
        src += 8;
    } while (dst < dst_end);
}

inline void copyOverlap8(UInt8 * op, const UInt8 *& match, size_t offset)
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


#if defined(__x86_64__) || defined(__PPC__) || defined(__riscv)

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
#if defined(__SSSE3__) && !defined(MEMORY_SANITIZER)

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

    unalignedStore<uint8x8_t>(op, vtbl1_u8(unalignedLoad<uint8x8_t>(match), unalignedLoad<uint8x8_t>(masks + 8 * offset)));
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

inline void wildCopy16(UInt8 * dst, const UInt8 * src, const UInt8 * dst_end)
{
    /// Unrolling with clang is doing >10% performance degrade.
#if defined(__clang__)
    #pragma nounroll
#endif
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


#if defined(__x86_64__) || defined(__PPC__) || defined (__riscv)

inline void copyOverlap16Shuffle(UInt8 * op, const UInt8 *& match, const size_t offset)
{
#if defined(__SSSE3__) && !defined(MEMORY_SANITIZER)

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

    unalignedStore<uint8x8_t>(op,
        vtbl2_u8(unalignedLoad<uint8x8x2_t>(match), unalignedLoad<uint8x8_t>(masks + 16 * offset)));

    unalignedStore<uint8x8_t>(op + 8,
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
    /// There was an AVX here but with mash with SSE instructions, we got a big slowdown.
#if defined(__SSE2__)
    _mm_storeu_si128(reinterpret_cast<__m128i *>(dst),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(src)));
    _mm_storeu_si128(reinterpret_cast<__m128i *>(dst + 16),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(src + 16)));
#else
    memcpy(dst, src, 16);
    memcpy(dst + 16, src + 16, 16);
#endif
}

inline void wildCopy32(UInt8 * dst, const UInt8 * src, const UInt8 * dst_end)
{
    /// Unrolling with clang is doing >10% performance degrade.
#if defined(__clang__)
    #pragma nounroll
#endif
    do
    {
        copy32(dst, src);
        dst += 32;
        src += 32;
    } while (dst < dst_end);
}

inline void copyOverlap32(UInt8 * op, const UInt8 *& match, const size_t offset)
{
    /// 4 % n.
    static constexpr int shift1[]
        = { 0,  1,  2,  1,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4 };

    /// 8 % n - 4 % n
    static constexpr int shift2[]
        = { 0,  0,  0,  1,  0, -1, -2, -3, -4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4 };

    /// 16 % n - 8 % n
    static constexpr int shift3[]
        = { 0,  0,  0, -1,  0, -2,  2,  1,  8, -1, -2, -3, -4, -5, -6, -7,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8 };

    /// 32 % n - 16 % n
    static constexpr int shift4[]
        = { 0,  0,  0,  1,  0,  1, -2,  2,  0, -2, -4,  5,  4,  3,  2,  1,  0, -1, -2, -3, -4, -5, -6, -7, -8, -9,-10,-11,-12,-13,-14,-15 };

    op[0] = match[0];
    op[1] = match[1];
    op[2] = match[2];
    op[3] = match[3];

    match += shift1[offset];
    memcpy(op + 4, match, 4);
    match += shift2[offset];
    memcpy(op + 8, match, 8);
    match += shift3[offset];
    memcpy(op + 16, match, 16);
    match += shift4[offset];
}


template <> void inline copy<32>(UInt8 * dst, const UInt8 * src) { copy32(dst, src); }
template <> void inline wildCopy<32>(UInt8 * dst, const UInt8 * src, UInt8 * dst_end) { wildCopy32(dst, src, dst_end); }
template <> void inline copyOverlap<32, false>(UInt8 * op, const UInt8 *& match, const size_t offset) { copyOverlap32(op, match, offset); }


/// See also https://stackoverflow.com/a/30669632

template <size_t copy_amount, bool use_shuffle>
bool NO_INLINE decompressImpl(
     const char * const source,
     char * const dest,
     size_t source_size,
     size_t dest_size)
{
    const UInt8 * ip = reinterpret_cast<const UInt8 *>(source);
    UInt8 * op = reinterpret_cast<UInt8 *>(dest);
    const UInt8 * const input_end = ip + source_size;
    UInt8 * const output_begin = op;
    UInt8 * const output_end = op + dest_size;

    /// Unrolling with clang is doing >10% performance degrade.
#if defined(__clang__)
    #pragma nounroll
#endif
    while (true)
    {
        size_t length;

        auto continue_read_length = [&]
        {
            unsigned s;
            do
            {
                s = *ip++;
                length += s;
            } while (unlikely(s == 255 && ip < input_end));
        };

        /// Get literal length.

        if (unlikely(ip >= input_end))
            return false;

        const unsigned token = *ip++;
        length = token >> 4;
        if (length == 0x0F)
        {
            if (unlikely(ip + 1 >= input_end))
                return false;
            continue_read_length();
        }

        /// Copy literals.

        UInt8 * copy_end = op + length;

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

        if (unlikely(copy_end > output_end))
            return false;

        // Due to implementation specifics the copy length is always a multiple of copy_amount
        size_t real_length = 0;

        static_assert(copy_amount == 8 || copy_amount == 16 || copy_amount == 32);
        if constexpr (copy_amount == 8)
            real_length = (((length >> 3) + 1) * 8);
        else if constexpr (copy_amount == 16)
            real_length = (((length >> 4) + 1) * 16);
        else if constexpr (copy_amount == 32)
            real_length = (((length >> 5) + 1) * 32);

        if (unlikely(ip + real_length >= input_end + ADDITIONAL_BYTES_AT_END_OF_BUFFER))
             return false;

        wildCopy<copy_amount>(op, ip, copy_end);    /// Here we can write up to copy_amount - 1 bytes after buffer.

        if (copy_end == output_end)
            return true;

        ip += length;
        op = copy_end;

        if (unlikely(ip + 1 >= input_end))
            return false;

        /// Get match offset.

        size_t offset = unalignedLoad<UInt16>(ip);
        ip += 2;
        const UInt8 * match = op - offset;

        if (unlikely(match < output_begin))
            return false;

        /// Get match length.

        length = token & 0x0F;
        if (length == 0x0F)
        {
            if (unlikely(ip + 1 >= input_end))
                return false;
            continue_read_length();
        }
        length += 4;

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
        {
            if (unlikely(copy_end > output_end))
                return false;
            wildCopy<copy_amount>(op + copy_amount, match + copy_amount, copy_end);
        }

        op = copy_end;
    }
}

}


bool decompress(
    const char * const source,
    char * const dest,
    size_t source_size,
    size_t dest_size,
    PerformanceStatistics & statistics [[maybe_unused]])
{
    if (source_size == 0 || dest_size == 0)
        return true;

    /// Don't run timer if the block is too small.
    if (dest_size >= 32768)
    {
        size_t best_variant = statistics.select();

        /// Run the selected method and measure time.

        Stopwatch watch;
        bool success = true;
        if (best_variant == 0)
            success = decompressImpl<16, true>(source, dest, source_size, dest_size);
        if (best_variant == 1)
            success = decompressImpl<16, false>(source, dest, source_size, dest_size);
        if (best_variant == 2)
            success = decompressImpl<8, true>(source, dest, source_size, dest_size);
        if (best_variant == 3)
            success = decompressImpl<32, false>(source, dest, source_size, dest_size);

        watch.stop();

        /// Update performance statistics.

        statistics.data[best_variant].update(watch.elapsedSeconds(), dest_size);

        return success;
    }
    else
    {
        return decompressImpl<8, false>(source, dest, source_size, dest_size);
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
        << ", Avg literal length: " << static_cast<double>(sum_literal_lengths) / num_tokens
        << ", Avg match length: " << static_cast<double>(sum_match_lengths) / num_tokens
        << ", Avg match offset: " << static_cast<double>(sum_match_offsets) / num_tokens
        << ", Offset < 8 ratio: " << static_cast<double>(count_match_offset_less_8) / num_tokens
        << ", Offset < 16 ratio: " << static_cast<double>(count_match_offset_less_16) / num_tokens
        << ", Match replicate itself: " << static_cast<double>(count_match_replicate_itself) / num_tokens
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
    while (true)
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
