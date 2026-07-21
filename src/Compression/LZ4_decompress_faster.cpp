#include <Compression/LZ4_decompress_faster.h>

#include <base/defines.h>
#include <base/MemorySanitizer.h>
#include <base/types.h>
#include <base/unaligned.h>
#include <Common/Stopwatch.h>

#include <cstring>

#ifdef __SSSE3__
#include <tmmintrin.h>
#endif

#if defined(__aarch64__) && defined(__ARM_NEON)
#include <arm_neon.h>
#endif


namespace LZ4
{

namespace
{

ALWAYS_INLINE UInt16 LZ4_readLE16(const void * mem_ptr)
{
    return unalignedLoadLittleEndian<UInt16>(mem_ptr);
}

template <size_t block_size>
ALWAYS_INLINE void copyFromOutput(UInt8 * dst, UInt8 * src)
{
    /// Match copies may overlap (src points into the already-decoded output).
    /// When offset < 32, a 256-bit load reads bytes just written in the same
    /// cache line, causing store-forwarding stalls.  Two 128-bit copies halve
    /// the stall window.
    if constexpr (block_size == 32)
    {
        __builtin_memcpy(dst, src, 16);
        __builtin_memcpy(dst + 16, src + 16, 16);
    }
    else
        __builtin_memcpy(dst, src, block_size);
}

template <size_t block_size>
ALWAYS_INLINE void wildCopyFromInput(UInt8 * __restrict dst, const UInt8 * __restrict src, size_t size)
{
    /// Unrolling with clang is doing >10% performance degrade on x86.
    /// On ARM (Graviton 4) the pragma has no measurable effect.
    size_t i = 0;
    #pragma nounroll
    do
    {
        __builtin_memcpy(dst, src, block_size);
        dst += block_size;
        src += block_size;
        i += block_size;
    } while (i < size);
}


template <size_t block_size>
ALWAYS_INLINE void wildCopyFromOutput(UInt8 * dst, const UInt8 * src, size_t size)
{
    /// Unrolling with clang is doing >10% performance degrade on x86.
    /// On ARM (Graviton 4) the pragma has no measurable effect.
    ///
    /// Same store-forwarding concern as `copyFromOutput`.
    size_t i = 0;
    #pragma nounroll
    do
    {
        if constexpr (block_size == 32)
        {
            __builtin_memcpy(dst, src, 16);
            __builtin_memcpy(dst + 16, src + 16, 16);
        }
        else
            __builtin_memcpy(dst, src, block_size);
        dst += block_size;
        src += block_size;
        i += block_size;
    } while (i < size);
}

template <size_t block_size>
void ALWAYS_INLINE copyOverlap(UInt8 * op, UInt8 *& match, size_t offset);

template <>
[[maybe_unused]] void ALWAYS_INLINE copyOverlap<8>(UInt8 * op, UInt8 *& match, size_t offset)
{
#if defined(__SSSE3__)

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

        std::vector<int> vec;                              // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::unordered_set<int> set(vec.begin(), vec.end()); // STYLE_CHECK_ALLOW_STD_CONTAINERS

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

    /// MSAN does not recognize the store as initializing the memory
    __msan_unpoison(op, 16);

    match += masks[offset];

#elif defined(__aarch64__) && defined(__ARM_NEON)

    /// Single `vtbl1_u8` is a native 8-byte D-register operation on ARM — measured 6% faster
    /// than the scalar fallback on Graviton 4 (geo mean 0.940x across test.hits columns).
    /// Unlike `copyOverlap<16>` and `copyOverlap<32>` where NEON `vtbl2_u8` was slower
    /// than scalar due to needing multiple calls, this single-instruction path is a clear win.
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

#else
    /// 4 % n.
    /// Or if 4 % n is zero, we use n.
    /// It gives equivalent result, but is better CPU friendly for unknown reason.
    static constexpr UInt8 shift1[] = {0, 1, 2, 1, 4, 4, 4, 4};

    /// Shift amount for match pointer.
    static constexpr UInt8 shift2[] = {0, 1, 2, 2, 4, 3, 2, 1};

    op[0] = match[0];
    op[1] = match[1];
    op[2] = match[2];
    op[3] = match[3];

    memcpy(op + 4, match + shift1[offset], 4);
    match += shift2[offset];
#endif
}

template <>
[[maybe_unused]] void ALWAYS_INLINE copyOverlap<32>(UInt8 * op, UInt8 *& match, size_t offset)
{
#if defined(__SSSE3__)

    /** In LZ4, when offset < copy_amount, the source and destination overlap during copying.
      * This creates a repeating pattern effect. For example:
      *   - If offset=1 and we copy 32 bytes, byte[0] repeats 32 times
      *   - If offset=2 and we copy 32 bytes, bytes[0,1] repeat 16 times
      *   - If offset=3 and we copy 32 bytes, bytes[0,1,2] repeat ~10.67 times
      *
      * The shuffle masks efficiently implement this pattern replication using SIMD instructions.
      * Each row represents the shuffle pattern for a specific offset value (1-15).
      */

    /// Shuffle masks for the first 16 output bytes: masks_lo[offset][i] = i % offset.
    /// This table generates indices for selecting bytes from the source pattern to fill positions 0-15.
    /// For offset=2: [0,1,0,1,0,1,0,1,0,1,0,1,0,1,0,1] means alternate between byte[0] and byte[1].
    /// For offset=3: [0,1,2,0,1,2,0,1,2,0,1,2,0,1,2,0] means cycle through bytes[0,1,2].
    /// For offset=5: [0,1,2,3,4,0,1,2,3,4,0,1,2,3,4,0] means repeat the 5-byte pattern.
    static constexpr UInt8 __attribute__((__aligned__(16))) masks_lo[] =
    {
        0,  1,  2,  1,  4,  1,  4,  2,  8,  7,  6,  5,  4,  3,  2,  1, /* offset = 0, not used as mask */
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

    /// Shuffle masks for the second 16 output bytes: masks_hi[offset][i] = (16 + i) % offset.
    /// This table continues the pattern replication for positions 16-31.
    /// The starting index is (16 % offset) to maintain pattern continuity from masks_lo.
    /// For offset=2: Still [0,1,0,1,...] because 16%2=0, pattern restarts.
    /// For offset=3: [1,2,0,1,2,0,...] because 16%3=1, pattern continues from index 1.
    /// For offset=5: [1,2,3,4,0,1,2,3,4,0,...] because 16%5=1, pattern continues from index 1.
    /// For offset=9: [7,8,0,1,2,3,4,5,6,7,8,0,...] because 16%9=7, pattern continues from index 7.
    static constexpr UInt8 __attribute__((__aligned__(16))) masks_hi[] =
    {
        0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, /* offset = 0, not used as mask */
        0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, /* offset = 1 */
        0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,
        1,  2,  0,  1,  2,  0,  1,  2,  0,  1,  2,  0,  1,  2,  0,  1,
        0,  1,  2,  3,  0,  1,  2,  3,  0,  1,  2,  3,  0,  1,  2,  3,
        1,  2,  3,  4,  0,  1,  2,  3,  4,  0,  1,  2,  3,  4,  0,  1,
        4,  5,  0,  1,  2,  3,  4,  5,  0,  1,  2,  3,  4,  5,  0,  1,
        2,  3,  4,  5,  6,  0,  1,  2,  3,  4,  5,  6,  0,  1,  2,  3,
        0,  1,  2,  3,  4,  5,  6,  7,  0,  1,  2,  3,  4,  5,  6,  7,
        7,  8,  0,  1,  2,  3,  4,  5,  6,  7,  8,  0,  1,  2,  3,  4,
        6,  7,  8,  9,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  0,  1,
        5,  6,  7,  8,  9, 10,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9,
        4,  5,  6,  7,  8,  9, 10, 11,  0,  1,  2,  3,  4,  5,  6,  7,
        3,  4,  5,  6,  7,  8,  9, 10, 11, 12,  0,  1,  2,  3,  4,  5,
        2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13,  0,  1,  2,  3,
        1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,  0,  1,
    };

    /// How many bytes to advance the match pointer after copying 32 bytes.
    /// This equals (32 % offset) when offset divides evenly, or the offset itself otherwise.
    /// For offset=2: shifts[2]=2 because we copied the 2-byte pattern 16 times, advance by 2.
    /// For offset=3: shifts[3]=2 because 32%3=2, the pattern repeated 10 full times plus 2 bytes.
    /// For offset=5: shifts[5]=2 because 32%5=2, the pattern repeated 6 full times plus 2 bytes.
    /// For offset=16: shifts[16]=16 because we copied exactly 16 bytes twice, advance by 16.
    /// For offset=17-31: shifts[n] counts down from 15 to 1, representing 32%n for each offset.
    static constexpr UInt8 shifts[]
        = {0, 1, 2, 2, 4, 2, 2, 4, 8, 5, 2, 10, 8, 6, 4, 2, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};

    if (offset >= 16)
    {
        /// The second 16 bytes may overlap with op (e.g. offset=16 means match+16 == op),
        /// so the first store must complete before the second load.
        _mm_storeu_si128(reinterpret_cast<__m128i *>(op),
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(match)));

        _mm_storeu_si128(reinterpret_cast<__m128i *>(op + 16),
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(match + 16)));
    }
    else
    {
        /// Load the source pattern once. Both shuffles reference only indices 0..offset-1,
        /// so any bytes beyond the pattern length in the register are never selected.
        __m128i src = _mm_loadu_si128(reinterpret_cast<const __m128i *>(match));

        _mm_storeu_si128(reinterpret_cast<__m128i *>(op),
            _mm_shuffle_epi8(src, _mm_load_si128(reinterpret_cast<const __m128i *>(masks_lo) + offset)));

        _mm_storeu_si128(reinterpret_cast<__m128i *>(op + 16),
            _mm_shuffle_epi8(src, _mm_load_si128(reinterpret_cast<const __m128i *>(masks_hi) + offset)));

        __msan_unpoison(op, 32);
    }

    match += shifts[offset];

    /// Note: an ARM NEON path using two `vqtbl1q_u8` calls (Q-register 16-byte table lookup)
    /// was tested on Graviton 4 but showed no improvement over the scalar fallback (geo mean
    /// 1.0000x across test.hits columns). The previous `vtbl2_u8` (D-register) path was also
    /// slower due to needing four calls per 32 bytes.

#else
    /** Fallback implementation without SIMD instructions.
      * Copies 32 bytes in four stages: 4 + 4 + 8 + 16 bytes.
      * Each stage copies from an offset computed to maintain the repeating pattern.
      */

    /// Where to read from for the second 4 bytes (positions 4-7): equals (4 % offset) when that's non-zero, else 4.
    /// For offset=2: shift1[2]=2, so we read from match+2, which wraps around to match[0,1] again.
    /// For offset=3: shift1[3]=1, so we read from match+1, continuing the pattern [0,1,2,0,1,2,...].
    /// For offset>=4: shift1[n]=4, so we read the next 4 fresh bytes.
    static constexpr UInt8 shift1[] = {0, 1, 2, 1, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4};

    /// Where to read from for the next 8 bytes (positions 8-15): equals (8 % offset) when that's non-zero, else 8.
    /// For offset=2: shift2[2]=2, wrapping the 2-byte pattern.
    /// For offset=3: shift2[3]=2, because 8%3=2, continuing from the 2nd byte of the pattern.
    /// For offset=5: shift2[5]=3, because 8%5=3, continuing from the 3rd byte of the pattern.
    /// For offset>=8: shift2[n]=8 or (8%n), reading fresh or wrapped bytes as appropriate.
    static constexpr UInt8 shift2[] = {0, 1, 2, 2, 4, 3, 2, 1, 0, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8};

    /// Where to read from for the next 16 bytes (positions 16-31): equals (16 % offset) when that's non-zero, else 16.
    /// For offset=3: shift3[3]=1, because 16%3=1.
    /// For offset=5: shift3[5]=1, because 16%5=1.
    /// For offset=9: shift3[9]=7, because 16%9=7.
    /// For offset>=16: shift3[n]=16, reading the next fresh 16 bytes without wrapping.
    static constexpr UInt8 shift3[]
        = {0, 1, 2, 1, 4, 1, 4, 2, 8, 7, 6, 5, 4, 3, 2, 1, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16};

    /// How far to advance match pointer after copying all 32 bytes: equals (32 % offset).
    /// This determines the final position after all four copy stages complete.
    /// Same values as the SSSE3 shifts table.
    static constexpr UInt8 shift4[]
        = {0, 1, 2, 2, 4, 2, 2, 4, 8, 5, 2, 10, 8, 6, 4, 2, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};

    /// Copy the first 4 bytes directly.
    op[0] = match[0];
    op[1] = match[1];
    op[2] = match[2];
    op[3] = match[3];

    /// Copy in stages, each reading from a position that maintains the repeating pattern.
    memcpy(op + 4, match + shift1[offset], 4);
    memcpy(op + 8, match + shift2[offset], 8);
    memcpy(op + 16, match + shift3[offset], 16);
    match += shift4[offset];
#endif
}


/// Branchless small-offset bootstrap masks (SSSE3), indexed by min(offset, copy_amount).
/// One unconditional `pshufb` writes the first `copy_amount` bytes correctly for ANY
/// offset (identity for offset >= copy_amount), and advancing `match` by `shift[idx]`
/// makes the effective offset >= copy_amount, so the remaining copies are non-overlapping.
template <size_t copy_amount>
struct BootMasks
{
    /// The inner dimension and the `pshufb` / `vqtbl1q_u8` shuffle it feeds are 16 bytes wide, so the
    /// generated masks are only correct for a 16-byte copy. Only `boot_masks<16>` is ever used; guard
    /// against a future `boot_masks<8>` / `<32>` silently getting wrong masks.
    static_assert(copy_amount == 16, "BootMasks is only valid for a 16-byte copy amount");

    alignas(16) UInt8 boot[copy_amount + 1][16];
    UInt8 shift[copy_amount + 1];
    constexpr BootMasks() : boot{}, shift{}
    {
        for (size_t idx = 1; idx <= copy_amount; ++idx)
        {
            for (size_t i = 0; i < 16; ++i)
                boot[idx][i] = (idx == copy_amount) ? static_cast<UInt8>(i) : static_cast<UInt8>(i % idx);
            shift[idx] = (idx == copy_amount)
                ? static_cast<UInt8>(copy_amount)
                : static_cast<UInt8>((copy_amount % idx) ? (copy_amount % idx) : idx);
        }
    }
};

template <size_t copy_amount>
inline constexpr BootMasks<copy_amount> boot_masks{};

/// The 16-byte small-offset overlap fill, via one unconditional table-lookup shuffle using the
/// constexpr `boot_masks` table. Here `offset` is the table index: callers on the branchless path
/// pass `min(real_offset, 16)`, so an index of 16 selects the identity shuffle (a plain copy).
template <>
[[maybe_unused]] void ALWAYS_INLINE copyOverlap<16>(UInt8 * op, UInt8 *& match, size_t offset)
{
#if defined(__SSSE3__)
    _mm_storeu_si128(reinterpret_cast<__m128i *>(op),
        _mm_shuffle_epi8(
            _mm_loadu_si128(reinterpret_cast<const __m128i *>(match)),
            _mm_load_si128(reinterpret_cast<const __m128i *>(boot_masks<16>.boot[offset]))));
    __msan_unpoison(op, 16);
    match += boot_masks<16>.shift[offset];
#elif defined(__aarch64__) && defined(__ARM_NEON)
    /// A non-branchless `vqtbl1q_u8` copyOverlap<16> was previously measured on Graviton 4 to be no
    /// better than the scalar fallback (geo mean 1.0000x on test.hits): the `offset < 16` branch it
    /// still carried was the bottleneck, not the copy. This branchless variant removes that branch, so
    /// the table lookup finally pays off — forced-variant-1 on Graviton 4 (armv8.2-a) vs the scalar
    /// 16-byte path: RegionID +27%, ResolutionWidth +28%, UserID +9%.
    unalignedStore<uint8x16_t>(op,
        vqtbl1q_u8(unalignedLoad<uint8x16_t>(match), unalignedLoad<uint8x16_t>(boot_masks<16>.boot[offset])));
    __msan_unpoison(op, 16);
    match += boot_masks<16>.shift[offset];
#else
    /// Scalar fallback (no SSSE3/NEON): reached only via the branched call path, where `offset` is
    /// the real offset < 16.
    static constexpr UInt8 shift1[] = {0, 1, 2, 1, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4};
    static constexpr UInt8 shift2[] = {0, 1, 2, 2, 4, 3, 2, 1, 0, 8, 8, 8, 8, 8, 8, 8};
    static constexpr UInt8 shift3[] = {0, 1, 2, 1, 4, 1, 4, 2, 8, 7, 6, 5, 4, 3, 2, 1};
    op[0] = match[0];
    op[1] = match[1];
    op[2] = match[2];
    op[3] = match[3];
    memcpy(op + 4, match + shift1[offset], 4);
    memcpy(op + 8, match + shift2[offset], 8);
    match += shift3[offset];
#endif
}

template <size_t copy_amount, bool branchless = false>
bool NO_INLINE decompressImpl(const char * const source, char * const dest, size_t source_size, size_t dest_size)
{
    const UInt8 * ip = reinterpret_cast<const UInt8 *>(source);
    UInt8 * op = reinterpret_cast<UInt8 *>(dest);
    const UInt8 * const input_end = ip + source_size;
    UInt8 * const output_begin = op;
    UInt8 * const output_end = op + dest_size;

    while (true)
    {
        size_t length = 0;

        auto continue_read_length = [&]
        {
            unsigned s = 0;
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

        UInt8 * copy_end = nullptr;
        size_t real_length = 0;

        /// It might be true fairly often for well-compressed columns.
        /// ATST it may hurt performance in other cases because this condition is hard to predict (especially if the number of zeros is ~50%).
        /// In such cases this `if` will significantly increase number of mispredicted instructions. But seems like it results in a
        /// noticeable slowdown only for implementations with `copy_amount` > 8. Probably because they use havier instructions.
        if constexpr (copy_amount == 8)
            if (length == 0)
                goto decompress_match;

        if (length == 0x0F)
        {
            if (unlikely(ip + 1 >= input_end))
                return false;
            continue_read_length();
        }

        /// Copy literals.

        copy_end = op + length;

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
        real_length = 0;

        static_assert(copy_amount == 8 || copy_amount == 16 || copy_amount == 32);
        if constexpr (copy_amount == 8)
            real_length = (((length >> 3) + 1) * 8);
        else if constexpr (copy_amount == 16)
            real_length = (((length >> 4) + 1) * 16);
        else if constexpr (copy_amount == 32)
            real_length = (((length >> 5) + 1) * 32);

        if (unlikely(ip + real_length >= input_end + ADDITIONAL_BYTES_AT_END_OF_BUFFER))
            return false;

        wildCopyFromInput<copy_amount>(op, ip, copy_end - op); /// Here we can write up to copy_amount - 1 bytes after buffer.

        if (copy_end == output_end)
            return true;

        ip += length;
        op = copy_end;

    decompress_match:

        if (unlikely(ip + 1 >= input_end))
            return false;

        /// Get match offset.

        const size_t offset = LZ4_readLE16(ip);
        ip += 2;

        /// Reject a zero offset (invalid in LZ4 — the minimum match distance is 1) together with an
        /// offset that reaches before the start of the output, in a single unsigned comparison that
        /// adds no branch to the hot loop. For any `offset >= 1` this is exactly `match < output_begin`
        /// (`match = op - offset`); for `offset == 0`, `offset - 1` wraps to `SIZE_MAX` and also fails,
        /// so the overlap copy can no longer synthesize output from the destination and wrongly report
        /// success. We fail closed here (the reference decoder is more lenient and returns garbage).
        /// The check is done before forming `match`, so `op - offset` is never computed for a malformed
        /// offset that would point before `output_begin` (which would be out-of-range pointer arithmetic).
        const size_t produced = static_cast<size_t>(op - output_begin);
        if (unlikely(offset - 1 >= produced))
            return false;

        UInt8 * match = op - offset;

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

        if (unlikely(copy_end > output_end))
            return false;

        /** Here we can write up to copy_amount - 1 - 4 * 2 bytes after buffer.
          * The worst case when offset = 1 and length = 4
          */

#if defined(__SSSE3__) || (defined(__aarch64__) && defined(__ARM_NEON))
        if constexpr (branchless && copy_amount == 16)
            /// Unconditional branchless overlap copy: clamp the index here (one `cmov`) so that
            /// `copyOverlap<16>` selects the identity shuffle for offset >= 16, dropping the
            /// highly mispredicted `offset < 16` branch.
            copyOverlap<copy_amount>(op, match, offset < copy_amount ? offset : copy_amount);
        else if (offset < copy_amount)
#else
        if (offset < copy_amount)
#endif
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

            copyOverlap<copy_amount>(op, match, offset);
        }
        else
        {
            copyFromOutput<copy_amount>(op, match);
            match += copy_amount;
        }

        op += copy_amount;

        copyFromOutput<copy_amount>(op, match);   /// copy_amount + copy_amount - 1 - 4 * 2 bytes after buffer.
        if (length > copy_amount * 2)
        {
            if (unlikely(copy_end > output_end))
                return false;
            wildCopyFromOutput<copy_amount>(op + copy_amount, match + copy_amount, copy_end - (op + copy_amount));
        }

        op = copy_end;
    }
}

}

/** Three variants of the decompression loop are instantiated, differing in copy granularity:
  *   variant 0: `decompressImpl<8>`  — copies 8 bytes at a time
  *   variant 1: `decompressImpl<16>` — copies 16 bytes at a time
  *   variant 2: `decompressImpl<32>` — copies 32 bytes at a time
  *
  * No single variant is universally fastest. On x86 with SSSE3:
  * - Variant 0 has the lowest per-iteration overhead (wins most files by count).
  * - Variant 1 matches the natural 16-byte `pshufb` width (best aggregate throughput).
  * - Variant 2 amortizes two `pshufb` ops per iteration (wins on large, high-repetition blocks).
  *
  * Measured on AMD 7950X3D with test.hits columns (175 files, 10 iterations each, min taken):
  *   variant 0:  total 1,099M cycles — best for 57% of files
  *   variant 1:  total   972M cycles — best for 26% of files (best single variant overall)
  *   variant 2:  total 1,079M cycles — best for 17% of files
  *   oracle:     total   869M cycles — per-file best, 10.6% faster than always-variant-1
  *
  * The adaptive bandit algorithm converges toward the oracle, making all three variants useful.
  */
bool decompress(
    const char * const source,
    char * const dest,
    size_t source_size,
    size_t dest_size,
    [[maybe_unused]] PerformanceStatistics & statistics)
{
    if (source_size == 0 || dest_size == 0)
        return true;

    /// When a specific method is forced, always use it regardless of block size.
    /// The size threshold below only applies to the adaptive bandit algorithm
    /// where timing very small blocks would add too much noise.
    if (statistics.choose_method >= 0 || dest_size >= 32768)
    {
        size_t variant_size = PerformanceStatistics::NUM_ELEMENTS;
        size_t best_variant = statistics.select(variant_size);

        Stopwatch watch;
        bool success = false;
        if (best_variant == 0)
            success = decompressImpl<8>(source, dest, source_size, dest_size);
        else if (best_variant == 1)
            success = decompressImpl<16, true>(source, dest, source_size, dest_size);
        else
            success = decompressImpl<32>(source, dest, source_size, dest_size);

        watch.stop();
        statistics.data[best_variant].update(watch.elapsedSeconds(), static_cast<double>(dest_size));  // NOLINT(clang-analyzer-security.ArrayBound)

        return success;
    }

    /// For small blocks (< 32 KiB), skip the bandit and always use variant 0 (8-byte copies).
    /// Timing such small blocks would add too much noise to the bandit's statistics.
    /// Variant 0 is the right default here: it has the lowest per-iteration overhead
    /// and wins the majority of files (57% on test.hits), especially smaller ones.
    /// Note: the exact threshold value is not performance-sensitive. On test.hits columns
    /// (AMD 7950X3D), sweeping it from 0 to 32K changes the oracle total by only 0.003%,
    /// because variant 0 is available in the bandit anyway and would be selected for
    /// the same files. The threshold is purely a noise-reduction measure.
    return decompressImpl<8>(source, dest, source_size, dest_size);
}

}
