#include <Compression/LZ4_decompress_faster.h>

#include <base/defines.h>
#include <base/MemorySanitizer.h>
#include <base/types.h>
#include <base/unaligned.h>

#include <cstring>

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

inline UInt16 LZ4_readLE16(const void* mem_ptr)
{
    const UInt8* p = reinterpret_cast<const UInt8*>(mem_ptr);
    return static_cast<UInt16>(p[0]) + (p[1] << 8);
}

template <size_t N> [[maybe_unused]] inline void copy(UInt8 * dst, const UInt8 * src);
template <size_t N> [[maybe_unused]] inline void wildCopy(UInt8 * __restrict dst, const UInt8 * __restrict src, size_t size);
template <size_t N> [[maybe_unused]] inline void wildCopyFree(UInt8 * __restrict dst, const UInt8 * __restrict src, size_t size);
template <size_t N> [[maybe_unused]] inline void copyOverlap(UInt8 * op, const UInt8 *& match, size_t offset);

inline void copy8(UInt8 *   dst, const UInt8 *   src)
{
    memcpy(dst, src, 8);
}

inline void wildCopy8(UInt8 * dst, const UInt8 * src, size_t size)
{
    size_t i = 0;
    do
    {
        copy8(dst, src);
        dst += 8;
        src += 8;
        i += 8;
    } while (i < size);
}

inline void wildCopyFree8(UInt8 * __restrict dst, const UInt8 * __restrict src, size_t size)
{
    size_t i = 0;
    do
    {
        memcpy(dst, src, 8);
        dst += 8;
        src += 8;
        i += 8;
    } while (i < size);
}

void copyOverlap8(UInt8 * op, const UInt8 *& match, size_t offset)
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

#elifdef __aarch64__

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
    static constexpr int shift1[] = { 0, 1, 2, 1, 4, 4, 4, 4 };

    /// Shift amount for match pointer.
    static constexpr int shift2[] = { 0, 1, 2, 2, 4, 3, 2, 1 };

    op[0] = match[0];
    op[1] = match[1];
    op[2] = match[2];
    op[3] = match[3];

    memcpy(op + 4, match + shift1[offset], 4);
    match += shift2[offset];
#endif
}


template <> void inline copy<8>(UInt8 * dst, const UInt8 * src) { copy8(dst, src); }
template <> void inline wildCopy<8>(UInt8 * dst, const UInt8 * src, size_t size) { wildCopy8(dst, src, size); }
template <> void inline wildCopyFree<8>(UInt8 * __restrict dst, const UInt8 * __restrict src, size_t size) { wildCopyFree8(dst, src, size); }
template <> void inline copyOverlap<8>(UInt8 * op, const UInt8 *& match, const size_t offset) { copyOverlap8(op, match, offset); }

template <size_t copy_amount>
bool decompressImpl(const char * const source, char * const dest, size_t source_size, size_t dest_size)
{
    /// TODO: Clean up and leave 8 only
    static_assert(copy_amount == 8);
    const UInt8 * ip = reinterpret_cast<const UInt8 *>(source);
    UInt8 * op = reinterpret_cast<UInt8 *>(dest);
    const UInt8 * const input_end = ip + source_size;
    UInt8 * const output_begin = op;
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
            } while (unlikely(s == 255 && ip < input_end));
        };

        /// Get literal length.

        if (unlikely(ip >= input_end))
            return false;

        const unsigned token = *ip++;
        length = token >> 4;

        UInt8 * copy_end;
        size_t real_length;

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

        wildCopyFree<copy_amount>(op, ip, copy_end - op); /// Here we can write up to copy_amount - 1 bytes after buffer.

        if (copy_end == output_end)
            return true;

        ip += length;
        op = copy_end;

    decompress_match:

        if (unlikely(ip + 1 >= input_end))
            return false;

        /// Get match offset.

        size_t offset = LZ4_readLE16(ip);
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

        if (unlikely(copy_end > output_end))
            return false;

        /** Here we can write up to copy_amount - 1 - 4 * 2 bytes after buffer.
          * The worst case when offset = 1 and length = 4
          */

        if (offset < copy_amount)
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
            copy<copy_amount>(op, match);
            match += copy_amount;
        }

        op += copy_amount;

        copy<copy_amount>(op, match);   /// copy_amount + copy_amount - 1 - 4 * 2 bytes after buffer.
        if (length > copy_amount * 2)
        {
            if (unlikely(copy_end > output_end))
                return false;
            wildCopy<copy_amount>(op + copy_amount, match + copy_amount, copy_end - (op + copy_amount));
        }

        op = copy_end;
    }
}

}

bool decompress(
    const char * const source,
    char * const dest,
    size_t source_size,
    size_t dest_size)
{
    if (source_size == 0 || dest_size == 0)
        return true;

    return decompressImpl<8>(source, dest, source_size, dest_size);
}

}
