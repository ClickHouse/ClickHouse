#pragma once

#include <base/defines.h>
#include <base/types.h>
#include <base/extended_types.h>
#include <Common/SipHash.h>

#include <algorithm>
#include <array>

#if defined(__PCLMUL__)
#    include <immintrin.h>
#elif defined(__aarch64__) && defined(__ARM_FEATURE_AES)
#    include <arm_neon.h>
#endif


namespace DB
{

/// A 128-bit hash of strings with a random key, for in-memory use only: it is a polynomial hash over GF(2^128)
/// (as GHASH in AES-GCM, or POLYVAL in AES-GCM-SIV), the value at the key of the polynomial whose coefficients are the
/// 16-byte blocks of the string, and whose constant term is the length of the string.
///
/// For any two different strings of at most `n` blocks, the probability over the key that their hashes are equal is at
/// most n / 2^128: the difference of the polynomials is not zero (the constant terms differ if the lengths differ, and
/// some blocks differ otherwise), and its degree is at most n, so it has at most n roots. It holds whatever the strings are, as long as they are chosen without the knowledge of the
/// key: unlike a hash with a fixed key, its collisions cannot be found in advance. It is several times faster than
/// `SipHash` for short strings, because the blocks are multiplied by the powers of the key independently, with
/// carry-less multiplication (PCLMULQDQ, or PMULL on ARM).
///
/// The hash is linear over GF(2) in the bits of the string, so its bits are not uniformly distributed for structured
/// strings. Mix it with a bijection if it is used to select a bucket.
///
/// Without carry-less multiplication, it is `sipHash128`, and the key is not used.
class UniversalHash128
{
public:
#if defined(__PCLMUL__) || (defined(__aarch64__) && defined(__ARM_FEATURE_AES))
    static constexpr bool is_universal = true;
#else
    static constexpr bool is_universal = false;
#endif

    /// The key must be random, and not zero.
    explicit UniversalHash128(UInt128 key);

    /// The string must be followed by at least 15 readable bytes, as the values of the padded columns (`PaddedPODArray`).
    ALWAYS_INLINE UInt128 hash(const char * data, size_t size) const;

private:
#if defined(__PCLMUL__) || (defined(__aarch64__) && defined(__ARM_FEATURE_AES))
    /// The products of the blocks with the powers of the key are summed without reduction, and reduced once for up to
    /// this many blocks.
    static constexpr size_t MAX_BLOCKS_PER_REDUCTION = 8;

#if defined(__PCLMUL__)
    using Vector = __m128i;
#else
    using Vector = uint64x2_t;
#endif

    /// The powers of the key: key^1, ..., key^MAX_BLOCKS_PER_REDUCTION, and the XOR of the words of each of them.
    std::array<Vector, MAX_BLOCKS_PER_REDUCTION> powers{};
    std::array<Vector, MAX_BLOCKS_PER_REDUCTION> folded_powers{};
#endif
};


#if defined(__PCLMUL__) || (defined(__aarch64__) && defined(__ARM_FEATURE_AES))

namespace UniversalHash128Impl
{

#if defined(__PCLMUL__)

using Vector = __m128i;

ALWAYS_INLINE inline Vector load(const char * data) { return _mm_loadu_si128(reinterpret_cast<const __m128i *>(data)); }
ALWAYS_INLINE inline Vector bitXor(Vector a, Vector b) { return _mm_xor_si128(a, b); }
ALWAYS_INLINE inline Vector bitAnd(Vector a, Vector b) { return _mm_and_si128(a, b); }
ALWAYS_INLINE inline Vector zero() { return _mm_setzero_si128(); }
ALWAYS_INLINE inline Vector fromWords(UInt64 low, UInt64 high) { return _mm_set_epi64x(static_cast<Int64>(high), static_cast<Int64>(low)); }
/// The low word moved to the high one, and the high word moved to the low one, the other words are zero.
ALWAYS_INLINE inline Vector shiftUp(Vector a) { return _mm_slli_si128(a, 8); }
ALWAYS_INLINE inline Vector shiftDown(Vector a) { return _mm_srli_si128(a, 8); }
/// The carry-less products of the low or high words.
ALWAYS_INLINE inline Vector mulLowLow(Vector a, Vector b) { return _mm_clmulepi64_si128(a, b, 0x00); }
ALWAYS_INLINE inline Vector mulHighLow(Vector a, Vector b) { return _mm_clmulepi64_si128(a, b, 0x01); }
ALWAYS_INLINE inline Vector mulLowHigh(Vector a, Vector b) { return _mm_clmulepi64_si128(a, b, 0x10); }
ALWAYS_INLINE inline Vector mulHighHigh(Vector a, Vector b) { return _mm_clmulepi64_si128(a, b, 0x11); }
ALWAYS_INLINE inline UInt64 lowWord(Vector a) { return static_cast<UInt64>(_mm_cvtsi128_si64(a)); }
ALWAYS_INLINE inline UInt64 highWord(Vector a) { return static_cast<UInt64>(_mm_extract_epi64(a, 1)); }
/// The XOR of the two words, in the low word.
ALWAYS_INLINE inline Vector fold(Vector a) { return _mm_xor_si128(a, _mm_shuffle_epi32(a, 0x4E)); }
/// Each word shifted left by `bits`.
template <int bits> ALWAYS_INLINE inline Vector shiftWordsLeft(Vector a) { return _mm_slli_epi64(a, bits); }

#else

using Vector = uint64x2_t;

ALWAYS_INLINE inline Vector load(const char * data) { return vreinterpretq_u64_u8(vld1q_u8(reinterpret_cast<const uint8_t *>(data))); }
ALWAYS_INLINE inline Vector bitXor(Vector a, Vector b) { return veorq_u64(a, b); }
ALWAYS_INLINE inline Vector bitAnd(Vector a, Vector b) { return vandq_u64(a, b); }
ALWAYS_INLINE inline Vector zero() { return vdupq_n_u64(0); }
ALWAYS_INLINE inline Vector fromWords(UInt64 low, UInt64 high) { return vcombine_u64(vcreate_u64(low), vcreate_u64(high)); }
ALWAYS_INLINE inline Vector shiftUp(Vector a) { return vextq_u64(vdupq_n_u64(0), a, 1); }
ALWAYS_INLINE inline Vector shiftDown(Vector a) { return vextq_u64(a, vdupq_n_u64(0), 1); }
ALWAYS_INLINE inline Vector mul(UInt64 a, UInt64 b) { return vreinterpretq_u64_p128(vmull_p64(a, b)); }
ALWAYS_INLINE inline Vector mulLowLow(Vector a, Vector b) { return mul(vgetq_lane_u64(a, 0), vgetq_lane_u64(b, 0)); }
ALWAYS_INLINE inline Vector mulHighLow(Vector a, Vector b) { return mul(vgetq_lane_u64(a, 1), vgetq_lane_u64(b, 0)); }
ALWAYS_INLINE inline Vector mulLowHigh(Vector a, Vector b) { return mul(vgetq_lane_u64(a, 0), vgetq_lane_u64(b, 1)); }
ALWAYS_INLINE inline Vector mulHighHigh(Vector a, Vector b)
{
    return vreinterpretq_u64_p128(vmull_high_p64(vreinterpretq_p64_u64(a), vreinterpretq_p64_u64(b)));
}
ALWAYS_INLINE inline UInt64 lowWord(Vector a) { return vgetq_lane_u64(a, 0); }
ALWAYS_INLINE inline UInt64 highWord(Vector a) { return vgetq_lane_u64(a, 1); }
ALWAYS_INLINE inline Vector fold(Vector a) { return veorq_u64(a, vextq_u64(a, a, 1)); }
template <int bits> ALWAYS_INLINE inline Vector shiftWordsLeft(Vector a) { return vshlq_n_u64(a, bits); }

#endif

/// A carry-less product of 128-bit values, or a sum of them, before the reduction, by Karatsuba's method: the product
/// of `a` and `b` is low + middle * x^64 + high * x^128, where low = a0 * b0, high = a1 * b1, and
/// middle = (a0 + a1) * (b0 + b1) + low + high, so three multiplications are needed instead of four.
struct Product
{
    Vector low = zero();
    Vector folded = zero();
    Vector high = zero();

    /// `b_folded` is `fold(b)`.
    ALWAYS_INLINE void add(Vector a, Vector b, Vector b_folded)
    {
        low = bitXor(low, mulLowLow(a, b));
        high = bitXor(high, mulHighHigh(a, b));
        folded = bitXor(folded, mulLowLow(fold(a), b_folded));
    }

    /// Reduces modulo x^128 + x^7 + x^2 + x + 1: x^128 is replaced by x^7 + x^2 + x + 1 (0x87), twice, because the
    /// first replacement of the top word leaves up to 7 bits above x^128. The second time, the multiplication is done
    /// with shifts, because these bits fit into a word without overflowing it.
    ALWAYS_INLINE Vector reduce() const
    {
        const Vector middle = bitXor(folded, bitXor(low, high));
        const Vector product_low = bitXor(low, shiftUp(middle));
        const Vector product_high = bitXor(high, shiftDown(middle));

        const Vector polynomial = fromWords(0x87, 0);
        const Vector from_low_word = mulLowLow(product_high, polynomial);
        const Vector from_high_word = mulHighLow(product_high, polynomial);
        const Vector overflow = shiftDown(from_high_word);
        const Vector overflow_reduced = bitXor(
            bitXor(overflow, shiftWordsLeft<1>(overflow)), bitXor(shiftWordsLeft<2>(overflow), shiftWordsLeft<7>(overflow)));
        return bitXor(bitXor(product_low, from_low_word), bitXor(shiftUp(from_high_word), overflow_reduced));
    }
};

ALWAYS_INLINE inline Vector multiply(Vector a, Vector b)
{
    Product product;
    product.add(a, b, fold(b));
    return product.reduce();
}

/// The first `size` bytes are ones, the others are zeros, for `size` from 0 to 16.
inline constexpr std::array<UInt8, 32> masks = {
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

ALWAYS_INLINE inline Vector loadMasked(const char * data, size_t size)
{
    return bitAnd(load(data), load(reinterpret_cast<const char *>(masks.data()) + 16 - size));
}

}

inline UniversalHash128::UniversalHash128(UInt128 key)
{
    using namespace UniversalHash128Impl;
    chassert(key != 0);
    const Vector key_vector = fromWords(static_cast<UInt64>(key), static_cast<UInt64>(key >> 64));
    powers[0] = key_vector;
    for (size_t i = 1; i < powers.size(); ++i)
        powers[i] = multiply(powers[i - 1], key_vector);
    for (size_t i = 0; i < powers.size(); ++i)
        folded_powers[i] = fold(powers[i]);
}

ALWAYS_INLINE inline UInt128 UniversalHash128::hash(const char * data, size_t size) const
{
    using namespace UniversalHash128Impl;

    /// The polynomial is evaluated by Horner's method, up to `MAX_BLOCKS_PER_REDUCTION` blocks at once: the
    /// accumulated value is multiplied by key^count, and the blocks by the descending powers of the key.
    const size_t num_blocks = (size + 15) / 16;

    Vector accumulated = zero();
    for (size_t begin = 0; begin < num_blocks; begin += MAX_BLOCKS_PER_REDUCTION)
    {
        const size_t count = std::min(MAX_BLOCKS_PER_REDUCTION, num_blocks - begin);
        Product product;
        if (begin != 0)
            product.add(accumulated, powers[count - 1], folded_powers[count - 1]);

        for (size_t i = 0; i + 1 < count; ++i)
            product.add(load(data + (begin + i) * 16), powers[count - 1 - i], folded_powers[count - 1 - i]);

        const size_t last = begin + count - 1;
        const Vector last_block = last + 1 == num_blocks ? loadMasked(data + last * 16, size - last * 16) : load(data + last * 16);
        product.add(last_block, powers[0], folded_powers[0]);
        accumulated = product.reduce();
    }
    accumulated = bitXor(accumulated, fromWords(size, 0));

    return (UInt128(highWord(accumulated)) << 64) | lowWord(accumulated);
}

#else

inline UniversalHash128::UniversalHash128(UInt128) { }

ALWAYS_INLINE inline UInt128 UniversalHash128::hash(const char * data, size_t size) const
{
    return sipHash128Padded(data, size);
}

#endif

}
