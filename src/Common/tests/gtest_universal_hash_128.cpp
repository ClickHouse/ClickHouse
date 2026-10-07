#include <Common/UniversalHash128.h>

#include <random>
#include <vector>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

/// Multiplication in GF(2^128) modulo x^128 + x^7 + x^2 + x + 1, bit by bit.
UInt128 referenceMultiply(UInt128 a, UInt128 b)
{
    UInt128 result = 0;
    for (size_t i = 0; i < 128; ++i)
    {
        if (((b >> i) & 1) != 0)
            result ^= a;
        const bool carry = ((a >> 127) & 1) != 0;
        a <<= 1;
        if (carry)
            a ^= 0x87;
    }
    return result;
}

/// The value at the key of the polynomial with the 16-byte blocks of the string (the last one padded with zeros) as the
/// coefficients from the highest degree to degree 1, and the length of the string as the constant term.
UInt128 referenceHash(UInt128 key, const char * data, size_t size)
{
    UInt128 accumulated = 0;
    for (size_t begin = 0; begin < size; begin += 16)
    {
        UInt128 block = 0;
        memcpy(&block, data + begin, std::min<size_t>(16, size - begin));
        accumulated = referenceMultiply(accumulated ^ block, key);
    }
    return accumulated ^ UInt128(size);
}

}

TEST(UniversalHash128, MatchesReference)
{
    if constexpr (!UniversalHash128::is_universal)
        GTEST_SKIP() << "No carry-less multiplication";

    std::mt19937_64 rng(42);
    std::vector<char> buffer(400 + 15);
    for (int iteration = 0; iteration < 10; ++iteration)
    {
        const UInt128 key = (UInt128(rng()) << 64) | rng() | 1;
        const UniversalHash128 hash(key);
        for (auto & c : buffer)
            c = static_cast<char>(rng());

        for (size_t offset = 0; offset < 4; ++offset)
            for (size_t size = 0; size <= 400 - offset; ++size)
                EXPECT_EQ(referenceHash(key, buffer.data() + offset, size), hash.hash(buffer.data() + offset, size))
                    << "offset=" << offset << " size=" << size;
    }
}

/// The length is a part of the hash, so the strings that differ only by trailing zero bytes do not collide.
TEST(UniversalHash128, TrailingZeros)
{
    const UniversalHash128 hash((UInt128(0x0123456789ABCDEFULL) << 64) | 0xFEDCBA9876543210ULL);
    std::vector<char> zeros(64 + 15);
    for (size_t size = 0; size < 64; ++size)
        EXPECT_NE(hash.hash(zeros.data(), size), hash.hash(zeros.data(), size + 1)) << "size=" << size;
}
