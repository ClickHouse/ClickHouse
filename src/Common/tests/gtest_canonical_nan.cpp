#include <gtest/gtest.h>

#include <bit>
#include <cmath>

#include <Common/NaNUtils.h>
#include <base/types.h>
#include <base/BFloat16.h>


/// `canonicalNaN<T>` must return a fixed IEEE 754 positive quiet NaN bit pattern
/// for each supported floating-point type, regardless of the libc/libstdc++ build.
/// `uniqExact` and similar set aggregates serialize keys byte-wise without
/// re-canonicalizing on read, so a stable cross-platform canonical form is
/// required for correctness of distributed merges. See issue #105748.

TEST(CanonicalNaN, Float32BitPattern)
{
    /// IEEE 754 binary32 positive quiet NaN: sign=0, exp=0xFF, mantissa MSB=1.
    EXPECT_EQ(std::bit_cast<UInt32>(canonicalNaN<Float32>()), UInt32{0x7FC00000});
}

TEST(CanonicalNaN, Float64BitPattern)
{
    /// IEEE 754 binary64 positive quiet NaN: sign=0, exp=0x7FF, mantissa MSB=1.
    EXPECT_EQ(std::bit_cast<UInt64>(canonicalNaN<Float64>()), UInt64{0x7FF8000000000000});
}

TEST(CanonicalNaN, BFloat16BitPattern)
{
    /// `bf16` positive quiet NaN: sign=0, exp=0xFF, mantissa MSB=1.
    EXPECT_EQ(std::bit_cast<UInt16>(canonicalNaN<BFloat16>()), UInt16{0x7FC0});
}

TEST(CanonicalNaN, IsNaN)
{
    /// Sanity check: the returned values are NaN according to `isNaN`.
    EXPECT_TRUE(isNaN(canonicalNaN<Float32>()));
    EXPECT_TRUE(isNaN(canonicalNaN<Float64>()));
    EXPECT_TRUE(isNaN(canonicalNaN<BFloat16>()));
}

TEST(CanonicalizeNaN, FoldsAllNaNBitPatternsFloat32)
{
    /// Different NaN producers can yield different bit patterns. After canonicalization
    /// they must all collapse onto a single representation.
    const Float32 positive_qnan = std::bit_cast<Float32>(UInt32{0x7FC00000});
    const Float32 negative_qnan = std::bit_cast<Float32>(UInt32{0xFFC00000});
    const Float32 positive_qnan_payload = std::bit_cast<Float32>(UInt32{0x7FC12345});
    const Float32 signaling_nan = std::bit_cast<Float32>(UInt32{0x7F800001});

    const UInt32 expected = 0x7FC00000;
    EXPECT_EQ(std::bit_cast<UInt32>(canonicalizeNaN(positive_qnan)), expected);
    EXPECT_EQ(std::bit_cast<UInt32>(canonicalizeNaN(negative_qnan)), expected);
    EXPECT_EQ(std::bit_cast<UInt32>(canonicalizeNaN(positive_qnan_payload)), expected);
    EXPECT_EQ(std::bit_cast<UInt32>(canonicalizeNaN(signaling_nan)), expected);
}

TEST(CanonicalizeNaN, FoldsAllNaNBitPatternsFloat64)
{
    const Float64 positive_qnan = std::bit_cast<Float64>(UInt64{0x7FF8000000000000});
    const Float64 negative_qnan = std::bit_cast<Float64>(UInt64{0xFFF8000000000000});
    const Float64 positive_qnan_payload = std::bit_cast<Float64>(UInt64{0x7FF8123456789ABC});
    const Float64 signaling_nan = std::bit_cast<Float64>(UInt64{0x7FF0000000000001});

    const UInt64 expected = 0x7FF8000000000000;
    EXPECT_EQ(std::bit_cast<UInt64>(canonicalizeNaN(positive_qnan)), expected);
    EXPECT_EQ(std::bit_cast<UInt64>(canonicalizeNaN(negative_qnan)), expected);
    EXPECT_EQ(std::bit_cast<UInt64>(canonicalizeNaN(positive_qnan_payload)), expected);
    EXPECT_EQ(std::bit_cast<UInt64>(canonicalizeNaN(signaling_nan)), expected);
}

TEST(CanonicalizeNaN, FoldsAllNaNBitPatternsBFloat16)
{
    const BFloat16 positive_qnan = BFloat16::fromBits(0x7FC0);
    const BFloat16 negative_qnan = BFloat16::fromBits(0xFFC0);
    const BFloat16 positive_qnan_payload = BFloat16::fromBits(0x7FC1);
    const BFloat16 signaling_nan = BFloat16::fromBits(0x7F81);

    const UInt16 expected = 0x7FC0;
    EXPECT_EQ(std::bit_cast<UInt16>(canonicalizeNaN(positive_qnan)), expected);
    EXPECT_EQ(std::bit_cast<UInt16>(canonicalizeNaN(negative_qnan)), expected);
    EXPECT_EQ(std::bit_cast<UInt16>(canonicalizeNaN(positive_qnan_payload)), expected);
    EXPECT_EQ(std::bit_cast<UInt16>(canonicalizeNaN(signaling_nan)), expected);
}

TEST(CanonicalizeNaN, PreservesNonNaNValues)
{
    /// Non-NaN floats must be returned unchanged.
    EXPECT_EQ(canonicalizeNaN(Float32{1.0f}), 1.0f);
    EXPECT_EQ(canonicalizeNaN(Float32{0.0f}), 0.0f);
    EXPECT_EQ(canonicalizeNaN(Float32{-1.5f}), -1.5f);
    EXPECT_EQ(std::bit_cast<UInt32>(canonicalizeNaN(std::bit_cast<Float32>(UInt32{0x7F800000}))), UInt32{0x7F800000}); /// +inf
    EXPECT_EQ(std::bit_cast<UInt32>(canonicalizeNaN(std::bit_cast<Float32>(UInt32{0xFF800000}))), UInt32{0xFF800000}); /// -inf

    EXPECT_EQ(canonicalizeNaN(Float64{1.0}), 1.0);
    EXPECT_EQ(canonicalizeNaN(Float64{0.0}), 0.0);
    EXPECT_EQ(canonicalizeNaN(Float64{-1.5}), -1.5);
}

TEST(CanonicalizeNaN, IsNoOpForIntegerTypes)
{
    /// For non-floating-point types `canonicalizeNaN` is a compile-time no-op.
    EXPECT_EQ(canonicalizeNaN(UInt32{42}), UInt32{42});
    EXPECT_EQ(canonicalizeNaN(Int64{-7}), Int64{-7});
    EXPECT_EQ(canonicalizeNaN(UInt16{0xFFFF}), UInt16{0xFFFF});
}
