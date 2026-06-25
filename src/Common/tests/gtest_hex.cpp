#include <Common/Hex.h>
#include <base/Decimal_fwd.h>
#include <base/types.h>
#include <city.h>

#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>


using namespace std::string_view_literals;

template <bool Lower, typename T>
static std::string hexer(T val)
{
    std::string res(2 * sizeof(T), ' ');
    Lower ? writeHexUIntLowercase(val, res.data()) : writeHexUIntUppercase(val, res.data());
    return res;
}

TEST(HexTest, HexingIntegral)
{
    ASSERT_EQ("1f", hexer<true>(UInt8{0x1f}));
    ASSERT_EQ("1F", hexer<false>(UInt8{0x1f}));
    ASSERT_EQ("012f", hexer<true>(UInt16{0x012f}));
    ASSERT_EQ("012F", hexer<false>(UInt16{0x012f}));
    ASSERT_EQ("0123456f", hexer<true>(UInt32{0x0123456f}));
    ASSERT_EQ("0123456F", hexer<false>(UInt32{0x0123456F}));
    ASSERT_EQ("0123456789abcdef", hexer<true>(UInt64{0x0123456789abcdef}));
    ASSERT_EQ("0123456789ABCDEF", hexer<false>(UInt64{0x0123456789ABCDEF}));
    UInt128 u128 = (UInt128{0x1020304050607080} << 64) | 0x90a0b0c0d0e0f000;
    ASSERT_EQ("102030405060708090a0b0c0d0e0f000", hexer<true>(u128));
    ASSERT_EQ("102030405060708090A0B0C0D0E0F000", hexer<false>(u128));
    UInt256 u256{u128};
    u256 = (u256 << 128) | (UInt128{0x0011223344556677} << 64) | 0x8899aabbccddeeff;
    ASSERT_EQ("102030405060708090a0b0c0d0e0f00000112233445566778899aabbccddeeff", hexer<true>(u256));
    ASSERT_EQ("102030405060708090A0B0C0D0E0F00000112233445566778899AABBCCDDEEFF", hexer<false>(u256));
}

TEST(HexTest, Hexing)
{
    constexpr auto input = "\x10\x20\x30\x40\x50\x60\x70\x80\x90\xA0\xB0\xC0\xD0\xE0\xF0"sv;
    ASSERT_EQ("102030405060708090a0b0c0d0e0f0", hexString(input.data(), input.size()));
}

template <typename T>
static T unhexer(std::string_view s)
{
    // NOLINTNEXTLINE(bugprone-suspicious-stringview-data-usage)
    return unhexUInt<T>(s.data());
}

TEST(HexTest, UnhexingIntegral)
{
    ASSERT_EQ(UInt8{0x1f}, unhexer<UInt8>("1f"));
    ASSERT_EQ(UInt8{0x1f}, unhexer<UInt8>("1F"));

    ASSERT_EQ(UInt16{0x012f}, unhexer<UInt16>("012f"));
    ASSERT_EQ(UInt16{0x012f}, unhexer<UInt16>("012F"));

    ASSERT_EQ(UInt32{0x0123456f}, unhexer<UInt32>("0123456f"));
    ASSERT_EQ(UInt32{0x0123456F}, unhexer<UInt32>("0123456F"));

    ASSERT_EQ(UInt64{0x0123456789abcdef}, unhexer<UInt64>("0123456789abcdef"));
    ASSERT_EQ(UInt64{0x0123456789ABCDEF}, unhexer<UInt64>("0123456789ABCDEF"));

    {
        UInt128 u128 = (UInt128{0x1020304050607080} << 64) | 0x90a0b0c0d0e0f000;
        ASSERT_EQ(u128, unhexer<UInt128>("102030405060708090a0b0c0d0e0f000"));
        ASSERT_EQ(u128, unhexer<UInt128>("102030405060708090A0B0C0D0E0F000"));
    }

    {
        UInt128 hi = (UInt128{0x1020304050607080} << 64) | 0x90a0b0c0d0e0f000;
        UInt128 lo = (UInt128{0x0011223344556677} << 64) | 0x8899aabbccddeeff;
        UInt256 u256{hi};
        u256 = (u256 << 128) | lo;

        ASSERT_EQ(u256, unhexer<UInt256>("102030405060708090a0b0c0d0e0f00000112233445566778899aabbccddeeff"));
        ASSERT_EQ(u256, unhexer<UInt256>("102030405060708090A0B0C0D0E0F00000112233445566778899AABBCCDDEEFF"));
    }
}

TEST(HexTest, UnhexSingleDigit)
{
    ASSERT_EQ(0, unhex('0'));
    ASSERT_EQ(9, unhex('9'));
    ASSERT_EQ(10, unhex('a'));
    ASSERT_EQ(15, unhex('f'));
    ASSERT_EQ(10, unhex('A'));
    ASSERT_EQ(15, unhex('F'));
}

TEST(HexTest, Unhex2)
{
    ASSERT_EQ(0x00, unhex2("00"));
    ASSERT_EQ(0xFF, unhex2("FF"));
    ASSERT_EQ(0xff, unhex2("ff"));
    ASSERT_EQ(0xAB, unhex2("AB"));
    ASSERT_EQ(0xab, unhex2("ab"));
    ASSERT_EQ(0x1F, unhex2("1F"));
}

TEST(HexTest, Unhex4)
{
    ASSERT_EQ(0x0000, unhex4("0000"));
    ASSERT_EQ(0xFFFF, unhex4("FFFF"));
    ASSERT_EQ(0xffff, unhex4("ffff"));
    ASSERT_EQ(0x1234, unhex4("1234"));
    ASSERT_EQ(0xABCD, unhex4("ABCD"));
    ASSERT_EQ(0xabcd, unhex4("abcd"));
}

TEST(HexTest, HexDigit)
{
    ASSERT_EQ('0', hexDigitUppercase(0));
    ASSERT_EQ('9', hexDigitUppercase(9));
    ASSERT_EQ('A', hexDigitUppercase(10));
    ASSERT_EQ('F', hexDigitUppercase(15));

    ASSERT_EQ('0', hexDigitLowercase(0));
    ASSERT_EQ('9', hexDigitLowercase(9));
    ASSERT_EQ('a', hexDigitLowercase(10));
    ASSERT_EQ('f', hexDigitLowercase(15));
}

TEST(HexTest, WriteHexByte)
{
    char buf[2];
    writeHexByteUppercase(0xAB, buf);
    ASSERT_EQ("AB", std::string_view(buf, 2));

    writeHexByteLowercase(0xAB, buf);
    ASSERT_EQ("ab", std::string_view(buf, 2));

    writeHexByteUppercase(0x00, buf);
    ASSERT_EQ("00", std::string_view(buf, 2));

    writeHexByteLowercase(0xFF, buf);
    ASSERT_EQ("ff", std::string_view(buf, 2));
}

TEST(HexTest, WriteBinByte)
{
    char buf[8];
    writeBinByte(0x00, buf);
    ASSERT_EQ("00000000", std::string_view(buf, 8));

    writeBinByte(0xFF, buf);
    ASSERT_EQ("11111111", std::string_view(buf, 8));

    writeBinByte(0xA5, buf);
    ASSERT_EQ("10100101", std::string_view(buf, 8));
}

TEST(HexTest, HexStringUppercase)
{
    constexpr auto input = "\x10\x20\x30\x40\x50\x60\x70\x80\x90\xA0\xB0\xC0\xD0\xE0\xF0"sv;

    std::string upper(input.size() * 2, '\0');
    hexString<false>(reinterpret_cast<UInt8 *>(upper.data()), input.data(), input.size());
    ASSERT_EQ("102030405060708090A0B0C0D0E0F0", upper);

    std::string lower(input.size() * 2, '\0');
    hexString<true>(reinterpret_cast<UInt8 *>(lower.data()), input.data(), input.size());
    ASSERT_EQ("102030405060708090a0b0c0d0e0f0", lower);
}

TEST(HexTest, HexStringRoundtrip)
{
    /// Exercise scalar path (< 16 bytes) and SIMD paths (>= 16 and >= 32 bytes).
    for (size_t size : {0, 1, 7, 15, 16, 17, 31, 32, 33, 64, 100})
    {
        std::vector<uint8_t> original(size);
        for (size_t i = 0; i < size; ++i)
            original[i] = static_cast<uint8_t>(i * 37 + 13);

        std::string encoded = hexString(original.data(), original.size());
        ASSERT_EQ(encoded.size(), size * 2);

        std::vector<uint8_t> decoded(size);
        unHexString(reinterpret_cast<UInt8 *>(decoded.data()), reinterpret_cast<const UInt8 *>(encoded.data()), size);
        ASSERT_EQ(original, decoded) << "roundtrip failed for size=" << size;
    }
}

TEST(HexTest, GetHexUInt)
{
    ASSERT_EQ("0123456789ABCDEF", getHexUIntUppercase(UInt64{0x0123456789ABCDEF}));
    ASSERT_EQ("0123456789abcdef", getHexUIntLowercase(UInt64{0x0123456789abcdef}));
}

TEST(HexTest, CityHashUInt128Roundtrip)
{
    CityHash_v1_0_2::uint128 original;
    original.high64 = 0x0123456789ABCDEF;
    original.low64  = 0xFEDCBA9876543210;

    /// Encode upper and lower, verify hex string has high64 first.
    auto upper = getHexUIntUppercase(original);
    ASSERT_EQ("0123456789ABCDEFFEDCBA9876543210", upper);

    auto lower = getHexUIntLowercase(original);
    ASSERT_EQ("0123456789abcdeffedcba9876543210", lower);

    /// Roundtrip: unhex the encoded string back and compare.
    auto decoded = unhexUInt<CityHash_v1_0_2::uint128>(upper.data());
    ASSERT_EQ(original.high64, decoded.high64);
    ASSERT_EQ(original.low64, decoded.low64);
}


/// ============================================================
/// Target-specific tests: exercise each architecture path
/// independently, so the scalar path is validated even on
/// machines that support AVX2 (and vice versa).
/// ============================================================

#define HEX_GTEST_UNIT_TEST
#include "Common/Hex.cpp" // NOLINT(bugprone-suspicious-include)

namespace
{

struct DefaultHexTrait
{
    static void skipIfUnsupported() { }

    static void encodeString(uint8_t * dst, const uint8_t * src, size_t size)
    {
        TargetSpecific::Default::encodeHexStringImpl(dst, src, size, heks::lower);
    }

    static void decodeString(uint8_t * dst, const uint8_t * src, size_t size)
    {
        TargetSpecific::Default::decodeHexStringImpl(dst, src, size);
    }

    static void encodeInt(uint8_t * dst, const void * value, size_t num_bytes)
    {
        TargetSpecific::Default::encodeHexIntImpl(dst, value, num_bytes, heks::lower);
    }

    static UInt64 decodeInt64(const uint8_t * src)
    {
        return TargetSpecific::Default::decodeHexInt64Impl(src);
    }

    static void encodeHex16LE(uint8_t * dst, const uint8_t * src)
    {
        TargetSpecific::Default::encodeHex16LEImpl(dst, src, heks::lower);
    }
};

#if USE_MULTITARGET_CODE && (defined(__x86_64__) || defined(_M_X64))

struct X86V3HexTrait
{
    static void skipIfUnsupported()
    {
        if (!DB::isArchSupported(DB::TargetArch::x86_64_v3))
            GTEST_SKIP() << "x86_64_v3 (AVX2) not supported on this host";
    }

    static void encodeString(uint8_t * dst, const uint8_t * src, size_t size)
    {
        TargetSpecific::x86_64_v3::encodeHexStringImpl(dst, src, size, heks::lower);
    }

    static void decodeString(uint8_t * dst, const uint8_t * src, size_t size)
    {
        TargetSpecific::x86_64_v3::decodeHexStringImpl(dst, src, size);
    }

    static void encodeInt(uint8_t * dst, const void * value, size_t num_bytes)
    {
        TargetSpecific::x86_64_v3::encodeHexIntImpl(dst, value, num_bytes, heks::lower);
    }

    static UInt64 decodeInt64(const uint8_t * src)
    {
        return TargetSpecific::x86_64_v3::decodeHexInt64Impl(src);
    }

    static void encodeHex16LE(uint8_t * dst, const uint8_t * src)
    {
        TargetSpecific::x86_64_v3::encodeHex16LEImpl(dst, src, heks::lower);
    }
};

#endif

} // namespace


template <typename T>
class HexMultiArchTest : public ::testing::Test
{
protected:
    void SetUp() override { T::skipIfUnsupported(); }
};

using HexImplementations = ::testing::Types<
    DefaultHexTrait
#if USE_MULTITARGET_CODE && (defined(__x86_64__) || defined(_M_X64))
    , X86V3HexTrait
#endif
    >;

TYPED_TEST_SUITE(HexMultiArchTest, HexImplementations);


TYPED_TEST(HexMultiArchTest, StringEncodeDecodeRoundtrip)
{
    for (size_t size : {0, 1, 7, 15, 16, 17, 31, 32, 33, 64, 100})
    {
        SCOPED_TRACE("size=" + std::to_string(size));

        std::vector<uint8_t> original(size);
        for (size_t i = 0; i < size; ++i)
            original[i] = static_cast<uint8_t>(i * 37 + 13);

        std::vector<uint8_t> encoded(size * 2);
        TypeParam::encodeString(encoded.data(), original.data(), size);

        std::vector<uint8_t> decoded(size);
        TypeParam::decodeString(decoded.data(), encoded.data(), size);

        ASSERT_EQ(original, decoded);
    }
}

TYPED_TEST(HexMultiArchTest, IntegralUInt64)
{
    UInt64 val = 0x0123456789ABCDEF;
    uint8_t hex_buf[16];
    TypeParam::encodeInt(hex_buf, &val, 8);
    ASSERT_EQ("0123456789abcdef", std::string_view(reinterpret_cast<const char *>(hex_buf), 16));

    UInt64 decoded = TypeParam::decodeInt64(hex_buf);
    ASSERT_EQ(val, decoded);
}

TYPED_TEST(HexMultiArchTest, IntegralUInt128)
{
    UInt128 val = (UInt128{0x1020304050607080} << 64) | 0x90a0b0c0d0e0f000;
    uint8_t hex_buf[32];
    TypeParam::encodeInt(hex_buf, &val, 16);
    ASSERT_EQ("102030405060708090a0b0c0d0e0f000", std::string_view(reinterpret_cast<const char *>(hex_buf), 32));
}

TYPED_TEST(HexMultiArchTest, IntegralUInt256)
{
    UInt128 hi = (UInt128{0x1020304050607080} << 64) | 0x90a0b0c0d0e0f000;
    UInt256 val{hi};
    val = (val << 128) | ((UInt128{0x0011223344556677} << 64) | 0x8899aabbccddeeff);
    uint8_t hex_buf[64];
    TypeParam::encodeInt(hex_buf, &val, 32);
    ASSERT_EQ(
        "102030405060708090a0b0c0d0e0f00000112233445566778899aabbccddeeff",
        std::string_view(reinterpret_cast<const char *>(hex_buf), 64));
}

TYPED_TEST(HexMultiArchTest, EncodeHex16LE)
{
    /// CityHash uint128 layout: low64 at bytes [0..7], high64 at bytes [8..15].
    uint8_t input[16];
    UInt64 low = 0xFEDCBA9876543210;
    UInt64 high = 0x0123456789ABCDEF;
    memcpy(input, &low, 8);
    memcpy(input + 8, &high, 8);

    uint8_t hex_buf[32];
    TypeParam::encodeHex16LE(hex_buf, input);

    /// Output should be hex(high64) followed by hex(low64), both MSB-first.
    ASSERT_EQ("0123456789abcdeffedcba9876543210", std::string_view(reinterpret_cast<const char *>(hex_buf), 32));
}
