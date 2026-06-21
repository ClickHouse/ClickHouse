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
