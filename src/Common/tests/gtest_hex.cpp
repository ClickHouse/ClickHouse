#include <base/hex.h>
#include <base/Decimal_fwd.h>
#include <base/types.h>

#include <string>
#include <string_view>

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
