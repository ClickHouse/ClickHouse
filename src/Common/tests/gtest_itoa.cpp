#include <charconv>
#include <cstring>
#include <string>
#include <vector>

#include <base/itoa.h>
#include <gtest/gtest.h>

namespace
{

/// The obvious, slow implementation, used as the reference for `writeFixedDigits`.
template <typename T>
std::string referenceFixedDigits(T value, UInt32 width)
{
    std::string result(width, '0');
    for (Int32 pos = static_cast<Int32>(width) - 1; pos >= 0; --pos)
    {
        result[pos] = static_cast<char>('0' + static_cast<int>(value % 10));
        value /= 10;
    }
    return result;
}

template <typename T>
std::string callItoa(T value)
{
    /// Deliberately more room than needed: `itoa` is allowed to write a few bytes past the end.
    char buf[128];
    memset(buf, '#', sizeof(buf));
    char * end = itoa(value, buf);
    return std::string(buf, static_cast<size_t>(end - buf));
}

template <typename T>
std::string callWriteFixedDigits(T value, UInt32 width)
{
    char buf[128];
    memset(buf, '#', sizeof(buf));
    char * end = writeFixedDigits(value, width, buf + 8);

    /// Exactly `width` bytes must be written: nothing before, nothing after.
    EXPECT_EQ(end, buf + 8 + width);
    for (size_t i = 0; i < 8; ++i)
        EXPECT_EQ(buf[i], '#') << "wrote in front of the buffer, width " << width;
    for (size_t i = 8 + width; i < sizeof(buf); ++i)
        EXPECT_EQ(buf[i], '#') << "wrote past the end of the buffer, width " << width;

    return std::string(buf + 8, width);
}

/// Runs `body` once with the AVX-512 implementation enabled (if the CPU supports it at all) and
/// once with it disabled, so that both implementations are covered by every test.
template <typename F>
void forBothImplementations(F && body)
{
    setUseAVX512ItoaForTests(true);
    if (getUseAVX512ItoaForTests())
    {
        body("AVX-512");
        setUseAVX512ItoaForTests(false);
    }
    body("portable");
    setUseAVX512ItoaForTests(true);
}

/// The obvious, slow implementation, used as the reference for `itoa` of the wide types.
template <typename T>
std::string referenceItoa(T value)
{
    using Unsigned = make_unsigned_t<T>;

    bool negative = value < 0;
    Unsigned magnitude = negative ? Unsigned(0) - Unsigned(value) : Unsigned(value);

    std::string digits;
    do
    {
        digits += static_cast<char>('0' + static_cast<int>(magnitude % 10));
        magnitude /= 10;
    } while (magnitude != 0);

    if (negative)
        digits += '-';

    return std::string(digits.rbegin(), digits.rend());
}

std::vector<UInt64> interestingUInt64Values()
{
    std::vector<UInt64> values{0, 1, 9, 10, 11, 99, 100, 101, std::numeric_limits<UInt64>::max()};

    /// The boundaries between the branches of every implementation.
    for (UInt64 power = 1; power <= 10000000000000000000ULL; power *= 10)
    {
        values.push_back(power - 1);
        values.push_back(power);
        values.push_back(power + 1);
        if (power > std::numeric_limits<UInt64>::max() / 10)
            break;
    }

    /// A deterministic pseudo-random sample of every length.
    UInt64 state = 0x9E3779B97F4A7C15ULL;
    for (int i = 0; i < 20000; ++i)
    {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        values.push_back(state >> (i % 64));
    }
    return values;
}

}

TEST(Itoa, UInt64MatchesToChars)
{
    const auto values = interestingUInt64Values();
    forBothImplementations([&](const char * implementation)
    {
        for (UInt64 value : values)
        {
            char reference[24];
            auto [ptr, ec] = std::to_chars(reference, reference + sizeof(reference), value);
            ASSERT_EQ(ec, std::errc{});
            ASSERT_EQ(callItoa(value), std::string(reference, static_cast<size_t>(ptr - reference)))
                << implementation << ", value " << value;
        }
    });
}

TEST(Itoa, Int64MatchesToChars)
{
    const auto values = interestingUInt64Values();
    forBothImplementations([&](const char * implementation)
    {
        for (UInt64 unsigned_value : values)
        {
            Int64 value = static_cast<Int64>(unsigned_value);
            char reference[24];
            auto [ptr, ec] = std::to_chars(reference, reference + sizeof(reference), value);
            ASSERT_EQ(ec, std::errc{});
            ASSERT_EQ(callItoa(value), std::string(reference, static_cast<size_t>(ptr - reference)))
                << implementation << ", value " << value;
        }
        ASSERT_EQ(callItoa(std::numeric_limits<Int64>::min()), "-9223372036854775808") << implementation;
    });
}

TEST(Itoa, WideIntegers)
{
    forBothImplementations([&](const char * implementation)
    {
        ASSERT_EQ(callItoa(UInt128{0}), "0") << implementation;
        ASSERT_EQ(callItoa(std::numeric_limits<UInt128>::max()), "340282366920938463463374607431768211455") << implementation;
        ASSERT_EQ(callItoa(std::numeric_limits<Int128>::min()), "-170141183460469231731687303715884105728") << implementation;
        ASSERT_EQ(callItoa(std::numeric_limits<UInt256>::max()),
                  "115792089237316195423570985008687907853269984665640564039457584007913129639935") << implementation;
        ASSERT_EQ(callItoa(std::numeric_limits<Int256>::min()),
                  "-57896044618658097711785492504343953926634992332820282019728792003956564819968") << implementation;

        /// Values just above the point where the implementation switches to a wider type.
        UInt128 above_uint64 = UInt128(std::numeric_limits<UInt64>::max()) + 1;
        ASSERT_EQ(callItoa(above_uint64), "18446744073709551616") << implementation;
        UInt256 above_uint128 = UInt256(std::numeric_limits<UInt128>::max()) + 1;
        ASSERT_EQ(callItoa(above_uint128), "340282366920938463463374607431768211456") << implementation;

        /// A value whose 18-digit blocks contain leading zeros, which must not be dropped.
        ASSERT_EQ(callItoa(UInt128(1000000000000000000ULL) * 1000000000000000000ULL), "1000000000000000000000000000000000000")
            << implementation;
    });
}

TEST(Itoa, WideIntegersMatchReference)
{
    /// `itoa` of a wide value is assembled from a leading part plus blocks of 18 digits, so it
    /// needs values of every length, on both sides of every block boundary.
    std::vector<UInt256> values{0, 1, 9, 10};

    UInt256 power = 1;
    for (int i = 0; i < 77; ++i)
    {
        values.push_back(power - 1);
        values.push_back(power);
        values.push_back(power + 1);
        power *= 10;
    }
    values.push_back(std::numeric_limits<UInt256>::max());
    values.push_back(UInt256(std::numeric_limits<UInt128>::max()));
    values.push_back(UInt256(std::numeric_limits<UInt64>::max()));

    UInt64 state = 0xB7E151628AED2A6BULL;
    auto next = [&]
    {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        return state;
    };
    for (int i = 0; i < 5000; ++i)
    {
        UInt256 value = 0;
        for (int limb = 0; limb < 4; ++limb)
            value = (value << 64) + UInt256(next());
        /// Truncate to a random length so that every digit count is covered.
        values.push_back(value >> (i % 256));
    }

    forBothImplementations([&](const char * implementation)
    {
        for (const UInt256 & value : values)
        {
            ASSERT_EQ(callItoa(value), referenceItoa(value)) << implementation;
            ASSERT_EQ(callItoa(Int256(value)), referenceItoa(Int256(value))) << implementation;

            UInt128 narrow{value.items[UInt256::_impl::little(0)], value.items[UInt256::_impl::little(1)]};
            ASSERT_EQ(callItoa(narrow), referenceItoa(narrow)) << implementation;
            ASSERT_EQ(callItoa(Int128(narrow)), referenceItoa(Int128(narrow))) << implementation;
        }
    });
}

TEST(Itoa, WriteFixedDigitsUInt64)
{
    const auto values = interestingUInt64Values();
    forBothImplementations([&](const char * implementation)
    {
        for (UInt32 width = 0; width <= 20; ++width)
        {
            for (UInt64 value : values)
            {
                ASSERT_EQ(callWriteFixedDigits(value, width), referenceFixedDigits(value, width))
                    << implementation << ", value " << value << ", width " << width;
            }
        }

        /// A Decimal256 column may have a scale far above the number of digits of its value.
        for (UInt32 width : {21u, 38u, 39u, 57u, 76u, 77u})
        {
            ASSERT_EQ(callWriteFixedDigits(UInt64(0), width), referenceFixedDigits(UInt64(0), width)) << implementation;
            ASSERT_EQ(callWriteFixedDigits(UInt64(1), width), referenceFixedDigits(UInt64(1), width)) << implementation;
            ASSERT_EQ(callWriteFixedDigits(std::numeric_limits<UInt64>::max(), width),
                      referenceFixedDigits(std::numeric_limits<UInt64>::max(), width)) << implementation;
        }
    });
}

TEST(Itoa, WriteFixedDigitsWideIntegers)
{
    std::vector<UInt128> values_128{
        UInt128(0),
        UInt128(1),
        UInt128(std::numeric_limits<UInt64>::max()),
        UInt128(std::numeric_limits<UInt64>::max()) + 1,
        std::numeric_limits<UInt128>::max(),
        UInt128(1000000000000000000ULL) * 1000000000000000000ULL,
    };

    UInt64 state = 0x243F6A8885A308D3ULL;
    for (int i = 0; i < 2000; ++i)
    {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        UInt64 low = state;
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        values_128.push_back((UInt128(state) << 64) + low);
    }

    forBothImplementations([&](const char * implementation)
    {
        for (UInt32 width : {0u, 1u, 8u, 17u, 18u, 19u, 20u, 36u, 37u, 38u, 39u, 76u})
        {
            for (const UInt128 & value : values_128)
            {
                ASSERT_EQ(callWriteFixedDigits(value, width), referenceFixedDigits(value, width))
                    << implementation << ", width " << width;
                ASSERT_EQ(callWriteFixedDigits(UInt256(value), width), referenceFixedDigits(UInt256(value), width))
                    << implementation << ", width " << width;
            }

            UInt256 huge = (UInt256(std::numeric_limits<UInt128>::max()) << 64) + 12345;
            ASSERT_EQ(callWriteFixedDigits(huge, width), referenceFixedDigits(huge, width)) << implementation << ", width " << width;
            ASSERT_EQ(callWriteFixedDigits(std::numeric_limits<UInt256>::max(), width),
                      referenceFixedDigits(std::numeric_limits<UInt256>::max(), width)) << implementation << ", width " << width;
        }
    });
}
