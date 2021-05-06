#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <type_traits>
#include <initializer_list>

#include <Core/Types.h>
#include <IO/WriteHelpers.h>
#include <common/demangle.h>


static_assert(is_signed_v<Int128>);
static_assert(!is_unsigned_v<Int128>);
static_assert(is_integer_v<Int128>);
static_assert(sizeof(Int128) == 16);

static_assert(is_signed_v<Int256>);
static_assert(!is_unsigned_v<Int256>);
static_assert(is_integer_v<Int256>);
static_assert(sizeof(Int256) == 32);

static_assert(!is_signed_v<UInt128>);
static_assert(is_unsigned_v<UInt128>);
static_assert(is_integer_v<UInt128>);
static_assert(sizeof(UInt128) == 16);

static_assert(!is_signed_v<UInt256>);
static_assert(is_unsigned_v<UInt256>);
static_assert(is_integer_v<UInt256>);
static_assert(sizeof(UInt256) == 32);


using namespace DB;


GTEST_TEST(WideInteger, Conversions)
{
    ASSERT_EQ(toString(UInt128(12345678901234567890ULL)), "12345678901234567890");
    ASSERT_EQ(toString(UInt256(12345678901234567890ULL)), "12345678901234567890");

    Int128 minus_one = -1;
    ASSERT_EQ(minus_one.items[0], -1);
    ASSERT_EQ(minus_one.items[1], -1);

    ASSERT_EQ(0, memcmp(&minus_one, "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", sizeof(minus_one)));

    ASSERT_EQ(minus_one, -1);
    ASSERT_EQ(minus_one, -1LL);
    ASSERT_EQ(minus_one, Int8(-1));
    ASSERT_EQ(minus_one, Int16(-1));
    ASSERT_EQ(minus_one, Int32(-1));
    ASSERT_EQ(minus_one, Int64(-1));

    ASSERT_LT(minus_one, 0);

    Int128 zero = 0;
    zero += -1;
    ASSERT_EQ(zero, -1);
    ASSERT_EQ(zero, minus_one);

    zero += minus_one;
    ASSERT_EQ(0, memcmp(&zero, "\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", sizeof(zero)));

    zero += 2;
    ASSERT_EQ(zero, 0);

    ASSERT_EQ(toString(Int128(-1)), "-1");
    ASSERT_EQ(toString(Int256(-1)), "-1");

    ASSERT_EQ(toString(Int128(-1LL)), "-1");
    ASSERT_EQ(toString(Int256(-1LL)), "-1");

    ASSERT_EQ(toString(Int128(-1234567890123456789LL)), "-1234567890123456789");
    ASSERT_EQ(toString(Int256(-1234567890123456789LL)), "-1234567890123456789");

    ASSERT_EQ(UInt64(UInt128(12345678901234567890ULL)), 12345678901234567890ULL);
    ASSERT_EQ(UInt64(UInt256(12345678901234567890ULL)), 12345678901234567890ULL);

    ASSERT_EQ(__uint128_t(UInt128(12345678901234567890ULL)), 12345678901234567890ULL);
    ASSERT_EQ(__uint128_t(UInt256(12345678901234567890ULL)), 12345678901234567890ULL);

    ASSERT_EQ(__int128_t(Int128(-1234567890123456789LL)), -1234567890123456789LL);
    ASSERT_EQ(__int128_t(Int256(-1234567890123456789LL)), -1234567890123456789LL);

    ASSERT_EQ(toString(Int128(-1)), "-1");
    ASSERT_EQ(toString(Int256(-1)), "-1");

    ASSERT_EQ(toString(UInt128(123.456)), "123");
    ASSERT_EQ(toString(UInt256(123.456)), "123");
    ASSERT_EQ(toString(Int128(-123.456)), "-123");
    ASSERT_EQ(toString(Int256(-123.456)), "-123");

    ASSERT_EQ(toString(UInt128(123.456f)), "123");
    ASSERT_EQ(toString(UInt256(123.456f)), "123");
    ASSERT_EQ(toString(Int128(-123.456f)), "-123");
    ASSERT_EQ(toString(Int256(-123.456f)), "-123");

    ASSERT_EQ(toString(UInt128(1) * 1000000000 * 1000000000 * 1000000000 * 1000000000), "1000000000000000000000000000000000000");
    ASSERT_EQ(Float64(UInt128(1) * 1000000000 * 1000000000 * 1000000000 * 1000000000), 1e36);

    ASSERT_EQ(toString(UInt256(1) * 1000000000 * 1000000000 * 1000000000 * 1000000000 * 1000000000 * 1000000000 * 1000000000 * 1000000000),
        "1000000000000000000000000000000000000000000000000000000000000000000000000");
    ASSERT_EQ(Float64(UInt256(1) * 1000000000 * 1000000000 * 1000000000 * 1000000000 * 1000000000 * 1000000000 * 1000000000 * 1000000000), 1e72);
}


GTEST_TEST(WideInteger, Arithmetic)
{
    Int128 minus_one = -1;
    Int128 zero = 0;

    zero += -1;
    ASSERT_EQ(zero, -1);
    ASSERT_EQ(zero, minus_one);

    zero += minus_one;
    ASSERT_EQ(0, memcmp(&zero, "\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", sizeof(zero)));

    zero += 2;
    ASSERT_EQ(zero, 0);

    ASSERT_EQ(UInt256(12345678901234567890ULL) * 12345678901234567890ULL / 12345678901234567890ULL, 12345678901234567890ULL);
    ASSERT_EQ(UInt256(12345678901234567890ULL) * UInt256(12345678901234567890ULL) / 12345678901234567890ULL, 12345678901234567890ULL);
    ASSERT_EQ(UInt256(12345678901234567890ULL) * 12345678901234567890ULL / UInt256(12345678901234567890ULL), 12345678901234567890ULL);
    ASSERT_EQ(UInt256(12345678901234567890ULL) * 12345678901234567890ULL / 12345678901234567890ULL, UInt256(12345678901234567890ULL));
    ASSERT_EQ(UInt128(12345678901234567890ULL) * 12345678901234567890ULL / UInt128(12345678901234567890ULL), 12345678901234567890ULL);
    ASSERT_EQ(UInt256(12345678901234567890ULL) * UInt128(12345678901234567890ULL) / 12345678901234567890ULL, 12345678901234567890ULL);

    ASSERT_EQ(Int128(0) + Int32(-1), Int128(-1));
}


GTEST_TEST(WideInteger, DecimalArithmetic)
{
    Decimal128 zero{};
    Decimal32 addend = -1000;

    zero += Decimal128(addend);
    ASSERT_EQ(zero.value, -1000);

    zero += addend;
    ASSERT_EQ(zero.value, -2000);
}
