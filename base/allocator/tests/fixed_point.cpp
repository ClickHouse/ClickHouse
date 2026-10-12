#include <allocator/FixedPoint.h>

#include "Test.h"

#include <cstring>

using namespace jemalloc;

/// Expected values are taken from the C implementation (`fxp_parse`, `fxp_print`).
TEST(FixedPoint, Parse)
{
    struct Case
    {
        const char * input;
        bool error;
        FixedPoint result;
        ptrdiff_t end;
    };
    const Case cases[] = {
        {"0", false, 0, 1},
        {"1", false, 65536, 1},
        {"1.5", false, 98304, 3},
        {".75", false, 49152, 3},
        {"0.1", false, 6553, 3},
        {"0.01", false, 655, 4},
        {"123.456", false, 8090812, 7},
        {"65535", false, 4294901760u, 5},
        {"65535.99999999999999999", false, 4294967295u, 23},
        {"0.001953125", false, 128, 11},
        {".00001", false, 0, 6},
        {"12a", false, 786432, 2},
        {"3.14159265358979323846", false, 205887, 22},
        {"65536", true, 0, 0},
        {"1.", true, 0, 0},
        {"x", true, 0, 0},
        {"123.", true, 0, 0},
        {"3.a", true, 0, 0},
        {".a", true, 0, 0},
        {"a.1", true, 0, 0},
        {"123456789", true, 0, 0},
        {"0000000123456789", true, 0, 0},
        {"1000000", true, 0, 0},
    };
    for (const auto & c : cases)
    {
        FixedPoint result = 0xdeadbeef;
        const char * end = nullptr;
        bool error = fixed_point::parse(&result, c.input, &end);
        CHECK_EQ(error, c.error);
        if (c.error)
        {
            CHECK_EQ(result, 0xdeadbeefu);
            CHECK(end == nullptr);
        }
        else
        {
            CHECK_EQ(result, c.result);
            CHECK_EQ(end - c.input, c.end);
        }
    }
    FixedPoint result;
    CHECK(!fixed_point::parse(&result, "2.5", static_cast<const char **>(nullptr)));
    CHECK_EQ(result, fixed_point::initInt(5) / 2);
}

TEST(FixedPoint, Print)
{
    struct Case
    {
        FixedPoint value;
        const char * expected;
    };
    const Case cases[] = {
        {0, "0.0"},
        {1, "0.00001525878906"},
        {2, "0.00003051757812"},
        {10, "0.00015258789062"},
        {32768, "0.5"},
        {65536, "1.0"},
        {98304, "1.5"},
        {6553, "0.09999084472656"},
        {6554, "0.10000610351562"},
        {655, "0.00999450683593"},
        {65, "0.00099182128906"},
        {7, "0.00010681152343"},
        {4294967295u, "65535.99998474121093"},
        {305419896, "4660.3377685546875"},
        {49152, "0.75"},
        {128, "0.001953125"},
    };
    for (const auto & c : cases)
    {
        char buf[fixed_point::BUF_SIZE];
        fixed_point::print(c.value, buf);
        CHECK_STREQ(buf, c.expected);
    }
}

TEST(FixedPoint, Arithmetic)
{
    static_assert(fixed_point::initInt(3) == 3u << 16);
    static_assert(fixed_point::initPercent(100) == fixed_point::initInt(1));
    static_assert(fixed_point::initPercent(75) == 49152);
    static_assert(fixed_point::initPercent(1) == 655);
    CHECK_EQ(fixed_point::add(fixed_point::initInt(1), fixed_point::initInt(2)), fixed_point::initInt(3));
    CHECK_EQ(fixed_point::sub(fixed_point::initInt(3), fixed_point::initInt(1)), fixed_point::initInt(2));
    CHECK_EQ(fixed_point::multiply(98304, 98304), 147456u); /// 1.5 * 1.5 = 2.25
    CHECK_EQ(fixed_point::divide(fixed_point::initInt(3), fixed_point::initInt(2)), 98304u);
    CHECK_EQ(fixed_point::divide(fixed_point::initInt(123), fixed_point::initInt(456)), uint32_t((uint64_t(123) << 32) / 456 >> 16));
    CHECK_EQ(fixed_point::roundDown(98304), 1u);
    CHECK_EQ(fixed_point::roundNearest(98304), 2u);
    CHECK_EQ(fixed_point::roundNearest(98303), 1u);
    CHECK_EQ(fixed_point::roundNearest(0x8000), 1u);
    CHECK_EQ(fixed_point::roundNearest(0x7fff), 0u);
    CHECK_EQ(fixed_point::multiplyByFraction(1000, 32768), 500u);
    CHECK_EQ(fixed_point::multiplyByFraction((size_t(1) << 48) - 1, 65536), (size_t(1) << 48) - 1);
    CHECK_EQ(fixed_point::multiplyByFraction(size_t(1) << 48, 32768), size_t(1) << 47);
    CHECK_EQ(fixed_point::multiplyByFraction((size_t(1) << 50) + 65535, 32768), size_t(1) << 49);
}
