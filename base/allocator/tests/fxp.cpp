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
        bool error = fxp::parse(&result, c.input, &end);
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
    CHECK(!fxp::parse(&result, "2.5", static_cast<const char **>(nullptr)));
    CHECK_EQ(result, fxp::initInt(5) / 2);
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
        char buf[fxp::BUF_SIZE];
        fxp::print(c.value, buf);
        CHECK_STREQ(buf, c.expected);
    }
}

TEST(FixedPoint, Arithmetic)
{
    static_assert(fxp::initInt(3) == 3u << 16);
    static_assert(fxp::initPercent(100) == fxp::initInt(1));
    static_assert(fxp::initPercent(75) == 49152);
    static_assert(fxp::initPercent(1) == 655);
    CHECK_EQ(fxp::add(fxp::initInt(1), fxp::initInt(2)), fxp::initInt(3));
    CHECK_EQ(fxp::sub(fxp::initInt(3), fxp::initInt(1)), fxp::initInt(2));
    CHECK_EQ(fxp::mul(98304, 98304), 147456u); /// 1.5 * 1.5 = 2.25
    CHECK_EQ(fxp::div(fxp::initInt(3), fxp::initInt(2)), 98304u);
    CHECK_EQ(fxp::div(fxp::initInt(123), fxp::initInt(456)), uint32_t((uint64_t(123) << 32) / 456 >> 16));
    CHECK_EQ(fxp::roundDown(98304), 1u);
    CHECK_EQ(fxp::roundNearest(98304), 2u);
    CHECK_EQ(fxp::roundNearest(98303), 1u);
    CHECK_EQ(fxp::roundNearest(0x8000), 1u);
    CHECK_EQ(fxp::roundNearest(0x7fff), 0u);
    CHECK_EQ(fxp::mulFrac(1000, 32768), 500u);
    CHECK_EQ(fxp::mulFrac((size_t(1) << 48) - 1, 65536), (size_t(1) << 48) - 1);
    CHECK_EQ(fxp::mulFrac(size_t(1) << 48, 32768), size_t(1) << 47);
    CHECK_EQ(fxp::mulFrac((size_t(1) << 50) + 65535, 32768), size_t(1) << 49);
}
