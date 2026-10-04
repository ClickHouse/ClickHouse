/// Compares `fxp::parse` / `fxp::print` with `fxp_parse` / `fxp_print` from the reference jemalloc.

#include <allocator/FixedPoint.h>

#include "Test.h"

#include <cstring>
#include <random>

extern "C"
{
bool fxp_parse(uint32_t * result, const char * str, char ** end);
void fxp_print(uint32_t a, char buf[21]);
}

using namespace jemalloc;

namespace
{

void comparePrint(FixedPoint value)
{
    char expected[fxp::BUF_SIZE];
    char actual[fxp::BUF_SIZE];
    std::memset(expected, 'Z', sizeof(expected));
    std::memset(actual, 'Z', sizeof(actual));
    fxp_print(value, expected);
    fxp::print(value, actual);
    if (std::memcmp(expected, actual, sizeof(expected)) != 0)
    {
        std::fprintf(stderr, "print mismatch for %u: \"%s\" vs \"%s\"\n", value, expected, actual);
        ++allocator_test::failureCount();
    }
}

void compareParse(const char * input)
{
    uint32_t expected = 0xdeadbeef;
    uint32_t actual = 0xdeadbeef;
    char * expected_end = nullptr;
    const char * actual_end = nullptr;
    bool expected_error = fxp_parse(&expected, input, &expected_end);
    bool actual_error = fxp::parse(&actual, input, &actual_end);
    if (expected_error != actual_error || expected != actual || expected_end != actual_end)
    {
        std::fprintf(stderr, "parse mismatch for \"%s\": %d %u vs %d %u\n", input, expected_error, expected, actual_error, actual);
        ++allocator_test::failureCount();
    }
}

}

TEST(FixedPointOracle, PrintAllFractions)
{
    for (uint32_t integer : {0u, 1u, 7u, 100u, 65535u})
        for (uint32_t fraction = 0; fraction < 65536; ++fraction)
            comparePrint((integer << 16) | fraction);

    std::mt19937 rng(42);
    for (size_t i = 0; i < 1000000; ++i)
        comparePrint(static_cast<uint32_t>(rng()));
}

TEST(FixedPointOracle, Parse)
{
    const char alphabet[] = "0123456789..x";
    std::mt19937_64 rng(7);
    char input[40];
    for (size_t iteration = 0; iteration < 500000; ++iteration)
    {
        size_t len = rng() % 30;
        for (size_t i = 0; i < len; ++i)
            input[i] = alphabet[rng() % (sizeof(alphabet) - 1)];
        input[len] = '\0';
        compareParse(input);
    }

    /// Round trip of printed values.
    for (size_t i = 0; i < 200000; ++i)
    {
        char buf[fxp::BUF_SIZE];
        fxp::print(static_cast<uint32_t>(rng()), buf);
        compareParse(buf);
    }
}
