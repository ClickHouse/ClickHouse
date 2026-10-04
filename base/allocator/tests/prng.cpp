/// Pins the PRNG streams to values computed with jemalloc's `prng.h`.

#include <allocator/Prng.h>

#include "Test.h"

using namespace jemalloc;

TEST(Prng, LgRangeU32)
{
    constexpr uint32_t expected[] = {1u, 60u, 1685u, 7809u, 1477088u, 55188319u, 887721383u, 14u};
    uint32_t state = 42;
    for (unsigned i = 0; i < 8; ++i)
        CHECK_EQ(prngLgRangeU32(state, 1 + (i * 5) % 32), expected[i]);
    CHECK_EQ(state, 3892475298u);
}

TEST(Prng, LgRangeU64)
{
    constexpr uint64_t expected[] = {1ull, 230ull, 216446ull, 169221187ull, 93478802833ull, 1845695507102ull,
        784015565464968ull, 2812299150962093586ull};
    uint64_t state = 42;
    for (unsigned i = 0; i < 8; ++i)
        CHECK_EQ(prngLgRangeU64(state, 1 + (i * 9) % 64), expected[i]);
    CHECK_EQ(state, 2812299150962093586ull);

    size_t state_zu = 42;
    for (unsigned i = 0; i < 8; ++i)
        CHECK_EQ(prngLgRangeZu(state_zu, 1 + (i * 9) % 64), expected[i]);
    CHECK_EQ(state_zu, 2812299150962093586ull);
}

TEST(Prng, Range)
{
    {
        constexpr uint32_t expected[] = {0u, 817u, 563u, 723u, 2507u, 3267u, 2346u, 4167u};
        uint32_t state = 7;
        for (unsigned i = 0; i < 8; ++i)
            CHECK_EQ(prngRangeU32(state, 1 + i * 1000), expected[i]);
        CHECK_EQ(state, 2184963730u);
    }
    {
        constexpr uint64_t expected[] = {0ull, 517170ull, 1901227ull, 1143981ull, 1117276ull, 1161161ull, 3379647ull, 2701624ull};
        uint64_t state = 7;
        for (unsigned i = 0; i < 8; ++i)
            CHECK_EQ(prngRangeU64(state, 1 + i * 1000003ull), expected[i]);
        CHECK_EQ(state, 5940934178883179151ull);
    }
    {
        constexpr size_t expected[] = {1ull, 35749ull, 69829ull, 36286ull, 211227ull, 168851ull, 372042ull, 440972ull};
        size_t state = 7;
        for (unsigned i = 0; i < 8; ++i)
            CHECK_EQ(prngRangeZu(state, 3 + i * 77777ull), expected[i]);
        CHECK_EQ(state, 7757674689382620148ull);
    }
}

TEST(Prng, RangeOneDoesNotAdvance)
{
    uint64_t state = 123;
    CHECK_EQ(prngRangeU64(state, 1), 0u);
    CHECK_EQ(state, 123u);
}

TEST(Prng, Constexpr)
{
    constexpr uint64_t value = []
    {
        uint64_t state = 42;
        return prngLgRangeU64(state, 1);
    }();
    static_assert(value == 1);
}
