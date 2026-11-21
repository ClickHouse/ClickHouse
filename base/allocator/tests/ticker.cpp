/// Tests of `Ticker` and `TickerGeometric`; the geometric ticker sequence is pinned to jemalloc's `ticker.h`.

#include <allocator/Ticker.h>

#include "Test.h"

using namespace jemalloc;

TEST(Ticker, Tick)
{
    Ticker ticker;
    ticker.init(3);
    CHECK_EQ(ticker.read(), 3);
    for (int round = 0; round < 3; ++round)
    {
        CHECK(!ticker.tickOnce(false));
        CHECK(!ticker.tickOnce(false));
        CHECK(!ticker.tickOnce(false));
        CHECK_EQ(ticker.read(), 0);
        CHECK(ticker.tickOnce(false));
        CHECK_EQ(ticker.read(), 3);
    }
}

TEST(Ticker, Ticks)
{
    Ticker ticker;
    ticker.init(10);
    CHECK(!ticker.ticks(10, false));
    CHECK(ticker.ticks(1, false));
    CHECK_EQ(ticker.read(), 10);
    CHECK(ticker.ticks(100, false));
    CHECK_EQ(ticker.read(), 10);
}

TEST(Ticker, DelayTrigger)
{
    Ticker ticker;
    ticker.init(2);
    CHECK(!ticker.ticks(3, true));
    CHECK_EQ(ticker.read(), 0);
    CHECK(ticker.tickOnce(false));
    CHECK_EQ(ticker.read(), 2);
}

TEST(Ticker, TryTick)
{
    Ticker ticker;
    ticker.init(1);
    CHECK(!ticker.tryTick());
    CHECK(ticker.tryTick());
    CHECK_EQ(ticker.read(), -1);

    Ticker copy;
    copy.copyFrom(ticker);
    CHECK_EQ(copy.read(), -1);
    CHECK_EQ(copy.num_ticks, 1);
}

TEST(TickerGeometric, Table)
{
    CHECK_EQ(ticker_geometric_table[0], 254u);
    CHECK_EQ(ticker_geometric_table[63], 0u);
    unsigned sum = 0;
    for (uint8_t value : ticker_geometric_table)
        sum += value;
    CHECK_EQ(sum, 3720u);
}

TEST(TickerGeometric, Sequence)
{
    constexpr int32_t expected[] = {2081, 1327, 114, 163, 1114, 573, 229, 901, 196, 1590};
    TickerGeometric ticker = tickerGeometricInit(1000);
    uint64_t prng_state = 12345;
    int fires = 0;
    for (int i = 0; i < 100000; ++i)
    {
        if (ticker.tickOnce(prng_state, false))
        {
            if (fires < 10)
                CHECK_EQ(ticker.read(), expected[fires]);
            ++fires;
        }
    }
    CHECK_EQ(fires, 104);
    CHECK_EQ(ticker.read(), 1644);
    CHECK_EQ(prng_state, 2704952026396713569ull);
}

TEST(TickerGeometric, TicksWithDelay)
{
    TickerGeometric ticker;
    ticker.init(1000);
    uint64_t prng_state = 99;
    int fires = 0;
    for (int i = 0; i < 1000; ++i)
        if (ticker.ticks(prng_state, 37, (i % 3) == 0))
            ++fires;
    CHECK_EQ(fires, 39);
    CHECK_EQ(ticker.read(), 315);
    CHECK_EQ(prng_state, 205073686209043884ull);
}
