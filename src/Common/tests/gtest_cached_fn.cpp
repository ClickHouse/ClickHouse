#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <base/CachedFn.h>

using namespace std::chrono_literals;

constexpr int add(int x, int y)
{
    return x + y;
}

int longFunction(int x, int y)
{
    std::this_thread::sleep_for(1s);
    return x + y;
}

auto f = [](int x, int y) { return x - y; };

TEST(CachedFn, Basic)
{
    CachedFn<&add> fn;

    const int res = fn(1, 2);
    EXPECT_EQ(fn(1, 2), res);

    /// In GCC, lambda can't be placed in TEST, producing "<labmda> has no linkage".
    /// Assuming http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2014/n4268.html,
    /// this is a GCC bug.
    CachedFn<+f> fn2;

    const int res2 = fn2(1, 2);
    EXPECT_EQ(fn2(1, 2), res2);
}

TEST(CachedFn, CachingResults)
{
    CachedFn<&longFunction> fn;

    for (int x = 0; x < 2; ++x)
    {
        for (int y = 0; y < 2; ++y)
        {
            const int res = fn(x, y);
            const time_t start = time(nullptr);

            for (int count = 0; count < 1000; ++count)
                EXPECT_EQ(fn(x, y), res);

            EXPECT_LT(time(nullptr) - start, 10);
        }
    }
}
