#include <base/sleep.h>
#include <gtest/gtest.h>

#include <stdexcept>

TEST(CancellableSleep, EmptyHook)
{
    EXPECT_NO_THROW(sleepForMilliseconds(1, {}));
}

TEST(CancellableSleep, ZeroDurationDoesNotCheckCancellation)
{
    EXPECT_NO_THROW(sleepForMilliseconds(0, [] { throw std::runtime_error("cancelled"); }));
}

TEST(CancellableSleep, PropagatesCancellationBeforeSleeping)
{
    EXPECT_THROW(sleepForMilliseconds(1, [] { throw std::runtime_error("cancelled"); }), std::runtime_error);
}

TEST(CancellableSleep, ChecksCancellationBetweenChunks)
{
    size_t checks = 0;
    EXPECT_THROW(
        sleepForMilliseconds(200, [&checks]
        {
            if (++checks == 2)
                throw std::runtime_error("cancelled");
        }),
        std::runtime_error);
    EXPECT_EQ(checks, 2);
}
