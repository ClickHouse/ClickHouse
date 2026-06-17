#include <IO/LongConnectionLimit.h>
#include <gtest/gtest.h>

#include <memory>
#include <utility>

using namespace DB;

TEST(LongConnectionLimit, AcquireUpToCapacityThenFail)
{
    auto limit = std::make_shared<LongConnectionLimit>(2);
    auto a = limit->tryAcquire(limit);
    auto b = limit->tryAcquire(limit);
    EXPECT_TRUE(static_cast<bool>(a));
    EXPECT_TRUE(static_cast<bool>(b));
    EXPECT_EQ(limit->getActiveCount(), 2u);

    auto c = limit->tryAcquire(limit);   /// at capacity -> empty lease
    EXPECT_FALSE(static_cast<bool>(c));
    EXPECT_EQ(limit->getActiveCount(), 2u);
}

TEST(LongConnectionLimit, ReleaseFreesCapacity)
{
    auto limit = std::make_shared<LongConnectionLimit>(1);
    {
        auto a = limit->tryAcquire(limit);
        EXPECT_TRUE(static_cast<bool>(a));
        EXPECT_FALSE(static_cast<bool>(limit->tryAcquire(limit)));   /// full
    }
    EXPECT_EQ(limit->getActiveCount(), 0u);   /// released on scope exit
    auto b = limit->tryAcquire(limit);
    EXPECT_TRUE(static_cast<bool>(b));
}

TEST(LongConnectionLimit, MoveTransfersOwnershipWithoutDoubleRelease)
{
    auto limit = std::make_shared<LongConnectionLimit>(1);
    auto a = limit->tryAcquire(limit);
    EXPECT_EQ(limit->getActiveCount(), 1u);

    LongConnectionSlot moved = std::move(a);
    /// Intentionally inspect the moved-from slot: its move leaves it defined-empty.
    // NOLINTNEXTLINE(bugprone-use-after-move,hicpp-invalid-access-moved)
    EXPECT_FALSE(static_cast<bool>(a));   /// moved-from holds nothing
    EXPECT_TRUE(static_cast<bool>(moved));
    EXPECT_EQ(limit->getActiveCount(), 1u);   /// still exactly one unit held
}

TEST(LongConnectionLimit, SetCapacityLoweringIsSoft)
{
    auto limit = std::make_shared<LongConnectionLimit>(2);
    auto a = limit->tryAcquire(limit);
    auto b = limit->tryAcquire(limit);
    EXPECT_EQ(limit->getActiveCount(), 2u);

    limit->setCapacity(1);   /// soft: existing leases are not revoked
    EXPECT_EQ(limit->getActiveCount(), 2u);
    EXPECT_EQ(limit->getCapacity(), 1u);
    EXPECT_FALSE(static_cast<bool>(limit->tryAcquire(limit)));   /// no new lease until below the new cap
}

TEST(LongConnectionLimit, ZeroCapacityDisablesReuse)
{
    auto limit = std::make_shared<LongConnectionLimit>(0);
    EXPECT_FALSE(static_cast<bool>(limit->tryAcquire(limit)));   /// 0 -> always stateless
    EXPECT_EQ(limit->getActiveCount(), 0u);
}
