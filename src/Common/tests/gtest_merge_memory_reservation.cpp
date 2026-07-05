#include <gtest/gtest.h>

#include <Common/MemoryTracker.h>

#include <utility>

/// The reservation primitives (background_memory_tracker, MergeMemoryReservation, getReservedMergeMemory)
/// live at global scope, so no namespace qualification is needed.

/// Saves and restores the merges/mutations memory soft limit around each test, since it is a process-global value.
class MergeMemoryReservationTest : public ::testing::Test
{
protected:
    Int64 old_soft_limit = 0;

    void SetUp() override
    {
        old_soft_limit = background_memory_tracker.getSoftLimit();
        /// Every test starts from a clean reservation counter.
        ASSERT_EQ(getReservedMergeMemory(), 0);
    }

    void TearDown() override
    {
        background_memory_tracker.setSoftLimit(old_soft_limit);
        ASSERT_EQ(getReservedMergeMemory(), 0);
    }
};

TEST_F(MergeMemoryReservationTest, RespectsSoftLimit)
{
    background_memory_tracker.setSoftLimit(1000);

    {
        auto r1 = MergeMemoryReservation::tryReserve(400);
        ASSERT_TRUE(r1.has_value());
        ASSERT_EQ(getReservedMergeMemory(), 400);

        auto r2 = MergeMemoryReservation::tryReserve(400);
        ASSERT_TRUE(r2.has_value());
        ASSERT_EQ(getReservedMergeMemory(), 800);

        /// 800 + 400 = 1200 > 1000 and something is already reserved -> reject, counter unchanged.
        auto r3 = MergeMemoryReservation::tryReserve(400);
        ASSERT_FALSE(r3.has_value());
        ASSERT_EQ(getReservedMergeMemory(), 800);

        /// Reserving exactly up to the limit is allowed.
        auto r4 = MergeMemoryReservation::tryReserve(200);
        ASSERT_TRUE(r4.has_value());
        ASSERT_EQ(getReservedMergeMemory(), 1000);

        /// At the limit, even one more byte is rejected.
        auto r5 = MergeMemoryReservation::tryReserve(1);
        ASSERT_FALSE(r5.has_value());
        ASSERT_EQ(getReservedMergeMemory(), 1000);
    }

    /// All reservations are released when they go out of scope.
    ASSERT_EQ(getReservedMergeMemory(), 0);
}

TEST_F(MergeMemoryReservationTest, AlwaysAllowsFirstMergeOverLimit)
{
    background_memory_tracker.setSoftLimit(1000);

    {
        /// Nothing reserved yet: a single merge whose estimate exceeds the whole limit must still proceed,
        /// so that a large merge never blocks forever.
        auto huge = MergeMemoryReservation::tryReserve(5000);
        ASSERT_TRUE(huge.has_value());
        ASSERT_EQ(getReservedMergeMemory(), 5000);

        /// While that reservation is held, further merges are rejected.
        auto other = MergeMemoryReservation::tryReserve(1);
        ASSERT_FALSE(other.has_value());
    }

    ASSERT_EQ(getReservedMergeMemory(), 0);
}

TEST_F(MergeMemoryReservationTest, UnlimitedWhenSoftLimitZero)
{
    background_memory_tracker.setSoftLimit(0);

    {
        auto r1 = MergeMemoryReservation::tryReserve(1'000'000'000);
        ASSERT_TRUE(r1.has_value());
        auto r2 = MergeMemoryReservation::tryReserve(1'000'000'000);
        ASSERT_TRUE(r2.has_value()); /// No limit -> never rejected.
        ASSERT_EQ(getReservedMergeMemory(), 2'000'000'000);
    }

    ASSERT_EQ(getReservedMergeMemory(), 0);
}

TEST_F(MergeMemoryReservationTest, MoveDoesNotDoubleRelease)
{
    background_memory_tracker.setSoftLimit(0);

    {
        MergeMemoryReservation outer;
        {
            auto inner = MergeMemoryReservation::reserve(100);
            ASSERT_EQ(getReservedMergeMemory(), 100);
            outer = std::move(inner);
            /// `inner` is now empty; leaving its scope must not release anything.
        }
        ASSERT_EQ(getReservedMergeMemory(), 100);
    }

    /// Released once, by `outer`.
    ASSERT_EQ(getReservedMergeMemory(), 0);
}
