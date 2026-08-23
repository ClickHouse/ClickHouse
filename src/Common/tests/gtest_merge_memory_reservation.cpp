#include <gtest/gtest.h>

#include <Common/MemoryTracker.h>

#include <limits>
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

TEST_F(MergeMemoryReservationTest, HugeEstimateSaturatesInsteadOfWrappingNegative)
{
    background_memory_tracker.setSoftLimit(1000);

    {
        /// The estimator saturates to the UInt64 maximum when a bound is unlimited. Reserving such an
        /// estimate must clamp to the Int64 maximum and keep the gate closed - a wrapped-negative
        /// reservation would instead *loosen* the gate as the merge footprint grows.
        auto huge = MergeMemoryReservation::reserve(std::numeric_limits<UInt64>::max());
        ASSERT_EQ(getReservedMergeMemory(), std::numeric_limits<Int64>::max());
        ASSERT_FALSE(canEnqueueBackgroundTask());

        /// A second unconditional huge reservation must not overflow the counter either.
        auto another = MergeMemoryReservation::reserve(std::numeric_limits<UInt64>::max());
        ASSERT_EQ(getReservedMergeMemory(), std::numeric_limits<Int64>::max());
        ASSERT_FALSE(canEnqueueBackgroundTask());

        /// And while the counter is saturated, conditional reservations are rejected.
        auto rejected = MergeMemoryReservation::tryReserve(1);
        ASSERT_FALSE(rejected.has_value());
        ASSERT_EQ(getReservedMergeMemory(), std::numeric_limits<Int64>::max());
    }

    /// Releases are symmetric with what each reservation added, so the counter returns to zero.
    ASSERT_EQ(getReservedMergeMemory(), 0);
}

TEST_F(MergeMemoryReservationTest, OverlappingHugeReservationsReleasedOutOfOrder)
{
    background_memory_tracker.setSoftLimit(1000);

    {
        /// Two overlapping huge reservations, and the first one finishes before the second.
        auto first = std::make_optional(MergeMemoryReservation::reserve(std::numeric_limits<UInt64>::max()));
        auto second = MergeMemoryReservation::reserve(std::numeric_limits<UInt64>::max());
        ASSERT_EQ(getReservedMergeMemory(), std::numeric_limits<Int64>::max());

        /// Releasing the first reservation must not reopen the gate: the second huge merge is
        /// still running, so the published counter must remain saturated (a single saturated
        /// counter would drop to zero here and admit merges against the still-running reservation).
        first.reset();
        ASSERT_EQ(getReservedMergeMemory(), std::numeric_limits<Int64>::max());
        ASSERT_FALSE(canEnqueueBackgroundTask());

        auto rejected = MergeMemoryReservation::tryReserve(1);
        ASSERT_FALSE(rejected.has_value());
    }

    ASSERT_EQ(getReservedMergeMemory(), 0);
}

TEST_F(MergeMemoryReservationTest, ReplacingReservationReleasesTheOldOne)
{
    background_memory_tracker.setSoftLimit(0);

    {
        /// Move-assignment releases the overwritten reservation exactly once. This is the RAII
        /// fallback; the production correction paths use `replace` (tested below), which never
        /// exposes the transient `old + new` total this two-step sequence goes through.
        auto reservation = MergeMemoryReservation::reserve(100);
        ASSERT_EQ(getReservedMergeMemory(), 100);

        reservation = MergeMemoryReservation::reserve(5000);
        ASSERT_EQ(getReservedMergeMemory(), 5000);
    }

    ASSERT_EQ(getReservedMergeMemory(), 0);
}

TEST_F(MergeMemoryReservationTest, ReplaceCorrectsTheReservationInPlace)
{
    background_memory_tracker.setSoftLimit(0);

    {
        /// The non-replicated path first reserves using a destination disk guessed from the source
        /// parts, and corrects the reservation once the tagger has chosen the actual destination disk
        /// (and again at task start, when the disk's request settings are re-read).
        auto reservation = MergeMemoryReservation::reserve(100);
        ASSERT_EQ(getReservedMergeMemory(), 100);

        /// Correct upward...
        reservation = MergeMemoryReservation::replace(std::move(reservation), 5000);
        ASSERT_EQ(getReservedMergeMemory(), 5000);

        /// ... and downward.
        reservation = MergeMemoryReservation::replace(std::move(reservation), 40);
        ASSERT_EQ(getReservedMergeMemory(), 40);

        /// Replacing an empty reservation is a plain unconditional reserve.
        MergeMemoryReservation empty;
        auto other = MergeMemoryReservation::replace(std::move(empty), 7);
        ASSERT_EQ(getReservedMergeMemory(), 47);
    }

    ASSERT_EQ(getReservedMergeMemory(), 0);
}

TEST_F(MergeMemoryReservationTest, ReplaceKeepsSaturationSymmetric)
{
    background_memory_tracker.setSoftLimit(0);

    constexpr UInt64 huge = std::numeric_limits<UInt64>::max();
    constexpr Int64 int64_max = std::numeric_limits<Int64>::max();

    {
        auto huge_reservation = MergeMemoryReservation::reserve(huge);
        auto small_reservation = MergeMemoryReservation::reserve(500);
        ASSERT_EQ(getReservedMergeMemory(), int64_max);

        /// Replacing a saturated reservation with another huge one keeps the published counter
        /// saturated and the accumulator exact.
        huge_reservation = MergeMemoryReservation::replace(std::move(huge_reservation), huge);
        ASSERT_EQ(getReservedMergeMemory(), int64_max);

        /// Correcting the huge reservation down uncovers exactly the small one.
        huge_reservation = MergeMemoryReservation::replace(std::move(huge_reservation), 100);
        ASSERT_EQ(getReservedMergeMemory(), 600);
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
