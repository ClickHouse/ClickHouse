#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include <Storages/MergeTree/TemporaryParts.h>

namespace DB
{

/// The arbitration methods are private, so the test reaches them through this friend accessor.
class TemporaryPartsTestAccessor
{
public:
    static void add(TemporaryParts & temporary_parts, const std::string & basename)
    {
        temporary_parts.add(basename);
    }

    static void remove(TemporaryParts & temporary_parts, const std::string & basename)
    {
        temporary_parts.remove(basename);
    }

    static bool tryClaimForCleanup(TemporaryParts & temporary_parts, const std::string & basename)
    {
        return temporary_parts.tryClaimForCleanup(basename);
    }

    static void releaseCleanupClaim(TemporaryParts & temporary_parts, const std::string & basename)
    {
        temporary_parts.releaseCleanupClaim(basename);
    }
};

}

using DB::TemporaryPartsTestAccessor;

TEST(TemporaryParts, CleanupCannotClaimOwnedName)
{
    DB::TemporaryParts temporary_parts;

    TemporaryPartsTestAccessor::add(temporary_parts, "tmp_a");
    EXPECT_TRUE(temporary_parts.contains("tmp_a"));

    /// The cleaner must not claim a name owned by an active operation.
    EXPECT_FALSE(TemporaryPartsTestAccessor::tryClaimForCleanup(temporary_parts, "tmp_a"));

    /// An unrelated name is claimable.
    EXPECT_TRUE(TemporaryPartsTestAccessor::tryClaimForCleanup(temporary_parts, "tmp_b"));
    TemporaryPartsTestAccessor::releaseCleanupClaim(temporary_parts, "tmp_b");

    /// Once the operation releases the name, the cleaner can claim it.
    TemporaryPartsTestAccessor::remove(temporary_parts, "tmp_a");
    EXPECT_TRUE(TemporaryPartsTestAccessor::tryClaimForCleanup(temporary_parts, "tmp_a"));
    TemporaryPartsTestAccessor::releaseCleanupClaim(temporary_parts, "tmp_a");
}

TEST(TemporaryParts, CleanupHoldIsExclusive)
{
    DB::TemporaryParts temporary_parts;

    EXPECT_TRUE(TemporaryPartsTestAccessor::tryClaimForCleanup(temporary_parts, "tmp_c"));

    /// A second cleanup claim on the same name must fail while the first hold is in place.
    EXPECT_FALSE(TemporaryPartsTestAccessor::tryClaimForCleanup(temporary_parts, "tmp_c"));

    TemporaryPartsTestAccessor::releaseCleanupClaim(temporary_parts, "tmp_c");

    /// After the hold is released, the name is claimable again.
    EXPECT_TRUE(TemporaryPartsTestAccessor::tryClaimForCleanup(temporary_parts, "tmp_c"));
    TemporaryPartsTestAccessor::releaseCleanupClaim(temporary_parts, "tmp_c");
}

TEST(TemporaryParts, OperationClaimWaitsForCleanup)
{
    DB::TemporaryParts temporary_parts;

    ASSERT_TRUE(TemporaryPartsTestAccessor::tryClaimForCleanup(temporary_parts, "tmp_e"));

    std::atomic<bool> started{false};
    std::atomic<bool> finished{false};

    std::thread operation(
        [&]
        {
            started = true;
            /// Must block until the cleanup hold on "tmp_e" is released.
            TemporaryPartsTestAccessor::add(temporary_parts, "tmp_e");
            finished = true;
        });

    while (!started)
        std::this_thread::yield();

    /// Watch for `add` returning while the hold is in place, which is what would happen if the
    /// condition-variable wait were removed. The sleep observes a non-occurrence, it does not hide a race.
    bool returned_while_held = false;
    for (size_t i = 0; i < 30 && !returned_while_held; ++i)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        returned_while_held = finished;
    }

    TemporaryPartsTestAccessor::releaseCleanupClaim(temporary_parts, "tmp_e");
    operation.join();

    EXPECT_FALSE(returned_while_held) << "add() returned while the name was held by the cleaner";
    EXPECT_TRUE(finished);
    EXPECT_TRUE(temporary_parts.contains("tmp_e"));
    TemporaryPartsTestAccessor::remove(temporary_parts, "tmp_e");
}
