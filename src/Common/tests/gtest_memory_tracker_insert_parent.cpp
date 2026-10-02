#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/MemoryTracker.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/MemoryTrackerSwitcher.h>
#include <Common/ThreadStatus.h>

#include <thread>
#include <tuple>

namespace
{

struct AccountingLedger
{
    /// An isolated root makes every assertion exact, independent of test-runner allocations.
    MemoryTracker global{nullptr, VariableContext::Global, false};
    MemoryTracker user{&global, VariableContext::User, false};
    MemoryTracker query{&global, VariableContext::Process, false};
};

TEST(MemoryTrackerInsertParent, QuerySetupLimitDoesNotRecheckExistingAncestors)
{
    AccountingLedger ledger;
    ledger.query.adjustWithUntrackedMemory(100);
    ledger.global.setHardLimit(1);
    ledger.query.setHardLimit(100);
    EXPECT_NO_THROW(ledger.query.checkQueryLimit());

    ledger.query.setHardLimit(99);
    EXPECT_THROW(ledger.query.checkQueryLimit(), DB::Exception);
    EXPECT_EQ(ledger.query.get(), 100);
    EXPECT_EQ(ledger.global.get(), 100);
    {
        LockMemoryExceptionInThread blocker(VariableContext::Process);
        EXPECT_NO_THROW(ledger.query.checkQueryLimit());
    }
    ledger.query.setHardLimit(0);
    EXPECT_NO_THROW(ledger.query.checkQueryLimit());
    ledger.query.adjustWithUntrackedMemory(-100);
    EXPECT_EQ(ledger.global.get(), 0);
}

TEST(MemoryTrackerInsertParent, CreditsOnlyNewAncestorAndBalancesSubsequentFrees)
{
    AccountingLedger ledger;
    ledger.user.adjustWithUntrackedMemory(50);
    ledger.query.adjustWithUntrackedMemory(100);
    ASSERT_FALSE(ledger.query.tryInsertParent(&ledger.user));
    EXPECT_EQ(ledger.query.get(), 100);
    EXPECT_EQ(ledger.user.get(), 150);
    EXPECT_EQ(ledger.global.get(), 150);
    EXPECT_EQ(ledger.user.getPeak(), 150);
    EXPECT_EQ(ledger.global.getPeak(), 150);

    ASSERT_FALSE(ledger.query.tryInsertParent(&ledger.user));
    ledger.query.adjustWithUntrackedMemory(-40);
    EXPECT_EQ(ledger.query.get(), 60);
    EXPECT_EQ(ledger.user.get(), 110);
    EXPECT_EQ(ledger.global.get(), 110);

    ledger.query.adjustWithUntrackedMemory(20);
    ledger.query.adjustWithUntrackedMemory(-80);
    EXPECT_EQ(ledger.query.get(), 0);
    EXPECT_EQ(ledger.user.get(), 50);
    EXPECT_EQ(ledger.global.get(), 50);
    ledger.user.adjustWithUntrackedMemory(-50);
    EXPECT_EQ(ledger.global.get(), 0);
}

TEST(MemoryTrackerInsertParent, RejectedAdmissionPreservesExistingUsageAndPeaks)
{
    AccountingLedger ledger;
    ledger.user.adjustWithUntrackedMemory(50);
    ledger.query.adjustWithUntrackedMemory(100);
    ledger.user.setHardLimit(120);

    for (size_t attempt = 0; attempt < 3; ++attempt)
    {
        auto rejected = ledger.query.tryInsertParent(&ledger.user);
        ASSERT_TRUE(rejected);
        EXPECT_EQ(rejected->size, 100);
        EXPECT_EQ(rejected->would_use, 150);
        EXPECT_EQ(rejected->limit, 120);
        EXPECT_EQ(ledger.query.getParent(), &ledger.global);
        EXPECT_EQ(ledger.query.get(), 100);
        EXPECT_EQ(ledger.user.get(), 50);
        EXPECT_EQ(ledger.user.getPeak(), 50);
        EXPECT_EQ(ledger.global.get(), 150);
        EXPECT_EQ(ledger.global.getPeak(), 150);
    }

    ledger.query.adjustWithUntrackedMemory(-100);
    EXPECT_EQ(ledger.user.get(), 50);
    EXPECT_EQ(ledger.global.get(), 50);
    ledger.user.adjustWithUntrackedMemory(-50);
    EXPECT_EQ(ledger.global.get(), 0);
}

TEST(MemoryTrackerInsertParent, ExistingBytesAreTransferredEvenWhenNewAllocationsAreBlocked)
{
    AccountingLedger ledger;
    ledger.query.adjustWithUntrackedMemory(100);
    {
        MemoryTrackerBlockerInThread blocker(VariableContext::Global);
        ASSERT_FALSE(ledger.query.tryInsertParent(&ledger.user));
    }
    EXPECT_EQ(ledger.user.get(), 100);
    EXPECT_EQ(ledger.global.get(), 100);
    ledger.query.adjustWithUntrackedMemory(-100);
    EXPECT_EQ(ledger.user.get(), 0);
    EXPECT_EQ(ledger.global.get(), 0);
}

TEST(MemoryTrackerInsertParent, AdmissionsIncludePreviouslyAdmittedSetup)
{
    AccountingLedger ledger;
    MemoryTracker second_query{&ledger.global, VariableContext::Process, false};
    ledger.user.setHardLimit(150);
    ledger.query.adjustWithUntrackedMemory(100);
    ASSERT_FALSE(ledger.query.tryInsertParent(&ledger.user));

    /// The first query can be descheduled immediately after admission. Its setup bytes
    /// must still prevent a second 130-byte query from passing the same 150-byte limit.
    second_query.adjustWithUntrackedMemory(130);
    auto rejected = second_query.tryInsertParent(&ledger.user);
    ASSERT_TRUE(rejected);
    EXPECT_EQ(rejected->would_use, 230);
    EXPECT_EQ(ledger.user.get(), 100);
    EXPECT_EQ(ledger.user.getPeak(), 100);
    EXPECT_EQ(ledger.global.getPeak(), 230);
    second_query.adjustWithUntrackedMemory(-130);
    ledger.query.adjustWithUntrackedMemory(-100);
    EXPECT_EQ(ledger.user.get(), 0);
    EXPECT_EQ(ledger.global.get(), 0);
}

TEST(MemoryTrackerInsertParent, DecisionDoesNotAllocate)
{
    AccountingLedger ledger;
    ledger.query.adjustWithUntrackedMemory(100);
    ledger.user.setHardLimit(50);
    std::optional<MemoryTracker::ParentLimitExceeded> rejected;
    {
        DENY_ALLOCATIONS_IN_SCOPE;
        rejected = ledger.query.tryInsertParent(&ledger.user);
    }
    ASSERT_TRUE(rejected);
    ledger.user.setHardLimit(200);
    {
        DENY_ALLOCATIONS_IN_SCOPE;
        rejected = ledger.query.tryInsertParent(&ledger.user);
    }
    ASSERT_FALSE(rejected);
    ledger.query.adjustWithUntrackedMemory(-100);
}

TEST(MemoryTrackerInsertParent, PendingBytesFollowCommittedParentAndOriginalBlocker)
{
    for (bool accept : {false, true})
    {
        for (bool globally_blocked : {false, true})
        {
            for (Int64 pending : {-25, 25})
            {
                AccountingLedger ledger;
                /// `MemoryTrackerSwitcher` validates that a thread's chain reaches the real root.
                /// The intermediate root still isolates local amounts from test-runner allocations.
                ledger.global.setParent(&total_memory_tracker);
                ledger.user.adjustWithUntrackedMemory(50);
                ledger.query.adjustWithUntrackedMemory(100);
                ledger.user.setHardLimit(accept ? 200 : 120);
                std::thread([&]
                {
                    DB::ThreadStatus thread;
                    bool rejected = false;
                    Int64 pending_before = 0;
                    Int64 query_before_flush = 0;
                    Int64 user_before_flush = 0;
                    Int64 global_before_flush = 0;
                    {
                        DB::MemoryTrackerSwitcher scope(&ledger.query, 1024);
                        {
                            MemoryTrackerBlockerInThread blocker(
                                globally_blocked ? VariableContext::Global : VariableContext::Max);
                            if (pending > 0)
                                std::ignore = CurrentMemoryTracker::allocNoThrow(pending);
                            else
                                std::ignore = CurrentMemoryTracker::free(-pending);
                        }
                        pending_before = thread.untracked_memory.load();
                        rejected = ledger.query.tryInsertParent(&ledger.user).has_value();
                        query_before_flush = ledger.query.get();
                        user_before_flush = ledger.user.get();
                        global_before_flush = ledger.global.get();
                        thread.flushUntrackedMemory();
                    }
                    EXPECT_EQ(rejected, !accept);
                    EXPECT_EQ(pending_before, pending);
                    EXPECT_EQ(query_before_flush, 100);
                    EXPECT_EQ(user_before_flush, accept ? 150 : 50);
                    EXPECT_EQ(global_before_flush, 150);
                }).join();

                const Int64 query_delta = globally_blocked ? 0 : pending;
                EXPECT_EQ(ledger.query.get(), 100 + query_delta);
                EXPECT_EQ(ledger.user.get(), accept ? 150 + query_delta : 50);
                EXPECT_EQ(ledger.global.get(), 150 + pending);
                ledger.query.adjustWithUntrackedMemory(-100 - query_delta);
                ledger.user.adjustWithUntrackedMemory(-50);
                if (globally_blocked)
                    ledger.global.adjustWithUntrackedMemory(-pending);
                EXPECT_EQ(ledger.global.get(), 0);
            }
        }
    }
}

}
