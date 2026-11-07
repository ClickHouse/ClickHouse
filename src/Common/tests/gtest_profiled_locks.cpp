#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ProfiledLocks.h>
#include <Common/ProfileEventsScope.h>
#include <Common/ThreadStatus.h>

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

namespace ProfileEvents
{
    extern const Event ZooKeeperWatchResponse;
}

namespace
{

/// Force the contended path without timing or competing-thread assumptions.
/// Publishing at acquisition requires a reservation already made by the profiled wrapper.
class TSA_CAPABILITY("ReservationCheckingMutex") ReservationCheckingMutex
{
public:
    explicit ReservationCheckingMutex(bool timed_success_) : timed_success(timed_success_) {}

    bool try_lock() TSA_TRY_ACQUIRE(true) { return false; }

    void lock() TSA_ACQUIRE()
    {
        verifyReservation();
        locked = true;
    }

    bool try_lock_for(std::chrono::milliseconds) TSA_TRY_ACQUIRE(true)
    {
        verifyReservation();
        locked = timed_success;
        return locked;
    }

    void unlock() TSA_RELEASE()
    {
        locked = false;
        ++unlocks;
    }

    bool locked = false;
    size_t reservation_checks = 0;
    size_t unlocks = 0;

private:
    void verifyReservation()
    {
        DENY_ALLOCATIONS_IN_SCOPE;
        /// The nonallocating API rejects missing backing even in release builds, where
        /// the allocation-denial macro is disabled. The event is deliberately cold.
        ProfileEvents::incrementNonAllocating(ProfileEvents::ZooKeeperWatchResponse);
        ++reservation_checks;
    }

    bool timed_success;
};

void checkReservation(bool timed, bool timed_success)
{
    /// A new thread and scope ensure that no earlier test reserved this cold page locally.
    /// TSA cannot model the fake mutex's conditional timed acquisition; ownership is asserted below.
    std::thread worker([=]() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        DB::ThreadStatus status;
        const auto event = ProfileEvents::ZooKeeperWatchResponse;
        ProfileEvents::Count published = 0;
        ReservationCheckingMutex mutex(timed_success);
        {
            DB::ProfileEventsScope scope;
            EXPECT_EQ(DB::CurrentThread::getProfileEvents()[event], 0);
            if (timed)
            {
                DB::ProfiledExclusiveLock lock(mutex, event, std::chrono::milliseconds(1));
                EXPECT_EQ(lock.owns_lock(), timed_success);
                EXPECT_EQ(mutex.locked, timed_success);
            }
            else
            {
                DB::ProfiledExclusiveLock lock(mutex, event);
                EXPECT_TRUE(lock.owns_lock());
                EXPECT_TRUE(mutex.locked);
            }
            published = DB::CurrentThread::getProfileEvents()[event];
            EXPECT_GE(published, 1);
            EXPECT_EQ(mutex.reservation_checks, 1);
            EXPECT_EQ(mutex.unlocks, !timed || timed_success ? 1 : 0);
            EXPECT_FALSE(mutex.locked);
        }
        /// Both the reservation probe and actual wait time propagate to the parent counters.
        EXPECT_EQ(DB::CurrentThread::getProfileEvents()[event], published);
    });
    worker.join();
}

}

TEST(ProfiledLocks, ColdWaitEventIsReservedBeforeAcquisition)
{
    checkReservation(false, false);
}

TEST(ProfiledLocks, ColdWaitEventIsReservedBeforeTimedAcquisition)
{
    checkReservation(true, true);
}

TEST(ProfiledLocks, ColdWaitEventIsReservedForTimedFailure)
{
    checkReservation(true, false);
}
