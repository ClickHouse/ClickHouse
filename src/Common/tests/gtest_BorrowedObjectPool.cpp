#include <base/BorrowedObjectPool.h>

#include <atomic>
#include <chrono>
#include <limits>
#include <memory>
#include <stdexcept>
#include <thread>

#include <gtest/gtest.h>

TEST(BorrowedObjectPool, FactoryExceptionDoesNotConsumeCapacity)
{
    BorrowedObjectPool<std::unique_ptr<int>> pool(1);
    std::unique_ptr<int> object;

    EXPECT_THROW(
        pool.tryBorrowObject(
            object,
            []() -> std::unique_ptr<int>
            {
                throw std::runtime_error("factory failed");
            }),
        std::runtime_error);

    EXPECT_EQ(pool.allocatedObjectsSize(), 0);
    EXPECT_EQ(pool.borrowedObjectsSize(), 0);

    EXPECT_TRUE(pool.tryBorrowObject(object, []
    {
        return std::make_unique<int>(42);
    }));
    ASSERT_NE(object, nullptr);
    EXPECT_EQ(*object, 42);
    EXPECT_EQ(pool.allocatedObjectsSize(), 1);
    EXPECT_EQ(pool.borrowedObjectsSize(), 1);

    pool.returnObject(std::move(object));
    EXPECT_EQ(pool.allocatedObjectsSize(), 1);
    EXPECT_EQ(pool.borrowedObjectsSize(), 0);
}

/// The one way into `BorrowedObjectPool::waitingBorrowersSize`, which is private: it exists for this
/// test, not for the pool's users.
struct BorrowedObjectPoolTestAccess
{
    template <typename Pool>
    static size_t waitingBorrowersSize(const Pool & pool) { return pool.waitingBorrowersSize(); }
};

namespace
{

/// What the tests below actually measure is how long a waiter takes to be let through, because a
/// missing wake-up does not fail a borrow: `wait_until` returns its predicate's value when the
/// deadline passes, and by then the predicate is true - the slot really is free, nobody just said
/// so. A waiter that was never notified therefore still gets its object, having slept out the whole
/// timeout first. So the timeout is the observable, and it is picked to leave no room for doubt: a
/// woken waiter is through in microseconds, an un-woken one takes exactly BORROW_TIMEOUT_MS, and
/// the tests draw the line at half of it. This is also why the timeout is not simply set to a large
/// number - it is what a regression costs the test run.
constexpr size_t BORROW_TIMEOUT_MS = 10000;
constexpr size_t PROMPT_BORROW_MS = BORROW_TIMEOUT_MS / 2;

/// Waits until `count` threads are asleep inside the pool's wait. See `waitingBorrowersSize` for
/// why the count answers "is the waiter registered on the condition variable" and not merely "has
/// the thread started". Returns false instead of spinning forever if they never get there, so a
/// broken pool fails the test rather than hanging it.
template <typename Pool>
[[nodiscard]] bool waitUntilWaitingBorrowers(const Pool & pool, size_t count)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(BORROW_TIMEOUT_MS);

    while (BorrowedObjectPoolTestAccess::waitingBorrowersSize(pool) < count)
    {
        if (std::chrono::steady_clock::now() > deadline)
            return false;
        std::this_thread::yield();
    }

    return true;
}

}

namespace
{

/// A payload whose hand-over to the borrower can be made to fail. The pool's rollback has to cover
/// that hand-over and not just the factory that produced the object: an assignment of a
/// user-supplied type can throw in its own right, and a slot that was counted as borrowed but
/// never reached anybody is a slot the pool loses for good.
///
/// The copy assignment is the one that throws, because that is the one the pool uses: the move
/// assignment below is not `noexcept`, and `moveOrCopyIfThrow` copies exactly when a move could
/// throw.
struct FailsToBeHandedOver
{
    int value = 0;
    /// Set by the test thread, consumed by whichever borrower thread the next hand-over runs on.
    static inline std::atomic<bool> fail_next_handover = false;

    FailsToBeHandedOver() = default;
    explicit FailsToBeHandedOver(int value_) : value(value_) {}
    FailsToBeHandedOver(const FailsToBeHandedOver &) = default;
    FailsToBeHandedOver(FailsToBeHandedOver &&) = default;

    FailsToBeHandedOver & operator=(const FailsToBeHandedOver & other)
    {
        if (this == &other)
            return *this;

        throwIfAsked();
        value = other.value;
        return *this;
    }

    /// Deliberately not `noexcept`, which is the whole point of the type: `moveOrCopyIfThrow` moves
    /// when the move assignment cannot throw and copies otherwise, so a `noexcept` move here would
    /// route the pool around the copy assignment above - the one that fails - and the test would
    /// prove nothing.
    FailsToBeHandedOver & operator=(FailsToBeHandedOver && other) // NOLINT(performance-noexcept-move-constructor,hicpp-noexcept-move)
    {
        if (this == &other)
            return *this;

        throwIfAsked();
        value = other.value;
        return *this;
    }

private:
    static void throwIfAsked()
    {
        if (!fail_next_handover.exchange(false))
            return;

        throw std::runtime_error("cannot hand this object over");
    }
};

}

/// A pool of one whose only slot was consumed by a failed hand-over would time out every borrow
/// after it, for the life of the process.
TEST(BorrowedObjectPool, FailedHandoverOfAFreshObjectDoesNotConsumeCapacity)
{
    BorrowedObjectPool<FailsToBeHandedOver> pool(1);

    FailsToBeHandedOver::fail_next_handover = true;

    FailsToBeHandedOver borrowed;
    EXPECT_THROW(pool.tryBorrowObject(borrowed, [] { return FailsToBeHandedOver(1); }, 1000), std::runtime_error);
    EXPECT_EQ(pool.allocatedObjectsSize(), 0u);
    EXPECT_EQ(pool.borrowedObjectsSize(), 0u);

    /// And the pool still lends.
    ASSERT_TRUE(pool.tryBorrowObject(borrowed, [] { return FailsToBeHandedOver(2); }, 1000));
    EXPECT_EQ(borrowed.value, 2);
}

/// The same for an object that was already in the pool: it is still there, so only the count of
/// borrowed objects has to be put back.
TEST(BorrowedObjectPool, FailedHandoverOfAPooledObjectLeavesItBorrowable)
{
    BorrowedObjectPool<FailsToBeHandedOver> pool(1);

    FailsToBeHandedOver borrowed;
    ASSERT_TRUE(pool.tryBorrowObject(borrowed, [] { return FailsToBeHandedOver(7); }, 1000));
    pool.returnObject(std::move(borrowed));

    FailsToBeHandedOver::fail_next_handover = true;

    FailsToBeHandedOver again;
    EXPECT_THROW(pool.tryBorrowObject(again, [] { return FailsToBeHandedOver(0); }, 1000), std::runtime_error);
    EXPECT_EQ(pool.borrowedObjectsSize(), 0u);

    ASSERT_TRUE(pool.tryBorrowObject(again, [] { return FailsToBeHandedOver(0); }, 1000));
    EXPECT_EQ(again.value, 7) << "the object that failed to be handed over was lost";
}

/// A hand-over of a pooled object that fails leaves the object in the pool - and must pass on the
/// wakeup it consumed. Two waiters on a pool of one: the object comes back, one waiter is woken,
/// its hand-over fails, and the object is still there, borrowable at once. Without a second wakeup
/// the other waiter sleeps out its whole timeout in front of it.
TEST(BorrowedObjectPool, FailedHandoverOfAPooledObjectWakesWaitingBorrower)
{
    BorrowedObjectPool<FailsToBeHandedOver> pool(1);

    FailsToBeHandedOver object;
    ASSERT_TRUE(pool.tryBorrowObject(object, [] { return FailsToBeHandedOver(7); }, BORROW_TIMEOUT_MS));

    std::atomic<size_t> failed_borrows = 0;
    std::atomic<size_t> successful_borrows = 0;
    std::atomic<size_t> slowest_borrow_ms = 0;

    auto borrow = [&]
    {
        FailsToBeHandedOver borrowed_object;
        const auto started_at = std::chrono::steady_clock::now();
        try
        {
            if (pool.tryBorrowObject(borrowed_object, [] { return FailsToBeHandedOver(0); }, BORROW_TIMEOUT_MS))
            {
                ++successful_borrows;
                EXPECT_EQ(borrowed_object.value, 7);
                pool.returnObject(std::move(borrowed_object));
            }
        }
        catch (const std::runtime_error &)
        {
            ++failed_borrows;
        }

        const size_t elapsed_ms = static_cast<size_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started_at).count());
        size_t previous = slowest_borrow_ms.load();
        while (previous < elapsed_ms && !slowest_borrow_ms.compare_exchange_weak(previous, elapsed_ms))
        {
        }
    };

    std::thread first_waiter(borrow);
    std::thread second_waiter(borrow);

    /// Both asleep before the object comes back, so that the return's wakeup reaches exactly one
    /// of them and the failing hand-over's own wakeup is the only thing that can reach the other.
    /// Not an ASSERT: returning from the test here would leave the borrower threads unjoined.
    EXPECT_TRUE(waitUntilWaitingBorrowers(pool, 2));

    /// The first hand-over out of the pool fails, whichever waiter gets it; the flag is consumed
    /// by that one attempt, so the next hand-over of the same object succeeds.
    FailsToBeHandedOver::fail_next_handover = true;
    pool.returnObject(std::move(object));

    first_waiter.join();
    second_waiter.join();

    EXPECT_LT(slowest_borrow_ms.load(), PROMPT_BORROW_MS);
    EXPECT_EQ(failed_borrows.load(), 1);
    EXPECT_EQ(successful_borrows.load(), 1);
    EXPECT_EQ(pool.allocatedObjectsSize(), 1);
    EXPECT_EQ(pool.borrowedObjectsSize(), 0);
}

/// A timeout meant as "wait forever" - a huge `max_command_execution_time` - must still wait. Added
/// to the clock without a bound it overflows the clock's counter, the deadline lands in the past, and
/// a full pool fails the call at once.
TEST(BorrowedObjectPool, HugeTimeoutWaits)
{
    BorrowedObjectPool<int> pool(1);

    int object = 0;
    ASSERT_TRUE(pool.tryBorrowObject(object, [] { return 1; }));

    std::atomic<bool> borrowed = false;
    std::thread borrower(
        [&]
        {
            int borrowed_object = 0;
            if (pool.tryBorrowObject(borrowed_object, [] { return 2; }, std::numeric_limits<size_t>::max() / 2))
            {
                borrowed = true;
                pool.returnObject(std::move(borrowed_object));
            }
        });

    /// The borrower is asleep on the full pool rather than failed at once.
    EXPECT_TRUE(waitUntilWaitingBorrowers(pool, 1));

    pool.returnObject(std::move(object));
    borrower.join();
    EXPECT_TRUE(borrowed.load());
}
