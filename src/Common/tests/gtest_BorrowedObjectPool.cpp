#include <base/BorrowedObjectPool.h>

#include <atomic>
#include <chrono>
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

    while (pool.waitingBorrowersSize() < count)
    {
        if (std::chrono::steady_clock::now() > deadline)
            return false;
        std::this_thread::yield();
    }

    return true;
}

/// An object whose move construction - the operation `returnObject` uses to put it back into the
/// pool - can be made to throw. Move assignment stays `noexcept` so that borrowing an object is
/// unaffected: only the return path is interesting here.
struct FailsToReturn
{
    bool throw_on_move_construction = false;

    FailsToReturn() = default;
    explicit FailsToReturn(bool throw_on_move_construction_) : throw_on_move_construction(throw_on_move_construction_) {}

    /// Throwing is the whole point of this type.
    /// NOLINTNEXTLINE(performance-noexcept-move-constructor, hicpp-noexcept-move)
    FailsToReturn(FailsToReturn && other) : throw_on_move_construction(other.throw_on_move_construction)
    {
        if (throw_on_move_construction)
            throw std::runtime_error("cannot be returned into the pool");
    }

    FailsToReturn & operator=(FailsToReturn && other) noexcept
    {
        throw_on_move_construction = other.throw_on_move_construction;
        return *this;
    }
};

}

/// A return that fails gives up the slot it occupied, and with the pool at `max_size` that slot is
/// all a waiting borrower has to go on - nothing was pushed back for it to take. It must therefore
/// be woken and allowed to allocate a replacement, or the only waiter of a `max_size == 1` pool
/// sleeps forever with the pool standing empty.
TEST(BorrowedObjectPool, FailedReturnWakesWaitingBorrower)
{
    BorrowedObjectPool<FailsToReturn> pool(1);

    FailsToReturn object;
    ASSERT_TRUE(pool.tryBorrowObject(object, [] { return FailsToReturn(/*throw_on_move_construction_=*/ true); }));
    ASSERT_EQ(pool.allocatedObjectsSize(), 1);

    /// The pool is full and holds nothing, so this thread has no choice but to wait.
    std::atomic<bool> borrowed = false;
    std::atomic<size_t> borrow_duration_ms = 0;
    std::thread borrower(
        [&]
        {
            FailsToReturn borrowed_object;
            const auto started_at = std::chrono::steady_clock::now();
            const bool got_object = pool.tryBorrowObject(borrowed_object, [] { return FailsToReturn(); }, BORROW_TIMEOUT_MS);
            borrow_duration_ms = static_cast<size_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started_at).count());

            if (got_object)
            {
                borrowed = true;
                pool.returnObject(std::move(borrowed_object));
            }
        });

    /// The regression this test guards against is a waiter that is never woken, so the waiter has
    /// to be asleep before the wake-up is triggered - otherwise the test passes through the trivial
    /// path (a borrower that has not started waiting yet finds the slot free) and proves nothing.
    /// `waitingBorrowersSize` is what makes that deterministic instead of a matter of scheduling:
    /// the count is taken under the pool's mutex, which the waiter only releases from inside the
    /// wait, so seeing it means the waiter is registered on the condition variable.
    /// Not an ASSERT: returning from the test here would leave the borrower threads unjoined.
    EXPECT_TRUE(waitUntilWaitingBorrowers(pool, 1));

    EXPECT_THROW(pool.returnObject(std::move(object)), std::runtime_error);

    borrower.join();
    EXPECT_TRUE(borrowed.load());
    EXPECT_LT(borrow_duration_ms.load(), PROMPT_BORROW_MS);
    EXPECT_EQ(pool.allocatedObjectsSize(), 1);
    EXPECT_EQ(pool.borrowedObjectsSize(), 0);
}

/// Same for the other path that gives a slot back without putting an object into the pool: a
/// borrower that was let through by a freed slot, only for its own factory to fail. The slot is
/// released again, and the next waiter has to be told about it - the failing borrower is the one
/// that consumed the wakeup that let it try in the first place.
TEST(BorrowedObjectPool, FailedFactoryWakesWaitingBorrower)
{
    BorrowedObjectPool<FailsToReturn> pool(1);

    FailsToReturn object;
    ASSERT_TRUE(pool.tryBorrowObject(object, [] { return FailsToReturn(/*throw_on_move_construction_=*/ true); }));

    /// Two waiters, one behind the other on the single slot this pool has.
    std::atomic<size_t> failed_borrows = 0;
    std::atomic<size_t> successful_borrows = 0;
    std::atomic<bool> factory_should_fail = true;

    std::atomic<size_t> slowest_borrow_ms = 0;

    auto borrow = [&]
    {
        FailsToReturn borrowed_object;
        const auto started_at = std::chrono::steady_clock::now();
        try
        {
            /// The first waiter to get through fails; the slot it took must reach the other one.
            if (pool.tryBorrowObject(
                    borrowed_object,
                    [&]() -> FailsToReturn
                    {
                        if (factory_should_fail.exchange(false))
                            throw std::runtime_error("factory failed");
                        return FailsToReturn();
                    },
                    BORROW_TIMEOUT_MS))
            {
                ++successful_borrows;
                pool.returnObject(std::move(borrowed_object));
            }
        }
        catch (const std::runtime_error &)
        {
            ++failed_borrows;
        }

        /// Both outcomes belong to what is measured here: the borrower whose own factory failed
        /// waited for the slot exactly as the other one did, and either of them is the one a
        /// missing wake-up would have left asleep.
        const size_t elapsed_ms = static_cast<size_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started_at).count());
        size_t previous = slowest_borrow_ms.load();
        while (previous < elapsed_ms && !slowest_borrow_ms.compare_exchange_weak(previous, elapsed_ms))
        {
        }
    };

    std::thread first_waiter(borrow);
    std::thread second_waiter(borrow);

    /// Both have to be asleep before the slot is freed - see the note in the test above. Only then
    /// is it certain that the wake-up reaches one of them and that the failing factory's own
    /// wake-up is the only thing that can reach the other.
    /// Not an ASSERT: returning from the test here would leave the borrower threads unjoined.
    EXPECT_TRUE(waitUntilWaitingBorrowers(pool, 2));

    EXPECT_THROW(pool.returnObject(std::move(object)), std::runtime_error);

    first_waiter.join();
    second_waiter.join();

    EXPECT_LT(slowest_borrow_ms.load(), PROMPT_BORROW_MS);
    EXPECT_EQ(failed_borrows.load(), 1);
    EXPECT_EQ(successful_borrows.load(), 1);
    EXPECT_EQ(pool.allocatedObjectsSize(), 1);
    EXPECT_EQ(pool.borrowedObjectsSize(), 0);
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
    static inline bool fail_next_handover = false;

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
        if (!fail_next_handover)
            return;

        fail_next_handover = false;
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
