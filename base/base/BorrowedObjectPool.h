#pragma once

#include <cstdint>
#include <vector>
#include <chrono>
#include <mutex>
#include <condition_variable>

#include <base/defines.h>
#include <base/MoveOrCopyIfThrow.h>

/** Pool for limited size objects that cannot be used from different threads simultaneously.
  * The main use case is to have fixed size of objects that can be reused in different threads during their lifetime
  * and have to be initialized on demand.
  * Two main properties of pool are allocated objects size and borrowed objects size.
  * Allocated objects size is size of objects that are currently allocated by the pool.
  * Borrowed objects size is size of objects that are borrowed by clients.
  * If max_size == 0 then pool has unlimited size and objects will be allocated without limit.
  *
  * Pool provides following strategy for borrowing object:
  * If max_size == 0 then pool has unlimited size and objects will be allocated without limit.
  * 1. If pool has objects that can be borrowed increase borrowed objects size and return it.
  * 2. If pool allocatedObjectsSize is lower than max objects size or pool has unlimited size
  * allocate new object, increase borrowed objects size and return it.
  * 3. If pool is full wait on condition variable with or without timeout until some object
  * will be returned to the pool.
  */
template <typename T>
class BorrowedObjectPool final
{
public:
    explicit BorrowedObjectPool(size_t max_size_) : max_size(max_size_) {}

    /// Borrow object from pool. If pull is full and all objects were borrowed
    /// then calling thread will wait until some object will be returned into pool.
    template <typename FactoryFunc>
    void borrowObject(T & dest, FactoryFunc && func)
    {
        std::unique_lock<std::mutex> lock(objects_mutex);

        while (true)
        {
            if (!objects.empty())
            {
                borrowFromObjects(lock, dest);
                return;
            }

            if (canAllocate())
            {
                allocateObjectForBorrowing(lock, dest, std::forward<FactoryFunc>(func));
                return;
            }

            ++waiting_borrowers_size;
            condition_variable.wait(lock, [this] { return canBorrowOrAllocate(); });
            --waiting_borrowers_size;
        }
    }

    /// Same as borrowObject function, but wait with timeout.
    /// Returns true if object was borrowed during timeout.
    template <typename FactoryFunc>
    bool tryBorrowObject(T & dest, FactoryFunc && func, size_t timeout_in_milliseconds = 0)
    {
        std::unique_lock<std::mutex> lock(objects_mutex);

        /// One deadline for the whole call: the wait below can be entered more than once (a slot
        /// that frees up wakes this thread, and another thread may take it first), and restarting
        /// the timeout each time would let the call outlast the timeout it was given.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_in_milliseconds);

        while (true)
        {
            if (!objects.empty())
            {
                borrowFromObjects(lock, dest);
                return true;
            }

            if (canAllocate())
            {
                allocateObjectForBorrowing(lock, dest, std::forward<FactoryFunc>(func));
                return true;
            }

            ++waiting_borrowers_size;
            const bool woken = condition_variable.wait_until(lock, deadline, [this] { return canBorrowOrAllocate(); });
            --waiting_borrowers_size;

            if (!woken)
                return false;
        }
    }

    /// Return object into pool. Client must return same object that was borrowed.
    void returnObject(T && object_to_return)
    {
        {
            std::lock_guard lock(objects_mutex);

            try
            {
                objects.emplace_back(std::move(object_to_return));
            }
            catch (...)
            {
                /// The object does not make it back into the pool, so the pool must not keep
                /// counting it: otherwise every such failure permanently costs one slot of
                /// `max_size`, and after enough of them borrowing only ever times out.
                --allocated_objects_size;
                --borrowed_objects_size;

                /// Nothing was pushed into `objects`, so the freed slot is the only thing a waiter
                /// can go on, and it is what waiters watch for besides a returned object. Wake one
                /// here - the notify after this block is skipped by the unwinding - or a pool with
                /// `max_size == 1` leaves its only waiter asleep with the pool standing empty.
                condition_variable.notify_one();
                throw;
            }

            --borrowed_objects_size;
        }

        condition_variable.notify_one();
    }

    /// Max pool size
    size_t maxSize() const
    {
        return max_size;
    }

    /// Allocated objects size by the pool. If allocatedObjectsSize == maxSize then pool is full.
    size_t allocatedObjectsSize() const
    {
        std::lock_guard lock(objects_mutex);
        return allocated_objects_size;
    }

    /// Returns allocatedObjectsSize == maxSize
    bool isFull() const
    {
        std::lock_guard lock(objects_mutex);
        return allocated_objects_size == max_size;
    }

    /// Number of threads currently blocked inside `borrowObject`/`tryBorrowObject` waiting for an
    /// object to be returned or for a slot to free up. The counter is incremented under
    /// `objects_mutex` before the wait, which only releases the mutex once the thread is registered
    /// on the condition variable: another thread that acquires the mutex and sees a non-zero count
    /// therefore knows the waiter is asleep and will observe a `notify_one`. That is what makes it
    /// usable as a synchronization point rather than only as a statistic.
    size_t waitingBorrowersSize() const
    {
        std::lock_guard lock(objects_mutex);
        return waiting_borrowers_size;
    }

    /// Borrowed objects size. If borrowedObjectsSize == allocatedObjectsSize and pool is full.
    /// Then client will wait during borrowObject function call.
    size_t borrowedObjectsSize() const
    {
        std::lock_guard lock(objects_mutex);
        return borrowed_objects_size;
    }

private:

    /// Both must be called under `objects_mutex`.

    bool canAllocate() const
    {
        bool has_unlimited_size = (max_size == 0);
        return unlikely(has_unlimited_size) || allocated_objects_size < max_size;
    }

    /// What a waiting borrower is waiting for. A returned object is the usual case, but free
    /// capacity counts too: `returnObject` can fail to put the object back and give up its slot
    /// instead, and then allocating a replacement is the only way forward.
    bool canBorrowOrAllocate() const
    {
        return !objects.empty() || canAllocate();
    }

    template <typename FactoryFunc>
    void allocateObjectForBorrowing(const std::unique_lock<std::mutex> &, T & dest, FactoryFunc && func)
    {
        ++allocated_objects_size;
        ++borrowed_objects_size;

        try
        {
            /// The hand-over to `dest` is inside the guard, not just the factory call: it is an
            /// assignment of a user-supplied type and may throw in its own right, and a slot that
            /// is accounted for but was never handed to anybody is a slot the pool loses for the
            /// rest of the process - with `max_size` of them, every later borrow times out.
            dest = std::forward<FactoryFunc>(func)();
        }
        catch (...)
        {
            /// No object was created or borrowed, so a failed factory must not consume one of the
            /// pool's slots permanently.
            --allocated_objects_size;
            --borrowed_objects_size;

            /// And the slot it gives back is what the next waiter proceeds on - nothing was pushed
            /// into `objects` here either - so one has to be woken for it, exactly as in
            /// `returnObject`. Without this a waiter that was woken by some earlier freed slot,
            /// only to have its own factory fail, takes the wakeup with it and leaves the waiter
            /// behind it asleep with the pool below `max_size`.
            condition_variable.notify_one();
            throw;
        }
    }

    void borrowFromObjects(const std::unique_lock<std::mutex> &, T & dest)
    {
        ++borrowed_objects_size;

        try
        {
            detail::moveOrCopyIfThrow(std::move(objects.back()), dest);
        }
        catch (...)
        {
            /// Nothing has left the pool: `moveOrCopyIfThrow` copies exactly when a move could
            /// throw, so a failure here leaves `objects.back()` intact and only the count to undo.
            /// Skipping this would keep an object that is still in the pool counted as borrowed,
            /// and after `max_size` such failures the pool would refuse to lend anything at all.
            --borrowed_objects_size;
            throw;
        }

        objects.pop_back();
    }

    size_t max_size;

    mutable std::mutex objects_mutex;
    std::condition_variable condition_variable;
    size_t allocated_objects_size = 0;
    size_t borrowed_objects_size = 0;
    size_t waiting_borrowers_size = 0;
    std::vector<T> objects;
};
