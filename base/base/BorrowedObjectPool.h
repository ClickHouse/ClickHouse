#pragma once

#include <cstdint>
#include <vector>
#include <chrono>
#include <mutex>
#include <condition_variable>

#include <base/defines.h>
#include <base/MoveOrCopyIfThrow.h>

/** Pool for limited size objects that cannot be used from different threads simultaneously.
  * The main use case is to have fixed size of objects that can be reused in difference threads during their lifetime
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

        if (!objects.empty())
        {
            dest = borrowFromObjects(lock);
            return;
        }

        bool has_unlimited_size = (max_size == 0);

        if (unlikely(has_unlimited_size) || allocated_objects_size < max_size)
        {
            dest = allocateObjectForBorrowing(lock, std::forward<FactoryFunc>(func));
            return;
        }

        condition_variable.wait(lock, [this] { return !objects.empty(); });
        dest = borrowFromObjects(lock);
    }

    /// Same as borrowObject function, but wait with timeout.
    /// Returns true if object was borrowed during timeout.
    template <typename FactoryFunc>
    bool tryBorrowObject(T & dest, FactoryFunc && func, size_t timeout_in_milliseconds = 0)
    {
        std::unique_lock<std::mutex> lock(objects_mutex);

        if (!objects.empty())
        {
            dest = borrowFromObjects(lock);
            return true;
        }

        bool has_unlimited_size = (max_size == 0);

        if (unlikely(has_unlimited_size) || allocated_objects_size < max_size)
        {
            dest = allocateObjectForBorrowing(lock, std::forward<FactoryFunc>(func));
            return true;
        }

        bool wait_result = condition_variable.wait_for(lock, std::chrono::milliseconds(timeout_in_milliseconds), [this] { return !objects.empty(); });

        if (wait_result)
            dest = borrowFromObjects(lock);

        return wait_result;
    }

    /// Return object into pool. Client must return same object that was borrowed.
    inline void returnObject(T && object_to_return)
    {
        {
            std::lock_guard lock(objects_mutex);

            objects.emplace_back(std::move(object_to_return));
            --borrowed_objects_size;
        }

        condition_variable.notify_one();
    }

    /// Max pool size
    inline size_t maxSize() const
    {
        return max_size;
    }

    /// Allocated objects size by the pool. If allocatedObjectsSize == maxSize then pool is full.
    inline size_t allocatedObjectsSize() const
    {
        std::lock_guard lock(objects_mutex);
        return allocated_objects_size;
    }

    /// Returns allocatedObjectsSize == maxSize
    inline bool isFull() const
    {
        std::lock_guard lock(objects_mutex);
        return allocated_objects_size == max_size;
    }

    /// Borrowed objects size. If borrowedObjectsSize == allocatedObjectsSize and pool is full.
    /// Then client will wait during borrowObject function call.
    inline size_t borrowedObjectsSize() const
    {
        std::lock_guard lock(objects_mutex);
        return borrowed_objects_size;
    }

private:

    template <typename FactoryFunc>
    inline T allocateObjectForBorrowing(const std::unique_lock<std::mutex> &, FactoryFunc && func)
    {
        ++allocated_objects_size;
        ++borrowed_objects_size;

        return std::forward<FactoryFunc>(func)();
    }

    inline T borrowFromObjects(const std::unique_lock<std::mutex> &)
    {
        T dst;
        detail::moveOrCopyIfThrow(std::move(objects.back()), dst);
        objects.pop_back();

        ++borrowed_objects_size;

        return dst;
    }

    size_t max_size;

    mutable std::mutex objects_mutex;
    std::condition_variable condition_variable;
    size_t allocated_objects_size = 0;
    size_t borrowed_objects_size = 0;
    std::vector<T> objects;
};
