#include <cstdint>
#include <vector>
#include <chrono>
#include <mutex>
#include <condition_variable>

#include <common/defines.h>

#include <Common/MoveOrCopyIfThrow.h>

/// TODO: Add documentation
template <typename T>
class BorrowedObjectPool final
{
public:
    explicit BorrowedObjectPool(size_t max_size_) : max_size(max_size_) {}

    template <typename FactoryFunc>
    T borrowObject(FactoryFunc && func)
    {
        return borrowObjectImpl<NoTimeoutStrategy>(std::forward<FactoryFunc>(func), NoTimeoutStrategy());
    }

    template <typename FactoryFunc>
    T tryBorrowObject(FactoryFunc && func, size_t timeout_in_milliseconds = 0)
    {
        return borrowObjectImpl<WaitTimeoutStrategy>(std::forward<FactoryFunc>(func), WaitTimeoutStrategy{timeout_in_milliseconds});
    }

    ALWAYS_INLINE inline void returnObject(T && object_to_return)
    {
        std::unique_lock<std::mutex> lck(objects_mutex);

        objects.emplace_back(std::move(object_to_return));
        --borrowed_objects_size;

        condition_variable.notify_one();
    }

    ALWAYS_INLINE inline size_t allocatedObjectsSize() const
    {
        std::unique_lock<std::mutex> lock(objects_mutex);
        return allocated_objects_size;
    }

    ALWAYS_INLINE inline size_t borrowedObjectsSize() const
    {
        std::unique_lock<std::mutex> lock(objects_mutex);
        return borrowed_objects_size;
    }

private:

    struct NoTimeoutStrategy {};

    struct WaitTimeoutStrategy { size_t timeout_in_milliseconds; };

    template <typename TimeoutStrategy, typename FactoryFunc>
    ALWAYS_INLINE inline T borrowObjectImpl(FactoryFunc && func, TimeoutStrategy strategy [[maybe_unused]])
    {
        std::unique_lock<std::mutex> lock(objects_mutex);

        if (!objects.empty())
                return borrowFromObjects();

        bool has_unlimited_size = (max_size == 0);

        if (unlikely(has_unlimited_size))
            return allocateObjectForBorrowing(std::forward<FactoryFunc>(func));

        if (allocated_objects_size < max_size)
            return allocateObjectForBorrowing(std::forward<FactoryFunc>(func));

        if constexpr (std::is_same_v<TimeoutStrategy, WaitTimeoutStrategy>)
            condition_variable.wait_for(lock, std::chrono::milliseconds(strategy.timeout_in_milliseconds), [this] { return !objects.empty(); });
        else
            condition_variable.wait(lock, [this] { return !objects.empty(); });

        return borrowFromObjects();
    }

    template <typename FactoryFunc>
    ALWAYS_INLINE inline T allocateObjectForBorrowing(FactoryFunc && func)
    {
        ++allocated_objects_size;
        ++borrowed_objects_size;

        return std::forward<FactoryFunc>(func)();
    }

    ALWAYS_INLINE inline T borrowFromObjects()
    {
        T dst;
        detail::moveOrCopyIfThrow(std::move(objects.back()), dst);
        objects.pop_back();

        ++borrowed_objects_size;

        return dst;
    }

    ALWAYS_INLINE inline bool hasUnlimitedSize() const
    {
        return max_size == 0;
    }

    size_t max_size;

    mutable std::mutex objects_mutex;
    std::condition_variable condition_variable;
    size_t allocated_objects_size = 0;
    size_t borrowed_objects_size = 0;
    std::vector<T> objects;
};
