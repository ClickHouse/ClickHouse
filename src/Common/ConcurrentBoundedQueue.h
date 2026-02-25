#pragma once

#include <deque>
#include <condition_variable>
#include <mutex>
#include <optional>

#include <base/MoveOrCopyIfThrow.h>
#include <base/defines.h>

namespace detail
{
template <typename T>
struct UnitWeight
{
    size_t operator()(const T &) const { return 1; }
};
}

/** A very simple thread-safe queue of limited size.
  * If you try to pop an item from an empty queue, the thread is blocked until the queue becomes nonempty or queue is finished.
  * If you try to push an element into an overflowed queue, the thread is blocked until space appears in the queue or queue is finished.
  *
  * WeightFunction maps each element to its weight; the default assigns weight 1 to every element,
  * preserving the original "bounded by element count" behaviour.
  * max_fill is the capacity limit expressed in total weight units.
  *
  * Push admission policy: a thread is admitted to push as soon as the current total weight
  * drops below max_fill (i.e. the predicate is current_weight < max_fill, not
  * current_weight + item_weight <= max_fill). As a consequence, after a push the total
  * weight may transiently exceed max_fill by at most one item's weight. This matches the
  * original element-count semantics and avoids starvation of high-weight items.
  */
template <typename T, typename WeightFunction = detail::UnitWeight<T>>
class ConcurrentBoundedQueue
{
private:
    using Container = std::deque<T>;
    Container queue;

    mutable std::mutex queue_mutex;
    std::condition_variable push_condition;
    std::condition_variable pop_condition;

    bool is_finished = false;

    size_t max_fill = 0;
    size_t current_weight = 0;
    WeightFunction weight_function;

    template <bool back, typename ... Args>
    bool emplaceImpl(std::optional<UInt64> timeout_milliseconds, Args &&...args)
    {
        {
            std::unique_lock<std::mutex> queue_lock(queue_mutex);

            auto predicate = [&]() { return is_finished || current_weight < max_fill; };

            if (timeout_milliseconds.has_value())
            {
                bool wait_result = push_condition.wait_for(queue_lock, std::chrono::milliseconds(timeout_milliseconds.value()), predicate);

                if (!wait_result)
                    return false;
            }
            else
            {
                push_condition.wait(queue_lock, predicate);
            }

            if (is_finished)
                return false;

            if constexpr (back)
            {
                queue.emplace_back(std::forward<Args>(args)...);
                current_weight += weight_function(queue.back());
            }
            else
            {
                queue.emplace_front(std::forward<Args>(args)...);
                current_weight += weight_function(queue.front());
            }
        }

        pop_condition.notify_one();
        return true;
    }

    template <bool front>
    bool popImpl(T & x, std::optional<UInt64> timeout_milliseconds)
    {
        {
            std::unique_lock<std::mutex> queue_lock(queue_mutex);

            auto predicate = [&]() { return is_finished || !queue.empty(); };

            if (timeout_milliseconds.has_value())
            {
                bool wait_result = pop_condition.wait_for(queue_lock, std::chrono::milliseconds(timeout_milliseconds.value()), predicate);

                if (!wait_result)
                    return false;
            }
            else
            {
                pop_condition.wait(queue_lock, predicate);
            }

            if (is_finished && queue.empty())
                return false;

            if constexpr (front)
            {
                current_weight -= weight_function(queue.front());
                detail::moveOrCopyIfThrow(std::move(queue.front()), x);
                queue.pop_front();
            }
            else
            {
                current_weight -= weight_function(queue.back());
                detail::moveOrCopyIfThrow(std::move(queue.back()), x);
                queue.pop_back();
            }
        }

        push_condition.notify_one();
        return true;
    }

public:

    explicit ConcurrentBoundedQueue(size_t max_fill_, WeightFunction weight_function_ = WeightFunction{})
        : max_fill(max_fill_)
        , weight_function(std::move(weight_function_))
    {
    }


    /// Returns false if queue is finished
    [[nodiscard]] bool pushFront(const T & x)
    {
        return emplaceImpl</* back= */ false>(/* timeout_milliseconds= */ std::nullopt, x);
    }

    /// Returns false if queue is finished
    [[nodiscard]] bool push(const T & x)
    {
        return emplace(x);
    }

    [[nodiscard]] bool push(T && x)
    {
        return emplace(std::move(x));
    }

    /// Returns false if queue is finished
    template <typename... Args>
    [[nodiscard]] bool emplace(Args &&... args)
    {
        return emplaceImpl</* back= */ true>(std::nullopt /* timeout in milliseconds */, std::forward<Args>(args)...);
    }

    /// Returns false if queue is finished or object was not pushed during timeout
    [[nodiscard]] bool tryPush(const T & x, UInt64 milliseconds = 0)
    {
        return emplaceImpl</* back= */ true>(milliseconds, x);
    }

    [[nodiscard]] bool tryPush(T && x, UInt64 milliseconds = 0)
    {
        return emplaceImpl</* back= */ true>(milliseconds, std::move(x));
    }

    /// Returns false if queue is finished or object was not emplaced during timeout
    template <typename... Args>
    [[nodiscard]] bool tryEmplace(UInt64 milliseconds, Args &&... args)
    {
        return emplaceImpl</* back= */ true>(milliseconds, std::forward<Args>(args)...);
    }

    /// Returns false if queue is finished and empty
    [[nodiscard]] bool pop(T & x)
    {
        return popImpl</* front= */ true>(x, std::nullopt /*timeout in milliseconds*/);
    }

    /// Returns false if queue is (finished and empty) or (object was not popped during timeout)
    [[nodiscard]] bool tryPop(T & x, UInt64 milliseconds)
    {
        return popImpl</* front= */ true>(x, milliseconds);
    }

    /// Returns false if queue is empty.
    [[nodiscard]] bool tryPop(T & x)
    {
        // we don't use popImpl to avoid CV wait
        {
            std::lock_guard queue_lock(queue_mutex);

            if (queue.empty())
                return false;

            current_weight -= weight_function(queue.front());
            detail::moveOrCopyIfThrow(std::move(queue.front()), x);
            queue.pop_front();
        }

        push_condition.notify_one();
        return true;
    }

    /// Returns size of queue
    size_t size() const
    {
        std::lock_guard lock(queue_mutex);
        return queue.size();
    }

    /// Returns if queue is empty
    bool empty() const
    {
        std::lock_guard lock(queue_mutex);
        return queue.empty();
    }

    /** Clear and finish queue
      * After that push operation will return false
      * pop operations will return values until queue become empty
      * Returns true if queue was already finished
      */
    bool finish()
    {
        {
            std::lock_guard lock(queue_mutex);

            if (is_finished)
                return true;

            is_finished = true;
        }

        pop_condition.notify_all();
        push_condition.notify_all();
        return false;
    }

    /// Returns if queue is finished
    bool isFinished() const
    {
        std::lock_guard lock(queue_mutex);
        return is_finished;
    }

    /// Returns if queue is finished and empty
    bool isFinishedAndEmpty() const
    {
        std::lock_guard lock(queue_mutex);
        return is_finished && queue.empty();
    }

    /// Clear queue
    void clear()
    {
        {
            std::lock_guard lock(queue_mutex);

            if (is_finished)
                return;

            Container empty_queue;
            queue.swap(empty_queue);
            current_weight = 0;
        }

        push_condition.notify_all();
    }

    /// Clear and finish queue
    void clearAndFinish() noexcept
    {
        {
            std::lock_guard lock(queue_mutex);

            Container empty_queue;
            queue.swap(empty_queue);
            current_weight = 0;
            is_finished = true;
        }

        pop_condition.notify_all();
        push_condition.notify_all();
    }
};
