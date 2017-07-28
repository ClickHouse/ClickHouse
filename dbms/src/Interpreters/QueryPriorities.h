#pragma once

#include <map>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <chrono>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>


namespace CurrentMetrics
{
    extern const Metric QueryPreempted;
}


namespace DB
{

/** Implements query priorities in very primitive way.
  * Allows to freeze query execution if at least one query of higher priority is executed.
  *
  * Priority value is integer, smaller means higher priority.
  *
  * Priority 0 is special - queries with that priority is always executed,
  *  not depends on other queries and not affect other queries.
  * Thus 0 means - don't use priorities.
  *
  * NOTE Possibilities for improvement:
  * - implement limit on maximum number of running queries with same priority.
  */
class QueryPriorities
{
public:
    using Priority = int;

private:
    friend struct Handle;

    using Count = int;

    /// Number of currently running queries for each priority.
    using Container = std::map<Priority, Count>;

    std::mutex mutex;
    std::condition_variable condvar;
    Container container;


    /** If there are higher priority queries - sleep until they are finish or timeout happens.
      * Returns true, if higher priority queries has finished at return of function, false, if timout exceeded.
      */
    template <typename Duration>
    bool waitIfNeed(Priority priority, Duration timeout)
    {
        if (0 == priority)
            return true;

        std::chrono::nanoseconds cur_timeout = timeout;
        Stopwatch watch(CLOCK_MONOTONIC_COARSE);

        std::unique_lock<std::mutex> lock(mutex);

        while (true)
        {
            /// Is there at least one more priority query?
            bool found = false;
            for (const auto & value : container)
            {
                if (value.first >= priority)
                    break;

                if (value.second > 0)
                {
                    found = true;
                    break;
                }
            }

            if (!found)
                return true;

            CurrentMetrics::Increment metric_increment{CurrentMetrics::QueryPreempted};
            if (std::cv_status::timeout == condvar.wait_for(lock, cur_timeout))
                return false;
            else
            {
                /// False awakening, check and update time limit
                auto elapsed = std::chrono::nanoseconds(watch.elapsed());
                if (elapsed >= timeout)
                    return false;
                cur_timeout = timeout - elapsed;
            }
        }
    }

public:
    struct HandleImpl
    {
    private:
        QueryPriorities & parent;
        QueryPriorities::Container::value_type & value;

    public:
        HandleImpl(QueryPriorities & parent_, QueryPriorities::Container::value_type & value_)
            : parent(parent_), value(value_) {}

        ~HandleImpl()
        {
            {
                std::lock_guard<std::mutex> lock(parent.mutex);
                --value.second;
            }
            parent.condvar.notify_all();
        }

        template <typename Duration>
        bool waitIfNeed(Duration timeout)
        {
            return parent.waitIfNeed(value.first, timeout);
        }
    };

    using Handle = std::shared_ptr<HandleImpl>;

    /** Register query with specified priority.
      * Returns an object that remove record in destructor.
      */
    Handle insert(Priority priority)
    {
        if (0 == priority)
            return {};

        std::lock_guard<std::mutex> lock(mutex);
        auto it = container.emplace(priority, 0).first;
        ++it->second;
        return std::make_shared<HandleImpl>(*this, *it);
    }
};

}
