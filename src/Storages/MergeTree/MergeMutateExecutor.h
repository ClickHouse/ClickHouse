#pragma once

#include <deque>
#include <functional>
#include <atomic>
#include <mutex>
#include <future>
#include <condition_variable>
#include <set>

#include <boost/circular_buffer.hpp>

#include <common/shared_ptr_helper.h>
#include <common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Common/Stopwatch.h>
#include <Storages/MergeTree/ExecutableTask.h>


namespace DB
{

/**
 *  Executor for a background MergeTree related operations such as merges, mutations, fetches an so on.
 *  It can execute only successors of ExecutableTask interface.
 *  Which is a self-written coroutine. It suspends, when returns true from execute() method.
 *
 *  Executor consists of ThreadPool to execute pieces of a task (basically calls 'execute' on a task)
 *  and a scheduler thread, which manages the tasks. Due to bad experience of working with high memory under
 *  high memory pressure scheduler thread mustn't do any allocations,
 *  because it will be a fatal error if this thread will die from a random exception.
 *
 *  There are two queues of a tasks: pending (main queue for all the tasks) and active (currently executing).
 *  There is an invariant, that task may occur only in one of these queue. It can occur in both queues only in critical sections.
 *
 *  Due to all caveats I described above we use boost::circular_buffer as a container for queues.
 *
 *  Another nuisance that we faces with is than backgroud operations always interacts with an associated Storage.
 *  So, when a Storage want to shutdown, it must wait until all its background operaions are finished.
 */
class MergeTreeBackgroundExecutor : public shared_ptr_helper<MergeTreeBackgroundExecutor>
{
public:

    using CountGetter = std::function<size_t()>;
    using Callback = std::function<void()>;

    enum class Type
    {
        MERGE_MUTATE,
        FETCH,
        MOVE
    };

    MergeTreeBackgroundExecutor(
        Type type_,
        CountGetter && threads_count_getter_,
        CountGetter && max_task_count_getter_,
        CurrentMetrics::Metric metric_)
        : type(type_)
        , threads_count_getter(threads_count_getter_)
        , max_task_count_getter(max_task_count_getter_)
        , metric(metric_)
    {
        name = toString(type);

        updateConfiguration();
        scheduler = ThreadFromGlobalPool([this]() { schedulerThreadFunction(); });
    }

    ~MergeTreeBackgroundExecutor()
    {
        wait();
    }

    bool trySchedule(ExecutableTaskPtr task);

    void removeTasksCorrespondingToStorage(StorageID id);

    void wait();

    size_t activeCount()
    {
        std::lock_guard lock(mutex);
        return active.size();
    }

    size_t pendingCount()
    {
        std::lock_guard lock(mutex);
        return pending.size();
    }

private:

    void updateConfiguration();

    static String toString(Type type);

    Type type;
    String name;
    CountGetter threads_count_getter;
    CountGetter max_task_count_getter;
    CurrentMetrics::Metric metric;

    size_t threads_count{0};
    size_t max_tasks_count{0};

    AtomicStopwatch update_timer;

    struct Item
    {
        explicit Item(ExecutableTaskPtr && task_, CurrentMetrics::Metric metric_)
            : task(std::move(task_))
            , increment(std::move(metric_))
        {
        }

        ExecutableTaskPtr task;
        CurrentMetrics::Increment increment;
        Poco::Event is_done;
    };

    using ItemPtr = std::shared_ptr<Item>;

    void routine(ItemPtr item);
    void schedulerThreadFunction();

    /// Initially it will be empty
    boost::circular_buffer<ItemPtr> pending{0};
    boost::circular_buffer<ItemPtr> active{0};
    std::set<StorageID> currently_deleting;

    std::mutex remove_mutex;
    std::mutex mutex;
    std::condition_variable has_tasks;

    std::atomic_bool shutdown{false};

    ThreadPool pool;
    ThreadFromGlobalPool scheduler;
};

}
