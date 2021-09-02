#pragma once

#include <deque>
#include <functional>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <unordered_set>

#include <common/shared_ptr_helper.h>
#include <Common/ThreadPool.h>
#include <Common/Stopwatch.h>
#include <Common/RingBuffer.h>
#include <Common/PlainMultiSet.h>
#include <Storages/MergeTree/ExecutableTask.h>
#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

class LambdaAdapter : public shared_ptr_helper<LambdaAdapter>, public ExecutableTask
{
public:

    template <typename T>
    explicit LambdaAdapter(T && inner_, MergeTreeData & data_) : inner(inner_), data(data_) {}

    bool execute() override
    {
        res = inner();
        inner = {};
        return false;
    }

    void onCompleted() override
    {
        data.triggerBackgroundOperationTask(!res);
    }

    StorageID getStorageID() override
    {
        return data.getStorageID();
    }

private:
    bool res = false;
    std::function<bool()> inner;
    MergeTreeData & data;
};


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

    bool trySchedule(ExecutableTaskPtr task)
    {
        std::lock_guard lock(mutex);

        if (shutdown_suspend)
            return false;

        auto & value = CurrentMetrics::values[metric];
        if (value.load() >= static_cast<int64_t>(max_tasks_count))
            return false;

        if (!pending.tryPush(std::make_shared<Item>(std::move(task), metric)))
            return false;


        has_tasks.notify_one();
        return true;
    }

    void removeTasksCorrespondingToStorage(StorageID id);

    void wait()
    {
        {
            std::lock_guard lock(mutex);
            shutdown_suspend = true;
            has_tasks.notify_all();
        }

        if (scheduler.joinable())
            scheduler.join();

        pool.wait();
    }

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

    void updateConfiguration()
    {
        auto new_threads_count = threads_count_getter();
        auto new_max_tasks_count = max_task_count_getter();

        try
        {
            pending.resize(new_max_tasks_count);
            active.reserve(new_max_tasks_count);

            pool.setMaxFreeThreads(0);
            pool.setMaxThreads(new_threads_count);
            pool.setQueueSize(new_max_tasks_count);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        threads_count = new_threads_count;
        max_tasks_count = new_max_tasks_count;
    }

    void schedulerThreadFunction();

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
            , future(promise.get_future())
        {
        }

        ExecutableTaskPtr task;
        CurrentMetrics::Increment increment;

        std::promise<void> promise;
        std::future<void> future;
    };

    using ItemPtr = std::shared_ptr<Item>;

    /// Initially it will be empty
    RingBuffer<ItemPtr> pending{0};
    PlainMultiSet<ItemPtr> active{0};
    std::set<StorageID> currently_deleting;

    std::mutex remove_mutex;
    std::mutex mutex;
    std::condition_variable has_tasks;

    std::atomic_bool shutdown_suspend{false};

    ThreadPool pool;
    ThreadFromGlobalPool scheduler;
};

}
