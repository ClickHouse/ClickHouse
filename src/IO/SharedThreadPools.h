#pragma once

#include <Common/ThreadPool.h>
#include <Common/CurrentMetrics.h>
#include <cstdlib>
#include <memory>

namespace DB
{

template <typename Derived>
class StaticThreadPool
{
public:
    static void initialize(size_t max_threads, size_t max_free_threads, size_t queue_size);
    static void reloadConfiguration(size_t max_threads, size_t max_free_threads, size_t queue_size);
    static ThreadPool & get();
};


/// ThreadPool used for the IO.
class IOThreadPool : public StaticThreadPool<IOThreadPool>
{
    friend class StaticThreadPool;
    static std::unique_ptr<ThreadPool> instance;
    static CurrentMetrics::Metric threads_metric;
    static CurrentMetrics::Metric threads_active_metric;
};


/// ThreadPool used for the Backup IO.
class BackupsIOThreadPool : public StaticThreadPool<BackupsIOThreadPool>
{
    friend class StaticThreadPool;
    static std::unique_ptr<ThreadPool> instance;
    static CurrentMetrics::Metric threads_metric;
    static CurrentMetrics::Metric threads_active_metric;
};


/// ThreadPool used for the loading of Outdated data parts for MergeTree tables.
class ActivePartsLoadingThreadPool : public StaticThreadPool<ActivePartsLoadingThreadPool>
{
    friend class StaticThreadPool;
    static std::unique_ptr<ThreadPool> instance;
    static CurrentMetrics::Metric threads_metric;
    static CurrentMetrics::Metric threads_active_metric;
};


/// ThreadPool used for deleting data parts for MergeTree tables.
class PartsCleaningThreadPool : public StaticThreadPool<PartsCleaningThreadPool>
{
    friend class StaticThreadPool;
    static std::unique_ptr<ThreadPool> instance;
    static CurrentMetrics::Metric threads_metric;
    static CurrentMetrics::Metric threads_active_metric;
};


/// This ThreadPool used for the loading of Outdated data parts for MergeTree tables.
/// Normally we will just load Outdated data parts concurrently in background, but in
/// case when we need to synchronously wait for the loading to be finished, we can increase
/// the number of threads by calling enableTurboMode() :-)
class OutdatedPartsLoadingThreadPool : public StaticThreadPool<OutdatedPartsLoadingThreadPool>
{
    friend class StaticThreadPool;
    static std::mutex mutex;
    static size_t max_threads_turbo;
    static size_t max_threads;
    /// If this counter is > 0 - this specific mode is enabled
    static size_t turbo_mode_enabled;
    static std::unique_ptr<ThreadPool> instance;
    static CurrentMetrics::Metric threads_metric;
    static CurrentMetrics::Metric threads_active_metric;

public:
    static void setMaxTurboThreads(size_t max_threads_turbo_);
    static void enableTurboMode();
    static void disableTurboMode();
};


extern template class StaticThreadPool<IOThreadPool>;
extern template class StaticThreadPool<BackupsIOThreadPool>;
extern template class StaticThreadPool<ActivePartsLoadingThreadPool>;
extern template class StaticThreadPool<PartsCleaningThreadPool>;
extern template class StaticThreadPool<ActivePartsLoadingThreadPool>;

}
