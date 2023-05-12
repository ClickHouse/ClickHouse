#include <IO/SharedThreadPools.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>
#include <Core/Field.h>

namespace CurrentMetrics
{
    extern const Metric IOThreads;
    extern const Metric IOThreadsActive;
    extern const Metric BackupsIOThreads;
    extern const Metric BackupsIOThreadsActive;
    extern const Metric MergeTreePartsLoaderThreads;
    extern const Metric MergeTreePartsLoaderThreadsActive;
    extern const Metric MergeTreePartsCleanerThreads;
    extern const Metric MergeTreePartsCleanerThreadsActive;
    extern const Metric MergeTreeOutdatedPartsLoaderThreads;
    extern const Metric MergeTreeOutdatedPartsLoaderThreadsActive;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename Derived>
void StaticThreadPool<Derived>::initialize(size_t max_threads, size_t max_free_threads, size_t queue_size)
{
    if (Derived::instance)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The thread pool is initialized twice");

    Derived::instance = std::make_unique<ThreadPool>(
        Derived::threads_metric,
        Derived::threads_active_metric,
        max_threads,
        max_free_threads,
        queue_size,
        /* shutdown_on_exception= */ false);
}

template <typename Derived>
void StaticThreadPool<Derived>::reloadConfiguration(size_t max_threads, size_t max_free_threads, size_t queue_size)
{
    if (!Derived::instance)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot reload configuration of the uninitialized pool");

    Derived::instance->setMaxThreads(max_threads);
    Derived::instance->setMaxFreeThreads(max_free_threads);
    Derived::instance->setQueueSize(queue_size);
}

template <typename Derived>
ThreadPool & StaticThreadPool<Derived>::get()
{
    if (!Derived::instance)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The thread pool is not initialized");

    return *Derived::instance;
}


template class StaticThreadPool<IOThreadPool>;
std::unique_ptr<ThreadPool> IOThreadPool::instance;
CurrentMetrics::Metric IOThreadPool::threads_metric = CurrentMetrics::IOThreads;
CurrentMetrics::Metric IOThreadPool::threads_active_metric = CurrentMetrics::IOThreadsActive;

template class StaticThreadPool<BackupsIOThreadPool>;
std::unique_ptr<ThreadPool> BackupsIOThreadPool::instance;
CurrentMetrics::Metric BackupsIOThreadPool::threads_metric = CurrentMetrics::BackupsIOThreads;
CurrentMetrics::Metric BackupsIOThreadPool::threads_active_metric = CurrentMetrics::BackupsIOThreadsActive;

template class StaticThreadPool<ActivePartsLoadingThreadPool>;
std::unique_ptr<ThreadPool> ActivePartsLoadingThreadPool::instance;
CurrentMetrics::Metric ActivePartsLoadingThreadPool::threads_metric = CurrentMetrics::MergeTreePartsLoaderThreads;
CurrentMetrics::Metric ActivePartsLoadingThreadPool::threads_active_metric = CurrentMetrics::MergeTreePartsLoaderThreadsActive;

template class StaticThreadPool<PartsCleaningThreadPool>;
std::unique_ptr<ThreadPool> PartsCleaningThreadPool::instance;
CurrentMetrics::Metric PartsCleaningThreadPool::threads_metric = CurrentMetrics::MergeTreePartsCleanerThreads;
CurrentMetrics::Metric PartsCleaningThreadPool::threads_active_metric = CurrentMetrics::MergeTreePartsCleanerThreadsActive;

template class StaticThreadPool<OutdatedPartsLoadingThreadPool>;
std::mutex OutdatedPartsLoadingThreadPool::mutex;
size_t OutdatedPartsLoadingThreadPool::max_threads_turbo = 0;
size_t OutdatedPartsLoadingThreadPool::max_threads = 0;
size_t OutdatedPartsLoadingThreadPool::turbo_mode_enabled = 0;
std::unique_ptr<ThreadPool> OutdatedPartsLoadingThreadPool::instance;
CurrentMetrics::Metric OutdatedPartsLoadingThreadPool::threads_metric = CurrentMetrics::MergeTreeOutdatedPartsLoaderThreads;
CurrentMetrics::Metric OutdatedPartsLoadingThreadPool::threads_active_metric = CurrentMetrics::MergeTreeOutdatedPartsLoaderThreadsActive;

void OutdatedPartsLoadingThreadPool::enableTurboMode()
{
    if (!instance)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The PartsLoadingThreadPool thread pool is not initialized");

    std::lock_guard lock(mutex);

    ++turbo_mode_enabled;
    if (turbo_mode_enabled == 1)
        instance->setMaxThreads(max_threads_turbo);
}

void OutdatedPartsLoadingThreadPool::disableTurboMode()
{
    if (!instance)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The PartsLoadingThreadPool thread pool is not initialized");

    std::lock_guard lock(mutex);

    --turbo_mode_enabled;
    if (turbo_mode_enabled == 0)
        instance->setMaxThreads(max_threads);
}

void OutdatedPartsLoadingThreadPool::setMaxTurboThreads(size_t max_threads_turbo_)
{
    if (!instance)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The PartsLoadingThreadPool thread pool is not initialized");

    std::lock_guard lock(mutex);

    max_threads_turbo = max_threads_turbo_;
    if (turbo_mode_enabled > 0)
        instance->setMaxThreads(max_threads_turbo);
}

}
