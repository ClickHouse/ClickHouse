#include <IO/SharedThreadPools.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Core/Field.h>

namespace CurrentMetrics
{
#define DECLARE_STATIC_THREAD_POOL_METRICS(SUFFIX, NAME, METRIC) \
    extern const Metric METRIC##Threads; \
    extern const Metric METRIC##ThreadsActive; \
    extern const Metric METRIC##ThreadsScheduled;
APPLY_FOR_STATIC_THREAD_POOLS(DECLARE_STATIC_THREAD_POOL_METRICS)
#undef DECLARE_STATIC_THREAD_POOL_METRICS
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


StaticThreadPool::StaticThreadPool(
    const String & name_,
    CurrentMetrics::Metric threads_metric_,
    CurrentMetrics::Metric threads_active_metric_,
    CurrentMetrics::Metric threads_scheduled_metric_)
    : name(name_)
    , threads_metric(threads_metric_)
    , threads_active_metric(threads_active_metric_)
    , threads_scheduled_metric(threads_scheduled_metric_)
{
}

void StaticThreadPool::initialize(size_t max_threads, size_t max_free_threads, size_t queue_size)
{
    if (instance)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The {} is initialized twice", name);

    std::call_once(init_flag, [&]
        {
            initializeImpl(max_threads, max_free_threads, queue_size);
        });
}

void StaticThreadPool::initializeWithDefaultSettingsIfNotInitialized()
{
    std::call_once(init_flag, [&]
        {
            size_t max_threads = getNumberOfCPUCoresToUse();
            initializeImpl(max_threads, /*max_free_threads*/ 0, /*queue_size*/ 10000);
        });
}

void StaticThreadPool::initializeImpl(size_t max_threads, size_t max_free_threads, size_t queue_size)
{
    /// By default enabling "turbo mode" won't affect the number of threads anyhow
    max_threads_turbo = max_threads;
    max_threads_normal = max_threads;
    instance = std::make_unique<ThreadPool>(
        threads_metric,
        threads_active_metric,
        threads_scheduled_metric,
        max_threads,
        max_free_threads,
        queue_size,
        /* shutdown_on_exception= */ false);
}

bool StaticThreadPool::isInitialized() const
{
    return instance.operator bool();
}

void StaticThreadPool::shutdown()
{
    std::lock_guard lock(mutex);
    instance.reset();
}

void StaticThreadPool::reloadConfiguration(size_t max_threads, size_t max_free_threads, size_t queue_size)
{
    if (!instance)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The {} is not initialized", name);

    std::lock_guard lock(mutex);

    instance->setMaxThreads(turbo_mode_enabled > 0 ? max_threads_turbo : max_threads);
    instance->setMaxFreeThreads(max_free_threads);
    instance->setQueueSize(queue_size);
}


ThreadPool & StaticThreadPool::get()
{
    if (!instance)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The {} is not initialized", name);

    return *instance;
}

void StaticThreadPool::enableTurboMode()
{
    if (!instance)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The {} is not initialized", name);

    std::lock_guard lock(mutex);

    ++turbo_mode_enabled;
    if (turbo_mode_enabled == 1)
        instance->setMaxThreads(max_threads_turbo);
}

void StaticThreadPool::disableTurboMode()
{
    if (!instance)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The {} is not initialized", name);

    std::lock_guard lock(mutex);

    --turbo_mode_enabled;
    if (turbo_mode_enabled == 0)
        instance->setMaxThreads(max_threads_normal);
}

void StaticThreadPool::setMaxTurboThreads(size_t max_threads_turbo_)
{
    if (!instance)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The {} is not initialized", name);

    std::lock_guard lock(mutex);

    max_threads_turbo = max_threads_turbo_;
    if (turbo_mode_enabled > 0)
        instance->setMaxThreads(max_threads_turbo);
}

/// NOLINTBEGIN(bugprone-macro-parentheses)
#define DEFINE_STATIC_THREAD_POOL_GETTER(SUFFIX, NAME, METRIC) \
StaticThreadPool & get##SUFFIX##ThreadPool() \
{ \
    static StaticThreadPool instance(NAME, CurrentMetrics::METRIC##Threads, CurrentMetrics::METRIC##ThreadsActive, CurrentMetrics::METRIC##ThreadsScheduled); \
    return instance; \
}
/// NOLINTEND(bugprone-macro-parentheses)
APPLY_FOR_STATIC_THREAD_POOLS(DEFINE_STATIC_THREAD_POOL_GETTER)
#undef DEFINE_STATIC_THREAD_POOL_GETTER

void StaticThreadPool::shutdownAll()
{
#define SHUTDOWN_STATIC_THREAD_POOL(SUFFIX, NAME, METRIC) get##SUFFIX##ThreadPool().shutdown();
    APPLY_FOR_STATIC_THREAD_POOLS(SHUTDOWN_STATIC_THREAD_POOL)
#undef SHUTDOWN_STATIC_THREAD_POOL
}

}
