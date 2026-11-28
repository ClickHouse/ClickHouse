#include <Common/AsynchronousMetricsWithContext.h>
#include <Core/ServerSettings.h>
#include <Daemon/BaseDaemon.h>
#include <Interpreters/Context.h>
#include <Common/PageCache.h>
#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>

#include <algorithm>
#include <cmath>

#include "config.h"

#if USE_EMBEDDED_COMPILER
#include <Interpreters/JIT/CompiledExpressionCache.h>
#endif


namespace DB
{

namespace ServerSetting
{
    extern const ServerSettingsUInt64 os_cpu_busy_time_threshold;
    extern const ServerSettingsBool os_collect_psi_metrics;
}

AsynchronousMetricsWithContext::AsynchronousMetricsWithContext(
    unsigned update_period_seconds,
    const ProtocolServerMetricsFunc & protocol_server_metrics_func_,
    bool update_jemalloc_epoch_,
    bool update_rss_,
    const ContextPtr & context_)
    : AsynchronousMetrics(
        update_period_seconds,
        protocol_server_metrics_func_,
        update_jemalloc_epoch_,
        update_rss_,
        context_->getServerSettings()[ServerSetting::os_collect_psi_metrics])
    , context(context_)
{
}

UInt64 AsynchronousMetricsWithContext::getUserspacePageCacheBytes() const
{
    if (auto page_cache = context->getPageCache())
        return page_cache->sizeInBytes();
    return 0;
}

AsynchronousMetricsKeyValuesMode AsynchronousMetricsWithContext::getKeyValuesMode() const
{
    return getAsynchronousMetricsKeyValuesMode(context->getConfigRef());
}

void AsynchronousMetricsWithContext::updateEnvironmentImpl(AsynchronousMetricValues & new_values)
{
    new_values["OSCPUOverload"] = { ProfileEvents::global_counters.getCPUOverload(context->getServerSettings()[ServerSetting::os_cpu_busy_time_threshold], /*reset*/ true), "Relative CPU deficit, calculated as: how many threads are waiting for CPU relative to the number of threads, using CPU. If it is greater than zero, the server would benefit from more CPU. If it is significantly greater than zero, the server could become unresponsive. The metric is accumulated between the updates of asynchronous metrics." };

    {
        AsynchronousMetricKeyValues async_logging_queue_sizes;
        for (const auto & metric : BaseDaemon::instance().getAsynchronousMetricsFromAsyncLogs())
            async_logging_queue_sizes[metric.first] = static_cast<double>(metric.second);

        if (!async_logging_queue_sizes.empty())
            new_values["AsyncLoggingQueueSize"]
                = {"channel", std::move(async_logging_queue_sizes),
                   "Number of async messages queued pending for logging, keyed by the logging channel name."};
    }

#if USE_EMBEDDED_COMPILER
    /// Compiled-expression cache: configured limits (the current usage is exposed via the
    /// `CompiledExpressionCacheBytes` and `CompiledExpressionCacheCount` CurrentMetrics).
    /// Note: `CompiledExpressionCacheBytes` is the page-block capacity reserved by
    /// `JITModuleMemoryManager` for cached compiled functions. Those blocks are allocated via
    /// `posix_memalign` (intercepted into `je_posix_memalign` in `Common/malloc.cpp`), so they
    /// go through jemalloc and are accounted within the dedicated JIT arena — i.e. this metric
    /// is a *subset* of `jemalloc.jit_arena.active_bytes` above, not a separate quantity.
    if (auto * compiled_cache = CompiledExpressionCacheFactory::instance().tryGetCache())
    {
        new_values["CompiledExpressionCacheBytesMax"] = { compiled_cache->maxSizeInBytes(),
            "Configured maximum bytes of the compiled-expression cache. "
            "Available headroom = this minus `CompiledExpressionCacheBytes` (CurrentMetric)." };
        new_values["CompiledExpressionCacheCountMax"] = { compiled_cache->maxCount(),
            "Configured maximum entries of the compiled-expression cache. "
            "Available headroom = this minus `CompiledExpressionCacheCount` (CurrentMetric)." };
    }
#endif
}

void AsynchronousMetricsWithContext::processWarnings(const AsynchronousMetricValues & values) const
{
    processWarningForMutationStats(values);
    // server resource overload warnings
    processWarningForMemoryOverload(values);
    processWarningForCPUOverload(values);
}

// Warnings for pending mutations
void AsynchronousMetricsWithContext::processWarningForMutationStats(const AsynchronousMetricValues & new_values) const
{
    // The following warnings are base on asynchronous metrics, and they are populated into the system.warnings table
    // Warnings for part mutations
    const auto * num_pending_mutations = getAsynchronousMetricValue(new_values, "NumberOfPendingMutations");
    if (!num_pending_mutations)
        return;

    auto max_pending_mutations_to_warn = context->getMaxPendingMutationsToWarn();

    if (num_pending_mutations->value > static_cast<double>(max_pending_mutations_to_warn))
    {
        context->addOrUpdateWarningMessage(
            Context::WarningType::MAX_PENDING_MUTATIONS_EXCEEDS_LIMIT,
            PreformattedMessage::create("The number of pending mutations is more than {}.", max_pending_mutations_to_warn));
    }
    if (num_pending_mutations->value <= static_cast<double>(max_pending_mutations_to_warn))
        context->removeWarningMessage(Context::WarningType::MAX_PENDING_MUTATIONS_EXCEEDS_LIMIT);

    const auto * num_pending_mutations_over_execution_time= getAsynchronousMetricValue(new_values, "NumberOfPendingMutationsOverExecutionTime");
    if (!num_pending_mutations_over_execution_time)
        return;

    if (num_pending_mutations_over_execution_time->value > 0)
    {
        context->addOrUpdateWarningMessage(
            Context::WarningType::MAX_PENDING_MUTATIONS_OVER_THRESHOLD,
            PreformattedMessage::create(
                "There are {} pending mutations that exceed the max_pending_mutations_execution_time_to_warn threshold.",
                num_pending_mutations_over_execution_time->value));
    }
    else
    {
        context->removeWarningMessage(Context::WarningType::MAX_PENDING_MUTATIONS_OVER_THRESHOLD);
    }
}

void AsynchronousMetricsWithContext::processWarningForMemoryOverload(const AsynchronousMetricValues & new_values) const
{
    double memory_resident{};
    double memory_total{};

    /// Compute the user space page cache size (in bytes) and subtract from total memory used if possible
    double user_space_cache_bytes{};
    if (const auto * rss = getAsynchronousMetricValue(new_values, "MemoryResident"),
        *rss_wo_pc = getAsynchronousMetricValue(new_values, "MemoryResidentWithoutPageCache");
        rss && rss_wo_pc && rss->value >= rss_wo_pc->value)
    {
        user_space_cache_bytes = rss->value - rss_wo_pc->value;
    }

    /// use cgroup memory metrics if available and > 0, otherwise fallback to OS memory metrics
    if (const auto *cgroup_memory_total = getAsynchronousMetricValue(new_values, "CGroupMemoryTotal"),
        *cgroup_memory_used = getAsynchronousMetricValue(new_values, "CGroupMemoryUsed");
        cgroup_memory_total && cgroup_memory_used && (cgroup_memory_total->value > 0.0 && cgroup_memory_used->value > 0.0))
    {
        memory_resident = cgroup_memory_used->value;
        memory_total = cgroup_memory_total->value;
    }
    else if (const auto * os_memory_used = getAsynchronousMetricValue(new_values, "MemoryResident"),
             *os_memory_total = getAsynchronousMetricValue(new_values, "OSMemoryTotal");
             os_memory_used && os_memory_total && (os_memory_total->value > 0.0 && os_memory_used->value > 0.0))
    {
        memory_resident = os_memory_used->value;
        memory_total = os_memory_total->value;
    }
    else
    {
        /// no memory metrics available
        return;
    }

    /// Exclude user space page cache from memory resident
    if (user_space_cache_bytes > 0.0)
        memory_resident = std::max(0.0, memory_resident - user_space_cache_bytes);

    const double ratio = memory_resident / memory_total;
    const double clamped_ratio = std::clamp(ratio, 0.0, 1.0);

    const auto & cfg = context->getConfigRef();
    const double mem_warn_ratio = cfg.getDouble("resource_overload_warnings.memory_overload_warn_ratio", 0.9);
    const double mem_clear_ratio = cfg.getDouble("resource_overload_warnings.memory_overload_clear_ratio", 0.85);
    const UInt64 min_duration = cfg.getUInt64("resource_overload_warnings.memory_overload_duration_seconds", 600);

    const auto now = Clock::now();
    const int usage_percent = static_cast<int>(std::lround(clamped_ratio * 100.0));

    const auto warning_message = PreformattedMessage::create(
        "High ClickHouse memory usage: {} of {} used ({}%) for at least {} second(s)",
        formatReadableSizeWithDecimalSuffix(memory_resident),
        formatReadableSizeWithDecimalSuffix(memory_total),
        usage_percent,
        min_duration);

    if (ratio >= mem_warn_ratio)
    {
        if (!mem_overload_started)
            mem_overload_started = now;

        if (now - *mem_overload_started >= std::chrono::seconds{min_duration})
            context->addOrUpdateWarningMessage(Context::WarningType::SERVER_MEMORY_OVERLOAD, warning_message);
        else
            context->removeWarningMessage(Context::WarningType::SERVER_MEMORY_OVERLOAD);
    }
    else
    {
        mem_overload_started.reset();
        if (ratio <= mem_clear_ratio)
            context->removeWarningMessage(Context::WarningType::SERVER_MEMORY_OVERLOAD);
    }
}

void AsynchronousMetricsWithContext::processWarningForCPUOverload(const AsynchronousMetricValues & new_values) const
{
    const auto * idle_ptr = getAsynchronousMetricValue(new_values, "OSIdleTimeNormalized");
    const auto * io_wait_ptr = getAsynchronousMetricValue(new_values, "OSIOWaitTimeNormalized");
    if (!idle_ptr || !io_wait_ptr)
        return;

    /// ensure that the value is always in [0.0, 1.0]
    const double busy_time = std::clamp(1.0 - idle_ptr->value - io_wait_ptr->value, 0.0, 1.0);

    const auto & cfg = context->getConfigRef();
    const double cpu_warn_ratio = cfg.getDouble("resource_overload_warnings.cpu_overload_warn_ratio", 0.9);
    const double cpu_clear_ratio = cfg.getDouble("resource_overload_warnings.cpu_overload_clear_ratio", 0.85);
    const UInt64 min_duration = cfg.getUInt64("resource_overload_warnings.cpu_overload_duration_seconds", 600);

    const auto now = Clock::now();
    const int busy_percent = static_cast<int>(std::lround(busy_time * 100.0));
    const int threshold_percent = static_cast<int>(std::lround(cpu_warn_ratio * 100.0));

    const auto warning_message = PreformattedMessage::create(
        "High CPU usage: {}% busy (>= {}%) for at least {} second(s)", busy_percent, threshold_percent, min_duration);

    if (busy_time >= cpu_warn_ratio)
    {
        if (!cpu_overload_started)
            cpu_overload_started = now;

        if (now - *cpu_overload_started >= std::chrono::seconds{min_duration})
            context->addOrUpdateWarningMessage(Context::WarningType::SERVER_CPU_OVERLOAD, warning_message);
        else
            context->removeWarningMessage(Context::WarningType::SERVER_CPU_OVERLOAD);
    }
    else
    {
        cpu_overload_started.reset();
        if (busy_time <= cpu_clear_ratio)
            context->removeWarningMessage(Context::WarningType::SERVER_CPU_OVERLOAD);
    }
}

}
