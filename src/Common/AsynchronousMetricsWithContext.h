#pragma once

#include <Common/AsynchronousMetrics.h>
#include <Interpreters/Context_fwd.h>

#include <chrono>
#include <optional>


namespace DB
{

/// `AsynchronousMetrics` of a process that has a `Context`: the server and Keeper.
/// Adds the metrics and warnings that depend on the server environment (settings, configuration, the userspace
/// page cache, the logging queues of `BaseDaemon`), so that `AsynchronousMetrics` itself does not depend on them
/// and can be used by a standalone process.
class AsynchronousMetricsWithContext : public AsynchronousMetrics
{
public:
    AsynchronousMetricsWithContext(
        unsigned update_period_seconds,
        const ProtocolServerMetricsFunc & protocol_server_metrics_func_,
        bool update_jemalloc_epoch_,
        bool update_rss_,
        const ContextPtr & context_);

private:
    UInt64 getUserspacePageCacheBytes() const override;
    void updateEnvironmentImpl(AsynchronousMetricValues & new_values) override;
    AsynchronousMetricsKeyValuesMode getKeyValuesMode() const override;
    void processWarnings(const AsynchronousMetricValues & values) const override;

    void processWarningForMutationStats(const AsynchronousMetricValues & new_values) const;
    void processWarningForMemoryOverload(const AsynchronousMetricValues & new_values) const;
    void processWarningForCPUOverload(const AsynchronousMetricValues & new_values) const;

    using Clock = std::chrono::steady_clock;
    mutable std::optional<Clock::time_point> mem_overload_started;
    mutable std::optional<Clock::time_point> cpu_overload_started;

    ContextPtr context;
};

}
