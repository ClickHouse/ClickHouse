#pragma once

#include <Common/AsynchronousMetrics.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

class ServerAsynchronousMetrics : WithContext, public AsynchronousMetrics
{
public:
    ServerAsynchronousMetrics(
        ContextPtr global_context_,
        unsigned update_period_seconds,
        unsigned heavy_metrics_update_period_seconds,
        const ProtocolServerMetricsFunc & protocol_server_metrics_func_,
        bool update_jemalloc_epoch_,
        bool update_rss_);

    ~ServerAsynchronousMetrics() override;

private:
    void updateImpl(TimePoint update_time, TimePoint current_time, bool force_update, bool first_run, AsynchronousMetricValues & new_values) override;
    void logImpl(AsynchronousMetricValues & new_values) override;

    const Duration heavy_metric_update_period;
    TimePoint heavy_metric_previous_update_time;
    double heavy_update_interval = 0.;

    struct DetachedPartsStats
    {
        size_t count;
        size_t detached_by_user;
    };

    DetachedPartsStats detached_parts_stats{};

    void updateDetachedPartsStats();
    void updateHeavyMetricsIfNeeded(TimePoint current_time, TimePoint update_time, bool force_update, bool first_run, AsynchronousMetricValues & new_values);
};

}
