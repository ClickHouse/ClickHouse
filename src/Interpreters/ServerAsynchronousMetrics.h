#pragma once

#include <Common/AsynchronousMetrics.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

class ServerAsynchronousMetrics : public AsynchronousMetrics, WithContext
{
public:
    ServerAsynchronousMetrics(
        ContextPtr global_context_,
        int update_period_seconds,
        int heavy_metrics_update_period_seconds,
        const ProtocolServerMetricsFunc & protocol_server_metrics_func_);
private:
    void updateImpl(AsynchronousMetricValues & new_values, TimePoint update_time, TimePoint current_time) override;
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
    void updateHeavyMetricsIfNeeded(TimePoint current_time, TimePoint update_time, AsynchronousMetricValues & new_values);
};

}
