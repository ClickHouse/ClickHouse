#pragma once

#include <Common/CurrentMetrics.h>

#include <atomic>

/** Allows to aggregate partially changing values into single CurrentMetric.
  * Destination CurrentMetric will be updated instantly after changes will be applied to handle.
  */
namespace AggregatedMetrics
{
    /// PreAggregated value of CurrentMetric. Used to implement partial updates to the destination metric.
    class MetricHandle
    {
    public:
        explicit MetricHandle(CurrentMetrics::Metric destination_metric_);
        ~MetricHandle();

        void set(CurrentMetrics::Value value);
        void add(CurrentMetrics::Value delta);
        void sub(CurrentMetrics::Value delta);

    private:
        CurrentMetrics::Metric destination_metric;
        alignas(64) std::atomic<CurrentMetrics::Value> accounted_value = 0;
    };
}
