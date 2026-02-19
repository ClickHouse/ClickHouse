#include <Common/AggregatedMetrics.h>
#include <Common/CurrentMetrics.h>

namespace AggregatedMetrics
{
    MetricHandle::MetricHandle(CurrentMetrics::Metric destination_metric_)
        : destination_metric(destination_metric_)
    {
    }

    MetricHandle::~MetricHandle()
    {
        set(0);
    }

    void MetricHandle::set(CurrentMetrics::Value value)
    {
        CurrentMetrics::Value prev_value = accounted_value.exchange(value, std::memory_order_relaxed);
        CurrentMetrics::Value delta = value - prev_value;
        CurrentMetrics::add(destination_metric, delta);
    }

    void MetricHandle::add(CurrentMetrics::Value delta)
    {
        accounted_value.fetch_add(delta, std::memory_order_relaxed);
        CurrentMetrics::add(destination_metric, delta);
    }

    void MetricHandle::sub(CurrentMetrics::Value delta)
    {
        accounted_value.fetch_sub(delta, std::memory_order_relaxed);
        CurrentMetrics::sub(destination_metric, delta);
    }
}
