#pragma once

#include <Common/CurrentMetrics.h>

#include <atomic>

/** Allows to aggregate partially changing values into single CurrentMetric.
  * Destination CurrentMetric will be updated instantly after changes will be applied to handle.
  */
namespace AggregatedMetrics
{

class GlobalSum
{
public:
    explicit GlobalSum(CurrentMetrics::Metric destination_metric_) noexcept;
    ~GlobalSum() noexcept;

    void set(CurrentMetrics::Value value) noexcept;
    void add(CurrentMetrics::Value delta) noexcept;
    void sub(CurrentMetrics::Value delta) noexcept;

private:
    const CurrentMetrics::Metric destination_metric;
    alignas(64) std::atomic<CurrentMetrics::Value> accounted_value = 0;
};

class GlobalQuantile
{
public:
    explicit GlobalQuantile(CurrentMetrics::Metric destination_metric_) noexcept;
    ~GlobalQuantile() noexcept;

    void set(CurrentMetrics::Value value) noexcept;

private:
    const CurrentMetrics::Metric destination_metric;

    double quantile;
    void * shared_buckets = nullptr;
    void * shared_update = nullptr;

    bool was_accounted = false;
    alignas(64) std::atomic<CurrentMetrics::Value> accounted_value = 0;
};

}
