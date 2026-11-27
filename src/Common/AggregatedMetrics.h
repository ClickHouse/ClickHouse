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
    std::atomic<CurrentMetrics::Value> accounted_value = 0;
};

class GlobalQuantile
{
    void updateBuckets(std::optional<CurrentMetrics::Value> prev_value, std::optional<CurrentMetrics::Value> new_value) noexcept;

public:
    explicit GlobalQuantile(CurrentMetrics::Metric destination_metric_) noexcept;
    ~GlobalQuantile() noexcept;

    void set(CurrentMetrics::Value value) noexcept;

private:
    const CurrentMetrics::Metric destination_metric;

    double quantile = 0;
    void * shared_buckets = nullptr;
    void * shared_update = nullptr;

    std::atomic<CurrentMetrics::Value> accounted_value = 0;
};

}
