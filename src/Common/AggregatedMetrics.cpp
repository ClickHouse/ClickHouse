#include <Common/AggregatedMetrics.h>
#include <Common/CurrentMetrics.h>

#include <base/defines.h>

#include <atomic>
#include <tuple>

namespace CurrentMetrics
{
    extern const Metric SharedMergeTreeMaxActiveReplicas;
    extern const Metric SharedMergeTreeMaxInactiveReplicas;
    extern const Metric SharedMergeTreeMaxReplicas;
    extern const Metric SharedMergeTreeMinActiveReplicas;
    extern const Metric SharedMergeTreeMinInactiveReplicas;
    extern const Metric SharedMergeTreeMinReplicas;
}

namespace AggregatedMetrics
{

namespace
{

class BucketCountQuantile
{
    int64_t getBucketNumber(int64_t value) const noexcept
    {
        chassert(min_value <= value && value <= max_value);
        const int64_t delta_from_min = value - min_value;
        return std::min(buckets_count - 1, delta_from_min / bucket_value_interval);
    }

    int64_t getBucketMiddle(int64_t bucket_number) const noexcept
    {
        return std::min(max_value, min_value + bucket_number * bucket_value_interval + bucket_value_interval / 2);
    }

    void updateBucketPath(int64_t bucket_number, int64_t delta) noexcept
    {
        int64_t node = 0;
        int64_t left_border = 0;
        int64_t right_border = buckets_count;
        while (right_border - left_border > 1)
        {
            chassert(node < 4 * buckets_count);
            buckets[node].fetch_add(delta, std::memory_order::relaxed);

            const int64_t midle = (left_border + right_border) / 2;
            if (bucket_number < midle)
            {
                node = 2 * node + 1;
                right_border = midle;
            }
            else
            {
                node = 2 * node + 2;
                left_border = midle;
            }
        }

        chassert(node < 4 * buckets_count);
        buckets[node].fetch_add(delta, std::memory_order::relaxed);
    }

    int64_t findQuantileBucket(int64_t need_to_observe) noexcept
    {
        int64_t node = 0;
        int64_t left_border = 0;
        int64_t right_border = buckets_count;
        while (right_border - left_border > 1)
        {
            chassert(2 * node + 1 < 4 * buckets_count);
            chassert(2 * node + 2 < 4 * buckets_count);

            const int64_t midle = (left_border + right_border) / 2;
            const int64_t left_subtree_count = buckets[2 * node + 1].load(std::memory_order::relaxed);

            if (left_subtree_count >= need_to_observe)
            {
                node = 2 * node + 1;
                right_border = midle;
            }
            else
            {
                node = 2 * node + 2;
                left_border = midle;
                need_to_observe -= left_subtree_count;
            }
        }

        chassert(node < 4 * buckets_count);
        return left_border;
    }

public:
    BucketCountQuantile(int64_t min_value_, int64_t max_value_, int64_t buckets_count_)
        : min_value(min_value_)
        , max_value(max_value_)
        , buckets_count(buckets_count_)
        , bucket_value_interval((max_value - min_value + buckets_count) / buckets_count)
        , buckets(std::make_unique<std::atomic<int64_t>[]>(4 * buckets_count))
    {
        for (int64_t i = 0; i < 4 * buckets_count; ++i)
            buckets[i].store(0, std::memory_order::relaxed);
    }

    void increment(int64_t value) noexcept
    {
        const int64_t bucket_number = getBucketNumber(value);
        updateBucketPath(bucket_number, /*delta=*/1);
        global_count.fetch_add(1, std::memory_order::relaxed);
    }

    void decrement(int64_t value) noexcept
    {
        const int64_t bucket_number = getBucketNumber(value);
        updateBucketPath(bucket_number, /*delta=*/-1);
        global_count.fetch_sub(1, std::memory_order::relaxed);
    }

    int64_t quantile(double request) noexcept
    {
        chassert(0 <= request && request <= 1);
        const int64_t global = global_count.load(std::memory_order::relaxed);
        if (global == 0)
            return 0;

        const int64_t need_to_observe = std::max<int64_t>(1, static_cast<int64_t>(global * request));
        const int64_t quantile_bucket_number = findQuantileBucket(need_to_observe);
        return getBucketMiddle(quantile_bucket_number);
    }

private:
    const int64_t min_value;
    const int64_t max_value;
    const int64_t buckets_count;
    const int64_t bucket_value_interval;

    std::atomic<int64_t> global_count = 0;
    std::unique_ptr<std::atomic<int64_t>[]> buckets;
};

std::tuple<double, BucketCountQuantile *, std::atomic<bool> *> takeSharedQuantileData(CurrentMetrics::Metric metric)
{
    static std::unordered_map<CurrentMetrics::Metric, double> quantiles;
    {
        quantiles.emplace(CurrentMetrics::SharedMergeTreeMaxActiveReplicas, 1);
        quantiles.emplace(CurrentMetrics::SharedMergeTreeMaxInactiveReplicas, 1);
        quantiles.emplace(CurrentMetrics::SharedMergeTreeMaxReplicas, 1);
        quantiles.emplace(CurrentMetrics::SharedMergeTreeMinActiveReplicas, 0);
        quantiles.emplace(CurrentMetrics::SharedMergeTreeMinInactiveReplicas, 0);
        quantiles.emplace(CurrentMetrics::SharedMergeTreeMinReplicas, 0);
    }

    static std::unordered_map<CurrentMetrics::Metric, BucketCountQuantile> buckets;
    {
        buckets.emplace(std::piecewise_construct, std::forward_as_tuple(CurrentMetrics::SharedMergeTreeMaxActiveReplicas), std::forward_as_tuple(0, 100, 101));
        buckets.emplace(std::piecewise_construct, std::forward_as_tuple(CurrentMetrics::SharedMergeTreeMaxInactiveReplicas), std::forward_as_tuple(0, 100, 101));
        buckets.emplace(std::piecewise_construct, std::forward_as_tuple(CurrentMetrics::SharedMergeTreeMaxReplicas), std::forward_as_tuple(0, 100, 101));
        buckets.emplace(std::piecewise_construct, std::forward_as_tuple(CurrentMetrics::SharedMergeTreeMinActiveReplicas), std::forward_as_tuple(0, 100, 101));
        buckets.emplace(std::piecewise_construct, std::forward_as_tuple(CurrentMetrics::SharedMergeTreeMinInactiveReplicas), std::forward_as_tuple(0, 100, 101));
        buckets.emplace(std::piecewise_construct, std::forward_as_tuple(CurrentMetrics::SharedMergeTreeMinReplicas), std::forward_as_tuple(0, 100, 101));
    }

    static std::unordered_map<CurrentMetrics::Metric, std::atomic<bool>> updates;
    {
        updates.emplace(CurrentMetrics::SharedMergeTreeMaxActiveReplicas, false);
        updates.emplace(CurrentMetrics::SharedMergeTreeMaxInactiveReplicas, false);
        updates.emplace(CurrentMetrics::SharedMergeTreeMaxReplicas, false);
        updates.emplace(CurrentMetrics::SharedMergeTreeMinActiveReplicas, false);
        updates.emplace(CurrentMetrics::SharedMergeTreeMinInactiveReplicas, false);
        updates.emplace(CurrentMetrics::SharedMergeTreeMinReplicas, false);
    }

    return std::make_tuple(quantiles.at(metric), &buckets.at(metric), &updates.at(metric));
}

}

GlobalSum::GlobalSum(CurrentMetrics::Metric destination_metric_) noexcept
    : destination_metric(destination_metric_)
{
}

GlobalSum::~GlobalSum() noexcept
{
    set(0);
}

void GlobalSum::set(CurrentMetrics::Value value) noexcept
{
    CurrentMetrics::Value prev_value = accounted_value.exchange(value, std::memory_order_relaxed);
    CurrentMetrics::Value delta = value - prev_value;
    CurrentMetrics::add(destination_metric, delta);
}

void GlobalSum::add(CurrentMetrics::Value delta) noexcept
{
    accounted_value.fetch_add(delta, std::memory_order_relaxed);
    CurrentMetrics::add(destination_metric, delta);
}

void GlobalSum::sub(CurrentMetrics::Value delta) noexcept
{
    accounted_value.fetch_sub(delta, std::memory_order_relaxed);
    CurrentMetrics::sub(destination_metric, delta);
}

GlobalQuantile::GlobalQuantile(CurrentMetrics::Metric destination_metric_) noexcept
    : destination_metric(destination_metric_)
{
    std::tie(quantile, shared_buckets, shared_update) = takeSharedQuantileData(destination_metric);
}

GlobalQuantile::~GlobalQuantile() noexcept
{
    if (!was_accounted)
        return;

    BucketCountQuantile * buckets = static_cast<BucketCountQuantile *>(shared_buckets);
    std::atomic<bool> * update = static_cast<std::atomic<bool> *>(shared_update);

    buckets->decrement(accounted_value.load());

    if (update->exchange(true) == false)
    {
        CurrentMetrics::set(destination_metric, buckets->quantile(quantile));
        update->store(false);
    }
}

void GlobalQuantile::set(CurrentMetrics::Value value) noexcept
{
    BucketCountQuantile * buckets = static_cast<BucketCountQuantile *>(shared_buckets);
    std::atomic<bool> * update = static_cast<std::atomic<bool> *>(shared_update);

    CurrentMetrics::Value prev_value = accounted_value.exchange(value, std::memory_order_relaxed);
    if (was_accounted)
        buckets->decrement(prev_value);

    buckets->increment(value);
    was_accounted = true;

    if (update->exchange(true) == false)
    {
        CurrentMetrics::set(destination_metric, buckets->quantile(quantile));
        update->store(false);
    }
}

}
