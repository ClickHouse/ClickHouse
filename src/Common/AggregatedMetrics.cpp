#include <Common/AggregatedMetrics.h>
#include <Common/CurrentMetrics.h>

#include <base/defines.h>

#include <algorithm>
#include <atomic>
#include <mutex>
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
        const int64_t delta_from_min = value - min_value;
        return std::clamp<int64_t>(delta_from_min / bucket_value_interval, 0, buckets_count - 1);
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
            buckets[node].fetch_add(delta);

            const int64_t middle = (left_border + right_border) / 2;
            if (bucket_number < middle)
            {
                node = 2 * node + 1;
                right_border = middle;
            }
            else
            {
                node = 2 * node + 2;
                left_border = middle;
            }
        }

        chassert(node < 4 * buckets_count);
        buckets[node].fetch_add(delta);
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

            const int64_t middle = (left_border + right_border) / 2;
            const int64_t left_subtree_count = buckets[2 * node + 1].load();

            if (left_subtree_count >= need_to_observe)
            {
                node = 2 * node + 1;
                right_border = middle;
            }
            else
            {
                node = 2 * node + 2;
                left_border = middle;
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
            buckets[i].store(0);
    }

    void increment(int64_t value) noexcept
    {
        const int64_t bucket_number = getBucketNumber(value);
        updateBucketPath(bucket_number, /*delta=*/1);
        global_count.fetch_add(1);
    }

    void decrement(int64_t value) noexcept
    {
        const int64_t bucket_number = getBucketNumber(value);
        updateBucketPath(bucket_number, /*delta=*/-1);
        global_count.fetch_sub(1);
    }

    int64_t quantile(double request) noexcept
    {
        chassert(0 <= request && request <= 1);
        const int64_t global = global_count.load();
        if (global == 0)
            return 0;

        const int64_t need_to_observe = std::max<int64_t>(1, static_cast<int64_t>(static_cast<double>(global) * request));
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
    static std::unordered_map<CurrentMetrics::Metric, BucketCountQuantile> buckets;
    static std::unordered_map<CurrentMetrics::Metric, std::atomic<bool>> updates;
    static std::once_flag initialize_metrics_shared_data;

    std::call_once(initialize_metrics_shared_data, [&]()
    {
        quantiles.emplace(CurrentMetrics::SharedMergeTreeMaxActiveReplicas, 1);
        quantiles.emplace(CurrentMetrics::SharedMergeTreeMaxInactiveReplicas, 1);
        quantiles.emplace(CurrentMetrics::SharedMergeTreeMaxReplicas, 1);
        quantiles.emplace(CurrentMetrics::SharedMergeTreeMinActiveReplicas, 0);
        quantiles.emplace(CurrentMetrics::SharedMergeTreeMinInactiveReplicas, 0);
        quantiles.emplace(CurrentMetrics::SharedMergeTreeMinReplicas, 0);

        buckets.emplace(std::piecewise_construct, std::forward_as_tuple(CurrentMetrics::SharedMergeTreeMaxActiveReplicas), std::forward_as_tuple(0, 500, 501));
        buckets.emplace(std::piecewise_construct, std::forward_as_tuple(CurrentMetrics::SharedMergeTreeMaxInactiveReplicas), std::forward_as_tuple(0, 500, 501));
        buckets.emplace(std::piecewise_construct, std::forward_as_tuple(CurrentMetrics::SharedMergeTreeMaxReplicas), std::forward_as_tuple(0, 500, 501));
        buckets.emplace(std::piecewise_construct, std::forward_as_tuple(CurrentMetrics::SharedMergeTreeMinActiveReplicas), std::forward_as_tuple(0, 500, 501));
        buckets.emplace(std::piecewise_construct, std::forward_as_tuple(CurrentMetrics::SharedMergeTreeMinInactiveReplicas), std::forward_as_tuple(0, 500, 501));
        buckets.emplace(std::piecewise_construct, std::forward_as_tuple(CurrentMetrics::SharedMergeTreeMinReplicas), std::forward_as_tuple(0, 500, 501));

        updates.emplace(CurrentMetrics::SharedMergeTreeMaxActiveReplicas, false);
        updates.emplace(CurrentMetrics::SharedMergeTreeMaxInactiveReplicas, false);
        updates.emplace(CurrentMetrics::SharedMergeTreeMaxReplicas, false);
        updates.emplace(CurrentMetrics::SharedMergeTreeMinActiveReplicas, false);
        updates.emplace(CurrentMetrics::SharedMergeTreeMinInactiveReplicas, false);
        updates.emplace(CurrentMetrics::SharedMergeTreeMinReplicas, false);
    });

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
    CurrentMetrics::Value prev_value = accounted_value.exchange(value, std::memory_order::relaxed);
    CurrentMetrics::Value delta = value - prev_value;
    CurrentMetrics::add(destination_metric, delta);
}

void GlobalSum::add(CurrentMetrics::Value delta) noexcept
{
    accounted_value.fetch_add(delta, std::memory_order::relaxed);
    CurrentMetrics::add(destination_metric, delta);
}

void GlobalSum::sub(CurrentMetrics::Value delta) noexcept
{
    accounted_value.fetch_sub(delta, std::memory_order::relaxed);
    CurrentMetrics::sub(destination_metric, delta);
}

void GlobalQuantile::updateBuckets(std::optional<CurrentMetrics::Value> prev_value, std::optional<CurrentMetrics::Value> new_value) noexcept
{
    BucketCountQuantile * buckets = static_cast<BucketCountQuantile *>(shared_buckets);
    std::atomic<bool> * update = static_cast<std::atomic<bool> *>(shared_update);

    if (prev_value.has_value())
        buckets->decrement(prev_value.value());

    if (new_value.has_value())
        buckets->increment(new_value.value());

    if (!update->exchange(true))
    {
        CurrentMetrics::set(destination_metric, buckets->quantile(quantile));
        update->store(false);
    }
}

GlobalQuantile::GlobalQuantile(CurrentMetrics::Metric destination_metric_) noexcept
    : destination_metric(destination_metric_)
{
    std::tie(quantile, shared_buckets, shared_update) = takeSharedQuantileData(destination_metric);
    updateBuckets(/*prev_value=*/std::nullopt, /*new_value=*/0);
}

GlobalQuantile::~GlobalQuantile() noexcept
{
    updateBuckets(/*prev_value=*/accounted_value.load(), /*new_value=*/std::nullopt);
}

void GlobalQuantile::set(CurrentMetrics::Value value) noexcept
{
    CurrentMetrics::Value prev_value = accounted_value.exchange(value);
    updateBuckets(/*prev_value=*/prev_value, /*new_value=*/value);
}

}
