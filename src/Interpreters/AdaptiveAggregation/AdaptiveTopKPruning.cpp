#include <Interpreters/AdaptiveAggregation/AdaptiveTopKPruning.h>

#include <algorithm>
#include <limits>
#include <numeric>

namespace DB
{

AdaptiveTopKPruning::ProducerBins AdaptiveTopKPruning::finalizeProducerCounts(AdaptiveTopKCountBins && counts)
{
    ProducerBins producer{.counts = std::move(counts)};
    for (size_t bucket = 0; bucket < ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++bucket)
    {
        const UInt16 * bucket_bins = producer.counts.bins.get() + bucket * adaptive_count_bins_per_bucket;
        producer.bucket_maxima[bucket] = *std::max_element(bucket_bins, bucket_bins + adaptive_count_bins_per_bucket);
    }
    return producer;
}

bool AdaptiveTopKPruning::prepare(size_t expected_producers)
{
    if (producer_bins.size() + merge_counted_tables.size() != expected_producers)
        return false;

    /// A bucket's largest bin is at most the sum of the producers' largest bins in it, which orders the buckets.
    std::array<UInt64, ADAPTIVE_AGGREGATION_NUM_BUCKETS> priorities{};
    for (const auto & producer : producer_bins)
        for (size_t bucket = 0; bucket < ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++bucket)
            priorities[bucket] += producer.bucket_maxima[bucket];
    std::iota(bucket_order.begin(), bucket_order.end(), 0);
    std::ranges::stable_sort(bucket_order, [&](UInt8 lhs, UInt8 rhs) { return priorities[lhs] > priorities[rhs]; });
    return true;
}

AdaptiveTopKPruning::BucketBounds AdaptiveTopKPruning::boundsForBucket(size_t bucket) const
{
    BucketBounds bounds{};
    for (const auto & producer : producer_bins)
    {
        const UInt16 * bins = producer.counts.bins.get() + bucket * adaptive_count_bins_per_bucket;
        for (size_t bin = 0; bin < adaptive_count_bins_per_bucket; ++bin)
        {
            if (bins[bin] == std::numeric_limits<UInt16>::max())
                bounds[bin] = std::numeric_limits<UInt64>::max();
            else if (bounds[bin] != std::numeric_limits<UInt64>::max())
                bounds[bin] += bins[bin];
        }
    }
    return bounds;
}

AdaptiveTopKPruning::UnitSelection
AdaptiveTopKPruning::selectUnit(const BucketBounds & bounds, size_t unit, size_t units, AliveBins & alive) const
{
    const UInt64 current = currentThreshold();
    const size_t bins_per_unit = adaptive_count_bins_per_bucket / units;
    size_t alive_count = 0;
    for (size_t bin = unit * bins_per_unit; bin < (unit + 1) * bins_per_unit; ++bin)
    {
        alive[bin] = bounds[bin] >= current;
        alive_count += alive[bin];
    }
    if (!alive_count)
        return UnitSelection::Skip;
    return alive_count == bins_per_unit ? UnitSelection::KeepAll : UnitSelection::Filter;
}

void AdaptiveTopKPruning::offerCounts(std::span<const UInt64> counts)
{
    std::lock_guard lock(best_mutex);
    for (const UInt64 count : counts)
    {
        if (best.size() < limit)
        {
            best.push(count);
        }
        else if (count > best.top())
        {
            best.pop();
            best.push(count);
        }
    }
    if (best.size() == limit)
        threshold.store(best.top(), std::memory_order_relaxed);
}

}
