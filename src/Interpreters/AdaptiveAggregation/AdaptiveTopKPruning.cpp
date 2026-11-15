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
        producer.max_bin_per_bucket[bucket] = *std::max_element(bucket_bins, bucket_bins + adaptive_count_bins_per_bucket);
    }
    return producer;
}

void AdaptiveTopKPruning::orderBuckets()
{
    std::iota(bucket_order.begin(), bucket_order.end(), 0);
    std::ranges::stable_sort(bucket_order, [&](UInt8 lhs, UInt8 rhs) { return count_bound_per_bucket[lhs] > count_bound_per_bucket[rhs]; });
}

AdaptiveTopKPruning::BucketBounds AdaptiveTopKPruning::boundsForBucket(size_t bucket) const
{
    BucketBounds bounds{};
    for (const auto & counts : producer_bins)
    {
        const UInt16 * bins = counts.bins.get() + bucket * adaptive_count_bins_per_bucket;
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
AdaptiveTopKPruning::selectUnit(const BucketBounds & bounds, size_t unit, size_t units, UInt64 threshold, AliveBins & alive) const
{
    const size_t bins_per_unit = adaptive_count_bins_per_bucket / units;
    size_t alive_count = 0;
    for (size_t bin = unit * bins_per_unit; bin < (unit + 1) * bins_per_unit; ++bin)
    {
        alive[bin] = bounds[bin] >= threshold;
        alive_count += alive[bin];
    }
    if (!alive_count)
        return UnitSelection::Skip;
    return alive_count == bins_per_unit ? UnitSelection::KeepAll : UnitSelection::Filter;
}

}
