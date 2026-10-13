#include <Interpreters/AdaptiveAggregation/AdaptiveTopKPruning.h>

#include <algorithm>
#include <limits>

#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>

namespace DB
{

std::array<UInt64, adaptive_count_bins_per_bucket> sumBucketCountBins(const AdaptiveTopKPruning & pruning, size_t bucket)
{
    std::array<UInt64, adaptive_count_bins_per_bucket> bounds{};
    for (const auto & producer : pruning.producer_bins)
    {
        const UInt16 * bins = producer.bins.get() + bucket * adaptive_count_bins_per_bucket;
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

void offerTopKCounts(AdaptiveTopKPruning & pruning, const IColumn & column)
{
    const auto & counts = assert_cast<const ColumnUInt64 &>(column).getData();
    std::lock_guard lock(pruning.best_mutex);
    for (const UInt64 count : counts)
    {
        if (pruning.best.size() < pruning.limit)
        {
            pruning.best.push(count);
        }
        else if (count > pruning.best.top())
        {
            pruning.best.pop();
            pruning.best.push(count);
        }
    }
    if (pruning.best.size() == pruning.limit)
        pruning.threshold.store(pruning.best.top(), std::memory_order_relaxed);
}

}
