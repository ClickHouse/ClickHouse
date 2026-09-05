#pragma once
#include "config.h"

#if USE_PARQUET

#include <Core/Range.h>
#include <Core/Types.h>
#include <Common/CacheBase.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>

#include <vector>

namespace ProfileEvents
{
    extern const Event ParquetOrderedRowGroupIndexCacheHits;
    extern const Event ParquetOrderedRowGroupIndexCacheMisses;
    extern const Event ParquetOrderedRowGroupIndexCacheWeightLost;
}

namespace CurrentMetrics
{
    extern const Metric ParquetOrderedRowGroupIndexCacheBytes;
    extern const Metric ParquetOrderedRowGroupIndexCacheFiles;
}

namespace DB::Parquet
{

enum class OrderedRowGroupDirection : UInt8
{
    Unknown,
    Ascending,
    Descending,
};

struct OrderedRowGroupBound
{
    size_t row_group_idx;
    Range range;
};

/// Validated fence index for one file + column: the row-group min/max ranges are strictly
/// ordered and non-overlapping (`proven`), so a point lookup binary-searches the single
/// candidate instead of checking every row group. Immutable once published.
struct OrderedRowGroupIndex
{
    bool proven = false;
    OrderedRowGroupDirection direction = OrderedRowGroupDirection::Unknown;
    std::vector<OrderedRowGroupBound> bounds;
};

struct OrderedRowGroupIndexWeightFunction
{
    size_t operator()(const OrderedRowGroupIndex & index) const
    {
        return 128 + index.bounds.size() * 128;
    }
};

class OrderedRowGroupIndexCache : public CacheBase<String, OrderedRowGroupIndex, std::hash<String>, OrderedRowGroupIndexWeightFunction>
{
public:
    using Base = CacheBase<String, OrderedRowGroupIndex, std::hash<String>, OrderedRowGroupIndexWeightFunction>;

    OrderedRowGroupIndexCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : Base(cache_policy, CurrentMetrics::ParquetOrderedRowGroupIndexCacheBytes, CurrentMetrics::ParquetOrderedRowGroupIndexCacheFiles, max_size_in_bytes, max_count, size_ratio)
    {
    }

private:
    void onEntryRemoval(const size_t weight_loss, const MappedPtr &) override
    {
        ProfileEvents::increment(ProfileEvents::ParquetOrderedRowGroupIndexCacheWeightLost, weight_loss);
    }
};

using OrderedRowGroupIndexCachePtr = std::shared_ptr<OrderedRowGroupIndexCache>;

/// Global fence-index cache, cleared together with the Parquet metadata cache by
/// `SYSTEM CLEAR PARQUET METADATA CACHE` (both are keyed by file identity).
OrderedRowGroupIndexCachePtr getGlobalOrderedRowGroupIndexCache();
void clearGlobalOrderedRowGroupIndexCache();

}

#endif
