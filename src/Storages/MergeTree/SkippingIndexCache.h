#pragma once

#include <Common/CacheBase.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

namespace ProfileEvents
{
    extern const Event SkippingIndexCacheMisses;
    extern const Event SkippingIndexCacheHits;
    extern const Event SkippingIndexCacheWeightLost;
}

namespace CurrentMetrics
{
    extern const Metric SkippingIndexCacheBytes;
    extern const Metric SkippingIndexCacheCells;
}

namespace DB
{

/// One cache entry holds a contiguous block of deserialized granules of one skipping index of one part.
/// Caching per block instead of per granule keeps the number of cache lookups (and thus the contention
/// on the cache mutex) low for indexes with a small GRANULARITY.
struct SkippingIndexCacheKey
{
    /// Storage-related path of the part - uniquely identifies one part from another
    String path_to_data_part;
    String index_name;
    /// Index of the block of granules: index_mark / SkippingIndexCache::GRANULES_PER_ENTRY.
    size_t block_number;

    bool operator==(const SkippingIndexCacheKey & rhs) const
    {
        return path_to_data_part == rhs.path_to_data_part && index_name == rhs.index_name && block_number == rhs.block_number;
    }
};

struct SkippingIndexCacheHashFunction
{
    size_t operator()(const SkippingIndexCacheKey & key) const
    {
        SipHash siphash;
        siphash.update(key.path_to_data_part);
        siphash.update(key.index_name);
        siphash.update(key.block_number);

        return siphash.get64();
    }
};

struct SkippingIndexCacheCell
{
    /// memoryUsageBytes() of the granules counts only the payload, so add a guess for the objects themselves.
    static constexpr auto GRANULE_OVERHEAD_BYTES_GUESS = 128uz;
    static constexpr auto ENTRY_OVERHEAD_BYTES_GUESS = 200uz;

    MergeTreeIndexGranules granules;
    size_t memory_bytes;

    explicit SkippingIndexCacheCell(MergeTreeIndexGranules granules_)
        : granules(std::move(granules_))
        , memory_bytes(ENTRY_OVERHEAD_BYTES_GUESS)
    {
        for (const auto & granule : granules)
            memory_bytes += granule->memoryUsageBytes() + GRANULE_OVERHEAD_BYTES_GUESS;
    }
};

struct SkippingIndexCacheWeightFunction
{
    size_t operator()(const SkippingIndexCacheCell & cell) const
    {
        return cell.memory_bytes;
    }
};

/// Cache of deserialized skipping index granules, see `IMergeTreeIndex::supportsGranuleCache`.
class SkippingIndexCache : public CacheBase<SkippingIndexCacheKey, SkippingIndexCacheCell, SkippingIndexCacheHashFunction, SkippingIndexCacheWeightFunction>
{
public:
    using Base = CacheBase<SkippingIndexCacheKey, SkippingIndexCacheCell, SkippingIndexCacheHashFunction, SkippingIndexCacheWeightFunction>;

    /// Number of consecutive index granules stored in one cache entry.
    static constexpr size_t GRANULES_PER_ENTRY = 128;

    SkippingIndexCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : Base(cache_policy, CurrentMetrics::SkippingIndexCacheBytes, CurrentMetrics::SkippingIndexCacheCells, max_size_in_bytes, max_count, size_ratio)
    {}

    /// Marks of the index granules stored in the block.
    static MarkRange blockRange(size_t block_number, size_t marks_count)
    {
        size_t begin = block_number * GRANULES_PER_ENTRY;
        return {begin, std::min(marks_count, begin + GRANULES_PER_ENTRY)};
    }

    template <typename LoadFunc>
    MappedPtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto [cell, missed] = Base::getOrSet(key, [&] { return std::make_shared<SkippingIndexCacheCell>(load()); });
        ProfileEvents::increment(missed ? ProfileEvents::SkippingIndexCacheMisses : ProfileEvents::SkippingIndexCacheHits);
        return cell;
    }

    void removeEntriesFromCache(const String & path_to_data_part)
    {
        Base::remove([path_to_data_part](const Key & key, const MappedPtr &) { return key.path_to_data_part == path_to_data_part; });
    }

private:
    void onEntryRemoval(const size_t weight_loss, const MappedPtr &) override
    {
        ProfileEvents::increment(ProfileEvents::SkippingIndexCacheWeightLost, weight_loss);
    }
};

using SkippingIndexCachePtr = std::shared_ptr<SkippingIndexCache>;

}
