#pragma once

#include <Common/CacheBase.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

namespace ProfileEvents
{
    extern const Event VectorSimilarityIndexCacheMisses;
    extern const Event VectorSimilarityIndexCacheHits;
    extern const Event VectorSimilarityIndexCacheWeightLost;
}

namespace CurrentMetrics
{
    extern const Metric VectorSimilarityIndexCacheBytes;
    extern const Metric VectorSimilarityIndexCacheCells;
}

namespace DB
{

struct VectorSimilarityIndexCacheKey
{
    /// Storage-related path of the part - uniquely identifies one part from another
    String path_to_data_part;

    /// Also have the index name and mark are part of the the key because
    /// - a table/part can have multiple vector indexes
    /// - if the vector index granularity is smaller than number of granules/marks in the part, then
    ///   multiple vector index granules, each starting at 'index_mark' will be created on the part.
    String index_name;
    size_t index_mark;

    bool operator==(const VectorSimilarityIndexCacheKey & rhs) const
    {
        return (path_to_data_part == rhs.path_to_data_part && index_name == rhs.index_name && index_mark == rhs.index_mark);
    }
};

struct VectorSimilarityIndexCacheHashFunction
{
    size_t operator()(const VectorSimilarityIndexCacheKey & key) const
    {
        SipHash siphash;
        siphash.update(key.path_to_data_part);
        siphash.update(key.index_name);
        siphash.update(key.index_mark);

        return siphash.get64();
    }
};

struct VectorSimilarityIndexCacheCell
{
    /// memoryUsageBytes() gives only approximate results ... adding some excess bytes should make it less bad
    static constexpr auto ENTRY_OVERHEAD_BYTES_GUESS = 200uz;

    MergeTreeIndexGranulePtr granule;
    size_t memory_bytes;

    explicit VectorSimilarityIndexCacheCell(MergeTreeIndexGranulePtr granule_)
        : granule(std::move(granule_))
        , memory_bytes(granule->memoryUsageBytes() + ENTRY_OVERHEAD_BYTES_GUESS)
    {
    }

    VectorSimilarityIndexCacheCell(const VectorSimilarityIndexCacheCell &) = delete;
    VectorSimilarityIndexCacheCell & operator=(const VectorSimilarityIndexCacheCell &) = delete;
};


struct VectorSimilarityIndexCacheWeightFunction
{
    size_t operator()(const VectorSimilarityIndexCacheCell & cell) const
    {
        return cell.memory_bytes;
    }
};

/// Cache of deserialized vector index granules.
class VectorSimilarityIndexCache : public CacheBase<VectorSimilarityIndexCacheKey, VectorSimilarityIndexCacheCell, VectorSimilarityIndexCacheHashFunction, VectorSimilarityIndexCacheWeightFunction>
{
public:

    using Base = CacheBase<VectorSimilarityIndexCacheKey, VectorSimilarityIndexCacheCell, VectorSimilarityIndexCacheHashFunction, VectorSimilarityIndexCacheWeightFunction>;

    VectorSimilarityIndexCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : Base(cache_policy, CurrentMetrics::VectorSimilarityIndexCacheBytes, CurrentMetrics::VectorSimilarityIndexCacheCells, max_size_in_bytes, max_count, size_ratio)
    {}

    /// LoadFunc should have signature () -> MergeTreeIndexGranulePtr.
    template <typename LoadFunc>
    MergeTreeIndexGranulePtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto wrapped_load = [&]() -> std::shared_ptr<VectorSimilarityIndexCacheCell>
        {
            MergeTreeIndexGranulePtr granule;
            load(granule);
            return std::make_shared<VectorSimilarityIndexCacheCell>(std::move(granule));
        };

        auto result = Base::getOrSet(key, wrapped_load);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::VectorSimilarityIndexCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::VectorSimilarityIndexCacheHits);

        return result.first->granule;
    }

    void removeEntriesFromCache(const String & path_to_data_part)
    {
        Base::remove([path_to_data_part](const Key & key, const MappedPtr &) { return key.path_to_data_part == path_to_data_part; });
    }

private:
    /// Called for each individual entry being evicted from cache
    void onEntryRemoval(const size_t weight_loss, const MappedPtr & mapped_ptr) override
    {
        ProfileEvents::increment(ProfileEvents::VectorSimilarityIndexCacheWeightLost, weight_loss);
        UNUSED(mapped_ptr);
    }
};

using VectorSimilarityIndexCachePtr = std::shared_ptr<VectorSimilarityIndexCache>;

}
