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
    extern const Metric VectorSimilarityIndexCacheSize;
}

namespace DB
{

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
        CurrentMetrics::add(CurrentMetrics::VectorSimilarityIndexCacheSize, memory_bytes);
    }

    ~VectorSimilarityIndexCacheCell()
    {
        CurrentMetrics::sub(CurrentMetrics::VectorSimilarityIndexCacheSize, memory_bytes);
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
class VectorSimilarityIndexCache : public CacheBase<UInt128, VectorSimilarityIndexCacheCell, UInt128TrivialHash, VectorSimilarityIndexCacheWeightFunction>
{
public:
    using Base = CacheBase<UInt128, VectorSimilarityIndexCacheCell, UInt128TrivialHash, VectorSimilarityIndexCacheWeightFunction>;

    VectorSimilarityIndexCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : Base(cache_policy, max_size_in_bytes, max_count, size_ratio)
    {}

    static UInt128 hash(const String & path_to_data_part, const String & index_name, size_t index_mark)
    {
        SipHash hash;
        hash.update(path_to_data_part.data(), path_to_data_part.size() + 1);
        hash.update(index_name.data(), index_name.size() + 1);
        hash.update(index_mark);
        return hash.get128();
    }

    /// LoadFunc should have signature () -> MergeTreeIndexGranulePtr.
    template <typename LoadFunc>
    MergeTreeIndexGranulePtr getOrSet(const Key & key, LoadFunc && load)
    {
        auto wrapped_load = [&]() -> std::shared_ptr<VectorSimilarityIndexCacheCell> {
            MergeTreeIndexGranulePtr granule = load();
            return std::make_shared<VectorSimilarityIndexCacheCell>(std::move(granule));
        };

        auto result = Base::getOrSet(key, wrapped_load);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::VectorSimilarityIndexCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::VectorSimilarityIndexCacheHits);

        return result.first->granule;
    }

private:
    void onRemoveOverflowWeightLoss(size_t weight_loss) override
    {
        ProfileEvents::increment(ProfileEvents::VectorSimilarityIndexCacheWeightLost, weight_loss);
    }
};

using VectorSimilarityIndexCachePtr = std::shared_ptr<VectorSimilarityIndexCache>;

}
