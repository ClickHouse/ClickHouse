#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>

namespace ProfileEvents
{
    extern const Event SecondaryIndexCacheMisses;
    extern const Event SecondaryIndexCacheHits;
    extern const Event SecondaryIndexCacheBytesEvicted;
}

namespace CurrentMetrics
{
    extern const Metric SecondaryIndexCacheSize;
}

namespace DB
{

struct SecondaryIndexCacheCell
{
    MergeTreeIndexGranulePtr granule;

    size_t memory_bytes;

    SecondaryIndexCacheCell(MergeTreeIndexGranulePtr g) : granule(std::move(g)), memory_bytes(granule->memoryUsageBytes())
    {
        CurrentMetrics::add(CurrentMetrics::SecondaryIndexCacheSize, memory_bytes);
    }

    ~SecondaryIndexCacheCell()
    {
        CurrentMetrics::sub(CurrentMetrics::SecondaryIndexCacheSize, memory_bytes);
    }

    SecondaryIndexCacheCell(const SecondaryIndexCacheCell &) = delete;
    SecondaryIndexCacheCell & operator=(const SecondaryIndexCacheCell &) = delete;
};

struct SecondaryIndexCacheWeightFunction
{
    size_t operator()(const SecondaryIndexCacheCell & x) const
    {
        return x.memory_bytes;
    }
};


/// Cache of deserialized index granules.
class SecondaryIndexCache : public CacheBase<UInt128, SecondaryIndexCacheCell, UInt128TrivialHash, SecondaryIndexCacheWeightFunction>
{
public:
    using Base = CacheBase<UInt128, SecondaryIndexCacheCell, UInt128TrivialHash, SecondaryIndexCacheWeightFunction>;

    SecondaryIndexCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : Base(cache_policy, max_size_in_bytes, max_count, size_ratio) {}

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
        auto wrapped_load = [&] {
            MergeTreeIndexGranulePtr granule = load();
            return std::make_shared<SecondaryIndexCacheCell>(std::move(granule));
        };
        auto result = Base::getOrSet(key, wrapped_load);

        if (result.second)
            ProfileEvents::increment(ProfileEvents::SecondaryIndexCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::SecondaryIndexCacheHits);

        return result.first->granule;
    }

private:
    void onRemoveOverflowWeightLoss(size_t weight_loss) override
    {
        ProfileEvents::increment(ProfileEvents::SecondaryIndexCacheBytesEvicted, weight_loss);
    }
};

using SecondaryIndexCachePtr = std::shared_ptr<SecondaryIndexCache>;

}
