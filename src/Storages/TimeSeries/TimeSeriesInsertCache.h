#pragma once

#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>
#include <base/extended_types.h>

#include <vector>


namespace DB
{

struct TimeSeriesInsertCacheWeightFunction
{
    static constexpr size_t OVERHEAD = 96;

    size_t operator()(const UInt128 & mapped) const
    {
        return sizeof(mapped) + OVERHEAD;
    }
};

class TimeSeriesInsertCache
    : public CacheBase<UInt128, UInt128, UInt128TrivialHash, TimeSeriesInsertCacheWeightFunction>
{
public:
    using Base = CacheBase<UInt128, UInt128, UInt128TrivialHash, TimeSeriesInsertCacheWeightFunction>;

    struct Entry
    {
        UInt128 key_hash;
        UInt128 value_hash;
    };

    explicit TimeSeriesInsertCache(size_t max_size_in_bytes);

    bool contains(UInt128 key_hash, UInt128 value_hash);
    void insert(const std::vector<Entry> & entries);
};

}
