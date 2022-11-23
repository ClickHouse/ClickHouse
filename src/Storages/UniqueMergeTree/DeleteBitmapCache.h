#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/UniqueMergeTree/DeleteBitmap.h>
#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>
#include <Common/SipHash.h>

namespace DB
{

class StorageUniqueMergeTree;

struct BitmapCacheKey
{
    BitmapCacheKey(const MergeTreePartInfo & part_info_, UInt64 version_) : part_info(part_info_), version(version_) { }
    MergeTreePartInfo part_info;
    UInt64 version;

    bool operator==(const BitmapCacheKey & rhs) const { return part_info == rhs.part_info && version == rhs.version; }
};

/// Just use min_block to hash is enough because block number is increment
struct BitmapCacheKeyHash
{
    size_t operator()(const BitmapCacheKey & key) const
    {
        return sipHash64(key.part_info.partition_id.data(), key.part_info.partition_id.size()) ^ intHash64(key.part_info.min_block)
            ^ sipHash64(key.version);
    }
};

class DeleteBitmapCache : public CacheBase<BitmapCacheKey, DeleteBitmap, BitmapCacheKeyHash>
{
public:
    explicit DeleteBitmapCache() : CacheBase(1024) { }

    DeleteBitmapPtr getOrCreate(const MergeTreeDataPartPtr & part, UInt64 version);

private:
};
}
