#pragma once

#include "config_core.h"

#if USE_ROCKSDB
#include <Storages/MergeTree/IPartMetadataManager.h>
#include <Storages/MergeTree/MergeTreeMetadataCache.h>

namespace DB
{

class PartMetadataManagerWithCache : public IPartMetadataManager
{
public:
    PartMetadataManagerWithCache(const IMergeTreeDataPart * part_, const MergeTreeMetadataCachePtr & cache_);

    ~PartMetadataManagerWithCache() override = default;

    std::unique_ptr<SeekableReadBuffer> read(const String & file_name) const override;

    bool exists(const String & file_name) const override;

    void deleteAll(bool include_projection) override;

    void assertAllDeleted(bool include_projection) const override;

    void updateAll(bool include_projection) override;

    std::unordered_map<String, uint128> check() const override;

private:
    String getKeyFromFilePath(const String & file_path) const;
    String getFilePathFromKey(const String & key) const;

    void getKeysAndCheckSums(Strings & keys, std::vector<uint128> & checksums) const;


    MergeTreeMetadataCachePtr cache;
};

}
#endif
