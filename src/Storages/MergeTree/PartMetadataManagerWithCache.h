#pragma once

#include "config_core.h"

#if USE_ROCKSDB
#include <Storages/MergeTree/IPartMetadataManager.h>
#include <Storages/MergeTree/MergeTreeMetadataCache.h>

namespace DB
{

/// PartMetadataManagerWithCache stores metadatas of part in RocksDB as cache layer to speed up
/// loading process of merge tree table.
class PartMetadataManagerWithCache : public IPartMetadataManager
{
public:
    PartMetadataManagerWithCache(const IMergeTreeDataPart * part_, const MergeTreeMetadataCachePtr & cache_);

    ~PartMetadataManagerWithCache() override = default;

    /// First read the metadata from RocksDB cache, then from disk.
    std::unique_ptr<ReadBuffer> read(const String & file_name) const override;

    /// First judge existence of the metadata in RocksDB cache, then in disk.
    bool exists(const String & file_name) const override;

    /// Delete all metadatas in part from RocksDB cache.
    void deleteAll(bool include_projection) override;

    /// Assert all metadatas in part from RocksDB cache are deleted.
    void assertAllDeleted(bool include_projection) const override;

    /// Update all metadatas in part from RocksDB cache.
    /// Need to be called after part directory is renamed.
    void updateAll(bool include_projection) override;

    /// Check if all metadatas in part from RocksDB cache are up to date.
    std::unordered_map<String, uint128> check() const override;

private:
    /// Get cache key from path of metadata file.
    /// Format: <disk_name>:relative/full/path/of/metadata/file
    String getKeyFromFilePath(const String & file_path) const;

    /// Get metadata file path from cache key.
    String getFilePathFromKey(const String & key) const;

    /// Get cache keys and checksums of corresponding metadata in a part(including projection parts)
    void getKeysAndCheckSums(Strings & keys, std::vector<uint128> & checksums) const;

    MergeTreeMetadataCachePtr cache;
};

}
#endif
