#pragma once

#include <Common/Clusters/ClusterCatalogTypes.h>
#include <Common/Clusters/ClusterCatalogMetadataBackend.h>
#include <Interpreters/Context_fwd.h>

#include <Common/logger_useful.h>

#include <memory>
#include <string>
#include <unordered_map>

namespace DB
{

/// Persists SQL `CREATE SHARD` definitions. Storage backend is selected by `shards_catalog_storage` in the
/// server config (same pattern as `named_collections_storage`: `local`, `local_encrypted`, `keeper`, …).
class ShardsMetadataStorage : private WithContext
{
public:
    /// `default_local_directory_path` is used when `shards_catalog_storage.type` is `local` and `path` is not set.
    static std::unique_ptr<ShardsMetadataStorage> create(const ContextPtr & context_, const String & default_local_directory_path);

    std::unordered_map<String, ShardCatalogDefinition> getAll() const;

    void writeCreateStatement(const String & shard_name, const String & create_statement_sql, bool replace = false);
    void remove(const String & shard_name);
    bool removeIfExists(const String & shard_name);

    void shutdown();

    bool isReplicated() const;
    /// Blocks up to `shards_catalog_storage.update_timeout_ms` when backend is Keeper.
    bool waitCatalogUpdate();

private:
    ShardsMetadataStorage(ContextPtr context_, std::unique_ptr<ClusterCatalogMetadataStorage> catalog_metadata_storage_);

    std::unique_ptr<ClusterCatalogMetadataStorage> catalog_metadata_storage;

    const LoggerPtr logger = getLogger("ShardsMetadataStorage");

    static String fileNameForShard(const String & shard_name);
};

}
