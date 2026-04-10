#pragma once

#include <Common/Clusters/ClusterCatalogTypes.h>
#include <Common/Clusters/ClustersMetadataStorage.h>
#include <Common/Clusters/ShardsMetadataStorage.h>
#include <Common/NamedCollections/NamedCollections_fwd.h>
#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

#include <boost/noncopyable.hpp>

#include <atomic>
#include <cstdint>

#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

/// Row for `system.shards` (SQL `CREATE SHARD` catalog).
struct SQLShardCatalogTableRow
{
    String name;
    std::vector<String> replica_collections;
    UInt32 weight = 0;
    bool internal_replication = false;
    std::vector<String> referenced_by_clusters;
};

/// Where a resolvable cluster name is defined (`ClusterFactory::cluster_registrations`).
enum class ClusterDefinitionSource : uint8_t
{
    RemoteServersConfig,
    SqlCatalog,
};

struct ClusterDefinitionRegistration
{
    ClusterDefinitionSource source = ClusterDefinitionSource::RemoteServersConfig;
    /// For `RemoteServersConfig`: typically `Context::getClustersVersion()` after reload.
    /// For `SqlCatalog`: bumped when the SQL catalog row is created or reloaded from disk.
    UInt64 definition_version = 0;
};

/// Central registry for SQL `CREATE SHARD` / `CREATE CLUSTER` and config `remote_servers` name registration,
/// following the same split as `NamedCollectionFactory`: in-memory maps plus `ShardsMetadataStorage` /
/// `ClustersMetadataStorage` for on-disk `CREATE` text (`.sql` per entity).
class ClusterFactory : boost::noncopyable
{
public:
    static ClusterFactory & instance();

    ~ClusterFactory();

    void initialize(const String & data_path);
    void shutdown();

    void reloadFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, UInt64 remote_servers_definition_version);

    void reloadFromSQL();

    void createShard(const String & shard_name, const std::vector<String> & replica_collections, UInt32 weight, bool internal_replication);
    void dropShard(const String & shard_name, bool if_exists);

    void createCluster(const String & cluster_name, const std::vector<String> & members);
    bool dropCluster(const String & cluster_name, bool if_exists);

    bool hasShard(const String & name) const;
    bool hasCluster(const String & name) const;

    std::vector<String> listClusterNames() const;

    String getShowCreateShard(const String & name) const;
    String getShowCreateCluster(const String & name) const;

    ClusterPtr tryMaterializeCluster(const String & cluster_name, ContextPtr context) const;

    std::optional<String> namedCollectionDropBlockReason(const String & collection_name) const;

    std::vector<SQLShardCatalogTableRow> listShardsForSystemTable() const;

    std::optional<ClusterDefinitionRegistration> getClusterRegistration(const String & cluster_name) const;

private:
    mutable std::mutex mutex;

    const LoggerPtr log = getLogger("ClusterFactory");

    bool initialized = false;
    std::atomic<bool> shutdown_called = false;

    std::unique_ptr<ShardsMetadataStorage> shards_metadata_storage;
    std::unique_ptr<ClustersMetadataStorage> clusters_metadata_storage;

    std::unordered_map<String, ShardCatalogDefinition> loaded_sql_shards;
    std::unordered_map<String, ClusterCatalogDefinition> loaded_sql_clusters;

    std::unordered_map<String, ClusterDefinitionRegistration> cluster_registrations;
    UInt64 sql_catalog_mutation_counter = 0;

    void reloadSqlDefinitionsLocked();
    void rebuildSqlClusterRegistrationsLocked();

    BackgroundSchedulePoolTaskHolder sql_catalog_update_task;
    void updateFunc();

    static void loadNamedCollectionsIfNeeded();
    static bool namedCollectionExists(const String & name);
    static NamedCollectionPtr getNamedCollection(const String & name);
};

}
