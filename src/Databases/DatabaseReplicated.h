#pragma once

#include <optional>

#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseReplicatedSettings.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/BackgroundSchedulePool.h>
#include <QueryPipeline/BlockIO.h>
#include <Interpreters/Context.h>


namespace DB
{

class DatabaseReplicatedDDLWorker;
using ZooKeeperPtr = std::shared_ptr<zkutil::ZooKeeper>;

class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;

struct ReplicaInfo
{
    bool is_active;
    std::optional<UInt32> replication_lag;
    UInt64 recovery_time;
};
using ReplicasInfo = std::vector<ReplicaInfo>;

class DatabaseReplicated : public DatabaseAtomic
{
public:
    static constexpr auto ALL_GROUPS_CLUSTER_PREFIX = "all_groups.";

    DatabaseReplicated(const String & name_, const String & metadata_path_, UUID uuid,
                       const String & zookeeper_path_, const String & shard_name_, const String & replica_name_,
                       DatabaseReplicatedSettings db_settings_,
                       ContextPtr context);

    ~DatabaseReplicated() override;

    String getEngineName() const override { return "Replicated"; }

    /// If current query is initial, then the following methods add metadata updating ZooKeeper operations to current ZooKeeperMetadataTransaction.
    void dropTable(ContextPtr, const String & table_name, bool sync) override;
    void renameTable(ContextPtr context, const String & table_name, IDatabase & to_database,
                     const String & to_table_name, bool exchange, bool dictionary) override;
    void commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                           const String & table_metadata_tmp_path, const String & table_metadata_path,
                           ContextPtr query_context) override;
    void commitAlterTable(const StorageID & table_id,
                          const String & table_metadata_tmp_path, const String & table_metadata_path,
                          const String & statement, ContextPtr query_context) override;
    void detachTablePermanently(ContextPtr context, const String & table_name) override;
    void removeDetachedPermanentlyFlag(ContextPtr context, const String & table_name, const String & table_metadata_path, bool attach) override;

    bool waitForReplicaToProcessAllEntries(UInt64 timeout_ms);

    /// Try to execute DLL query on current host as initial query. If query is succeed,
    /// then it will be executed on all replicas.
    BlockIO tryEnqueueReplicatedDDL(const ASTPtr & query, ContextPtr query_context, QueryFlags flags) override;

    bool canExecuteReplicatedMetadataAlter() const override;

    bool hasReplicationThread() const override { return true; }

    void stopReplication() override;

    String getShardName() const { return shard_name; }
    String getReplicaName() const { return replica_name; }
    String getReplicaGroupName() const { return replica_group_name; }
    String getFullReplicaName() const;
    static String getFullReplicaName(const String & shard, const String & replica);
    static std::pair<String, String> parseFullReplicaName(const String & name);

    const String & getZooKeeperPath() const { return zookeeper_path; }

    /// Returns cluster consisting of database replicas
    ClusterPtr tryGetCluster() const;
    ClusterPtr tryGetAllGroupsCluster() const;

    void drop(ContextPtr /*context*/) override;

    void beforeLoadingMetadata(ContextMutablePtr context_, LoadingStrictnessLevel mode) override;

    LoadTaskPtr startupDatabaseAsync(AsyncLoader & async_loader, LoadJobSet startup_after, LoadingStrictnessLevel mode) override;

    void shutdown() override;

    std::vector<std::pair<ASTPtr, StoragePtr>> getTablesForBackup(const FilterByNameFunction & filter, const ContextPtr &) const override;
    void createTableRestoredFromBackup(const ASTPtr & create_table_query, ContextMutablePtr local_context, std::shared_ptr<IRestoreCoordination> restore_coordination, UInt64 timeout_ms) override;

    bool shouldReplicateQuery(const ContextPtr & query_context, const ASTPtr & query_ptr) const override;

    static void dropReplica(DatabaseReplicated * database, const String & database_zookeeper_path, const String & shard, const String & replica, bool throw_if_noop);

    ReplicasInfo tryGetReplicasInfo(const ClusterPtr & cluster_) const;

    void renameDatabase(ContextPtr query_context, const String & new_name) override;

    friend struct DatabaseReplicatedTask;
    friend class DatabaseReplicatedDDLWorker;
private:
    void tryConnectToZooKeeperAndInitDatabase(LoadingStrictnessLevel mode);
    bool createDatabaseNodesInZooKeeper(const ZooKeeperPtr & current_zookeeper);
    static bool looksLikeReplicatedDatabasePath(const ZooKeeperPtr & current_zookeeper, const String & path);
    void createReplicaNodesInZooKeeper(const ZooKeeperPtr & current_zookeeper);

    struct
    {
        String cluster_username{"default"};
        String cluster_password;
        String cluster_secret;
        bool cluster_secure_connection{false};
    } cluster_auth_info;

    void fillClusterAuthInfo(String collection_name, const Poco::Util::AbstractConfiguration & config);

    void checkQueryValid(const ASTPtr & query, ContextPtr query_context) const;
    void checkTableEngine(const ASTCreateQuery & query, ASTStorage & storage, ContextPtr query_context) const;


    void recoverLostReplica(const ZooKeeperPtr & current_zookeeper, UInt32 our_log_ptr, UInt32 & max_log_ptr);

    std::map<String, String> tryGetConsistentMetadataSnapshot(const ZooKeeperPtr & zookeeper, UInt32 & max_log_ptr) const;

    std::map<String, String> getConsistentMetadataSnapshotImpl(const ZooKeeperPtr & zookeeper, const FilterByNameFunction & filter_by_table_name,
                                                               size_t max_retries, UInt32 & max_log_ptr) const;

    ASTPtr parseQueryFromMetadataInZooKeeper(const String & node_name, const String & query) const;
    String readMetadataFile(const String & table_name) const;

    ClusterPtr getClusterImpl(bool all_groups = false) const;
    void setCluster(ClusterPtr && new_cluster, bool all_groups = false);

    void createEmptyLogEntry(const ZooKeeperPtr & current_zookeeper);

    bool allowMoveTableToOtherDatabaseEngine(IDatabase & to_database) const override
    {
        return is_recovering && typeid_cast<DatabaseAtomic *>(&to_database);
    }

    UInt64 getMetadataHash(const String & table_name) const;
    bool checkDigestValid(const ContextPtr & local_context, bool debug_check = true) const TSA_REQUIRES(metadata_mutex);

    /// For debug purposes only, don't use in production code
    void dumpLocalTablesForDebugOnly(const ContextPtr & local_context) const;
    void dumpTablesInZooKeeperForDebugOnly() const;
    void tryCompareLocalAndZooKeeperTablesAndDumpDiffForDebugOnly(const ContextPtr & local_context) const;

    void waitDatabaseStarted() const override;
    void stopLoading() override;

    String zookeeper_path;
    String shard_name;
    String replica_name;
    String replica_group_name;
    String replica_path;
    DatabaseReplicatedSettings db_settings;

    zkutil::ZooKeeperPtr getZooKeeper() const;

    std::atomic_bool is_readonly = true;
    std::atomic_bool is_probably_dropped = false;
    std::atomic_bool is_recovering = false;
    std::atomic_bool ddl_worker_initialized = false;
    std::unique_ptr<DatabaseReplicatedDDLWorker> ddl_worker;
    mutable std::mutex ddl_worker_mutex;
    UInt32 max_log_ptr_at_creation = 0;

    /// Usually operation with metadata are single-threaded because of the way replication works,
    /// but StorageReplicatedMergeTree may call alterTable outside from DatabaseReplicatedDDLWorker causing race conditions.
    std::mutex metadata_mutex;

    /// Sum of hashes of pairs (table_name, table_create_statement).
    /// We calculate this sum from local metadata files and compare it will value in ZooKeeper.
    /// It allows to detect if metadata is broken and recover replica.
    UInt64 tables_metadata_digest TSA_GUARDED_BY(metadata_mutex);

    mutable ClusterPtr cluster;
    mutable ClusterPtr cluster_all_groups;

    LoadTaskPtr startup_replicated_database_task TSA_GUARDED_BY(mutex);
};

}
