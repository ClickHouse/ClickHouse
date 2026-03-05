#pragma once

#include <atomic>
#include <optional>

#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseReplicatedSettings.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/QueryFlags.h>
#include <Interpreters/DDLReplicator.h>


namespace zkutil
{
class ZooKeeper;
}

namespace DB
{

class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;

struct ReplicaInfo
{
    bool is_active;
    bool unsynced_after_recovery;
    std::optional<UInt32> replication_lag;
    UInt64 recovery_time;
};

struct ReplicasInfo
{
    std::vector<ReplicaInfo> replicas;
    bool replicas_belong_to_shared_catalog;
};

class DatabaseReplicated : public DatabaseAtomic, public DDLReplicator
{
public:
    static constexpr auto ALL_GROUPS_CLUSTER_PREFIX = "all_groups.";
    static constexpr auto BROKEN_TABLES_SUFFIX = "_broken_tables";
    static constexpr auto BROKEN_REPLICATED_TABLES_SUFFIX = "_broken_replicated_tables";

    /** For the system table database replicas. */
    struct ReplicatedStatus
    {
        bool is_readonly;
        bool is_session_expired;
        UInt32 max_log_ptr;
        String replica_name;
        String replica_path;
        String zookeeper_path;
        String shard_name;
        UInt32 log_ptr;
        UInt32 total_replicas;
        String zookeeper_exception;
    };

    DatabaseReplicated(const String & name_, const String & metadata_path_, UUID uuid,
                       const String & zookeeper_name_, const String & zookeeper_path_,
                       const String & shard_name_, const String & replica_name_,
                       DatabaseReplicatedSettings db_settings_,
                       ContextPtr context);

    ~DatabaseReplicated() override;

    String getEngineName() const override { return "Replicated"; }

    /// If current query is initial, then the following methods add metadata updating ZooKeeper operations to current ZooKeeperMetadataTransaction.
    void dropTable(ContextPtr, const String & table_name, bool sync) override;
    void renameTable(ContextPtr context, const String & table_name, IDatabase & to_database,
                     const String & to_table_name, bool exchange, bool dictionary) override;
    void detachTablePermanently(ContextPtr context, const String & table_name) override;
    void removeDetachedPermanentlyFlag(ContextPtr context, const String & table_name, const String & table_metadata_path, bool attach) override;

    bool waitForReplicaToProcessAllEntries(ContextPtr context_, UInt64 timeout_ms, SyncReplicaMode mode = SyncReplicaMode::DEFAULT) override; /// NOLINT

    /// Try to execute DLL query on current host as initial query. If query is succeed,
    /// then it will be executed on all replicas.
    BlockIO tryEnqueueReplicatedDDL(const ASTPtr & query, ContextPtr query_context, QueryFlags flags, DDLGuardPtr && database_guard) override;

    bool canExecuteReplicatedMetadataAlter() const override;

    /// RAII guard to suppress digest checks during SYSTEM RESTART REPLICA.
    /// The table is temporarily removed from the in-memory tables map during restart,
    /// making it inconsistent with tables_metadata_digest (which remains correct).
    struct RestartReplicaGuard
    {
        explicit RestartReplicaGuard(DatabaseReplicated & db_) : db(db_) { db.tables_being_restarted.fetch_add(1); }
        ~RestartReplicaGuard() { db.tables_being_restarted.fetch_sub(1); }
        RestartReplicaGuard(const RestartReplicaGuard &) = delete;
        RestartReplicaGuard & operator=(const RestartReplicaGuard &) = delete;
    private:
        DatabaseReplicated & db;
    };

    bool hasReplicationThread() const override { return true; }

    void stopReplication() override;

    String getName() const override;
    const DDLReplicatorSettings & getSettings() const override;

    void getStatus(ReplicatedStatus& response, bool with_zk_fields) const;

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

    static void dropReplica(
        DatabaseReplicated * database,
        const String & zookeeper_name_,
        const String & database_zookeeper_path,
        const String & shard,
        const String & replica,
        bool throw_if_noop);

    void restoreDatabaseInKeeper(ContextPtr ctx);

    ReplicasInfo tryGetReplicasInfo(const ClusterPtr & cluster_) const;

    void renameDatabase(ContextPtr query_context, const String & new_name) override;

    static ASTPtr parseQueryFromMetadataInZooKeeper(
        ContextPtr context_, const String & database_name_, const String & zookeeper_path_, const String & node_name, const String & query);

    friend struct DatabaseReplicatedTask;
    friend class DatabaseReplicatedDDLWorker;

protected:
    void commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                           const String & table_metadata_tmp_path, const String & table_metadata_path,
                           ContextPtr query_context) override;
    void commitAlterTable(const StorageID & table_id,
                          const String & table_metadata_tmp_path, const String & table_metadata_path,
                          const String & statement, ContextPtr query_context) override;

private:
    void tryConnectToZooKeeperAndInitDatabase(LoadingStrictnessLevel mode);
    void initDatabaseReplica(const ZooKeeperPtr & current_zookeeper, LoadingStrictnessLevel mode);
    /// For Replicated database will return ATTACH for MVs with inner table
    ASTPtr tryGetCreateOrAttachTableQuery(const String & name, ContextPtr context) const;

    struct
    {
        String cluster_username{"default"};
        String cluster_password;
        String cluster_secret;
        bool cluster_secure_connection{false};
    } cluster_auth_info;

    String getHostID(ContextPtr global_context, bool secure) const override;
    LoggerPtr getLogger() const override { return log; }

    void fillClusterAuthInfo(String collection_name, const Poco::Util::AbstractConfiguration & config);

    void checkQueryValid(const ASTPtr & query, ContextPtr query_context) const;
    void checkTableEngine(const ASTCreateQuery & query, ASTStorage & storage, ContextPtr query_context) const;

    void recoverLostReplica(const ZooKeeperPtr & current_zookeeper, UInt32 our_log_ptr, UInt32 & max_log_ptr) override;

    static ASTPtr parseQueryFromMetadata(
        ContextPtr context_, const String & database_name_, const String & table_name, const String & query, const String & description);
    ASTPtr parseQueryFromMetadataOnDisk(const String & table_name) const;
    String readMetadataFile(const String & table_name) const;

    ClusterPtr getClusterImpl(bool all_groups = false) const;
    void setCluster(ClusterPtr && new_cluster, bool all_groups = false);

    bool allowMoveTableToOtherDatabaseEngine(IDatabase & to_database) const override
    {
        return is_recovering && typeid_cast<DatabaseAtomic *>(&to_database);
    }

    UInt64 getLocalDigest() const override;
    UInt64 getMetadataHash(const String & table_name) const;
    bool checkDigestValid(const ContextPtr & local_context) const TSA_REQUIRES(metadata_mutex) override;

    /// For debug purposes only, don't use in production code
    void tryCompareLocalAndZooKeeperTablesAndDumpDiffForDebugOnly(const ContextPtr & local_context) const;

    void waitDatabaseStarted() const override;
    void stopLoading() override;

    void initDDLWorkerUnlocked() TSA_REQUIRES(ddl_worker_mutex);

    void restoreDatabaseNodesInKeeper(const ZooKeeperPtr & zookeeper);
    void reinitializeDDLWorker();

    DatabaseReplicatedSettings db_settings;

    std::atomic_bool is_probably_dropped = false;

    /// Counter for tables currently being restarted by SYSTEM RESTART REPLICA.
    /// During restart, the table is temporarily removed from the in-memory tables map,
    /// making it inconsistent with tables_metadata_digest. We skip digest checks
    /// while any restart is in progress to avoid false LOGICAL_ERROR exceptions in debug builds.
    std::atomic<int> tables_being_restarted{0};

    mutable ClusterPtr cluster;
    mutable ClusterPtr cluster_all_groups;

    LoadTaskPtr startup_replicated_database_task TSA_GUARDED_BY(mutex);
};

}
