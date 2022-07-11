#pragma once

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

class DatabaseReplicated : public DatabaseAtomic
{
public:
    DatabaseReplicated(const String & name_, const String & metadata_path_, UUID uuid,
                       const String & zookeeper_path_, const String & shard_name_, const String & replica_name_,
                       DatabaseReplicatedSettings db_settings_,
                       ContextPtr context);

    ~DatabaseReplicated() override;

    String getEngineName() const override { return "Replicated"; }

    /// If current query is initial, then the following methods add metadata updating ZooKeeper operations to current ZooKeeperMetadataTransaction.
    void dropTable(ContextPtr, const String & table_name, bool no_delay) override;
    void renameTable(ContextPtr context, const String & table_name, IDatabase & to_database,
                     const String & to_table_name, bool exchange, bool dictionary) override;
    void commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                           const String & table_metadata_tmp_path, const String & table_metadata_path,
                           ContextPtr query_context) override;
    void commitAlterTable(const StorageID & table_id,
                          const String & table_metadata_tmp_path, const String & table_metadata_path,
                          const String & statement, ContextPtr query_context) override;
    void detachTablePermanently(ContextPtr context, const String & table_name) override;
    void removeDetachedPermanentlyFlag(ContextPtr context, const String & table_name, const String & table_metadata_path, bool attach) const override;

    /// Try to execute DLL query on current host as initial query. If query is succeed,
    /// then it will be executed on all replicas.
    BlockIO tryEnqueueReplicatedDDL(const ASTPtr & query, ContextPtr query_context);

    bool hasReplicationThread() const override { return true; }

    void stopReplication() override;

    String getShardName() const { return shard_name; }
    String getReplicaName() const { return replica_name; }
    String getFullReplicaName() const;
    static std::pair<String, String> parseFullReplicaName(const String & name);

    /// Returns cluster consisting of database replicas
    ClusterPtr getCluster() const;

    void drop(ContextPtr /*context*/) override;

    void loadStoredObjects(ContextMutablePtr context, bool force_restore, bool force_attach, bool skip_startup_tables) override;

    void beforeLoadingMetadata(ContextMutablePtr context, bool force_restore, bool force_attach) override;

    void startupTables(ThreadPool & thread_pool, bool force_restore, bool force_attach) override;

    void shutdown() override;

    friend struct DatabaseReplicatedTask;
    friend class DatabaseReplicatedDDLWorker;
private:
    void tryConnectToZooKeeperAndInitDatabase(bool force_attach);
    bool createDatabaseNodesInZooKeeper(const ZooKeeperPtr & current_zookeeper);
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

    void recoverLostReplica(const ZooKeeperPtr & current_zookeeper, UInt32 our_log_ptr, UInt32 max_log_ptr);
    std::map<String, String> tryGetConsistentMetadataSnapshot(const ZooKeeperPtr & zookeeper, UInt32 & max_log_ptr);

    ASTPtr parseQueryFromMetadataInZooKeeper(const String & node_name, const String & query);
    String readMetadataFile(const String & table_name) const;

    ClusterPtr getClusterImpl() const;
    void setCluster(ClusterPtr && new_cluster);

    void createEmptyLogEntry(const ZooKeeperPtr & current_zookeeper);

    bool allowMoveTableToOtherDatabaseEngine(IDatabase & to_database) const override
    {
        return is_recovering && typeid_cast<DatabaseAtomic *>(&to_database);
    }

    String zookeeper_path;
    String shard_name;
    String replica_name;
    String replica_path;
    DatabaseReplicatedSettings db_settings;

    zkutil::ZooKeeperPtr getZooKeeper() const;

    std::atomic_bool is_readonly = true;
    std::atomic_bool is_recovering = false;
    std::unique_ptr<DatabaseReplicatedDDLWorker> ddl_worker;
    UInt32 max_log_ptr_at_creation = 0;

    mutable ClusterPtr cluster;
};

}
