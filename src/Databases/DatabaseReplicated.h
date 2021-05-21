#pragma once

#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseReplicatedSettings.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/BackgroundSchedulePool.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/OneBlockInputStream.h>
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
                       const Context & context);

    ~DatabaseReplicated() override;

    String getEngineName() const override { return "Replicated"; }

    /// If current query is initial, then the following methods add metadata updating ZooKeeper operations to current ZooKeeperMetadataTransaction.
    void dropTable(const Context &, const String & table_name, bool no_delay) override;
    void renameTable(const Context & context, const String & table_name, IDatabase & to_database,
                     const String & to_table_name, bool exchange, bool dictionary) override;
    void commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                           const String & table_metadata_tmp_path, const String & table_metadata_path,
                           const Context & query_context) override;
    void commitAlterTable(const StorageID & table_id,
                          const String & table_metadata_tmp_path, const String & table_metadata_path,
                          const String & statement, const Context & query_context) override;
    void createDictionary(const Context & context,
                          const String & dictionary_name,
                          const ASTPtr & query) override;
    void removeDictionary(const Context & context, const String & dictionary_name) override;
    void detachTablePermanently(const Context & context, const String & table_name) override;

    /// Try to execute DLL query on current host as initial query. If query is succeed,
    /// then it will be executed on all replicas.
    BlockIO tryEnqueueReplicatedDDL(const ASTPtr & query, const Context & query_context);

    void stopReplication();

    String getFullReplicaName() const;
    static std::pair<String, String> parseFullReplicaName(const String & name);

    /// Returns cluster consisting of database replicas
    ClusterPtr getCluster() const;

    void drop(const Context & /*context*/) override;

    void loadStoredObjects(Context & context, bool has_force_restore_data_flag, bool force_attach) override;
    void shutdown() override;

    friend struct DatabaseReplicatedTask;
    friend class DatabaseReplicatedDDLWorker;
private:
    void tryConnectToZooKeeperAndInitDatabase(bool force_attach);
    bool createDatabaseNodesInZooKeeper(const ZooKeeperPtr & current_zookeeper);
    void createReplicaNodesInZooKeeper(const ZooKeeperPtr & current_zookeeper);

    void recoverLostReplica(const ZooKeeperPtr & current_zookeeper, UInt32 our_log_ptr, UInt32 max_log_ptr);
    std::map<String, String> tryGetConsistentMetadataSnapshot(const ZooKeeperPtr & zookeeper, UInt32 & max_log_ptr);

    ASTPtr parseQueryFromMetadataInZooKeeper(const String & node_name, const String & query);
    String readMetadataFile(const String & table_name) const;

    String zookeeper_path;
    String shard_name;
    String replica_name;
    String replica_path;
    DatabaseReplicatedSettings db_settings;

    zkutil::ZooKeeperPtr getZooKeeper() const;

    std::atomic_bool is_readonly = true;
    std::unique_ptr<DatabaseReplicatedDDLWorker> ddl_worker;
};

}
