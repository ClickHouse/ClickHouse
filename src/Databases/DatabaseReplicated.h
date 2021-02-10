#pragma once

#include <Databases/DatabaseAtomic.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/BackgroundSchedulePool.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/Context.h>
#include <DataStreams/BlockIO.h>


namespace DB
{

class DatabaseReplicatedDDLWorker;
using ZooKeeperPtr = std::shared_ptr<zkutil::ZooKeeper>;

class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;

/** DatabaseReplicated engine
  * supports replication of metadata
  * via DDL log being written to ZooKeeper
  * and executed on all of the replicas
  * for a given database.
  *
  * One Clickhouse server can have multiple
  * replicated databases running and updating
  * at the same time.
  * 
  * The engine has two parameters ZooKeeper path and 
  * replica name.
  * The same ZooKeeper path corresponds to the same
  * database. Replica names MUST be different for all replicas
  * of the same database.
  *
  * Using this engine, creation of Replicated tables
  * requires no ZooKeeper path and replica name parameters.
  * Table's replica name is the same as database replica name.
  * Table's ZooKeeper path is a concatenation of database
  * ZooKeeper path, /tables/, and UUID of the table.
  */
class DatabaseReplicated : public DatabaseAtomic
{
public:
    DatabaseReplicated(const String & name_, const String & metadata_path_, UUID uuid,
                       const String & zookeeper_path_, const String & shard_name_, const String & replica_name_,
                       const Context & context);

    ~DatabaseReplicated() override;

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

    void drop(const Context & /*context*/) override;

    String getEngineName() const override { return "Replicated"; }

    BlockIO propose(const ASTPtr & query, const Context & query_context);

    void stopReplication();
    void shutdown() override;

    void loadStoredObjects(Context & context, bool has_force_restore_data_flag, bool force_attach) override;

    String getFullReplicaName() const;
    static std::pair<String, String> parseFullReplicaName(const String & name);

    ClusterPtr getCluster() const;

    //FIXME
    friend struct DatabaseReplicatedTask;
    friend class DatabaseReplicatedDDLWorker;
private:
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

    zkutil::ZooKeeperPtr getZooKeeper() const;

    std::unique_ptr<DatabaseReplicatedDDLWorker> ddl_worker;
};

}
