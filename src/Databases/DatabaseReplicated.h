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

    void drop(const Context & /*context*/) override;

    String getEngineName() const override { return "Replicated"; }

    BlockIO propose(const ASTPtr & query);

    void shutdown() override;

    void loadStoredObjects(Context & context, bool has_force_restore_data_flag, bool force_attach) override;

    String getFullReplicaName() const { return shard_name + '|' + replica_name; }

    //FIXME
    friend struct DatabaseReplicatedTask;
    friend class DatabaseReplicatedDDLWorker;
private:
    bool createDatabaseNodesInZooKeeper(const ZooKeeperPtr & current_zookeeper);
    void createReplicaNodesInZooKeeper(const ZooKeeperPtr & current_zookeeper);

    void onUnexpectedLogEntry(const String & entry_name, const ZooKeeperPtr & zookeeper);
    void recoverLostReplica(const ZooKeeperPtr & current_zookeeper, UInt32 from_snapshot);

    ASTPtr parseQueryFromMetadataInZooKeeper(const String & node_name, const String & query);

    String zookeeper_path;
    String shard_name;
    String replica_name;
    String replica_path;

    UInt32 log_entry_to_execute;

    zkutil::ZooKeeperPtr getZooKeeper() const;

    std::unique_ptr<DatabaseReplicatedDDLWorker> ddl_worker;
};

}
