#pragma once

#include <Databases/DatabaseAtomic.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/BackgroundSchedulePool.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/Context.h>


namespace DB
{
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
                       Context & context);

    void drop(const Context & /*context*/) override;

    String getEngineName() const override { return "Replicated"; }

    void propose(const ASTPtr & query) override;

    BlockIO getFeedback();

private:
    void createDatabaseZooKeeperNodes();
    void createReplicaZooKeeperNodes();

    void runBackgroundLogExecutor();
    void executeLogName(const String &);
    void writeLastExecutedToDiskAndZK();

    void loadMetadataFromSnapshot();
    void createSnapshot();
    void removeOutdatedSnapshotsAndLog();

    String zookeeper_path;
    String shard_name;
    String replica_name;

    std::unique_ptr<Context> current_context; // to run executeQuery

    std::mutex log_name_mutex;
    String log_name_to_exec_with_result;

    int snapshot_period;
    int feedback_timeout;

    String last_executed_log_entry = "";

    BackgroundSchedulePool::TaskHolder background_log_executor;

    zkutil::ZooKeeperPtr current_zookeeper;        /// Use only the methods below.
    mutable std::mutex current_zookeeper_mutex;    /// To recreate the session in the background thread.

    zkutil::ZooKeeperPtr tryGetZooKeeper() const;
    zkutil::ZooKeeperPtr getZooKeeper() const;
    void setZooKeeper(zkutil::ZooKeeperPtr zookeeper);

};

}
