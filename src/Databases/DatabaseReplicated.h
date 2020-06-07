#pragma once

#include <Databases/DatabaseAtomic.h>
#include <Common/randomSeed.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/BackgroundSchedulePool.h>

#include <atomic>
#include <thread>

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
  * database. Replica names must be different for all replicas
  * of the same database.
  *
  * Using this engine, creation of Replicated tables
  * requires no ZooKeeper path and replica name parameters.
  * Table's replica name is the same as database replica name.
  * Table's ZooKeeper path is a concatenation of database's
  * ZooKeeper path, /tables/, and UUID of the table.
  */
class DatabaseReplicated : public DatabaseAtomic
{
public:
    DatabaseReplicated(const String & name_, const String & metadata_path_, const String & zookeeper_path_, const String & replica_name_, Context & context);

    String getEngineName() const override { return "Replicated"; }

    void propose(const ASTPtr & query) override;

    String zookeeper_path;
    String replica_name;

private:
    void createDatabaseZKNodes();

    void runBackgroundLogExecutor();

    void executeFromZK(String & path);

    void saveState();
    void updateSnapshot();
    void loadMetadataFromSnapshot();

    std::unique_ptr<Context> current_context; // to run executeQuery

    std::atomic<size_t> current_log_entry_n = 0;

    String last_executed_log_entry = "";

    BackgroundSchedulePool::TaskHolder background_log_executor;

    String replica_path;

    zkutil::ZooKeeperPtr current_zookeeper;        /// Use only the methods below.
    mutable std::mutex current_zookeeper_mutex;    /// To recreate the session in the background thread.

    zkutil::ZooKeeperPtr tryGetZooKeeper() const;
    zkutil::ZooKeeperPtr getZooKeeper() const;
    void setZooKeeper(zkutil::ZooKeeperPtr zookeeper);

};

}
