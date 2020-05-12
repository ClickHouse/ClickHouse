#pragma once

#include <Databases/DatabaseOrdinary.h>
#include <Common/randomSeed.h>
#include <Common/ZooKeeper/ZooKeeper.h>

#include <atomic>
#include <thread>

namespace DB
{
/** Replicated database engine.
  * It stores tables list using list of .sql files,
  *  that contain declaration of table represented by SQL ATTACH TABLE query
  *  and operation log in zookeeper
  */
class DatabaseReplicated : public DatabaseOrdinary
{
public:
    DatabaseReplicated(const String & name_, const String & metadata_path_, const String & zookeeper_path_, const String & replica_name_, Context & context);

    ~DatabaseReplicated();

    String getEngineName() const override { return "Replicated"; }

    void propose(const ASTPtr & query) override;

private:

    void runMainThread();
    void runCleanupThread();

    void attachToThreadGroup();
    
    void executeLog(size_t n);

    std::unique_ptr<Context> current_context; // to run executeQuery

    std::atomic<size_t> current_log_entry_n = 0;
    std::atomic<bool> stop_flag{false};

    ThreadFromGlobalPool main_thread;
    ThreadGroupStatusPtr thread_group;

    String zookeeper_path;
    String replica_name;
    String replica_path;

    zkutil::ZooKeeperPtr current_zookeeper;        /// Use only the methods below.
    mutable std::mutex current_zookeeper_mutex;    /// To recreate the session in the background thread.

    zkutil::ZooKeeperPtr tryGetZooKeeper() const;
    zkutil::ZooKeeperPtr getZooKeeper() const;
    void setZooKeeper(zkutil::ZooKeeperPtr zookeeper);

};

}
