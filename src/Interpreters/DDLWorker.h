#pragma once

#include <Common/CurrentThread.h>
#include <Common/DNSResolver.h>
#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Storages/IStorage_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace zkutil
{
    class ZooKeeper;
}

namespace Poco
{
    class Logger;
    namespace Util { class AbstractConfiguration; }
}

namespace Coordination
{
    struct Stat;
}

namespace zkutil
{
    class ZooKeeperLock;
}

namespace DB
{
class ASTAlterQuery;
struct DDLLogEntry;
struct DDLTaskBase;
using DDLTaskPtr = std::unique_ptr<DDLTaskBase>;
using ZooKeeperPtr = std::shared_ptr<zkutil::ZooKeeper>;
class AccessRightsElements;

class DDLWorker
{
public:
    DDLWorker(int pool_size_, const std::string & zk_root_dir, ContextPtr context_, const Poco::Util::AbstractConfiguration * config, const String & prefix,
              const String & logger_name = "DDLWorker", const CurrentMetrics::Metric * max_entry_metric_ = nullptr, const CurrentMetrics::Metric * max_pushed_entry_metric_ = nullptr);
    virtual ~DDLWorker();

    /// Pushes query into DDL queue, returns path to created node
    virtual String enqueueQuery(DDLLogEntry & entry);

    /// Host ID (name:port) for logging purposes
    /// Note that in each task hosts are identified individually by name:port from initiator server cluster config
    std::string getCommonHostID() const
    {
        return host_fqdn_id;
    }

    void startup();
    virtual void shutdown();

    bool isCurrentlyActive() const { return initialized && !stop_flag; }

protected:

    /// Returns cached ZooKeeper session (possibly expired).
    ZooKeeperPtr tryGetZooKeeper() const;
    /// If necessary, creates a new session and caches it.
    ZooKeeperPtr getAndSetZooKeeper();

    /// Iterates through queue tasks in ZooKeeper, runs execution of new tasks
    void scheduleTasks(bool reinitialized);

    DDLTaskBase & saveTask(DDLTaskPtr && task);

    /// Reads entry and check that the host belongs to host list of the task
    /// Returns non-empty DDLTaskPtr if entry parsed and the check is passed
    virtual DDLTaskPtr initAndCheckTask(const String & entry_name, String & out_reason, const ZooKeeperPtr & zookeeper);

    void processTask(DDLTaskBase & task, const ZooKeeperPtr & zookeeper);
    void updateMaxDDLEntryID(const String & entry_name);

    /// Check that query should be executed on leader replica only
    static bool taskShouldBeExecutedOnLeader(const ASTPtr & ast_ddl, StoragePtr storage);

    /// Executes query only on leader replica in case of replicated table.
    /// Queries like TRUNCATE/ALTER .../OPTIMIZE have to be executed only on one node of shard.
    /// Most of these queries can be executed on non-leader replica, but actually they still send
    /// query via RemoteBlockOutputStream to leader, so to avoid such "2-phase" query execution we
    /// execute query directly on leader.
    bool tryExecuteQueryOnLeaderReplica(
        DDLTaskBase & task,
        StoragePtr storage,
        const String & rewritten_query,
        const String & node_path,
        const ZooKeeperPtr & zookeeper,
        std::unique_ptr<zkutil::ZooKeeperLock> & execute_on_leader_lock);

    bool tryExecuteQuery(const String & query, DDLTaskBase & task, const ZooKeeperPtr & zookeeper);

    /// Checks and cleanups queue's nodes
    void cleanupQueue(Int64 current_time_seconds, const ZooKeeperPtr & zookeeper);
    virtual bool canRemoveQueueEntry(const String & entry_name, const Coordination::Stat & stat);

    /// Init task node
    void createStatusDirs(const std::string & node_path, const ZooKeeperPtr & zookeeper);

    /// Return false if the worker was stopped (stop_flag = true)
    virtual bool initializeMainThread();

    void runMainThread();
    void runCleanupThread();

    ContextMutablePtr context;
    Poco::Logger * log;

    std::string host_fqdn;      /// current host domain name
    std::string host_fqdn_id;   /// host_name:port
    std::string queue_dir;      /// dir with queue of queries

    mutable std::mutex zookeeper_mutex;
    ZooKeeperPtr current_zookeeper;

    /// Save state of executed task to avoid duplicate execution on ZK error
    std::optional<String> last_skipped_entry_name;
    std::optional<String> first_failed_task_name;
    std::list<DDLTaskPtr> current_tasks;

    Coordination::Stat queue_node_stat;
    std::shared_ptr<Poco::Event> queue_updated_event = std::make_shared<Poco::Event>();
    std::shared_ptr<Poco::Event> cleanup_event = std::make_shared<Poco::Event>();
    std::atomic<bool> initialized = false;
    std::atomic<bool> stop_flag = false;

    ThreadFromGlobalPool main_thread;
    ThreadFromGlobalPool cleanup_thread;

    /// Size of the pool for query execution.
    size_t pool_size = 1;
    std::unique_ptr<ThreadPool> worker_pool;

    /// Cleaning starts after new node event is received if the last cleaning wasn't made sooner than N seconds ago
    Int64 cleanup_delay_period = 60; // minute (in seconds)
    /// Delete node if its age is greater than that
    Int64 task_max_lifetime = 7 * 24 * 60 * 60; // week (in seconds)
    /// How many tasks could be in the queue
    size_t max_tasks_in_queue = 1000;

    std::atomic<UInt64> max_id = 0;
    const CurrentMetrics::Metric * max_entry_metric;
    const CurrentMetrics::Metric * max_pushed_entry_metric;
};


}
