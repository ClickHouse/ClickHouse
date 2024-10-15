#pragma once

#include <Common/CurrentThread.h>
#include <Common/CurrentMetrics.h>
#include <Common/DNSResolver.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Storages/IStorage_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>

#include <Poco/Event.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <list>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_set>

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

    std::string getQueueDir() const
    {
        return queue_dir;
    }

    void startup();
    virtual void shutdown();

    bool isCurrentlyActive() const { return initialized && !stop_flag; }


    /// Returns cached ZooKeeper session (possibly expired).
    ZooKeeperPtr tryGetZooKeeper() const;
    /// If necessary, creates a new session and caches it.
    ZooKeeperPtr getAndSetZooKeeper();

protected:

    class ConcurrentSet
    {
    public:
        bool contains(const String & key) const
        {
            std::shared_lock lock(mtx);
            return set.contains(key);
        }

        bool insert(const String & key)
        {
            std::unique_lock lock(mtx);
            return set.emplace(key).second;
        }

        bool remove(const String & key)
        {
            std::unique_lock lock(mtx);
            return set.erase(key);
        }

    private:
        std::unordered_set<String> set;
        mutable std::shared_mutex mtx;
    };

    /// Iterates through queue tasks in ZooKeeper, runs execution of new tasks
    void scheduleTasks(bool reinitialized);

    DDLTaskBase & saveTask(DDLTaskPtr && task);

    /// Reads entry and check that the host belongs to host list of the task
    /// Returns non-empty DDLTaskPtr if entry parsed and the check is passed
    /// If dry_run = false, the task will be processed right after this call.
    virtual DDLTaskPtr initAndCheckTask(const String & entry_name, String & out_reason, const ZooKeeperPtr & zookeeper, bool dry_run);

    void processTask(DDLTaskBase & task, const ZooKeeperPtr & zookeeper);
    void updateMaxDDLEntryID(const String & entry_name);

    /// Check that query should be executed on leader replica only
    static bool taskShouldBeExecutedOnLeader(const ASTPtr & ast_ddl, StoragePtr storage);

    /// Executes query only on leader replica in case of replicated table.
    /// Queries like TRUNCATE/ALTER .../OPTIMIZE have to be executed only on one node of shard.
    /// Most of these queries can be executed on non-leader replica, but actually they still send
    /// query via RemoteQueryExecutor to leader, so to avoid such "2-phase" query execution we
    /// execute query directly on leader.
    bool tryExecuteQueryOnLeaderReplica(
        DDLTaskBase & task,
        StoragePtr storage,
        const String & node_path,
        const ZooKeeperPtr & zookeeper,
        std::unique_ptr<zkutil::ZooKeeperLock> & execute_on_leader_lock);

    bool tryExecuteQuery(DDLTaskBase & task, const ZooKeeperPtr & zookeeper);

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
    LoggerPtr log;

    std::optional<std::string> config_host_name; /// host_name from config

    std::string host_fqdn;      /// current host domain name
    std::string host_fqdn_id;   /// host_name:port
    std::string queue_dir;      /// dir with queue of queries

    mutable std::mutex zookeeper_mutex;
    ZooKeeperPtr current_zookeeper TSA_GUARDED_BY(zookeeper_mutex);

    /// Save state of executed task to avoid duplicate execution on ZK error
    std::optional<String> last_skipped_entry_name;
    std::optional<String> first_failed_task_name;
    std::list<DDLTaskPtr> current_tasks;

    /// This flag is needed for debug assertions only
    bool queue_fully_loaded_after_initialization_debug_helper = false;

    Coordination::Stat queue_node_stat;
    std::shared_ptr<Poco::Event> queue_updated_event = std::make_shared<Poco::Event>();
    std::shared_ptr<Poco::Event> cleanup_event = std::make_shared<Poco::Event>();
    std::atomic<bool> initialized = false;
    std::atomic<bool> stop_flag = true;

    std::unique_ptr<ThreadFromGlobalPool> main_thread;
    std::unique_ptr<ThreadFromGlobalPool> cleanup_thread;

    /// Size of the pool for query execution.
    size_t pool_size = 1;
    std::unique_ptr<ThreadPool> worker_pool;

    /// Cleaning starts after new node event is received if the last cleaning wasn't made sooner than N seconds ago
    Int64 cleanup_delay_period = 60; // minute (in seconds)
    /// Delete node if its age is greater than that
    Int64 task_max_lifetime = 7 * 24 * 60 * 60; // week (in seconds)
    /// How many tasks could be in the queue
    size_t max_tasks_in_queue = 1000;

    std::atomic<UInt32> max_id = 0;

    ConcurrentSet entries_to_skip;

    std::atomic_uint64_t subsequent_errors_count = 0;
    String last_unexpected_error;

    const CurrentMetrics::Metric * max_entry_metric;
    const CurrentMetrics::Metric * max_pushed_entry_metric;
};


}
