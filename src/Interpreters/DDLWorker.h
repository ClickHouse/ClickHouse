#pragma once

#include <DataStreams/BlockIO.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage_fwd.h>
#include <Poco/Net/NetException.h>
#include <Common/CurrentThread.h>
#include <Common/DNSResolver.h>
#include <Common/ThreadPool.h>
#include <Common/isLocalAddress.h>
#include <common/logger_useful.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace zkutil
{
class ZooKeeper;
}

namespace DB
{
class Context;
class ASTAlterQuery;
class AccessRightsElements;

struct HostID
{
    String host_name;
    UInt16 port;

    HostID() = default;

    explicit HostID(const Cluster::Address & address) : host_name(address.host_name), port(address.port) { }

    static HostID fromString(const String & host_port_str)
    {
        HostID res;
        std::tie(res.host_name, res.port) = Cluster::Address::fromString(host_port_str);
        return res;
    }

    String toString() const { return Cluster::Address::toString(host_name, port); }

    String readableString() const { return host_name + ":" + DB::toString(port); }

    bool isLocalAddress(UInt16 clickhouse_port) const
    {
        try
        {
            return DB::isLocalAddress(DNSResolver::instance().resolveAddress(host_name, port), clickhouse_port);
        }
        catch (const Poco::Net::NetException &)
        {
            /// Avoid "Host not found" exceptions
            return false;
        }
    }

    static String applyToString(const HostID & host_id) { return host_id.toString(); }
};

struct DDLLogEntry
{
    String query;
    std::vector<HostID> hosts;
    String initiator; // optional

    static constexpr int CURRENT_VERSION = 1;

public:
    String toString();
    void parse(const String & data);
};

struct DDLTask;
using DDLTaskPtr = std::unique_ptr<DDLTask>;


/// Pushes distributed DDL query to the queue
BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr, const Context & context);
BlockIO executeDDLQueryOnCluster(
    const ASTPtr & query_ptr,
    const Context & context,
    const AccessRightsElements & query_requires_access,
    bool query_requires_grant_option = false);
BlockIO executeDDLQueryOnCluster(
    const ASTPtr & query_ptr,
    const Context & context,
    AccessRightsElements && query_requires_access,
    bool query_requires_grant_option = false);


class DDLWorker
{
public:
    DDLWorker(int pool_size_, const std::string & zk_root_dir, Context & context_, const Poco::Util::AbstractConfiguration * config, const String & prefix);
    ~DDLWorker();

    /// Pushes query into DDL queue, returns path to created node
    String enqueueQuery(DDLLogEntry & entry);

    /// Host ID (name:port) for logging purposes
    /// Note that in each task hosts are identified individually by name:port from initiator server cluster config
    std::string getCommonHostID() const
    {
        return host_fqdn_id;
    }

private:
    using ZooKeeperPtr = std::shared_ptr<zkutil::ZooKeeper>;

    /// Returns cached ZooKeeper session (possibly expired).
    ZooKeeperPtr tryGetZooKeeper() const;
    /// If necessary, creates a new session and caches it.
    ZooKeeperPtr getAndSetZooKeeper();
    /// ZooKeeper recover loop (while not stopped).
    void recoverZooKeeper();

    void checkCurrentTasks();
    void scheduleTasks();
    void saveTask(const String & entry_name);

    /// Reads entry and check that the host belongs to host list of the task
    /// Returns non-empty DDLTaskPtr if entry parsed and the check is passed
    DDLTaskPtr initAndCheckTask(const String & entry_name, String & out_reason, const ZooKeeperPtr & zookeeper);

    void updateMaxDDLEntryID(const DDLTask & task);
    void enqueueTask(DDLTaskPtr task);
    void processTask(DDLTask & task);

    /// Check that query should be executed on leader replica only
    static bool taskShouldBeExecutedOnLeader(const ASTPtr ast_ddl, StoragePtr storage);

    /// Executes query only on leader replica in case of replicated table.
    /// Queries like TRUNCATE/ALTER .../OPTIMIZE have to be executed only on one node of shard.
    /// Most of these queries can be executed on non-leader replica, but actually they still send
    /// query via RemoteBlockOutputStream to leader, so to avoid such "2-phase" query execution we
    /// execute query directly on leader.
    bool tryExecuteQueryOnLeaderReplica(
        DDLTask & task,
        StoragePtr storage,
        const String & rewritten_query,
        const String & node_path,
        const ZooKeeperPtr & zookeeper);

    void parseQueryAndResolveHost(DDLTask & task);

    bool tryExecuteQuery(const String & query, const DDLTask & task, ExecutionStatus & status);

    /// Checks and cleanups queue's nodes
    void cleanupQueue(Int64 current_time_seconds, const ZooKeeperPtr & zookeeper);

    /// Init task node
    static void createStatusDirs(const std::string & node_path, const ZooKeeperPtr & zookeeper);


    void runMainThread();
    void runCleanupThread();

private:
    Context context;
    Poco::Logger * log;

    std::string host_fqdn;      /// current host domain name
    std::string host_fqdn_id;   /// host_name:port
    std::string queue_dir;      /// dir with queue of queries

    mutable std::mutex zookeeper_mutex;
    ZooKeeperPtr current_zookeeper;

    /// Save state of executed task to avoid duplicate execution on ZK error
    std::vector<std::string> last_tasks;

    std::shared_ptr<Poco::Event> queue_updated_event = std::make_shared<Poco::Event>();
    std::shared_ptr<Poco::Event> cleanup_event = std::make_shared<Poco::Event>();
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

    friend class DDLQueryStatusInputStream;
    friend struct DDLTask;
};


}
