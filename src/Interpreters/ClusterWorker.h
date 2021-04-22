#pragma once

#include <DataStreams/BlockIO.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/DDLTask.h>
#include <Storages/IStorage.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadPool.h>
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
class ASTClusterQuery;
class AccessRightsElements;
struct ClusterEntry;
struct NodeInfo;
using NodeInfoPtr = std::shared_ptr<NodeInfo>;
using HostMap = std::map<String, NodeInfoPtr>;
using HostVec = std::vector<NodeInfoPtr>;
using ZooKeeperPtr = std::shared_ptr<zkutil::ZooKeeper>;

/// Pushes distributed DDL query to the queue
BlockIO executeClusterQuery(const ASTPtr & query_ptr, ContextPtr context);

BlockIO executeClusterQuery(
    const ASTPtr & query_ptr,
    ContextPtr context,
    const AccessRightsElements & query_requires_access,
    bool query_requires_grant_option = false);
BlockIO executeClusterQuery(
    const ASTPtr & query_ptr,
    ContextPtr context,
    AccessRightsElements && query_requires_access,
    bool query_requires_grant_option = false);

using HostIDPtr = std::shared_ptr<HostID>;

class ClusterWorker
{
public:
    ClusterWorker(
        const std::string & zk_root_dir, ContextPtr context_, const Poco::Util::AbstractConfiguration * config, const String & prefix);
    ~ClusterWorker();

    /// Pushes query into cluster queue, returns path to created node
    String enqueueQuery(ClusterEntry & entry);

    void getHostsFromClusters(HostMap & host_map, String cluster);

    void processCluster();

    /// Host ID (name:port) for logging purposes
    /// Note that in each task hosts are identified individually by name:port from initiator server cluster config
    std::string getCommonHostID() const { return local_host_id.toString(); }

private:
    /// Returns cached ZooKeeper session (possibly expired).
    ZooKeeperPtr tryGetZooKeeper() const;
    /// If necessary, creates a new session and caches it.
    ZooKeeperPtr getAndSetZooKeeper();

    /// Register local config node info to zookeeper
    void registerNodes(const ZooKeeperPtr & zookeeper);

    /// Update local server status from zookeeper
    bool updateLocalNodes(Strings & cluster_nodes, const ZooKeeperPtr & zookeeper);

    /// Update queue status
    void updateQueueStatus(const ZooKeeperPtr & zookeeper);

    /// Clean queue list
    void cleanupQueue(const ZooKeeperPtr & zookeeper);

    /// Alter node status
    String alterNodes(ClusterEntry & entry, const ZooKeeperPtr & zookeeper);

    static void parseQuery(ClusterEntry & entry, NodeInfo & node);

    void runMainThread();

    void attachToThreadGroup();

private:
    ContextPtr context;
    Poco::Logger * log;
    std::unique_ptr<Context> current_context;

    HostID local_host_id; /// current host domain name

    std::string cluster_dir; /// dir with root of cluster
    std::string node_dir; /// dir with node status
    std::string queue_dir; ///dir with queue of latest query
    String current_entry_name; /// current entry name

    /// Name of last task that was skipped or successfully executed
    std::string last_processed_task_name;

    Int64 task_max_lifetime = 10 * 60; // 10 minutes (in seconds)

    mutable std::mutex zookeeper_mutex;
    mutable std::mutex sync_mutex;
    ZooKeeperPtr current_zookeeper;

    std::shared_ptr<Poco::Event> cluster_updated_event = std::make_shared<Poco::Event>();
    std::atomic<bool> stop_flag{false};

    ThreadFromGlobalPool main_thread;

    ThreadGroupStatusPtr thread_group;

    friend class ClusterQueryStatusInputStream;
};

}
