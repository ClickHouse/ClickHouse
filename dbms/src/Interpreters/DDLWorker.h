#pragma once
#include <Interpreters/Context.h>
#include <Interpreters/Cluster.h>
#include <DataStreams/BlockIO.h>
#include <common/logger_useful.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace DB
{

struct ASTAlterQuery;
struct DDLLogEntry;


BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr, Context & context);


class DDLWorker
{
public:
    DDLWorker(const std::string & zk_root_dir, Context & context_);
    ~DDLWorker();

    /// Pushes query into DDL queue, returns path to created node
    String enqueueQuery(DDLLogEntry & entry);

    std::string getHostName() const
    {
        return host_id;
    }

private:
    void processTasks();

    void processTask(const DDLLogEntry & node, const std::string & node_path);

    void processTaskAlter(
        const ASTAlterQuery * query_alter,
        const String & rewritten_query,
        const std::shared_ptr<Cluster> & cluster,
        ssize_t shard_num,
        const String & node_path);

    /// Checks and cleanups queue's nodes
    void cleanupQueue(const Strings * node_names_to_check = nullptr);

    void createStatusDirs(const std::string & node_name);
    ASTPtr getRewrittenQuery(const DDLLogEntry & node);

    void run();

private:
    Context & context;
    Logger * log = &Logger::get("DDLWorker");

    std::string host_id;        /// host_name:port
    std::string host_name;
    UInt16 port;

    std::string queue_dir;      /// dir with queue of queries
    std::string master_dir;     /// dir with queries was initiated by the server

    /// Used to omit already processed nodes. Maybe usage of set is more obvious.
    std::string last_processed_node_name;

    std::shared_ptr<zkutil::ZooKeeper> zookeeper;

    /// Save state of executed task to avoid duplicate execution on ZK error
    std::string current_node = {};
    bool current_node_was_executed = false;
    ExecutionStatus current_node_execution_status;

    std::shared_ptr<Poco::Event> event_queue_updated;
    std::atomic<bool> stop_flag{false};
    std::thread thread;

    size_t last_cleanup_time_seconds = 0;

    /// Delete node if its age is greater than that
    static const size_t node_max_lifetime_seconds;
    /// Cleaning starts after new node event is received if the last cleaning wasn't made sooner than N seconds ago
    static const size_t cleanup_min_period_seconds;

    friend class DDLQueryStatusInputSream;
};


}
