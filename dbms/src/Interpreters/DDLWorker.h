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

class ASTQueryWithOnCluster;
BlockIO executeDDLQueryOnCluster(const ASTPtr & query_ptr, Context & context);


struct DDLLogEntry;


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
    bool processTask(const DDLLogEntry & node, const std::string & node_path);

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

    std::string root_dir;       /// common dir with queue of queries
    std::string master_dir;     /// dir with queries was initiated by the server

    std::string last_processed_node_name;

    std::shared_ptr<Poco::Event> event_queue_updated;

    std::atomic<bool> stop_flag;
    std::condition_variable cond_var;
    std::mutex lock;
    std::thread thread;

    size_t last_cleanup_time_seconds = 0;
    static constexpr size_t node_max_lifetime_seconds = 60; // 7 * 24 * 60 * 60;
    static constexpr size_t cleanup_after_seconds = 60;

    friend class DDLQueryStatusInputSream;
};

}
