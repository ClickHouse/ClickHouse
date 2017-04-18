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

BlockIO executeDDLQueryOnCluster(const String & query, const String & cluster_name, Context & context);


struct DDLLogEntry;


class DDLWorker
{
public:
    DDLWorker(const std::string & zk_root_dir, Context & context_);
    ~DDLWorker();

    /// Push query into DDL query, return path to created node
    String enqueueQuery(DDLLogEntry & entry);

    std::string getHostName() const
    {
        return hostname;
    }

private:
    void processTasks();
    bool processTask(const DDLLogEntry & node, const std::string & node_path);

    void createStatusDirs(const std::string & node_name);

    void processQueries();
    bool processQuery(const std::string & task);

    void run();

private:
    Context & context;
    Logger * log = &Logger::get("DDLWorker");

    std::string hostname;
    std::string root_dir;       /// common dir with queue of queries
    std::string master_dir;     /// dir with queries was initiated by the server

    std::string last_processed_node_name;

    std::shared_ptr<zkutil::ZooKeeper> zookeeper;
    std::shared_ptr<Poco::Event> event_queue_updated;

    std::atomic<bool> stop_flag;
    std::condition_variable cond_var;
    std::mutex lock;
    std::thread thread;

    friend class DDLQueryStatusInputSream;
};

}
