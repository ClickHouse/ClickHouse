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


class DDLWorker
{
public:
    DDLWorker(const std::string & zk_root_dir, Context & context_);
    ~DDLWorker();

    void enqueueQuery(const String & query, const std::vector<Cluster::Address> & addrs);

    /// Returns root/ path in ZooKeeper
    std::string getRoot() const
    {
        return root_dir;
    }

    std::string getAssignsDir() const
    {
        return root_dir + "/assigns";
    }

    std::string getMastersDir() const
    {
        return root_dir + "/masters";
    }

    std::string getCurrentMasterDir() const
    {
        return getMastersDir() + "/" + getHostName();
    }

    std::string getHostName() const
    {
        return hostname;
    }

private:
    void processTasks();
    bool processTask(const std::string & task);

    void processQueries();
    bool processQuery(const std::string & task);

    void run();

private:
    Context & context;
    Logger * log = &Logger::get("DDLWorker");

    std::string hostname;
    std::string root_dir;       /// common dir with queue of queries
    std::string assign_dir;     /// dir with tasks assigned to the server
    std::string master_dir;    /// dir with queries was initiated by the server

    std::atomic<bool> stop_flag;
    std::condition_variable cond_var;
    std::mutex lock;
    std::thread thread;
};

}
