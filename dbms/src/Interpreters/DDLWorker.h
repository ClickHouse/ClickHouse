#pragma once

#include <Interpreters/Context.h>
#include <common/logger_useful.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace DB
{

class DDLWorker
{
public:
    DDLWorker(const Poco::Util::AbstractConfiguration & config,
              const std::string & config_name, Context & context_);
    ~DDLWorker();

private:
    void processTasks();
    void processCreate(const std::string & path);

    void run();

private:
    Context & context;
    Logger * log = &Logger::get("DDLWorker");

    std::string host_task_queue_path;

    std::atomic<bool> stop_flag;
    std::condition_variable cond_var;
    std::mutex lock;
    std::thread thread;
};

}
