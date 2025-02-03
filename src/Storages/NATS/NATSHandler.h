#pragma once

#include <uv.h>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <queue>
#include <nats.h>
#include <base/types.h>
#include <Common/Logger.h>

#include <Storages/NATS/NATSConnection.h>
#include <Storages/UVLoop.h>

namespace DB
{

class NATSHandler
{
    using Task = std::function<void ()>;

public:
    explicit NATSHandler(LoggerPtr log_);

    /// Loop for background thread worker.
    void runLoop();
    void stopLoop();

    std::future<NATSConnectionPtr> createConnection(const NATSConfiguration & configuration);

private:
    /// Execute task on event loop thread
    void post(Task task);

    NATSOptionsPtr createOptions();

    bool isRunning();

    UVLoop loop;
    LoggerPtr log;

    std::mutex loop_state_mutex;
    UInt8 loop_state;

    std::mutex tasks_mutex;
    std::queue<Task> tasks;
};

}
