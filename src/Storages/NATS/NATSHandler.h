#pragma once

#include <uv.h>
#include <memory>
#include <mutex>
#include <thread>
#include <queue>
#include <nats.h>
#include <base/types.h>
#include <Common/Logger.h>

#include <Storages/UVLoop.h>

namespace DB
{

namespace Loop
{
    static const UInt8 RUN = 1;
    static const UInt8 STOP = 2;
    static const UInt8 CLOSED = 3;
}

using NATSOptionsPtr = std::unique_ptr<natsOptions, decltype(&natsOptions_Destroy)>;
using LockPtr = std::unique_ptr<std::lock_guard<std::mutex>>;

class NATSHandler
{
    using Task = std::function<void ()>;

public:
    NATSHandler(LoggerPtr log_);

    /// Loop for background thread worker.
    void runLoop();
    void stopLoop();

    /// Execute task on event loop thread
    void post(Task task);

    UInt8 getLoopState() { return loop_state.load(); }

    NATSOptionsPtr createOptions();

private:
    UVLoop loop;
    LoggerPtr log;

    std::atomic<UInt8> loop_state;

    std::mutex tasks_mutex;
    std::queue<Task> tasks;
};

}
