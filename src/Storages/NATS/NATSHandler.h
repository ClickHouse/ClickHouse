#pragma once

#include <uv.h>
#include <memory>
#include <mutex>
#include <thread>
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
}

using NATSOptionsPtr = std::unique_ptr<natsOptions, decltype(&natsOptions_Destroy)>;
using LockPtr = std::unique_ptr<std::lock_guard<std::mutex>>;

class NATSHandler
{
public:
    NATSHandler(LoggerPtr log_);

    ~NATSHandler();

    /// Loop for background thread worker.
    void startLoop();

    /// Loop to wait for small tasks in a non-blocking mode.
    /// Adds synchronization with main background loop.
    void iterateLoop();

    LockPtr setThreadLocalLoop();

    void stopLoop();
    bool loopRunning() const { return loop_running.load(); }

    void updateLoopState(UInt8 state) { loop_state.store(state); }
    UInt8 getLoopState() { return loop_state.load(); }

    NATSOptionsPtr createOptions();

private:
    UVLoop loop;
    LoggerPtr log;

    std::atomic<bool> loop_running;
    std::atomic<UInt8> loop_state;
    std::mutex startup_mutex;
};

}
