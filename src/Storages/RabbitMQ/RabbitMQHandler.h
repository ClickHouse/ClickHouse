#pragma once

#include <thread>
#include <memory>
#include <mutex>
#include <amqpcpp.h>
#include <amqpcpp/linux_tcp.h>
#include <base/types.h>
#include <amqpcpp/libuv.h>
#include <Poco/Logger.h>

namespace DB
{

namespace Loop
{
    static const UInt8 RUN = 1;
    static const UInt8 STOP = 2;
}

/// Default timeout for any blocking AMQP wait that relies on broker callbacks to stop the loop
/// (table setup, exchange unbind, queue cleanup, producer channel close). Bounds how long
/// DROP TABLE / INSERT shutdown / server shutdown can block when the broker connection is dead.
static const uint64_t BLOCKING_LOOP_TIMEOUT_MS = 30000;

using ChannelPtr = std::unique_ptr<AMQP::TcpChannel>;

class RabbitMQHandler : public AMQP::LibUvHandler
{

public:
    RabbitMQHandler(uv_loop_t * loop_, LoggerPtr log_);

    void onError(AMQP::TcpConnection * connection, const char * message) override;
    void onReady(AMQP::TcpConnection * connection) override;

    /// Loop for background thread worker.
    void startLoop();

    /// Loop to wait for small tasks in a non-blocking mode.
    /// Adds synchronization with main background loop.
    int iterateLoop();

    /// Loop to wait for small tasks in a blocking mode.
    /// No synchronization is done with the main loop thread.
    int startBlockingLoop();

    /// Like startBlockingLoop() but stops automatically after timeout_ms milliseconds.
    /// Returns true if the loop exited naturally (callbacks fired), false if it timed out.
    bool startBlockingLoopWithTimeout(uint64_t timeout_ms);

    void stopLoop();
    void stopBlockingLoop();

    bool connectionRunning() const { return connection_running.load(); }
    bool loopRunning() const { return loop_running.load(); }

    void updateLoopState(UInt8 state) { loop_state.store(state); }
    UInt8 getLoopState() { return loop_state.load(); }

private:
    uv_loop_t * loop;
    LoggerPtr log;

    std::atomic<bool> connection_running, loop_running;
    std::atomic<UInt8> loop_state;
    std::mutex startup_mutex;
};

using RabbitMQHandlerPtr = std::shared_ptr<RabbitMQHandler>;

}
