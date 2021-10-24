#pragma once

#include <thread>
#include <memory>
#include <mutex>
#include <amqpcpp.h>
#include <amqpcpp/linux_tcp.h>
#include <common/types.h>
#include <amqpcpp/libuv.h>

namespace DB
{

namespace Loop
{
    static const UInt8 RUN = 1;
    static const UInt8 STOP = 2;
}

using ChannelPtr = std::unique_ptr<AMQP::TcpChannel>;

class RabbitMQHandler : public AMQP::LibUvHandler
{

public:
    RabbitMQHandler(uv_loop_t * loop_, Poco::Logger * log_);

    void onError(AMQP::TcpConnection * connection, const char * message) override;
    void onReady(AMQP::TcpConnection * connection) override;

    /// Loop for background thread worker.
    void startLoop();

    /// Loop to wait for small tasks in a non-blocking mode.
    /// Adds synchronization with main background loop.
    void iterateLoop();

    /// Loop to wait for small tasks in a blocking mode.
    /// No synchronization is done with the main loop thread.
    void startBlockingLoop();

    void stopLoop();

    bool connectionRunning() const { return connection_running.load(); }
    bool loopRunning() const { return loop_running.load(); }

    void updateLoopState(UInt8 state) { loop_state.store(state); }
    UInt8 getLoopState() { return loop_state.load(); }

private:
    uv_loop_t * loop;
    Poco::Logger * log;

    std::atomic<bool> connection_running, loop_running;
    std::atomic<UInt8> loop_state;
    std::mutex startup_mutex;
};

using RabbitMQHandlerPtr = std::shared_ptr<RabbitMQHandler>;

}
