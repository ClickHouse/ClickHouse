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

class RabbitMQHandler : public AMQP::LibUvHandler
{

public:
    RabbitMQHandler(uv_loop_t * loop_, Poco::Logger * log_);
    void onError(AMQP::TcpConnection * connection, const char * message) override;

    void stop() { stop_loop.store(true); }
    void startLoop();
    void iterateLoop();

private:
    uv_loop_t * loop;
    Poco::Logger * log;

    std::atomic<bool> stop_loop = false;
    std::mutex startup_mutex;
};

}
