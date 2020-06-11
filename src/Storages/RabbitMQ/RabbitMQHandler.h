#pragma once

#include <thread>
#include <memory>
#include <mutex>
#include <amqpcpp.h>
#include <amqpcpp/libevent.h>
#include <amqpcpp/linux_tcp.h>
#include <common/types.h>
#include <event2/event.h>

namespace DB
{

class RabbitMQHandler : public AMQP::LibEventHandler
{

public:
    RabbitMQHandler(event_base * evbase_, Poco::Logger * log_);

    void onError(AMQP::TcpConnection * connection, const char * message) override;
    void startConsumerLoop(std::atomic<bool> & loop_started);
    void startProducerLoop();
    void stopWithTimeout();
    void stop();
    std::atomic<bool> & checkStopIsScheduled() { return stop_scheduled; };

private:
    event_base * evbase;
    Poco::Logger * log;

    timeval tv;
    std::atomic<bool> stop_scheduled = false;
    std::timed_mutex mutex_before_event_loop;
    std::mutex mutex_before_loop_stop;
};

}
