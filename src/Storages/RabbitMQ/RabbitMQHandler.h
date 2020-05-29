#pragma once

#include <memory>
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
    void start(std::atomic<bool> & check_param); 
    void stop();

private:
    event_base * evbase;
    Poco::Logger * log;

    std::mutex mutex;
};

}
