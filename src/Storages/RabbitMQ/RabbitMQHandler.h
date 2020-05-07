#pragma once

#include <memory>
#include <amqpcpp.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <amqpcpp/libevent.h>
#include <amqpcpp/linux_tcp.h>
#include <Poco/Net/StreamSocket.h>
#include <common/types.h>
#include <event2/event.h>


namespace DB
{

class RabbitMQHandler : public AMQP::LibEventHandler
{
public:
    RabbitMQHandler(event_base * evbase_, Poco::Logger * log_);

    void onError(AMQP::TcpConnection * connection, const char * message) override;

    void start();  /// this loop waits for active events and is stopped only after stop() method
    bool startNonBlock(); /// this loop will not wait for events to become active and quits if there are no such events
    void stop();
    void free();

private:
    event_base * evbase;

    bool connection_error = false;

    Poco::Logger * log;
    String user_name;
    String password;
};

}
