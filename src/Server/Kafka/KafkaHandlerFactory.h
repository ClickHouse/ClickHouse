#pragma once

#include <Server/IServer.h>
#include <Server/Kafka/KafkaHandler.h>
#include <Server/Kafka/KafkaTopics.h>
#include <Server/TCPServerConnectionFactory.h>

#include <atomic>


namespace DB
{

class KafkaHandlerFactory : public TCPServerConnectionFactory
{
public:
    KafkaHandlerFactory(
        IServer & server_,
        const Kafka::BrokerSettings & settings_,
        bool secure_,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end())
        : server(server_), settings(settings_), secure(secure_), read_event(read_event_), write_event(write_event_)
    {
    }

    Poco::Net::TCPServerConnection * createConnectionImpl(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override
    {
        UInt64 connection_id = ++last_connection_id;
        LOG_TRACE(log, "Kafka connection. Id: {}. Address: {}. Secure: {}", connection_id, socket.peerAddress().toString(), secure);
        return new KafkaHandler(socket, server, tcp_server, settings, secure, connection_id, read_event, write_event);
    }

private:
    IServer & server;
    const Kafka::BrokerSettings settings;
    const bool secure;
    ProfileEvents::Event read_event;
    ProfileEvents::Event write_event;
    LoggerPtr log = getLogger("KafkaHandlerFactory");
    std::atomic<UInt64> last_connection_id = 0;
};

}
