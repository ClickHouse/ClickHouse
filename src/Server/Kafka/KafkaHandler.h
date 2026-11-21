#pragma once

#include <Server/Kafka/KafkaProtocol.h>
#include <Server/Kafka/KafkaTopics.h>

#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Poco/Net/TCPServerConnection.h>

#include <memory>


namespace CurrentMetrics
{
    extern const Metric KafkaConnection;
}

namespace DB
{

class IServer;
class Session;
class TCPServer;
class ReadBufferFromPocoSocket;
class WriteBufferFromPocoSocket;

/** A server of the Kafka protocol, which makes the MergeTree tables available as Kafka topics,
  * see `KafkaTopics.h`. The requests of a connection are processed one by one, as in Kafka.
  */
class KafkaHandler : public Poco::Net::TCPServerConnection
{
public:
    KafkaHandler(
        const Poco::Net::StreamSocket & socket_,
        IServer & server_,
        TCPServer & tcp_server_,
        const Kafka::BrokerSettings & settings_,
        bool secure_,
        UInt64 connection_id_,
        const ProfileEvents::Event & read_event_,
        const ProfileEvents::Event & write_event_);

    ~KafkaHandler() override;

    void run() override;

private:
    struct RequestHeader
    {
        Kafka::ApiKey api_key{};
        Int16 api_version = 0;
        Int32 correlation_id = 0;
        String client_id;
    };

    /// Returns false if the connection has to be closed.
    bool processRequest(const String & request);
    void processSaslToken(const String & token);

    void authenticate(const String & user, const String & password);
    void authenticateDefaultUserIfNeeded();
    ContextMutablePtr makeQueryContext();
    [[nodiscard]] bool isCancelled() const;

    String advertisedHost();
    Int32 advertisedPort();

    void apiVersions(const RequestHeader & header, Kafka::Reader & in, Kafka::Writer & out);
    void metadata(const RequestHeader & header, Kafka::Reader & in, Kafka::Writer & out);
    bool produce(const RequestHeader & header, Kafka::Reader & in, Kafka::Writer & out);
    void fetch(const RequestHeader & header, Kafka::Reader & in, Kafka::Writer & out);
    void listOffsets(const RequestHeader & header, Kafka::Reader & in, Kafka::Writer & out);
    void findCoordinator(const RequestHeader & header, Kafka::Reader & in, Kafka::Writer & out);
    void joinGroup(const RequestHeader & header, Kafka::Reader & in, Kafka::Writer & out);
    void syncGroup(const RequestHeader & header, Kafka::Reader & in, Kafka::Writer & out);
    void heartbeat(const RequestHeader & header, Kafka::Reader & in, Kafka::Writer & out);
    void leaveGroup(const RequestHeader & header, Kafka::Reader & in, Kafka::Writer & out);
    void offsetCommit(const RequestHeader & header, Kafka::Reader & in, Kafka::Writer & out);
    void offsetFetch(const RequestHeader & header, Kafka::Reader & in, Kafka::Writer & out);
    void listGroups(const RequestHeader & header, Kafka::Reader & in, Kafka::Writer & out);
    void describeGroups(const RequestHeader & header, Kafka::Reader & in, Kafka::Writer & out);
    void initProducerId(const RequestHeader & header, Kafka::Reader & in, Kafka::Writer & out);
    void createTopics(const RequestHeader & header, Kafka::Reader & in, Kafka::Writer & out);
    void deleteTopics(const RequestHeader & header, Kafka::Reader & in, Kafka::Writer & out);
    void saslHandshake(const RequestHeader & header, Kafka::Reader & in, Kafka::Writer & out);
    bool saslAuthenticate(const RequestHeader & header, Kafka::Reader & in, Kafka::Writer & out);

    IServer & server;
    TCPServer & tcp_server;
    const Kafka::BrokerSettings settings;
    /// The connection is to `kafka_port_secure`, over TLS.
    const bool secure;
    const UInt64 connection_id;
    LoggerPtr log = getLogger("KafkaHandler");

    std::unique_ptr<Session> session;
    bool authenticated = false;
    /// After a SaslHandshake of version 0, the SASL token is sent without a Kafka request header.
    bool expect_raw_sasl_token = false;
    bool sasl_handshake_done = false;

    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBufferFromPocoSocket> out;

    ProfileEvents::Event read_event;
    ProfileEvents::Event write_event;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::KafkaConnection};
};

}
