#include <Storages/Pulsar/Client/Client.h>

#include <cstdlib>
#include <string_view>

#include <Storages/Pulsar/Client/Connection.h>
#include <Common/logger_useful.h>

namespace DB::Pulsar
{

namespace
{

constexpr int MAX_LOOKUP_REDIRECTS = 20;

/// Splits a `host:port` authority (no scheme, no path) into its parts, applying `default_port` when the
/// port is omitted. Returns false if the host is empty or the port is not a number.
bool parseHostPort(const std::string & authority, uint16_t default_port, std::string & host, uint16_t & port)
{
    const auto colon = authority.rfind(':');
    if (colon == std::string::npos)
    {
        host = authority;
        port = default_port;
    }
    else
    {
        host = authority.substr(0, colon);
        const std::string port_string = authority.substr(colon + 1);
        if (port_string.empty())
            return false;
        char * end = nullptr;
        const long value = std::strtol(port_string.c_str(), &end, 10);
        if (*end != '\0' || value <= 0 || value > 65535)
            return false;
        port = static_cast<uint16_t>(value);
    }
    return !host.empty();
}

/// Parses `pulsar://host:port` (a single broker service URL from a lookup response).
bool parseBrokerServiceUrl(const std::string & url, std::string & host, uint16_t & port)
{
    static constexpr std::string_view scheme = "pulsar://";
    if (url.rfind(scheme, 0) != 0)
        return false;
    std::string rest = url.substr(scheme.size());
    if (const auto slash = rest.find('/'); slash != std::string::npos)
        rest = rest.substr(0, slash);
    return parseHostPort(rest, 6650, host, port);
}

}

Client::Client(const std::string & service_url, const ClientConfiguration & config_)
    : config(config_), log(getLogger("PulsarClient"))
{
    /// Parse `<scheme>://host[:port][,host[:port]...][/path]`. The engine validates the scheme and the
    /// hosts before constructing the client, so this only has to split a well-formed URL. TLS is not
    /// implemented: a `pulsar+ssl://` URL is remembered and every operation then fails cleanly.
    std::string rest = service_url;
    uint16_t default_port = 6650;
    if (rest.rfind("pulsar+ssl://", 0) == 0)
    {
        tls_requested = true;
        rest = rest.substr(std::string_view("pulsar+ssl://").size());
        default_port = 6651;
    }
    else if (rest.rfind("pulsar://", 0) == 0)
    {
        rest = rest.substr(std::string_view("pulsar://").size());
    }

    if (const auto slash = rest.find('/'); slash != std::string::npos)
        rest = rest.substr(0, slash);

    size_t start = 0;
    while (start <= rest.size())
    {
        const auto comma = rest.find(',', start);
        const std::string authority = rest.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
        std::string host;
        uint16_t port = default_port;
        if (!authority.empty() && parseHostPort(authority, default_port, host, port))
            service_brokers.push_back(BrokerAddress{host, port});
        if (comma == std::string::npos)
            break;
        start = comma + 1;
    }
}

Result Client::lookup(const std::string & topic, BrokerAddress & broker)
{
    if (tls_requested)
    {
        LOG_ERROR(log, "TLS (pulsar+ssl://) is not supported by this Pulsar client");
        return ResultNotSupported;
    }
    if (service_brokers.empty())
        return ResultConnectError;

    /// Connect to any of the service-URL brokers for the lookup.
    ConnectionPtr connection;
    Result result = ResultConnectError;
    for (const auto & service_broker : service_brokers)
    {
        connection = Connection::open(service_broker.host, service_broker.port, config, log, result);
        if (connection)
            break;
    }
    if (!connection)
        return result;
    registry.add(connection);

    /// Partitioned topics are not supported: a partitioned topic would need a consumer per partition.
    {
        pulsar::proto::BaseCommand command;
        command.set_type(pulsar::proto::BaseCommand::PARTITIONED_METADATA);
        const uint64_t request_id = connection->newRequestId();
        auto * metadata = command.mutable_partitionmetadata();
        metadata->set_topic(topic);
        metadata->set_request_id(request_id);

        pulsar::proto::BaseCommand response;
        if (Result r = connection->request(command, request_id, response); r != ResultOk)
            return r;
        if (response.type() == pulsar::proto::BaseCommand::PARTITIONED_METADATA_RESPONSE
            && response.partitionmetadataresponse().partitions() > 0)
        {
            LOG_ERROR(log, "Partitioned topics are not supported ({} has {} partitions)", topic, response.partitionmetadataresponse().partitions());
            return ResultNotSupported;
        }
    }

    bool authoritative = false;
    for (int redirect = 0; redirect <= MAX_LOOKUP_REDIRECTS; ++redirect)
    {
        pulsar::proto::BaseCommand command;
        command.set_type(pulsar::proto::BaseCommand::LOOKUP);
        const uint64_t request_id = connection->newRequestId();
        auto * lookup_topic = command.mutable_lookuptopic();
        lookup_topic->set_topic(topic);
        lookup_topic->set_request_id(request_id);
        lookup_topic->set_authoritative(authoritative);

        pulsar::proto::BaseCommand response;
        if (Result r = connection->request(command, request_id, response); r != ResultOk)
            return r;
        if (response.type() != pulsar::proto::BaseCommand::LOOKUP_RESPONSE)
        {
            if (response.type() == pulsar::proto::BaseCommand::ERROR)
                LOG_ERROR(log, "Lookup of Pulsar topic {} failed: {}", topic, response.error().message());
            return ResultTopicNotFound;
        }

        const auto & lookup_response = response.lookuptopicresponse();
        const std::string & broker_url = lookup_response.brokerserviceurl();
        std::string host;
        uint16_t port = 6650;
        if (!parseBrokerServiceUrl(broker_url, host, port))
        {
            LOG_ERROR(log, "Pulsar lookup returned an unusable broker URL '{}' for topic {}", broker_url, topic);
            return ResultServiceNotReady;
        }

        switch (lookup_response.response())
        {
            case pulsar::proto::CommandLookupTopicResponse_LookupType_Connect:
                broker = BrokerAddress{host, port};
                return ResultOk;
            case pulsar::proto::CommandLookupTopicResponse_LookupType_Redirect:
            {
                authoritative = lookup_response.authoritative();
                Result r = ResultConnectError;
                ConnectionPtr redirected = Connection::open(host, port, config, log, r);
                if (!redirected)
                    return r;
                registry.add(redirected);
                connection = redirected;
                break;
            }
            default:
                LOG_ERROR(log, "Pulsar lookup of topic {} failed: {}", topic, lookup_response.message());
                return ResultTopicNotFound;
        }
    }

    LOG_ERROR(log, "Too many lookup redirects for Pulsar topic {}", topic);
    return ResultTooManyLookupRedirects;
}

Result Client::createProducer(const std::string & topic, const ProducerConfiguration & producer_config, Producer & producer)
{
    BrokerAddress broker;
    if (Result r = lookup(topic, broker); r != ResultOk)
        return r;

    Result result = ResultConnectError;
    producer = Producer::open(broker.host, broker.port, config, topic, producer_config, registry, log, result);
    return result;
}

Result Client::subscribe(
    const std::vector<std::string> & topics,
    const std::string & subscription,
    const ConsumerConfiguration & consumer_config,
    Consumer & consumer)
{
    std::vector<Consumer::TopicBroker> resolved;
    resolved.reserve(topics.size());
    for (const auto & topic : topics)
    {
        BrokerAddress broker;
        if (Result r = lookup(topic, broker); r != ResultOk)
            return r;
        resolved.push_back(Consumer::TopicBroker{topic, broker.host, broker.port});
    }

    Result result = ResultConnectError;
    consumer = Consumer::open(resolved, subscription, config, consumer_config, registry, log, result);
    return result;
}

void Client::close()
{
    registry.closeAll();
}

}
