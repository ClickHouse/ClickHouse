#include <Storages/Pulsar/Client/Producer.h>

#include <atomic>
#include <chrono>

#include <Storages/Pulsar/Client/Connection.h>
#include <Storages/Pulsar/Client/ConnectionRegistry.h>
#include <Common/logger_useful.h>

namespace DB::Pulsar
{

namespace
{
uint64_t publishTimeMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}
}

/// Owns the connection to the broker that hosts the producer's topic and the broker-assigned identity.
class ProducerImpl
{
public:
    ProducerImpl(ConnectionPtr connection_, uint64_t producer_id_, std::string producer_name_, std::string topic_, int send_timeout_ms_, LoggerPtr log_)
        : connection(std::move(connection_))
        , producer_id(producer_id_)
        , producer_name(std::move(producer_name_))
        , topic(std::move(topic_))
        , log(std::move(log_))
        , send_timeout_ms(send_timeout_ms_)
    {
    }

    Result send(const Message & message)
    {
        if (closed || connection->isClosed())
            return ResultProducerClosed;

        const uint64_t sequence_id = next_sequence_id++;

        pulsar::proto::MessageMetadata metadata;
        metadata.set_producer_name(producer_name);
        metadata.set_sequence_id(sequence_id);
        metadata.set_publish_time(publishTimeMs());
        metadata.set_uncompressed_size(static_cast<uint32_t>(message.getLength()));
        if (!message.getPartitionKey().empty())
            metadata.set_partition_key(message.getPartitionKey());
        if (!message.getOrderingKey().empty())
            metadata.set_ordering_key(message.getOrderingKey());

        pulsar::proto::BaseCommand command;
        command.set_type(pulsar::proto::BaseCommand::SEND);
        auto * send = command.mutable_send();
        send->set_producer_id(producer_id);
        send->set_sequence_id(sequence_id);
        send->set_num_messages(1);

        if (Result r = connection->sendMessage(command, metadata.SerializeAsString(), message.getDataAsString()); r != ResultOk)
            return r;

        /// Wait for the receipt (or error) for this sequence id.
        Connection::Incoming incoming;
        while (true)
        {
            if (Result r = connection->receive(incoming, send_timeout_ms); r != ResultOk)
                return r;

            const auto type = incoming.command.type();
            if (type == pulsar::proto::BaseCommand::SEND_RECEIPT
                && incoming.command.send_receipt().producer_id() == producer_id
                && incoming.command.send_receipt().sequence_id() == sequence_id)
                return ResultOk;

            if (type == pulsar::proto::BaseCommand::SEND_ERROR
                && incoming.command.send_error().producer_id() == producer_id
                && incoming.command.send_error().sequence_id() == sequence_id)
            {
                LOG_ERROR(log, "Pulsar broker rejected a message for topic {}: {}", topic, incoming.command.send_error().message());
                return ResultBrokerError;
            }

            if (type == pulsar::proto::BaseCommand::CLOSE_PRODUCER)
                return ResultProducerClosed;
            /// Ignore anything else (e.g. a late receipt for another sequence id) and keep waiting.
        }
    }

    Result close()
    {
        if (closed)
            return ResultOk;
        closed = true;

        if (!connection->isClosed())
        {
            pulsar::proto::BaseCommand command;
            command.set_type(pulsar::proto::BaseCommand::CLOSE_PRODUCER);
            const uint64_t request_id = connection->newRequestId();
            auto * close_producer = command.mutable_close_producer();
            close_producer->set_producer_id(producer_id);
            close_producer->set_request_id(request_id);

            pulsar::proto::BaseCommand response;
            connection->request(command, request_id, response); /// best effort
        }
        connection->close();
        return ResultOk;
    }

private:
    ConnectionPtr connection;
    uint64_t producer_id;
    std::string producer_name;
    std::string topic;
    LoggerPtr log;
    std::atomic<uint64_t> next_sequence_id{0};
    int send_timeout_ms = 30000;
    bool closed = false;
};

Producer::Producer() = default;
Producer::~Producer() = default;
Producer::Producer(const Producer &) = default;
Producer & Producer::operator=(const Producer &) = default;
Producer::Producer(std::shared_ptr<ProducerImpl> impl_) : impl(std::move(impl_)) { }

Producer Producer::open(
    const std::string & host,
    uint16_t port,
    const ClientConfiguration & client_config,
    const std::string & topic,
    const ProducerConfiguration & producer_config,
    ConnectionRegistry & registry,
    const LoggerPtr & log,
    Result & result)
{
    ConnectionPtr connection = Connection::open(host, port, client_config, log, result);
    if (!connection)
        return Producer();
    registry.add(connection);

    /// One producer per connection, so a fixed id is unambiguous.
    const uint64_t producer_id = 0;
    const uint64_t request_id = connection->newRequestId();

    pulsar::proto::BaseCommand command;
    command.set_type(pulsar::proto::BaseCommand::PRODUCER);
    auto * producer = command.mutable_producer();
    producer->set_topic(topic);
    producer->set_producer_id(producer_id);
    producer->set_request_id(request_id);

    pulsar::proto::BaseCommand response;
    if (result = connection->request(command, request_id, response); result != ResultOk)
        return Producer();

    if (response.type() != pulsar::proto::BaseCommand::PRODUCER_SUCCESS)
    {
        if (response.type() == pulsar::proto::BaseCommand::ERROR)
            LOG_ERROR(log, "Cannot create a Pulsar producer for topic {}: {}", topic, response.error().message());
        result = ResultBrokerError;
        return Producer();
    }

    result = ResultOk;
    return Producer(std::make_shared<ProducerImpl>(
        connection, producer_id, response.producer_success().producer_name(), topic, producer_config.send_timeout_ms, log));
}

Result Producer::send(const Message & message)
{
    return impl ? impl->send(message) : ResultProducerClosed;
}

Result Producer::flush()
{
    return ResultOk;
}

Result Producer::close()
{
    return impl ? impl->close() : ResultOk;
}

}
