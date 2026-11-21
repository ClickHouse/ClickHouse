#include <Storages/Pulsar/Client/Producer.h>

#if USE_PULSAR

#include <Storages/Pulsar/Client/Connection.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>

#include <fmt/format.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_CONNECT_PULSAR;
}

namespace Pulsar
{

class ProducerImpl final : public Session::Owner
{
public:
    ProducerImpl(ClientStatePtr client_, String topic_, ProducerConfiguration configuration_);
    ~ProducerImpl();

    /// Within the operation timeout.
    void create();

    void send(std::string_view payload, std::optional<std::string_view> ordering_key);
    void interrupt() noexcept { interrupter.interrupt(); }
    void close() noexcept;

private:
    enum class State : uint8_t
    {
        NeedLookup,
        LookupPending,
        Connecting,
        ProducerPending,
        Ready,
    };

    struct PendingMessage
    {
        std::string_view payload;
        std::optional<std::string_view> ordering_key;
        UInt64 sequence_id = 0;
        UInt64 publish_time = 0;
        /// The connection the message was sent on.
        UInt64 sent_on = 0;
        bool receipt = false;
        std::optional<String> error;
    };

    void onFrame(Connection & connection, Frame & frame, const SharedBuffer & buffer) override;
    void onConnectionClosed(Connection & connection) override;
    void advance();
    void reset(const char * reason);
    void checkUsable() const;

    ClientStatePtr client;
    /// Outlives the connections of the session, which are registered in it.
    Interrupter interrupter;
    Session session;
    const String base_topic;
    String topic;
    const ProducerConfiguration configuration;
    LoggerPtr log;

    State state = State::NeedLookup;
    Session::RequestPtr lookup;
    UInt64 connection_id = 0;
    /// There is one producer per session.
    static constexpr UInt64 PRODUCER_ID = 1;
    UInt64 producer_request_id = 0;
    TimePoint step_started;
    /// Assigned by the broker when the producer is created first, and used when it is created again, so that the
    /// broker can deduplicate a message sent again by its producer name and sequence id.
    std::optional<String> producer_name;
    UInt64 epoch = 0;
    std::optional<UInt64> next_sequence_id;
    std::optional<PendingMessage> pending;

    bool failed = false;
    bool closed = false;
};


ProducerImpl::ProducerImpl(ClientStatePtr client_, String topic_, ProducerConfiguration configuration_)
    : client(std::move(client_))
    , session(client, *this, &interrupter)
    , base_topic(std::move(topic_))
    , configuration(configuration_)
    , log(client->log)
{
}

ProducerImpl::~ProducerImpl()
{
    close();
}

void ProducerImpl::checkUsable() const
{
    if (closed)
        throw Exception(ErrorCodes::ABORTED, "The Pulsar producer is closed");
    if (failed)
        throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "The Pulsar producer has failed before and must be created again");
}

void ProducerImpl::create()
{
    const auto deadline = Clock::now() + client->configuration.operation_timeout;
    auto wait_until = [&](auto && done)
    {
        while (true)
        {
            advance();
            session.process();
            advance();
            if (done())
                return;
            if (Clock::now() >= deadline)
                throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "Creating the Pulsar producer for topic {} did not complete within {} ms",
                    base_topic, client->configuration.operation_timeout.count());
            session.wait(deadline);
        }
    };

    auto metadata = session.partitionedMetadata(base_topic);
    wait_until([&] { return metadata->done; });
    if (metadata->error)
        throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "Cannot create the Pulsar producer: {}", *metadata->error);

    /// Like the default routing of the C++ client for the messages without a key.
    topic = metadata->partitions ? partitionTopicName(base_topic, thread_local_rng() % metadata->partitions) : base_topic;
    wait_until([&] { return state == State::Ready; });
}

void ProducerImpl::reset(const char * reason)
{
    if (state == State::Ready || state == State::ProducerPending)
        LOG_INFO(log, "Creating the Pulsar producer for topic {} again: {}", topic, reason);
    state = State::NeedLookup;
    lookup.reset();
    connection_id = 0;
    ++epoch;
    if (pending)
        pending->sent_on = 0;
}

void ProducerImpl::advance()
{
    /// The partition is not chosen yet.
    if (topic.empty())
        return;
    if (state == State::NeedLookup)
    {
        lookup = session.lookup(topic);
        state = State::LookupPending;
    }
    if (state == State::LookupPending)
    {
        if (!lookup->done)
            return;
        if (lookup->error)
            throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "Cannot create the Pulsar producer: {}", *lookup->error);
        connection_id = session.connection(lookup->broker).id();
        state = State::Connecting;
    }
    if (state == State::Connecting)
    {
        Connection * connection = session.findConnection(connection_id);
        if (!connection)
        {
            reset("the connection is lost");
            return;
        }
        if (!connection->isReady())
            return;

        producer_request_id = session.nextRequestId();
        PulsarProto::BaseCommand command;
        command.set_type(PulsarProto::BaseCommand::PRODUCER);
        auto & producer = *command.mutable_producer();
        producer.set_topic(topic);
        producer.set_producer_id(PRODUCER_ID);
        producer.set_request_id(producer_request_id);
        if (producer_name)
            producer.set_producer_name(*producer_name);
        producer.set_user_provided_producer_name(false);
        producer.set_epoch(epoch);
        connection->send(command);
        state = State::ProducerPending;
        step_started = Clock::now();
        return;
    }
    if (state == State::ProducerPending)
    {
        if (Clock::now() - step_started > client->configuration.operation_timeout)
            throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "Creating the Pulsar producer for topic {} did not complete within {} ms",
                topic, client->configuration.operation_timeout.count());
        return;
    }

    /// Ready.
    if (!pending || pending->receipt || pending->sent_on == connection_id)
        return;
    Connection * connection = session.findConnection(connection_id);
    if (!connection)
        return;

    PulsarProto::BaseCommand command;
    command.set_type(PulsarProto::BaseCommand::SEND);
    auto & send_command = *command.mutable_send();
    send_command.set_producer_id(PRODUCER_ID);
    send_command.set_sequence_id(pending->sequence_id);
    send_command.set_num_messages(1);

    PulsarProto::MessageMetadata metadata;
    metadata.set_producer_name(producer_name.value_or(""));
    metadata.set_sequence_id(pending->sequence_id);
    metadata.set_publish_time(pending->publish_time);
    if (pending->ordering_key)
        metadata.set_ordering_key(String(*pending->ordering_key));

    connection->sendFrame(encodeSendFrame(command, metadata, pending->payload));
    pending->sent_on = connection_id;
}

void ProducerImpl::onConnectionClosed(Connection & connection)
{
    if (connection.id() == connection_id)
        reset("the connection is lost");
}

void ProducerImpl::onFrame(Connection & connection, Frame & frame, const SharedBuffer &)
{
    using Type = PulsarProto::BaseCommand;
    const auto & command = frame.command;
    switch (command.type())
    {
        case Type::PRODUCER_SUCCESS:
        {
            if (!command.has_producer_success() || !command.producer_success().has_request_id() || !command.producer_success().has_producer_name())
                throwUnexpected("PRODUCER_SUCCESS");
            const auto & success = command.producer_success();
            if (state != State::ProducerPending || success.request_id() != producer_request_id || connection.id() != connection_id)
                throwUnexpected("PRODUCER_SUCCESS for an unknown request");
            /// The producer is not ready yet if the topic has another exclusive producer.
            if (!success.producer_ready())
                return;
            if (!producer_name)
                producer_name = success.producer_name();
            if (!next_sequence_id)
                next_sequence_id = static_cast<UInt64>(success.last_sequence_id() + 1);
            state = State::Ready;
            LOG_TRACE(log, "Created the Pulsar producer {} for topic {}", *producer_name, topic);
            return;
        }
        case Type::ERROR:
        {
            if (state == State::ProducerPending && command.error().request_id() == producer_request_id && connection.id() == connection_id)
                throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "Cannot create the Pulsar producer for topic {}: {} {}", topic,
                    PulsarProto::ServerError_Name(command.error().error()), command.error().message());
            throwUnexpected("ERROR for an unknown request");
        }
        case Type::SEND_RECEIPT:
        {
            if (!command.has_send_receipt() || !command.send_receipt().has_producer_id() || !command.send_receipt().has_sequence_id())
                throwUnexpected("SEND_RECEIPT");
            const auto & receipt = command.send_receipt();
            if (pending && receipt.producer_id() == PRODUCER_ID && receipt.sequence_id() == pending->sequence_id && connection.id() == connection_id)
                pending->receipt = true;
            return;
        }
        case Type::SEND_ERROR:
        {
            if (!command.has_send_error() || !command.send_error().has_producer_id() || !command.send_error().has_sequence_id())
                throwUnexpected("SEND_ERROR");
            const auto & error = command.send_error();
            if (pending && error.producer_id() == PRODUCER_ID && error.sequence_id() == pending->sequence_id && connection.id() == connection_id)
                pending->error = fmt::format("{} {}", PulsarProto::ServerError_Name(error.error()), error.message());
            return;
        }
        case Type::CLOSE_PRODUCER:
        {
            if (!command.has_close_producer() || !command.close_producer().has_producer_id())
                throwUnexpected("CLOSE_PRODUCER");
            if (command.close_producer().producer_id() == PRODUCER_ID && connection.id() == connection_id)
                reset("closed by the broker");
            return;
        }
        default:
            LOG_TRACE(log, "Ignoring {} from the Pulsar broker", Type::Type_Name(command.type()));
            return;
    }
}

void ProducerImpl::send(std::string_view payload, std::optional<std::string_view> ordering_key)
{
    checkUsable();
    try
    {
        Connection * connection = session.findConnection(connection_id);
        const size_t max_message_size = connection ? connection->maxMessageSize() : client->limits.max_message_size;
        if (payload.size() > max_message_size)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The message of {} bytes is bigger than the maximum message size {} of the Pulsar broker",
                payload.size(), max_message_size);

        const auto deadline = Clock::now() + configuration.send_timeout;
        pending.emplace();
        pending->payload = payload;
        pending->ordering_key = ordering_key;
        pending->sequence_id = (*next_sequence_id)++;
        pending->publish_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        while (true)
        {
            advance();
            session.process();
            advance();
            if (pending->error)
                throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "Cannot send a message to the Pulsar topic {}: {}", topic, *pending->error);
            if (pending->receipt)
                break;
            if (Clock::now() >= deadline)
                throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "The Pulsar broker did not confirm a message to topic {} within {} ms",
                    topic, configuration.send_timeout.count());
            session.wait(deadline);
        }
        pending.reset();
    }
    catch (...)
    {
        /// A partial frame may remain in the output.
        failed = true;
        pending.reset();
        throw;
    }
}

void ProducerImpl::close() noexcept
{
    if (closed)
        return;
    closed = true;
    session.close();
}


Producer::Producer(std::unique_ptr<ProducerImpl> impl_)
    : impl(std::move(impl_))
{
}

Producer::~Producer() = default;

void Producer::send(std::string_view payload, std::optional<std::string_view> ordering_key)
{
    impl->send(payload, ordering_key);
}

void Producer::interrupt() noexcept
{
    impl->interrupt();
}

void Producer::close() noexcept
{
    impl->close();
}

std::unique_ptr<Producer> createProducer(ClientStatePtr client, String topic, const ProducerConfiguration & configuration)
{
    auto impl = std::make_unique<ProducerImpl>(std::move(client), std::move(topic), configuration);
    impl->create();
    return std::make_unique<Producer>(std::move(impl));
}

}

}

#endif
