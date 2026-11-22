#include <Storages/Pulsar/Client/Consumer.h>

#if USE_PULSAR

#include <Storages/Pulsar/Client/Connection.h>
#include <Storages/Pulsar/Client/ConsumerImpl.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>

#include <fmt/format.h>

#include <chrono>


namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_CONNECT_PULSAR;
    extern const int LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

namespace Pulsar
{

namespace
{

/// The defaults of the Pulsar C++ client for the total prefetch across the partitions of a consumer.
constexpr size_t MAX_TOTAL_RECEIVER_QUEUE_SIZE = 50'000;

/// A `MessageIdData` with two 64-bit varints and its tag and length.
constexpr size_t MAX_ENCODED_MESSAGE_ID_SIZE = 24;

UInt64 wallClockMilliseconds()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

/// The broker closes the connection on a frame bigger than its maximum message size and a padding.
size_t messageIdsPerCommand(const Connection & connection)
{
    return std::max<size_t>(1, connection.maxMessageSize() / MAX_ENCODED_MESSAGE_ID_SIZE);
}

}


ConsumerImpl::SubConsumer::SubConsumer(String topic_, size_t index_, size_t window_)
    : topic(std::make_shared<const String>(std::move(topic_)))
    , index(index_)
    , window(window_)
    , chunks(wallClockMilliseconds)
{
}

size_t ConsumerImpl::BatchKeyHash::operator()(const BatchKey & key) const
{
    return std::hash<UInt64>()(key.sub_consumer * 0x9E3779B97F4A7C15ULL ^ key.entry.ledger_id * 0xC2B2AE3D27D4EB4FULL ^ key.entry.entry_id);
}

ConsumerImpl::ConsumerImpl(ClientStatePtr client_, VectorWithMemoryTracking<String> topics_, String subscription_, ConsumerConfiguration configuration_)
    : client(std::move(client_))
    , session(client, *this, nullptr)
    , topics(std::move(topics_))
    , subscription(std::move(subscription_))
    , configuration(configuration_)
    , serial(client->next_serial++)
    , consumer_name(fmt::format("{:016x}", static_cast<UInt64>(thread_local_rng())))
    , log(client->log)
{
}

ConsumerImpl::~ConsumerImpl()
{
    close();
}

size_t ConsumerImpl::windowFor(size_t total_sub_consumers) const
{
    return std::max<size_t>(1, std::min(configuration.receiver_queue_size, MAX_TOTAL_RECEIVER_QUEUE_SIZE / total_sub_consumers));
}

void ConsumerImpl::addSubConsumer(String topic, size_t window)
{
    sub_consumers.push_back(std::make_unique<SubConsumer>(std::move(topic), sub_consumers.size(), window));
}

void ConsumerImpl::subscribe()
{
    const auto deadline = Clock::now() + client->configuration.operation_timeout;
    auto wait_until = [&](auto && done, const char * what)
    {
        while (true)
        {
            advance();
            session.process();
            advance();
            if (done())
                return;
            if (Clock::now() >= deadline)
                throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "{} on the Pulsar topics did not complete within {} ms",
                    what, client->configuration.operation_timeout.count());
            session.wait(deadline);
        }
    };

    VectorWithMemoryTracking<Session::RequestPtr> requests;
    for (const auto & topic : topics)
        requests.push_back(session.partitionedMetadata(topic));
    wait_until([&] { return std::all_of(requests.begin(), requests.end(), [](const auto & request) { return request->done; }); },
        "Getting the partitions");

    size_t total = 0;
    for (const auto & request : requests)
    {
        if (request->error)
            throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "Cannot subscribe to the Pulsar topic {}: {}", request->topic, *request->error);
        total += std::max<size_t>(1, request->partitions);
    }
    if (total > client->limits.max_sub_consumers)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "The Pulsar topics have {} partitions in total, which is more than {}",
            total, client->limits.max_sub_consumers);

    const size_t window = windowFor(total);
    for (const auto & request : requests)
    {
        if (request->partitions == 0)
        {
            addSubConsumer(request->topic, window);
            continue;
        }
        partitioned_topics.push_back({request->topic, request->partitions});
        for (size_t i = 0; i < request->partitions; ++i)
            addSubConsumer(partitionTopicName(request->topic, i), window);
    }

    wait_until([&] { return std::all_of(sub_consumers.begin(), sub_consumers.end(), [](const auto & sub) { return sub->state == State::Ready; }); },
        "Subscribing");
    /// Sends the permits.
    session.process();
    next_refresh = Clock::now() + configuration.partitions_refresh_interval;
}

void ConsumerImpl::checkUsable() const
{
    session.checkInterrupted();
    if (closed)
        throw Exception(ErrorCodes::ABORTED, "The Pulsar consumer is closed");
    if (failed)
        throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "The Pulsar consumer has failed before and must be created again");
}

void ConsumerImpl::resetSubConsumer(SubConsumer & sub, const char * reason)
{
    if (sub.state == State::Ready || sub.state == State::SubscribePending)
        LOG_INFO(log, "Subscribing to the Pulsar topic {} again: {}", *sub.topic, reason);
    by_consumer_id.erase(sub.consumer_id);
    if (sub.queued)
    {
        /// The broker redelivers them.
        std::erase_if(queue, [&](const QueuedEntry & queued) { return queued.consumer_id == sub.consumer_id; });
        queued_messages -= sub.queued;
        sub.queued = 0;
    }
    sub.state = State::NeedLookup;
    sub.lookup.reset();
    sub.connection_id = 0;
    sub.consumer_id = 0;
    sub.granted = 0;
    sub.received_messages = 0;
    sub.received_entries = 0;
    sub.pending_flow = 0;
    sub.chunks = ChunkAssembler(wallClockMilliseconds);
}

void ConsumerImpl::advance()
{
    const auto now = Clock::now();
    for (auto & sub_ptr : sub_consumers)
    {
        SubConsumer & sub = *sub_ptr;
        if (sub.state == State::NeedLookup)
        {
            sub.lookup = session.lookup(*sub.topic);
            sub.state = State::LookupPending;
        }
        if (sub.state == State::LookupPending)
        {
            if (!sub.lookup->done)
                continue;
            if (sub.lookup->error)
                throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "Cannot subscribe to the Pulsar topic {}: {}", *sub.topic, *sub.lookup->error);
            sub.connection_id = session.connection(sub.lookup->broker).id();
            sub.state = State::Connecting;
        }
        if (sub.state == State::Connecting)
        {
            Connection * connection = session.findConnection(sub.connection_id);
            if (!connection)
            {
                resetSubConsumer(sub, "the connection is lost");
                continue;
            }
            if (!connection->isReady())
                continue;

            sub.consumer_id = next_consumer_id++;
            sub.subscribe_request_id = session.nextRequestId();
            by_consumer_id[sub.consumer_id] = &sub;

            PulsarProto::BaseCommand command;
            command.set_type(PulsarProto::BaseCommand::SUBSCRIBE);
            auto & subscribe = *command.mutable_subscribe();
            subscribe.set_topic(*sub.topic);
            subscribe.set_subscription(subscription);
            subscribe.set_subtype(PulsarProto::CommandSubscribe::Shared);
            subscribe.set_consumer_id(sub.consumer_id);
            subscribe.set_request_id(sub.subscribe_request_id);
            subscribe.set_consumer_name(consumer_name);
            subscribe.set_durable(true);
            subscribe.set_initialposition(configuration.initial_position == InitialPosition::Earliest
                ? PulsarProto::CommandSubscribe::Earliest : PulsarProto::CommandSubscribe::Latest);
            subscribe.set_force_topic_creation(true);
            connection->send(command);
            sub.state = State::SubscribePending;
            sub.step_started = now;
            continue;
        }
        if (sub.state == State::SubscribePending)
        {
            if (now - sub.step_started > client->configuration.operation_timeout)
                throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "Subscribing to the Pulsar topic {} did not complete within {} ms",
                    *sub.topic, client->configuration.operation_timeout.count());
            continue;
        }

        /// Ready.
        ChunkAssembler::Result expired;
        sub.chunks.expire(expired);
        if (!expired.ack.empty())
            sendAck(sub, expired.ack, {});
        if (!sub.pending_acks.empty())
        {
            sendAck(sub, sub.pending_acks, {});
            sub.pending_acks.clear();
        }
        if (sub.pending_flow >= std::max<size_t>(1, sub.window / 2))
            sendFlow(sub);
    }

    refreshPartitions(now);
}

void ConsumerImpl::refreshPartitions(TimePoint now)
{
    if (partitioned_topics.empty())
        return;

    if (refresh_requests.empty())
    {
        if (now < next_refresh)
            return;
        for (const auto & topic : partitioned_topics)
            refresh_requests.push_back(session.partitionedMetadata(topic.name));
        return;
    }

    if (!std::all_of(refresh_requests.begin(), refresh_requests.end(), [](const auto & request) { return request->done; }))
        return;

    auto requests = std::move(refresh_requests);
    refresh_requests.clear();
    next_refresh = now + configuration.partitions_refresh_interval;

    for (const auto & request : requests)
    {
        if (request->error)
        {
            LOG_WARNING(log, "Cannot refresh the partitions of the Pulsar topic {}: {}", request->topic, *request->error);
            return;
        }
    }

    /// Pulsar cannot remove partitions, a smaller number is ignored.
    size_t total = sub_consumers.size();
    for (size_t i = 0; i < requests.size(); ++i)
        if (requests[i]->partitions > partitioned_topics[i].partitions)
            total += requests[i]->partitions - partitioned_topics[i].partitions;
    if (total == sub_consumers.size())
        return;
    if (total > client->limits.max_sub_consumers)
    {
        LOG_WARNING(log, "Cannot refresh the partitions of the Pulsar topics: {} partitions in total is more than {}",
            total, client->limits.max_sub_consumers);
        return;
    }

    const size_t window = windowFor(total);
    for (size_t i = 0; i < requests.size(); ++i)
    {
        auto & topic = partitioned_topics[i];
        for (size_t partition = topic.partitions; partition < requests[i]->partitions; ++partition)
        {
            LOG_INFO(log, "Subscribing to the new partition {} of the Pulsar topic {}", partition, topic.name);
            addSubConsumer(partitionTopicName(topic.name, partition), window);
        }
        topic.partitions = std::max(topic.partitions, requests[i]->partitions);
    }
}

void ConsumerImpl::sendFlow(SubConsumer & sub)
{
    Connection * connection = session.findConnection(sub.connection_id);
    if (!connection || sub.state != State::Ready || sub.pending_flow == 0)
        return;
    UInt32 permits = static_cast<UInt32>(std::min<size_t>(sub.pending_flow, std::numeric_limits<UInt32>::max()));
    PulsarProto::BaseCommand command;
    command.set_type(PulsarProto::BaseCommand::FLOW);
    command.mutable_flow()->set_consumer_id(sub.consumer_id);
    command.mutable_flow()->set_messagepermits(permits);
    connection->send(command);
    sub.granted += permits;
    sub.pending_flow -= permits;
}

void ConsumerImpl::sendAck(SubConsumer & sub, const VectorWithMemoryTracking<EntryId> & ids, std::optional<ValidationError> error)
{
    Connection * connection = session.findConnection(sub.connection_id);
    if (!connection || sub.state != State::Ready)
    {
        /// Sent after subscribing again.
        if (!error)
            sub.pending_acks.insert(sub.pending_acks.end(), ids.begin(), ids.end());
        return;
    }
    const size_t group_size = messageIdsPerCommand(*connection);
    for (size_t begin = 0; begin < ids.size(); begin += group_size)
    {
        PulsarProto::BaseCommand command;
        command.set_type(PulsarProto::BaseCommand::ACK);
        auto & ack = *command.mutable_ack();
        ack.set_consumer_id(sub.consumer_id);
        ack.set_ack_type(PulsarProto::CommandAck::Individual);
        for (size_t i = begin; i < std::min(ids.size(), begin + group_size); ++i)
        {
            auto & message_id = *ack.add_message_id();
            message_id.set_ledgerid(ids[i].ledger_id);
            message_id.set_entryid(ids[i].entry_id);
        }
        if (error)
            ack.set_validation_error(*error);
        connection->send(command);
    }
    if (std::find(acked_connections.begin(), acked_connections.end(), connection->id()) == acked_connections.end())
        acked_connections.push_back(connection->id());
}

void ConsumerImpl::sendRedeliver(SubConsumer & sub, const VectorWithMemoryTracking<EntryId> & ids)
{
    Connection * connection = session.findConnection(sub.connection_id);
    if (!connection || sub.state != State::Ready || ids.empty())
        return;
    const size_t group_size = messageIdsPerCommand(*connection);
    for (size_t begin = 0; begin < ids.size(); begin += group_size)
    {
        PulsarProto::BaseCommand command;
        command.set_type(PulsarProto::BaseCommand::REDELIVER_UNACKNOWLEDGED_MESSAGES);
        auto & redeliver = *command.mutable_redeliverunacknowledgedmessages();
        redeliver.set_consumer_id(sub.consumer_id);
        for (size_t i = begin; i < std::min(ids.size(), begin + group_size); ++i)
        {
            auto & message_id = *redeliver.add_message_ids();
            message_id.set_ledgerid(ids[i].ledger_id);
            message_id.set_entryid(ids[i].entry_id);
        }
        connection->send(command);
    }
}

ConsumerImpl::SubConsumer * ConsumerImpl::findSubConsumer(Connection & connection, UInt64 consumer_id)
{
    auto it = by_consumer_id.find(consumer_id);
    if (it == by_consumer_id.end() || it->second->connection_id != connection.id())
        return nullptr;
    return it->second;
}

void ConsumerImpl::onConnectionClosed(Connection & connection)
{
    for (auto & sub : sub_consumers)
        if (sub->connection_id == connection.id())
            resetSubConsumer(*sub, "the connection is lost");
}

void ConsumerImpl::onFrame(Connection & connection, Frame & frame, const SharedBuffer & buffer)
{
    using Type = PulsarProto::BaseCommand;
    const auto & command = frame.command;
    switch (command.type())
    {
        case Type::MESSAGE:
        {
            if (!command.has_message() || !command.message().has_consumer_id() || !command.message().has_message_id()
                || !command.message().message_id().has_ledgerid() || !command.message().message_id().has_entryid())
                throwUnexpected("MESSAGE");
            SubConsumer * sub = findSubConsumer(connection, command.message().consumer_id());
            if (!sub || sub->state != State::Ready)
                return;
            onMessage(connection, *sub, frame, buffer);
            return;
        }
        case Type::SUCCESS:
        {
            if (!command.has_success() || !command.success().has_request_id())
                throwUnexpected("SUCCESS");
            for (auto & sub : sub_consumers)
            {
                if (sub->state == State::SubscribePending && sub->subscribe_request_id == command.success().request_id()
                    && sub->connection_id == connection.id())
                {
                    sub->state = State::Ready;
                    /// Before the permits, so that the broker applies them before it dispatches the redelivered messages.
                    if (!sub->pending_acks.empty())
                    {
                        auto pending = std::move(sub->pending_acks);
                        sub->pending_acks.clear();
                        sendAck(*sub, pending, {});
                    }
                    sub->pending_flow = sub->window;
                    sendFlow(*sub);
                    LOG_TRACE(log, "Subscribed to the Pulsar topic {} with the consumer id {}", *sub->topic, sub->consumer_id);
                    return;
                }
            }
            if (!session.wasIssued(command.success().request_id()))
                throwUnexpected("SUCCESS for an unknown request");
            LOG_DEBUG(log, "Ignoring SUCCESS for the request {}, which is not awaited", command.success().request_id());
            return;
        }
        case Type::ERROR:
        {
            for (auto & sub : sub_consumers)
                if (sub->state == State::SubscribePending && sub->subscribe_request_id == command.error().request_id()
                    && sub->connection_id == connection.id())
                    throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "Cannot subscribe to the Pulsar topic {}: {} {}", *sub->topic,
                        PulsarProto::ServerError_Name(command.error().error()), command.error().message());
            if (!session.wasIssued(command.error().request_id()))
                throwUnexpected("ERROR for an unknown request");
            LOG_DEBUG(log, "Ignoring ERROR for the request {}, which is not awaited", command.error().request_id());
            return;
        }
        case Type::CLOSE_CONSUMER:
        {
            if (!command.has_close_consumer() || !command.close_consumer().has_consumer_id())
                throwUnexpected("CLOSE_CONSUMER");
            if (SubConsumer * sub = findSubConsumer(connection, command.close_consumer().consumer_id()))
                resetSubConsumer(*sub, "closed by the broker");
            return;
        }
        case Type::REACHED_END_OF_TOPIC:
        case Type::ACTIVE_CONSUMER_CHANGE:
            LOG_DEBUG(log, "Ignoring {} from the Pulsar broker", Type::Type_Name(command.type()));
            return;
        default:
            LOG_TRACE(log, "Ignoring {} from the Pulsar broker", Type::Type_Name(command.type()));
            return;
    }
}

void ConsumerImpl::onMessage(Connection & connection, SubConsumer & sub, Frame & frame, const SharedBuffer & buffer)
{
    const auto & message = frame.command.message();
    const EntryId id{message.message_id().ledgerid(), message.message_id().entryid()};

    /// The broker sends a message only while it has permits, but a batch can take more permits than were left.
    if (sub.received_entries >= sub.granted)
        throwUnexpected("MESSAGE beyond the permits");
    ++sub.received_entries;

    decodeEntry(frame.tail, entry);
    const auto & metadata = entry.metadata;
    const size_t num_messages = entry.checksum_mismatch || !metadata.has_num_messages_in_batch()
        ? 1 : static_cast<size_t>(std::max<Int32>(1, metadata.num_messages_in_batch()));
    sub.received_messages += num_messages;

    auto discard = [&](ValidationError error, const char * reason)
    {
        LOG_ERROR(log, "Discarding the message {}:{} from the Pulsar topic {}: {}", id.ledger_id, id.entry_id, *sub.topic, reason);
        sendAck(sub, {id}, error);
        sub.pending_flow += num_messages;
    };

    if (entry.checksum_mismatch)
    {
        discard(PulsarProto::CommandAck::ChecksumMismatch, "checksum mismatch");
        return;
    }

    if (metadata.has_encryption_keys())
    {
        /// Like `ConsumerCryptoFailureAction::FAIL` of the C++ client: neither delivered nor acknowledged.
        LOG_ERROR(log, "Cannot consume the encrypted message {}:{} from the Pulsar topic {}", id.ledger_id, id.entry_id, *sub.topic);
        sub.pending_flow += num_messages;
        return;
    }

    std::shared_ptr<const VectorWithMemoryTracking<EntryId>> chunk_ids;
    DecodedPayload decoded;
    const bool is_chunk = !metadata.has_num_messages_in_batch() && metadata.num_chunks_from_msg() > 1;
    if (is_chunk)
    {
        const bool is_last = metadata.chunk_id() == metadata.num_chunks_from_msg() - 1;
        /// The permit of a chunk other than the last one is returned right away.
        if (!is_last)
            ++sub.pending_flow;

        ChunkAssembler::Result result;
        sub.chunks.add(metadata, id, entry.payload, connection.maxMessageSize(), client->limits, result);
        if (!result.ack.empty())
            sendAck(sub, result.ack, {});
        if (result.ack_corrupted)
            sendAck(sub, {*result.ack_corrupted}, PulsarProto::CommandAck::UncompressedSizeCorruption);
        sendRedeliver(sub, result.redeliver);
        if (!result.message)
        {
            if (is_last)
                ++sub.pending_flow;
            return;
        }

        auto assembled = std::make_shared<StringWithMemoryTracking>(std::move(*result.message));
        decoded = decodePayload(metadata, *assembled, assembled, 0, false, client->limits);
        auto ids = std::make_shared<VectorWithMemoryTracking<EntryId>>(std::move(result.message_chunks));
        if (decoded.error)
        {
            LOG_ERROR(log, "Discarding the chunked message {}:{} from the Pulsar topic {}", id.ledger_id, id.entry_id, *sub.topic);
            sendAck(sub, *ids, *decoded.error);
            ++sub.pending_flow;
            return;
        }
        chunk_ids = std::move(ids);
    }
    else
    {
        decoded = decodePayload(metadata, entry.payload, buffer, connection.maxMessageSize(), true, client->limits);
        if (decoded.error)
        {
            discard(*decoded.error, "the payload is corrupted");
            return;
        }
    }

    QueuedEntry & queued = queue.emplace_back(QueuedEntry{
        .sub_consumer = sub.index,
        .consumer_id = sub.consumer_id,
        .entry = id,
        .cursor = BatchCursor(std::move(decoded.batch), metadata),
        .chunks = std::move(chunk_ids),
    });
    sub.queued += queued.cursor.size();
    queued_messages += queued.cursor.size();
}

bool ConsumerImpl::nothingMoreCanArrive() const
{
    for (const auto & sub : sub_consumers)
        if (sub->state == State::Ready && sub->granted > sub->received_messages)
            return false;
    return true;
}

Messages ConsumerImpl::takeMessages(size_t max_messages)
{
    Messages res;
    res.reserve(std::min(max_messages, queued_messages));
    while (res.size() < max_messages && !queue.empty())
    {
        QueuedEntry & front = queue.front();
        SubConsumer & sub = *sub_consumers[front.sub_consumer];
        const MessageFields & fields = front.cursor.next();

        Message & message = res.emplace_back();
        message.buffer = front.cursor.buffer();
        message.data = fields.payload;
        message.topic_name = sub.topic;
        if (fields.ordering_key)
            message.ordering_key = *fields.ordering_key;
        if (fields.partition_key)
            message.partition_key = *fields.partition_key;
        message.event_time = fields.event_time;
        message.message_id.consumer_serial = serial;
        message.message_id.sub_consumer = front.sub_consumer;
        message.message_id.entry = front.entry;
        message.message_id.batch_index = fields.batch_index;
        message.message_id.batch_size = front.cursor.isBatched() ? static_cast<Int32>(front.cursor.size()) : 0;
        message.message_id.chunks = front.chunks;

        --queued_messages;
        --sub.queued;
        ++sub.pending_flow;
        if (!front.cursor.hasNext())
            queue.pop_front();
    }

    for (auto & sub : sub_consumers)
        if (sub->pending_flow >= std::max<size_t>(1, sub->window / 2))
            sendFlow(*sub);
    return res;
}

Messages ConsumerImpl::batchReceive(size_t max_messages, std::chrono::milliseconds timeout)
{
    checkUsable();
    if (max_messages == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The maximum number of messages to receive from Pulsar must be positive");

    try
    {
        const auto deadline = Clock::now() + timeout;
        while (true)
        {
            advance();
            session.process();
            advance();
            if (queued_messages >= max_messages || (queued_messages > 0 && nothingMoreCanArrive()) || Clock::now() >= deadline)
                break;
            session.wait(deadline);
        }
        Messages res = takeMessages(max_messages);
        /// Sends the permits.
        session.process();
        return res;
    }
    catch (...)
    {
        failed = true;
        /// An interruption replaces the failure it caused.
        session.checkInterrupted();
        throw;
    }
}

void ConsumerImpl::flushAcks()
{
    const auto deadline = Clock::now() + client->configuration.operation_timeout;
    while (true)
    {
        session.process();
        std::erase_if(acked_connections, [&](UInt64 id)
        {
            const Connection * connection = session.findConnection(id);
            return !connection || !connection->hasPendingOutput();
        });
        if (acked_connections.empty())
            return;
        if (Clock::now() >= deadline)
            throw Exception(ErrorCodes::CANNOT_CONNECT_PULSAR, "The acknowledgements were not sent to the Pulsar broker within {} ms",
                client->configuration.operation_timeout.count());
        session.wait(deadline);
    }
}

void ConsumerImpl::acknowledge(std::span<const MessageId> ids)
{
    checkUsable();
    try
    {
        for (const auto & id : ids)
        {
            if (id.consumer_serial != serial || id.sub_consumer >= sub_consumers.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "The Pulsar message {} was not received by this consumer", id.toString());

            SubConsumer & sub = *sub_consumers[id.sub_consumer];
            if (id.chunks)
            {
                sub.pending_acks.insert(sub.pending_acks.end(), id.chunks->begin(), id.chunks->end());
                continue;
            }
            if (id.batch_index >= 0 && id.batch_size > 1)
            {
                /// A batch is acknowledged when all its messages are.
                auto & acked = partial_acks[BatchKey{id.sub_consumer, id.entry}];
                if (acked.empty())
                    acked.resize(id.batch_size);
                if (static_cast<size_t>(id.batch_index) >= acked.size())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid batch index in the Pulsar message {}", id.toString());
                acked[id.batch_index] = true;
                if (!std::all_of(acked.begin(), acked.end(), [](bool x) { return x; }))
                    continue;
                partial_acks.erase(BatchKey{id.sub_consumer, id.entry});
            }
            sub.pending_acks.push_back(id.entry);
        }

        for (auto & sub : sub_consumers)
        {
            if (sub->state != State::Ready || sub->pending_acks.empty())
                continue;
            auto pending = std::move(sub->pending_acks);
            sub->pending_acks.clear();
            sendAck(*sub, pending, {});
        }
        flushAcks();
    }
    catch (...)
    {
        failed = true;
        session.checkInterrupted();
        throw;
    }
}

void ConsumerImpl::negativeAcknowledge(const MessageId & id) noexcept
{
    if (failed || closed)
        return;
    try
    {
        if (id.consumer_serial != serial || id.sub_consumer >= sub_consumers.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The Pulsar message {} was not received by this consumer", id.toString());
        SubConsumer & sub = *sub_consumers[id.sub_consumer];
        if (id.chunks)
            sendRedeliver(sub, *id.chunks);
        else
            sendRedeliver(sub, {id.entry});
        session.process();
    }
    catch (...)
    {
        failed = true;
        tryLogCurrentException(log, "Cannot negatively acknowledge a Pulsar message");
    }
}

void ConsumerImpl::close() noexcept
{
    if (closed)
        return;
    closed = true;
    session.close();
    queue.clear();
    queued_messages = 0;
}


Consumer::Consumer(std::unique_ptr<ConsumerImpl> impl_)
    : impl(std::move(impl_))
{
}

Consumer::~Consumer() = default;

Messages Consumer::batchReceive(size_t max_messages, std::chrono::milliseconds timeout)
{
    return impl->batchReceive(max_messages, timeout);
}

void Consumer::acknowledge(std::span<const MessageId> ids)
{
    impl->acknowledge(ids);
}

void Consumer::negativeAcknowledge(const MessageId & id) noexcept
{
    impl->negativeAcknowledge(id);
}

void Consumer::close() noexcept
{
    impl->close();
}

}

}

#endif
