#include <Storages/Pulsar/Client/Consumer.h>

#include <algorithm>
#include <chrono>
#include <map>

#include <Storages/Pulsar/Client/Compression.h>
#include <Storages/Pulsar/Client/Connection.h>
#include <Storages/Pulsar/Client/ConnectionRegistry.h>
#include <Common/logger_useful.h>

namespace DB::Pulsar
{

namespace
{

int64_t nowMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

uint32_t getBE32(const char * p)
{
    return (static_cast<uint32_t>(static_cast<unsigned char>(p[0])) << 24)
        | (static_cast<uint32_t>(static_cast<unsigned char>(p[1])) << 16)
        | (static_cast<uint32_t>(static_cast<unsigned char>(p[2])) << 8)
        | static_cast<uint32_t>(static_cast<unsigned char>(p[3]));
}

}

/// Owns one connection and consumer id per topic of the subscription. Everything runs on the thread
/// that calls the handle; `close` (from the client shutdown) just shuts the sockets down.
class ConsumerImpl
{
public:
    struct Sub
    {
        ConnectionPtr connection;
        uint64_t consumer_id = 0;
        std::string topic;
        int64_t permits = 0; /// messages granted to the broker but not yet received
        bool closed = false;
    };

    ConsumerImpl(std::vector<Sub> subs_, ConsumerConfiguration config_, LoggerPtr log_)
        : subs(std::move(subs_)), config(std::move(config_)), log(std::move(log_))
    {
    }

    Result batchReceive(Messages & messages)
    {
        const int max_messages = std::max(1, config.batch_receive_policy.max_messages);
        const int64_t deadline = nowMs() + std::max<long>(0, config.batch_receive_policy.timeout_ms);

        /// Grant each sub-consumer enough permits to fill one batch, without refilling within the call,
        /// so the broker never prefetches much more than one block.
        for (auto & sub : subs)
        {
            if (sub.closed)
                continue;
            const int64_t delta = max_messages - sub.permits;
            if (delta > 0)
            {
                if (Result r = sendFlow(sub, static_cast<uint32_t>(delta)); r != ResultOk)
                    return r;
                sub.permits += delta;
            }
        }

        bool terminal = false;
        while (static_cast<int>(messages.size()) < max_messages && nowMs() < deadline)
        {
            bool progressed = false;
            for (size_t sub_index = 0; sub_index < subs.size(); ++sub_index)
            {
                Sub & sub = subs[sub_index];
                if (sub.closed)
                    continue;

                const int64_t remaining = deadline - nowMs();
                if (remaining <= 0)
                    break;
                const int slice = subs.size() == 1 ? static_cast<int>(remaining) : static_cast<int>(std::min<int64_t>(remaining, 100));

                Connection::Incoming incoming;
                const Result r = sub.connection->receive(incoming, slice);
                if (r == ResultTimeout)
                    continue;
                if (r != ResultOk)
                {
                    sub.closed = true;
                    terminal = true;
                    continue;
                }

                const auto type = incoming.command.type();
                if (incoming.is_message)
                {
                    int entry_message_count = 1;
                    parseMessages(incoming, static_cast<int32_t>(sub_index), sub.topic, messages, entry_message_count);
                    sub.permits = std::max<int64_t>(0, sub.permits - entry_message_count);
                    progressed = true;
                    if (static_cast<int>(messages.size()) >= max_messages)
                        break;
                }
                else if (type == pulsar::proto::BaseCommand::CLOSE_CONSUMER)
                {
                    LOG_INFO(log, "Pulsar broker asked to close the consumer for topic {}", sub.topic);
                    sub.closed = true;
                    terminal = true;
                }
                /// REACHED_END_OF_TOPIC, ACTIVE_CONSUMER_CHANGE, ACK_RESPONSE, etc. are ignored.
            }

            if (isClosed())
                return ResultAlreadyClosed;
            if (terminal)
                break;
            if (!progressed && subs.size() == 1)
                break; /// the single receive() already waited out the slice
        }

        if (!messages.empty())
            return ResultOk;
        if (terminal)
            return ResultConsumerClosed;
        return isClosed() ? ResultAlreadyClosed : ResultOk;
    }

    Result acknowledge(const MessageIdList & message_ids)
    {
        /// Group by sub-consumer, then by broker entry, so a batched entry is acknowledged once with the
        /// final set of still-outstanding indices.
        std::map<int32_t, std::map<std::pair<int64_t, int64_t>, std::shared_ptr<BatchIndexAck>>> by_sub;
        for (const auto & id : message_ids)
        {
            if (id.consumer_index < 0 || static_cast<size_t>(id.consumer_index) >= subs.size())
                continue;
            if (id.batch_acker)
                id.batch_acker->ackIndex(id.batch_index);
            by_sub[id.consumer_index][{id.ledger_id, id.entry_id}] = id.batch_acker;
        }

        for (const auto & [sub_index, entries] : by_sub)
        {
            Sub & sub = subs[sub_index];
            if (sub.closed || sub.connection->isClosed())
                return ResultConsumerClosed;

            pulsar::proto::BaseCommand command;
            command.set_type(pulsar::proto::BaseCommand::ACK);
            auto * ack = command.mutable_ack();
            ack->set_consumer_id(sub.consumer_id);
            ack->set_ack_type(pulsar::proto::CommandAck_AckType_Individual);
            for (const auto & [entry, acker] : entries)
            {
                auto * message_id = ack->add_message_id();
                message_id->set_ledgerid(entry.first);
                message_id->set_entryid(entry.second);
                if (acker)
                    for (int64_t word : acker->outstanding())
                        message_id->add_ack_set(word);
            }

            if (Result r = sub.connection->send(command); r != ResultOk)
                return r;
        }
        return ResultOk;
    }

    Result negativeAcknowledge(const MessageId & id)
    {
        if (id.consumer_index < 0 || static_cast<size_t>(id.consumer_index) >= subs.size())
            return ResultOk;
        Sub & sub = subs[id.consumer_index];
        if (sub.closed || sub.connection->isClosed())
            return ResultOk;

        pulsar::proto::BaseCommand command;
        command.set_type(pulsar::proto::BaseCommand::REDELIVER_UNACKNOWLEDGED_MESSAGES);
        auto * redeliver = command.mutable_redeliverunacknowledgedmessages();
        redeliver->set_consumer_id(sub.consumer_id);
        auto * message_id = redeliver->add_message_ids();
        message_id->set_ledgerid(id.ledger_id);
        message_id->set_entryid(id.entry_id);

        sub.connection->send(command); /// best effort; redelivery also happens when the consumer closes
        return ResultOk;
    }

    Result close()
    {
        for (auto & sub : subs)
        {
            if (!sub.connection->isClosed())
            {
                pulsar::proto::BaseCommand command;
                command.set_type(pulsar::proto::BaseCommand::CLOSE_CONSUMER);
                const uint64_t request_id = sub.connection->newRequestId();
                auto * close_consumer = command.mutable_close_consumer();
                close_consumer->set_consumer_id(sub.consumer_id);
                close_consumer->set_request_id(request_id);
                pulsar::proto::BaseCommand response;
                sub.connection->request(command, request_id, response); /// best effort
                sub.connection->close();
            }
            sub.closed = true;
        }
        return ResultOk;
    }

private:
    bool isClosed() const
    {
        for (const auto & sub : subs)
            if (!sub.connection->isClosed())
                return false;
        return true;
    }

    Result sendFlow(Sub & sub, uint32_t permits)
    {
        pulsar::proto::BaseCommand command;
        command.set_type(pulsar::proto::BaseCommand::FLOW);
        auto * flow = command.mutable_flow();
        flow->set_consumer_id(sub.consumer_id);
        flow->set_messagepermits(permits);
        return sub.connection->send(command);
    }

    /// Turns one MESSAGE frame into one (non-batched) or several (batched) `Message`s. `entry_message_count`
    /// reports how many permits the broker consumed, even if the payload cannot be fully decoded.
    void parseMessages(
        const Connection::Incoming & incoming, int32_t consumer_index, const std::string & topic, Messages & out, int & entry_message_count)
    {
        pulsar::proto::MessageMetadata metadata;
        if (!metadata.ParseFromArray(incoming.metadata.data(), static_cast<int>(incoming.metadata.size())))
        {
            LOG_WARNING(log, "Cannot parse Pulsar message metadata for topic {}", topic);
            entry_message_count = 1;
            return;
        }

        const int num_messages = metadata.has_num_messages_in_batch() ? metadata.num_messages_in_batch() : 1;
        entry_message_count = std::max(1, num_messages);

        std::string uncompressed;
        if (!decompress(
                metadata.compression(), incoming.payload.data(), incoming.payload.size(), metadata.uncompressed_size(), uncompressed))
        {
            LOG_WARNING(log, "Cannot decompress a Pulsar message for topic {}", topic);
            return;
        }

        const auto & id_data = incoming.command.message().message_id();
        const int64_t ledger_id = id_data.ledgerid();
        const int64_t entry_id = id_data.entryid();
        const int32_t partition = id_data.has_partition() ? id_data.partition() : -1;

        if (num_messages <= 1)
        {
            auto data = std::make_shared<Message::Data>();
            data->payload = std::move(uncompressed);
            data->topic = topic;
            data->partition_key = metadata.partition_key();
            data->ordering_key = metadata.ordering_key();
            data->has_event_timestamp = metadata.has_event_time();
            data->event_timestamp = metadata.event_time();
            data->message_id = MessageId{ledger_id, entry_id, partition, -1, 0, consumer_index, nullptr};
            out.emplace_back(std::move(data));
            return;
        }

        /// Batched entry: split into single messages and share one acker across them.
        auto acker = std::make_shared<BatchIndexAck>(num_messages);
        const char * buffer = uncompressed.data();
        const size_t total = uncompressed.size();
        size_t offset = 0;
        for (int index = 0; index < num_messages; ++index)
        {
            if (offset + 4 > total)
            {
                LOG_WARNING(log, "Truncated Pulsar batch for topic {}: missing single-message metadata size", topic);
                break;
            }
            const uint32_t single_meta_size = getBE32(buffer + offset);
            offset += 4;
            if (offset + single_meta_size > total)
            {
                LOG_WARNING(log, "Truncated Pulsar batch for topic {}: single-message metadata exceeds payload", topic);
                break;
            }

            pulsar::proto::SingleMessageMetadata single_metadata;
            if (!single_metadata.ParseFromArray(buffer + offset, static_cast<int>(single_meta_size)))
            {
                LOG_WARNING(log, "Cannot parse single-message metadata in a Pulsar batch for topic {}", topic);
                break;
            }
            offset += single_meta_size;

            const int32_t payload_size = single_metadata.payload_size();
            if (payload_size < 0 || offset + static_cast<size_t>(payload_size) > total)
            {
                LOG_WARNING(log, "Truncated Pulsar batch for topic {}: single-message payload exceeds payload", topic);
                break;
            }

            auto data = std::make_shared<Message::Data>();
            data->payload.assign(buffer + offset, payload_size);
            offset += payload_size;
            data->topic = topic;
            data->partition_key = single_metadata.partition_key();
            data->ordering_key = single_metadata.ordering_key();
            if (single_metadata.has_event_time())
            {
                data->has_event_timestamp = true;
                data->event_timestamp = single_metadata.event_time();
            }
            else if (metadata.has_event_time())
            {
                data->has_event_timestamp = true;
                data->event_timestamp = metadata.event_time();
            }
            data->message_id = MessageId{ledger_id, entry_id, partition, index, num_messages, consumer_index, acker};
            out.emplace_back(std::move(data));
        }
    }

    std::vector<Sub> subs;
    ConsumerConfiguration config;
    LoggerPtr log;
};

Consumer::Consumer() = default;
Consumer::~Consumer() = default;
Consumer::Consumer(const Consumer &) = default;
Consumer & Consumer::operator=(const Consumer &) = default;
Consumer::Consumer(std::shared_ptr<ConsumerImpl> impl_) : impl(std::move(impl_)) { }

Consumer Consumer::open(
    const std::vector<TopicBroker> & topics,
    const std::string & subscription,
    const ClientConfiguration & client_config,
    const ConsumerConfiguration & consumer_config,
    ConnectionRegistry & registry,
    const LoggerPtr & log,
    Result & result)
{
    std::vector<ConsumerImpl::Sub> subs;
    for (const auto & topic_broker : topics)
    {
        ConnectionPtr connection = Connection::open(topic_broker.host, topic_broker.port, client_config, log, result);
        if (!connection)
            return Consumer();
        registry.add(connection);

        const uint64_t consumer_id = 0; /// one consumer per connection
        const uint64_t request_id = connection->newRequestId();

        pulsar::proto::BaseCommand command;
        command.set_type(pulsar::proto::BaseCommand::SUBSCRIBE);
        auto * subscribe = command.mutable_subscribe();
        subscribe->set_topic(topic_broker.topic);
        subscribe->set_subscription(subscription);
        subscribe->set_subtype(pulsar::proto::CommandSubscribe_SubType_Shared);
        subscribe->set_consumer_id(consumer_id);
        subscribe->set_request_id(request_id);
        subscribe->set_durable(true);
        subscribe->set_initialposition(
            consumer_config.subscription_initial_position == InitialPositionEarliest
                ? pulsar::proto::CommandSubscribe_InitialPosition_Earliest
                : pulsar::proto::CommandSubscribe_InitialPosition_Latest);

        pulsar::proto::BaseCommand response;
        if (result = connection->request(command, request_id, response); result != ResultOk)
            return Consumer();
        if (response.type() != pulsar::proto::BaseCommand::SUCCESS)
        {
            if (response.type() == pulsar::proto::BaseCommand::ERROR)
                LOG_ERROR(log, "Cannot subscribe to Pulsar topic {}: {}", topic_broker.topic, response.error().message());
            result = ResultBrokerError;
            return Consumer();
        }

        subs.push_back(ConsumerImpl::Sub{connection, consumer_id, topic_broker.topic, 0, false});
    }

    result = ResultOk;
    return Consumer(std::make_shared<ConsumerImpl>(std::move(subs), consumer_config, log));
}

Result Consumer::batchReceive(Messages & messages)
{
    return impl ? impl->batchReceive(messages) : ResultConsumerClosed;
}

Result Consumer::acknowledge(const MessageIdList & message_ids)
{
    return impl ? impl->acknowledge(message_ids) : ResultConsumerClosed;
}

Result Consumer::negativeAcknowledge(const MessageId & message_id)
{
    return impl ? impl->negativeAcknowledge(message_id) : ResultOk;
}

Result Consumer::close()
{
    return impl ? impl->close() : ResultOk;
}

}
