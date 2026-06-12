#include <atomic>
#include <chrono>
#include <memory>
#include <utility>
#include <Storages/NATS/INATSConsumer.h>
#include <IO/ReadBufferFromMemory.h>
#include <Poco/Timer.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_STATE;
}

INATSConsumer::INATSConsumer(
    NATSConnectionPtr connection_,
    const std::vector<String> & subjects_,
    const String & subscribe_queue_name,
    LoggerPtr log_,
    uint32_t queue_size_,
    const std::atomic<bool> & stopped_)
    : connection(std::move(connection_))
    , subjects(subjects_)
    , log(log_)
    , stopped(stopped_)
    , queue_name(subscribe_queue_name)
    , received(queue_size_)
{
}

bool INATSConsumer::isSubscribed() const
{
    return !subscriptions.empty();
}
void INATSConsumer::unsubscribe()
{
    subscriptions.clear();

    LOG_DEBUG(log, "Consumer {} unsubscribed", static_cast<void*>(this));
}

ReadBufferPtr INATSConsumer::consume(std::optional<UInt64> timeout_ms)
{
    if (stopped)
        return nullptr;

    const bool popped = timeout_ms ? received.tryPop(current, *timeout_ms) : received.tryPop(current);
    if (!popped)
        return nullptr;

    if (current.msg)
        consumed_messages.push_back(std::move(current.msg));

    return std::make_shared<ReadBufferFromMemory>(current.message);
}

void INATSConsumer::ackConsumed()
{
    for (auto & msg : consumed_messages)
        natsMsg_Ack(msg.get(), nullptr);
    consumed_messages.clear();
}

void INATSConsumer::dropConsumed()
{
    /// Release without acking, for JetStream the server redelivers these messages.
    consumed_messages.clear();
}

void INATSConsumer::onMsg(natsConnection *, natsSubscription *, natsMsg * msg, void * consumer)
{
    auto * nats_consumer = static_cast<INATSConsumer *>(consumer);

    /// For JetStream, keep the message so it can be acknowledged only after it has been inserted.
    /// For core NATS there is no ack, so it is destroyed right away.
    NatsMsgPtr owned_msg(nats_consumer->needsAck() ? msg : nullptr, &natsMsg_Destroy);

    const int msg_length = natsMsg_GetDataLength(msg);
    if (msg_length)
    {
        MessageData data = {
            .message = std::string(natsMsg_GetData(msg), msg_length),
            .subject = natsMsg_GetSubject(msg),
            .msg = std::move(owned_msg),
        };
        if (!nats_consumer->received.push(std::move(data)))
            throw Exception(ErrorCodes::INVALID_STATE, "Could not push to received queue");
    }

    if (!nats_consumer->needsAck())
        natsMsg_Destroy(msg);                 /// core NATS: we own the message and never ack it
    else if (msg_length == 0)
        natsMsg_Ack(owned_msg.get(), nullptr); /// empty JetStream message: ack so it is not redelivered
    /// else (JetStream with payload): ownership moved into the queue, acked after insertion
}

}
