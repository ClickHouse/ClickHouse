#include <atomic>
#include <chrono>
#include <memory>
#include <utility>
#include <Storages/NATS/INATSConsumer.h>
#include <IO/ReadBufferFromMemory.h>
#include <Poco/Timer.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

static const int64_t DRAIN_TIMEOUT_MS = 5000;

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
    if (stopped)
        received.finish();

    for (auto & subscription : subscriptions)
    {
        auto status = natsSubscription_DrainTimeout(subscription.get(), DRAIN_TIMEOUT_MS);
        if (status != NATS_OK)
        {
            LOG_WARNING(log, "Failed to start draining a subscription of consumer {}: {}",
                static_cast<void *>(this), natsStatus_GetText(status));
            continue;
        }

        status = natsSubscription_WaitForDrainCompletion(subscription.get(), DRAIN_TIMEOUT_MS);
        if (status != NATS_OK)
            LOG_WARNING(log, "A subscription of consumer {} did not finish draining: {}",
                static_cast<void *>(this), natsStatus_GetText(status));
    }

    subscriptions.clear();

    LOG_DEBUG(log, "Consumer {} unsubscribed", static_cast<void*>(this));
}

void INATSConsumer::dropBuffered()
{
    consumed_messages.clear();
    MessageData dropped;
    while (received.tryPop(dropped)) {}
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

    try
    {
        const int msg_length = natsMsg_GetDataLength(msg);
        if (msg_length)
        {
            String message_received = std::string(natsMsg_GetData(msg), msg_length);
            String subject = natsMsg_GetSubject(msg);

            MessageData data = {
                .message = message_received,
                .subject = subject,
                .msg = std::move(owned_msg),
            };
            if (!nats_consumer->received.push(std::move(data)))
            {
                LOG_DEBUG(nats_consumer->log, "Consumer {} is shutting down, dropping a message", static_cast<void *>(nats_consumer));
                nats_consumer->nackMessage(msg);
            }
        }
        else if (nats_consumer->needsAck())
        {
            /// empty JetStream message: ack so it is not redelivered
            natsMsg_Ack(msg, nullptr);
        }
    }
    catch (...)
    {
        tryLogCurrentException(nats_consumer->log, "Could not push to received queue");
        nats_consumer->nackMessage(msg);
    }

    if (!nats_consumer->needsAck())
        natsMsg_Destroy(msg);
}

void INATSConsumer::nackMessage(natsMsg *)
{
    /// Core NATS has no acknowledgements. Nothing to do.
}

}
