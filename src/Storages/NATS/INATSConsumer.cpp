#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
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
    , queue_size(queue_size_)
    , received(std::make_shared<ConcurrentBoundedQueue<MessageData>>(queue_size_))
{
}

std::shared_ptr<ConcurrentBoundedQueue<INATSConsumer::MessageData>> INATSConsumer::loadReceived() const
{
    std::lock_guard lock(received_mutex);
    return received;
}

void INATSConsumer::storeReceived(std::shared_ptr<ConcurrentBoundedQueue<MessageData>> queue)
{
    std::lock_guard lock(received_mutex);
    received = std::move(queue);
}

bool INATSConsumer::isSubscribed() const
{
    return !subscriptions.empty();
}

bool INATSConsumer::hasClosedSubscription() const
{
    return std::ranges::any_of(
        subscriptions, [](const auto & subscription) { return !natsSubscription_IsValid(subscription.get()); });
}

bool INATSConsumer::hasConnectionReconnected() const
{
    return connection->getReconnectCount() != connection_reconnect_count;
}

bool INATSConsumer::isConnectionConnected() const
{
    return connection->isConnected();
}

void INATSConsumer::subscribe()
{
    if (isSubscribed())
        return;

    if (loadReceived()->isFinished())
        storeReceived(std::make_shared<ConcurrentBoundedQueue<MessageData>>(queue_size));

    /// Read before subscribing: a reconnect racing `subscribeImpl` can already have dropped what the
    /// new subscription is waiting for, and the count from before still reports that reconnect.
    const UInt64 reconnect_count_before_subscribe = connection->getReconnectCount();

    subscribeImpl();

    connection_reconnect_count = reconnect_count_before_subscribe;
}

void INATSConsumer::unsubscribe()
{
    for (auto & subscription : subscriptions)
    {
        /// The client closes a subscription itself when its fetch is terminated, so draining an
        /// already-closed one is expected rather than a problem.
        const bool was_closed = !natsSubscription_IsValid(subscription.get());

        auto status = natsSubscription_DrainTimeout(subscription.get(), DRAIN_TIMEOUT_MS);
        if (status != NATS_OK)
        {
            if (was_closed)
                LOG_DEBUG(log, "A subscription of consumer {} was already closed by the NATS client",
                    static_cast<void *>(this));
            else
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

void INATSConsumer::finishAndReturnUnprocessed(SkippedMessages skipped_messages_action)
{
    /// Handing a message back needs the subscription it arrived on: `natsMsg_Nak` follows a plain
    /// pointer from the message to that subscription, and on to the JetStream context and the
    /// connection. An unsubscribed consumer holds only leftovers of a subscription that is already
    /// gone - a direct `SELECT` that ended leaves them behind - so there is nothing to hand them
    /// back through, and nothing more can arrive either.
    if (!isSubscribed())
    {
        loadReceived()->finish();
        dropBuffered();
        return;
    }

    /// Handles of messages this consumer has read but not acknowledged: nothing inserted them, so
    /// the broker has to deliver them again.
    for (auto & msg : consumed_messages)
        nackMessage(msg.get());
    consumed_messages.clear();

    /// Messages that yielded no rows are not waiting to be inserted: `nats_skip_broken_messages`
    /// passed over them on purpose. Where that skip is already final, handing them back would undo
    /// it and deliver the same malformed input again, so they are acknowledged instead. Where it is
    /// not - an uncommitted direct `SELECT`, which must consume nothing - they go back to the
    /// broker like every other message this consumer has not committed.
    if (skipped_messages_action == SkippedMessages::Acknowledge)
    {
        ackMessages(skipped_messages);
    }
    else
    {
        for (auto & msg : skipped_messages)
            nackMessage(msg.get());
        skipped_messages.clear();
    }

    /// Finishing the queue before draining it is what makes this complete rather than a snapshot:
    /// the queue serializes `push` with `finish`, so a message the NATS client thread is delivering
    /// right now either lands in the queue before it is finished, and the loop below returns it, or
    /// fails to push and `onMsg` returns it itself. Nothing can be appended afterwards.
    auto queue = loadReceived();
    queue->finish();

    MessageData buffered;
    while (queue->tryPop(buffered))
    {
        if (buffered.msg)
            nackMessage(buffered.msg.get());
    }
}

void INATSConsumer::dropBuffered()
{
    consumed_messages.clear();
    skipped_messages.clear();
    auto queue = loadReceived();
    MessageData dropped;
    while (queue->tryPop(dropped)) {}
}

ReadBufferPtr INATSConsumer::consume(std::optional<UInt64> timeout_ms)
{
    if (stopped)
        return nullptr;

    auto queue = loadReceived();
    const bool popped = timeout_ms ? queue->tryPop(current, *timeout_ms) : queue->tryPop(current);
    if (!popped)
        return nullptr;

    if (current.msg)
        consumed_messages.push_back(std::move(current.msg));

    return std::make_shared<ReadBufferFromMemory>(current.message);
}

void INATSConsumer::ackMessages(std::vector<NatsMsgPtr> & messages)
{
    for (auto & msg : messages)
    {
        auto status = natsMsg_Ack(msg.get(), nullptr);
        if (status != NATS_OK)
            LOG_WARNING(log, "Failed to acknowledge a message in consumer {}: {} (server may redeliver it)",
                static_cast<void *>(this), natsStatus_GetText(status));
    }
    messages.clear();
}

void INATSConsumer::ackConsumed()
{
    ackMessages(consumed_messages);
    ackMessages(skipped_messages);
}

void INATSConsumer::markLastConsumedSkipped()
{
    /// Core NATS messages are destroyed as they arrive - there is nothing to acknowledge and
    /// nothing was recorded - so a cycle that skips one has nothing to move here.
    if (consumed_messages.empty())
        return;

    skipped_messages.push_back(std::move(consumed_messages.back()));
    consumed_messages.pop_back();
}

void INATSConsumer::dropConsumed()
{
    /// Release without acking, for JetStream the server redelivers these messages. A skipped
    /// message is released the same way: nothing committed the cycle that skipped it, and the
    /// redelivered message is skipped again.
    consumed_messages.clear();
    skipped_messages.clear();
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
            auto queue = nats_consumer->loadReceived();
            if (!queue->push(std::move(data)))
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
        if (owned_msg)
            nats_consumer->nackMessage(owned_msg.get());
    }

    if (!nats_consumer->needsAck())
        natsMsg_Destroy(msg);
}

void INATSConsumer::nackMessage(natsMsg *)
{
    /// Core NATS has no acknowledgements. Nothing to do.
}

}
