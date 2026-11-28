#include <Storages/Pulsar/PulsarConsumer.h>

#include <IO/ReadBufferFromMemory.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>


namespace DB
{

PulsarConsumer::PulsarConsumer(
    LoggerPtr logger_, std::unique_ptr<Pulsar::Consumer> consumer_, size_t max_batch_size_, std::chrono::milliseconds poll_timeout_)
    : log(logger_)
    , consumer(std::move(consumer_))
    , max_batch_size(max_batch_size_)
    , poll_timeout(poll_timeout_)
    , next_message(polled_messages.end())
{
}

ReadBufferPtr PulsarConsumer::getNextMessage()
{
    /// Empty messages are skipped iteratively: a single poll can return the whole batch of them,
    /// and recursing once per skipped message could overflow the stack.
    while (next_message != polled_messages.end())
    {
        /// The payload points into a buffer owned by the message, which stays in `polled_messages`
        /// until the next poll.
        const auto payload = next_message->payload();

        /// The message is acknowledged only after the block it belongs to is durably written (see commit).
        pending_acks.emplace_back(next_message->id());
        ++next_message;

        if (!payload.empty())
            return std::make_shared<ReadBufferFromMemory>(payload);
    }

    return nullptr;
}

ReadBufferPtr PulsarConsumer::consume()
{
    if (hasPolledMessages())
        return getNextMessage();
    if (!polled_messages.empty())
    {
        polled_messages.clear();
        next_message = polled_messages.end();
    }
    Pulsar::Messages new_messages;
    try
    {
        /// On the timeout the client returns whatever messages were collected (possibly none).
        /// An exception is not an idle topic, and must not be mistaken for one, otherwise
        /// background streaming would silently stall forever.
        new_messages = consumer->batchReceive(max_batch_size, poll_timeout);
    }
    catch (...)
    {
        /// The client requires a consumer to be closed and created again after a call has thrown,
        /// so the storage drops it and recreates the slot (see `returnConsumer`).
        usable = false;
        throw;
    }
    if (new_messages.empty())
        return nullptr;
    LOG_TRACE(log, "Polled messages: {}", new_messages.size());
    polled_messages = std::move(new_messages);
    next_message = polled_messages.begin();
    return getNextMessage();
}

void PulsarConsumer::commit()
{
    if (pending_acks.empty())
        return;

    try
    {
        consumer->acknowledge(pending_acks);
    }
    catch (...)
    {
        /// Not a fatal error for the query: the engine provides at-least-once semantics, so the
        /// messages may be processed again. To make sure they are actually redelivered, the consumer
        /// must not go back to the pool: the storage closes it (which puts all its unacknowledged
        /// messages onto the redelivery path) and recreates the slot (see `returnConsumer`).
        tryLogCurrentException(log, fmt::format("Failed to acknowledge {} messages", pending_acks.size()), LogsLevel::warning);
        usable = false;
    }
    pending_acks.clear();
}

void PulsarConsumer::rollback()
{
    /// Request redelivery of everything that was consumed but not durably written.
    for (const auto & message_id : pending_acks)
        consumer->negativeAcknowledge(message_id);
    pending_acks.clear();

    /// Also put the prefetched but not yet returned tail of the current batch onto the
    /// redelivery path and drop it, so the next query reading from this pooled consumer
    /// does not resume from messages the aborted one already received.
    for (; next_message != polled_messages.end(); ++next_message)
        consumer->negativeAcknowledge(next_message->id());
    polled_messages.clear();
    next_message = polled_messages.end();

    /// The client keeps one more prefetch buffer below `batchReceive` (the receiver queue), whose
    /// messages are invisible here. Re-pooling the consumer would leave them attached to it, where
    /// they could stay stranded or be acknowledged by a later read that never returned them. So the
    /// storage closes the consumer instead (which puts all its unacknowledged messages, including
    /// the receiver queue, onto the redelivery path) and recreates the slot (see `returnConsumer`).
    usable = false;
}

void PulsarConsumer::release()
{
    /// Nothing is negatively acknowledged here: closing the consumer is what returns its messages
    /// (the acknowledgement-pending ones, the prefetched tail of the batch and the receiver queue)
    /// to the subscription, and a negative acknowledgement would only add a redelivery request
    /// which closing the consumer makes redundant.
    pending_acks.clear();
    polled_messages.clear();
    next_message = polled_messages.end();
    usable = false;
}

}
