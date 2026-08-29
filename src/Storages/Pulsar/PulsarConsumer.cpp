#include <Storages/Pulsar/PulsarConsumer.h>

#include <IO/ReadBufferFromMemory.h>
#include <pulsar/Client.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_CONNECT_PULSAR;
}

PulsarConsumer::PulsarConsumer(LoggerPtr logger_) : log(logger_), next_message(polled_messages.end())
{
}

ReadBufferPtr PulsarConsumer::getNextMessage()
{
    /// Empty messages are skipped iteratively: a single poll can return the whole batch of them,
    /// and recursing once per skipped message could overflow the stack.
    while (next_message != polled_messages.end())
    {
        const auto * data = reinterpret_cast<const unsigned char *>(next_message->getData());
        size_t size = next_message->getLength();

        /// The message is acknowledged only after the block it belongs to is durably written (see commit).
        pending_acks.emplace_back(next_message->getMessageId());
        ++next_message;

        if (size != 0)
            return std::make_shared<ReadBufferFromMemory>(data, size);
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
    pulsar::Messages new_messages;
    auto result = consumer.batchReceive(new_messages);
    /// On the batch receive timeout the client completes the call with `ResultOk` and whatever
    /// messages were collected (possibly none). Any other result is a terminal condition of the
    /// consumer (e.g. `ResultAlreadyClosed`, `ResultConsumerNotInitialized`) and must not be
    /// mistaken for an idle topic, otherwise background streaming would silently stall forever.
    if (result != pulsar::ResultOk)
    {
        /// The consumer must not be reused after a terminal error: the next poll would fail the
        /// same way, so the storage drops it and recreates the slot (see `returnConsumer`).
        usable = false;
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_PULSAR, "Failed to receive messages from Pulsar: {}", pulsar::strResult(result));
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

    auto result = consumer.acknowledge(pending_acks);
    if (result != pulsar::ResultOk)
    {
        /// Not a fatal error: unacknowledged messages will be redelivered, and the engine provides
        /// at-least-once semantics anyway. Report the failure and let the messages be processed again.
        LOG_WARNING(log, "Failed to acknowledge {} messages: {}", pending_acks.size(), pulsar::strResult(result));
    }
    pending_acks.clear();
}

void PulsarConsumer::rollback()
{
    /// Request redelivery of everything that was consumed but not durably written.
    for (const auto & message_id : pending_acks)
        consumer.negativeAcknowledge(message_id);
    pending_acks.clear();

    /// Also put the prefetched but not yet returned tail of the current batch onto the
    /// redelivery path and drop it, so the next query reading from this pooled consumer
    /// does not resume from messages the aborted one already received.
    for (; next_message != polled_messages.end(); ++next_message)
        consumer.negativeAcknowledge(next_message->getMessageId());
    polled_messages.clear();
    next_message = polled_messages.end();
}

}
