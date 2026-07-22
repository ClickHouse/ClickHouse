#include <Storages/Pulsar/PulsarConsumer.h>

#include <IO/ReadBufferFromMemory.h>
#include <pulsar/Client.h>
#include <Common/logger_useful.h>


namespace DB
{

PulsarConsumer::PulsarConsumer(LoggerPtr logger_) : log(logger_)
{
}

ReadBufferPtr PulsarConsumer::getNextMessage()
{
    if (next_message == polled_messages.end())
        return nullptr;
    const auto * data = reinterpret_cast<const unsigned char *>(next_message->getData());
    size_t size = next_message->getLength();

    /// The message is acknowledged only after the block it belongs to is durably written (see commit).
    pending_acks.emplace_back(next_message->getMessageId());
    ++next_message;

    if (size != 0)
        return std::make_shared<ReadBufferFromMemory>(data, size);

    return getNextMessage();
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
    consumer.batchReceive(new_messages);
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
}

}
