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

    {
        auto msg = next_message->getDataAsString();
        LOG_TRACE(log, "Message recieved: {}", msg);
    }

    ++next_message;

    if (size != 0)
        return std::make_shared<ReadBufferFromMemory>(data, size);

    return getNextMessage();
}

ReadBufferPtr PulsarConsumer::consume()
{
    if (hasPolledMessages())
    {
        return getNextMessage();
    }
    if (!polled_messages.empty())
    {
        pulsar::MessageIdList message_ids;
        for (size_t i = 0; i < polled_messages.size(); ++i)
        {
            message_ids.emplace_back(polled_messages[i].getMessageId());
        }
        consumer.acknowledge(message_ids);
        polled_messages.clear();
        next_message = polled_messages.end();
    }
    pulsar::Messages new_messages;
    consumer.batchReceive(new_messages);
    if (new_messages.empty()) {
        return nullptr;
    }
    LOG_TRACE(log, "polled messages: {}", new_messages.size());
    polled_messages = std::move(new_messages);
    next_message = polled_messages.begin();
    return getNextMessage();
}

}
