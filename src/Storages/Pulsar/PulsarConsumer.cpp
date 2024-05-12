#include <Storages/Pulsar/PulsarConsumer.h>

#include <IO/ReadBufferFromMemory.h>
#include <pulsar/Client.h>
#include <Common/logger_useful.h>


namespace DB
{

PulsarConsumer::PulsarConsumer(LoggerPtr logger_) : logger(logger_)
{
}

ReadBufferPtr PulsarConsumer::getNextMessage()
{
    LOG_TRACE(logger, "getting next message");
    if (next_message == polled_messages.end())
        return nullptr;
    const auto * data = reinterpret_cast<const unsigned char *>(next_message->getData());
    size_t size = next_message->getLength();

    {
        auto msg = next_message->getDataAsString();
        LOG_TRACE(logger, "message: {}", msg);
    }

    ++next_message;

    if (size != 0)
        return std::make_shared<ReadBufferFromMemory>(data, size);

    return getNextMessage();
}

ReadBufferPtr PulsarConsumer::consume()
{
    LOG_TRACE(logger, "trying consume");
    if (hasPolledMessages())
    {
        LOG_TRACE(logger, "got the message B)");
        return getNextMessage();
    }
    pulsar::Messages new_messages;
    consumer.batchReceive(new_messages);
    polled_messages = std::move(new_messages);
    next_message = polled_messages.begin();
    LOG_TRACE(logger, "here's the message B)");
    return getNextMessage();
}

}
