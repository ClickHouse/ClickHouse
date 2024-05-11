#include <Storages/Pulsar/PulsarConsumer.h>

#include <IO/ReadBufferFromMemory.h>
#include <pulsar/Client.h>

namespace DB
{

ReadBufferPtr PulsarConsumer::getNextMessage()
{
    if (next_message == polled_messages.end())
        return nullptr;
    const auto * data = reinterpret_cast<const unsigned char *>(next_message->getData());
    size_t size = next_message->getLength();
    ++next_message;

    if (size != 0)
        return std::make_shared<ReadBufferFromMemory>(data, size);

    return getNextMessage();
}

ReadBufferPtr PulsarConsumer::consume()
{
    if (hasPolledMessages())
        return getNextMessage();
    pulsar::Messages new_messages;
    consumer.batchReceive(new_messages);
    polled_messages = std::move(new_messages);
    next_message = polled_messages.begin();
    return getNextMessage();
}

}
