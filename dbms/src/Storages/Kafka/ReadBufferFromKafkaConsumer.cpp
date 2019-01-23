#include <Storages/Kafka/ReadBufferFromKafkaConsumer.h>

namespace DB
{
namespace
{
    /// How long to wait for a single message (applies to each individual message)
    const auto READ_POLL_MS = 500;
} // namespace

bool ReadBufferFromKafkaConsumer::nextImpl()
{
    // FIXME: we can speed up feed if we do poll in advance
    message = consumer->poll(std::chrono::milliseconds(READ_POLL_MS));
    if (!message)
        return false;

    if (message.is_eof())
    {
        // Reached EOF while reading current batch, skip it.
        LOG_TRACE(log, "EOF reached for partition " << message.get_partition() << " offset " << message.get_offset());
        return nextImpl();
    }
    else if (auto err = message.get_error())
    {
        // TODO: should throw exception
        LOG_ERROR(log, "Consumer error: " << err);
        return false;
    }

    ++read_messages;

    // XXX: very fishy place with const casting.
    auto new_position = reinterpret_cast<char *>(const_cast<unsigned char *>(message.get_payload().get_data()));
    BufferBase::set(new_position, message.get_payload().get_size(), 0);

    return true;
}

} // namespace DB
