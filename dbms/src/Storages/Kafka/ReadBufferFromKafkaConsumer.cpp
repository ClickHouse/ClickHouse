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
    if (current_pending)
    {
        // XXX: very fishy place with const casting.
        BufferBase::set(
            reinterpret_cast<char *>(const_cast<unsigned char *>(current.get_payload().get_data())), current.get_payload().get_size(), 0);
        current_pending = false;
        return true;
    }

    // Process next buffered message
    auto message = consumer->poll(std::chrono::milliseconds(READ_POLL_MS));
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
        LOG_ERROR(log, "Consumer error: " << err);
        return false;
    }

    ++read_messages;

    // Now we've received a new message. Check if we need to produce a delimiter
    if (row_delimiter != '\0' && current)
    {
        BufferBase::set(&row_delimiter, 1, 0);
        current = std::move(message);
        current_pending = true;
        return true;
    }

    // Consume message and mark the topic/partition offset
    // The offsets will be committed in the readSuffix() method after the block is completed
    // If an exception is thrown before that would occur, the client will rejoin without committing offsets
    current = std::move(message);

    // XXX: very fishy place with const casting.
    BufferBase::set(
        reinterpret_cast<char *>(const_cast<unsigned char *>(current.get_payload().get_data())), current.get_payload().get_size(), 0);
    return true;
}

} // namespace DB
