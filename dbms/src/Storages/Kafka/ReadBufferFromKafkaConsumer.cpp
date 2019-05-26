#include <Storages/Kafka/ReadBufferFromKafkaConsumer.h>

namespace DB
{

void ReadBufferFromKafkaConsumer::commit()
{
    if (messages.empty() || current == messages.begin())
        return;

    auto & previous = *std::prev(current);

    LOG_TRACE(log, "Committing message with offset " << previous.get_offset());
    consumer->async_commit(previous);
}

void ReadBufferFromKafkaConsumer::subscribe(const Names & topics)
{
    // While we wait for an assignment after subscribtion, we'll poll zero messages anyway.
    // If we're doing a manual select then it's better to get something after a wait, then immediate nothing.
    if (consumer->get_subscription().empty())
    {
        using namespace std::chrono_literals;

        consumer->pause(); // don't accidentally read any messages
        consumer->subscribe(topics);
        consumer->poll(5s);
        consumer->resume();
    }

    stalled = false;
}

void ReadBufferFromKafkaConsumer::unsubscribe()
{
    LOG_TRACE(log, "Re-joining claimed consumer after failure");
    consumer->unsubscribe();
}

/// Do commit messages implicitly after we processed the previous batch.
bool ReadBufferFromKafkaConsumer::nextImpl()
{
    /// NOTE: ReadBuffer was implemented with an immutable underlying contents in mind.
    ///       If we failed to poll any message once - don't try again.
    ///       Otherwise, the |poll_timeout| expectations get flawn.
    if (stalled)
        return false;

    if (current == messages.end())
    {
        if (intermediate_commit)
            commit();
        messages = consumer->poll_batch(batch_size, std::chrono::milliseconds(poll_timeout));
        current = messages.begin();

        LOG_TRACE(log, "Polled batch of " << messages.size() << " messages");
    }

    if (messages.empty() || current == messages.end())
    {
        stalled = true;
        return false;
    }

    if (auto err = current->get_error())
    {
        ++current;

        // TODO: should throw exception instead
        LOG_ERROR(log, "Consumer error: " << err);
        return false;
    }

    // XXX: very fishy place with const casting.
    auto new_position = reinterpret_cast<char *>(const_cast<unsigned char *>(current->get_payload().get_data()));
    BufferBase::set(new_position, current->get_payload().get_size(), 0);

    ++current;

    return true;
}

}
