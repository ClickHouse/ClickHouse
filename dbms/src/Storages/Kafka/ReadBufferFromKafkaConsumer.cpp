#include <Storages/Kafka/ReadBufferFromKafkaConsumer.h>

#include <common/logger_useful.h>

#include <cppkafka/cppkafka.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

using namespace std::chrono_literals;

ReadBufferFromKafkaConsumer::ReadBufferFromKafkaConsumer(
    ConsumerPtr consumer_, Poco::Logger * log_, size_t max_batch_size, size_t poll_timeout_)
    : ReadBuffer(nullptr, 0)
    , consumer(consumer_)
    , log(log_)
    , batch_size(max_batch_size)
    , poll_timeout(poll_timeout_)
    , current(messages.begin())
{
}

ReadBufferFromKafkaConsumer::~ReadBufferFromKafkaConsumer()
{
    /// NOTE: see https://github.com/edenhill/librdkafka/issues/2077
    consumer->unsubscribe();
    consumer->unassign();
    while (consumer->get_consumer_queue().next_event(1s));
}

void ReadBufferFromKafkaConsumer::commit()
{
    auto PrintOffsets = [this] (const char * prefix, const cppkafka::TopicPartitionList & offsets)
    {
        for (const auto & topic_part : offsets)
        {
            auto print_special_offset = [&topic_part]
            {
                switch (topic_part.get_offset())
                {
                    case cppkafka::TopicPartition::OFFSET_BEGINNING: return "BEGINNING";
                    case cppkafka::TopicPartition::OFFSET_END: return "END";
                    case cppkafka::TopicPartition::OFFSET_STORED: return "STORED";
                    case cppkafka::TopicPartition::OFFSET_INVALID: return "INVALID";
                    default: return "";
                }
            };

            if (topic_part.get_offset() < 0)
            {
                LOG_TRACE(
                    log,
                    prefix << " " << print_special_offset() << " (topic: " << topic_part.get_topic()
                           << ", partition: " << topic_part.get_partition() << ")");
            }
            else
            {
                LOG_TRACE(
                    log,
                    prefix << " " << topic_part.get_offset() << " (topic: " << topic_part.get_topic()
                           << ", partition: " << topic_part.get_partition() << ")");
            }
        }
    };

    PrintOffsets("Polled offset", consumer->get_offsets_position(consumer->get_assignment()));

    consumer->async_commit();

    PrintOffsets("Committed offset", consumer->get_offsets_committed(consumer->get_assignment()));
}

void ReadBufferFromKafkaConsumer::subscribe(const Names & topics)
{
    {
        String message = "Already subscribed to topics:";
        for (const auto & topic : consumer->get_subscription())
            message += " " + topic;
        LOG_TRACE(log, message);
    }

    {
        String message = "Already assigned to topics:";
        for (const auto & toppar : consumer->get_assignment())
            message += " " + toppar.get_topic();
        LOG_TRACE(log, message);
    }

    if (consumer->get_subscription().empty())
    {
        try
        {
            consumer->subscribe(topics);
            /// FIXME: if we failed to receive "subscribe" response while polling and destroy consumer now, then we may hang up.
            ///        see https://github.com/edenhill/librdkafka/issues/2077
        }
        catch (cppkafka::HandleException & e)
        {
            if (e.get_error() != RD_KAFKA_RESP_ERR__TIMED_OUT)
                throw;
        }
    }

    if (poll(poll_timeout))
        return;

    /// While we wait for an assignment after subscribtion, we'll poll zero messages anyway.
    /// If we're doing a manual select then it's better to get something after a wait, then immediate nothing.
    /// But due to the nature of async pause/resume/subscribe we can't guarantee any persistent state.
    /// See https://github.com/edenhill/librdkafka/issues/2455
    while (consumer->get_subscription().empty())
    {
        if (poll(poll_timeout))
            break;
    }
}

void ReadBufferFromKafkaConsumer::unsubscribe()
{
    LOG_TRACE(log, "Unsubscribing (possibly after some failure)");

    messages.clear();
    current = messages.begin();
    BufferBase::set(nullptr, 0, 0);

    consumer->unsubscribe();
}

bool ReadBufferFromKafkaConsumer::reset()
{
    if (current != messages.end())
    {
        ++current;
        return true;
    }

    return poll(0);
}

bool ReadBufferFromKafkaConsumer::nextImpl()
{
    while (current != messages.end())
    {
        if (auto error = current->get_error())
        {
            /// FIXME: should throw exception instead
            LOG_ERROR(log, "Consumer error: " << error);
            ++current;
            continue;
        }

        break;
    }

    if (current == messages.end())
        return false;

    /// XXX: very fishy place with const-casting.
    auto new_position = reinterpret_cast<char *>(const_cast<unsigned char *>(current->get_payload().get_data()));
    if (buffer().begin() == new_position)
        return false;

    BufferBase::set(new_position, current->get_payload().get_size(), 0);

    /// Since we can poll more messages than we already processed - commit only processed messages.
    consumer->store_offset(*current);

    return true;
}

bool ReadBufferFromKafkaConsumer::poll(size_t timeout)
{
    if (current != messages.end())
        throw Exception("Trying to poll new batch, while buffer still has pending messages!", ErrorCodes::LOGICAL_ERROR);

    try
    {
        messages = consumer->poll_batch(batch_size, std::chrono::milliseconds(timeout));
    }
    catch (cppkafka::HandleException & e)
    {
        if (e.get_error() != RD_KAFKA_RESP_ERR__TIMED_OUT)
            throw;
        messages.clear();
    }
    current = messages.begin();

    if (messages.empty())
    {
        LOG_TRACE(log, "No new messages");
        return false;
    }

    LOG_TRACE(log, "Polled batch of " << messages.size() << " messages");

    return true;
}

}
