#include <Storages/Kafka/ReadBufferFromKafkaConsumer.h>

#include <common/logger_useful.h>

#include <cppkafka/cppkafka.h>
#include <boost/algorithm/string/join.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMMIT_OFFSET;
}

using namespace std::chrono_literals;
const auto MAX_TIME_TO_WAIT_FOR_ASSIGNMENT_MS = 15000;
const auto DRAIN_TIMEOUT_MS = 5000ms;


ReadBufferFromKafkaConsumer::ReadBufferFromKafkaConsumer(
    ConsumerPtr consumer_,
    Poco::Logger * log_,
    size_t max_batch_size,
    size_t poll_timeout_,
    bool intermediate_commit_,
    const std::atomic<bool> & stopped_,
    const Names & _topics)
    : ReadBuffer(nullptr, 0)
    , consumer(consumer_)
    , log(log_)
    , batch_size(max_batch_size)
    , poll_timeout(poll_timeout_)
    , intermediate_commit(intermediate_commit_)
    , stopped(stopped_)
    , current(messages.begin())
    , topics(_topics)
{
    // called (synchroniously, during poll) when we enter the consumer group
    consumer->set_assignment_callback([this](const cppkafka::TopicPartitionList & topic_partitions)
    {
        LOG_TRACE(log, "Topics/partitions assigned: {}", topic_partitions);
        assignment = topic_partitions;
    });

    // called (synchroniously, during poll) when we leave the consumer group
    consumer->set_revocation_callback([this](const cppkafka::TopicPartitionList & topic_partitions)
    {
        // Rebalance is happening now, and now we have a chance to finish the work
        // with topics/partitions we were working with before rebalance
        LOG_TRACE(log, "Rebalance initiated. Revoking partitions: {}", topic_partitions);

        // we can not flush data to target from that point (it is pulled, not pushed)
        // so the best we can now it to
        // 1) repeat last commit in sync mode (async could be still in queue, we need to be sure is is properly committed before rebalance)
        // 2) stop / brake the current reading:
        //     * clean buffered non-commited messages
        //     * set flag / flush

        messages.clear();
        current = messages.begin();
        BufferBase::set(nullptr, 0, 0);

        rebalance_happened = true;
        assignment.clear();

        // for now we use slower (but reliable) sync commit in main loop, so no need to repeat
        // try
        // {
        //     consumer->commit();
        // }
        // catch (cppkafka::HandleException & e)
        // {
        //     LOG_WARNING(log, "Commit error: {}", e.what());
        // }
    });

    consumer->set_rebalance_error_callback([this](cppkafka::Error err)
    {
        LOG_ERROR(log, "Rebalance error: {}", err);
    });
}

ReadBufferFromKafkaConsumer::~ReadBufferFromKafkaConsumer()
{
    try
    {
        if (!consumer->get_subscription().empty())
        {
            try
            {
                consumer->unsubscribe();
            }
            catch (const cppkafka::HandleException & e)
            {
                LOG_ERROR(log, "Error during unsubscribe: {}", e.what());
            }
            drain();
        }
    }
    catch (const cppkafka::HandleException & e)
    {
        LOG_ERROR(log, "Error while destructing consumer: {}", e.what());
    }

}

// Needed to drain rest of the messages / queued callback calls from the consumer
// after unsubscribe, otherwise consumer will hang on destruction
// see https://github.com/edenhill/librdkafka/issues/2077
//     https://github.com/confluentinc/confluent-kafka-go/issues/189 etc.
void ReadBufferFromKafkaConsumer::drain()
{
    auto start_time = std::chrono::steady_clock::now();
    cppkafka::Error last_error(RD_KAFKA_RESP_ERR_NO_ERROR);

    while (true)
    {
        auto msg = consumer->poll(100ms);
        if (!msg)
            break;

        auto error = msg.get_error();

        if (error)
        {
            if (msg.is_eof() || error == last_error)
            {
                break;
            }
            else
            {
                LOG_ERROR(log, "Error during draining: {}", error);
            }
        }

        // i don't stop draining on first error,
        // only if it repeats once again sequentially
        last_error = error;

        auto ts = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::milliseconds>(ts-start_time) > DRAIN_TIMEOUT_MS)
        {
            LOG_ERROR(log, "Timeout during draining.");
            break;
        }
    }
}


void ReadBufferFromKafkaConsumer::commit()
{
    auto print_offsets = [this] (const char * prefix, const cppkafka::TopicPartitionList & offsets)
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
                LOG_TRACE(log, "{} {} (topic: {}, partition: {})", prefix, print_special_offset(), topic_part.get_topic(), topic_part.get_partition());
            }
            else
            {
                LOG_TRACE(log, "{} {} (topic: {}, partition: {})", prefix, topic_part.get_offset(), topic_part.get_topic(), topic_part.get_partition());
            }
        }
    };

    print_offsets("Polled offset", consumer->get_offsets_position(consumer->get_assignment()));

    if (hasMorePolledMessages())
    {
        LOG_WARNING(log, "Logical error. Non all polled messages were processed.");
    }

    if (offsets_stored > 0)
    {
        // if we will do async commit here (which is faster)
        // we may need to repeat commit in sync mode in revocation callback,
        // but it seems like existing API doesn't allow us to to that
        // in a controlled manner (i.e. we don't know the offsets to commit then)

        size_t max_retries = 5;
        bool commited = false;

        while (!commited && max_retries > 0)
        {
            try
            {
                // See https://github.com/edenhill/librdkafka/issues/1470
                // broker may reject commit if during offsets.commit.timeout.ms (5000 by default),
                // there were not enough replicas available for the __consumer_offsets topic.
                // also some other temporary issues like client-server connectivity problems are possible
                consumer->commit();
                commited = true;
                print_offsets("Committed offset", consumer->get_offsets_committed(consumer->get_assignment()));
            }
            catch (const cppkafka::HandleException & e)
            {
                LOG_ERROR(log, "Exception during commit attempt: {}", e.what());
            }
            --max_retries;
        }

        if (!commited)
        {
            // TODO: insert atomicity / transactions is needed here (possibility to rollback, ot 2 phase commits)
            throw Exception("All commit attempts failed. Last block was already written to target table(s), but was not commited to Kafka.", ErrorCodes::CANNOT_COMMIT_OFFSET);
        }
    }
    else
    {
        LOG_TRACE(log, "Nothing to commit.");
    }

    offsets_stored = 0;
    stalled = false;
}

void ReadBufferFromKafkaConsumer::subscribe()
{
    LOG_TRACE(log, "Already subscribed to topics: [{}]", boost::algorithm::join(consumer->get_subscription(), ", "));
    LOG_TRACE(log, "Already assigned to: {}", assignment);

    size_t max_retries = 5;

    while (consumer->get_subscription().empty())
    {
        --max_retries;
        try
        {
            consumer->subscribe(topics);
            // FIXME: if we failed to receive "subscribe" response while polling and destroy consumer now, then we may hang up.
            //        see https://github.com/edenhill/librdkafka/issues/2077
        }
        catch (cppkafka::HandleException & e)
        {
            if (max_retries > 0 && e.get_error() == RD_KAFKA_RESP_ERR__TIMED_OUT)
                continue;
            throw;
        }
    }

    stalled = false;
    rebalance_happened = false;
    offsets_stored = 0;
}

void ReadBufferFromKafkaConsumer::unsubscribe()
{
    LOG_TRACE(log, "Re-joining claimed consumer after failure");

    messages.clear();
    current = messages.begin();
    BufferBase::set(nullptr, 0, 0);

    // it should not raise exception as used in destructor
    try
    {
        // From docs: Any previous subscription will be unassigned and unsubscribed first.
        consumer->subscribe(topics);

        // I wanted to avoid explicit unsubscribe as it requires draining the messages
        // to close the consumer safely after unsubscribe
        // see https://github.com/edenhill/librdkafka/issues/2077
        //     https://github.com/confluentinc/confluent-kafka-go/issues/189 etc.
    }
    catch (const cppkafka::HandleException & e)
    {
        LOG_ERROR(log, "Exception from ReadBufferFromKafkaConsumer::unsubscribe: {}", e.what());
    }

}


bool ReadBufferFromKafkaConsumer::hasMorePolledMessages() const
{
    return (!stalled) && (current != messages.end());
}


void ReadBufferFromKafkaConsumer::resetToLastCommitted(const char * msg)
{
    if (assignment.empty())
    {
        LOG_TRACE(log, "Not assignned. Can't reset to last committed position.");
        return;
    }
    auto committed_offset = consumer->get_offsets_committed(consumer->get_assignment());
    consumer->assign(committed_offset);
    LOG_TRACE(log, "{} Returned to committed position: {}", msg, committed_offset);
}

/// Do commit messages implicitly after we processed the previous batch.
bool ReadBufferFromKafkaConsumer::nextImpl()
{

    /// NOTE: ReadBuffer was implemented with an immutable underlying contents in mind.
    ///       If we failed to poll any message once - don't try again.
    ///       Otherwise, the |poll_timeout| expectations get flawn.

    // we can react on stop only during fetching data
    // after block is formed (i.e. during copying data to MV / commiting)  we ignore stop attempts
    if (stopped)
    {
        was_stopped = true;
        offsets_stored = 0;
        return false;
    }

    if (stalled || was_stopped || !allowed || rebalance_happened)
        return false;


    if (current == messages.end())
    {
        if (intermediate_commit)
            commit();

        size_t waited_for_assignment = 0;
        while (true)
        {
            /// Don't drop old messages immediately, since we may need them for virtual columns.
            auto new_messages = consumer->poll_batch(batch_size, std::chrono::milliseconds(poll_timeout));

            if (stopped)
            {
                was_stopped = true;
                offsets_stored = 0;
                return false;
            }
            else if (rebalance_happened)
            {
                if (!new_messages.empty())
                {
                    // we have polled something just after rebalance.
                    // we will not use current batch, so we need to return to last commited position
                    // otherwise we will continue polling from that position
                    resetToLastCommitted("Rewind last poll after rebalance.");
                }

                offsets_stored = 0;
                return false;
            }

            if (new_messages.empty())
            {
                // While we wait for an assignment after subscription, we'll poll zero messages anyway.
                // If we're doing a manual select then it's better to get something after a wait, then immediate nothing.
                if (assignment.empty())
                {
                    waited_for_assignment += poll_timeout; // slightly innaccurate, but rough calculation is ok.
                    if (waited_for_assignment < MAX_TIME_TO_WAIT_FOR_ASSIGNMENT_MS)
                    {
                        continue;
                    }
                    else
                    {
                        LOG_TRACE(log, "Can't get assignment");
                        stalled = true;
                        return false;
                    }

                }
                else
                {
                    LOG_TRACE(log, "Stalled");
                    stalled = true;
                    return false;
                }
            }
            else
            {
                messages = std::move(new_messages);
                current = messages.begin();
                LOG_TRACE(log, "Polled batch of {} messages. Offset position: {}", messages.size(), consumer->get_offsets_position(consumer->get_assignment()));
                break;
            }
        }
    }

    if (auto err = current->get_error())
    {
        ++current;

        // TODO: should throw exception instead
        LOG_ERROR(log, "Consumer error: {}", err);
        return false;
    }

    // XXX: very fishy place with const casting.
    auto * new_position = reinterpret_cast<char *>(const_cast<unsigned char *>(current->get_payload().get_data()));
    BufferBase::set(new_position, current->get_payload().get_size(), 0);
    allowed = false;

    ++current;

    return true;
}

void ReadBufferFromKafkaConsumer::storeLastReadMessageOffset()
{
    if (!stalled && !was_stopped && !rebalance_happened)
    {
        consumer->store_offset(*(current - 1));
        ++offsets_stored;
    }
}

}
