#include <fmt/ranges.h>
#include <Storages/Kafka/KafkaConsumer.h>
#include <IO/ReadBufferFromMemory.h>

#include <Common/logger_useful.h>

#include <cppkafka/cppkafka.h>
#include <boost/algorithm/string/join.hpp>
#include <algorithm>

#include <Common/CurrentMetrics.h>
#include <Storages/Kafka/StorageKafkaUtils.h>
#include <Common/ProfileEvents.h>
#include <base/defines.h>

namespace CurrentMetrics
{
    extern const Metric KafkaAssignedPartitions;
    extern const Metric KafkaConsumersWithAssignment;
}

namespace ProfileEvents
{
extern const Event KafkaRebalanceRevocations;
extern const Event KafkaRebalanceAssignments;
extern const Event KafkaRebalanceErrors;
extern const Event KafkaMessagesPolled;
extern const Event KafkaCommitFailures;
extern const Event KafkaCommits;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMMIT_OFFSET;
}

using namespace std::chrono_literals;
const auto MAX_TIME_TO_WAIT_FOR_ASSIGNMENT_MS = 15000;
const std::size_t POLL_TIMEOUT_WO_ASSIGNMENT_MS = 50;
const auto DRAIN_TIMEOUT_MS = 5000ms;


KafkaConsumer::KafkaConsumer(
    LoggerPtr log_,
    size_t max_batch_size,
    size_t poll_timeout_,
    bool intermediate_commit_,
    const std::atomic<bool> & stopped_,
    const Names & _topics)
    : log(log_)
    , batch_size(max_batch_size)
    , poll_timeout(poll_timeout_)
    , intermediate_commit(intermediate_commit_)
    , stopped(stopped_)
    , current(messages.begin())
    , topics(_topics)
    , exceptions_buffer(EXCEPTIONS_DEPTH)
{
}

void KafkaConsumer::createConsumer(cppkafka::Configuration consumer_config)
{
    chassert(!consumer.get());

    /// Using this should be safe, since cppkafka::Consumer can poll messages
    /// (including statistics, which will trigger the callback below) only via
    /// KafkaConsumer.
    if (consumer_config.get("statistics.interval.ms") != "0")
    {
        consumer_config.set_stats_callback([this](cppkafka::KafkaHandleBase &, const std::string & stat_json)
        {
            setRDKafkaStat(stat_json);
        });
    }
    consumer = std::make_shared<cppkafka::Consumer>(consumer_config);
    consumer->set_destroy_flags(RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);

    // called (synchronously, during poll) when we enter the consumer group
    consumer->set_assignment_callback([this](const cppkafka::TopicPartitionList & topic_partitions)
    {
        CurrentMetrics::add(CurrentMetrics::KafkaAssignedPartitions, topic_partitions.size());
        ProfileEvents::increment(ProfileEvents::KafkaRebalanceAssignments);

        if (topic_partitions.empty())
        {
            LOG_INFO(log, "Got empty assignment: Not enough partitions in the topic for all consumers?");
        }
        else
        {
            LOG_TRACE(log, "Topics/partitions assigned: {}", topic_partitions);
            CurrentMetrics::add(CurrentMetrics::KafkaConsumersWithAssignment, 1);
        }

        assignment = topic_partitions;
        num_rebalance_assignments++;
    });

    // called (synchronously, during poll) when we leave the consumer group
    consumer->set_revocation_callback([this](const cppkafka::TopicPartitionList & topic_partitions)
    {
        CurrentMetrics::sub(CurrentMetrics::KafkaAssignedPartitions, topic_partitions.size());
        ProfileEvents::increment(ProfileEvents::KafkaRebalanceRevocations);

        // Rebalance is happening now, and now we have a chance to finish the work
        // with topics/partitions we were working with before rebalance
        LOG_TRACE(log, "Rebalance initiated. Revoking partitions: {}", topic_partitions);

        if (!topic_partitions.empty())
        {
            CurrentMetrics::sub(CurrentMetrics::KafkaConsumersWithAssignment, 1);
        }

        // we can not flush data to target from that point (it is pulled, not pushed)
        // so the best we can now it to
        // 1) repeat last commit in sync mode (async could be still in queue, we need to be sure is is properly committed before rebalance)
        // 2) stop / brake the current reading:
        //     * clean buffered non-commited messages
        //     * set flag / flush

        cleanUnprocessed();

        stalled_status = REBALANCE_HAPPENED;
        last_rebalance_timestamp_usec = static_cast<UInt64>(Poco::Timestamp().epochTime());

        assignment.reset();
        waited_for_assignment = 0;

        // for now we use slower (but reliable) sync commit in main loop, so no need to repeat
        // try
        // {
        //     consumer->commit();
        // }
        // catch (cppkafka::HandleException & e)
        // {
        //     LOG_WARNING(log, "Commit error: {}", e.what());
        // }
        num_rebalance_revocations++;
    });

    consumer->set_rebalance_error_callback([this](cppkafka::Error err)
    {
        LOG_ERROR(log, "Rebalance error: {}", err);
        ProfileEvents::increment(ProfileEvents::KafkaRebalanceErrors);
        setExceptionInfo(err);
    });
}

ConsumerPtr && KafkaConsumer::moveConsumer()
{
    cleanUnprocessed();
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
    return std::move(consumer);
}

KafkaConsumer::~KafkaConsumer()
{
    if (!consumer)
        return;

    cleanUnprocessed();
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
void KafkaConsumer::drain()
{
    StorageKafkaUtils::drainConsumer(*consumer, DRAIN_TIMEOUT_MS, log, [this](const cppkafka::Error & err) { setExceptionInfo(err); });
}

void KafkaConsumer::commit()
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
        bool committed = false;

        while (!committed && max_retries > 0)
        {
            try
            {
                // See https://github.com/edenhill/librdkafka/issues/1470
                // broker may reject commit if during offsets.commit.timeout.ms (5000 by default),
                // there were not enough replicas available for the __consumer_offsets topic.
                // also some other temporary issues like client-server connectivity problems are possible
                consumer->commit();
                committed = true;
                print_offsets("Committed offset", consumer->get_offsets_committed(consumer->get_assignment()));
                last_commit_timestamp_usec = static_cast<UInt64>(Poco::Timestamp().epochTime());
                num_commits += 1;
            }
            catch (const cppkafka::HandleException & e)
            {
                // If there were actually no offsets to commit, return. Retrying won't solve
                // anything here
                if (e.get_error() == RD_KAFKA_RESP_ERR__NO_OFFSET)
                    committed = true;
                else
                {
                    LOG_ERROR(log, "Exception during commit attempt: {}", e.what());
                    setExceptionInfo(e.what());
                }
            }
            --max_retries;
        }

        if (!committed)
        {
            // TODO: insert atomicity / transactions is needed here (possibility to rollback, on 2 phase commits)
            ProfileEvents::increment(ProfileEvents::KafkaCommitFailures);
            throw Exception(ErrorCodes::CANNOT_COMMIT_OFFSET,
                            "All commit attempts failed. Last block was already written to target table(s), "
                            "but was not committed to Kafka.");
        }
        else
        {
            ProfileEvents::increment(ProfileEvents::KafkaCommits);
        }

    }
    else
    {
        LOG_TRACE(log, "Nothing to commit.");
    }

    offsets_stored = 0;
}

void KafkaConsumer::subscribe()
{
    LOG_TRACE(log, "Already subscribed to topics: [{}]", boost::algorithm::join(consumer->get_subscription(), ", "));

    if (assignment.has_value())
    {
        LOG_TRACE(log, "Already assigned to: {}", assignment.value());
    }
    else
    {
        LOG_TRACE(log, "No assignment");
    }


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

    cleanUnprocessed();

    // we can reset any flags (except of CONSUMER_STOPPED) before attempt of reading new block of data
    if (stalled_status != CONSUMER_STOPPED)
        stalled_status = NO_MESSAGES_RETURNED;
}

void KafkaConsumer::cleanUnprocessed()
{
    messages.clear();
    current = messages.begin();
    offsets_stored = 0;
}

void KafkaConsumer::unsubscribe()
{
    LOG_TRACE(log, "Re-joining claimed consumer after failure");
    cleanUnprocessed();

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
        LOG_ERROR(log, "Exception from KafkaConsumer::unsubscribe: {}", e.what());
    }

}


void KafkaConsumer::resetToLastCommitted(const char * msg)
{
    if (!assignment.has_value() || assignment->empty())
    {
        LOG_TRACE(log, "Not assigned. Can't reset to last committed position.");
        return;
    }
    auto committed_offset = consumer->get_offsets_committed(consumer->get_assignment());
    consumer->assign(committed_offset);
    LOG_TRACE(log, "{} Returned to committed position: {}", msg, committed_offset);
}

// it do the poll when needed
ReadBufferPtr KafkaConsumer::consume()
{
    resetIfStopped();

    if (polledDataUnusable())
        return nullptr;

    if (hasMorePolledMessages())
        return getNextMessage();

    if (intermediate_commit)
        commit();

    while (true)
    {
        stalled_status = NO_MESSAGES_RETURNED;

        // we already wait enough for assignment in the past,
        // let's make polls shorter and not block other consumer
        // which can work successfully in parallel
        // POLL_TIMEOUT_WO_ASSIGNMENT_MS (50ms) is 100% enough just to check if we got assignment
        //  (see https://github.com/ClickHouse/ClickHouse/issues/11218)
        auto actual_poll_timeout_ms = (waited_for_assignment >= MAX_TIME_TO_WAIT_FOR_ASSIGNMENT_MS)
                        ? std::min(POLL_TIMEOUT_WO_ASSIGNMENT_MS,poll_timeout)
                        : poll_timeout;

        /// Don't drop old messages immediately, since we may need them for virtual columns.
        auto new_messages = consumer->poll_batch(batch_size,
                            std::chrono::milliseconds(actual_poll_timeout_ms));
        last_poll_timestamp_usec = static_cast<UInt64>(Poco::Timestamp().epochTime());
        num_messages_read += new_messages.size();

        resetIfStopped();
        if (stalled_status == CONSUMER_STOPPED)
        {
            return nullptr;
        }
        else if (stalled_status == REBALANCE_HAPPENED)
        {
            if (!new_messages.empty())
            {
                // we have polled something just after rebalance.
                // we will not use current batch, so we need to return to last committed position
                // otherwise we will continue polling from that position
                resetToLastCommitted("Rewind last poll after rebalance.");
            }
            return nullptr;
        }

        if (new_messages.empty())
        {
            // While we wait for an assignment after subscription, we'll poll zero messages anyway.
            // If we're doing a manual select then it's better to get something after a wait, then immediate nothing.
            if (!assignment.has_value())
            {
                waited_for_assignment += poll_timeout; // slightly inaccurate, but rough calculation is ok.
                if (waited_for_assignment < MAX_TIME_TO_WAIT_FOR_ASSIGNMENT_MS)
                {
                    continue;
                }
                else
                {
                    LOG_WARNING(log, "Can't get assignment. Will keep trying.");
                    stalled_status = NO_ASSIGNMENT;
                    return nullptr;
                }
            }
            else if (assignment->empty())
            {
                LOG_TRACE(log, "Empty assignment.");
                return nullptr;
            }
            else
            {
                LOG_TRACE(log, "Stalled");
                return nullptr;
            }
        }
        else
        {
            messages = std::move(new_messages);
            current = messages.begin();
            LOG_TRACE(log, "Polled batch of {} messages. Offsets position: {}",
                messages.size(), consumer->get_offsets_position(consumer->get_assignment()));
            break;
        }
    }

    filterMessageErrors();
    if (current == messages.end())
    {
        LOG_ERROR(log, "Only errors left");
        stalled_status = ERRORS_RETURNED;
        return nullptr;
    }

    ProfileEvents::increment(ProfileEvents::KafkaMessagesPolled, messages.size());

    stalled_status = NOT_STALLED;
    return getNextMessage();
}

ReadBufferPtr KafkaConsumer::getNextMessage()
{
    if (current == messages.end())
        return nullptr;

    const auto * data = current->get_payload().get_data();
    size_t size = current->get_payload().get_size();
    ++current;

    if (data)
        return std::make_shared<ReadBufferFromMemory>(data, size);

    return getNextMessage();
}

void KafkaConsumer::filterMessageErrors()
{
    assert(current == messages.begin());

    StorageKafkaUtils::eraseMessageErrors(messages, log, [this](const cppkafka::Error & err) { setExceptionInfo(err); });
    current = messages.begin();
}

void KafkaConsumer::resetIfStopped()
{
    // we can react on stop only during fetching data
    // after block is formed (i.e. during copying data to MV / committing)  we ignore stop attempts
    if (stopped)
    {
        stalled_status = CONSUMER_STOPPED;
        cleanUnprocessed();
    }
}


void KafkaConsumer::storeLastReadMessageOffset()
{
    if (!isStalled())
    {
        consumer->store_offset(*(current - 1));
        ++offsets_stored;
    }
}

void KafkaConsumer::setExceptionInfo(const cppkafka::Error & err, bool with_stacktrace)
{
    setExceptionInfo(err.to_string(), with_stacktrace);
}

void KafkaConsumer::setExceptionInfo(const std::string & text, bool with_stacktrace)
{
    std::string enriched_text = text;

    if (with_stacktrace)
    {
        enriched_text.append(StackTrace().toString());
    }

    std::lock_guard<std::mutex> lock(exception_mutex);
    exceptions_buffer.push_back({enriched_text, static_cast<UInt64>(Poco::Timestamp().epochTime())});
}

std::string KafkaConsumer::getMemberId() const
{
    if (!consumer)
        return "";

    return consumer->get_member_id();
}

KafkaConsumer::Stat KafkaConsumer::getStat() const
{
    KafkaConsumer::Stat::Assignments assignments;
    cppkafka::TopicPartitionList cpp_assignments;
    cppkafka::TopicPartitionList cpp_offsets;

    if (consumer)
    {
        cpp_assignments = consumer->get_assignment();
        cpp_offsets = consumer->get_offsets_position(cpp_assignments);
    }

    for (size_t num = 0; num < cpp_assignments.size(); ++num)
    {
        assignments.push_back({
            cpp_assignments[num].get_topic(),
            cpp_assignments[num].get_partition(),
            cpp_offsets[num].get_offset(),
        });
    }

    return {
        .consumer_id = getMemberId(),
        .assignments = std::move(assignments),
        .last_poll_time = last_poll_timestamp_usec.load(),
        .num_messages_read = num_messages_read.load(),

        .last_commit_timestamp_usec = last_commit_timestamp_usec.load(),
        .last_rebalance_timestamp_usec = last_rebalance_timestamp_usec.load(),
        .num_commits = num_commits.load(),
        .num_rebalance_assignments = num_rebalance_assignments.load(),
        .num_rebalance_revocations = num_rebalance_revocations.load(),
        .exceptions_buffer = [&]()
        {
            std::lock_guard<std::mutex> lock(exception_mutex);
            return exceptions_buffer;
        }(),
        .in_use = in_use.load(),
        .last_used_usec = last_used_usec.load(),
        .rdkafka_stat = [&]()
        {
            std::lock_guard<std::mutex> lock(rdkafka_stat_mutex);
            return rdkafka_stat;
        }(),
    };
}

}
