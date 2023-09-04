// Needs to go first because its partial specialization of fmt::formatter
// should be defined before any instantiation
#include <cppkafka/exceptions.h>
#include <cppkafka/topic_partition.h>
#include <cppkafka/topic_partition_list.h>
#include <fmt/ostream.h>

#include <IO/ReadBufferFromMemory.h>
#include <Storages/Kafka/KafkaConsumer2.h>

#include <Common/logger_useful.h>

#include <algorithm>
#include <iterator>
#include <boost/algorithm/string/join.hpp>
#include <cppkafka/cppkafka.h>

#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include "base/scope_guard.h"

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
extern const Event KafkaConsumerErrors;
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


bool KafkaConsumer2::TopicPartition::operator<(const TopicPartition & other) const
{
    return std::tie(topic, partition_id, offset) < std::tie(other.topic, other.partition_id, other.offset);
}


KafkaConsumer2::KafkaConsumer2(
    ConsumerPtr consumer_,
    Poco::Logger * log_,
    size_t max_batch_size,
    size_t poll_timeout_,
    bool intermediate_commit_,
    const std::atomic<bool> & stopped_,
    const Names & _topics)
    : consumer(consumer_)
    , log(log_)
    , batch_size(max_batch_size)
    , poll_timeout(poll_timeout_)
    , intermediate_commit(intermediate_commit_)
    , stopped(stopped_)
    , current(messages.begin())
    , topics(_topics)
{
    // called (synchronously, during poll) when we enter the consumer group
    consumer->set_assignment_callback(
        [this](const cppkafka::TopicPartitionList & topic_partitions)
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

            chassert(!assignment.has_value());

            assignment.emplace();
            assignment->reserve(topic_partitions.size());
            needs_offset_update = true;
            for (const auto & topic_partition : topic_partitions)
            {
                assignment->push_back(TopicPartition{topic_partition.get_topic(), topic_partition.get_partition(), INVALID_OFFSET});
            }

            // We need to initialize the queues here in order to detach them from the consumer queue. Otherwise `pollEvents` might eventually poll actual messages also.
            initializeQueues(topic_partitions);
        });

    // called (synchronously, during poll) when we leave the consumer group
    consumer->set_revocation_callback(
        [this](const cppkafka::TopicPartitionList & topic_partitions)
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

            assignment.reset();
            queues.clear();
            needs_offset_update = true;
            waited_for_assignment = 0;
        });

    consumer->set_rebalance_error_callback(
        [this](cppkafka::Error err)
        {
            LOG_ERROR(log, "Rebalance error: {}", err);
            ProfileEvents::increment(ProfileEvents::KafkaRebalanceErrors);
        });

    consumer->subscribe(topics);
}

KafkaConsumer2::~KafkaConsumer2()
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
            drainConsumerQueue();
        }
    }
    catch (const cppkafka::HandleException & e)
    {
        LOG_ERROR(log, "Error while destructing consumer: {}", e.what());
    }
}

// Needed to drain rest of the messages / queued callback calls from the consumer after unsubscribe, otherwise consumer
// will hang on destruction. Partition queues doesn't have to be attached as events are not handled by those queues.
// see https://github.com/edenhill/librdkafka/issues/2077
//     https://github.com/confluentinc/confluent-kafka-go/issues/189 etc.
void KafkaConsumer2::drainConsumerQueue()
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
        if (std::chrono::duration_cast<std::chrono::milliseconds>(ts - start_time) > DRAIN_TIMEOUT_MS)
        {
            LOG_ERROR(log, "Timeout during draining.");
            break;
        }
    }
}

void KafkaConsumer2::pollEvents()
{
    // All the partition queues are detached, so the consumer shouldn't be able to poll any messages
    auto msg = consumer->poll(10ms);
    chassert(!msg && "Consumer returned a message when it was not expected");
};

KafkaConsumer2::TopicPartitionCounts KafkaConsumer2::getPartitionCounts() const
{
    TopicPartitionCounts result;
    try
    {
        auto metadata = consumer->get_metadata();
        auto topic_metadatas = metadata.get_topics();

        for (auto & topic_metadata : topic_metadatas)
        {
            if (const auto it = std::find(topics.begin(), topics.end(), topic_metadata.get_name()); it != topics.end())
            {
                result.push_back({topic_metadata.get_name(), topic_metadata.get_partitions().size()});
            }
        }
    }
    catch (cppkafka::HandleException & e)
    {
        chassert(e.what() != nullptr);
    }
    return result;
}

bool KafkaConsumer2::polledDataUnusable(const TopicPartition & topic_partition) const
{
    const auto different_topic_partition = current == messages.end()
        ? false
        : (current->get_topic() != topic_partition.topic || current->get_partition() != topic_partition.partition_id);
    return different_topic_partition;
}

KafkaConsumer2::TopicPartitions const * KafkaConsumer2::getKafkaAssignment() const
{
    if (assignment.has_value())
    {
        return &*assignment;
    }

    return nullptr;
}

void KafkaConsumer2::updateOffsets(const TopicPartitions & topic_partitions)
{
    cppkafka::TopicPartitionList original_topic_partitions;
    original_topic_partitions.reserve(topic_partitions.size());
    std::transform(
        topic_partitions.begin(),
        topic_partitions.end(),
        std::back_inserter(original_topic_partitions),
        [](const TopicPartition & tp) {
            return cppkafka::TopicPartition{tp.topic, tp.partition_id, tp.offset};
        });
    initializeQueues(original_topic_partitions);
    needs_offset_update = false;
    stalled_status = StalledStatus::NOT_STALLED;
}

void KafkaConsumer2::initializeQueues(const cppkafka::TopicPartitionList & topic_partitions)
{
    queues.clear();
    messages.clear();
    current = messages.end();
    // cppkafka itself calls assign(), but in order to detach the queues here we have to do the assignment manually. Later on we have to reassign the topic partitions with correct offsets.
    consumer->assign(topic_partitions);
    for (const auto & topic_partition : topic_partitions)
        // This will also detach the partition queues from the consumer, thus the messages won't be forwarded without attaching them manually
        queues.emplace(
            TopicPartition{topic_partition.get_topic(), topic_partition.get_partition(), topic_partition.get_offset()},
            consumer->get_partition_queue(topic_partition));
}

// it do the poll when needed
ReadBufferPtr KafkaConsumer2::consume(const TopicPartition & topic_partition)
{
    resetIfStopped();

    if (polledDataUnusable(topic_partition))
        return nullptr;

    if (hasMorePolledMessages())
        return getNextMessage();


        // TODO(antaljanosbenjamin): check if we should poll new messages or not
    while (true)
    {
        stalled_status = StalledStatus::NO_MESSAGES_RETURNED;

        // we already wait enough for assignment in the past,
        // let's make polls shorter and not block other consumer
        // which can work successfully in parallel
        // POLL_TIMEOUT_WO_ASSIGNMENT_MS (50ms) is 100% enough just to check if we got assignment
        //  (see https://github.com/ClickHouse/ClickHouse/issues/11218)
        auto actual_poll_timeout_ms = (waited_for_assignment >= MAX_TIME_TO_WAIT_FOR_ASSIGNMENT_MS)
            ? std::min(POLL_TIMEOUT_WO_ASSIGNMENT_MS, poll_timeout)
            : poll_timeout;

        auto & queue_to_poll_from = queues[topic_partition];
        queue_to_poll_from.forward_to_queue(consumer->get_consumer_queue());
        SCOPE_EXIT({ queue_to_poll_from.disable_queue_forwarding(); });

        /// Don't drop old messages immediately, since we may need them for virtual columns.
        auto new_messages = consumer->poll_batch(batch_size, std::chrono::milliseconds(actual_poll_timeout_ms));

        resetIfStopped();
        if (stalled_status == StalledStatus::CONSUMER_STOPPED)
        {
            return nullptr;
        }

        if (new_messages.empty())
        {
            // While we wait for an assignment after subscription, we'll poll zero messages anyway.
            // If we're doing a manual select then it's better to get something after a wait, then immediate nothing.
            if (!assignment.has_value())
            {
                waited_for_assignment += poll_timeout; // slightly innaccurate, but rough calculation is ok.
                if (waited_for_assignment < MAX_TIME_TO_WAIT_FOR_ASSIGNMENT_MS)
                {
                    continue;
                }
                else
                {
                    LOG_WARNING(log, "Can't get assignment. Will keep trying.");
                    stalled_status = StalledStatus::NO_ASSIGNMENT;
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
            LOG_TRACE(
                log,
                "Polled batch of {} messages. Offsets position: {}",
                messages.size(),
                consumer->get_offsets_position(consumer->get_assignment()));
            break;
        }
    }

    filterMessageErrors();
    if (current == messages.end())
    {
        LOG_ERROR(log, "Only errors left");
        stalled_status = StalledStatus::ERRORS_RETURNED;
        return nullptr;
    }

    ProfileEvents::increment(ProfileEvents::KafkaMessagesPolled, messages.size());

    stalled_status = StalledStatus::NOT_STALLED;
    return getNextMessage();
}

ReadBufferPtr KafkaConsumer2::getNextMessage()
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

size_t KafkaConsumer2::filterMessageErrors()
{
    assert(current == messages.begin());

    size_t skipped = std::erase_if(
        messages,
        [this](auto & message)
        {
            if (auto error = message.get_error())
            {
                ProfileEvents::increment(ProfileEvents::KafkaConsumerErrors);
                LOG_ERROR(log, "Consumer error: {}", error);
                return true;
            }
            return false;
        });

    if (skipped)
        LOG_ERROR(log, "There were {} messages with an error", skipped);

    return skipped;
}

void KafkaConsumer2::resetIfStopped()
{
    // we can react on stop only during fetching data
    // after block is formed (i.e. during copying data to MV / committing)  we ignore stop attempts
    if (stopped)
    {
        stalled_status = StalledStatus::CONSUMER_STOPPED;
    }
}
}
