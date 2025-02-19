#include <Storages/Kafka/KafkaConsumer2.h>

#include <fmt/ranges.h>
#include <cppkafka/exceptions.h>
#include <cppkafka/topic_partition.h>
#include <cppkafka/cppkafka.h>
#include <cppkafka/topic_partition_list.h>
#include <fmt/ostream.h>
#include <boost/algorithm/string/join.hpp>

#include <IO/ReadBufferFromMemory.h>
#include <Storages/Kafka/StorageKafkaUtils.h>
#include <Common/logger_useful.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>

#include <algorithm>
#include <iterator>


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

using namespace std::chrono_literals;
static constexpr auto EVENT_POLL_TIMEOUT = 50ms;
static constexpr auto DRAIN_TIMEOUT_MS = 5000ms;


bool KafkaConsumer2::TopicPartition::operator<(const TopicPartition & other) const
{
    return std::tie(topic, partition_id, offset) < std::tie(other.topic, other.partition_id, other.offset);
}


KafkaConsumer2::KafkaConsumer2(
    ConsumerPtr consumer_,
    LoggerPtr log_,
    size_t max_batch_size,
    size_t poll_timeout_,
    const std::atomic<bool> & stopped_,
    const Names & topics_)
    : consumer(consumer_)
    , log(log_)
    , batch_size(max_batch_size)
    , poll_timeout(poll_timeout_)
    , stopped(stopped_)
    , current(messages.begin())
    , topics(topics_)
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
                assignment->push_back(
                    TopicPartition{topic_partition.get_topic(), topic_partition.get_partition(), topic_partition.get_offset()});
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
        });

    consumer->set_rebalance_error_callback(
        [this](cppkafka::Error err)
        {
            LOG_ERROR(log, "Rebalance error: {}", err);
            ProfileEvents::increment(ProfileEvents::KafkaRebalanceErrors);
        });
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
    StorageKafkaUtils::drainConsumer(*consumer, DRAIN_TIMEOUT_MS, log);
}

void KafkaConsumer2::pollEvents()
{
    static constexpr auto max_tries = 5;
    for (auto i = 0; i < max_tries; ++i)
    {
        auto msg = consumer->poll(EVENT_POLL_TIMEOUT);
        if (!msg)
            return;
        // All the partition queues are detached, so the consumer shouldn't be able to poll any real messages
        const auto err = msg.get_error();
        chassert(RD_KAFKA_RESP_ERR_NO_ERROR != err.get_error() && "Consumer returned a message when it was not expected");
        LOG_ERROR(log, "Consumer received error while polling events, code {}, error '{}'", err.get_error(), err.to_string());
    }
};

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
        [](const TopicPartition & tp)
        {
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
ReadBufferPtr KafkaConsumer2::consume(const TopicPartition & topic_partition, const std::optional<int64_t> & message_count)
{
    resetIfStopped();

    if (polledDataUnusable(topic_partition))
        return nullptr;

    if (hasMorePolledMessages())
    {
        if (auto next_message = getNextMessage(); next_message)
            return next_message;
    }

    while (true)
    {
        stalled_status = StalledStatus::NO_MESSAGES_RETURNED;

        auto & queue_to_poll_from = queues.at(topic_partition);
        LOG_TRACE(log, "Batch size {}, offset {}", batch_size, topic_partition.offset);
        const auto messages_to_pull = message_count.value_or(batch_size);
        /// Don't drop old messages immediately, since we may need them for virtual columns.
        auto new_messages = queue_to_poll_from.consume_batch(messages_to_pull, std::chrono::milliseconds(poll_timeout));

        resetIfStopped();
        if (stalled_status == StalledStatus::CONSUMER_STOPPED)
        {
            return nullptr;
        }

        if (new_messages.empty())
        {
            LOG_TRACE(log, "Stalled");
            return nullptr;
        }

        messages = std::move(new_messages);
        current = messages.begin();
        LOG_TRACE(
            log,
            "Polled batch of {} messages. Offsets position: {}",
            messages.size(),
            consumer->get_offsets_position(consumer->get_assignment()));
        break;
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

void KafkaConsumer2::commit(const TopicPartition & topic_partition)
{
    static constexpr auto max_retries = 5;
    bool committed = false;

    LOG_TEST(
        log,
        "Trying to commit offset {} to Kafka for topic-partition [{}:{}]",
        topic_partition.offset,
        topic_partition.topic,
        topic_partition.partition_id);

    const auto topic_partition_list = std::vector{cppkafka::TopicPartition{
        topic_partition.topic,
        topic_partition.partition_id,
        topic_partition.offset,
    }};
    for (auto try_count = 0; try_count < max_retries && !committed; ++try_count)
    {
        try
        {
            // See https://github.com/edenhill/librdkafka/issues/1470
            // broker may reject commit if during offsets.commit.timeout.ms (5000 by default),
            // there were not enough replicas available for the __consumer_offsets topic.
            // also some other temporary issues like client-server connectivity problems are possible

            consumer->commit(topic_partition_list);
            committed = true;
            LOG_INFO(
                log,
                "Committed offset {} to Kafka for topic-partition [{}:{}]",
                topic_partition.offset,
                topic_partition.topic,
                topic_partition.partition_id);
        }
        catch (const cppkafka::HandleException & e)
        {
            // If there were actually no offsets to commit, return. Retrying won't solve
            // anything here
            if (e.get_error() == RD_KAFKA_RESP_ERR__NO_OFFSET)
                committed = true;
            else
                LOG_ERROR(log, "Exception during attempt to commit to Kafka: {}", e.what());
        }
    }

    if (!committed)
    {
        // The failure is not the biggest issue, it only counts when a table is dropped and recreated, otherwise the offsets are taken from keeper.
        ProfileEvents::increment(ProfileEvents::KafkaCommitFailures);
        LOG_ERROR(log, "All commit attempts failed");
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::KafkaCommits);
    }
}

void KafkaConsumer2::subscribeIfNotSubscribedYet()
{
    if (likely(is_subscribed))
        return;

    consumer->subscribe(topics);
    is_subscribed = true;
    LOG_DEBUG(log, "Subscribed.");
}

ReadBufferPtr KafkaConsumer2::getNextMessage()
{
    while (current != messages.end())
    {
        const auto * data = current->get_payload().get_data();
        size_t size = current->get_payload().get_size();
        ++current;

        // `data` can be nullptr on case of the Kafka message has empty payload
        if (data)
            return std::make_shared<ReadBufferFromMemory>(data, size);
    }

    return nullptr;
}

void KafkaConsumer2::filterMessageErrors()
{
    assert(current == messages.begin());

    StorageKafkaUtils::eraseMessageErrors(messages, log);
    current = messages.begin();
}

void KafkaConsumer2::resetIfStopped()
{
    if (stopped)
    {
        stalled_status = StalledStatus::CONSUMER_STOPPED;
    }
}
}
