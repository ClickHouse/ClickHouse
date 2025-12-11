#include <Storages/Kafka/KafkaConsumer2.h>

#include <iterator>
#include <IO/ReadBufferFromMemory.h>
#include <Storages/Kafka/StorageKafkaUtils.h>
#include <cppkafka/cppkafka.h>
#include <cppkafka/exceptions.h>
#include <cppkafka/topic_partition.h>
#include <cppkafka/topic_partition_list.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <Common/CurrentMetrics.h>
#include <Common/DateLUT.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>


namespace ProfileEvents
{
extern const Event KafkaMessagesPolled;
extern const Event KafkaCommitFailures;
extern const Event KafkaCommits;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
};

using namespace std::chrono_literals;
static constexpr auto EVENT_POLL_TIMEOUT = 50ms;
static constexpr auto DRAIN_TIMEOUT_MS = 5000ms;
static constexpr auto GET_KAFKA_METADATA_TIMEOUT_MS = 1000ms;

bool KafkaConsumer2::TopicPartition::operator<(const TopicPartition & other) const
{
    return std::tie(topic, partition_id) < std::tie(other.topic, other.partition_id);
}

KafkaConsumer2::KafkaConsumer2(
    LoggerPtr log_,
    size_t max_batch_size,
    size_t poll_timeout_,
    const std::atomic<bool> & stopped_,
    const Names & topics_,
    size_t skip_bytes_)
    : exceptions_buffer(EXCEPTIONS_DEPTH)
    , log(log_)
    , batch_size(max_batch_size)
    , poll_timeout(poll_timeout_)
    , skip_bytes(skip_bytes_)
    , stopped(stopped_)
    , current(messages.begin())
    , topics(topics_)
{
}

KafkaConsumer2::~KafkaConsumer2()
{
    if (!consumer)
        return;

    moveConsumer();
}

void KafkaConsumer2::createConsumer(cppkafka::Configuration consumer_config)
{
    if (consumer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "KafkaConsumer2::createConsumer called when consumer already exists");

    if (consumer_config.get("statistics.interval.ms") != "0")
    {
        consumer_config.set_stats_callback(
            [weak_this = weak_from_this()](cppkafka::KafkaHandleBase &, const std::string & stat_json)
            {
                auto shared_this = weak_this.lock();
                if (!shared_this)
                    return;

                std::lock_guard<std::mutex> lock(shared_this->rdkafka_stat_mutex);
                shared_this->rdkafka_stat = stat_json;
            });
    }
    // Create a consumer and subscribe to topics
    consumer = std::make_shared<cppkafka::Consumer>(std::move(consumer_config));
    consumer->set_destroy_flags(RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE);
}

CppKafkaConsumerPtr && KafkaConsumer2::moveConsumer()
{
    // messages and queues should be destroyed before consumer
    cleanQueuesAndMessages();
    StorageKafkaUtils::consumerStopWithoutRebalance(
        *consumer,
        DRAIN_TIMEOUT_MS,
        log,
        [this](const cppkafka::Error & err) { IKafkaExceptionInfoSink::setExceptionInfo(err, /* with_stacktrace = */ true); });
    return std::move(consumer);
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

void KafkaConsumer2::updateOffsets(TopicPartitionOffsets && topic_partition_offsets)
{
    cppkafka::TopicPartitionList original_topic_partitions;
    original_topic_partitions.reserve(topic_partition_offsets.size());
    std::transform(
        std::move_iterator(topic_partition_offsets.begin()),
        std::move_iterator(topic_partition_offsets.end()),
        std::back_inserter(original_topic_partitions),
        [](TopicPartitionOffset && tp) { return cppkafka::TopicPartition{std::move(tp.topic), tp.partition_id, tp.offset}; });
    initializeQueues(original_topic_partitions);
    stalled_status = StalledStatus::NOT_STALLED;
}

void KafkaConsumer2::cleanQueuesAndMessages()
{
    queues.clear();
    messages.clear();
    current = messages.end();
}

void KafkaConsumer2::initializeQueues(const cppkafka::TopicPartitionList & topic_partitions)
{
    cleanQueuesAndMessages();
    // cppkafka itself calls assign(), but in order to detach the queues here we have to do the assignment manually.
    // Later on we have to reassign the topic partitions with correct offsets.
    consumer->assign(topic_partitions);
    for (const auto & topic_partition : topic_partitions)
        // This will also detach the partition queues from the consumer, thus the messages won't be forwarded without
        // attaching them manually
        queues.emplace(
            TopicPartition{topic_partition.get_topic(), topic_partition.get_partition()}, consumer->get_partition_queue(topic_partition));
}

// it does the poll when needed
ReadBufferPtr KafkaConsumer2::consume(const TopicPartition & topic_partition, const std::optional<uint64_t> & message_count)
{
    resetIfStopped();

    // TODO: `polledDataUnusable` should be renamed to `clearMessagesIfNotSameTopicPartition` and it should be okay to
    // continue without returning nullptr.
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
        LOG_TRACE(log, "Batch size {}", batch_size);
        const auto messages_to_pull = message_count.value_or(batch_size);
        /// Don't drop old messages immediately, since we may need them for virtual columns.
        auto new_messages = queue_to_poll_from.consume_batch(messages_to_pull, std::chrono::milliseconds(poll_timeout));
        num_messages_read += new_messages.size();

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

void KafkaConsumer2::commit(const TopicPartitionOffset & topic_partition_offset)
{
    static constexpr auto max_retries = 5;
    bool committed = false;

    LOG_TEST(
        log,
        "Trying to commit offset {} to Kafka for topic-partition [{}:{}]",
        topic_partition_offset.offset,
        topic_partition_offset.topic,
        topic_partition_offset.partition_id);

    const auto topic_partition_list = std::vector{cppkafka::TopicPartition{
        topic_partition_offset.topic,
        topic_partition_offset.partition_id,
        topic_partition_offset.offset,
    }};

    for (auto try_count = 0; try_count < max_retries && !committed; ++try_count)
    {
        try
        {
            // See https://github.com/edenhill/librdkafka/issues/1470
            // broker may reject commit if during offsets.commit.timeout.ms (5000 by default), there were not enough
            // replicas available for the __consumer_offsets topic. also some other temporary issues like client-server
            // connectivity problems are possible

            consumer->commit(topic_partition_list);
            committed = true;
            LOG_INFO(
                log,
                "Committed offset {} to Kafka for topic-partition [{}:{}]",
                topic_partition_offset.offset,
                topic_partition_offset.topic,
                topic_partition_offset.partition_id);
        }
        catch (const cppkafka::HandleException & e)
        {
            // If there were actually no offsets to commit, return. Retrying won't solve anything here. Furthermore, we
            // don't consider failing to commit as an error, because Kafka is not the primary store for committed
            // offsets.
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

KafkaConsumer2::TopicPartitionOffsets KafkaConsumer2::getAllTopicPartitionOffsets() const
{
    std::unordered_set<String> topics_set;
    topics_set.insert(topics.begin(), topics.end());
    TopicPartitionOffsets topic_partition_offsets;

    try
    {
        auto metadata = consumer->get_metadata(true, GET_KAFKA_METADATA_TIMEOUT_MS);
        for (const auto & topic_metadata : metadata.get_topics())
        {
            if (!topics_set.contains(topic_metadata.get_name()))
                continue;

            for (const auto & partition_metadata : topic_metadata.get_partitions())
            {
                topic_partition_offsets.emplace_back(
                    TopicPartitionOffset{{topic_metadata.get_name(), static_cast<int32_t>(partition_metadata.get_id())}, INVALID_OFFSET});
            }
        }
    } catch (const cppkafka::HandleException & e)
    {
        LOG_ERROR(log, "Exception during get topic partitions from Kafka: {}", e.what());
    }

    return topic_partition_offsets;
}

KafkaConsumer2::Stat KafkaConsumer2::getStat() const
{
    CurrentOffsetsMap current_offsets;
    cppkafka::TopicPartitionList cppkafka_offsets;
    std::string consumer_id{};

    if (consumer)
    {
        const auto cpp_assignments = consumer->get_assignment();
        cppkafka_offsets = consumer->get_offsets_position(cpp_assignments);
        consumer_id = consumer->get_member_id();
    }

    for (const auto & tpoffset : cppkafka_offsets)
        current_offsets.emplace(TopicPartition{tpoffset.get_topic(), tpoffset.get_partition()}, tpoffset.get_offset());

    return {
        .current_offsets = std::move(current_offsets),
        .consumer_id = std::move(consumer_id),
        .rdkafka_stat = std::invoke(
            [&]()
            {
                std::lock_guard<std::mutex> lock(rdkafka_stat_mutex);
                return rdkafka_stat;
            }),
        .num_messages_read = num_messages_read.load(),
        .exceptions_buffer = std::invoke(
            [&]()
            {
                std::lock_guard<std::mutex> lock(exception_mutex);
                return exceptions_buffer;
            }),
    };
}

void KafkaConsumer2::setExceptionInfo(const std::string & text, bool with_stacktrace)
{
    std::string enriched_text = text;

    if (with_stacktrace)
    {
        if (!enriched_text.ends_with('\n'))
            enriched_text.append(1, '\n');
        enriched_text.append(StackTrace().toString());
    }

    std::lock_guard<std::mutex> lock(exception_mutex);
    exceptions_buffer.push_back({std::move(enriched_text), timeInSeconds(std::chrono::system_clock::now())});
}

ReadBufferPtr KafkaConsumer2::getNextMessage()
{
    while (current != messages.end())
    {
        const auto * data = current->get_payload().get_data();
        size_t size = current->get_payload().get_size();
        ++current;

        // `data` can be nullptr on case of the Kafka message has empty payload
        if (data && size >= skip_bytes)
            return std::make_shared<ReadBufferFromMemory>(data + skip_bytes, size - skip_bytes);
    }

    return nullptr;
}

void KafkaConsumer2::filterMessageErrors()
{
    assert(current == messages.begin());

    StorageKafkaUtils::eraseMessageErrors(
        messages,
        log,
        [this](const cppkafka::Error & err) { IKafkaExceptionInfoSink::setExceptionInfo(err, /* with_stacktrace = */ true); });
    current = messages.begin();
}

void KafkaConsumer2::resetIfStopped()
{
    if (stopped)
    {
        stalled_status = StalledStatus::CONSUMER_STOPPED;
    }
}

std::size_t KafkaConsumer2::TopicPartitionHash::operator()(const TopicPartition & tp) const
{
    SipHash s;
    s.update(tp.topic);
    s.update(tp.partition_id);
    return s.get64();
}

}
