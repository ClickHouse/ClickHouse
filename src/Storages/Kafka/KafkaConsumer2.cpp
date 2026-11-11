#include <Storages/Kafka/KafkaConsumer2.h>

#include <IO/ReadBufferFromMemory.h>
#include <Storages/Kafka/Clients/Error.h>
#include <Storages/Kafka/Clients/Types.h>
#include <fmt/ranges.h>
#include <Common/CurrentMetrics.h>
#include <Common/DateLUT.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/StackTrace.h>
#include <Common/logger_useful.h>

#include <unordered_set>

namespace ProfileEvents
{
extern const Event KafkaMessagesPolled;
extern const Event KafkaCommitFailures;
extern const Event KafkaCommits;
extern const Event KafkaConsumerErrors;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
};

using namespace std::chrono_literals;
static constexpr auto GET_KAFKA_METADATA_TIMEOUT = 1000ms;
/// Offset resolution and commits are coordinator round trips with internal
/// retries; the old implementation gave commits five tries of librdkafka's
/// synchronous commit, which is in the same ballpark.
static constexpr auto OFFSET_RESOLUTION_TIMEOUT = 10000ms;
static constexpr auto COMMIT_TIMEOUT = 5000ms;

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
    const String & group_,
    size_t skip_bytes_)
    : exceptions_buffer(EXCEPTIONS_DEPTH)
    , log(log_)
    , batch_size(max_batch_size)
    , poll_timeout(poll_timeout_)
    , skip_bytes(skip_bytes_)
    , stopped(stopped_)
    , current(messages.begin())
    , topics(topics_)
    , group(group_)
{
}

/// The client owns no threads and runs no callbacks, so the destruction is
/// a plain synchronous teardown.
KafkaConsumer2::~KafkaConsumer2() = default;

void KafkaConsumer2::createConsumer(Kafka::ClientConfig consumer_config)
{
    if (client)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "KafkaConsumer2::createConsumer called when consumer already exists");

    client_id = consumer_config.client_id;
    client = std::make_unique<Kafka::Client>(std::move(consumer_config));
}

void KafkaConsumer2::updateOffsets(TopicPartitionOffsets && topic_partition_offsets)
{
    chassert(client);

    /// Offsets missing from Keeper resolve to the broker-committed offset of
    /// the group, and to the earliest offset when the group has none either.
    std::vector<Kafka::TopicPartition> unresolved;
    for (const auto & tpo : topic_partition_offsets)
        if (tpo.offset == INVALID_OFFSET)
            unresolved.push_back(Kafka::TopicPartition{tpo.topic, tpo.partition_id});

    CurrentOffsetsMap resolved;
    if (!unresolved.empty())
    {
        for (const auto & committed : client->fetchCommittedOffsets(group, unresolved, OFFSET_RESOLUTION_TIMEOUT))
            if (committed.offset >= 0)
                resolved.emplace(TopicPartition{committed.topic, committed.partition}, committed.offset);

        for (const auto & tp : unresolved)
        {
            if (resolved.contains(TopicPartition{tp.topic, tp.partition}))
                continue;
            const auto earliest = client->listOffset(tp, Kafka::OffsetSpec::earliest(), OFFSET_RESOLUTION_TIMEOUT);
            LOG_TRACE(
                log,
                "Topic-partition [{}:{}] has no committed offset in Keeper or Kafka, starting from the earliest offset {}",
                tp.topic,
                tp.partition,
                earliest);
            resolved.emplace(TopicPartition{tp.topic, tp.partition}, earliest);
        }
    }

    std::unordered_set<TopicPartition, TopicPartitionHash, TopicPartitionEquality> new_partitions;
    CurrentOffsetsMap new_offsets;
    for (const auto & tpo : topic_partition_offsets)
    {
        new_partitions.insert(tpo);
        const auto offset = tpo.offset == INVALID_OFFSET ? resolved.at(tpo) : tpo.offset;
        new_offsets.emplace(tpo, offset);
    }

    std::erase_if(readers, [&](const auto & entry) { return !new_partitions.contains(entry.first); });
    for (const auto & [tp, offset] : new_offsets)
    {
        if (const auto it = readers.find(tp); it != readers.end())
            it->second.seek(offset);
        else
            readers.try_emplace(tp, *client, Kafka::TopicPartition{tp.topic, tp.partition_id}, offset);
    }

    messages.clear();
    current = messages.end();
    {
        std::lock_guard lock(offsets_snapshot_mutex);
        offsets_snapshot = std::move(new_offsets);
    }
    stalled_status = StalledStatus::NOT_STALLED;
}

// it does the poll when needed
ReadBufferPtr KafkaConsumer2::consume(const TopicPartition & topic_partition, const std::optional<uint64_t> & message_count)
{
    resetIfStopped();
    if (stalled_status == StalledStatus::CONSUMER_STOPPED)
        return nullptr;

    // TODO: it should be okay to continue after clearing the leftover messages of another topic-partition
    // instead of returning nullptr.
    if (current != messages.end() && current_topic_partition != topic_partition)
        return nullptr;

    if (hasMorePolledMessages())
    {
        if (auto next_message = getNextMessage(); next_message)
            return next_message;
    }

    stalled_status = StalledStatus::NO_MESSAGES_RETURNED;

    const auto reader = readers.find(topic_partition);
    if (reader == readers.end())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "No reader for topic-partition [{}:{}]; `updateOffsets` must assign it first",
            topic_partition.topic,
            topic_partition.partition_id);

    LOG_TRACE(log, "Batch size {}", batch_size);
    /// The requested count may exceed the configured batch size (e.g. when a large intent is
    /// replayed), but a single pull must still respect `kafka_poll_max_batch_size`. The caller
    /// pulls the rest with further calls.
    const auto messages_to_pull = std::min<uint64_t>(message_count.value_or(batch_size), batch_size);

    Messages new_messages;
    try
    {
        new_messages = reader->second.poll(std::chrono::milliseconds(poll_timeout), messages_to_pull);
    }
    catch (const Kafka::Exception & e)
    {
        /// Fetch errors are transient from the engine's point of view (stale
        /// leaders and lost connections are retried inside the client already):
        /// report them and stall; the next cycle retries. This mirrors the old
        /// behavior where broker errors arrived as error messages in the batch.
        ProfileEvents::increment(ProfileEvents::KafkaConsumerErrors);
        LOG_ERROR(
            log,
            "Consumer error while polling topic-partition [{}:{}]: {}",
            topic_partition.topic,
            topic_partition.partition_id,
            e.message());
        setExceptionInfo(e.message(), /* with_stacktrace = */ false);
        stalled_status = StalledStatus::ERRORS_RETURNED;
        return nullptr;
    }

    num_messages_read += new_messages.size();

    resetIfStopped();
    if (stalled_status == StalledStatus::CONSUMER_STOPPED)
        return nullptr;

    if (new_messages.empty())
    {
        LOG_TRACE(log, "Stalled");
        return nullptr;
    }

    messages = std::move(new_messages);
    current = messages.begin();
    current_topic_partition = topic_partition;
    {
        std::lock_guard lock(offsets_snapshot_mutex);
        offsets_snapshot[topic_partition] = reader->second.position();
    }
    LOG_TRACE(
        log,
        "Polled batch of {} messages from [{}:{}]. Next position: {}",
        messages.size(),
        topic_partition.topic,
        topic_partition.partition_id,
        reader->second.position());

    ProfileEvents::increment(ProfileEvents::KafkaMessagesPolled, messages.size());

    stalled_status = StalledStatus::NOT_STALLED;
    return getNextMessage();
}

void KafkaConsumer2::commit(const TopicPartitionOffset & topic_partition_offset)
{
    chassert(client);
    static constexpr auto max_retries = 5;
    bool committed = false;

    LOG_TEST(
        log,
        "Trying to commit offset {} to Kafka for topic-partition [{}:{}]",
        topic_partition_offset.offset,
        topic_partition_offset.topic,
        topic_partition_offset.partition_id);

    const std::vector<Kafka::TopicPartitionOffset> offsets_to_commit{
        {.topic = topic_partition_offset.topic,
         .partition = topic_partition_offset.partition_id,
         .offset = topic_partition_offset.offset,
         .leader_epoch = -1,
         .metadata = {}}};

    for (auto try_count = 0; try_count < max_retries && !committed; ++try_count)
    {
        try
        {
            /// The broker may transiently reject the commit, e.g. while the replicas of the
            /// __consumer_offsets topic catch up, or on client-server connectivity problems.
            client->commitOffsets(group, offsets_to_commit, COMMIT_TIMEOUT);
            committed = true;
            LOG_INFO(
                log,
                "Committed offset {} to Kafka for topic-partition [{}:{}]",
                topic_partition_offset.offset,
                topic_partition_offset.topic,
                topic_partition_offset.partition_id);
        }
        catch (const Kafka::Exception & e)
        {
            /// Failing to commit is not an error: Kafka is not the primary store of committed
            /// offsets, it only matters when a table is dropped and recreated.
            LOG_ERROR(log, "Exception during attempt to commit to Kafka: {}", e.message());
        }
    }

    if (!committed)
    {
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
    chassert(client);
    TopicPartitionOffsets topic_partition_offsets;

    try
    {
        const auto metadata = client->getMetadata(topics, GET_KAFKA_METADATA_TIMEOUT);
        for (const auto & topic_metadata : metadata.topics)
        {
            for (const auto & partition_metadata : topic_metadata.partitions)
            {
                topic_partition_offsets.emplace_back(
                    TopicPartitionOffset{{topic_metadata.name, partition_metadata.partition}, INVALID_OFFSET});
            }
        }
    }
    catch (const Kafka::Exception & e)
    {
        LOG_ERROR(log, "Exception during getting topic partitions from Kafka: {}", e.message());
        topic_partition_offsets.clear();
    }

    return topic_partition_offsets;
}

KafkaConsumer2::Stat KafkaConsumer2::getStat() const
{
    return {
        .current_offsets = std::invoke(
            [&]()
            {
                std::lock_guard lock(offsets_snapshot_mutex);
                return offsets_snapshot;
            }),
        .consumer_id = client_id,
        .num_messages_read = num_messages_read.load(),
        .exceptions_buffer = std::invoke(
            [&]()
            {
                std::lock_guard lock(exception_mutex);
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
        const auto & value = current->value;
        ++current;

        /// Kafka messages can have an absent (null) payload.
        if (value && value->size() >= skip_bytes)
            return std::make_shared<ReadBufferFromMemory>(value->data() + skip_bytes, value->size() - skip_bytes);
    }

    return nullptr;
}

void KafkaConsumer2::resetIfStopped()
{
    if (stopped)
    {
        stalled_status = StalledStatus::CONSUMER_STOPPED;
        /// Discard the interrupted batch; a later batch must not continue from its middle.
        messages.clear();
        current = messages.end();
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
