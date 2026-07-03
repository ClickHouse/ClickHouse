#include <Storages/Kafka/KeeperHandlingConsumer.h>

#include <IO/ReadHelpers.h>
#include <boost/algorithm/string/join.hpp>
#include <pcg-random/pcg_random.hpp>
#include <Common/DateLUT.h>
#include <Common/randomSeed.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace fs = std::filesystem;

namespace
{
const std::string lock_file_name{"lock"};
const std::string commit_file_name{"committed"};
const std::string intent_file_name{"intention"};
constexpr auto LOCKS_REFRESH_POLLS = 15;

std::optional<int64_t> getNumber(zkutil::ZooKeeper & keeper, const fs::path & path)
{
    std::string result;
    if (!keeper.tryGet(path, result))
        return std::nullopt;

    return DB::parse<int64_t>(result);
}
}

KeeperHandlingConsumer::MessageInfo::MessageInfo(const KafkaConsumer2 & kafka_consumer_)
    : kafka_consumer(kafka_consumer_)
{
}

String KeeperHandlingConsumer::MessageInfo::currentTopic() const
{
    return kafka_consumer.currentTopic();
}

int32_t KeeperHandlingConsumer::MessageInfo::currentPartition() const
{
    return kafka_consumer.currentPartition();
}

int64_t KeeperHandlingConsumer::MessageInfo::currentOffset() const
{
    return kafka_consumer.currentOffset();
}

String KeeperHandlingConsumer::MessageInfo::currentKey() const
{
    return kafka_consumer.currentKey();
}

String KeeperHandlingConsumer::MessageInfo::currentPayload() const
{
    return kafka_consumer.currentPayload();
}

boost::optional<cppkafka::MessageTimestamp> KeeperHandlingConsumer::MessageInfo::currentTimestamp() const
{
    return kafka_consumer.currentTimestamp();
}

const cppkafka::Message::HeaderListType & KeeperHandlingConsumer::MessageInfo::currentHeaderList() const
{
    return kafka_consumer.currentHeaderList();
}

KeeperHandlingConsumer::OffsetGuard::OffsetGuard(KeeperHandlingConsumer & consumer_, const int64_t new_offset_)
    : consumer(&consumer_)
    , new_offset(new_offset_)
{
}

KeeperHandlingConsumer::OffsetGuard::OffsetGuard(OffsetGuard && other) noexcept
    : needs_rollback(other.needs_rollback)
    , consumer(other.consumer)
    , new_offset(other.new_offset)
{
    if (&other != this)
    {
        other.needs_rollback = false;
        other.consumer = nullptr;
        other.new_offset = 0;
    }
}

KeeperHandlingConsumer::OffsetGuard::~OffsetGuard()
{
    if (consumer && needs_rollback)
        consumer->rollbackToCommittedOffsets();
}

void KeeperHandlingConsumer::OffsetGuard::commit()
{
    chassert(needs_rollback && consumer);
    consumer->saveCommittedOffset(new_offset);
    needs_rollback = false;
}

KeeperHandlingConsumer::KeeperHandlingConsumer(
    const KafkaConsumer2Ptr & kafka_consumer_,
    const std::shared_ptr<zkutil::ZooKeeper> & keeper_,
    const std::filesystem::path & keeper_path_,
    const String & replica_name_,
    size_t idx_,
    const LoggerPtr & log_)
    : keeper_path(keeper_path_)
    , replica_name(replica_name_)
    , idx(idx_)
    , kafka_consumer(kafka_consumer_)
    , keeper(keeper_)
    , log(log_)
{
    if (!keeper)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "KeeperHandlingConsumer requires a valid ZooKeeper instance");
}

bool KeeperHandlingConsumer::needsNewKeeper() const
{
    return keeper->expired();
}

void KeeperHandlingConsumer::setKeeper(const std::shared_ptr<zkutil::ZooKeeper> & keeper_)
{
    keeper = keeper_;
    {
        std::lock_guard lock(topic_partition_locks_mutex);
        permanent_locks.clear();
        tmp_locks.clear();
    }
    tmp_locks_quota = 0;
    assigned_topic_partitions.clear();
    topic_partition_index_to_consume_from = 0;
    poll_count = 0;
}

std::optional<KeeperHandlingConsumer::CannotPollReason> KeeperHandlingConsumer::prepareToPoll()
{
    chassert(isInUse());
    LOG_TRACE(log, "Polling consumer {} for events", idx);
    /// Always poll consumer for events to enable librdkafka callbacks to be handled
    kafka_consumer->pollEvents();

    /// If the consumer has some permanent locks and the lock mustn't be refreshed yet, the consumer should be able to poll.
    if (!assigned_topic_partitions.empty() && poll_count <= LOCKS_REFRESH_POLLS)
        return std::nullopt;

    assigned_topic_partitions.clear();
    // Clear temporary locks to give a chance to lock them as permanent locks and to make it possible to gather available topic partitions only once.

    {
        // Getting the metadata from Kafka might take a long time, let's not hold the mutex for too long
        std::lock_guard lock(topic_partition_locks_mutex);
        tmp_locks.clear();
    }
    if (keeper->expired())
    {
        LOG_TRACE(log, "Keeper session has ended");
        return CannotPollReason::KeeperSessionEnded;
    }

    // TODO(antaljanosbenjamin): this can be simplified to use only TopicPartitions instead of TopicPartitionOffsets
    auto all_topic_partitions = kafka_consumer->getAllTopicPartitionOffsets();
    if (all_topic_partitions.empty())
    {
        LOG_TRACE(log, "Couldn't get list of all topic partitions");
        return CannotPollReason::NoMetadata;
    }

    const auto [available_topic_partitions, active_replicas_info] = getAvailableTopicPartitions(all_topic_partitions);
    {
        std::lock_guard lock(topic_partition_locks_mutex);
        // These operations are expected to be fast, because they only modify in-memory data and read/write to Keeper. For huge number of topic partitions
        updatePermanentLocksLocked(available_topic_partitions, all_topic_partitions.size(), active_replicas_info.active_replica_count);
        lockTemporaryLocksLocked(available_topic_partitions, active_replicas_info.has_replica_without_locks);
        poll_count = 0;

        assigned_topic_partitions.reserve(permanent_locks.size() + tmp_locks.size());
        appendToAssignedTopicPartitions(permanent_locks);
        appendToAssignedTopicPartitions(tmp_locks);
    }
    // In `rollbackToCommittedOffsets` `topic_partition_locks_mutex` is locked again, this means `getStat` can be called
    // in-between the two locks. However this is not a problem, because in `getStat` the main source of information is
    // the acquired locks and the information from KafkaConsumer2 about the offset are only used to provide more recent
    // information about the offsets in case the consumer is polling message while `getStat` is called. Here this is not
    // the case, so the offset values in the lock infos are good enough.
    rollbackToCommittedOffsets();

    if (assigned_topic_partitions.empty())
    {
        LOG_TRACE(log, "Consumer {} has no partitions", idx);
        return CannotPollReason::NoPartitions;
    }
    return std::nullopt;
}

KeeperHandlingConsumer::LockedTopicPartitionInfo &
KeeperHandlingConsumer::getTopicPartitionLockLocked(const TopicPartition & topic_partition) TSA_REQUIRES(topic_partition_locks_mutex)
{
    auto locks_it = permanent_locks.find(topic_partition);

    if (locks_it != permanent_locks.end())
        return locks_it->second;

    locks_it = tmp_locks.find(topic_partition);

    if (locks_it != tmp_locks.end())
        return locks_it->second;

    throw Exception(
        ErrorCodes::LOGICAL_ERROR, "Cannot find locks for topic partition {}:{}", topic_partition.topic, topic_partition.partition_id);
}


// We go through all the topic partitions, count the number of live replicas,
// and see which partitions are already locked by other replicas
std::pair<KeeperHandlingConsumer::TopicPartitionSet, KeeperHandlingConsumer::ActiveReplicasInfo>
KeeperHandlingConsumer::getLockedTopicPartitions()
{
    LOG_TRACE(log, "Starting to lookup replica's state");
    TopicPartitionSet locked_partitions;
    auto lock_nodes = keeper->getChildren(keeper_path / "topic_partition_locks");
    std::unordered_set<String> replicas_with_lock;

    for (const auto & lock_name : lock_nodes)
    {
        replicas_with_lock.insert(keeper->get(keeper_path / "topic_partition_locks" / lock_name));
        auto base = lock_name.substr(0, lock_name.size() - 5); // drop ".lock"
        auto sep = base.rfind('_');
        if (sep == String::npos)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Topic partition lock path {} is not in correct format.", lock_name);

        String topic = base.substr(0, sep);
        Int32 partition = parse<Int32>(base.substr(sep + 1));
        TopicPartition topic_partition{.topic = topic, .partition_id = partition};

        locked_partitions.insert(topic_partition);
    }

    Strings already_locked_partitions_str;
    already_locked_partitions_str.reserve(locked_partitions.size());
    for (const auto & already_locks : locked_partitions)
        already_locked_partitions_str.push_back(fmt::format("[{}:{}]", already_locks.topic, already_locks.partition_id));
    LOG_INFO(log, "Already locked topic partitions are [{}]", boost::algorithm::join(already_locked_partitions_str, ", "));

    const auto replicas_count = keeper->getChildren(keeper_path / "replicas").size();
    LOG_TEST(log, "There are {} replicas with lock and there are {} replicas in total", replicas_with_lock.size(), replicas_count);
    const auto has_replica_without_locks = replicas_with_lock.size() < replicas_count;
    return {locked_partitions, ActiveReplicasInfo{replicas_count, has_replica_without_locks}};
}

std::pair<KeeperHandlingConsumer::TopicPartitions, KeeperHandlingConsumer::ActiveReplicasInfo>
KeeperHandlingConsumer::getAvailableTopicPartitions(const TopicPartitionOffsets & all_topic_partitions)
{
    const auto get_locked_partitions_res = getLockedTopicPartitions();
    const auto & already_locked_partitions = get_locked_partitions_res.first;
    TopicPartitions available_topic_partitions;
    available_topic_partitions.reserve(all_topic_partitions.size());
    for (const auto & partition : all_topic_partitions)
    {
        if (!already_locked_partitions.contains(partition))
            available_topic_partitions.push_back(partition);
    }
    Strings available_topic_partitions_str;
    available_topic_partitions_str.reserve(available_topic_partitions.size());
    for (const auto & available_partition : available_topic_partitions)
        available_topic_partitions_str.push_back(fmt::format("[{}:{}]", available_partition.topic, available_partition.partition_id));
    LOG_INFO(log, "Topic partitions [{}] are available to lock", boost::algorithm::join(available_topic_partitions_str, ", "));

    return {available_topic_partitions, get_locked_partitions_res.second};
}

std::optional<KeeperHandlingConsumer::LockedTopicPartitionInfo>
KeeperHandlingConsumer::createLocksInfoIfFree(const TopicPartition & partition_to_lock)
{
    const auto topic_partition_path = getTopicPartitionPath(partition_to_lock);
    const auto lock_file_path = getTopicPartitionLockPath(partition_to_lock);
    keeper->createAncestors(lock_file_path);
    try
    {
        using zkutil::EphemeralNodeHolder;
        LockedTopicPartitionInfo lock_info{
            EphemeralNodeHolder::create(lock_file_path, *keeper, replica_name),
            getNumber(*keeper, topic_partition_path / commit_file_name),
            getNumber(*keeper, topic_partition_path / intent_file_name)};

        LOG_TRACE(
            log,
            "Locked topic partition: {}:{} at offset (present: {}) {} with intent size (present: {}) {}",
            partition_to_lock.topic,
            partition_to_lock.partition_id,
            lock_info.committed_offset.has_value(),
            lock_info.committed_offset.value_or(0),
            lock_info.intent_size.has_value(),
            lock_info.intent_size.value_or(0));

        return lock_info;
    }
    catch (const Coordination::Exception & e)
    {
        if (e.code == Coordination::Error::ZNODEEXISTS)
        {
            LOG_TRACE(
                log,
                "Skipping lock for topic partition {}:{} because it already exists",
                partition_to_lock.topic,
                partition_to_lock.partition_id);
            return std::nullopt;
        }
        throw;
    }
}

void KeeperHandlingConsumer::lockTemporaryLocksLocked(
    const TopicPartitions & available_topic_partitions, const bool has_replica_without_locks) TSA_REQUIRES(topic_partition_locks_mutex)
{
    /// There are no available topic partitions, so drop the quota to 0
    if (available_topic_partitions.empty())
    {
        LOG_TRACE(log, "There are no available topic partitions to lock");
        tmp_locks_quota = 0;
        return;
    }
    LOG_TRACE(log, "Starting to lock temporary locks");

    /// We have some temporary lock quota, but there is at least one replica without locks, let's drop the quote to give
    /// the other replica a chance to lock some partitions
    if (tmp_locks_quota > 0 && has_replica_without_locks)
    {
        LOG_TRACE(log, "There is at least one consumer without locks, won't lock any temporary locks this round");
        tmp_locks_quota = 0;
        return;
    }

    /// We have some temporary lock quota, but it is greater than the number of available topic partitions, so we will
    /// reduce the quota to give other replicas a chance to lock some partitions
    if (tmp_locks_quota > 0 && tmp_locks_quota <= available_topic_partitions.size())
    {
        LOG_TRACE(log, "Reducing temporary locks to give other replicas a chance to lock some partitions");
        tmp_locks_quota = std::min(tmp_locks_quota - 1, available_topic_partitions.size() - 1);
    }
    else
    {
        tmp_locks_quota = std::min(available_topic_partitions.size(), tmp_locks_quota + 1);
    }
    LOG_INFO(log, "The replica can take {} temporary locks in the current round", tmp_locks_quota);

    if (tmp_locks_quota == 0)
        return;

    auto available_topic_partitions_copy = available_topic_partitions;
    pcg64 generator(randomSeed());
    std::shuffle(available_topic_partitions_copy.begin(), available_topic_partitions_copy.end(), generator);

    for (const auto & tp : available_topic_partitions)
    {
        if (tmp_locks.size() >= tmp_locks_quota)
            break;
        auto maybe_lock = createLocksInfoIfFree(tp);
        if (!maybe_lock.has_value())
            continue;
        tmp_locks.emplace(TopicPartition(tp), std::move(*maybe_lock));
    }
}

// If the number of locks on a replica is greater than it can hold, then we first release the partitions that we can no
// longer hold. Otherwise, we try to lock free partitions one by one.
void KeeperHandlingConsumer::updatePermanentLocksLocked(
    const TopicPartitions & available_topic_partitions, const size_t topic_partitions_count, const size_t active_replica_count)
    TSA_REQUIRES(topic_partition_locks_mutex)
{
    LOG_TRACE(log, "Starting to update permanent locks");
    chassert(active_replica_count > 0 && "There should be at least one active replica, because we are active");
    size_t can_lock_partitions = std::max<size_t>(topic_partitions_count / static_cast<size_t>(active_replica_count), 1);

    LOG_TRACE(log, "The replica can have {} permanent locks after the current round", can_lock_partitions);

    if (can_lock_partitions == permanent_locks.size())
    {
        LOG_TRACE(log, "The number of permanent locks is equal to the number of locks that can be taken, will not update them");
        return;
    }

    if (can_lock_partitions < permanent_locks.size())
    {
        LOG_TRACE(log, "Will release the extra {} topic partition locks", permanent_locks.size() - can_lock_partitions);
        size_t need_to_unlock = permanent_locks.size() - can_lock_partitions;
        auto permanent_locks_it = permanent_locks.begin();
        for (size_t i = 0; i < need_to_unlock && permanent_locks_it != permanent_locks.end(); ++i)
        {
            LOG_TEST(
                log,
                "Releasing topic partition lock for [{}:{}] at offset",
                permanent_locks_it->first.topic,
                permanent_locks_it->first.partition_id);
            permanent_locks_it = permanent_locks.erase(permanent_locks_it);
        }
    }
    else
    {
        size_t need_to_lock = can_lock_partitions - permanent_locks.size();
        LOG_TRACE(log, "Will try to lock {} topic partitions", need_to_lock);
        auto tp_it = available_topic_partitions.begin();
        for (size_t i = 0; i < need_to_lock && tp_it != available_topic_partitions.end();)
        {
            const auto & tp = *tp_it;
            ++tp_it;
            auto maybe_lock = createLocksInfoIfFree(tp);
            if (!maybe_lock.has_value())
                continue;
            permanent_locks.emplace(TopicPartition(tp), std::move(*maybe_lock));
            ++i;
        }
    }
}

void KeeperHandlingConsumer::rollbackToCommittedOffsets()
{
    TopicPartitionOffsets offsets_to_rollback;
    offsets_to_rollback.reserve(assigned_topic_partitions.size());
    {
        std::lock_guard lock(topic_partition_locks_mutex);
        for (const auto & topic_partition : assigned_topic_partitions)
        {
            auto & lock_info = getTopicPartitionLockLocked(topic_partition);
            offsets_to_rollback.push_back(
                TopicPartitionOffset{{topic_partition}, lock_info.committed_offset.value_or(KafkaConsumer2::INVALID_OFFSET)});
        }
    }
    kafka_consumer->updateOffsets(std::move(offsets_to_rollback));
}

void KeeperHandlingConsumer::saveIntentSize(const KafkaConsumer2::TopicPartition & topic_partition, const std::optional<int64_t> & offset, const uint64_t intent)
{
    // offset is used only for debugging purposes in tests, because it greatly helps understanding failures and it is
    // not expensive to get it.
    LOG_TEST(
        log,
        "Saving intent of {} for topic-partition [{}:{}] at offset {}",
        intent,
        topic_partition.topic,
        topic_partition.partition_id,
        offset.value_or(KafkaConsumer2::INVALID_OFFSET));

    {
        std::lock_guard lock(topic_partition_locks_mutex);
        getTopicPartitionLockLocked(topic_partition).intent_size = intent;
    }

    const auto partition_prefix = getTopicPartitionPath(topic_partition);
    writeTopicPartitionInfoToKeeper(partition_prefix / intent_file_name, toString(intent));
}

void KeeperHandlingConsumer::saveCommittedOffset(const int64_t new_offset)
{
    if (topic_partition_index_to_consume_from >= assigned_topic_partitions.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Topic partition index to consume from ({}) is out of bounds for assigned topic partitions size ({})",
            topic_partition_index_to_consume_from,
            assigned_topic_partitions.size());

    auto & topic_partition = assigned_topic_partitions[topic_partition_index_to_consume_from];
    const auto partition_prefix = getTopicPartitionPath(topic_partition);
    writeTopicPartitionInfoToKeeper(partition_prefix / commit_file_name, toString(new_offset));
    last_commit_timestamp = timeInSeconds(std::chrono::system_clock::now());
    ++num_commits;
    {
        std::lock_guard lock(topic_partition_locks_mutex);
        auto & lock_info = getTopicPartitionLockLocked(topic_partition);
        lock_info.committed_offset = new_offset;
        lock_info.intent_size.reset();
    }
    // This is best effort, if it fails we will override it in the next round
    keeper->tryRemove(partition_prefix / intent_file_name, -1);
    LOG_TEST(
        log, "Saved offset {} for topic-partition [{}:{}]", new_offset, topic_partition.topic, topic_partition.partition_id);
    kafka_consumer->commit(TopicPartitionOffset{{topic_partition}, new_offset});
}

void KeeperHandlingConsumer::writeTopicPartitionInfoToKeeper(const std::filesystem::path & keeper_path_to_data, const String & data)
{
    Coordination::Requests ops;
    keeper->checkExistsAndGetCreateAncestorsOps(keeper_path_to_data, ops);
    if (keeper->exists(keeper_path_to_data))
        ops.emplace_back(zkutil::makeSetRequest(keeper_path_to_data, data, -1));
    else
        ops.emplace_back(zkutil::makeCreateRequest(keeper_path_to_data, data, zkutil::CreateMode::Persistent));

    Coordination::Responses responses;
    const auto code = keeper->tryMulti(ops, responses);
    if (code != Coordination::Error::ZOK)
        zkutil::KeeperMultiException::check(code, ops, responses);
}

bool KeeperHandlingConsumer::startUsing(const ConfigCreator & config_creator)
{
    auto create_consumer = !kafka_consumer->hasConsumer();
    if (create_consumer)
        kafka_consumer->createConsumer(config_creator(kafka_consumer));

    in_use = true;

    return create_consumer;
}

void KeeperHandlingConsumer::stopUsing()
{
    in_use = false;
    last_used_usec = timeInMicroseconds(std::chrono::system_clock::now());
}

CppKafkaConsumerPtr KeeperHandlingConsumer::moveConsumer() const
{
    chassert(!isInUse() && "KeeperHandlingConsumer cannot destroy consumer when it is in use!");
    return kafka_consumer->moveConsumer();
}

std::optional<KeeperHandlingConsumer::OffsetGuard> KeeperHandlingConsumer::poll(MessageSinkFunction & message_sink)
{
    chassert(isInUse() && !assigned_topic_partitions.empty());

    ++poll_count;

    topic_partition_index_to_consume_from = (topic_partition_index_to_consume_from + 1) % assigned_topic_partitions.size();
    const auto & topic_partition = assigned_topic_partitions[topic_partition_index_to_consume_from];
    LOG_TRACE(
        log,
        "Will fetch {}:{} (topic_partition_index_to_consume_from is {})",
        topic_partition.topic,
        topic_partition.partition_id,
        topic_partition_index_to_consume_from);

    const auto [committed_offset, intent_size] = std::invoke(
        [&]
        {
            std::lock_guard lock(topic_partition_locks_mutex);
            const auto & lock_info = getTopicPartitionLockLocked(topic_partition);
            return std::make_pair(lock_info.committed_offset, lock_info.intent_size);
        });

    MessageInfo message_info(*kafka_consumer);
    ReadBufferPtr buf;
    uint64_t consumed_messages = 0;
    int64_t last_read_offset = 0;
    while (true)
    {
        buf = kafka_consumer->consume(topic_partition, intent_size);
        last_poll_timestamp = timeInSeconds(std::chrono::system_clock::now());
        if (buf)
        {
            ++consumed_messages;
            last_read_offset = message_info.currentOffset();
        }
        /// Let's call message sink even if we couldn't pull any messages, so it can count of how many failed polled attempts did we have
        if (message_sink(buf, message_info, kafka_consumer->hasMorePolledMessages(), kafka_consumer->isStalled()))
        {
            if (consumed_messages == 0)
                return std::nullopt;

            saveIntentSize(topic_partition, committed_offset, consumed_messages);
            return OffsetGuard(*this, last_read_offset + 1);
        }
    }
}

StorageKafkaUtils::ConsumerStatistics KeeperHandlingConsumer::getStat() const
{
    using CommittedOffsetAndIntentSize = std::pair<int64_t, std::optional<uint64_t>>;
    using CommittedOffsetAndIntentSizes = std::unordered_map<
        KafkaConsumer2::TopicPartition,
        CommittedOffsetAndIntentSize,
        KafkaConsumer2::TopicPartitionHash,
        KafkaConsumer2::TopicPartitionEquality>;
    CommittedOffsetAndIntentSizes committed_offsets_and_intent_sizes;
    KafkaConsumer2::Stat consumer_stat;
    {
        std::lock_guard lock(topic_partition_locks_mutex);
        for (const auto & [topic_partition, info] : permanent_locks)
            committed_offsets_and_intent_sizes[topic_partition]
                = {info.committed_offset.value_or(KafkaConsumer2::INVALID_OFFSET), info.intent_size};
        for (const auto & [topic_partition, info] : tmp_locks)
            committed_offsets_and_intent_sizes[topic_partition]
                = {info.committed_offset.value_or(KafkaConsumer2::INVALID_OFFSET), info.intent_size};
        // TODO(antaljanosbenjamin): make sure this is still safe even if the consumer is being closed, maybe it can be
        // by protecting that in `StorageKafka2` similarly to how it is done in StorageKafka
        consumer_stat = kafka_consumer->getStat();
    }

    using Assignment = StorageKafkaUtils::ConsumerStatistics::Assignment;
    using Assignments = StorageKafkaUtils::ConsumerStatistics::Assignments;

    Assignments assignments;
    assignments.reserve(committed_offsets_and_intent_sizes.size());

    /// It is possible that some topic partitions are already locked, but they were not assigned to the consumer yet. In
    /// that case the committed offset is the current offset. See comment in `prepareToPoll`.
    for (const auto & [topic_partition, committed_offset_and_intent_size] : committed_offsets_and_intent_sizes)
    {
        int64_t offset = committed_offset_and_intent_size.first;
        if (auto it = consumer_stat.current_offsets.find(topic_partition); it != consumer_stat.current_offsets.end())
            offset = it->second;

        assignments.push_back(Assignment{
            .topic_str = topic_partition.topic,
            .partition_id = topic_partition.partition_id,
            .current_offset = offset,
            .intent_size = committed_offset_and_intent_size.second,
        });
    }

    return {
        .consumer_id = consumer_stat.consumer_id,
        .assignments = std::move(assignments),
        .last_poll_time = last_poll_timestamp.load(),
        .num_messages_read = consumer_stat.num_messages_read,
        .last_commit_timestamp = last_commit_timestamp.load(),
        .last_rebalance_timestamp = 0,
        .num_commits = num_commits.load(),
        .num_rebalance_assignments = 0,
        .num_rebalance_revocations = 0,
        .exceptions_buffer = std::move(consumer_stat.exceptions_buffer),
        .in_use = in_use.load(),
        .last_used_usec = last_used_usec.load(),
        .rdkafka_stat = std::move(consumer_stat.rdkafka_stat),
    };
}

IKafkaExceptionInfoSinkPtr KeeperHandlingConsumer::getExceptionInfoSink() const
{
    return kafka_consumer;
}

fs::path KeeperHandlingConsumer::getTopicPartitionPath(const TopicPartition & topic_partition)
{
    return keeper_path / "topics" / topic_partition.topic / "partitions" / std::to_string(topic_partition.partition_id);
}

fs::path KeeperHandlingConsumer::getTopicPartitionLockPath(const TopicPartition & topic_partition)
{
    auto topic_partition_name = fmt::format("{}_{}.{}", topic_partition.topic, topic_partition.partition_id, lock_file_name);
    return keeper_path / "topic_partition_locks" / topic_partition_name;
}

void KeeperHandlingConsumer::appendToAssignedTopicPartitions(const TopicPartitionLocks & locks)
{
    for (const auto & [topic_partition, info] : locks)
        assigned_topic_partitions.push_back(topic_partition);
}
}
