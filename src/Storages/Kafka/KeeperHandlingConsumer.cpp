#include <Storages/Kafka/KeeperHandlingConsumer.h>

#include <algorithm>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <boost/algorithm/string/join.hpp>
#include <pcg-random/pcg_random.hpp>
#include <Common/DateLUT.h>
#include <Common/LockMemoryExceptionInThread.h>
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
/// Rounds to keep free partitions reserved (up to a lockless replica's permanent quota)
/// for a possibly restarting replica; past this window a published demand keeps them reserved.
constexpr size_t MAX_ROUNDS_TO_YIELD_TO_LOCKLESS_REPLICA = 8;

size_t parseDemandRetentionMs(const String & value)
{
    /// Retention = a few of the retry intervals the demand value declares; an unchanged
    /// demand past it means nobody is requesting the partitions anymore.
    constexpr size_t demand_retention_factor = 3;
    constexpr size_t default_retention_ms = 1500;
    constexpr size_t max_retention_ms = 1800000;

    const auto sep = value.find(':');
    if (sep == String::npos || sep == 0 || sep + 1 == value.size())
        return default_retention_ms;
    try
    {
        return std::min(std::max<UInt64>(parse<UInt64>(value.substr(0, sep)), 1) * demand_retention_factor, max_retention_ms);
    }
    catch (...) /// Ok: a malformed declared interval falls back to the default retention
    {
        return default_retention_ms;
    }
}

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

KeeperHandlingConsumer::OffsetGuard::OffsetGuard(
    KeeperHandlingConsumer & consumer_, const int64_t new_offset_, const bool replays_existing_intent_)
    : consumer(&consumer_)
    , new_offset(new_offset_)
    , replays_existing_intent(replays_existing_intent_)
{
}

KeeperHandlingConsumer::OffsetGuard::OffsetGuard(OffsetGuard && other) noexcept
    : needs_rollback(other.needs_rollback)
    , consumer(other.consumer)
    , new_offset(other.new_offset)
    , replays_existing_intent(other.replays_existing_intent)
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
        consumer->rollbackToCommittedOffsetsNoThrow();
}

void KeeperHandlingConsumer::OffsetGuard::commit()
{
    chassert(needs_rollback && consumer);
    consumer->saveCommittedOffset(new_offset);
    needs_rollback = false;
}

void KeeperHandlingConsumer::OffsetGuard::rollbackBeforeInsert()
{
    chassert(needs_rollback && consumer);
    /// A replayed intent must survive the rollback: the insert of an earlier attempt may have
    /// committed the rows of the batch, so deduplication still needs the exact replay.
    if (!replays_existing_intent)
        consumer->discardIntentSizeNoThrow();
    consumer->rollbackToCommittedOffsetsNoThrow();
    needs_rollback = false;
}

KeeperHandlingConsumer::KeeperHandlingConsumer(
    const KafkaConsumer2Ptr & kafka_consumer_,
    const std::shared_ptr<zkutil::ZooKeeper> & keeper_,
    const std::filesystem::path & keeper_path_,
    const String & replica_name_,
    size_t idx_,
    const LoggerPtr & log_,
    size_t num_consumers_,
    UInt64 partition_shard_num_,
    UInt64 shard_count_)
    : keeper_path(keeper_path_)
    , replica_name(replica_name_)
    , idx(idx_)
    , num_consumers(std::max<size_t>(num_consumers_, 1))
    , partition_shard_num(partition_shard_num_)
    , shard_count(shard_count_)
    , kafka_consumer(kafka_consumer_)
    , keeper(keeper_)
    , log(log_)
{
    if (!keeper)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "KeeperHandlingConsumer requires a valid ZooKeeper instance");
}

KeeperHandlingConsumer::~KeeperHandlingConsumer()
{
    /// The demand node must not outlive the consumer that published it.
    try
    {
        removeDemandInKeeper();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to remove the demand node");
    }
}

bool KeeperHandlingConsumer::needsNewKeeper() const
{
    return keeper->expired();
}

void KeeperHandlingConsumer::setKeeper(const std::shared_ptr<zkutil::ZooKeeper> & keeper_)
{
    /// Drop the lock holders before replacing `keeper` -- same use-after-free hazard as
    /// `replica_is_active_node` in `StorageKafka2::partialShutdown` (see comment there).
    {
        std::lock_guard lock(topic_partition_locks_mutex);
        permanent_locks.clear();
        tmp_locks.clear();
        lockless_rounds_by_replica.clear();
        demand_state_by_replica.clear();
    }
    keeper = keeper_;
    tmp_locks_quota = 0;
    demand_published = false;
    last_demand_time = {};
    assigned_topic_partitions.clear();
    topic_partition_index_to_consume_from = 0;
    poll_count = 0;
}

std::optional<KeeperHandlingConsumer::CannotPollReason> KeeperHandlingConsumer::prepareToPoll(
    UInt64 locks_refresh_polls, size_t retry_interval_ms)
{
    chassert(isInUse());
    LOG_TRACE(log, "Polling consumer {} for events", idx);
    /// Always poll consumer for events to enable librdkafka callbacks to be handled
    kafka_consumer->pollEvents();

    /// If the consumer has some permanent locks and the lock mustn't be refreshed yet, the consumer should be able to poll.
    if (!assigned_topic_partitions.empty() && poll_count <= locks_refresh_polls)
        return std::nullopt;

    if (!assigned_topic_partitions.empty())
        LOG_DEBUG(
            log,
            "Consumer {} refreshing its Keeper locks after {} polls (stream_kafka_locks_refresh_polls = {})",
            idx,
            poll_count,
            locks_refresh_polls);

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

    /// Filter partitions by affinity: partition_id % shard_count == partition_shard_num - 1.
    if (shard_count > 0)
    {
        const auto total_before = all_topic_partitions.size();
        std::erase_if(all_topic_partitions, [&](const auto & tp)
        {
            return static_cast<UInt64>(tp.partition_id) % shard_count != partition_shard_num - 1;
        });
        LOG_TRACE(log, "Partition affinity filter: {} -> {} partitions (partition_shard_num={}, shard_count={})",
            total_before, all_topic_partitions.size(), partition_shard_num, shard_count);

        if (all_topic_partitions.empty())
        {
            LOG_TRACE(log, "No partitions match the affinity filter");
            return CannotPollReason::NoPartitions;
        }
    }

    const auto [available_topic_partitions, active_replicas_info] = getAvailableTopicPartitions(all_topic_partitions);
    /// The fast path above lets the next cycle poll on any non-empty assignment, so from here on the
    /// assignment must be left either rewound to the committed offsets or empty.
    try
    {
        {
            std::lock_guard lock(topic_partition_locks_mutex);
            // These operations are expected to be fast, because they only modify in-memory data and read/write to Keeper. For huge number of topic partitions
            const size_t node_quota = std::max<size_t>(all_topic_partitions.size() / active_replicas_info.active_replica_count, 1);
            updatePermanentLocksLocked(available_topic_partitions, all_topic_partitions.size(), active_replicas_info.active_replica_count);
            lockTemporaryLocksLocked(available_topic_partitions, node_quota, active_replicas_info.lockless_active_replicas, active_replicas_info.demand_of_lockless);
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
    }
    catch (...)
    {
        assigned_topic_partitions.clear();
        throw;
    }

    if (assigned_topic_partitions.empty())
    {
        /// Only demand when this consumer could use the reserved partitions (a consumer
        /// with a zero sub-quota cannot take permanent locks, so a reservation is wasted).
        const size_t node_quota = std::max<size_t>(all_topic_partitions.size() / active_replicas_info.active_replica_count, 1);
        if (computeConsumerQuota(node_quota) > 0)
            markDemandInKeeper(retry_interval_ms);
        LOG_TRACE(log, "Consumer {} has no partitions", idx);
        return CannotPollReason::NoPartitions;
    }
    removeDemandInKeeper();
    return std::nullopt;
}

void KeeperHandlingConsumer::markDemandInKeeper(size_t retry_interval_ms)
{
    static std::atomic<UInt64> demand_counter{0};
    const auto now = std::chrono::steady_clock::now();

    /// Declare the measured retry interval (attempts include metadata fetches and event
    /// polling beyond the scheduling delay); the first attempt falls back to the setting.
    size_t declared_interval_ms = retry_interval_ms;
    if (last_demand_time != std::chrono::steady_clock::time_point{})
    {
        const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_demand_time).count();
        declared_interval_ms = std::max<size_t>(retry_interval_ms, elapsed_ms);
    }
    last_demand_time = now;

    const auto demand_path = fs::path(keeper_path) / "replicas" / replica_name / "demand";
    const auto value = fmt::format("{}:{}", declared_interval_ms, demand_counter.fetch_add(1) + 1);
    const auto code = keeper->tryCreate(demand_path, value, zkutil::CreateMode::Ephemeral);
    if (code == Coordination::Error::ZNODEEXISTS)
        keeper->set(demand_path, value);
    else if (code != Coordination::Error::ZOK)
        throw Coordination::Exception::fromPath(code, demand_path);
    demand_published = true;
}

void KeeperHandlingConsumer::removeDemandInKeeper()
{
    if (!demand_published)
        return;
    const auto demand_path = fs::path(keeper_path) / "replicas" / replica_name / "demand";
    const auto code = keeper->tryRemove(demand_path);
    if (code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE)
        demand_published = false;
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

    Strings lock_paths;
    lock_paths.reserve(lock_nodes.size());
    for (const auto & lock_name : lock_nodes)
        lock_paths.push_back(keeper_path / "topic_partition_locks" / lock_name);

    /// Locks are released and re-acquired as replicas rebalance. A lock listed by
    /// getChildren() may already be gone by the time we read it; such a lock is held
    /// by no one, so skip it instead of failing the whole polling round. The read data
    /// (owning replica) feeds the lockless-replica detection in getActiveReplicasInfo.
    auto lock_responses = keeper->tryGet(lock_paths);

    for (size_t i = 0; i < lock_nodes.size(); ++i)
    {
        const auto & lock_response = lock_responses[i];
        if (lock_response.error == Coordination::Error::ZNONODE)
            continue;
        if (lock_response.error != Coordination::Error::ZOK)
            throw Coordination::Exception::fromPath(lock_response.error, lock_paths[i]);
        replicas_with_lock.insert(lock_response.data);

        const auto & lock_name = lock_nodes[i];
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

    return {locked_partitions, getActiveReplicasInfo(replicas_with_lock)};
}

KeeperHandlingConsumer::ActiveReplicasInfo
KeeperHandlingConsumer::getActiveReplicasInfo(const std::unordered_set<String> & replicas_with_lock)
{
    const auto replica_names = keeper->getChildren(keeper_path / "replicas");

    /// In affinity mode only replicas of our own shard share the quota; the shard num is stored
    /// as the replica znode data. Filter by it first so we don't probe is_active for discarded replicas.
    Strings candidates;
    if (shard_count == 0)
    {
        candidates = replica_names;
    }
    else
    {
        const auto my_shard_num = toString(partition_shard_num);

        Strings replica_paths;
        replica_paths.reserve(replica_names.size());
        for (const auto & name : replica_names)
            replica_paths.push_back(keeper_path / "replicas" / name);

        auto shard_num_responses = keeper->tryGet(replica_paths);
        for (size_t i = 0; i < replica_names.size(); ++i)
        {
            const auto & response = shard_num_responses[i];
            if (response.error == Coordination::Error::ZOK && response.data == my_shard_num)
                candidates.push_back(replica_names[i]);
        }
    }

    /// The replica znode is persistent and outlives a dead replica, so is_active is the liveness signal.
    Strings is_active_paths;
    is_active_paths.reserve(candidates.size());
    for (const auto & name : candidates)
        is_active_paths.push_back(keeper_path / "replicas" / name / "is_active");

    auto is_active_responses = keeper->exists(is_active_paths);

    size_t active_replica_count = 0;
    std::vector<String> lockless_active_replicas;
    for (size_t i = 0; i < candidates.size(); ++i)
    {
        const auto & response = is_active_responses[i];
        if (response.error == Coordination::Error::ZNONODE)
            continue;
        if (response.error != Coordination::Error::ZOK)
            throw Coordination::Exception::fromPath(response.error, is_active_paths[i]);

        ++active_replica_count;
        if (!replicas_with_lock.contains(candidates[i]))
            lockless_active_replicas.push_back(std::move(candidates[i]));
    }

    /// Read the demand published by lockless replicas (value refreshed on every attempt).
    std::unordered_map<String, String> demand_of_lockless;
    if (!lockless_active_replicas.empty())
    {
        Strings demand_paths;
        demand_paths.reserve(lockless_active_replicas.size());
        for (const auto & replica : lockless_active_replicas)
            demand_paths.push_back(fs::path(keeper_path) / "replicas" / replica / "demand");

        auto demand_responses = keeper->tryGet(demand_paths);
        for (size_t i = 0; i < lockless_active_replicas.size(); ++i)
        {
            const auto & response = demand_responses[i];
            if (response.error == Coordination::Error::ZOK)
                demand_of_lockless.emplace(lockless_active_replicas[i], response.data);
            else if (response.error != Coordination::Error::ZNONODE)
                throw Coordination::Exception::fromPath(response.error, demand_paths[i]);
        }
    }

    /// Clamp to 1: our own is_active may be transiently missing, and 0 would divide by zero in
    /// updatePermanentLocksLocked (its chassert is a no-op in the release build).
    active_replica_count = std::max<size_t>(active_replica_count, 1);
    LOG_TEST(log, "There are {} active replicas, {} of them without locks, out of {} total replicas (shard_count={})",
             active_replica_count, lockless_active_replicas.size(), replica_names.size(), shard_count);
    return ActiveReplicasInfo{active_replica_count, std::move(lockless_active_replicas), std::move(demand_of_lockless)};
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
    const TopicPartitions & available_topic_partitions,
    size_t node_quota,
    const std::vector<String> & lockless_active_replicas,
    const std::unordered_map<String, String> & demand_of_lockless) TSA_REQUIRES(topic_partition_locks_mutex)
{
    /// Track how long each still-active replica has been lockless; streaks reset when the
    /// replica takes locks or disappears.
    for (const auto & replica : lockless_active_replicas)
        ++lockless_rounds_by_replica[replica];
    std::erase_if(lockless_rounds_by_replica, [&](const auto & entry)
    {
        return std::find(lockless_active_replicas.begin(), lockless_active_replicas.end(), entry.first)
            == lockless_active_replicas.end();
    });

    /// Reserve free partitions for the lockless replicas: those in their initial rounds, and
    /// those whose demand changed within its declared retention (a still-trying replica).
    const auto now = std::chrono::steady_clock::now();
    size_t lockless_replicas_to_reserve_for = 0;
    for (const auto & entry : lockless_rounds_by_replica)
    {
        const bool initial_window = entry.second <= MAX_ROUNDS_TO_YIELD_TO_LOCKLESS_REPLICA;
        bool demanding = false;
        if (const auto demand_it = demand_of_lockless.find(entry.first); demand_it != demand_of_lockless.end())
        {
            auto & state = demand_state_by_replica[entry.first];
            if (state.last_value != demand_it->second)
            {
                state.last_value = demand_it->second;
                state.retention_ms = parseDemandRetentionMs(demand_it->second);
                state.last_change = now;
            }
            demanding = now - state.last_change < std::chrono::milliseconds(state.retention_ms);
        }
        if (initial_window || demanding)
            ++lockless_replicas_to_reserve_for;
    }
    std::erase_if(demand_state_by_replica, [&](const auto & entry)
    {
        return !demand_of_lockless.contains(entry.first);
    });
    const size_t reserved_for_lockless = lockless_replicas_to_reserve_for
        ? std::min(available_topic_partitions.size(), lockless_replicas_to_reserve_for * node_quota)
        : 0;

    /// There are no available topic partitions, so drop the quota to 0
    if (available_topic_partitions.empty())
    {
        LOG_TRACE(log, "There are no available topic partitions to lock");
        tmp_locks_quota = 0;
    }
    /// We have some temporary lock quota, but it is greater than the number of available topic partitions, so we will
    /// reduce the quota to give other replicas a chance to lock some partitions
    else if (tmp_locks_quota > available_topic_partitions.size())
    {
        LOG_TRACE(log, "Reducing temporary locks to give other replicas a chance to lock some partitions");
        tmp_locks_quota = std::min(tmp_locks_quota - 1, available_topic_partitions.size() - 1);
    }
    else
    {
        tmp_locks_quota = std::min(available_topic_partitions.size(), tmp_locks_quota + 1);
    }
    /// The reservation limits only this round's take; the accumulated quota survives it.
    const size_t locks_to_take = std::min(tmp_locks_quota, available_topic_partitions.size() - reserved_for_lockless);
    if (reserved_for_lockless)
        LOG_TRACE(log, "Reserving {} free partitions for lockless replicas", reserved_for_lockless);

    /// Logged for every round, including the ones that reset the quota, so the sequence of quotas can be followed
    LOG_INFO(
        log,
        "The consumer can take {} temporary locks in the current round (available={}, idx={})",
        tmp_locks_quota,
        available_topic_partitions.size(),
        idx);

    if (locks_to_take == 0)
        return;

    auto available_topic_partitions_copy = available_topic_partitions;
    pcg64 generator(randomSeed());
    std::shuffle(available_topic_partitions_copy.begin(), available_topic_partitions_copy.end(), generator);

    for (const auto & tp : available_topic_partitions_copy)
    {
        if (tmp_locks.size() >= locks_to_take)
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

    const size_t node_quota = std::max<size_t>(topic_partitions_count / active_replica_count, 1);
    const size_t can_lock_partitions = computeConsumerQuota(node_quota);

    LOG_TRACE(
        log,
        "The consumer can have {} permanent locks after the current round "
        "(node_quota={}, active_replicas={}, num_consumers={}, idx={})",
        can_lock_partitions,
        node_quota,
        active_replica_count,
        num_consumers,
        idx);

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

size_t KeeperHandlingConsumer::computeConsumerQuota(size_t node_quota) const
{
    if (num_consumers <= 1)
        return node_quota;
    return node_quota / num_consumers + (idx < node_quota % num_consumers ? 1 : 0);
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

void KeeperHandlingConsumer::rollbackToCommittedOffsetsNoThrow() noexcept
{
    /// The rollback allocates, and it is also called from inside a catch handler, where the memory
    /// tracker is still allowed to raise `MEMORY_LIMIT_EXCEEDED`.
    LockMemoryExceptionInThread memory_tracker_lock(VariableContext::Global);
    try
    {
        rollbackToCommittedOffsets();
    }
    catch (...)
    {
        /// The consumer may still hold the messages of the aborted batch, and committing a later
        /// batch would skip them. Drop the assignment so `prepareToPoll` rebuilds it and rewinds.
        assigned_topic_partitions.clear();
        tryLogCurrentException(log, "Failed to return the consumer to the committed offsets");
    }
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
    Coordination::Requests ops;
    keeper->checkExistsAndGetCreateAncestorsOps(partition_prefix / intent_file_name, ops);
    addSetOrCreateRequest(ops, partition_prefix / intent_file_name, toString(intent));
    executeMultiInKeeper(std::move(ops));
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

    /// Commit the offset and remove the intent atomically. An interruption between the two would
    /// leave an advanced offset together with the intent of the already committed batch, and the
    /// next owner of the topic-partition would try to replay a batch that does not start at the
    /// committed offset. With fewer new messages than the stale intent, every replay would come up
    /// short and be discarded, blocking the partition until enough new messages arrive.
    Coordination::Requests ops;
    keeper->checkExistsAndGetCreateAncestorsOps(partition_prefix / commit_file_name, ops);
    addSetOrCreateRequest(ops, partition_prefix / commit_file_name, toString(new_offset));
    if (keeper->exists(partition_prefix / intent_file_name))
        ops.emplace_back(zkutil::makeRemoveRequest(partition_prefix / intent_file_name, -1));
    executeMultiInKeeper(std::move(ops));

    last_commit_timestamp = timeInSeconds(std::chrono::system_clock::now());
    ++num_commits;
    {
        std::lock_guard lock(topic_partition_locks_mutex);
        auto & lock_info = getTopicPartitionLockLocked(topic_partition);
        lock_info.committed_offset = new_offset;
        lock_info.intent_size.reset();
    }
    LOG_TEST(
        log, "Saved offset {} for topic-partition [{}:{}]", new_offset, topic_partition.topic, topic_partition.partition_id);
    kafka_consumer->commit(TopicPartitionOffset{{topic_partition}, new_offset});
}

void KeeperHandlingConsumer::discardIntentSizeNoThrow() noexcept
{
    /// Callable while an exception or a cancellation is propagating, so it must not throw. The
    /// removal is best effort: a leftover intent describes messages that were never inserted, so
    /// replaying it just inserts them, it cannot cause duplicates.
    LockMemoryExceptionInThread memory_tracker_lock(VariableContext::Global);
    try
    {
        if (topic_partition_index_to_consume_from >= assigned_topic_partitions.size())
            return;

        auto & topic_partition = assigned_topic_partitions[topic_partition_index_to_consume_from];
        {
            std::lock_guard lock(topic_partition_locks_mutex);
            getTopicPartitionLockLocked(topic_partition).intent_size.reset();
        }
        keeper->tryRemove(getTopicPartitionPath(topic_partition) / intent_file_name, -1);
        LOG_TEST(
            log,
            "Discarded the intent for topic-partition [{}:{}], because the batch was abandoned before the insert started",
            topic_partition.topic,
            topic_partition.partition_id);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to discard the intent of an abandoned batch");
    }
}

void KeeperHandlingConsumer::addSetOrCreateRequest(Coordination::Requests & ops, const std::filesystem::path & path, const String & data)
{
    if (keeper->exists(path))
        ops.emplace_back(zkutil::makeSetRequest(path, data, -1));
    else
        ops.emplace_back(zkutil::makeCreateRequest(path, data, zkutil::CreateMode::Persistent));
}

void KeeperHandlingConsumer::executeMultiInKeeper(Coordination::Requests ops)
{
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
    chassert(!isInUse() && "KeeperHandlingConsumer cannot destroy consumer when it is in use");
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

    if (intent_size.has_value())
        LOG_TRACE(
            log,
            "Replaying the previous batch of {} messages for topic-partition [{}:{}] to produce a block identical to the failed "
            "insert for deduplication",
            *intent_size,
            topic_partition.topic,
            topic_partition.partition_id);

    MessageInfo message_info(*kafka_consumer);
    ReadBufferPtr buf;
    uint64_t consumed_messages = 0;
    int64_t last_read_offset = 0;
    try
    {
        while (true)
        {
            /// When replaying an intent, never consume past it: a batch with different messages would have a different
            /// hash, and deduplication could not filter out the messages inserted by the previous (failed) attempt.
            const auto messages_left_in_intent
                = intent_size.has_value() ? std::optional<uint64_t>(*intent_size - consumed_messages) : std::nullopt;
            buf = kafka_consumer->consume(topic_partition, messages_left_in_intent);
            last_poll_timestamp = timeInSeconds(std::chrono::system_clock::now());

            /// The batch is incomplete: `consume` dropped the messages it had pulled, but the Kafka
            /// position moved past them. Committing here would skip them. Must precede `message_info`,
            /// whose getters read `current[-1]`.
            if (kafka_consumer->wasStoppedWhileConsuming())
            {
                LOG_TRACE(
                    log,
                    "Discarding the batch of {} messages for topic-partition [{}:{}], because the consumer was stopped",
                    consumed_messages,
                    topic_partition.topic,
                    topic_partition.partition_id);
                rollbackToCommittedOffsetsNoThrow();
                return std::nullopt;
            }

            if (buf)
            {
                ++consumed_messages;
                last_read_offset = message_info.currentOffset();
            }
            /// Each `consume` call returns at most one message and pulls at most `messages_left_in_intent` messages
            /// into the consumer, so a replay cannot run past the intent.
            chassert(!intent_size.has_value() || consumed_messages <= *intent_size);
            /// Let's call message sink even if we couldn't pull any messages, so it can count of how many failed polled attempts did we have
            const bool sink_requests_stop = message_sink(
                buf, message_info, kafka_consumer->hasMorePolledMessages(), kafka_consumer->isStalled(), intent_size.has_value());
            const bool intent_is_fulfilled = intent_size.has_value() && consumed_messages >= *intent_size;
            if (sink_requests_stop || intent_is_fulfilled)
            {
                if (consumed_messages == 0)
                    return std::nullopt;

                /// The batch ended short of the intent: the flush timer expired or the polls stalled before all the
                /// intended messages arrived. Inserting a smaller batch would produce a different block hash, and
                /// deduplication could not protect against duplicates. Drop the batch and let a later attempt replay
                /// the intent from the committed offset.
                if (intent_size.has_value() && consumed_messages < *intent_size)
                {
                    LOG_WARNING(
                        log,
                        "Replayed only {} messages out of the intended {} for topic-partition [{}:{}]. Discarding the batch, "
                        "because deduplication cannot handle a block that differs from the failed insert",
                        consumed_messages,
                        *intent_size,
                        topic_partition.topic,
                        topic_partition.partition_id);
                    rollbackToCommittedOffsets();
                    return std::nullopt;
                }

                saveIntentSize(topic_partition, committed_offset, consumed_messages);
                return OffsetGuard(*this, last_read_offset + 1, /* replays_existing_intent = */ intent_size.has_value());
            }
        }
    }
    catch (...)
    {
        /// Consuming a message advances the consumer past it, and only an `OffsetGuard` returns it to
        /// the committed offsets. No guard exists yet here, so undo the advance: otherwise the next
        /// batch would commit past messages that were never delivered.
        rollbackToCommittedOffsetsNoThrow();
        throw;
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
