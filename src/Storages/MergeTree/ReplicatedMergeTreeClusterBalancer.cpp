#include <algorithm>
#include <limits>
#include <unordered_set>
#include <Storages/MergeTree/ReplicatedMergeTreeClusterBalancer.h>
#include <Storages/MergeTree/ReplicatedMergeTreeCluster.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreePartHeader.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Parsers/SyncReplicaMode.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>
#include <base/types.h>
#include <base/defines.h>

namespace
{

using namespace DB;

ReplicatedMergeTreeClusterBalancerStep getBalancerStep(const ReplicatedMergeTreeClusterPartition & partition)
{
    switch (partition.getState())
    {
        case MIGRATING: return BALANCER_MIGRATE_PARTITION;
        case CLONING: return BALANCER_CLONE_PARTITION;
        case DROPPING: return BALANCER_DROP_PARTITION;
        /// This means that ReplicatedMergeTreeClusterPartition::revert() had
        /// been called already, so we need to commit information to ZooKeeper.
        case UP_TO_DATE: return BALANCER_REVERT;
    }
}

}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int REPLICA_STATUS_CHANGED;
    extern const int ABORTED;
    extern const int TABLE_IS_READ_ONLY;
};

/// TODO(cluster): add setting
static constexpr size_t DISTRIBUTOR_NO_JOB_DELAY_MS = 5'000;
static constexpr size_t DISTRIBUTOR_ERROR_DELAY_MS = 5'000;
/// Or simply old_parts_lifetime?
static constexpr time_t DISTRIBUTOR_PARTITION_DROP_TTL_SEC = 60;
static constexpr size_t DISTRIBUTOR_MIGRATION_TIMEOUT_MS = 3600'000;

using LogEntry = ReplicatedMergeTreeLogEntry;
using LogEntryPtr = LogEntry::Ptr;

/// NOTE(cluster):
/// - do not skip mutations for migrated parts
///   this is actually not easy, maybe just disable them? and merges as well
///   but what about new data then?
ReplicatedMergeTreeClusterBalancer::ReplicatedMergeTreeClusterBalancer(ReplicatedMergeTreeCluster & cluster_)
    : cluster(cluster_)
    , storage(cluster.storage)
    , log(&Poco::Logger::get(storage.getStorageID().getFullTableName() + " (ClusterBalancer)"))
    , background_task(storage.getContext()->getSchedulePool().createTask(log->name(), [this]{ run(); }))
{
}
ReplicatedMergeTreeClusterBalancer::~ReplicatedMergeTreeClusterBalancer()
{
    shutdown();
}

void ReplicatedMergeTreeClusterBalancer::wakeup()
{
    if (is_stopped)
        throw Exception(ErrorCodes::ABORTED, "Shutdown is called for table");

    restoreStateFromCoordinator();
    background_task->activateAndSchedule();
}

void ReplicatedMergeTreeClusterBalancer::shutdown()
{
    is_stopped = true;
    background_task->deactivate();
}

void ReplicatedMergeTreeClusterBalancer::waitSynced(const zkutil::ZooKeeperPtr & zookeeper, bool throw_if_stopped)
{
    LOG_TRACE(log, "Syncing cluster");
    Stopwatch watch;
    SCOPE_EXIT({ LOG_TRACE(log, "Cluster sync took: {} ms", watch.elapsedMilliseconds()); });

    auto task_blocker = background_task->getExecLock();

    /// TODO:
    /// - check that local partition map matches the cluster partitions map
    ///   (i.e. that this replica has all parts that it respnonsible for)
    /// - seems that it is not easy to do the first item, so I guess we should
    ///   move cluster partitions into the per-replica information? but this is tricky...
    ///
    /// Or
    ///
    /// - cleanup non is_active replicas (ephemeral node)
    /// - cleanup when is_lost sets to 1

    while (true)
    {
        try
        {
            runStep(zookeeper);
        }
        catch (const Coordination::Exception & e)
        {
            if (e.code == Coordination::Error::ZSESSIONEXPIRED)
                throw;

            if (state.step == BALANCER_REVERT)
                tryLogCurrentException(log, "While trying to REVERT, retrying");
            else
                throw;
        }
        catch (...)
        {
            if (state.step == BALANCER_REVERT)
                tryLogCurrentException(log, "While trying to REVERT, retrying");
            else
                throw;
        }

        /// Always process REVERT regardless of is_stopped.
        if (state.step == BALANCER_REVERT)
            continue;
        if (state.step == BALANCER_NOTHING_TODO)
            break;
        if (is_stopped)
            break;
    }

    cleanupOldPartitions(zookeeper, 0);

    if (throw_if_stopped && is_stopped)
        throw Exception(ErrorCodes::ABORTED, "Shutdown is called for table");
}

void ReplicatedMergeTreeClusterBalancer::restoreStateFromCoordinator()
{
    auto zookeeper = cluster.getZooKeeper();

    cluster.loadFromCoordinator(zookeeper);

    const auto & partitions = cluster.getClusterPartitions();
    const auto & replica_name = storage.getReplicaName();
    for (const auto & partition : partitions)
    {
        if (!partition.isUnderReSharding())
            continue;

        if (partition.getNewReplica() != replica_name)
            continue;

        auto & target = state.target.emplace(partition);
        state.step = getBalancerStep(target);
        LOG_INFO(log, "Restore balancer task for partition {}", target.toStringForLog());
        break;
    }
}

void ReplicatedMergeTreeClusterBalancer::run()
try
{
    auto zookeeper = cluster.getZooKeeper();

    cleanupOldPartitions(zookeeper, DISTRIBUTOR_PARTITION_DROP_TTL_SEC);

    while (!is_stopped)
    {
        runStep(zookeeper);
        if (state.step == BALANCER_NOTHING_TODO)
            break;
    }

    if (!is_stopped)
        background_task->scheduleAfter(DISTRIBUTOR_NO_JOB_DELAY_MS);
}
catch (...)
{
    tryLogCurrentException(log);
    if (!is_stopped)
        background_task->scheduleAfter(DISTRIBUTOR_ERROR_DELAY_MS);
}

void ReplicatedMergeTreeClusterBalancer::runStep(const zkutil::ZooKeeperPtr & zookeeper)
{
    switch (state.step)
    {
        case BALANCER_SELECT_PARTITION:
        {
            ReplicatedMergeTreeClusterPartitionSelector selector(zookeeper, cluster);
            auto partition = selector.select();

            if (partition.has_value())
            {
                state.target = partition;
                state.step = getBalancerStep(*partition);
            }
            else
                state.step = BALANCER_NOTHING_TODO;
            break;
        }
        case BALANCER_MIGRATE_PARTITION:
        case BALANCER_CLONE_PARTITION:
        {
            std::list<LogEntryPtr> entries;

            try
            {
                entries = clonePartition(zookeeper, *state.target);
            }
            catch (...)
            {
                state.step = BALANCER_SELECT_PARTITION;
                tryLogCurrentException(log, fmt::format("Cannot process partition {}, will restart", state.target->toStringForLog()));
                break;
            }

            try
            {
                replicatePartition(zookeeper, *state.target, entries);
                /// NOTE: should we introduce some log entry for DROP_RANGE with dependencies?
                finish(zookeeper, *state.target);

                state.target.reset();
                state.step = BALANCER_SELECT_PARTITION;
            }
            catch (const Coordination::Exception & e)
            {
                if (e.code == Coordination::Error::ZSESSIONEXPIRED)
                    throw;

                state.target->revert();
                state.step = BALANCER_REVERT;
                tryLogCurrentException(log, fmt::format("Cannot process partition {}, will revert", state.target->toStringForLog()));
            }
            catch (const Exception & e)
            {
                if (e.code() == ErrorCodes::TABLE_IS_READ_ONLY)
                    throw;
                if (e.code() == ErrorCodes::LOGICAL_ERROR)
                    throw;

                state.target->revert();
                state.step = BALANCER_REVERT;
                tryLogCurrentException(log, fmt::format("Cannot process partition {}, will revert", state.target->toStringForLog()));
            }
            break;
        }
        case BALANCER_DROP_PARTITION:
            /// No real drop happens, it will be done in background (if any)
            finish(zookeeper, *state.target);
            state.target.reset();
            state.step = BALANCER_SELECT_PARTITION;
            break;
        case BALANCER_REVERT:
            revert(zookeeper, *state.target);
            state.step = BALANCER_SELECT_PARTITION;
            break;
        case BALANCER_NOTHING_TODO:
            state.step = BALANCER_SELECT_PARTITION;
            break;
    }
}

void ReplicatedMergeTreeClusterBalancer::replicatePartition(const zkutil::ZooKeeperPtr & zookeeper, const ReplicatedMergeTreeClusterPartition & target, const std::list<ReplicatedMergeTreeLogEntryPtr> & entries)
{
    /// clonePartition() insert entries to the queue (not to the common log),
    /// and those entries need to be loaded to the in memory queue to wait them
    /// below
    storage.queue.load(zookeeper);
    /// Only pullLogsToQueue() triggers background operations, so we need to do
    /// this manually to process entries ASAP.
    storage.background_operations_assignee.trigger();

    Stopwatch watch;

    const auto & stop_waiting = [&]()
    {
        bool shutdown = storage.partial_shutdown_called || storage.shutdown_called;
        bool deadline = watch.elapsedMilliseconds() > DISTRIBUTOR_MIGRATION_TIMEOUT_MS;
        /// NOTE: in case DROP REPLICA we should ignore is_readonly and is_stopped?
        return deadline || shutdown || storage.is_dropped || storage.is_readonly || is_stopped;
    };

    /// FIXME: Right now we cannot execute DROP_RANGE before all parts had been
    /// fetched from the source replica, since there is no way to ensure that
    /// those parts had been fetched by us.
    ///
    /// NOTE: that this is a problem not because it is bad, but also because it
    /// may hang the partition clone, if such part does not already exist on
    /// any replicas, but the fetches had been scheduled (like in 03015_replicated_cluster_mutations).
    for (const auto & entry : entries)
    {
        Stopwatch entry_watch;
        if (!zookeeper->waitForDisappear(entry->znode_name, stop_waiting))
        {
            throw Exception(ErrorCodes::ABORTED, "Processing of {} had been aborted (shutdown={}, is_dropped={}, is_readonly={}, is_stopped={}, took={} ms).",
                entry->znode_name,
                storage.partial_shutdown_called || storage.shutdown_called,
                storage.is_dropped,
                storage.is_readonly,
                is_stopped,
                watch.elapsedMilliseconds());
        }
        LOG_INFO(log, "Waiting for entry {} ({}). Took {} ms.",
            entry->getDescriptionForLogs(storage.format_version), entry->znode_name, watch.elapsedMilliseconds());
    }

    LOG_INFO(log, "Partition {} had been replicated. Took {} ms", target.toStringForLog(), watch.elapsedMilliseconds());
}

/// Partial copy of StorageReplicatedMergeTree::cloneReplica(),
/// but clones only specific partition and doing this a single transaction.
///
/// TODO:
/// - make log_pointer per partition until the per-partition log_pointer will
///   catch up with the global replicas log_pointer?
///
/// @see also:
/// - StorageReplicatedMergeTree::cloneReplica()
/// - StorageReplicatedMergeTree::allocateBlockNumber()
/// - StorageReplicatedMergeTree::movePartitionToTable() and friends
std::list<LogEntryPtr> ReplicatedMergeTreeClusterBalancer::clonePartition(const zkutil::ZooKeeperPtr & zookeeper, ReplicatedMergeTreeClusterPartition & target)
{
    const auto & source_replica = target.getSourceReplica();
    const auto & partition = target.getPartitionId();
    const auto & source_path = cluster.zookeeper_path / "replicas" / source_replica;
    const auto & replica_path = cluster.replica_path;
    const auto & format_version = storage.format_version;

    /// Cloning partition is done in one transaction because we cannot allow
    /// situation when source replica has unprocessed log entries, since we
    /// cannot clone log_pointer and if source replica had unprocessed log
    /// entries we cannot fetch this changes and we cannot later execute
    /// DROP_RANGE without some dependencies.
    ///
    /// NOTE: we should skip log entries that is not related to specific partition.
    Coordination::Requests ops;

    /// Update cluster partition
    {
        Coordination::Stat balancer_stat;
        fs::path balancer_path = cluster.zookeeper_path / "cluster" / "balancer";
        zookeeper->get(balancer_path, &balancer_stat);

        String partition_path = cluster.zookeeper_path / "block_numbers" / partition;
        ops.emplace_back(zkutil::makeSetRequest(partition_path, target.toString(), target.getVersion()));
        ops.emplace_back(zkutil::makeSetRequest(balancer_path, "", balancer_stat.version));
    }

    /// The order of the following three actions is important.

    Coordination::Stat source_is_lost_stat;
    zookeeper->get(source_path / "is_lost", &source_is_lost_stat);

    Coordination::Stat source_log_pointer_stat;
    String source_log_pointer_raw = zookeeper->get(source_path / "log_pointer", &source_log_pointer_stat);
    UInt64 source_log_pointer = parse<UInt64>(source_log_pointer_raw);
    String min_source_log_pointer = "log-" + padIndex(source_log_pointer);

    Coordination::Stat log_stat;
    zookeeper->get(cluster.zookeeper_path / "log", &log_stat);

    UInt64 last_log_entry = 0;
    Strings log_entries_names = zookeeper->getChildren(cluster.zookeeper_path / "log");
    if (!log_entries_names.empty())
        last_log_entry = parse<UInt64>(std::max_element(log_entries_names.begin(), log_entries_names.end())->substr(strlen("log-")));

    LOG_DEBUG(log, "Trying to clone partition {} from replica {} (log_pointer: {}, last_log_entry: {}, log entries: {})",
        partition, source_replica, source_log_pointer, last_log_entry, log_entries_names.size());

    std::erase_if(log_entries_names, [&min_source_log_pointer](const String & entry) { return entry < min_source_log_pointer; });
    /// FIXME: we cannot use this, since we explicitly check version after, while it checks it in the loop, in other words this loop is useless for us.
    auto source_queue_entries_names = storage.getSourceQueueEntries(source_replica, source_is_lost_stat, zookeeper, /* update_source_replica_log_pointer= */ false);
    LOG_DEBUG(log, "Unprocessed log entries by source replica: {}, queue entries: {}",
        log_entries_names.size(), source_queue_entries_names.size());

    /// We got log pointer and list of queue entries of source replica.
    /// At first we will get queue entries and then we will get list of active parts of source replica
    /// to enqueue fetches for missing parts. If source replica executes and removes some entry concurrently
    /// we will see produced part (or covering part) in replicas/source/parts and will enqueue fetch.
    /// We will try to parse queue entries before copying them
    /// to avoid creation of excessive and duplicating entries in our queue.
    /// See also removePartAndEnqueueFetch(...)
    std::vector<StorageReplicatedMergeTree::QueueEntryInfo> queue_entries;
    ActiveDataPartSet get_part_set{format_version};
    ActiveDataPartSet drop_range_set{format_version};
    std::unordered_set<String> exact_part_names;

    {
        /// <get_future, path, is_log_entry>
        std::vector<std::tuple<zkutil::ZooKeeper::FutureGet, String, bool>> get_futures;
        get_futures.reserve(source_queue_entries_names.size() + log_entries_names.size());

        for (const String & entry_name : source_queue_entries_names)
        {
            String path = source_path / "queue" / entry_name;
            get_futures.emplace_back(zookeeper->asyncTryGet(path), std::move(path), /* is_log_entry= */ false);
        }
        /// Since we cannot copy log_pointer we need to process log entries as well in addition to queue entries.
        for (const String & entry_name : log_entries_names)
        {
            String path = cluster.zookeeper_path / "log" / entry_name;
            get_futures.emplace_back(zookeeper->asyncTryGet(path), path, /* is_log_entry= */ true);
        }

        auto add_entry_to_queue = [&](Coordination::GetResponse && res, LogEntryPtr && parsed_entry) -> StorageReplicatedMergeTree::QueueEntryInfo &
        {
            queue_entries.emplace_back();
            auto & info = queue_entries.back();
            info.data = std::move(res.data);
            info.stat = std::move(res.stat);
            info.parsed_entry = std::move(parsed_entry);
            return info;
        };

        queue_entries.reserve(get_futures.size());
        for (auto & [future, path, is_log_entry] : get_futures)
        {
            auto res = future.get();

            /// It's ok if entry is already executed and removed: we also will get source parts set.
            if (res.error == Coordination::Error::ZNONODE)
                continue;

            chassert(res.error == Coordination::Error::ZOK);
            LogEntryPtr parsed_entry;
            try
            {
                parsed_entry = LogEntry::parse(res.data, res.stat, format_version);
            }
            catch (Exception & e)
            {
                e.addMessage("Cannot parse entry " + path);

                /// log entry cannot be unparsed since we need to filter by replicas them.
                if (is_log_entry)
                    throw;
                else
                    tryLogCurrentException(log);
            }

            if (is_log_entry)
            {
                const auto & entry_replicas = parsed_entry->replicas;
                if (std::find(entry_replicas.begin(), entry_replicas.end(), source_replica) == entry_replicas.end())
                    continue;
            }

            /// It may be ok if entry from source replica queue has newer version. We will copy entry as is.
            if (!parsed_entry)
            {
                add_entry_to_queue(std::move(res), std::move(parsed_entry));
                continue;
            }

            parsed_entry->znode_name = path;

            /// Do not process unrelated partitions
            bool entry_contains_partition = false;
            if (!parsed_entry->new_part_name.empty())
            {
                auto part_info = MergeTreePartInfo::fromPartName(parsed_entry->new_part_name, format_version);
                if (part_info.partition_id == partition)
                    entry_contains_partition = true;
            }
            for (const auto & part : parsed_entry->getVirtualPartNames(format_version))
            {
                auto part_info = MergeTreePartInfo::fromPartName(part, format_version);
                if (part_info.partition_id == partition)
                    entry_contains_partition = true;
            }
            if (!entry_contains_partition)
                continue;

            auto & info = add_entry_to_queue(std::move(res), std::move(parsed_entry));
            if (info.parsed_entry->type == LogEntry::DROP_RANGE || info.parsed_entry->type == LogEntry::DROP_PART)
            {
                drop_range_set.add(info.parsed_entry->new_part_name);
            }
            else if (info.parsed_entry->type == LogEntry::GET_PART)
            {
                String maybe_covering_drop_range = drop_range_set.getContainingPart(info.parsed_entry->new_part_name);
                if (maybe_covering_drop_range.empty())
                    get_part_set.add(info.parsed_entry->new_part_name);
            }
            else
            {
                /// We should keep local parts if they present in the queue of source replica.
                /// There's a chance that we are the only replica that has these parts.
                Strings entry_virtual_parts = info.parsed_entry->getVirtualPartNames(format_version);
                std::move(entry_virtual_parts.begin(), entry_virtual_parts.end(), std::inserter(exact_part_names, exact_part_names.end()));
            }
        }
    }

    /// Add to the queue jobs to receive all the active parts that the reference/master replica has.
    Strings source_replica_parts = zookeeper->getChildren(source_path / "parts");
    for (const auto & active_part : source_replica_parts)
    {
        auto part_info = MergeTreePartInfo::fromPartName(active_part, format_version);
        if (part_info.partition_id != partition)
            continue;
        get_part_set.add(active_part);
    }

    Strings active_parts = get_part_set.getParts();

    /// Remove local parts if source replica does not have them, because such parts will never be fetched by other replicas.
    Strings local_parts_in_zk = zookeeper->getChildren(replica_path / "parts");
    Strings parts_to_remove_from_zk;

    for (const auto & part : local_parts_in_zk)
    {
        auto part_info = MergeTreePartInfo::fromPartName(part, format_version);
        if (part_info.partition_id != partition)
            continue;

        /// We look for exact match (and not for any covering part)
        /// because our part might be dropped and covering part might be merged though gap.
        /// (avoid resurrection of data that was removed a long time ago)
        if (get_part_set.getContainingPart(part) == part)
            continue;

        if (exact_part_names.contains(part))
            continue;

        parts_to_remove_from_zk.emplace_back(part);
        LOG_WARNING(log, "Source replica does not have part {}. Removing it from ZooKeeper.", part);
    }

    {
        /// Check "is_lost" version after retrieving queue and parts.
        /// If version has changed, then replica most likely has been dropped and parts set is inconsistent,
        /// so throw exception and retry cloning.
        Coordination::Stat is_lost_stat_new;
        zookeeper->get(source_path / "is_lost", &is_lost_stat_new);
        if (is_lost_stat_new.version != source_is_lost_stat.version)
            throw Exception(ErrorCodes::REPLICA_STATUS_CHANGED, "Cannot clone {}, because it suddenly become lost "
                                                                "or removed broken part from ZooKeeper", source_replica);
    }

    storage.removePartsFromZooKeeperWithRetries(parts_to_remove_from_zk);

    auto local_active_parts = storage.getDataPartsVectorInPartitionForInternalUsage(MergeTreeDataPartState::Active, partition);

    MergeTreeData::DataPartsVector parts_to_remove_from_working_set;

    for (const auto & part : local_active_parts)
    {
        if (get_part_set.getContainingPart(part->name) == part->name)
            continue;

        if (exact_part_names.contains(part->name))
            continue;

        parts_to_remove_from_working_set.emplace_back(part);
        LOG_WARNING(log, "Source replica does not have part {}. Removing it from working set.", part->name);
    }

    if (storage.getSettings()->detach_old_local_parts_when_cloning_replica)
    {
        auto metadata_snapshot = storage.getInMemoryMetadataPtr();

        for (const auto & part : parts_to_remove_from_working_set)
        {
            LOG_INFO(log, "Detaching {}", part->getDataPartStorage().getPartDirectory());
            part->makeCloneInDetached("clone", metadata_snapshot, /* disk_transaction= */ {});
        }
    }

    /// NOTE: it is OK not to acquire lock here, since parts for this partition should not be changed:
    /// - by user, because everything should go via coordinator
    /// - by clickhouse, because they should not be active
    ///
    /// FIXME: but this is not always true, due to deferred DROP_RANGE (enqueueDropPartition())
    storage.removePartsFromWorkingSet(NO_TRANSACTION_RAW, parts_to_remove_from_working_set, false);

    std::unordered_set<String> created_get_parts;

    /// Avoid creation of GET_PART entries which covered by another GET_PART or DROP_RANGE
    /// and creation of multiple entries with the same new_part_name.
    auto should_ignore_log_entry = [&drop_range_set, &get_part_set, this] (std::unordered_set<String> & created_gets,
                                                                    const String & part_name, const String & log_msg_context) -> bool
    {
        /// We should not create entries covered by DROP_RANGE, because we will remove them anyway (kind of optimization).
        String covering_drop_range = drop_range_set.getContainingPart(part_name);
        if (!covering_drop_range.empty())
        {
            LOG_TRACE(log, "{} {}: it's covered by drop range {}", log_msg_context, part_name, covering_drop_range);
            return true;
        }

        /// We should not create entries covered by GET_PART,
        /// because GET_PART entry has no source parts and we can execute it only by fetching.
        /// Parts covered by GET_PART are useless and may cause replication to stuck if covered part is lost.
        String covering_get_part_entry = get_part_set.getContainingPart(part_name);

        if (covering_get_part_entry.empty())
            return false;

        if (covering_get_part_entry != part_name)
        {
            LOG_TRACE(log, "{} {}: it's covered by GET_PART {}", log_msg_context, part_name, covering_get_part_entry);
            return true;
        }

        /// NOTE: It does not completely avoids duplication of GET_PART entries,
        /// because it's possible that source replica has executed some GET_PART after we copied it's queue,
        /// but before we copied its active parts set. In this case we will GET_PART entry in our queue
        /// and later will pull the original GET_PART from replication log.
        /// It should not cause any issues, but it does not allow to get rid of duplicated entries and add an assertion.
        if (created_gets.contains(part_name))
        {
            /// NOTE It would be better to copy log entry instead of creating GET_PART
            /// if there are GET_PART and log entry of other type with the same new_part_name.
            /// But it's a bit harder to implement, because it requires full-fledged virtual_parts set.
            LOG_TRACE(log, "{} {}: GET_PART for it is already created", log_msg_context, part_name);
            return true;
        }

        return false;
    };

    std::list<LogEntryPtr> fetch_log_entries;
    std::list<size_t> fetch_log_entries_indexes;
    for (const String & name : active_parts)
    {
        if (should_ignore_log_entry(created_get_parts, name, "Not fetching"))
            continue;

        LogEntryPtr log_entry_ptr = std::make_shared<LogEntry>();
        LogEntry & log_entry = *log_entry_ptr;

        /// NOTE: Instead of fetching part each time we may check if such part
        /// exists and checksum matches (similar to are_restoring_replica case
        /// in cloneReplica()).
        log_entry.type = LogEntry::GET_PART;
        log_entry.source_replica = "";
        log_entry.new_part_name = name;
        log_entry.create_time = StorageReplicatedMergeTree::tryGetPartCreateTime(zookeeper, source_path, name);

        LOG_TEST(log, "Enqueueing {} for fetch", name);
        ops.emplace_back(zkutil::makeCreateRequest(replica_path / "queue/queue-", log_entry.toString(), zkutil::CreateMode::PersistentSequential));
        created_get_parts.insert(name);
        fetch_log_entries.emplace_back(log_entry_ptr);
        fetch_log_entries_indexes.emplace_back(ops.size() - 1);
    }

    /// Add content of the reference/master replica queue to the queue.
    size_t total_entries_to_copy = 0;
    for (const auto & entry_info : queue_entries)
    {
        chassert(!entry_info.data.empty());
        if (entry_info.parsed_entry && !entry_info.parsed_entry->new_part_name.empty())
        {
            const String & part_name = entry_info.parsed_entry->new_part_name;
            const String & entry_name = entry_info.parsed_entry->znode_name;
            const auto & entry_type = entry_info.parsed_entry->type;

            if (should_ignore_log_entry(created_get_parts, part_name, fmt::format("Not copying {} {}", entry_name, entry_type)))
                continue;

            if (entry_info.parsed_entry->type == LogEntry::GET_PART)
                created_get_parts.insert(part_name);
        }

        /// It is OK to keep skip updating replicas here, since we insert
        /// directly into queue, and the entry will be processed anyway.
        LOG_TEST(log, "Copying entry {}", entry_info.data);
        ops.emplace_back(zkutil::makeCreateRequest(replica_path / "queue/queue-", entry_info.data, zkutil::CreateMode::PersistentSequential));
        ++total_entries_to_copy;
    }

    ops.emplace_back(zkutil::makeCheckRequest(cluster.zookeeper_path / "log", log_stat.version));
    ops.emplace_back(zkutil::makeCheckRequest(source_path / "log_pointer", source_log_pointer_stat.version));

    Coordination::Responses responses;
    auto code = zookeeper->tryMulti(ops, responses);
    if (code == Coordination::Error::ZBADVERSION)
    {
        if (responses[0]->error == Coordination::Error::ZBADVERSION)
            LOG_DEBUG(log, "Partition {} is already balanced by another replica. Cannot continue, will restart.", target.toStringForLog());
        else if (responses[1]->error == Coordination::Error::ZBADVERSION)
            LOG_DEBUG(log, "Another part had been assigned for re-sharding. Cannot continue, will restart.");
        else if (responses.back()->error == Coordination::Error::ZBADVERSION)
            LOG_DEBUG(log, "Replication log had been updated. Cannot continue, will restart.");
    }
    zkutil::KeeperMultiException::check(code, ops, responses);

    auto it = fetch_log_entries.begin();
    for (size_t ops_index : fetch_log_entries_indexes)
    {
        const auto & response = *responses[ops_index];
        const auto & path_created = dynamic_cast<const Coordination::CreateResponse &>(response).path_created;
        it->get()->znode_name = path_created;
        ++it;
    }

    size_t total_parts_to_fetch = created_get_parts.size();
    LOG_DEBUG(log, "Queued {} parts to be fetched for partition {}, {} parts ignored", total_parts_to_fetch, partition, active_parts.size() - total_parts_to_fetch);

    LOG_DEBUG(log, "Copied {} queue entries, {} entries ignored (for partition {})", total_entries_to_copy, queue_entries.size() - total_entries_to_copy, partition);
    {
        auto parts = zookeeper->getChildren(replica_path / "parts");
        LOG_TRACE(log, "{} parts in ZooKeeper after mimic partition {}: {}", parts.size(), partition, fmt::join(parts, ", "));
    }
    LOG_TRACE(log, "Enqueued {} fetches after mimic partition {}: {}", created_get_parts.size(), partition, fmt::join(created_get_parts, ", "));

    /// Update in-memory representation
    cluster.updateClusterPartition(target);
    target.incrementVersion();

    return fetch_log_entries;
}

void ReplicatedMergeTreeClusterBalancer::finish(const zkutil::ZooKeeperPtr & zookeeper, const ReplicatedMergeTreeClusterPartition & target)
{
    auto new_partition = target;
    new_partition.finish();
    String partition_path = cluster.zookeeper_path / "block_numbers" / new_partition.getPartitionId();
    {
        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeSetRequest(partition_path, new_partition.toString(), new_partition.getVersion()));
        zookeeper->multi(ops);
    }
    cluster.updateClusterPartition(new_partition);
    LOG_INFO(log, "Task had been successfully processed for partition {}", target.toStringForLog());
}

void ReplicatedMergeTreeClusterBalancer::revert(const zkutil::ZooKeeperPtr & zookeeper, const ReplicatedMergeTreeClusterPartition & target)
{
    const auto & new_partition = target;
    String partition_path = cluster.zookeeper_path / "block_numbers" / new_partition.getPartitionId();

    auto error = zookeeper->trySet(partition_path, new_partition.toString(), target.getVersion());
    if (error == Coordination::Error::ZOK)
    {
        cluster.updateClusterPartition(new_partition);
        LOG_INFO(log, "Task had been reverted for partition {}", target.toStringForLog());
    }
    if (error == Coordination::Error::ZBADVERSION)
        LOG_INFO(log, "Task had been reverted but partition {} had been updated by someone else, keep it as-is", target.toStringForLog());
    else if (error != Coordination::Error::ZOK)
        throw zkutil::KeeperException::fromPath(error, partition_path);
}

void ReplicatedMergeTreeClusterBalancer::enqueueDropPartition(const zkutil::ZooKeeperPtr & zookeeper, const String & source_replica, const String & partition_id)
{
    MergeTreePartInfo drop_range;
    std::optional<EphemeralLockInZooKeeper> delimiting_block_lock;
    storage.getFakePartCoveringAllPartsInPartition(partition_id, drop_range, delimiting_block_lock, true);
    String drop_range_fake_part_name = getPartNamePossiblyFake(storage.format_version, drop_range);

    ReplicatedMergeTreeLogEntryData entry_delete;
    {
        entry_delete.type = LogEntry::DROP_RANGE;
        entry_delete.source_replica = source_replica;
        entry_delete.new_part_name = drop_range_fake_part_name;
        entry_delete.detach = false;
        entry_delete.create_time = time(nullptr);
        entry_delete.replicas.push_back(source_replica);
    }

    Coordination::Requests ops_src;
    ops_src.emplace_back(zkutil::makeCreateRequest(
        cluster.zookeeper_path / "log/log-", entry_delete.toString(), zkutil::CreateMode::PersistentSequential));
    delimiting_block_lock->getUnlockOp(ops_src);

    zookeeper->multi(ops_src);

    LOG_INFO(log, "Partition {} had been scheduled for removal from {} ({})",
        partition_id, source_replica, entry_delete.getDescriptionForLogs(storage.format_version));
}

void ReplicatedMergeTreeClusterBalancer::cleanupOldPartitions(const zkutil::ZooKeeperPtr & zookeeper, time_t ttl)
{
    cluster.loadFromCoordinator(zookeeper);
    time_t now = time(nullptr);
    auto partitions = cluster.getClusterPartitions();

    const auto & replica_name = cluster.replica_name;
    const auto & local_partitions = storage.getAllPartitionIds();

    for (const auto & partition : partitions)
    {
        time_t modification_time = static_cast<time_t>(partition.getModificationTimeMs() / 1e3);
        if (modification_time + ttl > now)
            continue;

        if (!local_partitions.contains(partition.getPartitionId()))
            continue;

        if (partition.getNewReplica() == replica_name)
            continue;

        const auto & all_replicas = partition.getAllReplicas();
        if (std::find(all_replicas.begin(), all_replicas.end(), replica_name) != all_replicas.end())
            continue;

        /// FIXME: no synchronization between queue.load(), this leads to
        /// LOGICAL_ERROR, due to double call to removePartsFromWorkingSet()
        enqueueDropPartition(zookeeper, replica_name, partition.getPartitionId());
    }
}

}
