#include <optional>
#include <vector>
#include <Storages/MergeTree/PartMovesBetweenShardsOrchestrator.h>
#include <Storages/MergeTree/PinnedPartUUIDs.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include "Common/ZooKeeper/Types.h"
#include "Common/ZooKeeper/ZooKeeper.h"
#include <Common/ZooKeeper/KeeperException.h>
#include "Core/Types.h"
#include "Disks/IStoragePolicy.h"
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
}

PartMovesBetweenShardsOrchestrator::PartMovesBetweenShardsOrchestrator(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , zookeeper_path(storage.zookeeper_path)
    , logger_name(storage.getStorageID().getFullTableName() + " (PartMovesBetweenShardsOrchestrator)")
    , log(getLogger(logger_name))
    , entries_znode_path(zookeeper_path + "/part_moves_shard")
{
    task = storage.getContext()->getSchedulePool().createTask(logger_name, [this]{ run(); });
}

void PartMovesBetweenShardsOrchestrator::run()
{
    if (!storage.getSettings()->part_moves_between_shards_enable)
        return;

    if (need_stop)
        return;

    /// Don't poll ZooKeeper too often.
    auto sleep_ms = 3 * 1000;

    try
    {
        if (storage.is_leader)
        {
            std::optional<Entry> selected_entry = selectEntryFromZk();
            if (selected_entry.has_value())
            {
                /// Schedule for immediate re-execution as likely there is more work
                /// to be done.
                step(selected_entry.value());
                task->schedule();
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }


    task->scheduleAfter(sleep_ms);
}

void PartMovesBetweenShardsOrchestrator::shutdown()
{
    need_stop = true;
    task->deactivate();
    LOG_TRACE(log, "PartMovesBetweenShardsOrchestrator thread finished");
}

std::vector<PartMovesBetweenShardsOrchestrator::Entry> PartMovesBetweenShardsOrchestrator::getEntriesFromZk()
{
    std::vector<Entry> tasks;
    auto zk = storage.getZooKeeper();

    Strings task_names = zk->getChildren(entries_znode_path + "/tasks");

    for (auto const & task_name : task_names)
    {
        PartMovesBetweenShardsOrchestrator::Entry e;
        Coordination::Stat stat;

        e.znode_path = entries_znode_path + "/tasks/" + task_name;

        auto entry_str = zk->get(e.znode_path, &stat);
        e.fromString(entry_str);

        e.version = stat.version;
        e.znode_name = task_name;

        tasks.push_back(std::move(e));
    }
    return tasks;
}

std::optional<PartMovesBetweenShardsOrchestrator::Entry> PartMovesBetweenShardsOrchestrator::selectEntryFromZk()
{
    std::lock_guard lock(state_mutex);
    auto zk = storage.getZooKeeper();

    Strings signaled_entries = zk->getChildren(entries_znode_path + "/task_queue");

    for (String & signaled_entry : signaled_entries)
    {
        Entry entry_to_process;
        Coordination::Stat stat;
        entry_to_process.znode_path = entries_znode_path + "/tasks/" + signaled_entry;
        auto entry_str = zk->get(entry_to_process.znode_path, &stat);
        entry_to_process.fromString(entry_str);
        entry_to_process.version = stat.version;
        entry_to_process.znode_name = signaled_entry;

        try
        {
            zk->create(entry_to_process.znode_path + "/replica", storage.replica_name, zkutil::CreateMode::Ephemeral);
            return entry_to_process;
        }
        catch (const Coordination::Exception & e)
        {
            if (e.code == Coordination::Error::ZNODEEXISTS)
            {
                LOG_DEBUG(log, "Task {} is being processed by another replica", entry_to_process.znode_name);
            }
            else
            {
                throw;
            }
        }
    }
    return std::nullopt;
}

void PartMovesBetweenShardsOrchestrator::step(Entry & entry)
{
    auto zk = storage.getZooKeeper();

    LOG_DEBUG(log, "stepEntry on task {} from state {} (rollback: {}), try: {}",
              entry.znode_name,
              entry.state.toString(),
              entry.rollback,
              entry.num_tries);

    EntryState current_state = entry.state;

    if (!entry.rollback)
    {
        entry.state = getNextState(entry);
        if (entry.state.value == current_state.value)
        {
           throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot process the step entry with out quorum success for state {}", entry.state.value);
        }
    }
    Coordination::Requests ops;

    try
    {
        stepEntry(entry);
        entry.num_tries = 0;
        entry.last_exception_msg = "";
        entry.replicas.clear();
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        entry.last_exception_msg = getCurrentExceptionMessage(false);
        entry.num_tries += 1;
        entry.state = current_state;
    }

    entry.update_time = std::time(nullptr);
    ops.emplace_back(zkutil::makeSetRequest(entry.znode_path, entry.toString(), entry.version));
    ops.emplace_back(zkutil::makeRemoveRequest(entry.znode_path+ "/replica", -1));
    Coordination::Responses responses;
    Coordination::Error rc = zk->tryMulti(ops, responses);
    zkutil::KeeperMultiException::check(rc, ops, responses);
}

PartMovesBetweenShardsOrchestrator::EntryState PartMovesBetweenShardsOrchestrator::getNextState(Entry & entry) const
{
    if (entry.state.value == EntryState::TODO || entry.replicas.size() == entry.required_number_of_replicas)
    {
        return EntryState::nextState(entry.state.value);
    }
    return entry.state;
}

void PartMovesBetweenShardsOrchestrator::stepEntry(Entry & entry)
{
    auto zk = storage.getZooKeeper();
    switch (entry.state.value)
    {
        case EntryState::DONE: [[fallthrough]];
        case EntryState::CANCELLED:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't stepEntry after terminal state. This is a bug.");

        case EntryState::TODO:
        {
            if (entry.rollback)
            {
                removePins(entry,zk);
                entry.state = EntryState::CANCELLED;
            }
            /// The forward transition happens implicitly when task is created by `StorageReplicatedMergeTree::movePartitionToShard`.
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected entry state ({}) in stepEntry. This is a bug.", entry.state.toString());
            break;
        }

        case EntryState::SYNC_SOURCE:
        {
            if (entry.rollback)
            {
                entry.state = EntryState::TODO;
            }
            else
            {
                entry.required_number_of_replicas = getQuorum(zookeeper_path);
                ReplicatedMergeTreeLogEntryData sync_source_log_entry;

               /// Log entry.
                Coordination::Requests ops;
                ops.emplace_back(zkutil::makeCheckRequest(entry.znode_path, entry.version));

                sync_source_log_entry.type = ReplicatedMergeTreeLogEntryData::SYNC_PINNED_PART_UUIDS;
                sync_source_log_entry.create_time = std::time(nullptr);
                sync_source_log_entry.source_replica = storage.replica_name;
                sync_source_log_entry.task_name = entry.znode_name;
                sync_source_log_entry.task_entry_zk_path = entries_znode_path;

                ops.emplace_back(zkutil::makeSetRequest(zookeeper_path + "/log", "", -1));
                ops.emplace_back(zkutil::makeCreateRequest(
                    zookeeper_path + "/log/log-", sync_source_log_entry.toString(), zkutil::CreateMode::PersistentSequential));
                ops.emplace_back(zkutil::makeRemoveRequest(entries_znode_path + "/task_queue/" + entry.znode_name, -1));

                Coordination::Responses responses;
                Coordination::Error rc = zk->tryMulti(ops, responses);
                zkutil::KeeperMultiException::check(rc, ops, responses);
                LOG_DEBUG(log, "Pushed log entry for task {} and state {}", entry.znode_name, entry.state.toString());
            }
            break;
        }

        case EntryState::SYNC_DESTINATION:
        {
            if (entry.rollback)
            {
                entry.state = EntryState::SYNC_SOURCE;
            }
            else
            {
                entry.required_number_of_replicas = getQuorum(entry.to_shard);
                ReplicatedMergeTreeLogEntryData sync_destination_log_entry;

                /// Log entry.
                Coordination::Requests ops;
                ops.emplace_back(zkutil::makeCheckRequest(entry.znode_path, entry.version));

                sync_destination_log_entry.type = ReplicatedMergeTreeLogEntryData::SYNC_PINNED_PART_UUIDS;
                sync_destination_log_entry.create_time = std::time(nullptr);
                sync_destination_log_entry.source_replica = storage.replica_name;
                sync_destination_log_entry.source_shard = zookeeper_path;
                sync_destination_log_entry.task_name = entry.znode_name;
                sync_destination_log_entry.task_entry_zk_path = entries_znode_path;

                ops.emplace_back(zkutil::makeSetRequest(entry.to_shard + "/log", "", -1));
                ops.emplace_back(zkutil::makeCreateRequest(
                    entry.to_shard + "/log/log-", sync_destination_log_entry.toString(), zkutil::CreateMode::PersistentSequential));
                ops.emplace_back(zkutil::makeRemoveRequest(entries_znode_path + "/task_queue/" + entry.znode_name, -1));

                Coordination::Responses responses;
                Coordination::Error rc = zk->tryMulti(ops, responses);
                zkutil::KeeperMultiException::check(rc, ops, responses);
                LOG_DEBUG(log, "Pushed log entry for task {} and state {}", entry.znode_name, entry.state.toString());
            }
            break;
        }

        case EntryState::DESTINATION_FETCH:
        {
            if (entry.rollback)
            {
                // TODO(nv): Do we want to cleanup fetched data on the destination?
                //   Maybe leave it there and make sure a background cleanup will take
                //   care of it sometime later.

                entry.state = EntryState::SYNC_DESTINATION;
            }
            else
            {
                /// Note: Table structure shouldn't be changed while there are part movements in progress.
                entry.required_number_of_replicas = getQuorum(entry.to_shard);

                ReplicatedMergeTreeLogEntryData fetch_log_entry;

                Coordination::Requests ops;
                ops.emplace_back(zkutil::makeCheckRequest(entry.znode_path, entry.version));

                fetch_log_entry.type = ReplicatedMergeTreeLogEntryData::CLONE_PART_FROM_SHARD;
                fetch_log_entry.create_time = std::time(nullptr);
                fetch_log_entry.new_part_name = entry.part_name;
                fetch_log_entry.source_replica = storage.replica_name;
                fetch_log_entry.source_shard = zookeeper_path;
                fetch_log_entry.task_name = entry.znode_name;
                fetch_log_entry.task_entry_zk_path = entries_znode_path;

                ops.emplace_back(zkutil::makeSetRequest(entry.to_shard + "/log", "", -1));
                ops.emplace_back(zkutil::makeCreateRequest(
                    entry.to_shard + "/log/log-", fetch_log_entry.toString(), zkutil::CreateMode::PersistentSequential));
                ops.emplace_back(zkutil::makeRemoveRequest(entries_znode_path + "/task_queue/" + entry.znode_name, -1));

                Coordination::Responses responses;
                Coordination::Error rc = zk->tryMulti(ops, responses);
                zkutil::KeeperMultiException::check(rc, ops, responses);

                LOG_DEBUG(log, "Pushed log entry for task {} and state {}", entry.znode_name, entry.state.toString());
            }
            break;
        }

        case EntryState::DESTINATION_ATTACH:
        {
            String attach_log_entry_barrier_path = fs::path(entry.znode_path) / ("log_" + entry.state.toString());

            if (entry.rollback)
            {
                Coordination::Stat attach_log_entry_stat;
                String attach_log_entry_str;
                if (!zk->tryGet(attach_log_entry_barrier_path, attach_log_entry_str, &attach_log_entry_stat))
                {
                    LOG_DEBUG(log, "Log entry for DESTINATION_ATTACH not found. Not sending DROP_RANGE log entry.");

                    // ATTACH_PART wasn't issued, nothing to revert.
                    entry.state = EntryState::DESTINATION_FETCH;
                }
                else
                {
                    // Need to remove ATTACH_PART from the queue or drop data.
                    // Similar to `StorageReplicatedMergeTree::dropPart` without extra
                    // checks as we know drop shall be possible.
                    ReplicatedMergeTreeLogEntryData attach_rollback_log_entry;

                    String attach_rollback_log_entry_barrier_path = fs::path(entry.znode_path) / ("log_" + entry.state.toString() + "_rollback");
                    Coordination::Stat attach_rollback_log_entry_stat;
                    String attach_rollback_log_entry_str;
                    if (zk->tryGet(attach_rollback_log_entry_barrier_path, attach_rollback_log_entry_str, &attach_rollback_log_entry_stat))
                    {
                        LOG_DEBUG(log, "Log entry was already created will check the existing one.");

                        attach_rollback_log_entry = *ReplicatedMergeTreeLogEntry::parse(attach_rollback_log_entry_str,
                                                                                        attach_rollback_log_entry_stat,
                                                                                        storage.format_version);
                    }
                    else
                    {
                        const auto attach_log_entry = ReplicatedMergeTreeLogEntry::parse(attach_log_entry_str, attach_log_entry_stat,
                                                                                         storage.format_version);

                        Coordination::Requests ops;
                        ops.emplace_back(zkutil::makeCheckRequest(entry.znode_path, entry.version));

                        auto drop_part_info = MergeTreePartInfo::fromPartName(attach_log_entry->new_part_name, storage.format_version);

                        storage.getClearBlocksInPartitionOps(
                            ops, *zk, drop_part_info.partition_id, drop_part_info.min_block, drop_part_info.max_block);
                        size_t clear_block_ops_size = ops.size();

                        attach_rollback_log_entry.type = ReplicatedMergeTreeLogEntryData::DROP_RANGE;
                        attach_rollback_log_entry.log_entry_id = attach_rollback_log_entry_barrier_path;
                        attach_rollback_log_entry.source_replica = storage.replica_name;
                        attach_rollback_log_entry.source_shard = zookeeper_path;

                        attach_rollback_log_entry.new_part_name = getPartNamePossiblyFake(storage.format_version, drop_part_info);
                        attach_rollback_log_entry.create_time = time(nullptr);

                        ops.emplace_back(zkutil::makeCreateRequest(attach_rollback_log_entry_barrier_path, attach_rollback_log_entry.toString(), -1));
                        ops.emplace_back(zkutil::makeCreateRequest(
                            entry.to_shard + "/log/log-", attach_rollback_log_entry.toString(), zkutil::CreateMode::PersistentSequential));
                        ops.emplace_back(zkutil::makeSetRequest(entry.to_shard + "/log", "", -1));
                        Coordination::Responses responses;
                        Coordination::Error rc = zk->tryMulti(ops, responses);
                        zkutil::KeeperMultiException::check(rc, ops, responses);

                        String log_znode_path
                            = dynamic_cast<const Coordination::CreateResponse &>(*responses[clear_block_ops_size]).path_created;
                        attach_rollback_log_entry.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);

                        LOG_DEBUG(log, "Pushed log entry: {}", log_znode_path);
                    }

                    Strings unwaited = storage.tryWaitForAllReplicasToProcessLogEntry(entry.to_shard, attach_rollback_log_entry, 1);
                    if (!unwaited.empty())
                        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Some replicas haven't processed event: {}, will retry later.", toString(unwaited));

                    entry.state = EntryState::DESTINATION_FETCH;
                }
            }
            else
            {
                entry.required_number_of_replicas = getQuorum(entry.to_shard);
                /// There is a chance that attach on destination will fail and this task will be left in the queue forever.
                Coordination::Requests ops;
                ops.emplace_back(zkutil::makeCheckRequest(entry.znode_path, entry.version));

                auto part = storage.getActiveContainingPart(entry.part_name);

                /// Allocating block number in other replicas zookeeper path
                /// TODO Maybe we can do better.
                auto block_number_lock = storage.allocateBlockNumber(part->info.partition_id, zk, attach_log_entry_barrier_path, entry.to_shard);

                ReplicatedMergeTreeLogEntryData log_entry;

                auto block_number = block_number_lock->getNumber();

                auto part_info = part->info;
                part_info.min_block = block_number;
                part_info.max_block = block_number;
                part_info.level = 0;
                part_info.mutation = 0;

                /// Attach log entry (all replicas already fetched part)
                log_entry.type = ReplicatedMergeTreeLogEntryData::ATTACH_PART;
                log_entry.log_entry_id = attach_log_entry_barrier_path;
                log_entry.part_checksum = part->checksums.getTotalChecksumHex();
                log_entry.create_time = std::time(nullptr);
                log_entry.new_part_name = part_info.getPartNameAndCheckFormat(storage.format_version);
                log_entry.task_name = entry.znode_name;
                log_entry.task_entry_zk_path = entries_znode_path;

                ops.emplace_back(zkutil::makeCreateRequest(attach_log_entry_barrier_path, log_entry.toString(), -1));
                ops.emplace_back(zkutil::makeSetRequest(entry.to_shard + "/log", "", -1));
                ops.emplace_back(zkutil::makeCreateRequest(
                    entry.to_shard + "/log/log-", log_entry.toString(), zkutil::CreateMode::PersistentSequential));
                ops.emplace_back(zkutil::makeRemoveRequest(entries_znode_path + "/task_queue/" + entry.znode_name, -1));

                Coordination::Responses responses;
                Coordination::Error rc = zk->tryMulti(ops, responses);
                zkutil::KeeperMultiException::check(rc, ops, responses);

                LOG_DEBUG(log, "Pushed log entry for task {} and state {}", entry.znode_name, entry.state.toString());

                entry.dst_part_name = log_entry.new_part_name;
            }
            break;
        }

        case EntryState::SOURCE_DROP:
        {
            if (entry.rollback)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "It is not possible to rollback from this state. This is a bug.");
            else
            {
                entry.required_number_of_replicas = getQuorum(zookeeper_path);
                // Can't use dropPartImpl directly as we need additional zk ops to remember the log entry
                // for subsequent retries.

                ReplicatedMergeTreeLogEntryData source_drop_log_entry;
                auto source_drop_part_info = MergeTreePartInfo::fromPartName(entry.part_name, storage.format_version);

                Coordination::Requests ops;
                ops.emplace_back(zkutil::makeCheckRequest(entry.znode_path, entry.version));

                storage.getClearBlocksInPartitionOps(ops, *zk, source_drop_part_info.partition_id, source_drop_part_info.min_block, source_drop_part_info.max_block);

                source_drop_log_entry.type = ReplicatedMergeTreeLogEntryData::DROP_RANGE;
                source_drop_log_entry.create_time = std::time(nullptr);
                source_drop_log_entry.new_part_name = getPartNamePossiblyFake(storage.format_version, source_drop_part_info);
                source_drop_log_entry.source_replica = storage.replica_name;
                source_drop_log_entry.task_name = entry.znode_name;
                source_drop_log_entry.task_entry_zk_path = entries_znode_path;

                ops.emplace_back(zkutil::makeSetRequest(zookeeper_path + "/log", "", -1));
                ops.emplace_back(zkutil::makeCreateRequest(
                    zookeeper_path + "/log/log-", source_drop_log_entry.toString(), zkutil::CreateMode::PersistentSequential));
                ops.emplace_back(zkutil::makeRemoveRequest(entries_znode_path + "/task_queue/" + entry.znode_name, -1));

                Coordination::Responses responses;
                Coordination::Error rc = zk->tryMulti(ops, responses);
                zkutil::KeeperMultiException::check(rc, ops, responses);

                LOG_DEBUG(log, "Pushed log entry for task {} and state {}", entry.znode_name, entry.state.toString());
            }
            break;
        }

        case EntryState::REMOVE_UUID_PIN:
        {
            if (entry.rollback)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "It is not possible to rollback from this state. This is a bug.");
            else
            {
                removePins(entry,zk);
                entry.state = EntryState::DONE;
            }
            break;
        }
    }
}

void PartMovesBetweenShardsOrchestrator::removePins(Entry & entry, zkutil::ZooKeeperPtr zk)
{
    PinnedPartUUIDs src_pins;
    PinnedPartUUIDs dst_pins;

    {
        String s = zk->get(zookeeper_path + "/pinned_part_uuids", &src_pins.stat);
        src_pins.fromString(s);
    }

    {
        String s = zk->get(entry.to_shard + "/pinned_part_uuids", &dst_pins.stat);
        dst_pins.fromString(s);
    }

    dst_pins.part_uuids.erase(entry.part_uuid);
    src_pins.part_uuids.erase(entry.part_uuid);

    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeSetRequest(zookeeper_path + "/pinned_part_uuids", src_pins.toString(), src_pins.stat.version));
    ops.emplace_back(zkutil::makeSetRequest(entry.to_shard + "/pinned_part_uuids", dst_pins.toString(), dst_pins.stat.version));
    ops.emplace_back(zkutil::makeRemoveRequest(entries_znode_path + "/task_queue/" + entry.znode_name, -1));

    zk->multi(ops);
}

CancellationCode PartMovesBetweenShardsOrchestrator::killPartMoveToShard(const UUID & task_uuid)
{
    while (true)
    {
        auto entry_to_process = getEntryByUUID(task_uuid);

        // If the task is in this state or any that follows it is too late to rollback
        // since we can't be sure if the source data still exists.
        auto not_possible_to_rollback_after_state = EntryState(EntryState::SOURCE_DROP);
        if (entry_to_process.state.value >= not_possible_to_rollback_after_state.value)
        {
            LOG_DEBUG(log, "Can't kill move part between shards entry {} ({}) after state {}. Current state: {}.",
                      toString(entry_to_process.task_uuid), entry_to_process.znode_name, not_possible_to_rollback_after_state.toString(), entry_to_process.state.toString());
            return CancellationCode::CancelCannotBeSent;
        }

        LOG_TRACE(log, "Will try to mark move part between shards entry {} ({}) for rollback.",
                  toString(entry_to_process.task_uuid), entry_to_process.znode_name);

        auto zk = storage.getZooKeeper();

        // State transition.
        entry_to_process.rollback = true;
        entry_to_process.update_time = std::time(nullptr);
        entry_to_process.num_tries = 0;
        entry_to_process.last_exception_msg = "";

        String task_name = "task-" + toString(entry_to_process.task_uuid);

        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeSetRequest(entry_to_process.znode_path, entry_to_process.toString(), entry_to_process.version));
        ops.emplace_back(zkutil::makeCreateRequest(fs::path(entries_znode_path) / "task_queue" / task_name, "", zkutil::CreateMode::Persistent, true));

        Coordination::Responses responses;
        auto code = zk->tryMulti(ops, responses);

        if (code == Coordination::Error::ZOK)
        {
            // Orchestrator will process it in background.
            return CancellationCode::CancelSent;
        }
        else if (code == Coordination::Error::ZBADVERSION)
        {
            /// Node was updated meanwhile. We must re-read it and repeat all the actions.
            continue;
        }
        else
            throw Coordination::Exception::fromPath(code, entry_to_process.znode_path);
    }
}

std::vector<PartMovesBetweenShardsOrchestrator::Entry> PartMovesBetweenShardsOrchestrator::getEntries()
{
    return getEntriesFromZk();
}

PartMovesBetweenShardsOrchestrator::Entry PartMovesBetweenShardsOrchestrator::getEntryByUUID(const UUID & task_uuid)
{
    /// Need latest state in case user tries to kill a move observed on a different replica.
    std::vector<Entry> all_entries = getEntriesFromZk();

    std::lock_guard lock(state_mutex);
    for (auto const & entry_to_process : all_entries)
    {
        if (entry_to_process.task_uuid == task_uuid)
            return entry_to_process;
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Task with id {} not found", task_uuid);
}

String PartMovesBetweenShardsOrchestrator::Entry::toString() const
{
    Poco::JSON::Object json;

    json.set(JSON_KEY_CREATE_TIME, DB::toString(create_time));
    json.set(JSON_KEY_UPDATE_TIME, DB::toString(update_time));
    json.set(JSON_KEY_TASK_UUID, DB::toString(task_uuid));
    json.set(JSON_KEY_PART_NAME, part_name);
    json.set(JSON_KEY_PART_UUID, DB::toString(part_uuid));
    json.set(JSON_KEY_TO_SHARD, to_shard);
    json.set(JSON_KEY_DST_PART_NAME, dst_part_name);
    json.set(JSON_KEY_STATE, state.toString());
    json.set(JSON_KEY_ROLLBACK, DB::toString(rollback));
    json.set(JSON_KEY_LAST_EX_MSG, last_exception_msg);
    json.set(JSON_KEY_NUM_TRIES, DB::toString(num_tries));
    json.set(JSON_KEY_REQUIRED_NUM_REPLICAS, DB::toString(required_number_of_replicas));
    json.set(JSON_KEY_REPLICAS, DB::toString(replicas));

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);

    // Always escape unicode to make last_exception_msg json safe.
    // It may contain random binary data when exception is a parsing error
    // of unexpected contents.
    Poco::JSON::Stringifier::stringify(json, oss, 0, -1, Poco::JSON_WRAP_STRINGS | Poco::JSON_ESCAPE_UNICODE);

    return oss.str();
}

void PartMovesBetweenShardsOrchestrator::Entry::fromString(const String & buf)
{
    Poco::JSON::Parser parser;
    auto json = parser.parse(buf).extract<Poco::JSON::Object::Ptr>();

    create_time = parseFromString<time_t>(json->getValue<std::string>(JSON_KEY_CREATE_TIME));
    update_time = parseFromString<time_t>(json->getValue<std::string>(JSON_KEY_UPDATE_TIME));
    task_uuid = parseFromString<UUID>(json->getValue<std::string>(JSON_KEY_TASK_UUID));
    part_name = json->getValue<std::string>(JSON_KEY_PART_NAME);
    part_uuid = parseFromString<UUID>(json->getValue<std::string>(JSON_KEY_PART_UUID));
    to_shard = json->getValue<std::string>(JSON_KEY_TO_SHARD);
    dst_part_name = json->getValue<std::string>(JSON_KEY_DST_PART_NAME);
    state.value = EntryState::fromString(json->getValue<std::string>(JSON_KEY_STATE));
    rollback = json->getValue<bool>(JSON_KEY_ROLLBACK);
    last_exception_msg = json->getValue<std::string>(JSON_KEY_LAST_EX_MSG);
    num_tries = json->getValue<UInt64>(JSON_KEY_NUM_TRIES);
    required_number_of_replicas = json->getValue<UInt64>(JSON_KEY_REQUIRED_NUM_REPLICAS);
    replicas = parseFromString<std::vector<String>>(json->getValue<std::string>(JSON_KEY_REPLICAS));
}

UInt64 PartMovesBetweenShardsOrchestrator::getQuorum(String zk_path)
{
    auto zk = storage.getZooKeeper();
    Strings replicas = zk->getChildren(fs::path(zk_path) / "replicas");
    return replicas.size();
}
}
