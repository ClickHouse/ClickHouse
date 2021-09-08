#include <Storages/MergeTree/PartMovesBetweenShardsOrchestrator.h>
#include <Storages/MergeTree/PinnedPartUUIDs.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <boost/range/adaptor/map.hpp>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

namespace DB
{
PartMovesBetweenShardsOrchestrator::PartMovesBetweenShardsOrchestrator(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , zookeeper_path(storage.zookeeper_path)
    , logger_name(storage.getStorageID().getFullTableName() + " (PartMovesBetweenShardsOrchestrator)")
    , log(&Poco::Logger::get(logger_name))
    , entries_znode_path(zookeeper_path + "/part_moves_shard")
{
    /// Schedule pool is not designed for long-running tasks. TODO replace with a separate thread?
    task = storage.getContext()->getSchedulePool().createTask(logger_name, [this]{ run(); });
}

void PartMovesBetweenShardsOrchestrator::run()
{
    if (!storage.getSettings()->part_moves_between_shards_enable)
        return;

    if (need_stop)
        return;

    auto sleep_ms = 10;

    try
    {
        fetchStateFromZK();

        if (step())
            fetchStateFromZK();
        else
            sleep_ms = 3 * 1000;
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

void PartMovesBetweenShardsOrchestrator::fetchStateFromZK()
{
    std::lock_guard lock(state_mutex);

    entries.clear();

    auto zk = storage.getZooKeeper();

    Strings task_names = zk->getChildren(entries_znode_path);
    for (auto const & task_name : task_names)
    {
        PartMovesBetweenShardsOrchestrator::Entry e;
        Coordination::Stat stat;

        e.znode_path = entries_znode_path + "/" + task_name;

        auto entry_str = zk->get(e.znode_path, &stat);
        e.fromString(entry_str);

        e.version = stat.version;
        e.znode_name = task_name;

        entries[task_name] = std::move(e);
    }
}

bool PartMovesBetweenShardsOrchestrator::step()
{
    if (!storage.is_leader)
        return false;

    auto zk = storage.getZooKeeper();

    std::optional<Entry> entry_to_process;

    /// Try find an entry to process and copy it.
    {
        std::lock_guard lock(state_mutex);

        for (auto const & entry : entries | boost::adaptors::map_values)
        {
            if (entry.state.value == EntryState::DONE || entry.state.value == EntryState::CANCELLED)
                continue;

            entry_to_process.emplace(entry);
            break;
        }
    }

    if (!entry_to_process.has_value())
        return false;

    /// Since some state transitions are long running (waiting on replicas acknowledgement we create this lock to avoid
    /// other replicas trying to do the same work. All state transitions should be idempotent so is is safe to lose the
    /// lock and have another replica retry.
    ///
    /// Note: This blocks all other entries from being executed. Technical debt.
    zkutil::EphemeralNodeHolder::Ptr entry_node_holder;

    try
    {
        entry_node_holder = zkutil::EphemeralNodeHolder::create(entry_to_process->znode_path + "/lock_holder", *zk, storage.replica_name);
    }
    catch (const Coordination::Exception & e)
    {
        if (e.code == Coordination::Error::ZNODEEXISTS)
        {
            LOG_DEBUG(log, "Task {} is being processed by another replica", entry_to_process->znode_name);
            return false;
        }

        throw;
    }

    try
    {
        /// Use the same ZooKeeper connection. If we'd lost the lock then connection
        /// will become expired and all consequent operations will fail.
        stepEntry(entry_to_process.value(), zk);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

        Entry entry_copy = entry_to_process.value();
        entry_copy.last_exception_msg = getCurrentExceptionMessage(false);
        entry_copy.update_time = std::time(nullptr);
        zk->set(entry_copy.znode_path, entry_copy.toString(), entry_copy.version);

        return false;
    }

    return true;
}

void PartMovesBetweenShardsOrchestrator::stepEntry(const Entry & entry, zkutil::ZooKeeperPtr zk)
{
    switch (entry.state.value)
    {
        case EntryState::DONE:
            break;

        case EntryState::CANCELLED:
            break;

        case EntryState::TODO:
        {
            /// State transition.
            Entry entry_copy = entry;
            entry_copy.state = EntryState::SYNC_SOURCE;
            entry_copy.update_time = std::time(nullptr);
            zk->set(entry_copy.znode_path, entry_copy.toString(), entry_copy.version);
        }
        break;

        case EntryState::SYNC_SOURCE:
        {
            {
                /// Log entry.
                Coordination::Requests ops;
                ops.emplace_back(zkutil::makeCheckRequest(entry.znode_path, entry.version));

                ReplicatedMergeTreeLogEntryData log_entry;
                log_entry.type = ReplicatedMergeTreeLogEntryData::SYNC_PINNED_PART_UUIDS;
                log_entry.create_time = std::time(nullptr);
                log_entry.source_replica = storage.replica_name;
                ops.emplace_back(zkutil::makeSetRequest(zookeeper_path + "/log", "", -1));
                ops.emplace_back(zkutil::makeCreateRequest(
                    zookeeper_path + "/log/log-", log_entry.toString(), zkutil::CreateMode::PersistentSequential));

                Coordination::Responses responses;
                Coordination::Error rc = zk->tryMulti(ops, responses);
                zkutil::KeeperMultiException::check(rc, ops, responses);

                String log_znode_path = dynamic_cast<const Coordination::CreateResponse &>(*responses.back()).path_created;
                log_entry.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);

                /// This wait in background schedule pool is useless. It'd be
                /// better to have some notification which will call `step`
                /// function when all replicated will finish. TODO.
                storage.waitForAllReplicasToProcessLogEntry(log_entry, true);
            }

            {
                /// State transition.
                Entry entry_copy = entry;
                entry_copy.state = EntryState::SYNC_DESTINATION;
                entry_copy.update_time = std::time(nullptr);
                zk->set(entry_copy.znode_path, entry_copy.toString(), entry_copy.version);
            }
        }
        break;

        case EntryState::SYNC_DESTINATION:
        {
            {
                /// Log entry.
                Coordination::Requests ops;
                ops.emplace_back(zkutil::makeCheckRequest(entry.znode_path, entry.version));

                ReplicatedMergeTreeLogEntryData log_entry;
                log_entry.type = ReplicatedMergeTreeLogEntryData::SYNC_PINNED_PART_UUIDS;
                log_entry.create_time = std::time(nullptr);
                log_entry.source_replica = storage.replica_name;
                log_entry.source_shard = zookeeper_path;

                ops.emplace_back(zkutil::makeSetRequest(entry.to_shard + "/log", "", -1));
                ops.emplace_back(zkutil::makeCreateRequest(
                    entry.to_shard + "/log/log-", log_entry.toString(), zkutil::CreateMode::PersistentSequential));

                Coordination::Responses responses;
                Coordination::Error rc = zk->tryMulti(ops, responses);
                zkutil::KeeperMultiException::check(rc, ops, responses);

                String log_znode_path = dynamic_cast<const Coordination::CreateResponse &>(*responses.back()).path_created;
                log_entry.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);

                storage.waitForAllTableReplicasToProcessLogEntry(entry.to_shard, log_entry, true);
            }

            {
                /// State transition.
                Entry entry_copy = entry;
                entry_copy.state = EntryState::DESTINATION_FETCH;
                entry_copy.update_time = std::time(nullptr);
                zk->set(entry_copy.znode_path, entry_copy.toString(), entry_copy.version);
            }
        }
        break;

        case EntryState::DESTINATION_FETCH:
        {
            /// Make sure table structure doesn't change when there are part movements in progress.
            {
                Coordination::Requests ops;
                ops.emplace_back(zkutil::makeCheckRequest(entry.znode_path, entry.version));

                /// Log entry.
                ReplicatedMergeTreeLogEntryData log_entry;
                log_entry.type = ReplicatedMergeTreeLogEntryData::CLONE_PART_FROM_SHARD;
                log_entry.create_time = std::time(nullptr);
                log_entry.new_part_name = entry.part_name;
                log_entry.source_replica = storage.replica_name;
                log_entry.source_shard = zookeeper_path;
                ops.emplace_back(zkutil::makeSetRequest(entry.to_shard + "/log", "", -1));
                ops.emplace_back(zkutil::makeCreateRequest(
                    entry.to_shard + "/log/log-", log_entry.toString(), zkutil::CreateMode::PersistentSequential));

                Coordination::Responses responses;
                Coordination::Error rc = zk->tryMulti(ops, responses);
                zkutil::KeeperMultiException::check(rc, ops, responses);

                String log_znode_path = dynamic_cast<const Coordination::CreateResponse &>(*responses.back()).path_created;
                log_entry.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);

                storage.waitForAllTableReplicasToProcessLogEntry(entry.to_shard, log_entry, true);
            }

            {
                /// State transition.
                Entry entry_copy = entry;
                entry_copy.state = EntryState::DESTINATION_ATTACH;
                entry_copy.update_time = std::time(nullptr);
                zk->set(entry_copy.znode_path, entry_copy.toString(), entry_copy.version);
            }
        }
        break;

        case EntryState::DESTINATION_ATTACH:
        {
            /// There is a chance that attach on destination will fail and this task will be left in the queue forever.
            {
                Coordination::Requests ops;
                ops.emplace_back(zkutil::makeCheckRequest(entry.znode_path, entry.version));

                auto part = storage.getActiveContainingPart(entry.part_name);
                /// Allocating block number in other replicas zookeeper path
                /// TODO Maybe we can do better.
                auto block_number_lock = storage.allocateBlockNumber(part->info.partition_id, zk, "", entry.to_shard);
                auto block_number = block_number_lock->getNumber();

                auto part_info = part->info;
                part_info.min_block = block_number;
                part_info.max_block = block_number;
                part_info.level = 0;
                part_info.mutation = 0;

                /// Attach log entry (all replicas already fetched part)
                ReplicatedMergeTreeLogEntryData log_entry;
                log_entry.type = ReplicatedMergeTreeLogEntryData::ATTACH_PART;
                log_entry.part_checksum = part->checksums.getTotalChecksumHex();
                log_entry.create_time = std::time(nullptr);
                log_entry.new_part_name = part_info.getPartName();
                ops.emplace_back(zkutil::makeSetRequest(entry.to_shard + "/log", "", -1));
                ops.emplace_back(zkutil::makeCreateRequest(
                    entry.to_shard + "/log/log-", log_entry.toString(), zkutil::CreateMode::PersistentSequential));

                Coordination::Responses responses;
                Coordination::Error rc = zk->tryMulti(ops, responses);
                zkutil::KeeperMultiException::check(rc, ops, responses);

                String log_znode_path = dynamic_cast<const Coordination::CreateResponse &>(*responses.back()).path_created;
                log_entry.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);

                storage.waitForAllTableReplicasToProcessLogEntry(entry.to_shard, log_entry, true);
            }

            {
                /// State transition.
                Entry entry_copy = entry;
                entry_copy.state = EntryState::SOURCE_DROP_PRE_DELAY;
                entry_copy.update_time = std::time(nullptr);
                zk->set(entry_copy.znode_path, entry_copy.toString(), entry_copy.version);
            }
        }
        break;

        case EntryState::SOURCE_DROP_PRE_DELAY:
        {
            std::this_thread::sleep_for(std::chrono::seconds(storage.getSettings()->part_moves_between_shards_delay_seconds));

            /// State transition.
            Entry entry_copy = entry;
            entry_copy.state = EntryState::SOURCE_DROP;
            entry_copy.update_time = std::time(nullptr);
            zk->set(entry_copy.znode_path, entry_copy.toString(), entry_copy.version);
        }
        break;

        case EntryState::SOURCE_DROP:
        {
            {
                ReplicatedMergeTreeLogEntry log_entry;
                if (storage.dropPartImpl(zk, entry.part_name, log_entry, false, false))
                    storage.waitForAllReplicasToProcessLogEntry(log_entry, true);
            }

            {
                /// State transition.
                Entry entry_copy = entry;
                entry_copy.state = EntryState::SOURCE_DROP_POST_DELAY;
                entry_copy.update_time = std::time(nullptr);
                zk->set(entry_copy.znode_path, entry_copy.toString(), entry_copy.version);
            }
        }
        break;

        case EntryState::SOURCE_DROP_POST_DELAY:
        {
            std::this_thread::sleep_for(std::chrono::seconds(storage.getSettings()->part_moves_between_shards_delay_seconds));

            /// State transition.
            Entry entry_copy = entry;
            entry_copy.state = EntryState::REMOVE_UUID_PIN;
            entry_copy.update_time = std::time(nullptr);
            zk->set(entry_copy.znode_path, entry_copy.toString(), entry_copy.version);
        }
        break;

        case EntryState::REMOVE_UUID_PIN:
        {
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

                src_pins.part_uuids.erase(entry.part_uuid);
                dst_pins.part_uuids.erase(entry.part_uuid);

                Coordination::Requests ops;
                ops.emplace_back(zkutil::makeSetRequest(zookeeper_path + "/pinned_part_uuids", src_pins.toString(), src_pins.stat.version));
                ops.emplace_back(zkutil::makeSetRequest(entry.to_shard + "/pinned_part_uuids", dst_pins.toString(), dst_pins.stat.version));

                zk->multi(ops);
            }

            /// State transition.
            Entry entry_copy = entry;
            entry_copy.state = EntryState::DONE;
            entry_copy.update_time = std::time(nullptr);
            zk->set(entry_copy.znode_path, entry_copy.toString(), entry_copy.version);
        }
        break;
    }
}

std::vector<PartMovesBetweenShardsOrchestrator::Entry> PartMovesBetweenShardsOrchestrator::getEntries() const
{
    std::lock_guard lock(state_mutex);

    std::vector<Entry> res;

    for (const auto & e : entries)
        res.push_back(e.second);

    return res;
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
    json.set(JSON_KEY_STATE, state.toString());
    json.set(JSON_KEY_LAST_EX_MSG, last_exception_msg);

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    json.stringify(oss);

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
    state.value = EntryState::fromString(json->getValue<std::string>(JSON_KEY_STATE));
    last_exception_msg = json->getValue<std::string>(JSON_KEY_LAST_EX_MSG);
}

}
