#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumEntry.h>
#include <Storages/MergeTree/ReplicatedMergeTreeMergeStrategyPicker.h>
#include <Common/StringUtils/StringUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNEXPECTED_NODE_IN_ZOOKEEPER;
    extern const int UNFINISHED;
    extern const int ABORTED;
}


ReplicatedMergeTreeQueue::ReplicatedMergeTreeQueue(StorageReplicatedMergeTree & storage_, ReplicatedMergeTreeMergeStrategyPicker & merge_strategy_picker_)
    : storage(storage_)
    , merge_strategy_picker(merge_strategy_picker_)
    , format_version(storage.format_version)
    , current_parts(format_version)
    , virtual_parts(format_version)
{
    zookeeper_path = storage.zookeeper_path;
    replica_path = storage.replica_path;
    logger_name = storage.getStorageID().getFullTableName() + " (ReplicatedMergeTreeQueue)";
    log = &Poco::Logger::get(logger_name);
}


void ReplicatedMergeTreeQueue::initialize(const MergeTreeData::DataParts & parts)
{
    addVirtualParts(parts);
}


void ReplicatedMergeTreeQueue::addVirtualParts(const MergeTreeData::DataParts & parts)
{
    std::lock_guard lock(state_mutex);

    for (const auto & part : parts)
    {
        current_parts.add(part->name);
        virtual_parts.add(part->name);
    }
}


bool ReplicatedMergeTreeQueue::isVirtualPart(const MergeTreeData::DataPartPtr & data_part) const
{
    std::lock_guard lock(state_mutex);
    auto virtual_part_name = virtual_parts.getContainingPart(data_part->info);
    return !virtual_part_name.empty() && virtual_part_name != data_part->name;
}


bool ReplicatedMergeTreeQueue::load(zkutil::ZooKeeperPtr zookeeper)
{
    auto queue_path = replica_path + "/queue";
    LOG_DEBUG(log, "Loading queue from {}", queue_path);

    bool updated = false;
    std::optional<time_t> min_unprocessed_insert_time_changed;

    {
        std::lock_guard pull_logs_lock(pull_logs_to_queue_mutex);

        /// Reset batch size on initialization to recover from possible errors of too large batch size.
        current_multi_batch_size = 1;

        String log_pointer_str = zookeeper->get(replica_path + "/log_pointer");
        log_pointer = log_pointer_str.empty() ? 0 : parse<UInt64>(log_pointer_str);

        std::unordered_set<String> already_loaded_paths;
        {
            std::lock_guard lock(state_mutex);
            for (const LogEntryPtr & log_entry : queue)
                already_loaded_paths.insert(log_entry->znode_name);
        }

        Strings children = zookeeper->getChildren(queue_path);

        auto to_remove_it = std::remove_if(
            children.begin(), children.end(), [&](const String & path)
            {
                return already_loaded_paths.count(path);
            });

        LOG_DEBUG(log, "Having {} queue entries to load, {} entries already loaded.", (to_remove_it - children.begin()), (children.end() - to_remove_it));
        children.erase(to_remove_it, children.end());

        std::sort(children.begin(), children.end());

        zkutil::AsyncResponses<Coordination::GetResponse> futures;
        futures.reserve(children.size());

        for (const String & child : children)
            futures.emplace_back(child, zookeeper->asyncGet(queue_path + "/" + child));

        for (auto & future : futures)
        {
            Coordination::GetResponse res = future.second.get();
            LogEntryPtr entry = LogEntry::parse(res.data, res.stat);
            entry->znode_name = future.first;

            std::lock_guard lock(state_mutex);

            insertUnlocked(entry, min_unprocessed_insert_time_changed, lock);

            updated = true;
        }

        zookeeper->tryGet(replica_path + "/mutation_pointer", mutation_pointer);
    }

    updateTimesInZooKeeper(zookeeper, min_unprocessed_insert_time_changed, {});

    merge_strategy_picker.refreshState();

    LOG_TRACE(log, "Loaded queue");
    return updated;
}


void ReplicatedMergeTreeQueue::insertUnlocked(
    const LogEntryPtr & entry, std::optional<time_t> & min_unprocessed_insert_time_changed,
    std::lock_guard<std::mutex> & state_lock)
{
    for (const String & virtual_part_name : entry->getVirtualPartNames())
    {
        virtual_parts.add(virtual_part_name);
        addPartToMutations(virtual_part_name);
    }

    /// Put 'DROP PARTITION' entries at the beginning of the queue not to make superfluous fetches of parts that will be eventually deleted
    if (entry->type != LogEntry::DROP_RANGE)
        queue.push_back(entry);
    else
        queue.push_front(entry);

    if (entry->type == LogEntry::GET_PART)
    {
        inserts_by_time.insert(entry);

        if (entry->create_time && (!min_unprocessed_insert_time || entry->create_time < min_unprocessed_insert_time))
        {
            min_unprocessed_insert_time = entry->create_time;
            min_unprocessed_insert_time_changed = min_unprocessed_insert_time;
        }
    }
    if (entry->type == LogEntry::ALTER_METADATA)
    {
        LOG_TRACE(log, "Adding alter metadata version {} to the queue", entry->alter_version);
        alter_sequence.addMetadataAlter(entry->alter_version, entry->have_mutation, state_lock);
    }
}


void ReplicatedMergeTreeQueue::insert(zkutil::ZooKeeperPtr zookeeper, LogEntryPtr & entry)
{
    std::optional<time_t> min_unprocessed_insert_time_changed;

    {
        std::lock_guard lock(state_mutex);
        insertUnlocked(entry, min_unprocessed_insert_time_changed, lock);
    }

    updateTimesInZooKeeper(zookeeper, min_unprocessed_insert_time_changed, {});
}


void ReplicatedMergeTreeQueue::updateStateOnQueueEntryRemoval(
    const LogEntryPtr & entry,
    bool is_successful,
    std::optional<time_t> & min_unprocessed_insert_time_changed,
    std::optional<time_t> & max_processed_insert_time_changed,
    std::unique_lock<std::mutex> & state_lock)
{
    /// Update insert times.
    if (entry->type == LogEntry::GET_PART)
    {
        inserts_by_time.erase(entry);

        if (inserts_by_time.empty())
        {
            min_unprocessed_insert_time = 0;
            min_unprocessed_insert_time_changed = min_unprocessed_insert_time;
        }
        else if ((*inserts_by_time.begin())->create_time > min_unprocessed_insert_time)
        {
            min_unprocessed_insert_time = (*inserts_by_time.begin())->create_time;
            min_unprocessed_insert_time_changed = min_unprocessed_insert_time;
        }

        if (entry->create_time > max_processed_insert_time)
        {
            max_processed_insert_time = entry->create_time;
            max_processed_insert_time_changed = max_processed_insert_time;
        }
    }

    if (is_successful)
    {
        if (!entry->actual_new_part_name.empty())
        {
            /// We don't add bigger fetched part to current_parts because we
            /// have an invariant `virtual_parts` = `current_parts` + `queue`.
            ///
            /// But we remove covered parts from mutations, because we actually
            /// have replacing part.
            ///
            /// NOTE actual_new_part_name is very confusing and error-prone. This approach must be fixed.
            removeCoveredPartsFromMutations(entry->actual_new_part_name, /*remove_part = */ false, /*remove_covered_parts = */ true);
        }

        for (const String & virtual_part_name : entry->getVirtualPartNames())
        {
            current_parts.add(virtual_part_name);

            /// These parts are already covered by newer part, we don't have to
            /// mutate it.
            removeCoveredPartsFromMutations(virtual_part_name, /*remove_part = */ false, /*remove_covered_parts = */ true);
        }

        String drop_range_part_name;
        if (entry->type == LogEntry::DROP_RANGE)
            drop_range_part_name = entry->new_part_name;
        else if (entry->type == LogEntry::REPLACE_RANGE)
            drop_range_part_name = entry->replace_range_entry->drop_range_part_name;

        if (!drop_range_part_name.empty())
        {
            current_parts.remove(drop_range_part_name);
            virtual_parts.remove(drop_range_part_name);
        }

        if (entry->type == LogEntry::ALTER_METADATA)
        {
            LOG_TRACE(log, "Finishing metadata alter with version {}", entry->alter_version);
            alter_sequence.finishMetadataAlter(entry->alter_version, state_lock);
        }
    }
    else
    {
        for (const String & virtual_part_name : entry->getVirtualPartNames())
        {
            /// Because execution of the entry is unsuccessful,
            /// `virtual_part_name` will never appear so we won't need to mutate
            /// it.
            removeCoveredPartsFromMutations(virtual_part_name, /*remove_part = */ true, /*remove_covered_parts = */ false);
        }
    }
}


void ReplicatedMergeTreeQueue::removeCoveredPartsFromMutations(const String & part_name, bool remove_part, bool remove_covered_parts)
{
    auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);
    auto in_partition = mutations_by_partition.find(part_info.partition_id);
    if (in_partition == mutations_by_partition.end())
        return;

    bool some_mutations_are_probably_done = false;

    auto from_it = in_partition->second.lower_bound(part_info.getDataVersion());
    for (auto it = from_it; it != in_partition->second.end(); ++it)
    {
        MutationStatus & status = *it->second;

        if (remove_part && remove_covered_parts)
            status.parts_to_do.removePartAndCoveredParts(part_name);
        else if (remove_covered_parts)
            status.parts_to_do.removePartsCoveredBy(part_name);
        else if (remove_part)
            status.parts_to_do.remove(part_name);
        else
            throw Exception("Called remove part from mutations, but nothing removed", ErrorCodes::LOGICAL_ERROR);

        if (status.parts_to_do.size() == 0)
            some_mutations_are_probably_done = true;

        if (!status.latest_failed_part.empty() && part_info.contains(status.latest_failed_part_info))
        {
            status.latest_failed_part.clear();
            status.latest_failed_part_info = MergeTreePartInfo();
            status.latest_fail_time = 0;
            status.latest_fail_reason.clear();
        }
    }

    if (some_mutations_are_probably_done)
        storage.mutations_finalizing_task->schedule();
}

void ReplicatedMergeTreeQueue::addPartToMutations(const String & part_name)
{
    auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);
    auto in_partition = mutations_by_partition.find(part_info.partition_id);
    if (in_partition == mutations_by_partition.end())
        return;

    auto from_it = in_partition->second.upper_bound(part_info.getDataVersion());
    for (auto it = from_it; it != in_partition->second.end(); ++it)
    {
        MutationStatus & status = *it->second;
        status.parts_to_do.add(part_name);
    }
}

void ReplicatedMergeTreeQueue::updateTimesInZooKeeper(
    zkutil::ZooKeeperPtr zookeeper,
    std::optional<time_t> min_unprocessed_insert_time_changed,
    std::optional<time_t> max_processed_insert_time_changed) const
{
    /// Here there can be a race condition (with different remove at the same time)
    ///  because we update times in ZooKeeper with unlocked mutex, while these times may change.
    /// Consider it unimportant (for a short time, ZK will have a slightly different time value).

    Coordination::Requests ops;

    if (min_unprocessed_insert_time_changed)
        ops.emplace_back(zkutil::makeSetRequest(
            replica_path + "/min_unprocessed_insert_time", toString(*min_unprocessed_insert_time_changed), -1));

    if (max_processed_insert_time_changed)
        ops.emplace_back(zkutil::makeSetRequest(
            replica_path + "/max_processed_insert_time", toString(*max_processed_insert_time_changed), -1));

    if (!ops.empty())
    {
        Coordination::Responses responses;
        auto code = zookeeper->tryMulti(ops, responses);

        if (code != Coordination::Error::ZOK)
            LOG_ERROR(log, "Couldn't set value of nodes for insert times ({}/min_unprocessed_insert_time, max_processed_insert_time): {}. This shouldn't happen often.", replica_path, Coordination::errorMessage(code));
    }
}


void ReplicatedMergeTreeQueue::removeProcessedEntry(zkutil::ZooKeeperPtr zookeeper, LogEntryPtr & entry)
{
    std::optional<time_t> min_unprocessed_insert_time_changed;
    std::optional<time_t> max_processed_insert_time_changed;

    bool found = false;
    bool need_remove_from_zk = true;
    size_t queue_size = 0;

    /// First remove from memory then from ZooKeeper
    {
        std::unique_lock lock(state_mutex);
        if (entry->removed_by_other_entry)
        {
            need_remove_from_zk = false;
            queue_size = queue.size();
        }
        else
        {
            /// Remove the job from the queue in the RAM.
            /// You can not just refer to a pre-saved iterator, because someone else might be able to delete the task.
            /// Why do we view the queue from the end?
            ///  - because the task for execution first is moved to the end of the queue, so that in case of failure it remains at the end.
            for (Queue::iterator it = queue.end(); it != queue.begin();)
            {
                --it;

                if (*it == entry)
                {
                    found = true;
                    updateStateOnQueueEntryRemoval(
                            entry, /* is_successful = */ true,
                            min_unprocessed_insert_time_changed, max_processed_insert_time_changed, lock);

                    queue.erase(it);
                    queue_size = queue.size();
                    break;
                }
            }
        }
    }

    if (!found && need_remove_from_zk)
        throw Exception("Can't find " + entry->znode_name + " in the memory queue. It is a bug", ErrorCodes::LOGICAL_ERROR);

    notifySubscribers(queue_size);

    if (!need_remove_from_zk)
        return;

    auto code = zookeeper->tryRemove(replica_path + "/queue/" + entry->znode_name);
    if (code != Coordination::Error::ZOK)
        LOG_ERROR(log, "Couldn't remove {}/queue/{}: {}. This shouldn't happen often.", replica_path, entry->znode_name, Coordination::errorMessage(code));

    updateTimesInZooKeeper(zookeeper, min_unprocessed_insert_time_changed, max_processed_insert_time_changed);
}


bool ReplicatedMergeTreeQueue::remove(zkutil::ZooKeeperPtr zookeeper, const String & part_name)
{
    LogEntryPtr found;
    size_t queue_size = 0;

    std::optional<time_t> min_unprocessed_insert_time_changed;
    std::optional<time_t> max_processed_insert_time_changed;

    {
        std::unique_lock lock(state_mutex);

        virtual_parts.remove(part_name);

        for (Queue::iterator it = queue.begin(); it != queue.end();)
        {
            if ((*it)->new_part_name == part_name)
            {
                found = *it;
                updateStateOnQueueEntryRemoval(
                    found, /* is_successful = */ false,
                    min_unprocessed_insert_time_changed, max_processed_insert_time_changed, lock);
                queue.erase(it++);
                queue_size = queue.size();
                break;
            }
            else
                ++it;
        }
    }

    if (!found)
        return false;

    notifySubscribers(queue_size);

    zookeeper->tryRemove(replica_path + "/queue/" + found->znode_name);
    updateTimesInZooKeeper(zookeeper, min_unprocessed_insert_time_changed, max_processed_insert_time_changed);

    return true;
}


bool ReplicatedMergeTreeQueue::removeFromVirtualParts(const MergeTreePartInfo & part_info)
{
    std::lock_guard lock(state_mutex);
    return virtual_parts.remove(part_info);
}

int32_t ReplicatedMergeTreeQueue::pullLogsToQueue(zkutil::ZooKeeperPtr zookeeper, Coordination::WatchCallback watch_callback)
{
    std::lock_guard lock(pull_logs_to_queue_mutex);
    if (pull_log_blocker.isCancelled())
        throw Exception("Log pulling is cancelled", ErrorCodes::ABORTED);

    String index_str = zookeeper->get(replica_path + "/log_pointer");
    UInt64 index;

    /// The version of "/log" is modified when new entries to merge/mutate/drop appear.
    Coordination::Stat stat;
    zookeeper->get(zookeeper_path + "/log", &stat);

    Strings log_entries = zookeeper->getChildrenWatch(zookeeper_path + "/log", nullptr, watch_callback);

    /// We update mutations after we have loaded the list of log entries, but before we insert them
    /// in the queue.
    /// With this we ensure that if you read the log state L1 and then the state of mutations M1,
    /// then L1 "happened-before" M1.
    updateMutations(zookeeper);

    if (index_str.empty())
    {
        /// If we do not already have a pointer to the log, put a pointer to the first entry in it.
        index = log_entries.empty() ? 0 : parse<UInt64>(std::min_element(log_entries.begin(), log_entries.end())->substr(strlen("log-")));

        zookeeper->set(replica_path + "/log_pointer", toString(index));
    }
    else
    {
        index = parse<UInt64>(index_str);
    }

    String min_log_entry = "log-" + padIndex(index);

    /// Multiple log entries that must be copied to the queue.

    log_entries.erase(
        std::remove_if(log_entries.begin(), log_entries.end(), [&min_log_entry](const String & entry) { return entry < min_log_entry; }),
        log_entries.end());

    if (!log_entries.empty())
    {
        std::sort(log_entries.begin(), log_entries.end());

        for (size_t entry_idx = 0, num_entries = log_entries.size(); entry_idx < num_entries;)
        {
            auto begin = log_entries.begin() + entry_idx;
            auto end = entry_idx + current_multi_batch_size >= log_entries.size()
                ? log_entries.end()
                : (begin + current_multi_batch_size);
            auto last = end - 1;

            /// Increment entry_idx before batch size increase (we copied at most current_multi_batch_size entries)
            entry_idx += current_multi_batch_size;

            /// Increase the batch size exponentially, so it will saturate to MAX_MULTI_OPS.
            if (current_multi_batch_size < MAX_MULTI_OPS)
                current_multi_batch_size = std::min<size_t>(MAX_MULTI_OPS, current_multi_batch_size * 2);

            String last_entry = *last;
            if (!startsWith(last_entry, "log-"))
                throw Exception("Error in zookeeper data: unexpected node " + last_entry + " in " + zookeeper_path + "/log",
                    ErrorCodes::UNEXPECTED_NODE_IN_ZOOKEEPER);

            UInt64 last_entry_index = parse<UInt64>(last_entry.substr(strlen("log-")));

            LOG_DEBUG(log, "Pulling {} entries to queue: {} - {}", (end - begin), *begin, *last);

            zkutil::AsyncResponses<Coordination::GetResponse> futures;
            futures.reserve(end - begin);

            for (auto it = begin; it != end; ++it)
                futures.emplace_back(*it, zookeeper->asyncGet(zookeeper_path + "/log/" + *it));

            /// Simultaneously add all new entries to the queue and move the pointer to the log.

            Coordination::Requests ops;
            std::vector<LogEntryPtr> copied_entries;
            copied_entries.reserve(end - begin);

            std::optional<time_t> min_unprocessed_insert_time_changed;

            for (auto & future : futures)
            {
                Coordination::GetResponse res = future.second.get();

                copied_entries.emplace_back(LogEntry::parse(res.data, res.stat));

                ops.emplace_back(zkutil::makeCreateRequest(
                    replica_path + "/queue/queue-", res.data, zkutil::CreateMode::PersistentSequential));

                const auto & entry = *copied_entries.back();
                if (entry.type == LogEntry::GET_PART)
                {
                    std::lock_guard state_lock(state_mutex);
                    if (entry.create_time && (!min_unprocessed_insert_time || entry.create_time < min_unprocessed_insert_time))
                    {
                        min_unprocessed_insert_time = entry.create_time;
                        min_unprocessed_insert_time_changed = min_unprocessed_insert_time;
                    }
                }
            }

            ops.emplace_back(zkutil::makeSetRequest(
                replica_path + "/log_pointer", toString(last_entry_index + 1), -1));

            if (min_unprocessed_insert_time_changed)
                ops.emplace_back(zkutil::makeSetRequest(
                    replica_path + "/min_unprocessed_insert_time", toString(*min_unprocessed_insert_time_changed), -1));

            auto responses = zookeeper->multi(ops);

            /// Now we have successfully updated the queue in ZooKeeper. Update it in RAM.

            try
            {
                std::lock_guard state_lock(state_mutex);

                log_pointer = last_entry_index + 1;

                for (size_t copied_entry_idx = 0, num_copied_entries = copied_entries.size(); copied_entry_idx < num_copied_entries; ++copied_entry_idx)
                {
                    String path_created = dynamic_cast<const Coordination::CreateResponse &>(*responses[copied_entry_idx]).path_created;
                    copied_entries[copied_entry_idx]->znode_name = path_created.substr(path_created.find_last_of('/') + 1);

                    std::optional<time_t> unused = false;
                    insertUnlocked(copied_entries[copied_entry_idx], unused, state_lock);
                }

                last_queue_update = time(nullptr);
            }
            catch (...)
            {
                tryLogCurrentException(log);
                /// If it fails, the data in RAM is incorrect. In order to avoid possible further corruption of data in ZK, we will kill ourselves.
                /// This is possible only if there is an unknown logical error.
                std::terminate();
            }

            if (!copied_entries.empty())
            {
                LOG_DEBUG(log, "Pulled {} entries to queue.", copied_entries.size());
                merge_strategy_picker.refreshState();
            }
        }

        storage.background_executor.triggerTask();
    }

    return stat.version;
}


namespace
{

Names getPartNamesToMutate(
    const ReplicatedMergeTreeMutationEntry & mutation, const ActiveDataPartSet & parts)
{
    Names result;
    for (const auto & pair : mutation.block_numbers)
    {
        const String & partition_id = pair.first;
        Int64 block_num = pair.second;

        /// Note that we cannot simply count all parts to mutate using getPartsCoveredBy(appropriate part_info)
        /// because they are not consecutive in `parts`.
        MergeTreePartInfo covering_part_info(
            partition_id, 0, block_num, MergeTreePartInfo::MAX_LEVEL, MergeTreePartInfo::MAX_BLOCK_NUMBER);
        for (const String & covered_part_name : parts.getPartsCoveredBy(covering_part_info))
        {
            auto part_info = MergeTreePartInfo::fromPartName(covered_part_name, parts.getFormatVersion());
            if (part_info.getDataVersion() < block_num)
                result.push_back(covered_part_name);
        }
    }

    return result;
}

}

void ReplicatedMergeTreeQueue::updateMutations(zkutil::ZooKeeperPtr zookeeper, Coordination::WatchCallback watch_callback)
{
    std::lock_guard lock(update_mutations_mutex);

    Strings entries_in_zk = zookeeper->getChildrenWatch(zookeeper_path + "/mutations", nullptr, watch_callback);
    StringSet entries_in_zk_set(entries_in_zk.begin(), entries_in_zk.end());

    /// Compare with the local state, delete obsolete entries and determine which new entries to load.
    Strings entries_to_load;
    bool some_active_mutations_were_killed = false;
    {
        std::lock_guard state_lock(state_mutex);

        for (auto it = mutations_by_znode.begin(); it != mutations_by_znode.end();)
        {
            const ReplicatedMergeTreeMutationEntry & entry = *it->second.entry;
            if (!entries_in_zk_set.count(entry.znode_name))
            {
                if (!it->second.is_done)
                {
                    LOG_DEBUG(log, "Removing killed mutation {} from local state.", entry.znode_name);
                    some_active_mutations_were_killed = true;
                    if (entry.isAlterMutation())
                    {
                        LOG_DEBUG(log, "Removed alter {} because mutation {} were killed.", entry.alter_version, entry.znode_name);
                        alter_sequence.finishDataAlter(entry.alter_version, state_lock);
                    }
                }
                else
                    LOG_DEBUG(log, "Removing obsolete mutation {} from local state.", entry.znode_name);

                for (const auto & partition_and_block_num : entry.block_numbers)
                {
                    auto & in_partition = mutations_by_partition[partition_and_block_num.first];
                    in_partition.erase(partition_and_block_num.second);
                    if (in_partition.empty())
                        mutations_by_partition.erase(partition_and_block_num.first);
                }

                it = mutations_by_znode.erase(it);
            }
            else
                ++it;
        }

        for (const String & znode : entries_in_zk_set)
        {
            if (!mutations_by_znode.count(znode))
                entries_to_load.push_back(znode);
        }
    }

    if (some_active_mutations_were_killed)
        storage.background_executor.triggerTask();

    if (!entries_to_load.empty())
    {
        LOG_INFO(log, "Loading {} mutation entries: {} - {}", toString(entries_to_load.size()), entries_to_load.front(), entries_to_load.back());

        std::vector<std::future<Coordination::GetResponse>> futures;
        for (const String & entry : entries_to_load)
            futures.emplace_back(zookeeper->asyncGet(zookeeper_path + "/mutations/" + entry));

        std::vector<ReplicatedMergeTreeMutationEntryPtr> new_mutations;
        for (size_t i = 0; i < entries_to_load.size(); ++i)
        {
            new_mutations.push_back(std::make_shared<ReplicatedMergeTreeMutationEntry>(
                ReplicatedMergeTreeMutationEntry::parse(futures[i].get().data, entries_to_load[i])));
        }

        bool some_mutations_are_probably_done = false;
        {
            std::lock_guard state_lock(state_mutex);

            for (const ReplicatedMergeTreeMutationEntryPtr & entry : new_mutations)
            {
                auto & mutation = mutations_by_znode.emplace(entry->znode_name, MutationStatus(entry, format_version))
                    .first->second;

                for (const auto & pair : entry->block_numbers)
                {
                    const String & partition_id = pair.first;
                    Int64 block_num = pair.second;
                    mutations_by_partition[partition_id].emplace(block_num, &mutation);
                    LOG_TRACE(log, "Adding mutation {} for partition {} for all block numbers less than {}", entry->znode_name, partition_id, block_num);
                }

                /// Initialize `mutation.parts_to_do`. First we need to mutate all parts in `current_parts`.
                Strings current_parts_to_mutate = getPartNamesToMutate(*entry, current_parts);
                for (const String & current_part_to_mutate : current_parts_to_mutate)
                    mutation.parts_to_do.add(current_part_to_mutate);

                /// And next we would need to mutate all parts with getDataVersion() greater than
                /// mutation block number that would appear as a result of executing the queue.
                for (const auto & queue_entry : queue)
                {
                    for (const String & produced_part_name : queue_entry->getVirtualPartNames())
                    {
                        auto part_info = MergeTreePartInfo::fromPartName(produced_part_name, format_version);

                        /// Oddly enough, getVirtualPartNames() may return _virtual_ part name.
                        /// Such parts do not exist and will never appear, so we should not add virtual parts to parts_to_do list.
                        /// Fortunately, it's easy to distinguish virtual parts from normal parts by part level.
                        /// See StorageReplicatedMergeTree::getFakePartCoveringAllPartsInPartition(...)
                        auto max_level = MergeTreePartInfo::MAX_LEVEL;      /// DROP/DETACH PARTITION
                        auto another_max_level = std::numeric_limits<decltype(part_info.level)>::max();    /// REPLACE/MOVE PARTITION
                        if (part_info.level == max_level || part_info.level == another_max_level)
                            continue;

                        auto it = entry->block_numbers.find(part_info.partition_id);
                        if (it != entry->block_numbers.end() && it->second > part_info.getDataVersion())
                            mutation.parts_to_do.add(produced_part_name);
                    }
                }

                if (mutation.parts_to_do.size() == 0)
                {
                    some_mutations_are_probably_done = true;
                }

                /// otherwise it's already done
                if (entry->isAlterMutation() && entry->znode_name > mutation_pointer)
                {
                    LOG_TRACE(log, "Adding mutation {} with alter version {} to the queue", entry->znode_name, entry->alter_version);
                    alter_sequence.addMutationForAlter(entry->alter_version, state_lock);
                }
            }
        }

        storage.merge_selecting_task->schedule();

        if (some_mutations_are_probably_done)
            storage.mutations_finalizing_task->schedule();
    }
}


ReplicatedMergeTreeMutationEntryPtr ReplicatedMergeTreeQueue::removeMutation(
    zkutil::ZooKeeperPtr zookeeper, const String & mutation_id)
{
    std::lock_guard lock(update_mutations_mutex);

    auto rc = zookeeper->tryRemove(zookeeper_path + "/mutations/" + mutation_id);
    if (rc == Coordination::Error::ZOK)
        LOG_DEBUG(log, "Removed mutation {} from ZooKeeper.", mutation_id);

    ReplicatedMergeTreeMutationEntryPtr entry;
    bool mutation_was_active = false;
    {
        std::lock_guard state_lock(state_mutex);

        auto it = mutations_by_znode.find(mutation_id);
        if (it == mutations_by_znode.end())
            return nullptr;

        mutation_was_active = !it->second.is_done;

        entry = it->second.entry;
        for (const auto & partition_and_block_num : entry->block_numbers)
        {
            auto & in_partition = mutations_by_partition[partition_and_block_num.first];
            in_partition.erase(partition_and_block_num.second);
            if (in_partition.empty())
                mutations_by_partition.erase(partition_and_block_num.first);
        }

        if (entry->isAlterMutation())
        {
            LOG_DEBUG(log, "Removed alter {} because mutation {} were killed.", entry->alter_version, entry->znode_name);
            alter_sequence.finishDataAlter(entry->alter_version, state_lock);
        }

        mutations_by_znode.erase(it);
        LOG_DEBUG(log, "Removed mutation {} from local state.", entry->znode_name);
    }

    if (mutation_was_active)
        storage.background_executor.triggerTask();

    return entry;
}


ReplicatedMergeTreeQueue::StringSet ReplicatedMergeTreeQueue::moveSiblingPartsForMergeToEndOfQueue(const String & part_name)
{
    std::lock_guard lock(state_mutex);

    /// Let's find the action to merge this part with others. Let's remember others.
    StringSet parts_for_merge;
    Queue::iterator merge_entry = queue.end();
    for (Queue::iterator it = queue.begin(); it != queue.end(); ++it)
    {
        if ((*it)->type == LogEntry::MERGE_PARTS || (*it)->type == LogEntry::MUTATE_PART)
        {
            if (std::find((*it)->source_parts.begin(), (*it)->source_parts.end(), part_name)
                != (*it)->source_parts.end())
            {
                parts_for_merge = StringSet((*it)->source_parts.begin(), (*it)->source_parts.end());
                merge_entry = it;
                break;
            }
        }
    }

    if (!parts_for_merge.empty())
    {
        /// Move to the end of queue actions that result in one of the parts in `parts_for_merge`.
        for (Queue::iterator it = queue.begin(); it != queue.end();)
        {
            auto it0 = it;
            ++it;

            if (it0 == merge_entry)
                break;

            if (((*it0)->type == LogEntry::MERGE_PARTS || (*it0)->type == LogEntry::GET_PART || (*it0)->type == LogEntry::MUTATE_PART)
                && parts_for_merge.count((*it0)->new_part_name))
            {
                queue.splice(queue.end(), queue, it0, it);
            }
        }
    }

    return parts_for_merge;
}

bool ReplicatedMergeTreeQueue::checkReplaceRangeCanBeRemoved(const MergeTreePartInfo & part_info, const LogEntryPtr entry_ptr, const ReplicatedMergeTreeLogEntryData & current) const
{
    if (entry_ptr->type != LogEntry::REPLACE_RANGE)
        return false;

    if (current.type != LogEntry::REPLACE_RANGE && current.type != LogEntry::DROP_RANGE)
        return false;

    if (entry_ptr->replace_range_entry != nullptr && entry_ptr->replace_range_entry == current.replace_range_entry) /// same partition, don't want to drop ourselves
        return false;

    for (const String & new_part_name : entry_ptr->replace_range_entry->new_part_names)
        if (!part_info.contains(MergeTreePartInfo::fromPartName(new_part_name, format_version)))
            return false;

    return true;
}

void ReplicatedMergeTreeQueue::removePartProducingOpsInRange(
    zkutil::ZooKeeperPtr zookeeper,
    const MergeTreePartInfo & part_info,
    const ReplicatedMergeTreeLogEntryData & current)
{
    /// TODO is it possible to simplify it?
    Queue to_wait;
    size_t removed_entries = 0;
    std::optional<time_t> min_unprocessed_insert_time_changed;
    std::optional<time_t> max_processed_insert_time_changed;

    /// Remove operations with parts, contained in the range to be deleted, from the queue.
    std::unique_lock lock(state_mutex);

    [[maybe_unused]] bool called_from_alter_query_directly = current.replace_range_entry && current.replace_range_entry->columns_version < 0;
    assert(currently_executing_drop_or_replace_range || called_from_alter_query_directly);

    for (Queue::iterator it = queue.begin(); it != queue.end();)
    {
        auto type = (*it)->type;

        if (((type == LogEntry::GET_PART || type == LogEntry::MERGE_PARTS || type == LogEntry::MUTATE_PART)
             && part_info.contains(MergeTreePartInfo::fromPartName((*it)->new_part_name, format_version)))
            || checkReplaceRangeCanBeRemoved(part_info, *it, current))
        {
            if ((*it)->currently_executing)
                to_wait.push_back(*it);
            auto code = zookeeper->tryRemove(replica_path + "/queue/" + (*it)->znode_name);
            /// FIXME it's probably unsafe to remove entries non-atomically
            /// when this method called directly from alter query (not from replication queue task),
            /// because entries will be lost if ALTER fails.
            if (code != Coordination::Error::ZOK)
                LOG_INFO(log, "Couldn't remove {}: {}", replica_path + "/queue/" + (*it)->znode_name, Coordination::errorMessage(code));

            updateStateOnQueueEntryRemoval(
                *it, /* is_successful = */ false,
                min_unprocessed_insert_time_changed, max_processed_insert_time_changed, lock);

            (*it)->removed_by_other_entry = true;
            queue.erase(it++);
            ++removed_entries;
        }
        else
            ++it;
    }

    updateTimesInZooKeeper(zookeeper, min_unprocessed_insert_time_changed, max_processed_insert_time_changed);

    LOG_DEBUG(log, "Removed {} entries from queue. Waiting for {} entries that are currently executing.", removed_entries, to_wait.size());

    /// Let's wait for the operations with the parts contained in the range to be deleted.
    for (LogEntryPtr & entry : to_wait)
        entry->execution_complete.wait(lock, [&entry] { return !entry->currently_executing; });
}


size_t ReplicatedMergeTreeQueue::getConflictsCountForRange(
    const MergeTreePartInfo & range, const LogEntry & entry,
    String * out_description, std::lock_guard<std::mutex> & /* queue_lock */) const
{
    std::vector<std::pair<String, LogEntryPtr>> conflicts;

    for (const auto & future_part_elem : future_parts)
    {
        /// Do not check itself log entry
        if (future_part_elem.second->znode_name == entry.znode_name)
            continue;

        if (!range.isDisjoint(MergeTreePartInfo::fromPartName(future_part_elem.first, format_version)))
        {
            conflicts.emplace_back(future_part_elem.first, future_part_elem.second);
            continue;
        }
    }

    if (out_description)
    {
        WriteBufferFromOwnString ss;
        ss << "Can't execute command for range " << range.getPartName() << " (entry " << entry.znode_name << "). ";
        ss << "There are " << conflicts.size() << " currently executing entries blocking it: ";
        for (const auto & conflict : conflicts)
            ss << conflict.second->typeToString() << " part " << conflict.first << ", ";

        *out_description = ss.str();
    }

    return conflicts.size();
}


void ReplicatedMergeTreeQueue::checkThereAreNoConflictsInRange(const MergeTreePartInfo & range, const LogEntry & entry)
{
    String conflicts_description;
    std::lock_guard lock(state_mutex);

    if (0 != getConflictsCountForRange(range, entry, &conflicts_description, lock))
        throw Exception(conflicts_description, ErrorCodes::UNFINISHED);
}


bool ReplicatedMergeTreeQueue::isNotCoveredByFuturePartsImpl(const String & log_entry_name, const String & new_part_name,
                                                             String & out_reason, std::lock_guard<std::mutex> & /* queue_lock */) const
{
    /// Let's check if the same part is now being created by another action.
    if (future_parts.count(new_part_name))
    {
        const char * format_str = "Not executing log entry {} for part {} "
                                  "because another log entry for the same part is being processed. This shouldn't happen often.";
        LOG_INFO(log, format_str, log_entry_name, new_part_name);
        out_reason = fmt::format(format_str, log_entry_name, new_part_name);
        return false;

        /** When the corresponding action is completed, then `isNotCoveredByFuturePart` next time, will succeed,
            *  and queue element will be processed.
            * Immediately in the `executeLogEntry` function it will be found that we already have a part,
            *  and queue element will be immediately treated as processed.
            */
    }

    /// A more complex check is whether another part is currently created by other action that will cover this part.
    /// NOTE The above is redundant, but left for a more convenient message in the log.
    auto result_part = MergeTreePartInfo::fromPartName(new_part_name, format_version);

    /// It can slow down when the size of `future_parts` is large. But it can not be large, since `BackgroundProcessingPool` is limited.
    for (const auto & future_part_elem : future_parts)
    {
        auto future_part = MergeTreePartInfo::fromPartName(future_part_elem.first, format_version);

        if (future_part.contains(result_part))
        {
            const char * format_str = "Not executing log entry {} for part {} "
                                      "because it is covered by part {} that is currently executing.";
            LOG_TRACE(log, format_str, log_entry_name, new_part_name, future_part_elem.first);
            out_reason = fmt::format(format_str, log_entry_name, new_part_name, future_part_elem.first);
            return false;
        }
    }

    return true;
}

bool ReplicatedMergeTreeQueue::addFuturePartIfNotCoveredByThem(const String & part_name, LogEntry & entry, String & reject_reason)
{
    std::lock_guard lock(state_mutex);

    if (isNotCoveredByFuturePartsImpl(entry.znode_name, part_name, reject_reason, lock))
    {
        CurrentlyExecuting::setActualPartName(entry, part_name, *this);
        return true;
    }

    return false;
}


bool ReplicatedMergeTreeQueue::shouldExecuteLogEntry(
    const LogEntry & entry,
    String & out_postpone_reason,
    MergeTreeDataMergerMutator & merger_mutator,
    MergeTreeData & data,
    std::lock_guard<std::mutex> & state_lock) const
{
    /// If our entry produce part which is already covered by
    /// some other entry which is currently executing, then we can postpone this entry.
    if (entry.type == LogEntry::MERGE_PARTS
        || entry.type == LogEntry::GET_PART
        || entry.type == LogEntry::MUTATE_PART)
    {
        for (const String & new_part_name : entry.getBlockingPartNames())
        {
            if (!isNotCoveredByFuturePartsImpl(entry.znode_name, new_part_name, out_postpone_reason, state_lock))
                return false;
        }
    }

    /// Check that fetches pool is not overloaded
    if (entry.type == LogEntry::GET_PART && !storage.canExecuteFetch(entry, out_postpone_reason))
    {
        /// Don't print log message about this, because we can have a lot of fetches,
        /// for example during replica recovery.
        return false;
    }

    if (entry.type == LogEntry::MERGE_PARTS || entry.type == LogEntry::MUTATE_PART)
    {
        /** If any of the required parts are now fetched or in merge process, wait for the end of this operation.
          * Otherwise, even if all the necessary parts for the merge are not present, you should try to make a merge.
          * If any parts are missing, instead of merge, there will be an attempt to download a part.
          * Such a situation is possible if the receive of a part has failed, and it was moved to the end of the queue.
          */
        size_t sum_parts_size_in_bytes = 0;
        for (const auto & name : entry.source_parts)
        {
            if (future_parts.count(name))
            {
                const char * format_str = "Not executing log entry {} of type {} for part {} "
                                          "because part {} is not ready yet (log entry for that part is being processed).";
                LOG_TRACE(log, format_str, entry.znode_name, entry.typeToString(), entry.new_part_name, name);
                /// Copy-paste of above because we need structured logging (instead of already formatted message).
                out_postpone_reason = fmt::format(format_str, entry.znode_name, entry.typeToString(), entry.new_part_name, name);
                return false;
            }

            auto part = data.getPartIfExists(name, {MergeTreeDataPartState::PreCommitted, MergeTreeDataPartState::Committed, MergeTreeDataPartState::Outdated});
            if (part)
            {
                if (auto part_in_memory = asInMemoryPart(part))
                    sum_parts_size_in_bytes += part_in_memory->block.bytes();
                else
                    sum_parts_size_in_bytes += part->getBytesOnDisk();
            }
        }

        if (merger_mutator.merges_blocker.isCancelled())
        {
            const char * format_str = "Not executing log entry {} of type {} for part {} because merges and mutations are cancelled now.";
            LOG_DEBUG(log, format_str, entry.znode_name, entry.typeToString(), entry.new_part_name);
            out_postpone_reason = fmt::format(format_str, entry.znode_name, entry.typeToString(), entry.new_part_name);
            return false;
        }

        if (merge_strategy_picker.shouldMergeOnSingleReplica(entry))
        {
            auto replica_to_execute_merge = merge_strategy_picker.pickReplicaToExecuteMerge(entry);

            if (replica_to_execute_merge && !merge_strategy_picker.isMergeFinishedByReplica(replica_to_execute_merge.value(), entry))
            {
                String reason = "Not executing merge for the part " + entry.new_part_name
                    +  ", waiting for " + replica_to_execute_merge.value() + " to execute merge.";
                out_postpone_reason = reason;
                return false;
            }
        }

        UInt64 max_source_parts_size = entry.type == LogEntry::MERGE_PARTS ? merger_mutator.getMaxSourcePartsSizeForMerge()
                                                                           : merger_mutator.getMaxSourcePartSizeForMutation();
        /** If there are enough free threads in background pool to do large merges (maximal size of merge is allowed),
          * then ignore value returned by getMaxSourcePartsSizeForMerge() and execute merge of any size,
          * because it may be ordered by OPTIMIZE or early with different settings.
          * Setting max_bytes_to_merge_at_max_space_in_pool still working for regular merges,
          * because the leader replica does not assign merges of greater size (except OPTIMIZE PARTITION and OPTIMIZE FINAL).
          */
        const auto data_settings = data.getSettings();
        bool ignore_max_size = false;
        if (entry.type == LogEntry::MERGE_PARTS)
        {
            ignore_max_size = max_source_parts_size == data_settings->max_bytes_to_merge_at_max_space_in_pool;

            if (isTTLMergeType(entry.merge_type))
            {
                if (merger_mutator.ttl_merges_blocker.isCancelled())
                {
                    const char * format_str = "Not executing log entry {} for part {} because merges with TTL are cancelled now.";
                    LOG_DEBUG(log, format_str,
                              entry.znode_name, entry.new_part_name);
                    out_postpone_reason = fmt::format(format_str, entry.znode_name, entry.new_part_name);
                    return false;
                }
                size_t total_merges_with_ttl = data.getTotalMergesWithTTLInMergeList();
                if (total_merges_with_ttl >= data_settings->max_number_of_merges_with_ttl_in_pool)
                {
                    const char * format_str = "Not executing log entry {} for part {}"
                        " because {} merges with TTL already executing, maximum {}.";
                    LOG_DEBUG(log, format_str,
                        entry.znode_name, entry.new_part_name, total_merges_with_ttl,
                        data_settings->max_number_of_merges_with_ttl_in_pool);

                    out_postpone_reason = fmt::format(format_str,
                        entry.znode_name, entry.new_part_name, total_merges_with_ttl,
                        data_settings->max_number_of_merges_with_ttl_in_pool);
                    return false;
                }
            }
        }

        if (!ignore_max_size && sum_parts_size_in_bytes > max_source_parts_size)
        {
            const char * format_str = "Not executing log entry {} of type {} for part {}"
                " because source parts size ({}) is greater than the current maximum ({}).";

            LOG_DEBUG(log, format_str, entry.znode_name,
                entry.typeToString(), entry.new_part_name,
                ReadableSize(sum_parts_size_in_bytes), ReadableSize(max_source_parts_size));

            out_postpone_reason = fmt::format(format_str, entry.znode_name,
                entry.typeToString(), entry.new_part_name,
                ReadableSize(sum_parts_size_in_bytes), ReadableSize(max_source_parts_size));

            return false;
        }
    }

    /// Alters must be executed one by one. First metadata change, and after that data alter (MUTATE_PART entries with).
    /// corresponding alter_version.
    if (entry.type == LogEntry::ALTER_METADATA)
    {
        if (!alter_sequence.canExecuteMetaAlter(entry.alter_version, state_lock))
        {
            int head_alter = alter_sequence.getHeadAlterVersion(state_lock);
            const char * format_str = "Cannot execute alter metadata {} with version {} because another alter {} must be executed before";
            LOG_TRACE(log, format_str, entry.znode_name, entry.alter_version, head_alter);
            out_postpone_reason = fmt::format(format_str, entry.znode_name, entry.alter_version, head_alter);
            return false;
        }
    }

    /// If this MUTATE_PART is part of alter modify/drop query, than we have to execute them one by one
    if (entry.isAlterMutation())
    {
        if (!alter_sequence.canExecuteDataAlter(entry.alter_version, state_lock))
        {
            int head_alter = alter_sequence.getHeadAlterVersion(state_lock);
            if (head_alter == entry.alter_version)
            {
                const char * format_str = "Cannot execute alter data {} with version {} because metadata still not altered";
                LOG_TRACE(log, format_str, entry.znode_name, entry.alter_version);
                out_postpone_reason = fmt::format(format_str, entry.znode_name, entry.alter_version);
            }
            else
            {
                const char * format_str = "Cannot execute alter data {} with version {} because another alter {} must be executed before";
                LOG_TRACE(log, format_str, entry.znode_name, entry.alter_version, head_alter);
                out_postpone_reason = fmt::format(format_str, entry.znode_name, entry.alter_version, head_alter);
            }

            return false;
        }
    }

    if (entry.type == LogEntry::DROP_RANGE || entry.type == LogEntry::REPLACE_RANGE)
    {
        /// DROP_RANGE and REPLACE_RANGE entries remove other entries, which produce parts in the range.
        /// If such part producing operations are currently executing, then DROP/REPLACE RANGE wait them to finish.
        /// Deadlock is possible if multiple DROP/REPLACE RANGE entries are executing in parallel and wait each other.
        /// See also removePartProducingOpsInRange(...) and ReplicatedMergeTreeQueue::CurrentlyExecuting.
        if (currently_executing_drop_or_replace_range)
        {

            const char * format_str = "Not executing log entry {} of type {} for part {} "
                                      "because another DROP_RANGE or REPLACE_RANGE entry are currently executing.";
            LOG_TRACE(log, format_str, entry.znode_name, entry.typeToString(), entry.new_part_name);
            out_postpone_reason = fmt::format(format_str, entry.znode_name, entry.typeToString(), entry.new_part_name);
            return false;
        }
    }

    return true;
}


Int64 ReplicatedMergeTreeQueue::getCurrentMutationVersionImpl(
    const String & partition_id, Int64 data_version, std::lock_guard<std::mutex> & /* state_lock */) const
{
    auto in_partition = mutations_by_partition.find(partition_id);
    if (in_partition == mutations_by_partition.end())
        return 0;

    auto it = in_partition->second.upper_bound(data_version);
    if (it == in_partition->second.begin())
        return 0;

    --it;
    return it->first;
}


Int64 ReplicatedMergeTreeQueue::getCurrentMutationVersion(const String & partition_id, Int64 data_version) const
{
    std::lock_guard lock(state_mutex);
    return getCurrentMutationVersionImpl(partition_id, data_version, lock);
}


ReplicatedMergeTreeQueue::CurrentlyExecuting::CurrentlyExecuting(const ReplicatedMergeTreeQueue::LogEntryPtr & entry_, ReplicatedMergeTreeQueue & queue_)
    : entry(entry_), queue(queue_)
{
    if (entry->type == ReplicatedMergeTreeLogEntry::DROP_RANGE || entry->type == ReplicatedMergeTreeLogEntry::REPLACE_RANGE)
    {
        assert(!queue.currently_executing_drop_or_replace_range);
        queue.currently_executing_drop_or_replace_range = true;
    }
    entry->currently_executing = true;
    ++entry->num_tries;
    entry->last_attempt_time = time(nullptr);

    for (const String & new_part_name : entry->getBlockingPartNames())
    {
        if (!queue.future_parts.emplace(new_part_name, entry).second)
            throw Exception("Tagging already tagged future part " + new_part_name + ". This is a bug.", ErrorCodes::LOGICAL_ERROR);
    }
}


void ReplicatedMergeTreeQueue::CurrentlyExecuting::setActualPartName(ReplicatedMergeTreeQueue::LogEntry & entry,
                                                                     const String & actual_part_name, ReplicatedMergeTreeQueue & queue)
{
    if (!entry.actual_new_part_name.empty())
        throw Exception("Entry actual part isn't empty yet. This is a bug.", ErrorCodes::LOGICAL_ERROR);

    entry.actual_new_part_name = actual_part_name;

    /// Check if it is the same (and already added) part.
    if (entry.actual_new_part_name == entry.new_part_name)
        return;

    if (!queue.future_parts.emplace(entry.actual_new_part_name, entry.shared_from_this()).second)
        throw Exception("Attaching already existing future part " + entry.actual_new_part_name + ". This is a bug.", ErrorCodes::LOGICAL_ERROR);
}


ReplicatedMergeTreeQueue::CurrentlyExecuting::~CurrentlyExecuting()
{
    std::lock_guard lock(queue.state_mutex);

    if (entry->type == ReplicatedMergeTreeLogEntry::DROP_RANGE || entry->type == ReplicatedMergeTreeLogEntry::REPLACE_RANGE)
    {
        assert(queue.currently_executing_drop_or_replace_range);
        queue.currently_executing_drop_or_replace_range = false;
    }
    entry->currently_executing = false;
    entry->execution_complete.notify_all();

    for (const String & new_part_name : entry->getBlockingPartNames())
    {
        if (!queue.future_parts.erase(new_part_name))
            LOG_ERROR(queue.log, "Untagging already untagged future part {}. This is a bug.", new_part_name);
    }

    if (!entry->actual_new_part_name.empty())
    {
        if (entry->actual_new_part_name != entry->new_part_name && !queue.future_parts.erase(entry->actual_new_part_name))
            LOG_ERROR(queue.log, "Untagging already untagged future part {}. This is a bug.", entry->actual_new_part_name);

        entry->actual_new_part_name.clear();
    }
}


ReplicatedMergeTreeQueue::SelectedEntryPtr ReplicatedMergeTreeQueue::selectEntryToProcess(MergeTreeDataMergerMutator & merger_mutator, MergeTreeData & data)
{
    LogEntryPtr entry;

    std::lock_guard lock(state_mutex);

    for (auto it = queue.begin(); it != queue.end(); ++it)
    {
        if ((*it)->currently_executing)
            continue;

        if (shouldExecuteLogEntry(**it, (*it)->postpone_reason, merger_mutator, data, lock))
        {
            entry = *it;
            /// We gave a chance for the entry, move it to the tail of the queue, after that
            /// we move it to the end of the queue.
            queue.splice(queue.end(), queue, it);
            break;
        }
        else
        {
            ++(*it)->num_postponed;
            (*it)->last_postpone_time = time(nullptr);
        }
    }

    if (entry)
        return std::make_shared<SelectedEntry>(entry, std::unique_ptr<CurrentlyExecuting>{ new CurrentlyExecuting(entry, *this) });
    else
        return {};
}


bool ReplicatedMergeTreeQueue::processEntry(
    std::function<zkutil::ZooKeeperPtr()> get_zookeeper,
    LogEntryPtr & entry,
    const std::function<bool(LogEntryPtr &)> func)
{
    std::exception_ptr saved_exception;

    try
    {
        /// We don't have any backoff for failed entries
        /// we just count amount of tries for each of them.
        if (func(entry))
            removeProcessedEntry(get_zookeeper(), entry);
    }
    catch (...)
    {
        saved_exception = std::current_exception();
    }

    if (saved_exception)
    {
        std::lock_guard lock(state_mutex);

        entry->exception = saved_exception;

        if (entry->type == ReplicatedMergeTreeLogEntryData::MUTATE_PART)
        {
            /// Record the exception in the system.mutations table.
            Int64 result_data_version = MergeTreePartInfo::fromPartName(entry->new_part_name, format_version)
                .getDataVersion();
            auto source_part_info = MergeTreePartInfo::fromPartName(
                entry->source_parts.at(0), format_version);

            auto in_partition = mutations_by_partition.find(source_part_info.partition_id);
            if (in_partition != mutations_by_partition.end())
            {
                auto mutations_begin_it = in_partition->second.upper_bound(source_part_info.getDataVersion());
                auto mutations_end_it = in_partition->second.upper_bound(result_data_version);
                for (auto it = mutations_begin_it; it != mutations_end_it; ++it)
                {
                    MutationStatus & status = *it->second;
                    status.latest_failed_part = entry->source_parts.at(0);
                    status.latest_failed_part_info = source_part_info;
                    status.latest_fail_time = time(nullptr);
                    status.latest_fail_reason = getExceptionMessage(saved_exception, false);
                }
            }
        }

        return false;
    }

    return true;
}


ReplicatedMergeTreeQueue::OperationsInQueue ReplicatedMergeTreeQueue::countMergesAndPartMutations() const
{
    std::lock_guard lock(state_mutex);

    size_t count_merges = 0;
    size_t count_mutations = 0;
    size_t count_merges_with_ttl = 0;
    for (const auto & entry : queue)
    {
        if (entry->type == ReplicatedMergeTreeLogEntry::MERGE_PARTS)
        {
            ++count_merges;
            if (isTTLMergeType(entry->merge_type))
                ++count_merges_with_ttl;
        }
        else if (entry->type == ReplicatedMergeTreeLogEntry::MUTATE_PART)
            ++count_mutations;
    }

    return OperationsInQueue{count_merges, count_mutations, count_merges_with_ttl};
}


size_t ReplicatedMergeTreeQueue::countMutations() const
{
    std::lock_guard lock(state_mutex);
    return mutations_by_znode.size();
}


size_t ReplicatedMergeTreeQueue::countFinishedMutations() const
{
    std::lock_guard lock(state_mutex);

    size_t count = 0;
    for (const auto & pair : mutations_by_znode)
    {
        const auto & mutation = pair.second;
        if (!mutation.is_done)
            break;

        ++count;
    }

    return count;
}


ReplicatedMergeTreeMergePredicate ReplicatedMergeTreeQueue::getMergePredicate(zkutil::ZooKeeperPtr & zookeeper)
{
    return ReplicatedMergeTreeMergePredicate(*this, zookeeper);
}


MutationCommands ReplicatedMergeTreeQueue::getFirstAlterMutationCommandsForPart(const MergeTreeData::DataPartPtr & part) const
{
    std::lock_guard lock(state_mutex);
    auto in_partition = mutations_by_partition.find(part->info.partition_id);
    if (in_partition == mutations_by_partition.end())
        return MutationCommands{};

    Int64 part_version = part->info.getDataVersion();
    for (auto [mutation_version, mutation_status] : in_partition->second)
        if (mutation_version > part_version && mutation_status->entry->alter_version != -1)
            return mutation_status->entry->commands;

    return MutationCommands{};
}

MutationCommands ReplicatedMergeTreeQueue::getMutationCommands(
    const MergeTreeData::DataPartPtr & part, Int64 desired_mutation_version) const
{
    /// NOTE: If the corresponding mutation is not found, the error is logged (and not thrown as an exception)
    /// to allow recovering from a mutation that cannot be executed. This way you can delete the mutation entry
    /// from /mutations in ZK and the replicas will simply skip the mutation.

    if (part->info.getDataVersion() > desired_mutation_version)
    {
        LOG_WARNING(log, "Data version of part {} is already greater than desired mutation version {}", part->name, desired_mutation_version);
        return MutationCommands{};
    }

    std::lock_guard lock(state_mutex);

    auto in_partition = mutations_by_partition.find(part->info.partition_id);
    if (in_partition == mutations_by_partition.end())
    {
        LOG_WARNING(log, "There are no mutations for partition ID {} (trying to mutate part {} to {})", part->info.partition_id, part->name, toString(desired_mutation_version));
        return MutationCommands{};
    }

    auto begin = in_partition->second.upper_bound(part->info.getDataVersion());

    auto end = in_partition->second.lower_bound(desired_mutation_version);
    if (end == in_partition->second.end() || end->first != desired_mutation_version)
        LOG_WARNING(log, "Mutation with version {} not found in partition ID {} (trying to mutate part {}", desired_mutation_version, part->info.partition_id, part->name + ")");
    else
        ++end;

    MutationCommands commands;
    for (auto it = begin; it != end; ++it)
        commands.insert(commands.end(), it->second->entry->commands.begin(), it->second->entry->commands.end());

    return commands;
}


bool ReplicatedMergeTreeQueue::tryFinalizeMutations(zkutil::ZooKeeperPtr zookeeper)
{
    std::vector<ReplicatedMergeTreeMutationEntryPtr> candidates;
    {
        std::lock_guard lock(state_mutex);

        for (auto & kv : mutations_by_znode)
        {
            const String & znode = kv.first;
            MutationStatus & mutation = kv.second;

            if (mutation.is_done)
                continue;

            if (znode <= mutation_pointer)
            {
                LOG_TRACE(log, "Marking mutation {} done because it is <= mutation_pointer ({})", znode, mutation_pointer);
                mutation.is_done = true;
                alter_sequence.finishDataAlter(mutation.entry->alter_version, lock);
                if (mutation.parts_to_do.size() != 0)
                {
                    LOG_INFO(log, "Seems like we jumped over mutation {} when downloaded part with bigger mutation number.{}", znode, " It's OK, tasks for rest parts will be skipped, but probably a lot of mutations were executed concurrently on different replicas.");
                    mutation.parts_to_do.clear();
                }
            }
            else if (mutation.parts_to_do.size() == 0)
            {
                LOG_TRACE(log, "Will check if mutation {} is done", mutation.entry->znode_name);
                candidates.push_back(mutation.entry);
            }
        }
    }

    if (candidates.empty())
        return false;
    else
        LOG_DEBUG(log, "Trying to finalize {} mutations", candidates.size());

    auto merge_pred = getMergePredicate(zookeeper);

    std::vector<const ReplicatedMergeTreeMutationEntry *> finished;
    for (const ReplicatedMergeTreeMutationEntryPtr & candidate : candidates)
    {
        if (merge_pred.isMutationFinished(*candidate))
            finished.push_back(candidate.get());
    }

    if (!finished.empty())
    {
        zookeeper->set(replica_path + "/mutation_pointer", finished.back()->znode_name);

        std::lock_guard lock(state_mutex);

        mutation_pointer = finished.back()->znode_name;

        for (const ReplicatedMergeTreeMutationEntry * entry : finished)
        {
            auto it = mutations_by_znode.find(entry->znode_name);
            if (it != mutations_by_znode.end())
            {
                LOG_TRACE(log, "Mutation {} is done", entry->znode_name);
                it->second.is_done = true;
                if (entry->isAlterMutation())
                {
                    LOG_TRACE(log, "Finishing data alter with version {} for entry {}", entry->alter_version, entry->znode_name);
                    alter_sequence.finishDataAlter(entry->alter_version, lock);
                }
            }
        }
    }

    /// Mutations may finish in non sequential order because we may fetch
    /// already mutated parts from other replicas. So, because we updated
    /// mutation pointer we have to recheck all previous mutations, they may be
    /// also finished.
    return !finished.empty();
}


void ReplicatedMergeTreeQueue::disableMergesInBlockRange(const String & part_name)
{
    std::lock_guard lock(state_mutex);
    virtual_parts.add(part_name);
}


ReplicatedMergeTreeQueue::Status ReplicatedMergeTreeQueue::getStatus() const
{
    std::lock_guard lock(state_mutex);

    Status res;

    res.future_parts = future_parts.size();
    res.queue_size = queue.size();
    res.last_queue_update = last_queue_update;

    res.inserts_in_queue = 0;
    res.merges_in_queue = 0;
    res.part_mutations_in_queue = 0;
    res.queue_oldest_time = 0;
    res.inserts_oldest_time = 0;
    res.merges_oldest_time = 0;
    res.part_mutations_oldest_time = 0;

    for (const LogEntryPtr & entry : queue)
    {
        if (entry->create_time && (!res.queue_oldest_time || entry->create_time < res.queue_oldest_time))
            res.queue_oldest_time = entry->create_time;

        if (entry->type == LogEntry::GET_PART)
        {
            ++res.inserts_in_queue;

            if (entry->create_time && (!res.inserts_oldest_time || entry->create_time < res.inserts_oldest_time))
            {
                res.inserts_oldest_time = entry->create_time;
                res.oldest_part_to_get = entry->new_part_name;
            }
        }

        if (entry->type == LogEntry::MERGE_PARTS)
        {
            ++res.merges_in_queue;

            if (entry->create_time && (!res.merges_oldest_time || entry->create_time < res.merges_oldest_time))
            {
                res.merges_oldest_time = entry->create_time;
                res.oldest_part_to_merge_to = entry->new_part_name;
            }
        }

        if (entry->type == LogEntry::MUTATE_PART)
        {
            ++res.part_mutations_in_queue;

            if (entry->create_time && (!res.part_mutations_oldest_time || entry->create_time < res.part_mutations_oldest_time))
            {
                res.part_mutations_oldest_time = entry->create_time;
                res.oldest_part_to_mutate_to = entry->new_part_name;
            }
        }
    }

    return res;
}


void ReplicatedMergeTreeQueue::getEntries(LogEntriesData & res) const
{
    res.clear();
    std::lock_guard lock(state_mutex);

    res.reserve(queue.size());
    for (const auto & entry : queue)
        res.emplace_back(*entry);
}


void ReplicatedMergeTreeQueue::getInsertTimes(time_t & out_min_unprocessed_insert_time, time_t & out_max_processed_insert_time) const
{
    std::lock_guard lock(state_mutex);
    out_min_unprocessed_insert_time = min_unprocessed_insert_time;
    out_max_processed_insert_time = max_processed_insert_time;
}


std::optional<MergeTreeMutationStatus> ReplicatedMergeTreeQueue::getIncompleteMutationsStatus(const String & znode_name, std::set<String> * mutation_ids) const
{

    std::lock_guard lock(state_mutex);
    auto current_mutation_it = mutations_by_znode.find(znode_name);
    /// killed
    if (current_mutation_it == mutations_by_znode.end())
        return {};

    const MutationStatus & status = current_mutation_it->second;
    MergeTreeMutationStatus result
    {
        .is_done = status.is_done,
        .latest_failed_part = status.latest_failed_part,
        .latest_fail_time = status.latest_fail_time,
        .latest_fail_reason = status.latest_fail_reason,
    };

    if (mutation_ids && !status.latest_fail_reason.empty())
    {
        const auto & latest_failed_part_info = status.latest_failed_part_info;
        auto in_partition = mutations_by_partition.find(latest_failed_part_info.partition_id);
        if (in_partition != mutations_by_partition.end())
        {
            const auto & version_to_status = in_partition->second;
            auto begin_it = version_to_status.upper_bound(latest_failed_part_info.getDataVersion());
            for (auto it = begin_it; it != version_to_status.end(); ++it)
            {
                /// All mutations with the same failure
                if (!it->second->is_done && it->second->latest_fail_reason == status.latest_fail_reason)
                    mutation_ids->insert(it->second->entry->znode_name);
            }
        }
    }
    return result;
}

std::vector<MergeTreeMutationStatus> ReplicatedMergeTreeQueue::getMutationsStatus() const
{
    std::lock_guard lock(state_mutex);

    std::vector<MergeTreeMutationStatus> result;
    for (const auto & pair : mutations_by_znode)
    {
        const MutationStatus & status = pair.second;
        const ReplicatedMergeTreeMutationEntry & entry = *status.entry;
        Names parts_to_mutate = status.parts_to_do.getParts();

        for (const MutationCommand & command : entry.commands)
        {
            WriteBufferFromOwnString buf;
            formatAST(*command.ast, buf, false, true);
            result.push_back(MergeTreeMutationStatus
            {
                entry.znode_name,
                buf.str(),
                entry.create_time,
                entry.block_numbers,
                parts_to_mutate,
                status.is_done,
                status.latest_failed_part,
                status.latest_fail_time,
                status.latest_fail_reason,
            });
        }
    }

    return result;
}

ReplicatedMergeTreeQueue::QueueLocks ReplicatedMergeTreeQueue::lockQueue()
{
    return QueueLocks(state_mutex, pull_logs_to_queue_mutex, update_mutations_mutex);
}

ReplicatedMergeTreeMergePredicate::ReplicatedMergeTreeMergePredicate(
    ReplicatedMergeTreeQueue & queue_, zkutil::ZooKeeperPtr & zookeeper)
    : queue(queue_)
    , prev_virtual_parts(queue.format_version)
{
    {
        std::lock_guard lock(queue.state_mutex);
        prev_virtual_parts = queue.virtual_parts;
    }

    /// Load current quorum status.
    auto quorum_status_future = zookeeper->asyncTryGet(queue.zookeeper_path + "/quorum/status");

    /// Load current inserts
    std::unordered_set<String> lock_holder_paths;
    for (const String & entry : zookeeper->getChildren(queue.zookeeper_path + "/temp"))
    {
        if (startsWith(entry, "abandonable_lock-"))
            lock_holder_paths.insert(queue.zookeeper_path + "/temp/" + entry);
    }

    if (!lock_holder_paths.empty())
    {
        Strings partitions = zookeeper->getChildren(queue.zookeeper_path + "/block_numbers");
        std::vector<std::future<Coordination::ListResponse>> lock_futures;
        for (const String & partition : partitions)
            lock_futures.push_back(zookeeper->asyncGetChildren(queue.zookeeper_path + "/block_numbers/" + partition));

        struct BlockInfoInZooKeeper
        {
            String partition;
            Int64 number;
            String zk_path;
            std::future<Coordination::GetResponse> contents_future;
        };

        std::vector<BlockInfoInZooKeeper> block_infos;
        for (size_t i = 0; i < partitions.size(); ++i)
        {
            Strings partition_block_numbers = lock_futures[i].get().names;
            for (const String & entry : partition_block_numbers)
            {
                /// TODO: cache block numbers that are abandoned.
                /// We won't need to check them on the next iteration.
                if (startsWith(entry, "block-"))
                {
                    Int64 block_number = parse<Int64>(entry.substr(strlen("block-")));
                    String zk_path = queue.zookeeper_path + "/block_numbers/" + partitions[i] + "/" + entry;
                    block_infos.emplace_back(
                        BlockInfoInZooKeeper{partitions[i], block_number, zk_path, zookeeper->asyncTryGet(zk_path)});
                }
            }
        }

        for (auto & block : block_infos)
        {
            Coordination::GetResponse resp = block.contents_future.get();
            if (resp.error == Coordination::Error::ZOK && lock_holder_paths.count(resp.data))
                committing_blocks[block.partition].insert(block.number);
        }
    }

    merges_version = queue_.pullLogsToQueue(zookeeper);

    Coordination::GetResponse quorum_status_response = quorum_status_future.get();
    if (quorum_status_response.error == Coordination::Error::ZOK)
    {
        ReplicatedMergeTreeQuorumEntry quorum_status;
        quorum_status.fromString(quorum_status_response.data);
        inprogress_quorum_part = quorum_status.part_name;
    }
    else
        inprogress_quorum_part.clear();
}

bool ReplicatedMergeTreeMergePredicate::operator()(
    const MergeTreeData::DataPartPtr & left,
    const MergeTreeData::DataPartPtr & right,
    String * out_reason) const
{
    if (left)
        return canMergeTwoParts(left, right, out_reason);
    else
        return canMergeSinglePart(right, out_reason);
}


bool ReplicatedMergeTreeMergePredicate::canMergeTwoParts(
    const MergeTreeData::DataPartPtr & left,
    const MergeTreeData::DataPartPtr & right,
    String * out_reason) const
{
    /// A sketch of a proof of why this method actually works:
    ///
    /// The trickiest part is to ensure that no new parts will ever appear in the range of blocks between left and right.
    /// Inserted parts get their block numbers by acquiring an ephemeral lock (see EphemeralLockInZooKeeper.h).
    /// These block numbers are monotonically increasing in a partition.
    ///
    /// Because there is a window between the moment the inserted part gets its block number and
    /// the moment it is committed (appears in the replication log), we can't get the name of all parts up to the given
    /// block number just by looking at the replication log - some parts with smaller block numbers may be currently committing
    /// and will appear in the log later than the parts with bigger block numbers.
    ///
    /// We also can't take a consistent snapshot of parts that are already committed plus parts that are about to commit
    /// due to limitations of ZooKeeper transactions.
    ///
    /// So we do the following (see the constructor):
    /// * copy virtual_parts from queue to prev_virtual_parts
    ///   (a set of parts which corresponds to executing the replication log up to a certain point)
    /// * load committing_blocks (inserts and mutations that have already acquired a block number but haven't appeared in the log yet)
    /// * do pullLogsToQueue() again to load fresh queue.virtual_parts and mutations.
    ///
    /// Now we have an invariant: if some part is in prev_virtual_parts then:
    /// * all parts with smaller block numbers are either in committing_blocks or in queue.virtual_parts
    ///   (those that managed to commit before we loaded committing_blocks).
    /// * all mutations with smaller block numbers are either in committing_blocks or in queue.mutations_by_partition
    ///
    /// So to check that no new parts will ever appear in the range of blocks between left and right we first check that
    /// left and right are already present in prev_virtual_parts (we can't give a definite answer for parts that were committed later)
    /// and then check that there are no blocks between them in committing_blocks and no parts in queue.virtual_parts.
    ///
    /// Similarly, to check that there will be no mutation with a block number between two parts from prev_virtual_parts
    /// (only then we can merge them without mutating the left part), we first check committing_blocks
    /// and then check that these two parts have the same mutation version according to queue.mutations_by_partition.

    if (left->info.partition_id != right->info.partition_id)
    {
        if (out_reason)
            *out_reason = "Parts " + left->name + " and " + right->name + " belong to different partitions";
        return false;
    }

    for (const MergeTreeData::DataPartPtr & part : {left, right})
    {
        if (part->name == inprogress_quorum_part)
        {
            if (out_reason)
                *out_reason = "Quorum insert for part " + part->name + " is currently in progress";
            return false;
        }

        if (prev_virtual_parts.getContainingPart(part->info).empty())
        {
            if (out_reason)
                *out_reason = "Entry for part " + part->name + " hasn't been read from the replication log yet";
            return false;
        }
    }

    Int64 left_max_block = left->info.max_block;
    Int64 right_min_block = right->info.min_block;
    if (left_max_block > right_min_block)
        std::swap(left_max_block, right_min_block);

    if (left_max_block + 1 < right_min_block)
    {
        auto committing_blocks_in_partition = committing_blocks.find(left->info.partition_id);
        if (committing_blocks_in_partition != committing_blocks.end())
        {
            const std::set<Int64> & block_numbers = committing_blocks_in_partition->second;

            auto block_it = block_numbers.upper_bound(left_max_block);
            if (block_it != block_numbers.end() && *block_it < right_min_block)
            {
                if (out_reason)
                    *out_reason = "Block number " + toString(*block_it) + " is still being inserted between parts "
                        + left->name + " and " + right->name;

                return false;
            }
        }
    }

    std::lock_guard lock(queue.state_mutex);

    for (const MergeTreeData::DataPartPtr & part : {left, right})
    {
        /// We look for containing parts in queue.virtual_parts (and not in prev_virtual_parts) because queue.virtual_parts is newer
        /// and it is guaranteed that it will contain all merges assigned before this object is constructed.
        String containing_part = queue.virtual_parts.getContainingPart(part->info);
        if (containing_part != part->name)
        {
            if (out_reason)
                *out_reason = "Part " + part->name + " has already been assigned a merge into " + containing_part;
            return false;
        }
    }

    if (left_max_block + 1 < right_min_block)
    {
        /// Fake part which will appear as merge result
        MergeTreePartInfo gap_part_info(
            left->info.partition_id, left_max_block + 1, right_min_block - 1,
            MergeTreePartInfo::MAX_LEVEL, MergeTreePartInfo::MAX_BLOCK_NUMBER);

        /// We don't select parts if any smaller part covered by our merge must exist after
        /// processing replication log up to log_pointer.
        Strings covered = queue.virtual_parts.getPartsCoveredBy(gap_part_info);
        if (!covered.empty())
        {
            if (out_reason)
                *out_reason = "There are " + toString(covered.size()) + " parts (from " + covered.front()
                    + " to " + covered.back() + ") that are still not present or being processed by "
                    + " other background process on this replica between " + left->name + " and " + right->name;
            return false;
        }
    }

    Int64 left_mutation_ver = queue.getCurrentMutationVersionImpl(
        left->info.partition_id, left->info.getDataVersion(), lock);

    Int64 right_mutation_ver = queue.getCurrentMutationVersionImpl(
        left->info.partition_id, right->info.getDataVersion(), lock);

    if (left_mutation_ver != right_mutation_ver)
    {
        if (out_reason)
            *out_reason = "Current mutation versions of parts " + left->name + " and " + right->name + " differ: "
                + toString(left_mutation_ver) + " and " + toString(right_mutation_ver) + " respectively";
        return false;
    }

    return true;
}

bool ReplicatedMergeTreeMergePredicate::canMergeSinglePart(
    const MergeTreeData::DataPartPtr & part,
    String * out_reason) const
{
    if (part->name == inprogress_quorum_part)
    {
        if (out_reason)
            *out_reason = "Quorum insert for part " + part->name + " is currently in progress";
        return false;
    }

    if (prev_virtual_parts.getContainingPart(part->info).empty())
    {
        if (out_reason)
            *out_reason = "Entry for part " + part->name + " hasn't been read from the replication log yet";
        return false;
    }

    std::lock_guard<std::mutex> lock(queue.state_mutex);

    /// We look for containing parts in queue.virtual_parts (and not in prev_virtual_parts) because queue.virtual_parts is newer
    /// and it is guaranteed that it will contain all merges assigned before this object is constructed.
    String containing_part = queue.virtual_parts.getContainingPart(part->info);
    if (containing_part != part->name)
    {
        if (out_reason)
            *out_reason = "Part " + part->name + " has already been assigned a merge into " + containing_part;
        return false;
    }

    return true;
}


std::optional<std::pair<Int64, int>> ReplicatedMergeTreeMergePredicate::getDesiredMutationVersion(const MergeTreeData::DataPartPtr & part) const
{
    /// Assigning mutations is easier than assigning merges because mutations appear in the same order as
    /// the order of their version numbers (see StorageReplicatedMergeTree::mutate).
    /// This means that if we have loaded the mutation with version number X then all mutations with
    /// the version numbers less than X are also loaded and if there is no merge or mutation assigned to
    /// the part (checked by querying queue.virtual_parts), we can confidently assign a mutation to
    /// version X for this part.

    /// We cannot mutate part if it's being inserted with quorum and it's not
    /// already reached.
    if (part->name == inprogress_quorum_part)
        return {};

    std::lock_guard lock(queue.state_mutex);

    if (queue.virtual_parts.getContainingPart(part->info) != part->name)
        return {};

    auto in_partition = queue.mutations_by_partition.find(part->info.partition_id);
    if (in_partition == queue.mutations_by_partition.end())
        return {};

    Int64 current_version = queue.getCurrentMutationVersionImpl(part->info.partition_id, part->info.getDataVersion(), lock);
    Int64 max_version = in_partition->second.rbegin()->first;

    int alter_version = -1;
    for (auto [mutation_version, mutation_status] : in_partition->second)
    {
        max_version = mutation_version;
        if (mutation_status->entry->isAlterMutation())
        {
            /// We want to assign mutations for part which version is bigger
            /// than part current version. But it doesn't make sense to assign
            /// more fresh versions of alter-mutations if previous alter still
            /// not done because alters execute one by one in strict order.
            if (mutation_version > current_version || !mutation_status->is_done)
            {
                alter_version = mutation_status->entry->alter_version;
                break;
            }
        }
    }

    if (current_version >= max_version)
        return {};

    return std::make_pair(max_version, alter_version);
}


bool ReplicatedMergeTreeMergePredicate::isMutationFinished(const ReplicatedMergeTreeMutationEntry & mutation) const
{
    for (const auto & kv : mutation.block_numbers)
    {
        const String & partition_id = kv.first;
        Int64 block_num = kv.second;

        auto partition_it = committing_blocks.find(partition_id);
        if (partition_it != committing_blocks.end())
        {
            size_t blocks_count = std::distance(
                partition_it->second.begin(), partition_it->second.lower_bound(block_num));
            if (blocks_count)
            {
                LOG_TRACE(queue.log, "Mutation {} is not done yet because in partition ID {} there are still {} uncommitted blocks.", mutation.znode_name, partition_id, blocks_count);
                return false;
            }
        }
    }

    {
        std::lock_guard lock(queue.state_mutex);

        size_t suddenly_appeared_parts = getPartNamesToMutate(mutation, queue.virtual_parts).size();
        if (suddenly_appeared_parts)
        {
            LOG_TRACE(queue.log, "Mutation {} is not done yet because {} parts to mutate suddenly appeared.", mutation.znode_name, suddenly_appeared_parts);
            return false;
        }
    }

    return true;
}


ReplicatedMergeTreeQueue::SubscriberHandler
ReplicatedMergeTreeQueue::addSubscriber(ReplicatedMergeTreeQueue::SubscriberCallBack && callback)
{
    std::lock_guard lock(state_mutex);
    std::lock_guard lock_subscribers(subscribers_mutex);

    auto it = subscribers.emplace(subscribers.end(), std::move(callback));

    /// Atomically notify about current size
    (*it)(queue.size());

    return SubscriberHandler(it, *this);
}

ReplicatedMergeTreeQueue::SubscriberHandler::~SubscriberHandler()
{
    std::lock_guard lock(queue.subscribers_mutex);
    queue.subscribers.erase(it);
}

void ReplicatedMergeTreeQueue::notifySubscribers(size_t new_queue_size)
{
    std::lock_guard lock_subscribers(subscribers_mutex);
    for (auto & subscriber_callback : subscribers)
        subscriber_callback(new_queue_size);
}

ReplicatedMergeTreeQueue::~ReplicatedMergeTreeQueue()
{
    notifySubscribers(0);
}

String padIndex(Int64 index)
{
    String index_str = toString(index);
    return std::string(10 - index_str.size(), '0') + index_str;
}

void ReplicatedMergeTreeQueue::removeCurrentPartsFromMutations()
{
    std::lock_guard state_lock(state_mutex);
    for (const auto & part_name : current_parts.getParts())
        removeCoveredPartsFromMutations(part_name, /*remove_part = */ true, /*remove_covered_parts = */ true);
}

}
