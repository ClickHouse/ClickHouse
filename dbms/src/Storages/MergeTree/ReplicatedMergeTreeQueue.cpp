#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/MergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataMerger.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQuorumEntry.h>
#include <Common/StringUtils/StringUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_NODE_IN_ZOOKEEPER;
    extern const int UNFINISHED;
}


ReplicatedMergeTreeQueue::ReplicatedMergeTreeQueue(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , format_version(storage.data.format_version)
    , virtual_parts(format_version)
{}


void ReplicatedMergeTreeQueue::initVirtualParts(const MergeTreeData::DataParts & parts)
{
    std::lock_guard lock(target_state_mutex);

    for (const auto & part : parts)
        virtual_parts.add(part->name);
}


bool ReplicatedMergeTreeQueue::load(zkutil::ZooKeeperPtr zookeeper)
{
    auto queue_path = replica_path + "/queue";
    LOG_DEBUG(log, "Loading queue from " << queue_path);

    bool updated = false;
    std::optional<time_t> min_unprocessed_insert_time_changed;

    {
        std::lock_guard target_state_lock(target_state_mutex);

        String log_pointer_str = zookeeper->get(replica_path + "/log_pointer");
        log_pointer = log_pointer_str.empty() ? 0 : parse<UInt64>(log_pointer_str);

        Strings children = zookeeper->getChildren(queue_path);
        LOG_DEBUG(log, "Having " << children.size() << " queue entries to load.");
        std::sort(children.begin(), children.end());

        std::vector<std::pair<String, std::future<zkutil::GetResponse>>> futures;
        futures.reserve(children.size());

        for (const String & child : children)
            futures.emplace_back(child, zookeeper->asyncGet(queue_path + "/" + child));

        std::lock_guard queue_lock(queue_mutex);
        for (auto & future : futures)
        {
            zkutil::GetResponse res = future.second.get();
            LogEntryPtr entry = LogEntry::parse(res.data, res.stat);
            entry->znode_name = future.first;

            insertUnlocked(entry, min_unprocessed_insert_time_changed, target_state_lock, queue_lock);

            updated = true;
        }
    }

    updateTimesInZooKeeper(zookeeper, min_unprocessed_insert_time_changed, {});

    LOG_TRACE(log, "Loaded queue");
    return updated;
}


void ReplicatedMergeTreeQueue::initialize(
    const String & zookeeper_path_, const String & replica_path_, const String & logger_name_,
    const MergeTreeData::DataParts & parts, zkutil::ZooKeeperPtr zookeeper)
{
    zookeeper_path = zookeeper_path_;
    replica_path = replica_path_;
    logger_name = logger_name_;
    log = &Logger::get(logger_name);

    initVirtualParts(parts);
    load(zookeeper);
}


void ReplicatedMergeTreeQueue::insertUnlocked(
    const LogEntryPtr & entry, std::optional<time_t> & min_unprocessed_insert_time_changed,
    std::lock_guard<std::mutex> & /* target_state_lock */,
    std::lock_guard<std::mutex> & /* queue_lock */)
{
    virtual_parts.add(entry->new_part_name);

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
}


void ReplicatedMergeTreeQueue::insert(zkutil::ZooKeeperPtr zookeeper, LogEntryPtr & entry)
{
    std::optional<time_t> min_unprocessed_insert_time_changed;

    {
        std::lock_guard target_state_lock(target_state_mutex);
        std::lock_guard queue_lock(queue_mutex);
        insertUnlocked(entry, min_unprocessed_insert_time_changed, target_state_lock, queue_lock);
    }

    updateTimesInZooKeeper(zookeeper, min_unprocessed_insert_time_changed, {});
}


void ReplicatedMergeTreeQueue::updateTimesOnRemoval(
    const LogEntryPtr & entry,
    std::optional<time_t> & min_unprocessed_insert_time_changed,
    std::optional<time_t> & max_processed_insert_time_changed,
    std::unique_lock<std::mutex> & /* queue_lock */)
{
    if (entry->type != LogEntry::GET_PART)
        return;

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


void ReplicatedMergeTreeQueue::updateTimesInZooKeeper(
    zkutil::ZooKeeperPtr zookeeper,
    std::optional<time_t> min_unprocessed_insert_time_changed,
    std::optional<time_t> max_processed_insert_time_changed) const
{
    /// Here there can be a race condition (with different remove at the same time)
    ///  because we update times in ZooKeeper with unlocked mutex, while these times may change.
    /// Consider it unimportant (for a short time, ZK will have a slightly different time value).

    zkutil::Requests ops;

    if (min_unprocessed_insert_time_changed)
        ops.emplace_back(zkutil::makeSetRequest(
            replica_path + "/min_unprocessed_insert_time", toString(*min_unprocessed_insert_time_changed), -1));

    if (max_processed_insert_time_changed)
        ops.emplace_back(zkutil::makeSetRequest(
            replica_path + "/max_processed_insert_time", toString(*max_processed_insert_time_changed), -1));

    if (!ops.empty())
    {
        zkutil::Responses responses;
        auto code = zookeeper->tryMulti(ops, responses);

        if (code)
            LOG_ERROR(log, "Couldn't set value of nodes for insert times ("
                << replica_path << "/min_unprocessed_insert_time, max_processed_insert_time)" << ": "
                << zkutil::ZooKeeper::error2string(code) + ". This shouldn't happen often.");
    }
}


void ReplicatedMergeTreeQueue::remove(zkutil::ZooKeeperPtr zookeeper, LogEntryPtr & entry)
{
    auto code = zookeeper->tryRemove(replica_path + "/queue/" + entry->znode_name);

    if (code)
        LOG_ERROR(log, "Couldn't remove " << replica_path << "/queue/" << entry->znode_name << ": "
            << zkutil::ZooKeeper::error2string(code) << ". This shouldn't happen often.");

    std::optional<time_t> min_unprocessed_insert_time_changed;
    std::optional<time_t> max_processed_insert_time_changed;

    {
        std::unique_lock<std::mutex> lock(queue_mutex);

        /// Remove the job from the queue in the RAM.
        /// You can not just refer to a pre-saved iterator, because someone else might be able to delete the task.
        /// Why do we view the queue from the end?
        ///  - because the task for execution first is moved to the end of the queue, so that in case of failure it remains at the end.
        for (Queue::iterator it = queue.end(); it != queue.begin();)
        {
            --it;
            if (*it == entry)
            {
                queue.erase(it);
                break;
            }
        }

        updateTimesOnRemoval(entry, min_unprocessed_insert_time_changed, max_processed_insert_time_changed, lock);
    }

    updateTimesInZooKeeper(zookeeper, min_unprocessed_insert_time_changed, max_processed_insert_time_changed);
}


bool ReplicatedMergeTreeQueue::remove(zkutil::ZooKeeperPtr zookeeper, const String & part_name)
{
    LogEntryPtr found;

    std::optional<time_t> min_unprocessed_insert_time_changed;
    std::optional<time_t> max_processed_insert_time_changed;

    {
        std::unique_lock<std::mutex> target_state_lock(target_state_mutex);
        std::unique_lock<std::mutex> queue_lock(queue_mutex);

        virtual_parts.remove(part_name);

        for (Queue::iterator it = queue.begin(); it != queue.end();)
        {
            if ((*it)->new_part_name == part_name)
            {
                found = *it;
                queue.erase(it++);
                updateTimesOnRemoval(found, min_unprocessed_insert_time_changed, max_processed_insert_time_changed, queue_lock);
                break;
            }
            else
                ++it;
        }
    }

    if (!found)
        return false;

    zookeeper->tryRemove(replica_path + "/queue/" + found->znode_name);
    updateTimesInZooKeeper(zookeeper, min_unprocessed_insert_time_changed, max_processed_insert_time_changed);

    return true;
}


bool ReplicatedMergeTreeQueue::removeFromVirtualParts(const MergeTreePartInfo & part_info)
{
    std::unique_lock<std::mutex> target_state_lock(target_state_mutex);
    return virtual_parts.remove(part_info);
}


void ReplicatedMergeTreeQueue::pullLogsToQueue(zkutil::ZooKeeperPtr zookeeper, zkutil::EventPtr next_update_event)
{
    std::lock_guard lock(pull_logs_to_queue_mutex);

    String index_str = zookeeper->get(replica_path + "/log_pointer");
    UInt64 index;

    Strings log_entries = zookeeper->getChildren(zookeeper_path + "/log", nullptr, next_update_event);

    /// We update mutations after we have loaded the list of log entries, but before we insert them
    /// in the queue.
    /// With this we ensure that if you read the log state L1 and then the state of mutations M1,
    /// then L1 "happened-before" M1.
    updateMutations(zookeeper, nullptr);

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

        /// ZK contains a limit on the number or total size of operations in a multi-request.
        /// If the limit is exceeded, the connection is simply closed.
        /// The constant is selected with a margin. The default limit in ZK is 1 MB of data in total.
        /// The average size of the node value in this case is less than 10 kilobytes.
        static constexpr auto MAX_MULTI_OPS = 100;

        for (size_t i = 0, size = log_entries.size(); i < size; i += MAX_MULTI_OPS)
        {
            auto begin = log_entries.begin() + i;
            auto end = i + MAX_MULTI_OPS >= log_entries.size()
                ? log_entries.end()
                : (begin + MAX_MULTI_OPS);
            auto last = end - 1;

            String last_entry = *last;
            if (!startsWith(last_entry, "log-"))
                throw Exception("Error in zookeeper data: unexpected node " + last_entry + " in " + zookeeper_path + "/log",
                    ErrorCodes::UNEXPECTED_NODE_IN_ZOOKEEPER);

            UInt64 last_entry_index = parse<UInt64>(last_entry.substr(strlen("log-")));

            LOG_DEBUG(log, "Pulling " << (end - begin) << " entries to queue: " << *begin << " - " << *last);

            std::vector<std::pair<String, std::future<zkutil::GetResponse>>> futures;
            futures.reserve(end - begin);

            for (auto it = begin; it != end; ++it)
                futures.emplace_back(*it, zookeeper->asyncGet(zookeeper_path + "/log/" + *it));

            /// Simultaneously add all new entries to the queue and move the pointer to the log.

            zkutil::Requests ops;
            std::vector<LogEntryPtr> copied_entries;
            copied_entries.reserve(end - begin);

            std::optional<time_t> min_unprocessed_insert_time_changed;

            for (auto & future : futures)
            {
                zkutil::GetResponse res = future.second.get();

                copied_entries.emplace_back(LogEntry::parse(res.data, res.stat));

                ops.emplace_back(zkutil::makeCreateRequest(
                    replica_path + "/queue/queue-", res.data, zkutil::CreateMode::PersistentSequential));

                const auto & entry = *copied_entries.back();
                if (entry.type == LogEntry::GET_PART)
                {
                    std::lock_guard lock(queue_mutex);
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
                std::lock_guard target_state_lock(target_state_mutex);

                log_pointer = last_entry_index + 1;

                std::lock_guard queue_lock(queue_mutex);

                for (size_t i = 0, size = copied_entries.size(); i < size; ++i)
                {
                    String path_created = dynamic_cast<const zkutil::CreateResponse &>(*responses[i]).path_created;
                    copied_entries[i]->znode_name = path_created.substr(path_created.find_last_of('/') + 1);

                    std::optional<time_t> unused = false;
                    insertUnlocked(copied_entries[i], unused, target_state_lock, queue_lock);
                }

                last_queue_update = time(nullptr);
            }
            catch (...)
            {
                /// If it fails, the data in RAM is incorrect. In order to avoid possible further corruption of data in ZK, we will kill ourselves.
                /// This is possible only if there is an unknown logical error.
                std::terminate();
            }

            if (!copied_entries.empty())
                LOG_DEBUG(log, "Pulled " << copied_entries.size() << " entries to queue.");
        }

        if (storage.queue_task_handle)
            storage.queue_task_handle->wake();
    }
}


void ReplicatedMergeTreeQueue::updateMutations(zkutil::ZooKeeperPtr zookeeper, zkutil::EventPtr next_update_event)
{
    std::lock_guard lock(update_mutations_mutex);

    Strings entries_in_zk = zookeeper->getChildren(zookeeper_path + "/mutations", nullptr, next_update_event);
    StringSet entries_in_zk_set(entries_in_zk.begin(), entries_in_zk.end());

    /// Compare with the local state, delete obsolete entries and determine which new entries to load.
    Strings entries_to_load;
    {
        std::lock_guard lock(target_state_mutex);

        for (auto it = mutations_by_znode.begin(); it != mutations_by_znode.end(); )
        {
            const ReplicatedMergeTreeMutationEntry & entry = it->second;
            if (!entries_in_zk_set.count(entry.znode_name))
            {
                LOG_DEBUG(log, "Removing obsolete mutation " + entry.znode_name + " from local state.");
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

    if (!entries_to_load.empty())
    {
        LOG_INFO(log, "Loading " + toString(entries_to_load.size()) + " mutation entries: "
            + entries_to_load.front() + " - " + entries_to_load.back());

        std::vector<std::future<zkutil::GetResponse>> futures;
        for (const String & entry : entries_to_load)
            futures.emplace_back(zookeeper->asyncGet(zookeeper_path + "/mutations/" + entry));

        std::vector<ReplicatedMergeTreeMutationEntry> new_mutations;
        for (size_t i = 0; i < entries_to_load.size(); ++i)
        {
            new_mutations.push_back(
                ReplicatedMergeTreeMutationEntry::parse(futures[i].get().data, entries_to_load[i]));
        }

        {
            std::lock_guard lock(target_state_mutex);

            for (ReplicatedMergeTreeMutationEntry & entry : new_mutations)
            {
                String znode = entry.znode_name;
                const ReplicatedMergeTreeMutationEntry & inserted_entry =
                    mutations_by_znode.emplace(znode, std::move(entry)).first->second;

                for (const auto & partition_and_block_num : inserted_entry.block_numbers)
                    mutations_by_partition[partition_and_block_num.first].emplace(
                        partition_and_block_num.second, &inserted_entry);
            }
        }
    }
}


ReplicatedMergeTreeQueue::StringSet ReplicatedMergeTreeQueue::moveSiblingPartsForMergeToEndOfQueue(const String & part_name)
{
    std::lock_guard lock(queue_mutex);

    /// Let's find the action to merge this part with others. Let's remember others.
    StringSet parts_for_merge;
    Queue::iterator merge_entry;
    for (Queue::iterator it = queue.begin(); it != queue.end(); ++it)
    {
        if ((*it)->type == LogEntry::MERGE_PARTS)
        {
            if (std::find((*it)->parts_to_merge.begin(), (*it)->parts_to_merge.end(), part_name)
                != (*it)->parts_to_merge.end())
            {
                parts_for_merge = StringSet((*it)->parts_to_merge.begin(), (*it)->parts_to_merge.end());
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

            if (((*it0)->type == LogEntry::MERGE_PARTS || (*it0)->type == LogEntry::GET_PART)
                && parts_for_merge.count((*it0)->new_part_name))
            {
                queue.splice(queue.end(), queue, it0, it);
            }
        }
    }

    return parts_for_merge;
}


void ReplicatedMergeTreeQueue::removeGetsAndMergesInRange(zkutil::ZooKeeperPtr zookeeper, const String & part_name)
{
    Queue to_wait;
    size_t removed_entries = 0;
    std::optional<time_t> min_unprocessed_insert_time_changed;
    std::optional<time_t> max_processed_insert_time_changed;

    /// Remove operations with parts, contained in the range to be deleted, from the queue.
    std::unique_lock<std::mutex> lock(queue_mutex);
    for (Queue::iterator it = queue.begin(); it != queue.end();)
    {
        if (((*it)->type == LogEntry::GET_PART || (*it)->type == LogEntry::MERGE_PARTS) &&
            MergeTreePartInfo::contains(part_name, (*it)->new_part_name, format_version))
        {
            if ((*it)->currently_executing)
                to_wait.push_back(*it);
            auto code = zookeeper->tryRemove(replica_path + "/queue/" + (*it)->znode_name);
            if (code)
                LOG_INFO(log, "Couldn't remove " << replica_path + "/queue/" + (*it)->znode_name << ": "
                    << zkutil::ZooKeeper::error2string(code));

            updateTimesOnRemoval(*it, min_unprocessed_insert_time_changed, max_processed_insert_time_changed, lock);
            queue.erase(it++);
            ++removed_entries;
        }
        else
            ++it;
    }

    updateTimesInZooKeeper(zookeeper, min_unprocessed_insert_time_changed, max_processed_insert_time_changed);

    LOG_DEBUG(log, "Removed " << removed_entries << " entries from queue. "
        "Waiting for " << to_wait.size() << " entries that are currently executing.");

    /// Let's wait for the operations with the parts contained in the range to be deleted.
    for (LogEntryPtr & entry : to_wait)
        entry->execution_complete.wait(lock, [&entry] { return !entry->currently_executing; });
}


ReplicatedMergeTreeQueue::Queue ReplicatedMergeTreeQueue::getConflictsForClearColumnCommand(
    const LogEntry & entry, String * out_conflicts_description, std::lock_guard<std::mutex> & /* queue_lock */) const
{
    Queue conflicts;

    for (auto & elem : queue)
    {
        if (elem->currently_executing && elem->znode_name != entry.znode_name)
        {
            if (elem->type == LogEntry::MERGE_PARTS || elem->type == LogEntry::GET_PART || elem->type == LogEntry::ATTACH_PART)
            {
                if (MergeTreePartInfo::contains(entry.new_part_name, elem->new_part_name, format_version))
                    conflicts.emplace_back(elem);
            }

            if (elem->type == LogEntry::CLEAR_COLUMN)
            {
                auto cur_part = MergeTreePartInfo::fromPartName(elem->new_part_name, format_version);
                auto part = MergeTreePartInfo::fromPartName(entry.new_part_name, format_version);

                if (part.partition_id == cur_part.partition_id)
                    conflicts.emplace_back(elem);
            }
        }
    }

    if (out_conflicts_description)
    {
        std::stringstream ss;
        ss << "Can't execute " << entry.typeToString() << " entry " << entry.znode_name << ". ";
        ss << "There are " << conflicts.size() << " currently executing entries blocking it: ";
        for (const auto & conflict : conflicts)
            ss << conflict->typeToString() << " " << conflict->new_part_name << " " << conflict->znode_name << ", ";

        *out_conflicts_description = ss.str();
    }

    return conflicts;
}


void ReplicatedMergeTreeQueue::disableMergesAndFetchesInRange(const LogEntry & entry)
{
    std::lock_guard lock(queue_mutex);
    String conflicts_description;

    if (!getConflictsForClearColumnCommand(entry, &conflicts_description, lock).empty())
        throw Exception(conflicts_description, ErrorCodes::UNFINISHED);

    if (!future_parts.count(entry.new_part_name))
        throw Exception("Expected that merges and fetches should be blocked in range " + entry.new_part_name + ". This is a bug", ErrorCodes::LOGICAL_ERROR);
}


bool ReplicatedMergeTreeQueue::isNotCoveredByFuturePartsImpl(const String & new_part_name, String & out_reason, std::lock_guard<std::mutex> & /* queue_lock */) const
{
    /// Let's check if the same part is now being created by another action.
    if (future_parts.count(new_part_name))
    {
        out_reason = "Not executing log entry for part " + new_part_name
            + " because another log entry for the same part is being processed. This shouldn't happen often.";
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
    for (const auto & future_part_name : future_parts)
    {
        auto future_part = MergeTreePartInfo::fromPartName(future_part_name, format_version);

        if (future_part.contains(result_part))
        {
            return false;
        }
    }

    return true;
}

bool ReplicatedMergeTreeQueue::addFuturePartIfNotCoveredByThem(const String & part_name, const LogEntry & entry, String & reject_reason)
{
    std::lock_guard lock(queue_mutex);

    if (isNotCoveredByFuturePartsImpl(part_name, reject_reason, lock))
    {
        CurrentlyExecuting::setActualPartName(entry, part_name, *this);
        return true;
    }

    return false;
}


bool ReplicatedMergeTreeQueue::shouldExecuteLogEntry(
    const LogEntry & entry,
    String & out_postpone_reason,
    MergeTreeDataMerger & merger,
    MergeTreeData & data,
    std::lock_guard<std::mutex> & queue_lock) const
{
    if (entry.type == LogEntry::MERGE_PARTS || entry.type == LogEntry::GET_PART || entry.type == LogEntry::ATTACH_PART)
    {
        if (!isNotCoveredByFuturePartsImpl(entry.new_part_name, out_postpone_reason, queue_lock))
        {
            if (!out_postpone_reason.empty())
                LOG_DEBUG(log, out_postpone_reason);
            return false;
        }
    }

    if (entry.type == LogEntry::MERGE_PARTS)
    {
        /** If any of the required parts are now transferred or in merge process, wait for the end of this operation.
          * Otherwise, even if all the necessary parts for the merge are not present, you should try to make a merge.
          * If any parts are missing, instead of merge, there will be an attempt to download a part.
          * Such a situation is possible if the receive of a part has failed, and it was moved to the end of the queue.
          */
        size_t sum_parts_size_in_bytes = 0;
        for (const auto & name : entry.parts_to_merge)
        {
            if (future_parts.count(name))
            {
                String reason = "Not merging into part " + entry.new_part_name
                    + " because part " + name + " is not ready yet (log entry for that part is being processed).";
                LOG_TRACE(log, reason);
                out_postpone_reason = reason;
                return false;
            }

            auto part = data.getPartIfExists(name, {MergeTreeDataPartState::PreCommitted, MergeTreeDataPartState::Committed, MergeTreeDataPartState::Outdated});
            if (part)
                sum_parts_size_in_bytes += part->bytes_on_disk;
        }

        if (merger.merges_blocker.isCancelled())
        {
            String reason = "Not executing log entry for part " + entry.new_part_name + " because merges are cancelled now.";
            LOG_DEBUG(log, reason);
            out_postpone_reason = reason;
            return false;
        }

        /** Execute merge only if there are enough free threads in background pool to do merges of that size.
          * But if all threads are free (maximal size of merge is allowed) then execute any merge,
          *  (because it may be ordered by OPTIMIZE or early with differrent settings).
          */
        size_t max_parts_size_for_merge = merger.getMaxPartsSizeForMerge();
        if (max_parts_size_for_merge != data.settings.max_bytes_to_merge_at_max_space_in_pool
            && sum_parts_size_in_bytes > max_parts_size_for_merge)
        {
            String reason = "Not executing log entry for part " + entry.new_part_name
                + " because its size (" + formatReadableSizeWithBinarySuffix(sum_parts_size_in_bytes)
                + ") is greater than current maximum (" + formatReadableSizeWithBinarySuffix(max_parts_size_for_merge) + ").";
            LOG_DEBUG(log, reason);
            out_postpone_reason = reason;
            return false;
        }
    }

    if (entry.type == LogEntry::CLEAR_COLUMN)
    {
        String conflicts_description;
        if (!getConflictsForClearColumnCommand(entry, &conflicts_description, queue_lock).empty())
        {
            LOG_DEBUG(log, conflicts_description);
            return false;
        }
    }

    return true;
}


ReplicatedMergeTreeQueue::CurrentlyExecuting::CurrentlyExecuting(ReplicatedMergeTreeQueue::LogEntryPtr & entry, ReplicatedMergeTreeQueue & queue)
    : entry(entry), queue(queue)
{
    entry->currently_executing = true;
    ++entry->num_tries;
    entry->last_attempt_time = time(nullptr);

    if (!queue.future_parts.insert(entry->new_part_name).second)
        throw Exception("Tagging already tagged future part " + entry->new_part_name + ". This is a bug.", ErrorCodes::LOGICAL_ERROR);
}


void ReplicatedMergeTreeQueue::CurrentlyExecuting::setActualPartName(const ReplicatedMergeTreeLogEntry & entry,
                                                                     const String & actual_part_name, ReplicatedMergeTreeQueue & queue)
{
    if (!entry.actual_new_part_name.empty())
        throw Exception("Entry actual part isn't empty yet. This is a bug.", ErrorCodes::LOGICAL_ERROR);

    entry.actual_new_part_name = actual_part_name;

    /// Check if it is the same (and already added) part.
    if (entry.actual_new_part_name == entry.new_part_name)
        return;

    if (!queue.future_parts.insert(entry.actual_new_part_name).second)
        throw Exception("Attaching already exsisting future part " + entry.actual_new_part_name + ". This is a bug.", ErrorCodes::LOGICAL_ERROR);
}


ReplicatedMergeTreeQueue::CurrentlyExecuting::~CurrentlyExecuting()
{
    std::lock_guard lock(queue.queue_mutex);

    entry->currently_executing = false;
    entry->execution_complete.notify_all();

    if (!queue.future_parts.erase(entry->new_part_name))
        LOG_ERROR(queue.log, "Untagging already untagged future part " + entry->new_part_name + ". This is a bug.");

    if (!entry->actual_new_part_name.empty())
    {
        if (entry->actual_new_part_name != entry->new_part_name && !queue.future_parts.erase(entry->actual_new_part_name))
            LOG_ERROR(queue.log, "Untagging already untagged future part " + entry->actual_new_part_name + ". This is a bug.");

        entry->actual_new_part_name.clear();
    }
}


ReplicatedMergeTreeQueue::SelectedEntry ReplicatedMergeTreeQueue::selectEntryToProcess(MergeTreeDataMerger & merger, MergeTreeData & data)
{
    std::lock_guard lock(queue_mutex);

    LogEntryPtr entry;

    for (auto it = queue.begin(); it != queue.end(); ++it)
    {
        if ((*it)->currently_executing)
            continue;

        if (shouldExecuteLogEntry(**it, (*it)->postpone_reason, merger, data, lock))
        {
            entry = *it;
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
        return { entry, std::unique_ptr<CurrentlyExecuting>{ new CurrentlyExecuting(entry, *this) } };
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
        if (func(entry))
            remove(get_zookeeper(), entry);
    }
    catch (...)
    {
        saved_exception = std::current_exception();
    }

    if (saved_exception)
    {
        std::lock_guard lock(queue_mutex);
        entry->exception = saved_exception;
        return false;
    }

    return true;
}


ReplicatedMergeTreeMergePredicate ReplicatedMergeTreeQueue::getMergePredicate(zkutil::ZooKeeperPtr & zookeeper) const
{
    ActiveDataPartSet cur_virtual_parts(format_version);
    Int64 cur_log_pointer;
    {
        std::lock_guard lock(target_state_mutex);
        cur_virtual_parts = virtual_parts;
        cur_log_pointer = log_pointer;
    }

    return ReplicatedMergeTreeMergePredicate(*this, std::move(cur_virtual_parts), cur_log_pointer, zookeeper);
}


void ReplicatedMergeTreeQueue::disableMergesInRange(const String & part_name)
{
    std::lock_guard lock(target_state_mutex);
    virtual_parts.add(part_name);
}


ReplicatedMergeTreeQueue::Status ReplicatedMergeTreeQueue::getStatus() const
{
    std::lock_guard lock(queue_mutex);

    Status res;

    res.future_parts = future_parts.size();
    res.queue_size = queue.size();
    res.last_queue_update = last_queue_update;

    res.inserts_in_queue = 0;
    res.merges_in_queue = 0;
    res.queue_oldest_time = 0;
    res.inserts_oldest_time = 0;
    res.merges_oldest_time = 0;

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
    }

    return res;
}


void ReplicatedMergeTreeQueue::getEntries(LogEntriesData & res) const
{
    res.clear();
    std::lock_guard lock(queue_mutex);

    res.reserve(queue.size());
    for (const auto & entry : queue)
        res.emplace_back(*entry);
}


size_t ReplicatedMergeTreeQueue::countMerges() const
{
    size_t all_merges = 0;

    std::lock_guard lock(queue_mutex);

    for (const auto & entry : queue)
        if (entry->type == LogEntry::MERGE_PARTS)
            ++all_merges;

    return all_merges;
}


void ReplicatedMergeTreeQueue::getInsertTimes(time_t & out_min_unprocessed_insert_time, time_t & out_max_processed_insert_time) const
{
    std::lock_guard lock(queue_mutex);
    out_min_unprocessed_insert_time = min_unprocessed_insert_time;
    out_max_processed_insert_time = max_processed_insert_time;
}


ReplicatedMergeTreeMergePredicate::ReplicatedMergeTreeMergePredicate(
    const ReplicatedMergeTreeQueue & queue_, ActiveDataPartSet virtual_parts_, Int64 log_pointer,
    zkutil::ZooKeeperPtr & zookeeper)
    : queue(queue_)
    , virtual_parts(std::move(virtual_parts_))
    , next_virtual_parts(virtual_parts)
{
    /// NOTE: virtual_parts are copied two times. More efficient is to store in next_virtual_parts
    /// only the parts that are not in virtual_parts but it will make the code more complicated.

    /// Load current inserts
    std::unordered_set<String> abandonable_lock_holders;
    for (const String & entry : zookeeper->getChildren(queue.zookeeper_path + "/temp"))
    {
        if (startsWith(entry, "abandonable_lock-"))
            abandonable_lock_holders.insert(queue.zookeeper_path + "/temp/" + entry);
    }

    if (!abandonable_lock_holders.empty())
    {
        Strings partitions = zookeeper->getChildren(queue.zookeeper_path + "/block_numbers");
        std::vector<std::future<zkutil::ListResponse>> lock_futures;
        for (const String & partition : partitions)
            lock_futures.push_back(zookeeper->asyncGetChildren(queue.zookeeper_path + "/block_numbers/" + partition));

        struct BlockInfo
        {
            String partition;
            Int64 number;
            String zk_path;
            std::future<zkutil::GetResponse> contents_future;
        };

        std::vector<BlockInfo> block_infos;
        for (size_t i = 0; i < partitions.size(); ++i)
        {
            Strings partition_block_numbers = lock_futures[i].get().names;
            for (const String & entry : partition_block_numbers)
            {
                /// TODO: cache block numbers that are abandoned.
                /// We won't need to check them on the next iteration.
                Int64 block_number = parse<Int64>(entry.substr(strlen("block-")));
                String zk_path = queue.zookeeper_path + "/block_numbers/" + partitions[i] + "/" + entry;
                block_infos.push_back(
                    BlockInfo{partitions[i], block_number, zk_path, zookeeper->asyncTryGet(zk_path)});
            }
        }

        for (BlockInfo & block : block_infos)
        {
            zkutil::GetResponse resp = block.contents_future.get();
            if (!resp.error && abandonable_lock_holders.count(resp.data))
                current_inserts[block.partition].insert(block.number);
        }
    }

    /// Load log entries that appeared after we loaded virtual_parts.
    Strings new_log_entries = zookeeper->getChildren(queue.zookeeper_path + "/log");
    String min_log_entry = "log-" + padIndex(log_pointer);
    new_log_entries.erase(
        std::remove_if(new_log_entries.begin(), new_log_entries.end(),
            [&](const String & entry) { return entry < min_log_entry; }),
        new_log_entries.end());

    std::vector<std::future<zkutil::GetResponse>> new_log_entry_futures;
    for (const String & entry : new_log_entries)
        new_log_entry_futures.push_back(
            zookeeper->asyncTryGet(queue.zookeeper_path + "/log/" + entry));

    for (auto & future : new_log_entry_futures)
    {
        zkutil::GetResponse res = future.get();
        if (res.error == ZooKeeperImpl::ZooKeeper::ZOK)
            next_virtual_parts.add(ReplicatedMergeTreeLogEntry::parse(res.data, res.stat)->new_part_name);
    }

    /// Load current quorum status.
    zookeeper->tryGet(queue.zookeeper_path + "/quorum/last_part", last_quorum_part);

    String quorum_status_str;
    if (zookeeper->tryGet(queue.zookeeper_path + "/quorum/status", quorum_status_str))
    {
        ReplicatedMergeTreeQuorumEntry quorum_status;
        quorum_status.fromString(quorum_status_str);
        inprogress_quorum_part = quorum_status.part_name;
    }
    else
        inprogress_quorum_part.clear();
}

bool ReplicatedMergeTreeMergePredicate::operator()(
        const MergeTreeData::DataPartPtr & left, const MergeTreeData::DataPartPtr & right,
        String * out_reason) const
{
    /// A sketch of a proof of why this method actually works:
    ///
    /// The trickiest part is to ensure that no new parts will ever appear in the range of blocks between left and right.
    /// Inserted parts get their block numbers by acquiring an abandonable lock (see AbandonableLockInZooKeeper.h).
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
    /// * load virtual_parts (a set of parts which corresponds to executing the replication logs up to a certain point)
    /// * load current_inserts (inserts that have already acquired a block number but haven't appeared in the log yet)
    /// * load virtual_parts again and store it in next_virtual_parts
    ///
    /// Now we have an invariant: if some part is in virtual_parts then all parts with smaller block numbers are
    /// either in current_inserts or in next_virtual_parts (those that managed to commit before we loaded current_inserts).
    ///
    /// So to check that no new parts will ever appear in the range of blocks between left and right we first check that
    /// left and right are already present in virtual_parts (we can't give a definite answer for parts that were committed later)
    /// and then check that there aren't any parts between them in either current_inserts or next_virtual_parts.

    if (left->info.partition_id != right->info.partition_id)
    {
        if (out_reason)
            *out_reason = "Parts " + left->name + " and " + right->name + " belong to different partitions";
        return false;
    }

    for (const MergeTreeData::DataPartPtr & part : {left, right})
    {
        if (part->name == last_quorum_part)
        {
            if (out_reason)
                *out_reason = "Part " + part->name + " is the most recent part with a satisfied quorum";
            return false;
        }

        if (part->name == inprogress_quorum_part)
        {
            if (out_reason)
                *out_reason = "Quorum insert for part " + part->name + " is currently in progress";
            return false;
        }

        if (virtual_parts.getContainingPart(part->info).empty())
        {
            if (out_reason)
                *out_reason = "Entry for part " + part->name + " hasn't been read from the replication log yet";
            return false;
        }

        /// We look for containing parts in next_virtual_parts (and not in virtual_parts) because next_virtual_parts is newer
        /// and it is guaranteed that it will contain all merges assigned before this object is constructed.
        String next_containing_part = next_virtual_parts.getContainingPart(part->info);
        if (next_containing_part != part->name)
        {
            if (out_reason)
                *out_reason = "Part " + part->name + " has already been assigned a merge into " + next_containing_part;
            return false;
        }
    }

    Int64 left_max_block = left->info.max_block;
    Int64 right_min_block = right->info.min_block;
    if (left_max_block > right_min_block)
        std::swap(left_max_block, right_min_block);

    if (left_max_block + 1 < right_min_block)
    {
        auto current_inserts_in_partition = current_inserts.find(left->info.partition_id);
        if (current_inserts_in_partition != current_inserts.end())
        {
            const std::set<Int64> & block_numbers = current_inserts_in_partition->second;

            auto block_it = block_numbers.upper_bound(left_max_block);
            if (block_it != block_numbers.end() && *block_it < right_min_block)
            {
                if (out_reason)
                    *out_reason = "Block number " + toString(*block_it) + " is still being inserted between parts "
                        + left->name + " and " + right->name;

                return false;
            }
        }

        MergeTreePartInfo gap_part_info(
            left->info.partition_id, left_max_block + 1, right_min_block - 1, 999999999);

        Strings covered = next_virtual_parts.getPartsCoveredBy(gap_part_info);
        if (!covered.empty())
        {
            if (out_reason)
                *out_reason = "There are " + toString(covered.size()) + " parts (from " + covered.front()
                    + " to " + covered.back() + ") that are still not present on this replica between "
                    + left->name + " and " + right->name;
            return false;
        }
    }

    return true;
}


String padIndex(Int64 index)
{
    String index_str = toString(index);
    return std::string(10 - index_str.size(), '0') + index_str;
}

}
