#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/MergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
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
    , current_parts(format_version)
    , virtual_parts(format_version)
{}


void ReplicatedMergeTreeQueue::addVirtualParts(const MergeTreeData::DataParts & parts)
{
    std::lock_guard lock(state_mutex);

    for (const auto & part : parts)
    {
        current_parts.add(part->name);
        virtual_parts.add(part->name);
    }
}


bool ReplicatedMergeTreeQueue::load(zkutil::ZooKeeperPtr zookeeper)
{
    auto queue_path = replica_path + "/queue";
    LOG_DEBUG(log, "Loading queue from " << queue_path);

    bool updated = false;
    std::optional<time_t> min_unprocessed_insert_time_changed;

    {
        std::lock_guard lock(state_mutex);

        String log_pointer_str = zookeeper->get(replica_path + "/log_pointer");
        log_pointer = log_pointer_str.empty() ? 0 : parse<UInt64>(log_pointer_str);

        Strings children = zookeeper->getChildren(queue_path);
        LOG_DEBUG(log, "Having " << children.size() << " queue entries to load.");
        std::sort(children.begin(), children.end());

        zkutil::AsyncResponses<zkutil::GetResponse> futures;
        futures.reserve(children.size());

        for (const String & child : children)
            futures.emplace_back(child, zookeeper->asyncGet(queue_path + "/" + child));

        for (auto & future : futures)
        {
            zkutil::GetResponse res = future.second.get();
            LogEntryPtr entry = LogEntry::parse(res.data, res.stat);
            entry->znode_name = future.first;

            insertUnlocked(entry, min_unprocessed_insert_time_changed, lock);

            updated = true;
        }

        zookeeper->tryGet(replica_path + "/mutation_pointer", mutation_pointer);
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

    addVirtualParts(parts);
    load(zookeeper);
}


void ReplicatedMergeTreeQueue::insertUnlocked(
    const LogEntryPtr & entry, std::optional<time_t> & min_unprocessed_insert_time_changed,
    std::lock_guard<std::mutex> & /* state_lock */)
{
    for (const String & virtual_part_name : entry->getVirtualPartNames())
    {
        virtual_parts.add(virtual_part_name);
        updateMutationsPartsToDo(virtual_part_name, /* add = */ true);
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
    std::unique_lock<std::mutex> & /* queue_lock */)
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
        for (const String & virtual_part_name : entry->getVirtualPartNames())
        {
            Strings replaced_parts;
            current_parts.add(virtual_part_name, &replaced_parts);

            /// Each part from `replaced_parts` should become Obsolete as a result of executing the entry.
            /// So it is one less part to mutate for each mutation with block number greater than part_info.getDataVersion()
            for (const String & replaced_part_name : replaced_parts)
                updateMutationsPartsToDo(replaced_part_name, /* add = */ false);
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
    }
    else
    {
        for (const String & virtual_part_name : entry->getVirtualPartNames())
        {
            /// Because execution of the entry is unsuccessful, `virtual_part_name` will never appear
            /// so we won't need to mutate it.
            updateMutationsPartsToDo(virtual_part_name, /* add = */ false);
        }
    }
}


void ReplicatedMergeTreeQueue::updateMutationsPartsToDo(const String & part_name, bool add)
{
    auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);
    auto in_partition = mutations_by_partition.find(part_info.partition_id);
    if (in_partition == mutations_by_partition.end())
        return;

    bool some_mutations_are_probably_done = false;

    auto from_it = in_partition->second.upper_bound(part_info.getDataVersion());
    for (auto it = from_it; it != in_partition->second.end(); ++it)
    {
        it->second->parts_to_do += (add ? +1 : -1);
        if (it->second->parts_to_do <= 0)
            some_mutations_are_probably_done = true;
    }

    if (some_mutations_are_probably_done)
        storage.mutations_finalizing_task->schedule();
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


void ReplicatedMergeTreeQueue::removeProcessedEntry(zkutil::ZooKeeperPtr zookeeper, LogEntryPtr & entry)
{
    auto code = zookeeper->tryRemove(replica_path + "/queue/" + entry->znode_name);

    if (code)
        LOG_ERROR(log, "Couldn't remove " << replica_path << "/queue/" << entry->znode_name << ": "
            << zkutil::ZooKeeper::error2string(code) << ". This shouldn't happen often.");

    std::optional<time_t> min_unprocessed_insert_time_changed;
    std::optional<time_t> max_processed_insert_time_changed;

    bool found = false;
    size_t queue_size = 0;

    {
        std::unique_lock<std::mutex> lock(state_mutex);

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

    if (!found)
        throw Exception("Can't find " + entry->znode_name + " in the memory queue. It is a bug", ErrorCodes::LOGICAL_ERROR);

    notifySubscribers(queue_size);

    updateTimesInZooKeeper(zookeeper, min_unprocessed_insert_time_changed, max_processed_insert_time_changed);
}


bool ReplicatedMergeTreeQueue::remove(zkutil::ZooKeeperPtr zookeeper, const String & part_name)
{
    LogEntryPtr found;
    size_t queue_size = 0;

    std::optional<time_t> min_unprocessed_insert_time_changed;
    std::optional<time_t> max_processed_insert_time_changed;

    {
        std::unique_lock<std::mutex> lock(state_mutex);

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
    std::unique_lock<std::mutex> lock(state_mutex);
    return virtual_parts.remove(part_info);
}


void ReplicatedMergeTreeQueue::pullLogsToQueue(zkutil::ZooKeeperPtr zookeeper, zkutil::WatchCallback watch_callback)
{
    std::lock_guard lock(pull_logs_to_queue_mutex);

    String index_str = zookeeper->get(replica_path + "/log_pointer");
    UInt64 index;

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

            zkutil::AsyncResponses<zkutil::GetResponse> futures;
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
                    std::lock_guard lock(state_mutex);
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
                std::lock_guard lock(state_mutex);

                log_pointer = last_entry_index + 1;

                for (size_t i = 0, size = copied_entries.size(); i < size; ++i)
                {
                    String path_created = dynamic_cast<const zkutil::CreateResponse &>(*responses[i]).path_created;
                    copied_entries[i]->znode_name = path_created.substr(path_created.find_last_of('/') + 1);

                    std::optional<time_t> unused = false;
                    insertUnlocked(copied_entries[i], unused, lock);
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


static size_t countPartsToMutate(
    const ReplicatedMergeTreeMutationEntry & mutation, const ActiveDataPartSet & parts)
{
    size_t count = 0;
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
                ++count;
        }
    }

    return count;
}


void ReplicatedMergeTreeQueue::updateMutations(zkutil::ZooKeeperPtr zookeeper, zkutil::WatchCallback watch_callback)
{
    std::lock_guard lock(update_mutations_mutex);

    Strings entries_in_zk = zookeeper->getChildrenWatch(zookeeper_path + "/mutations", nullptr, watch_callback);
    StringSet entries_in_zk_set(entries_in_zk.begin(), entries_in_zk.end());

    /// Compare with the local state, delete obsolete entries and determine which new entries to load.
    Strings entries_to_load;
    {
        std::lock_guard lock(state_mutex);

        for (auto it = mutations_by_znode.begin(); it != mutations_by_znode.end(); )
        {
            const ReplicatedMergeTreeMutationEntry & entry = *it->second.entry;
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

        std::vector<ReplicatedMergeTreeMutationEntryPtr> new_mutations;
        for (size_t i = 0; i < entries_to_load.size(); ++i)
        {
            new_mutations.push_back(std::make_shared<ReplicatedMergeTreeMutationEntry>(
                ReplicatedMergeTreeMutationEntry::parse(futures[i].get().data, entries_to_load[i])));
        }

        bool some_mutations_are_probably_done = false;
        {
            std::lock_guard lock(state_mutex);

            for (const ReplicatedMergeTreeMutationEntryPtr & entry : new_mutations)
            {
                auto & mutation = mutations_by_znode.emplace(entry->znode_name, MutationStatus{entry, 0, false})
                    .first->second;

                for (const auto & pair : entry->block_numbers)
                {
                    const String & partition_id = pair.first;
                    Int64 block_num = pair.second;
                    mutations_by_partition[partition_id].emplace(block_num, &mutation);
                }

                /// Initialize `mutation.parts_to_do`. First we need to mutate all parts in `current_parts`.
                mutation.parts_to_do += countPartsToMutate(*entry, current_parts);

                /// And next we would need to mutate all parts with getDataVersion() greater than
                /// mutation block number that would appear as a result of executing the queue.
                for (const auto & queue_entry : queue)
                {
                    for (const String & produced_part_name : queue_entry->getVirtualPartNames())
                    {
                        auto part_info = MergeTreePartInfo::fromPartName(produced_part_name, format_version);
                        auto it = entry->block_numbers.find(part_info.partition_id);
                        if (it != entry->block_numbers.end() && it->second > part_info.getDataVersion())
                            ++mutation.parts_to_do;
                    }
                }

                if (mutation.parts_to_do == 0)
                    some_mutations_are_probably_done = true;
            }
        }

        storage.merge_selecting_task->schedule();

        if (some_mutations_are_probably_done)
            storage.mutations_finalizing_task->schedule();
    }
}


ReplicatedMergeTreeQueue::StringSet ReplicatedMergeTreeQueue::moveSiblingPartsForMergeToEndOfQueue(const String & part_name)
{
    std::lock_guard lock(state_mutex);

    /// Let's find the action to merge this part with others. Let's remember others.
    StringSet parts_for_merge;
    Queue::iterator merge_entry;
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


void ReplicatedMergeTreeQueue::removePartProducingOpsInRange(zkutil::ZooKeeperPtr zookeeper, const MergeTreePartInfo & part_info)
{
    Queue to_wait;
    size_t removed_entries = 0;
    std::optional<time_t> min_unprocessed_insert_time_changed;
    std::optional<time_t> max_processed_insert_time_changed;

    /// Remove operations with parts, contained in the range to be deleted, from the queue.
    std::unique_lock<std::mutex> lock(state_mutex);
    for (Queue::iterator it = queue.begin(); it != queue.end();)
    {
        auto type = (*it)->type;

        if ((type == LogEntry::GET_PART || type == LogEntry::MERGE_PARTS || type == LogEntry::MUTATE_PART)
            && part_info.contains(MergeTreePartInfo::fromPartName((*it)->new_part_name, format_version)))
        {
            if ((*it)->currently_executing)
                to_wait.push_back(*it);
            auto code = zookeeper->tryRemove(replica_path + "/queue/" + (*it)->znode_name);
            if (code)
                LOG_INFO(log, "Couldn't remove " << replica_path + "/queue/" + (*it)->znode_name << ": "
                    << zkutil::ZooKeeper::error2string(code));

            updateStateOnQueueEntryRemoval(
                *it, /* is_successful = */ false,
                min_unprocessed_insert_time_changed, max_processed_insert_time_changed, lock);
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


size_t ReplicatedMergeTreeQueue::getConflictsCountForRange(
    const MergeTreePartInfo & range, const LogEntry & entry,
    String * out_description, std::lock_guard<std::mutex> & /* queue_lock */) const
{
    std::vector<std::pair<String, LogEntryPtr>> conflicts;

    for (auto & future_part_elem : future_parts)
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
        std::stringstream ss;
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
    std::lock_guard<std::mutex> lock(state_mutex);

    if (0 != getConflictsCountForRange(range, entry, &conflicts_description, lock))
        throw Exception(conflicts_description, ErrorCodes::UNFINISHED);
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
    for (const auto & future_part_elem : future_parts)
    {
        auto future_part = MergeTreePartInfo::fromPartName(future_part_elem.first, format_version);

        if (future_part.contains(result_part))
        {
            out_reason = "Not executing log entry for part " + new_part_name + " because it is covered by part "
                         + future_part_elem.first + " that is currently executing";
            return false;
        }
    }

    return true;
}

bool ReplicatedMergeTreeQueue::addFuturePartIfNotCoveredByThem(const String & part_name, LogEntry & entry, String & reject_reason)
{
    std::lock_guard lock(state_mutex);

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
    MergeTreeDataMergerMutator & merger_mutator,
    MergeTreeData & data,
    std::lock_guard<std::mutex> & queue_lock) const
{
    if (entry.type == LogEntry::MERGE_PARTS
        || entry.type == LogEntry::GET_PART
        || entry.type == LogEntry::MUTATE_PART)
    {
        for (const String & new_part_name : entry.getBlockingPartNames())
        {
            if (!isNotCoveredByFuturePartsImpl(new_part_name, out_postpone_reason, queue_lock))
            {
                if (!out_postpone_reason.empty())
                    LOG_DEBUG(log, out_postpone_reason);
                return false;
            }
        }
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

        if (merger_mutator.actions_blocker.isCancelled())
        {
            String reason = "Not executing log entry for part " + entry.new_part_name + " because merges and mutations are cancelled now.";
            LOG_DEBUG(log, reason);
            out_postpone_reason = reason;
            return false;
        }

        /** Execute merge only if there are enough free threads in background pool to do merges of that size.
          * But if all threads are free (maximal size of merge is allowed) then execute any merge,
          *  (because it may be ordered by OPTIMIZE or early with differrent settings).
          */
        size_t max_source_parts_size = merger_mutator.getMaxSourcePartsSize();
        if (max_source_parts_size != data.settings.max_bytes_to_merge_at_max_space_in_pool
            && sum_parts_size_in_bytes > max_source_parts_size)
        {
            String reason = "Not executing log entry for part " + entry.new_part_name
                + " because source parts size (" + formatReadableSizeWithBinarySuffix(sum_parts_size_in_bytes)
                + ") is greater than the current maximum (" + formatReadableSizeWithBinarySuffix(max_source_parts_size) + ").";
            LOG_DEBUG(log, reason);
            out_postpone_reason = reason;
            return false;
        }
    }

    /// TODO: it makes sense to check DROP_RANGE also
    if (entry.type == LogEntry::CLEAR_COLUMN || entry.type == LogEntry::REPLACE_RANGE)
    {
        String conflicts_description;
        String range_name = (entry.type == LogEntry::REPLACE_RANGE) ? entry.replace_range_entry->drop_range_part_name : entry.new_part_name;
        auto range = MergeTreePartInfo::fromPartName(range_name, format_version);

        if (0 != getConflictsCountForRange(range, entry, &conflicts_description, queue_lock))
        {
            LOG_DEBUG(log, conflicts_description);
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


ReplicatedMergeTreeQueue::CurrentlyExecuting::CurrentlyExecuting(const ReplicatedMergeTreeQueue::LogEntryPtr & entry_, ReplicatedMergeTreeQueue & queue)
    : entry(entry_), queue(queue)
{
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

    entry->currently_executing = false;
    entry->execution_complete.notify_all();

    for (const String & new_part_name : entry->getBlockingPartNames())
    {
        if (!queue.future_parts.erase(new_part_name))
            LOG_ERROR(queue.log, "Untagging already untagged future part " + new_part_name + ". This is a bug.");
    }

    if (!entry->actual_new_part_name.empty())
    {
        if (entry->actual_new_part_name != entry->new_part_name && !queue.future_parts.erase(entry->actual_new_part_name))
            LOG_ERROR(queue.log, "Untagging already untagged future part " + entry->actual_new_part_name + ". This is a bug.");

        entry->actual_new_part_name.clear();
    }
}


ReplicatedMergeTreeQueue::SelectedEntry ReplicatedMergeTreeQueue::selectEntryToProcess(MergeTreeDataMergerMutator & merger_mutator, MergeTreeData & data)
{
    LogEntryPtr entry;

    std::lock_guard<std::mutex> lock(state_mutex);

    for (auto it = queue.begin(); it != queue.end(); ++it)
    {
        if ((*it)->currently_executing)
            continue;

        if (shouldExecuteLogEntry(**it, (*it)->postpone_reason, merger_mutator, data, lock))
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
        return false;
    }

    return true;
}


ReplicatedMergeTreeMergePredicate ReplicatedMergeTreeQueue::getMergePredicate(zkutil::ZooKeeperPtr & zookeeper)
{
    return ReplicatedMergeTreeMergePredicate(*this, zookeeper);
}


MutationCommands ReplicatedMergeTreeQueue::getMutationCommands(
    const MergeTreeData::DataPartPtr & part, Int64 desired_mutation_version) const
{
    /// NOTE: If the corresponding mutation is not found, the error is logged (and not thrown as an exception)
    /// to allow recovering from a mutation that cannot be executed. This way you can delete the mutation entry
    /// from /mutations in ZK and the replicas will simply skip the mutation.

    if (part->info.getDataVersion() > desired_mutation_version)
    {
        LOG_WARNING(log, "Data version of part " << part->name << " is already greater than "
            "desired mutation version " << desired_mutation_version);
        return MutationCommands{};
    }

    std::lock_guard lock(state_mutex);

    auto in_partition = mutations_by_partition.find(part->info.partition_id);
    if (in_partition == mutations_by_partition.end())
    {
        LOG_ERROR(log, "There are no mutations for partition ID " << part->info.partition_id
            << " (trying to mutate part " << part->name << "to " << toString(desired_mutation_version) << ")");
        return MutationCommands{};
    }

    auto begin = in_partition->second.upper_bound(part->info.getDataVersion());

    auto end = in_partition->second.lower_bound(desired_mutation_version);
    if (end == in_partition->second.end() || end->first != desired_mutation_version)
        LOG_ERROR(log, "Mutation with version " << desired_mutation_version
            << " not found in partition ID " << part->info.partition_id
            << " (trying to mutate part " << part->name + ")");
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
                LOG_TRACE(log, "Marking mutation " << znode << " done because it is <= mutation_pointer (" << mutation_pointer << ")");
                mutation.is_done = true;
            }
            else if (mutation.parts_to_do == 0)
            {
                LOG_TRACE(log, "Will check if mutation " << mutation.entry->znode_name << " is done");
                candidates.push_back(mutation.entry);
            }
        }
    }

    if (candidates.empty())
        return false;

    auto merge_pred = getMergePredicate(zookeeper);

    std::vector<const ReplicatedMergeTreeMutationEntry *> finished;
    for (const ReplicatedMergeTreeMutationEntryPtr & candidate : candidates)
    {
        if (merge_pred.isMutationFinished(*candidate))
            finished.push_back(candidate.get());
    }

    if (!finished.empty())
        zookeeper->set(replica_path + "/mutation_pointer", finished.back()->znode_name);

    {
        std::lock_guard lock(state_mutex);

        for (const ReplicatedMergeTreeMutationEntry * entry : finished)
        {
            auto it = mutations_by_znode.find(entry->znode_name);
            if (it != mutations_by_znode.end())
            {
                LOG_TRACE(log, "Mutation " << entry->znode_name << " is done");
                it->second.is_done = true;
            }
        }
    }

    return candidates.size() != finished.size();
}


void ReplicatedMergeTreeQueue::disableMergesInRange(const String & part_name)
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


std::vector<MergeTreeMutationStatus> ReplicatedMergeTreeQueue::getMutationsStatus() const
{
    std::lock_guard lock(state_mutex);

    std::vector<MergeTreeMutationStatus> result;
    for (const auto & pair : mutations_by_znode)
    {
        const MutationStatus & status = pair.second;
        const ReplicatedMergeTreeMutationEntry & entry = *status.entry;

        for (const MutationCommand & command : entry.commands)
        {
            std::stringstream ss;
            formatAST(*command.ast, ss, false, true);
            result.push_back(MergeTreeMutationStatus
            {
                entry.znode_name,
                ss.str(),
                entry.create_time,
                entry.block_numbers,
                status.parts_to_do,
                status.is_done,
            });
        }
    }

    return result;
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
                if (startsWith(entry, "block-"))
                {
                    Int64 block_number = parse<Int64>(entry.substr(strlen("block-")));
                    String zk_path = queue.zookeeper_path + "/block_numbers/" + partitions[i] + "/" + entry;
                    block_infos.push_back(
                        BlockInfo{partitions[i], block_number, zk_path, zookeeper->asyncTryGet(zk_path)});
                }
            }
        }

        for (BlockInfo & block : block_infos)
        {
            zkutil::GetResponse resp = block.contents_future.get();
            if (!resp.error && abandonable_lock_holders.count(resp.data))
                committing_blocks[block.partition].insert(block.number);
        }
    }

    queue_.pullLogsToQueue(zookeeper);

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
        MergeTreePartInfo gap_part_info(
            left->info.partition_id, left_max_block + 1, right_min_block - 1,
            MergeTreePartInfo::MAX_LEVEL, MergeTreePartInfo::MAX_BLOCK_NUMBER);

        Strings covered = queue.virtual_parts.getPartsCoveredBy(gap_part_info);
        if (!covered.empty())
        {
            if (out_reason)
                *out_reason = "There are " + toString(covered.size()) + " parts (from " + covered.front()
                    + " to " + covered.back() + ") that are still not present on this replica between "
                    + left->name + " and " + right->name;
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


size_t ReplicatedMergeTreeMergePredicate::countMergesAndPartMutations() const
{
    std::lock_guard lock(queue.state_mutex);

    size_t count = 0;
    for (const auto & entry : queue.queue)
        if (entry->type == ReplicatedMergeTreeLogEntry::MERGE_PARTS
            || entry->type == ReplicatedMergeTreeLogEntry::MUTATE_PART)
            ++count;

    return count;
}


size_t ReplicatedMergeTreeMergePredicate::countMutations() const
{
    std::lock_guard lock(queue.state_mutex);
    return queue.mutations_by_znode.size();
}


std::optional<Int64> ReplicatedMergeTreeMergePredicate::getDesiredMutationVersion(const MergeTreeData::DataPartPtr & part) const
{
    /// Assigning mutations is easier than assigning merges because mutations appear in the same order as
    /// the order of their version numbers (see StorageReplicatedMergeTree::mutate()).
    /// This means that if we have loaded the mutation with version number X then all mutations with
    /// the version numbers less than X are also loaded and if there is no merge or mutation assigned to
    /// the part (checked by querying queue.virtual_parts), we can confidently assign a mutation to
    /// version X for this part.

    if (part->name == last_quorum_part
        || part->name == inprogress_quorum_part)
        return {};

    std::lock_guard lock(queue.state_mutex);

    if (queue.virtual_parts.getContainingPart(part->info) != part->name)
        return {};

    auto in_partition = queue.mutations_by_partition.find(part->info.partition_id);
    if (in_partition == queue.mutations_by_partition.end())
        return {};

    Int64 current_version = queue.getCurrentMutationVersionImpl(part->info.partition_id, part->info.getDataVersion(), lock);
    Int64 max_version = in_partition->second.rbegin()->first;
    if (current_version >= max_version)
        return {};

    return max_version;
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
                LOG_TRACE(queue.log, "Mutation " << mutation.znode_name << " is not done yet because "
                    << "in partition ID " << partition_id  << " there are still "
                    << blocks_count << " uncommitted blocks.");
                return false;
            }
        }
    }

    {
        std::lock_guard lock(queue.state_mutex);

        size_t suddenly_appeared_parts = countPartsToMutate(mutation, queue.virtual_parts);
        if (suddenly_appeared_parts)
        {
            LOG_TRACE(queue.log, "Mutation " << mutation.znode_name << " is not done yet because "
                << suddenly_appeared_parts << " parts to mutate suddenly appeared.");
            return false;
        }
    }

    return true;
}


ReplicatedMergeTreeQueue::SubscriberHandler
ReplicatedMergeTreeQueue::addSubscriber(ReplicatedMergeTreeQueue::SubscriberCallBack && callback)
{
    std::lock_guard<std::mutex> lock(state_mutex);
    std::lock_guard<std::mutex> lock_subscribers(subscribers_mutex);

    auto it = subscribers.emplace(subscribers.end(), std::move(callback));

    /// Atomically notify about current size
    (*it)(queue.size());

    return SubscriberHandler(it, *this);
}

ReplicatedMergeTreeQueue::SubscriberHandler::~SubscriberHandler()
{
    std::lock_guard<std::mutex> lock(queue.subscribers_mutex);
    queue.subscribers.erase(it);
}

void ReplicatedMergeTreeQueue::notifySubscribers(size_t new_queue_size)
{
    std::lock_guard<std::mutex> lock_subscribers(subscribers_mutex);
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

}
