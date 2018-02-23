#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/MergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataMerger.h>
#include <Common/StringUtils/StringUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_NODE_IN_ZOOKEEPER;
    extern const int UNFINISHED;
}


void ReplicatedMergeTreeQueue::initVirtualParts(const MergeTreeData::DataParts & parts)
{
    std::lock_guard<std::mutex> lock(mutex);

    for (const auto & part : parts)
        virtual_parts.add(part->name);
}


bool ReplicatedMergeTreeQueue::load(zkutil::ZooKeeperPtr zookeeper)
{
    auto queue_path = replica_path + "/queue";
    LOG_DEBUG(log, "Loading queue from " << queue_path);

    bool updated = false;
    bool min_unprocessed_insert_time_changed = false;
    {
        std::lock_guard<std::mutex> lock(mutex);

        std::unordered_set<String> already_loaded_paths;
        for (const LogEntryPtr & log_entry : queue)
            already_loaded_paths.insert(log_entry->znode_name);

        Strings children = zookeeper->getChildren(queue_path);
        auto to_remove_it = std::remove_if(
            children.begin(), children.end(), [&](const String & path)
            {
                return already_loaded_paths.count(path);
            });

        LOG_DEBUG(log,
            "Having " << (to_remove_it - children.begin()) << " queue entries to load, "
            << (children.end() - to_remove_it) << " entries already loaded.");
        children.erase(to_remove_it, children.end());

        std::sort(children.begin(), children.end());

        std::vector<std::pair<String, zkutil::ZooKeeper::GetFuture>> futures;
        futures.reserve(children.size());

        for (const String & child : children)
            futures.emplace_back(child, zookeeper->asyncGet(queue_path + "/" + child));

        for (auto & future : futures)
        {
            zkutil::ZooKeeper::ValueAndStat res = future.second.get();
            LogEntryPtr entry = LogEntry::parse(res.value, res.stat);
            entry->znode_name = future.first;

            time_t prev_min_unprocessed_insert_time = min_unprocessed_insert_time;

            insertUnlocked(entry);

            updated = true;
            if (min_unprocessed_insert_time != prev_min_unprocessed_insert_time)
                min_unprocessed_insert_time_changed = true;
        }
    }

    updateTimesInZooKeeper(zookeeper, min_unprocessed_insert_time_changed, false);

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


void ReplicatedMergeTreeQueue::insertUnlocked(LogEntryPtr & entry)
{
    virtual_parts.add(entry->new_part_name);
    queue.push_back(entry);

    if (entry->type == LogEntry::GET_PART)
    {
        inserts_by_time.insert(entry);

        if (entry->create_time && (!min_unprocessed_insert_time || entry->create_time < min_unprocessed_insert_time))
            min_unprocessed_insert_time = entry->create_time;
    }
}


void ReplicatedMergeTreeQueue::insert(zkutil::ZooKeeperPtr zookeeper, LogEntryPtr & entry)
{
    time_t prev_min_unprocessed_insert_time;

    {
        std::lock_guard<std::mutex> lock(mutex);
        prev_min_unprocessed_insert_time = min_unprocessed_insert_time;
        insertUnlocked(entry);
    }

    if (min_unprocessed_insert_time != prev_min_unprocessed_insert_time)
        updateTimesInZooKeeper(zookeeper, true, false);
}


void ReplicatedMergeTreeQueue::updateTimesOnRemoval(
    const LogEntryPtr & entry,
    bool & min_unprocessed_insert_time_changed,
    bool & max_processed_insert_time_changed)
{
    if (entry->type != LogEntry::GET_PART)
        return;

    inserts_by_time.erase(entry);

    if (inserts_by_time.empty())
    {
        min_unprocessed_insert_time = 0;
        min_unprocessed_insert_time_changed = true;
    }
    else if ((*inserts_by_time.begin())->create_time > min_unprocessed_insert_time)
    {
        min_unprocessed_insert_time = (*inserts_by_time.begin())->create_time;
        min_unprocessed_insert_time_changed = true;
    }

    if (entry->create_time > max_processed_insert_time)
    {
        max_processed_insert_time = entry->create_time;
        max_processed_insert_time_changed = true;
    }
}


void ReplicatedMergeTreeQueue::updateTimesInZooKeeper(
    zkutil::ZooKeeperPtr zookeeper,
    bool min_unprocessed_insert_time_changed,
    bool max_processed_insert_time_changed)
{
    /// Here there can be a race condition (with different remove at the same time).
    /// Consider it unimportant (for a short time, ZK will have a slightly different time value).
    /// We also read values of `min_unprocessed_insert_time`, `max_processed_insert_time` without synchronization.
    zkutil::Ops ops;

    if (min_unprocessed_insert_time_changed)
        ops.emplace_back(std::make_unique<zkutil::Op::SetData>(
            replica_path + "/min_unprocessed_insert_time", toString(min_unprocessed_insert_time), -1));

    if (max_processed_insert_time_changed)
        ops.emplace_back(std::make_unique<zkutil::Op::SetData>(
            replica_path + "/max_processed_insert_time", toString(max_processed_insert_time), -1));

    if (!ops.empty())
    {
        auto code = zookeeper->tryMulti(ops);

        if (code != ZOK)
            LOG_ERROR(log, "Couldn't set value of nodes for insert times ("
                << replica_path << "/min_unprocessed_insert_time, max_processed_insert_time)" << ": "
                << zkutil::ZooKeeper::error2string(code) + ". This shouldn't happen often.");
    }
}


void ReplicatedMergeTreeQueue::remove(zkutil::ZooKeeperPtr zookeeper, LogEntryPtr & entry)
{
    auto code = zookeeper->tryRemove(replica_path + "/queue/" + entry->znode_name);

    if (code != ZOK)
        LOG_ERROR(log, "Couldn't remove " << replica_path << "/queue/" << entry->znode_name << ": "
            << zkutil::ZooKeeper::error2string(code) << ". This shouldn't happen often.");

    bool min_unprocessed_insert_time_changed = false;
    bool max_processed_insert_time_changed = false;

    {
        std::lock_guard<std::mutex> lock(mutex);

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

        updateTimesOnRemoval(entry, min_unprocessed_insert_time_changed, max_processed_insert_time_changed);
    }

    updateTimesInZooKeeper(zookeeper, min_unprocessed_insert_time_changed, max_processed_insert_time_changed);
}


bool ReplicatedMergeTreeQueue::remove(zkutil::ZooKeeperPtr zookeeper, const String & part_name)
{
    LogEntryPtr found;

    bool min_unprocessed_insert_time_changed = false;
    bool max_processed_insert_time_changed = false;

    {
        std::lock_guard<std::mutex> lock(mutex);

        for (Queue::iterator it = queue.begin(); it != queue.end();)
        {
            if ((*it)->new_part_name == part_name)
            {
                found = *it;
                queue.erase(it++);
                updateTimesOnRemoval(found, min_unprocessed_insert_time_changed, max_processed_insert_time_changed);
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


bool ReplicatedMergeTreeQueue::pullLogsToQueue(zkutil::ZooKeeperPtr zookeeper, zkutil::EventPtr next_update_event)
{
    std::lock_guard<std::mutex> lock(pull_logs_to_queue_mutex);

    bool dirty_entries_loaded = false;
    if (is_dirty)
    {
        dirty_entries_loaded = load(zookeeper);
        is_dirty = false;
    }

    String index_str = zookeeper->get(replica_path + "/log_pointer");
    UInt64 index;

    Strings log_entries = zookeeper->getChildren(zookeeper_path + "/log");

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

            std::vector<std::pair<String, zkutil::ZooKeeper::GetFuture>> futures;
            futures.reserve(end - begin);

            for (auto it = begin; it != end; ++it)
                futures.emplace_back(*it, zookeeper->asyncGet(zookeeper_path + "/log/" + *it));

            /// Simultaneously add all new entries to the queue and move the pointer to the log.

            zkutil::Ops ops;
            std::vector<LogEntryPtr> copied_entries;
            copied_entries.reserve(end - begin);

            bool min_unprocessed_insert_time_changed = false;

            for (auto & future : futures)
            {
                zkutil::ZooKeeper::ValueAndStat res = future.second.get();
                copied_entries.emplace_back(LogEntry::parse(res.value, res.stat));

                ops.emplace_back(std::make_unique<zkutil::Op::Create>(
                    replica_path + "/queue/queue-", res.value, zookeeper->getDefaultACL(), zkutil::CreateMode::PersistentSequential));

                const auto & entry = *copied_entries.back();
                if (entry.type == LogEntry::GET_PART)
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    if (entry.create_time && (!min_unprocessed_insert_time || entry.create_time < min_unprocessed_insert_time))
                    {
                        min_unprocessed_insert_time = entry.create_time;
                        min_unprocessed_insert_time_changed = true;
                    }
                }
            }

            ops.emplace_back(std::make_unique<zkutil::Op::SetData>(
                replica_path + "/log_pointer", toString(last_entry_index + 1), -1));

            if (min_unprocessed_insert_time_changed)
                ops.emplace_back(std::make_unique<zkutil::Op::SetData>(
                    replica_path + "/min_unprocessed_insert_time", toString(min_unprocessed_insert_time), -1));

            try
            {
                zookeeper->multi(ops);
            }
            catch (const zkutil::KeeperException & ex)
            {
                if (ex.isTemporaryError())
                {
                    LOG_WARNING(log, "Unknown status of queue update, marking queue dirty (will reload on next iteration).");
                    is_dirty = true;
                }

                throw;
            }

            /// Now we have successfully updated the queue in ZooKeeper. Update it in RAM.

            try
            {
                std::lock_guard<std::mutex> lock(mutex);

                for (size_t i = 0, size = copied_entries.size(); i < size; ++i)
                {
                    String path_created = dynamic_cast<zkutil::Op::Create &>(*ops[i]).getPathCreated();
                    copied_entries[i]->znode_name = path_created.substr(path_created.find_last_of('/') + 1);

                    insertUnlocked(copied_entries[i]);
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
    }

    if (next_update_event)
    {
        if (zookeeper->exists(zookeeper_path + "/log/log-" + padIndex(index), nullptr, next_update_event))
            next_update_event->set();
    }

    return dirty_entries_loaded || !log_entries.empty();
}


ReplicatedMergeTreeQueue::StringSet ReplicatedMergeTreeQueue::moveSiblingPartsForMergeToEndOfQueue(const String & part_name)
{
    std::lock_guard<std::mutex> lock(mutex);

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
        /// Move to the end of queue actions that are receiving `parts_for_merge`.
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
    bool min_unprocessed_insert_time_changed = false;
    bool max_processed_insert_time_changed = false;

    /// Remove operations with parts, contained in the range to be deleted, from the queue.
    std::unique_lock<std::mutex> lock(mutex);
    for (Queue::iterator it = queue.begin(); it != queue.end();)
    {
        if (((*it)->type == LogEntry::GET_PART || (*it)->type == LogEntry::MERGE_PARTS) &&
            MergeTreePartInfo::contains(part_name, (*it)->new_part_name, format_version))
        {
            if ((*it)->currently_executing)
                to_wait.push_back(*it);
            auto code = zookeeper->tryRemove(replica_path + "/queue/" + (*it)->znode_name);
            if (code != ZOK)
                LOG_INFO(log, "Couldn't remove " << replica_path + "/queue/" + (*it)->znode_name << ": "
                    << zkutil::ZooKeeper::error2string(code));

            updateTimesOnRemoval(*it, min_unprocessed_insert_time_changed, max_processed_insert_time_changed);
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
    const LogEntry & entry, String * out_conflicts_description)
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
    std::lock_guard<std::mutex> lock(mutex);
    String conflicts_description;

    if (!getConflictsForClearColumnCommand(entry, &conflicts_description).empty())
        throw Exception(conflicts_description, ErrorCodes::UNFINISHED);

    if (!future_parts.count(entry.new_part_name))
        throw Exception("Expected that merges and fetches should be blocked in range " + entry.new_part_name + ". This is a bug", ErrorCodes::LOGICAL_ERROR);
}


bool ReplicatedMergeTreeQueue::isNotCoveredByFuturePartsImpl(const String & new_part_name, String & out_reason)
{
    /// mutex should been already acquired

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
    std::lock_guard<std::mutex> lock(mutex);

    if (isNotCoveredByFuturePartsImpl(part_name, reject_reason))
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
    MergeTreeData & data)
{
    /// mutex has already been acquired. The function is called only from `selectEntryToProcess`.

    if (entry.type == LogEntry::MERGE_PARTS || entry.type == LogEntry::GET_PART || entry.type == LogEntry::ATTACH_PART)
    {
        if (!isNotCoveredByFuturePartsImpl(entry.new_part_name, out_postpone_reason))
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
                sum_parts_size_in_bytes += part->size_in_bytes;
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
        if (!getConflictsForClearColumnCommand(entry, &conflicts_description).empty())
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
    std::lock_guard<std::mutex> lock(queue.mutex);

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
    std::lock_guard<std::mutex> lock(mutex);

    LogEntryPtr entry;

    for (auto it = queue.begin(); it != queue.end(); ++it)
    {
        if ((*it)->currently_executing)
            continue;

        if (shouldExecuteLogEntry(**it, (*it)->postpone_reason, merger, data))
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
        std::lock_guard<std::mutex> lock(mutex);
        entry->exception = saved_exception;
        return false;
    }

    return true;
}


bool ReplicatedMergeTreeQueue::partWillBeMergedOrMergesDisabled(const String & part_name) const
{
    return virtual_parts.getContainingPart(part_name) != part_name;
}

void ReplicatedMergeTreeQueue::disableMergesInRange(const String & part_name)
{
    virtual_parts.add(part_name);
}



ReplicatedMergeTreeQueue::Status ReplicatedMergeTreeQueue::getStatus()
{
    std::lock_guard<std::mutex> lock(mutex);

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


void ReplicatedMergeTreeQueue::getEntries(LogEntriesData & res)
{
    res.clear();
    std::lock_guard<std::mutex> lock(mutex);

    res.reserve(queue.size());
    for (const auto & entry : queue)
        res.emplace_back(*entry);
}


size_t ReplicatedMergeTreeQueue::countMerges()
{
    size_t all_merges = 0;

    std::lock_guard<std::mutex> lock(mutex);

    for (const auto & entry : queue)
        if (entry->type == LogEntry::MERGE_PARTS)
            ++all_merges;

    return all_merges;
}


void ReplicatedMergeTreeQueue::getInsertTimes(time_t & out_min_unprocessed_insert_time, time_t & out_max_processed_insert_time) const
{
    std::lock_guard<std::mutex> lock(mutex);
    out_min_unprocessed_insert_time = min_unprocessed_insert_time;
    out_max_processed_insert_time = max_processed_insert_time;
}


String padIndex(Int64 index)
{
    String index_str = toString(index);
    return std::string(10 - index_str.size(), '0') + index_str;
}

}
