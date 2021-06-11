#include <Storages/MergeTree/ReplicatedMergeTreePartCheckThread.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/MergeTree/ReplicatedMergeTreePartHeader.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Interpreters/Context.h>


namespace ProfileEvents
{
    extern const Event ReplicatedPartChecks;
    extern const Event ReplicatedPartChecksFailed;
    extern const Event ReplicatedDataLoss;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_DIFFERS_TOO_MUCH;
}

static const auto PART_CHECK_ERROR_SLEEP_MS = 5 * 1000;


ReplicatedMergeTreePartCheckThread::ReplicatedMergeTreePartCheckThread(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , log_name(storage.getStorageID().getFullTableName() + " (ReplicatedMergeTreePartCheckThread)")
    , log(&Poco::Logger::get(log_name))
{
    task = storage.global_context.getSchedulePool().createTask(log_name, [this] { run(); });
    task->schedule();
}

ReplicatedMergeTreePartCheckThread::~ReplicatedMergeTreePartCheckThread()
{
    stop();
}

void ReplicatedMergeTreePartCheckThread::start()
{
    std::lock_guard lock(start_stop_mutex);
    need_stop = false;
    task->activateAndSchedule();
}

void ReplicatedMergeTreePartCheckThread::stop()
{
    //based on discussion on https://github.com/ClickHouse/ClickHouse/pull/1489#issuecomment-344756259
    //using the schedule pool there is no problem in case stop is called two time in row and the start multiple times

    std::lock_guard lock(start_stop_mutex);
    need_stop = true;
    task->deactivate();
}

void ReplicatedMergeTreePartCheckThread::enqueuePart(const String & name, time_t delay_to_check_seconds)
{
    std::lock_guard lock(parts_mutex);

    if (parts_set.count(name))
        return;

    parts_queue.emplace_back(name, time(nullptr) + delay_to_check_seconds);
    parts_set.insert(name);
    task->schedule();
}


size_t ReplicatedMergeTreePartCheckThread::size() const
{
    std::lock_guard lock(parts_mutex);
    return parts_set.size();
}


ReplicatedMergeTreePartCheckThread::MissingPartSearchResult ReplicatedMergeTreePartCheckThread::searchForMissingPartOnOtherReplicas(const String & part_name)
{
    auto zookeeper = storage.getZooKeeper();

    /// If the part is not in ZooKeeper, we'll check if it's at least somewhere.
    auto part_info = MergeTreePartInfo::fromPartName(part_name, storage.format_version);

    /** The logic is as follows:
        * - if some live or inactive replica has such a part, or a part covering it
        *   - it is Ok, nothing is needed, it is then downloaded when processing the queue, when the replica comes to life;
        *   - or, if the replica never comes to life, then the administrator will delete or create a new replica with the same address and see everything from the beginning;
        * - if no one has such part or a part covering it, then
        *   - if there are two smaller parts, one with the same min block and the other with the same
        *     max block, we hope that all parts in between are present too and the needed part
        *     will appear on other replicas as a result of a merge.
        *   - otherwise, consider the part lost and delete the entry from the queue.
        *
        *   Note that this logic is not perfect - some part in the interior may be missing and the
        *   needed part will never appear. But precisely determining whether the part will appear as
        *   a result of a merge is complicated - we can't just check if all block numbers covered
        *   by the missing part are present somewhere (because gaps between blocks are possible)
        *   and to determine the constituent parts of the merge we need to query the replication log
        *   (both the common log and the queues of the individual replicas) and then, if the
        *   constituent parts are in turn not found, solve the problem recursively for them.
        *
        *   Considering the part lost when it is not in fact lost is very dangerous because it leads
        *   to divergent replicas and intersecting parts. So we err on the side of caution
        *   and don't delete the queue entry when in doubt.
        */

    LOG_WARNING(log, "Checking if anyone has a part {} or covering part.", part_name);

    bool found_part_with_the_same_min_block = false;
    bool found_part_with_the_same_max_block = false;

    Strings replicas = zookeeper->getChildren(storage.zookeeper_path + "/replicas");
    for (const String & replica : replicas)
    {
        String replica_path = storage.zookeeper_path + "/replicas/" + replica;

        Strings parts = zookeeper->getChildren(replica_path + "/parts");
        for (const String & part_on_replica : parts)
        {
            auto part_on_replica_info = MergeTreePartInfo::fromPartName(part_on_replica, storage.format_version);

            if (part_info == part_on_replica_info)
            {
                /// Found missing part at ourself. If we are here then something wrong with this part, so skipping.
                if (replica_path == storage.replica_path)
                    continue;

                LOG_WARNING(log, "Found the missing part {} at {} on {}", part_name, part_on_replica, replica);
                return MissingPartSearchResult::FoundAndNeedFetch;
            }

            if (part_on_replica_info.contains(part_info))
            {
                LOG_WARNING(log, "Found part {} on {} that covers the missing part {}", part_on_replica, replica, part_name);
                return MissingPartSearchResult::FoundAndDontNeedFetch;
            }

            if (part_info.contains(part_on_replica_info))
            {
                if (part_on_replica_info.min_block == part_info.min_block)
                    found_part_with_the_same_min_block = true;
                if (part_on_replica_info.max_block == part_info.max_block)
                    found_part_with_the_same_max_block = true;

                if (found_part_with_the_same_min_block && found_part_with_the_same_max_block)
                {
                    LOG_WARNING(log, "Found parts with the same min block and with the same max block as the missing part {}. Hoping that it will eventually appear as a result of a merge.", part_name);
                    return MissingPartSearchResult::FoundAndDontNeedFetch;
                }
            }
        }
    }

    /// No one has such a part and the merge is impossible.
    String not_found_msg;
    if (found_part_with_the_same_max_block)
        not_found_msg = "a smaller part with the same max block.";
    else if (found_part_with_the_same_min_block)
        not_found_msg = "a smaller part with the same min block.";
    else
        not_found_msg = "smaller parts with either the same min block or the same max block.";
    LOG_ERROR(log, "No replica has part covering {} and a merge is impossible: we didn't find {}", part_name, not_found_msg);

    return MissingPartSearchResult::LostForever;
}

void ReplicatedMergeTreePartCheckThread::searchForMissingPartAndFetchIfPossible(const String & part_name, bool exists_in_zookeeper)
{
    auto zookeeper = storage.getZooKeeper();
    auto missing_part_search_result = searchForMissingPartOnOtherReplicas(part_name);

    /// If the part is in ZooKeeper, remove it from there and add the task to download it to the queue.
    if (exists_in_zookeeper)
    {
        /// If part found on some other replica
        if (missing_part_search_result == MissingPartSearchResult::FoundAndNeedFetch)
        {
            LOG_WARNING(log, "Part {} exists in ZooKeeper but not locally and found on other replica. Removing from ZooKeeper and queueing a fetch.", part_name);
            storage.removePartAndEnqueueFetch(part_name);
        }
        else /// If we have covering part on other replica or part is lost forever we don't need to fetch anything
        {
            LOG_WARNING(log, "Part {} exists in ZooKeeper but not locally and not found on other replica. Removing it from ZooKeeper.", part_name);
            storage.removePartFromZooKeeper(part_name);
        }
    }

    ProfileEvents::increment(ProfileEvents::ReplicatedPartChecksFailed);

    if (missing_part_search_result == MissingPartSearchResult::LostForever)
    {
        /// Is it in the replication queue? If there is - delete, because the task can not be processed.
        if (!storage.queue.remove(zookeeper, part_name))
        {
            /// The part was not in our queue.
            LOG_WARNING(log, "Missing part {} is not in our queue, this can happen rarely.", part_name);
        }

        /** This situation is possible if on all the replicas where the part was, it deteriorated.
            * For example, a replica that has just written it has power turned off and the data has not been written from cache to disk.
            */
        LOG_ERROR(log, "Part {} is lost forever.", part_name);
        ProfileEvents::increment(ProfileEvents::ReplicatedDataLoss);
    }
}

std::pair<bool, MergeTreeDataPartPtr> ReplicatedMergeTreePartCheckThread::findLocalPart(const String & part_name)
{
    auto zookeeper = storage.getZooKeeper();
    String part_path = storage.replica_path + "/parts/" + part_name;

    /// It's important to check zookeeper first and after that check local storage,
    /// because our checks of local storage and zookeeper are not consistent.
    /// If part exists in zookeeper and doesn't exists in local storage definitely require
    /// to fetch this part. But if we check local storage first and than check zookeeper
    /// some background process can successfully commit part between this checks (both to the local stoarge and zookeeper),
    /// but checker thread will remove part from zookeeper and queue fetch.
    bool exists_in_zookeeper = zookeeper->exists(part_path);

    /// If the part is still in the PreCommitted -> Committed transition, it is not lost
    /// and there is no need to go searching for it on other replicas. To definitely find the needed part
    /// if it exists (or a part containing it) we first search among the PreCommitted parts.
    auto part = storage.getPartIfExists(part_name, {MergeTreeDataPartState::PreCommitted});
    if (!part)
        part = storage.getActiveContainingPart(part_name);

    return std::make_pair(exists_in_zookeeper, part);
}

CheckResult ReplicatedMergeTreePartCheckThread::checkPart(const String & part_name)
{
    LOG_WARNING(log, "Checking part {}", part_name);
    ProfileEvents::increment(ProfileEvents::ReplicatedPartChecks);

    auto [exists_in_zookeeper, part] = findLocalPart(part_name);

    /// We do not have this or a covering part.
    if (!part)
    {
        searchForMissingPartAndFetchIfPossible(part_name, exists_in_zookeeper);
        return {part_name, false, "Part is missing, will search for it"};
    }

    /// We have this part, and it's active. We will check whether we need this part and whether it has the right data.
    if (part->name == part_name)
    {
        auto zookeeper = storage.getZooKeeper();
        auto table_lock = storage.lockForShare(RWLockImpl::NO_QUERY, storage.getSettings()->lock_acquire_timeout_for_background_operations);

        auto local_part_header = ReplicatedMergeTreePartHeader::fromColumnsAndChecksums(
            part->getColumns(), part->checksums);

        String part_path = storage.replica_path + "/parts/" + part_name;
        String part_znode;
        /// If the part is in ZooKeeper, check its data with its checksums, and them with ZooKeeper.
        if (zookeeper->tryGet(part_path, part_znode))
        {
            LOG_WARNING(log, "Checking data of part {}.", part_name);

            try
            {
                ReplicatedMergeTreePartHeader zk_part_header;
                if (!part_znode.empty())
                    zk_part_header = ReplicatedMergeTreePartHeader::fromString(part_znode);
                else
                {
                    String columns_znode = zookeeper->get(part_path + "/columns");
                    String checksums_znode = zookeeper->get(part_path + "/checksums");
                    zk_part_header = ReplicatedMergeTreePartHeader::fromColumnsAndChecksumsZNodes(
                        columns_znode, checksums_znode);
                }

                if (local_part_header.getColumnsHash() != zk_part_header.getColumnsHash())
                    throw Exception("Columns of local part " + part_name + " are different from ZooKeeper", ErrorCodes::TABLE_DIFFERS_TOO_MUCH);

                zk_part_header.getChecksums().checkEqual(local_part_header.getChecksums(), true);

                checkDataPart(
                    part,
                    true,
                    [this] { return need_stop.load(); });

                if (need_stop)
                {
                    LOG_INFO(log, "Checking part was cancelled.");
                    return {part_name, false, "Checking part was cancelled"};
                }

                LOG_INFO(log, "Part {} looks good.", part_name);
            }
            catch (const Exception & e)
            {
                /// Don't count the part as broken if there is not enough memory to load it.
                /// In fact, there can be many similar situations.
                /// But it is OK, because there is a safety guard against deleting too many parts.
                if (isNotEnoughMemoryErrorCode(e.code()))
                    throw;

                tryLogCurrentException(log, __PRETTY_FUNCTION__);

                String message = "Part " + part_name + " looks broken. Removing it and will try to fetch.";
                LOG_ERROR(log, message);

                /// Part is broken, let's try to find it and fetch.
                searchForMissingPartAndFetchIfPossible(part_name, exists_in_zookeeper);

                /// Delete part locally.
                storage.forgetPartAndMoveToDetached(part, "broken");
                return {part_name, false, message};
            }
        }
        else if (part->modification_time + MAX_AGE_OF_LOCAL_PART_THAT_WASNT_ADDED_TO_ZOOKEEPER < time(nullptr))
        {
            /// If the part is not in ZooKeeper, delete it locally.
            /// Probably, someone just wrote down the part, and has not yet added to ZK.
            /// Therefore, delete only if the part is old (not very reliable).
            ProfileEvents::increment(ProfileEvents::ReplicatedPartChecksFailed);

            String message = "Unexpected part " + part_name + " in filesystem. Removing.";
            LOG_ERROR(log, message);
            storage.forgetPartAndMoveToDetached(part, "unexpected");
            return {part_name, false, message};
        }
        else
        {
            /// TODO You need to make sure that the part is still checked after a while.
            /// Otherwise, it's possible that the part was not added to ZK,
            ///  but remained in the filesystem and in a number of active parts.
            /// And then for a long time (before restarting), the data on the replicas will be different.

            LOG_TRACE(log, "Young part {} with age {} seconds hasn't been added to ZooKeeper yet. It's ok.", part_name, (time(nullptr) - part->modification_time));
        }
    }
    else
    {
        /// If we have a covering part, ignore all the problems with this part.
        /// In the worst case, errors will still appear `old_parts_lifetime` seconds in error log until the part is removed as the old one.
        LOG_WARNING(log, "We have part {} covering part {}", part->name, part_name);
    }

    return {part_name, true, ""};
}


void ReplicatedMergeTreePartCheckThread::run()
{
    if (need_stop)
        return;

    try
    {
        time_t current_time = time(nullptr);

        /// Take part from the queue for verification.
        PartsToCheckQueue::iterator selected = parts_queue.end();    /// end from std::list is not get invalidated
        time_t min_check_time = std::numeric_limits<time_t>::max();

        {
            std::lock_guard lock(parts_mutex);

            if (parts_queue.empty())
            {
                if (!parts_set.empty())
                {
                    LOG_ERROR(log, "Non-empty parts_set with empty parts_queue. This is a bug.");
                    parts_set.clear();
                }
            }
            else
            {
                for (auto it = parts_queue.begin(); it != parts_queue.end(); ++it)
                {
                    if (it->second <= current_time)
                    {
                        selected = it;
                        break;
                    }

                    if (it->second < min_check_time)
                        min_check_time = it->second;
                }
            }
        }

        if (selected == parts_queue.end())
            return;

        checkPart(selected->first);

        if (need_stop)
            return;

        /// Remove the part from check queue.
        {
            std::lock_guard lock(parts_mutex);

            if (parts_queue.empty())
            {
                LOG_ERROR(log, "Someone erased checking part from parts_queue. This is a bug.");
            }
            else
            {
                parts_set.erase(selected->first);
                parts_queue.erase(selected);
            }
        }

        task->schedule();
    }
    catch (const Coordination::Exception & e)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

        if (e.code == Coordination::Error::ZSESSIONEXPIRED)
            return;

        task->scheduleAfter(PART_CHECK_ERROR_SLEEP_MS);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        task->scheduleAfter(PART_CHECK_ERROR_SLEEP_MS);
    }
}

}
