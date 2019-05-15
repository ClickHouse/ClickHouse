#include <Storages/MergeTree/ReplicatedMergeTreePartCheckThread.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/MergeTree/ReplicatedMergeTreePartHeader.h>
#include <Storages/StorageReplicatedMergeTree.h>


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
    , log_name(storage.database_name + "." + storage.table_name + " (ReplicatedMergeTreePartCheckThread)")
    , log(&Logger::get(log_name))
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
    //based on discussion on https://github.com/yandex/ClickHouse/pull/1489#issuecomment-344756259
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


void ReplicatedMergeTreePartCheckThread::searchForMissingPart(const String & part_name)
{
    auto zookeeper = storage.getZooKeeper();
    String part_path = storage.replica_path + "/parts/" + part_name;

    /// If the part is in ZooKeeper, remove it from there and add the task to download it to the queue.
    if (zookeeper->exists(part_path))
    {
        LOG_WARNING(log, "Part " << part_name << " exists in ZooKeeper but not locally. "
            "Removing from ZooKeeper and queueing a fetch.");
        ProfileEvents::increment(ProfileEvents::ReplicatedPartChecksFailed);

        storage.removePartAndEnqueueFetch(part_name);
        return;
    }

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

    LOG_WARNING(log, "Checking if anyone has a part covering " << part_name << ".");

    bool found_part_with_the_same_min_block = false;
    bool found_part_with_the_same_max_block = false;

    Strings replicas = zookeeper->getChildren(storage.zookeeper_path + "/replicas");
    for (const String & replica : replicas)
    {
        Strings parts = zookeeper->getChildren(storage.zookeeper_path + "/replicas/" + replica + "/parts");
        for (const String & part_on_replica : parts)
        {
            auto part_on_replica_info = MergeTreePartInfo::fromPartName(part_on_replica, storage.format_version);

            if (part_on_replica_info.contains(part_info))
            {
                LOG_WARNING(log, "Found part " << part_on_replica << " on " << replica << " that covers the missing part " << part_name);
                return;
            }

            if (part_info.contains(part_on_replica_info))
            {
                if (part_on_replica_info.min_block == part_info.min_block)
                    found_part_with_the_same_min_block = true;
                if (part_on_replica_info.max_block == part_info.max_block)
                    found_part_with_the_same_max_block = true;

                if (found_part_with_the_same_min_block && found_part_with_the_same_max_block)
                {
                    LOG_WARNING(log,
                        "Found parts with the same min block and with the same max block as the missing part "
                        << part_name << ". Hoping that it will eventually appear as a result of a merge.");
                    return;
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
    LOG_ERROR(log, "No replica has part covering " << part_name
              << " and a merge is impossible: we didn't find " << not_found_msg);

    ProfileEvents::increment(ProfileEvents::ReplicatedPartChecksFailed);

    /// Is it in the replication queue? If there is - delete, because the task can not be processed.
    if (!storage.queue.remove(zookeeper, part_name))
    {
        /// The part was not in our queue. Why did it happen?
        LOG_ERROR(log, "Missing part " << part_name << " is not in our queue.");
        return;
    }

    /** This situation is possible if on all the replicas where the part was, it deteriorated.
        * For example, a replica that has just written it has power turned off and the data has not been written from cache to disk.
        */
    LOG_ERROR(log, "Part " << part_name << " is lost forever.");
    ProfileEvents::increment(ProfileEvents::ReplicatedDataLoss);
}


void ReplicatedMergeTreePartCheckThread::checkPart(const String & part_name)
{
    LOG_WARNING(log, "Checking part " << part_name);
    ProfileEvents::increment(ProfileEvents::ReplicatedPartChecks);

    /// If the part is still in the PreCommitted -> Committed transition, it is not lost
    /// and there is no need to go searching for it on other replicas. To definitely find the needed part
    /// if it exists (or a part containing it) we first search among the PreCommitted parts.
    auto part = storage.getPartIfExists(part_name, {MergeTreeDataPartState::PreCommitted});
    if (!part)
        part = storage.getActiveContainingPart(part_name);

    /// We do not have this or a covering part.
    if (!part)
    {
        searchForMissingPart(part_name);
    }
    /// We have this part, and it's active. We will check whether we need this part and whether it has the right data.
    else if (part->name == part_name)
    {
        auto zookeeper = storage.getZooKeeper();
        auto table_lock = storage.lockStructureForShare(false, RWLockImpl::NO_QUERY);

        auto local_part_header = ReplicatedMergeTreePartHeader::fromColumnsAndChecksums(
            part->columns, part->checksums);

        String part_path = storage.replica_path + "/parts/" + part_name;
        String part_znode;
        /// If the part is in ZooKeeper, check its data with its checksums, and them with ZooKeeper.
        if (zookeeper->tryGet(part_path, part_znode))
        {
            LOG_WARNING(log, "Checking data of part " << part_name << ".");

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
                    storage.primary_key_data_types,
                    storage.skip_indices,
                    [this] { return need_stop.load(); });

                if (need_stop)
                {
                    LOG_INFO(log, "Checking part was cancelled.");
                    return;
                }

                LOG_INFO(log, "Part " << part_name << " looks good.");
            }
            catch (const Exception &)
            {
                /// TODO Better to check error code.

                tryLogCurrentException(log, __PRETTY_FUNCTION__);

                LOG_ERROR(log, "Part " << part_name << " looks broken. Removing it and queueing a fetch.");
                ProfileEvents::increment(ProfileEvents::ReplicatedPartChecksFailed);

                storage.removePartAndEnqueueFetch(part_name);

                /// Delete part locally.
                storage.forgetPartAndMoveToDetached(part, "broken_");
            }
        }
        else if (part->modification_time + MAX_AGE_OF_LOCAL_PART_THAT_WASNT_ADDED_TO_ZOOKEEPER < time(nullptr))
        {
            /// If the part is not in ZooKeeper, delete it locally.
            /// Probably, someone just wrote down the part, and has not yet added to ZK.
            /// Therefore, delete only if the part is old (not very reliable).
            ProfileEvents::increment(ProfileEvents::ReplicatedPartChecksFailed);

            LOG_ERROR(log, "Unexpected part " << part_name << " in filesystem. Removing.");
            storage.forgetPartAndMoveToDetached(part, "unexpected_");
        }
        else
        {
            /// TODO You need to make sure that the part is still checked after a while.
            /// Otherwise, it's possible that the part was not added to ZK,
            ///  but remained in the filesystem and in a number of active parts.
            /// And then for a long time (before restarting), the data on the replicas will be different.

            LOG_TRACE(log, "Young part " << part_name
                << " with age " << (time(nullptr) - part->modification_time)
                << " seconds hasn't been added to ZooKeeper yet. It's ok.");
        }
    }
    else
    {
        /// If we have a covering part, ignore all the problems with this part.
        /// In the worst case, errors will still appear `old_parts_lifetime` seconds in error log until the part is removed as the old one.
        LOG_WARNING(log, "We have part " << part->name << " covering part " << part_name);
    }
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
                LOG_ERROR(log, "Someone erased cheking part from parts_queue. This is a bug.");
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

        if (e.code == Coordination::ZSESSIONEXPIRED)
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
