#include <Storages/MergeTree/ReplicatedMergeTreePartCheckThread.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/ReplicatedMergeTreePartHeader.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/ThreadFuzzer.h>
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
    extern const int LOGICAL_ERROR;
}

static const auto PART_CHECK_ERROR_SLEEP_MS = 5 * 1000;


ReplicatedMergeTreePartCheckThread::ReplicatedMergeTreePartCheckThread(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , log_name(storage.getStorageID().getFullTableName() + " (ReplicatedMergeTreePartCheckThread)")
    , log(getLogger(log_name))
{
    task = storage.getContext()->getSchedulePool().createTask(log_name, [this] { run(); });
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

    if (parts_set.contains(name))
        return;

    LOG_TRACE(log, "Enqueueing {} for check after {}s", name, delay_to_check_seconds);
    parts_queue.emplace_back(name, std::chrono::steady_clock::now() + std::chrono::seconds(delay_to_check_seconds));
    parts_set.insert(name);
    task->schedule();
}

std::unique_lock<std::mutex> ReplicatedMergeTreePartCheckThread::pausePartsCheck()
{
    /// Wait for running tasks to finish and temporarily stop checking
    return task->getExecLock();
}

void ReplicatedMergeTreePartCheckThread::cancelRemovedPartsCheck(const MergeTreePartInfo & drop_range_info)
{
    Strings parts_to_remove;
    {
        std::lock_guard lock(parts_mutex);
        for (const auto & elem : parts_queue)
            if (drop_range_info.contains(MergeTreePartInfo::fromPartName(elem.name, storage.format_version)))
                parts_to_remove.push_back(elem.name);
    }

    /// We have to remove parts that were not removed by removePartAndEnqueueFetch
    LOG_INFO(log, "Removing broken parts from ZooKeeper: {}", fmt::join(parts_to_remove, ", "));
    storage.removePartsFromZooKeeperWithRetries(parts_to_remove);   /// May throw

    /// Now we can remove parts from the check queue.
    /// It's not atomic (because it's bad idea to hold the mutex while removing something from zk with retries),
    /// but the check thread is currently paused, and no new parts in drop_range_info can by enqueued
    /// while the corresponding DROP_RANGE/REPLACE_RANGE exists, so it should be okay. We will recheck it just in case.

    StringSet removed_parts;
    for (auto & part : parts_to_remove)
        removed_parts.emplace(std::move(part));
    size_t count = 0;

    std::lock_guard lock(parts_mutex);
    for (const auto & elem : parts_queue)
    {
        bool is_removed = removed_parts.contains(elem.name);
        bool should_have_been_removed = drop_range_info.contains(MergeTreePartInfo::fromPartName(elem.name, storage.format_version));
        if (is_removed != should_have_been_removed)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Inconsistent parts_queue: name={}, is_removed={}, should_have_been_removed={}",
                            elem.name, is_removed, should_have_been_removed);
        count += is_removed;
    }

    if (count != parts_to_remove.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected number of parts to remove from parts_queue: should be {}, got {}",
                        parts_to_remove.size(), count);

    auto new_end = std::remove_if(parts_queue.begin(), parts_queue.end(), [&removed_parts] (const auto & elem)
    {
        return removed_parts.contains(elem.name);
    });

    parts_queue.erase(new_end, parts_queue.end());

    for (const auto & elem : removed_parts)
        parts_set.erase(elem);
}

size_t ReplicatedMergeTreePartCheckThread::size() const
{
    std::lock_guard lock(parts_mutex);
    return parts_set.size();
}


bool ReplicatedMergeTreePartCheckThread::searchForMissingPartOnOtherReplicas(const String & part_name) const
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

    LOG_INFO(log, "Checking if anyone has a part {} or covering part.", part_name);

    bool found_part_with_the_same_min_block = false;
    bool found_part_with_the_same_max_block = false;

    Strings replicas = zookeeper->getChildren(storage.zookeeper_path + "/replicas");
    /// Move our replica to the end of replicas
    for (auto it = replicas.begin(); it != replicas.end(); ++it)
    {
        String replica_path = storage.zookeeper_path + "/replicas/" + *it;
        if (replica_path == storage.replica_path)
        {
            std::iter_swap(it, replicas.rbegin());
            break;
        }
    }

    /// Check all replicas and our replica must be this last one
    for (const String & replica : replicas)
    {
        String replica_path = storage.zookeeper_path + "/replicas/" + replica;

        Strings parts = zookeeper->getChildren(replica_path + "/parts");
        Strings parts_found;
        for (const String & part_on_replica : parts)
        {
            auto part_on_replica_info = MergeTreePartInfo::fromPartName(part_on_replica, storage.format_version);

            /// All three following cases are "good" outcome for check thread and don't require
            /// any special attention.
            if (part_info == part_on_replica_info)
            {
                /// Found missing part at ourself. If we are here then something wrong with this part, so skipping.
                if (replica_path == storage.replica_path)
                    continue;

                LOG_INFO(log, "Found the missing part {} at {} on {}", part_name, part_on_replica, replica);
                return true;
            }

            if (part_on_replica_info.contains(part_info))
            {
                LOG_INFO(log, "Found part {} on {} that covers the missing part {}", part_on_replica, replica, part_name);
                return true;
            }

            if (part_info.contains(part_on_replica_info))
            {
                if (part_on_replica_info.min_block == part_info.min_block)
                {
                    found_part_with_the_same_min_block = true;
                    parts_found.push_back(part_on_replica);
                }

                if (part_on_replica_info.max_block == part_info.max_block)
                {
                    found_part_with_the_same_max_block = true;

                    /// If we are looking for part like partition_X_X_level we can add part
                    /// partition_X_X_(level-1) two times, avoiding it
                    if (parts_found.empty() || parts_found.back() != part_on_replica)
                        parts_found.push_back(part_on_replica);
                }

                if (found_part_with_the_same_min_block && found_part_with_the_same_max_block)
                {
                    LOG_INFO(log, "Found parts with the same min block and with the same max block as the missing part {} on replica {}. "
                             "Hoping that it will eventually appear as a result of a merge. Parts: {}",
                             part_name, replica, fmt::join(parts_found, ", "));
                    return true;
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

    return false;
}


std::pair<bool, MergeTreeDataPartPtr> ReplicatedMergeTreePartCheckThread::findLocalPart(const String & part_name)
{
    auto zookeeper = storage.getZooKeeper();
    String part_path = storage.replica_path + "/parts/" + part_name;

    /// It's important to check zookeeper first and after that check local storage,
    /// because our checks of local storage and zookeeper are not consistent.
    /// If part exists in zookeeper and doesn't exists in local storage definitely require
    /// to fetch this part. But if we check local storage first and than check zookeeper
    /// some background process can successfully commit part between this checks (both to the local storage and zookeeper),
    /// but checker thread will remove part from zookeeper and queue fetch.
    bool exists_in_zookeeper = zookeeper->exists(part_path);

    /// If the part is still in the PreActive -> Active transition, it is not lost
    /// and there is no need to go searching for it on other replicas. To definitely find the needed part
    /// if it exists (or a part containing it) we first search among the PreActive parts.
    auto part = storage.getPartIfExists(part_name, {MergeTreeDataPartState::PreActive});
    if (!part)
        part = storage.getActiveContainingPart(part_name);

    return std::make_pair(exists_in_zookeeper, part);
}

ReplicatedCheckResult ReplicatedMergeTreePartCheckThread::checkPartImpl(const String & part_name, bool throw_on_broken_projection)
{
    ReplicatedCheckResult result;
    auto [exists_in_zookeeper, part] = findLocalPart(part_name);
    result.exists_in_zookeeper = exists_in_zookeeper;
    result.part = part;

    LOG_TRACE(log, "Part {} in zookeeper: {}, locally: {}", part_name, exists_in_zookeeper, part != nullptr);

    if (exists_in_zookeeper && !part)
    {
        auto outdated = storage.getPartIfExists(part_name, {MergeTreeDataPartState::Outdated, MergeTreeDataPartState::Deleting});
        if (outdated)
        {
            /// We cannot rely on exists_in_zookeeper, because the cleanup thread is probably going to remove it from ZooKeeper
            /// Also, it will avoid "Cannot commit empty part: Part ... (state Outdated) already exists, but it will be deleted soon"
            time_t lifetime = time(nullptr) - outdated->remove_time;
            time_t max_lifetime = storage.getSettings()->old_parts_lifetime.totalSeconds();
            time_t delay = lifetime >= max_lifetime ? 0 : max_lifetime - lifetime;
            result.recheck_after_seconds = delay + 30;

            auto message = PreformattedMessage::create("Part {} is Outdated, will wait for cleanup thread to handle it "
                                                       "and check again after {}s", part_name, result.recheck_after_seconds);
            LOG_WARNING(log, message);
            result.status = {part_name, true, message.text};
            result.action = ReplicatedCheckResult::RecheckLater;
            return result;
        }
    }

    /// We do not have this or a covering part.
    if (!part)
    {
        result.status = {part_name, false, "Part is missing, will search for it"};
        result.action = ReplicatedCheckResult::TryFetchMissing;
        return result;
    }

    /// We have this part, and it's active. We will check whether we need this part and whether it has the right data.
    if (part->name != part_name)
    {
        /// If we have a covering part, ignore all the problems with this part.
        /// In the worst case, errors will still appear `old_parts_lifetime` seconds in error log until the part is removed as the old one.
        auto message = PreformattedMessage::create("We have part {} covering part {}, will not check", part->name, part_name);
        LOG_WARNING(log, message);
        result.status = {part_name, true, message.text};
        result.action = ReplicatedCheckResult::DoNothing;
        return result;
    }

    time_t current_time = time(nullptr);
    auto zookeeper = storage.getZooKeeper();
    auto table_lock = storage.lockForShare(RWLockImpl::NO_QUERY, storage.getSettings()->lock_acquire_timeout_for_background_operations);

    auto local_part_header = ReplicatedMergeTreePartHeader::fromColumnsAndChecksums(
        part->getColumns(), part->checksums);


    /// If the part is in ZooKeeper, check its data with its checksums, and them with ZooKeeper.
    if (exists_in_zookeeper)
    {
        LOG_INFO(log, "Checking data of part {}.", part_name);

        /// The double get scheme is needed to retain compatibility with very old parts that were created
        /// before the ReplicatedMergeTreePartHeader was introduced.
        String part_path = storage.replica_path + "/parts/" + part_name;
        String part_znode = zookeeper->get(part_path);
        bool is_broken_projection = false;

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
                throw Exception(ErrorCodes::TABLE_DIFFERS_TOO_MUCH, "Columns of local part {} are different from ZooKeeper", part_name);

            zk_part_header.getChecksums().checkEqual(local_part_header.getChecksums(), true, part_name);

            checkDataPart(
                part,
                /* require_checksums */true,
                is_broken_projection,
                [this] { return need_stop.load(); },
                throw_on_broken_projection);

            if (need_stop)
            {
                result.status = {part_name, false, "Checking part was cancelled"};
                result.action = ReplicatedCheckResult::Cancelled;
                return result;
            }

            LOG_INFO(log, "Part {} looks good.", part_name);
            result.status = {part_name, true, ""};
            result.action = ReplicatedCheckResult::DoNothing;
            return result;
        }
        catch (...)
        {
            if (isRetryableException(std::current_exception()))
                throw;

            PreformattedMessage message;
            if (is_broken_projection && throw_on_broken_projection)
            {
                WriteBufferFromOwnString wb;
                message = PreformattedMessage::create(
                    "Part `{}` has broken projections. It will be ignored. Broken projections info: {}",
                    part_name, getCurrentExceptionMessage(true));
                LOG_DEBUG(log, message);
                result.action = ReplicatedCheckResult::DoNothing;
            }
            else
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);

                message = PreformattedMessage::create("Part {} looks broken. Removing it and will try to fetch.", part_name);
                LOG_ERROR(log, message);
                result.action = ReplicatedCheckResult::TryFetchMissing;
            }

            /// Part is broken, let's try to find it and fetch.
            result.status = {part_name, false, message};
            return result;

        }
    }
    else if (part->modification_time + MAX_AGE_OF_LOCAL_PART_THAT_WASNT_ADDED_TO_ZOOKEEPER < current_time)
    {
        /// If the part is not in ZooKeeper, delete it locally.
        /// Probably, someone just wrote down the part, and has not yet added to ZK.
        /// Therefore, delete only if the part is old (not very reliable).
        constexpr auto fmt_string = "Unexpected part {} in filesystem. Removing.";
        String message = fmt::format(fmt_string, part_name);
        LOG_ERROR(log, fmt_string, part_name);
        result.status = {part_name, false, message};
        result.action = ReplicatedCheckResult::DetachUnexpected;
        return result;
    }
    else
    {
        auto message = PreformattedMessage::create("Young part {} with age {} seconds hasn't been added to ZooKeeper yet. It's ok.",
                                                   part_name, (current_time - part->modification_time));
        LOG_INFO(log, message);
        result.recheck_after_seconds = part->modification_time + MAX_AGE_OF_LOCAL_PART_THAT_WASNT_ADDED_TO_ZOOKEEPER - current_time;
        result.status = {part_name, true, message};
        result.action = ReplicatedCheckResult::RecheckLater;
        return result;
    }
}


CheckResult ReplicatedMergeTreePartCheckThread::checkPartAndFix(const String & part_name, std::optional<time_t> * recheck_after, bool throw_on_broken_projection)
{
    LOG_INFO(log, "Checking part {}", part_name);
    ProfileEvents::increment(ProfileEvents::ReplicatedPartChecks);

    ReplicatedCheckResult result = checkPartImpl(part_name, throw_on_broken_projection);
    switch (result.action)
    {
        case ReplicatedCheckResult::None: UNREACHABLE();
        case ReplicatedCheckResult::DoNothing: break;
        case ReplicatedCheckResult::Cancelled:
            LOG_INFO(log, "Checking part was cancelled.");
            break;

        case ReplicatedCheckResult::RecheckLater:
            /// NOTE We cannot enqueue it from the check thread itself
            if (recheck_after)
                *recheck_after = result.recheck_after_seconds;
            else
                enqueuePart(part_name, result.recheck_after_seconds);
            break;

        case ReplicatedCheckResult::DetachUnexpected:
            chassert(!result.exists_in_zookeeper);
            ProfileEvents::increment(ProfileEvents::ReplicatedPartChecksFailed);

            storage.outdateUnexpectedPartAndCloneToDetached(result.part);
            break;

        case ReplicatedCheckResult::TryFetchMissing:
        {
            ProfileEvents::increment(ProfileEvents::ReplicatedPartChecksFailed);

            /// If the part is in ZooKeeper, remove it from there and add the task to download it to the queue (atomically).
            if (result.exists_in_zookeeper)
            {
                /// We cannot simply remove part from ZooKeeper, because it may be removed from virtual_part,
                /// so we have to create some entry in the queue. Maybe we will execute it (by fetching part or covering part from somewhere),
                /// maybe will simply replace with empty part.
                if (result.part)
                    LOG_WARNING(log, "Part {} exists in ZooKeeper and the local part was broken. Detaching it, removing from ZooKeeper and queueing a fetch.", part_name);
                else
                    LOG_WARNING(log, "Part {} exists in ZooKeeper but not locally. Removing from ZooKeeper and queueing a fetch.", part_name);

                storage.removePartAndEnqueueFetch(part_name, /* storage_init = */ false);
                break;
            }

            chassert(!result.part);

            /// Part is not in ZooKeeper and not on disk (so there's nothing to detach or remove from ZooKeeper).
            /// Probably we cannot execute some entry from the replication queue (so don't need to enqueue another one).
            /// Either all replicas having the part are not active...
            bool found_something = searchForMissingPartOnOtherReplicas(part_name);
            if (found_something)
                break;

            /// ... or the part is lost forever
            bool handled_lost_part = onPartIsLostForever(part_name);
            if (handled_lost_part)
                break;

            /// We failed to create empty part, need retry
            constexpr time_t retry_after_seconds = 30;
            if (recheck_after)
                *recheck_after = retry_after_seconds;
            else
                enqueuePart(part_name, retry_after_seconds);

            break;
        }
    }

    return result.status;
}

bool ReplicatedMergeTreePartCheckThread::onPartIsLostForever(const String & part_name)
{
    auto lost_part_info = MergeTreePartInfo::fromPartName(part_name, storage.format_version);
    if (lost_part_info.level != 0 || lost_part_info.mutation != 0)
    {
        Strings source_parts;
        bool part_in_queue = storage.queue.checkPartInQueueAndGetSourceParts(part_name, source_parts);

        /// If it's MERGE/MUTATION etc. we shouldn't replace result part with empty part
        /// because some source parts can be lost, but some of them can exist.
        if (part_in_queue && !source_parts.empty())
        {
            LOG_ERROR(log, "Part {} found in queue and some source parts for it was lost. Will check all source parts.", part_name);
            for (const String & source_part_name : source_parts)
                enqueuePart(source_part_name);

            return true;
        }
    }

    ThreadFuzzer::maybeInjectSleep();

    if (storage.createEmptyPartInsteadOfLost(storage.getZooKeeper(), part_name))
    {
        /** This situation is possible if on all the replicas where the part was, it deteriorated.
            * For example, a replica that has just written it has power turned off and the data has not been written from cache to disk.
            */
        LOG_ERROR(log, "Part {} is lost forever.", part_name);
        ProfileEvents::increment(ProfileEvents::ReplicatedDataLoss);
        return true;
    }

    LOG_WARNING(log, "Cannot create empty part {} instead of lost. Will retry later", part_name);
    return false;
}


void ReplicatedMergeTreePartCheckThread::run()
{
    if (need_stop)
        return;

    try
    {
        const auto current_time = std::chrono::steady_clock::now();

        /// Take part from the queue for verification.
        PartsToCheckQueue::iterator selected = parts_queue.end();    /// end from std::list is not get invalidated

        {
            std::lock_guard lock(parts_mutex);

            if (parts_queue.empty() && !parts_set.empty())
            {
                parts_set.clear();
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Non-empty parts_set with empty parts_queue. This is a bug.");
            }

            selected = std::find_if(parts_queue.begin(), parts_queue.end(), [current_time](const auto & elem)
            {
                return elem.time <= current_time;
            });
            if (selected == parts_queue.end())
            {
                // Find next part to check in the queue and schedule the check
                // Otherwise, scheduled for later checks won't be executed until
                // a new check is enqueued (i.e. task is scheduled again)
                auto next_it = std::min_element(
                    begin(parts_queue), end(parts_queue), [](const auto & l, const auto & r) { return l.time < r.time; });
                if (next_it != parts_queue.end())
                {
                    auto delay = next_it->time - current_time;
                    task->scheduleAfter(duration_cast<std::chrono::milliseconds>(delay).count());
                }
                return;
            }

            /// Move selected part to the end of the queue
            parts_queue.splice(parts_queue.end(), parts_queue, selected);
        }

        std::optional<time_t> recheck_after;
        checkPartAndFix(selected->name, &recheck_after, /* throw_on_broken_projection */false);

        if (need_stop)
            return;

        /// Remove the part from check queue.
        {
            std::lock_guard lock(parts_mutex);

            if (parts_queue.empty())
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Someone erased checking part from parts_queue. This is a bug.");
            }
            else if (recheck_after.has_value())
            {
                LOG_TRACE(log, "Will recheck part {} after after {}s", selected->name, *recheck_after);
                selected->time = std::chrono::steady_clock::now() + std::chrono::seconds(*recheck_after);
            }
            else
            {
                parts_set.erase(selected->name);
                parts_queue.erase(selected);
            }
        }

        storage.checkBrokenDisks();

        task->schedule();
    }
    catch (const Coordination::Exception & e)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

        if (Coordination::isHardwareError(e.code))
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
