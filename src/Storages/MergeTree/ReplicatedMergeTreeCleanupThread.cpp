#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/ReplicatedMergeTreeCleanupThread.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/ZooKeeper/KeeperException.h>

#include <random>
#include <unordered_set>

#include <base/sort.h>
#include <Poco/Timestamp.h>


namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 cleanup_delay_period;
    extern const MergeTreeSettingsUInt64 cleanup_delay_period_random_add;
    extern const MergeTreeSettingsUInt64 cleanup_thread_preferred_points_per_iteration;
    extern const MergeTreeSettingsUInt64 finished_mutations_to_keep;
    extern const MergeTreeSettingsSeconds lock_acquire_timeout_for_background_operations;
    extern const MergeTreeSettingsUInt64 max_cleanup_delay_period;
    extern const MergeTreeSettingsUInt64 max_replicated_logs_to_keep;
    extern const MergeTreeSettingsUInt64 min_replicated_logs_to_keep;
    extern const MergeTreeSettingsUInt64 replicated_deduplication_window;
    extern const MergeTreeSettingsUInt64 replicated_deduplication_window_for_async_inserts;
    extern const MergeTreeSettingsUInt64 replicated_deduplication_window_seconds;
    extern const MergeTreeSettingsUInt64 replicated_deduplication_window_seconds_for_async_inserts;
    extern const MergeTreeSettingsSeconds temporary_directories_lifetime;
}

namespace ErrorCodes
{
    extern const int NOT_FOUND_NODE;
    extern const int ALL_REPLICAS_LOST;
    extern const int REPLICA_STATUS_CHANGED;
}


ReplicatedMergeTreeCleanupThread::ReplicatedMergeTreeCleanupThread(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , log_name(storage.getStorageID().getFullTableName() + " (ReplicatedMergeTreeCleanupThread)")
    , log(getLogger(log_name))
    , sleep_ms((*storage.getSettings())[MergeTreeSetting::cleanup_delay_period] * 1000)
{
    task = storage.getContext()->getSchedulePool().createTask(log_name, [this]{ run(); });
}

void ReplicatedMergeTreeCleanupThread::run()
{
    if (cleanup_blocker.isCancelled())
    {
        LOG_TRACE(LogFrequencyLimiter(log, 30), "Cleanup is cancelled, exiting");
        return;
    }

    SCOPE_EXIT({ is_running.store(false, std::memory_order_relaxed); });
    is_running.store(true, std::memory_order_relaxed);

    auto storage_settings = storage.getSettings();

    Float32 cleanup_points = 0;
    try
    {
        cleanup_points = iterate();
    }
    catch (const Coordination::Exception & e)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

        if (e.code == Coordination::Error::ZSESSIONEXPIRED)
            return;
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    UInt64 prev_timestamp = prev_cleanup_timestamp_ms.load(std::memory_order_relaxed);
    UInt64 now_ms = clock_gettime_ns_adjusted(prev_timestamp * 1'000'000) / 1'000'000;

    /// Do not adjust sleep_ms on the first run after starting the server
    if (prev_timestamp && (*storage_settings)[MergeTreeSetting::cleanup_thread_preferred_points_per_iteration])
    {
        /// We don't want to run the task too often when the table was barely changed and there's almost nothing to cleanup.
        /// But we cannot simply sleep max_cleanup_delay_period (300s) when nothing was cleaned up and cleanup_delay_period (30s)
        /// when we removed something, because inserting one part per 30s will lead to running cleanup each 30s just to remove one part.
        /// So we need some interpolation based on preferred batch size.
        auto expected_cleanup_points = (*storage_settings)[MergeTreeSetting::cleanup_thread_preferred_points_per_iteration];

        /// How long should we sleep to remove cleanup_thread_preferred_points_per_iteration on the next iteration?
        Float32 ratio = cleanup_points / expected_cleanup_points;
        if (ratio == 0)
            sleep_ms = (*storage_settings)[MergeTreeSetting::max_cleanup_delay_period] * 1000;
        else
            sleep_ms = static_cast<UInt64>(sleep_ms / ratio);

        sleep_ms = std::clamp(sleep_ms, (*storage_settings)[MergeTreeSetting::cleanup_delay_period] * 1000, (*storage_settings)[MergeTreeSetting::max_cleanup_delay_period] * 1000);

        UInt64 interval_ms = now_ms - prev_timestamp;
        LOG_TRACE(log, "Scheduling next cleanup after {}ms (points: {}, interval: {}ms, ratio: {}, points per minute: {})",
                  sleep_ms, cleanup_points, interval_ms, ratio, cleanup_points / interval_ms * 60'000);
    }
    prev_cleanup_timestamp_ms.store(now_ms, std::memory_order_relaxed);

    sleep_ms += std::uniform_int_distribution<UInt64>(0, (*storage_settings)[MergeTreeSetting::cleanup_delay_period_random_add] * 1000)(rng);
    task->scheduleAfter(sleep_ms);
}

void ReplicatedMergeTreeCleanupThread::wakeupEarlierIfNeeded()
{
    /// It may happen that the tables was idle for a long time, but then a user started to aggressively insert (or mutate) data.
    /// In this case, sleep_ms was set to the highest possible value, the task is not going to wake up soon,
    /// but the number of objects to clean up is growing. We need to wakeup the task earlier.
    auto storage_settings = storage.getSettings();
    if (!(*storage_settings)[MergeTreeSetting::cleanup_thread_preferred_points_per_iteration])
        return;

    /// The number of other objects (logs, blocks, etc) is usually correlated with the number of Outdated parts.
    /// Do not wake up unless we have too many.
    size_t number_of_outdated_objects = storage.getOutdatedPartsCount();
    if (number_of_outdated_objects < (*storage_settings)[MergeTreeSetting::cleanup_thread_preferred_points_per_iteration] * 2)
        return;

    /// A race condition is possible here, but it's okay
    if (is_running.load(std::memory_order_relaxed))
        return;

    /// Do not re-check all parts too often (avoid constantly calling getNumberOfOutdatedPartsWithExpiredRemovalTime())
    if (!wakeup_check_timer.compareAndRestart((*storage_settings)[MergeTreeSetting::cleanup_delay_period] / 4.0))
        return;

    UInt64 prev_run_timestamp_ms = prev_cleanup_timestamp_ms.load(std::memory_order_relaxed);
    UInt64 now_ms = clock_gettime_ns_adjusted(prev_run_timestamp_ms * 1'000'000) / 1'000'000;
    if (!prev_run_timestamp_ms || now_ms <= prev_run_timestamp_ms)
        return;

    /// Don't run it more often than cleanup_delay_period
    UInt64 seconds_passed = (now_ms - prev_run_timestamp_ms) / 1000;
    if (seconds_passed < (*storage_settings)[MergeTreeSetting::cleanup_delay_period])
        return;

    /// Do not count parts that cannot be removed anyway. Do not wake up unless we have too many.
    number_of_outdated_objects = storage.getNumberOfOutdatedPartsWithExpiredRemovalTime();
    if (number_of_outdated_objects < (*storage_settings)[MergeTreeSetting::cleanup_thread_preferred_points_per_iteration] * 2)
        return;

    LOG_TRACE(log, "Waking up cleanup thread because there are {} outdated objects and previous cleanup finished {}s ago",
              number_of_outdated_objects, seconds_passed);

    wakeup();
}


Float32 ReplicatedMergeTreeCleanupThread::iterate()
{
    size_t cleaned_logs = 0;
    Float32 cleaned_blocks = 0;
    size_t cleaned_other = 0;
    size_t cleaned_part_like = 0;
    size_t cleaned_parts = storage.clearOldPartsAndRemoveFromZK();

    auto storage_settings = storage.getSettings();

    {
        auto lock = storage.lockForShare(RWLockImpl::NO_QUERY, (*storage.getSettings())[MergeTreeSetting::lock_acquire_timeout_for_background_operations]);
        /// Both use relative_data_path which changes during rename, so we
        /// do it under share lock
        cleaned_part_like += storage.clearOldTemporaryDirectories((*storage.getSettings())[MergeTreeSetting::temporary_directories_lifetime].totalSeconds());
    }

    /// This is loose condition: no problem if we actually had lost leadership at this moment
    ///  and two replicas will try to do cleanup simultaneously.
    if (storage.is_leader)
    {
        cleaned_logs = clearOldLogs();
        size_t normal_blocks = clearOldBlocks("blocks", (*storage_settings)[MergeTreeSetting::replicated_deduplication_window_seconds],
                                   (*storage_settings)[MergeTreeSetting::replicated_deduplication_window], cached_block_stats_for_sync_inserts);

        size_t async_blocks = clearOldBlocks("async_blocks",
                                   (*storage_settings)[MergeTreeSetting::replicated_deduplication_window_seconds_for_async_inserts],
                                   (*storage_settings)[MergeTreeSetting::replicated_deduplication_window_for_async_inserts],
                                   cached_block_stats_for_async_inserts);

        /// Many async blocks are transformed into one ordinary block
        Float32 async_blocks_per_block = static_cast<Float32>((*storage_settings)[MergeTreeSetting::replicated_deduplication_window]) /
            ((*storage_settings)[MergeTreeSetting::replicated_deduplication_window_for_async_inserts] + 1);
        cleaned_blocks = (normal_blocks + async_blocks * async_blocks_per_block) / 2;

        cleaned_other += clearOldMutations();
        cleaned_part_like += storage.clearEmptyParts();
    }

    cleaned_part_like += storage.unloadPrimaryKeysOfOutdatedParts();

    /// We need to measure the number of removed objects somehow (for better scheduling),
    /// but just summing the number of removed async blocks, logs, and empty parts does not make any sense.
    /// So we are trying to (approximately) measure the number of inserted blocks/parts, so we will be able to compare apples to apples.

    /// Each inserted block produces 3 objects that have to be cleaned up: one block, one log entry and one part.
    /// A few new parts get merged together producing one log entry and one part.

    /// Other objects (like mutations and WALs) are much more rare than Outdated parts (because mutations usually produce
    /// many Outdated parts, and WALs usually contain many parts too). We count then as one part for simplicity.

    constexpr Float32 parts_number_amplification = 1.3f;     /// Assuming we merge 4-5 parts each time
    Float32 cleaned_inserted_parts = (cleaned_blocks + (cleaned_logs + cleaned_parts) / parts_number_amplification) / 3;
    return cleaned_inserted_parts + cleaned_part_like + cleaned_other;
}


size_t ReplicatedMergeTreeCleanupThread::clearOldLogs()
{
    auto zookeeper = storage.getZooKeeper();
    auto storage_settings = storage.getSettings();

    Coordination::Stat stat;
    if (!zookeeper->exists(storage.zookeeper_path + "/log", &stat))
        throw Exception(ErrorCodes::NOT_FOUND_NODE, "{}/log doesn't exist", storage.zookeeper_path);

    int children_count = stat.numChildren;

    /// We will wait for 1.05 to 1.15 times more records to accumulate than necessary.
    /// Randomization is needed to spread the time when multiple replicas come here.
    /// Numbers are arbitrary.
    std::uniform_real_distribution<double> distr(1.05, 1.15);
    double ratio = distr(rng);
    size_t min_replicated_logs_to_keep = static_cast<size_t>((*storage_settings)[MergeTreeSetting::min_replicated_logs_to_keep] * ratio);

    if (static_cast<double>(children_count) < min_replicated_logs_to_keep)
        return 0;

    Strings replicas = zookeeper->getChildren(storage.zookeeper_path + "/replicas", &stat);

    /// We will keep logs after and including this threshold.
    UInt64 min_saved_log_pointer = std::numeric_limits<UInt64>::max();

    UInt64 min_log_pointer_lost_candidate = std::numeric_limits<UInt64>::max();

    Strings entries = zookeeper->getChildren(storage.zookeeper_path + "/log");

    if (entries.empty())
        return 0;

    ::sort(entries.begin(), entries.end());

    String min_saved_record_log_str = entries[
        entries.size() > (*storage_settings)[MergeTreeSetting::max_replicated_logs_to_keep]
            ? entries.size() - (*storage_settings)[MergeTreeSetting::max_replicated_logs_to_keep]
            : 0];

    /// Replicas that were marked is_lost but are active.
    std::unordered_set<String> recovering_replicas;

    /// Lost replica -> a version of 'host' node.
    std::unordered_map<String, UInt32> host_versions_lost_replicas;

    /// Replica -> log pointer.
    std::unordered_map<String, String> log_pointers_candidate_lost_replicas;

    size_t num_replicas_were_marked_is_lost = 0;

    for (const String & replica : replicas)
    {
        Coordination::Stat host_stat;
        zookeeper->get(storage.zookeeper_path + "/replicas/" + replica + "/host", &host_stat);
        String pointer = zookeeper->get(storage.zookeeper_path + "/replicas/" + replica + "/log_pointer");

        UInt64 log_pointer = 0;

        if (!pointer.empty())
            log_pointer = parse<UInt64>(pointer);

        /// Check status of replica (active or not).
        /// If replica was not active, we could check when its log_pointer locates.

        /// There can be three possibilities for "is_lost" node:
        /// It doesn't exist: in old version of ClickHouse.
        /// It exists and value is 0.
        /// It exists and value is 1.
        String is_lost_str;

        bool has_is_lost_node = zookeeper->tryGet(storage.zookeeper_path + "/replicas/" + replica + "/is_lost", is_lost_str);

        if (zookeeper->exists(storage.zookeeper_path + "/replicas/" + replica + "/is_active"))
        {
            if (has_is_lost_node && is_lost_str == "1")
            {
                /// Lost and active: recovering.
                recovering_replicas.insert(replica);
                ++num_replicas_were_marked_is_lost;
            }
            else
            {
                /// Not lost and active: usual case.
                min_saved_log_pointer = std::min(min_saved_log_pointer, log_pointer);
            }
        }
        else
        {
            if (!has_is_lost_node)
            {
                /// Only to support old versions CH.
                /// If replica did not have "/is_lost" we must save it's log_pointer.
                /// Because old version CH can not work with recovering.
                min_saved_log_pointer = std::min(min_saved_log_pointer, log_pointer);
            }
            else
            {
                if (is_lost_str == "0")
                {
                    /// Not active and not lost: a candidate to be marked as lost.
                    String log_pointer_str = "log-" + padIndex(log_pointer);
                    if (log_pointer_str >= min_saved_record_log_str)
                    {
                        /// Its log pointer is fresh enough.
                        min_saved_log_pointer = std::min(min_saved_log_pointer, log_pointer);
                    }
                    else
                    {
                        /// Its log pointer is stale: will mark replica as lost.
                        host_versions_lost_replicas[replica] = host_stat.version;
                        log_pointers_candidate_lost_replicas[replica] = log_pointer_str;
                        min_log_pointer_lost_candidate = std::min(min_log_pointer_lost_candidate, log_pointer);
                    }
                }
                else
                {
                    ++num_replicas_were_marked_is_lost;
                    host_versions_lost_replicas[replica] = host_stat.version;
                }
            }
        }
    }

    /// We must check log_pointer of recovering replicas at the end.
    /// Because log pointer of recovering replicas can move backward.
    for (const String & replica : recovering_replicas)
    {
        String pointer = zookeeper->get(storage.zookeeper_path + "/replicas/" + replica + "/log_pointer");
        UInt64 log_pointer = 0;
        if (!pointer.empty())
            log_pointer = parse<UInt64>(pointer);
        min_saved_log_pointer = std::min(min_saved_log_pointer, log_pointer);
    }

    if (!recovering_replicas.empty())
        min_saved_log_pointer = std::min(min_saved_log_pointer, min_log_pointer_lost_candidate);

    /// We will not touch the last `min_replicated_logs_to_keep` records.
    entries.erase(entries.end() - std::min<UInt64>(entries.size(), (*storage_settings)[MergeTreeSetting::min_replicated_logs_to_keep]), entries.end());
    /// We will not touch records that are no less than `min_saved_log_pointer`.
    entries.erase(std::lower_bound(entries.begin(), entries.end(), "log-" + padIndex(min_saved_log_pointer)), entries.end());

    if (entries.empty())
        return 0;

    markLostReplicas(
        host_versions_lost_replicas,
        log_pointers_candidate_lost_replicas,
        replicas.size() - num_replicas_were_marked_is_lost,
        zookeeper);

    Coordination::Requests ops;
    size_t i = 0;
    for (; i < entries.size(); ++i)
    {
        ops.emplace_back(zkutil::makeRemoveRequest(storage.zookeeper_path + "/log/" + entries[i], -1));

        if (ops.size() > 4 * zkutil::MULTI_BATCH_SIZE || i + 1 == entries.size())
        {
            /// We need to check this because the replica that was restored from one of the marked replicas does not copy a non-valid log_pointer.
            for (const auto & host_version : host_versions_lost_replicas)
                ops.emplace_back(zkutil::makeCheckRequest(storage.zookeeper_path + "/replicas/" + host_version.first + "/host", host_version.second));

            /// Simultaneously with clearing the log, we check to see if replica was added since we received replicas list.
            ops.emplace_back(zkutil::makeCheckRequest(storage.zookeeper_path + "/replicas", stat.version));

            Coordination::Responses responses;
            Coordination::Error e = zookeeper->tryMulti(ops, responses);

            if (e == Coordination::Error::ZNONODE)
            {
                /// Another replica already deleted the same node concurrently.
                break;
            }

            zkutil::KeeperMultiException::check(e, ops, responses);

            ops.clear();
        }
    }

    if (i != 0)
        LOG_DEBUG(log, "Removed {} old log entries: {} - {}", i, entries[0], entries[i - 1]);

    return i;
}


void ReplicatedMergeTreeCleanupThread::markLostReplicas(const std::unordered_map<String, UInt32> & host_versions_lost_replicas,
                                                        const std::unordered_map<String, String> & log_pointers_candidate_lost_replicas,
                                                        size_t replicas_count, const zkutil::ZooKeeperPtr & zookeeper)
{
    Strings candidate_lost_replicas;
    std::vector<Coordination::Requests> requests;

    for (const auto & pair : log_pointers_candidate_lost_replicas)
    {
        String replica = pair.first;
        LOG_WARNING(log, "Will mark replica {} as lost, because it has stale log pointer: {}", replica, pair.second);
        Coordination::Requests ops;
        /// If host changed version we can not mark replicas, because replica started to be active.
        ops.emplace_back(zkutil::makeCheckRequest(
            storage.zookeeper_path + "/replicas/" + replica + "/host", host_versions_lost_replicas.at(replica)));
        ops.emplace_back(zkutil::makeSetRequest(
            storage.zookeeper_path + "/replicas/" + replica + "/is_lost", "1", -1));
        candidate_lost_replicas.push_back(replica);
        requests.push_back(ops);
    }

    if (candidate_lost_replicas.size() == replicas_count)
        throw Exception(ErrorCodes::ALL_REPLICAS_LOST, "All replicas are stale: we won't mark any replica as lost");

    std::vector<zkutil::ZooKeeper::FutureMulti> futures;
    for (size_t i = 0; i < candidate_lost_replicas.size(); ++i)
        futures.emplace_back(zookeeper->asyncTryMultiNoThrow(requests[i]));

    for (size_t i = 0; i < candidate_lost_replicas.size(); ++i)
    {
        auto multi_responses = futures[i].get();
        if (multi_responses.responses[0]->error == Coordination::Error::ZBADVERSION)
            throw Exception(DB::ErrorCodes::REPLICA_STATUS_CHANGED, "{} became active when we marked lost replicas.", candidate_lost_replicas[i]);
        zkutil::KeeperMultiException::check(multi_responses.error, requests[i], multi_responses.responses);
    }
}


struct ReplicatedMergeTreeCleanupThread::NodeWithStat
{
    String node;
    Int64 ctime = 0;
    Int32 version = 0;

    NodeWithStat(String node_, Int64 ctime_, Int32 version_) : node(std::move(node_)), ctime(ctime_), version(version_) {}

    static bool greaterByTime(const NodeWithStat & lhs, const NodeWithStat & rhs)
    {
        return std::forward_as_tuple(lhs.ctime, lhs.node) > std::forward_as_tuple(rhs.ctime, rhs.node);
    }
};

size_t ReplicatedMergeTreeCleanupThread::clearOldBlocks(const String & blocks_dir_name, UInt64 window_seconds, UInt64 window_size, NodeCTimeAndVersionCache & cached_block_stats)
{
    auto zookeeper = storage.getZooKeeper();

    std::vector<NodeWithStat> timed_blocks;
    getBlocksSortedByTime(blocks_dir_name, *zookeeper, timed_blocks, cached_block_stats);

    if (timed_blocks.empty())
        return 0;

    /// Use ZooKeeper's first node (last according to time) timestamp as "current" time.
    Int64 current_time = timed_blocks.front().ctime;
    Int64 time_threshold = std::max(
        static_cast<Int64>(0),
        current_time - static_cast<Int64>(1000 * window_seconds));

    /// Virtual node, all nodes that are "greater" than this one will be deleted
    NodeWithStat block_threshold{{}, time_threshold, 0};

    size_t current_deduplication_window = std::min<size_t>(timed_blocks.size(), window_size);
    auto first_outdated_block_fixed_threshold = timed_blocks.begin() + current_deduplication_window;
    auto first_outdated_block_time_threshold = std::upper_bound(
        timed_blocks.begin(), timed_blocks.end(), block_threshold, NodeWithStat::greaterByTime);
    auto first_outdated_block = std::min(first_outdated_block_fixed_threshold, first_outdated_block_time_threshold);

    auto num_nodes_to_delete = timed_blocks.end() - first_outdated_block;
    if (!num_nodes_to_delete)
        return 0;

    auto last_outdated_block = timed_blocks.end() - 1;
    LOG_TRACE(log, "Will clear {} old blocks from {} (ctime {}) to {} (ctime {})", num_nodes_to_delete,
              first_outdated_block->node, first_outdated_block->ctime,
              last_outdated_block->node, last_outdated_block->ctime);

    zkutil::AsyncResponses<Coordination::RemoveResponse> try_remove_futures;
    for (auto it = first_outdated_block; it != timed_blocks.end(); ++it)
    {
        String path = storage.zookeeper_path + "/" + blocks_dir_name + "/" + it->node;
        try_remove_futures.emplace_back(path, zookeeper->asyncTryRemove(path, it->version));
    }

    for (auto & pair : try_remove_futures)
    {
        const String & path = pair.first;
        Coordination::Error rc = pair.second.get().error;
        if (rc == Coordination::Error::ZNOTEMPTY)
        {
            /// Can happen if there are leftover block nodes with children created by previous server versions.
            zookeeper->removeRecursive(path);
            cached_block_stats.erase(first_outdated_block->node);
        }
        else if (rc == Coordination::Error::ZOK || rc == Coordination::Error::ZNONODE || rc == Coordination::Error::ZBADVERSION)
        {
            /// No node is Ok. Another replica is removing nodes concurrently.
            /// Successfully removed blocks have to be removed from cache
            cached_block_stats.erase(first_outdated_block->node);
        }
        else
        {
            LOG_WARNING(log, "Error while deleting ZooKeeper path `{}`: {}, ignoring.", path, rc);
        }
        first_outdated_block++;
    }

    LOG_TRACE(log, "Cleared {} old blocks from ZooKeeper", num_nodes_to_delete);
    return num_nodes_to_delete;
}


void ReplicatedMergeTreeCleanupThread::getBlocksSortedByTime(const String & blocks_dir_name, zkutil::ZooKeeper & zookeeper, std::vector<NodeWithStat> & timed_blocks, NodeCTimeAndVersionCache & cached_block_stats)
{
    timed_blocks.clear();

    Strings blocks;
    Coordination::Stat stat;
    if (Coordination::Error::ZOK != zookeeper.tryGetChildren(storage.zookeeper_path + "/" + blocks_dir_name, blocks, &stat))
        throw Exception(ErrorCodes::NOT_FOUND_NODE, "{}/{} doesn't exist", storage.zookeeper_path, blocks_dir_name);

    /// Seems like this code is obsolete, because we delete blocks from cache
    /// when they are deleted from zookeeper. But we don't know about all (maybe future) places in code
    /// where they can be removed, so just to be sure that cache would not leak we check it here.
    {
        NameSet blocks_set(blocks.begin(), blocks.end());
        for (auto it = cached_block_stats.begin(); it != cached_block_stats.end();)
        {
            if (!blocks_set.contains(it->first))
                it = cached_block_stats.erase(it);
            else
                ++it;
        }
    }

    auto not_cached_blocks = stat.numChildren - cached_block_stats.size();
    if (not_cached_blocks)
    {
        LOG_TRACE(log, "Checking {} {} ({} are not cached){}, path is {}", stat.numChildren, blocks_dir_name, not_cached_blocks, " to clear old ones from ZooKeeper.", storage.zookeeper_path + "/" + blocks_dir_name);
    }

    std::vector<std::string> exists_paths;
    for (const String & block : blocks)
    {
        auto it = cached_block_stats.find(block);
        if (it == cached_block_stats.end())
        {
            /// New block. Fetch its stat asynchronously.
            exists_paths.emplace_back(storage.zookeeper_path + "/" + blocks_dir_name + "/" + block);
        }
        else
        {
            /// Cached block
            const auto & ctime_and_version = it->second;
            timed_blocks.emplace_back(block, ctime_and_version.first, ctime_and_version.second);
        }
    }

    auto exists_size = exists_paths.size();
    auto exists_results = zookeeper.exists(exists_paths);

    /// Put fetched stats into the cache
    for (size_t i = 0; i < exists_size; ++i)
    {
        auto status = exists_results[i];
        if (status.error != Coordination::Error::ZNONODE)
        {
            auto node_name = fs::path(exists_paths[i]).filename();
            cached_block_stats.emplace(node_name, std::make_pair(status.stat.ctime, status.stat.version));
            timed_blocks.emplace_back(node_name, status.stat.ctime, status.stat.version);
        }
    }

    ::sort(timed_blocks.begin(), timed_blocks.end(), NodeWithStat::greaterByTime);
}


size_t ReplicatedMergeTreeCleanupThread::clearOldMutations()
{
    auto storage_settings = storage.getSettings();
    if (!(*storage_settings)[MergeTreeSetting::finished_mutations_to_keep])
        return 0;

    if (storage.queue.countFinishedMutations() <= (*storage_settings)[MergeTreeSetting::finished_mutations_to_keep])
    {
        /// Not strictly necessary, but helps to avoid unnecessary ZooKeeper requests.
        /// If even this replica hasn't finished enough mutations yet, then we don't need to clean anything.
        return 0;
    }

    auto zookeeper = storage.getZooKeeper();

    Coordination::Stat replicas_stat;
    Strings replicas = zookeeper->getChildren(storage.zookeeper_path + "/replicas", &replicas_stat);

    UInt64 min_pointer = std::numeric_limits<UInt64>::max();
    for (const String & replica : replicas)
    {
        String pointer;
        // No Need to check return value to delete mutations.
        zookeeper->tryGet(storage.zookeeper_path + "/replicas/" + replica + "/mutation_pointer", pointer);
        if (pointer.empty())
            return 0; /// One replica hasn't done anything yet so we can't delete any mutations.
        min_pointer = std::min(parse<UInt64>(pointer), min_pointer);
    }

    Strings entries = zookeeper->getChildren(storage.zookeeper_path + "/mutations");
    ::sort(entries.begin(), entries.end());

    /// Do not remove entries that are greater than `min_pointer` (they are not done yet).
    entries.erase(std::upper_bound(entries.begin(), entries.end(), padIndex(min_pointer)), entries.end());
    /// Do not remove last `(*storage_settings)[MergeTreeSetting::finished_mutations_to_keep`] entries.
    if (entries.size() <= (*storage_settings)[MergeTreeSetting::finished_mutations_to_keep])
        return 0;
    entries.erase(entries.end() - (*storage_settings)[MergeTreeSetting::finished_mutations_to_keep], entries.end());

    if (entries.empty())
        return 0;

    Coordination::Requests ops;
    size_t batch_start_i = 0;
    for (size_t i = 0; i < entries.size(); ++i)
    {
        ops.emplace_back(zkutil::makeRemoveRequest(storage.zookeeper_path + "/mutations/" + entries[i], -1));

        if (ops.size() > 4 * zkutil::MULTI_BATCH_SIZE || i + 1 == entries.size())
        {
            /// Simultaneously with clearing the log, we check to see if replica was added since we received replicas list.
            ops.emplace_back(zkutil::makeCheckRequest(storage.zookeeper_path + "/replicas", replicas_stat.version));
            try
            {
                zookeeper->multi(ops);
            }
            catch (const zkutil::KeeperMultiException & e)
            {
                /// Another replica already deleted the same node concurrently.
                if (e.code == Coordination::Error::ZNONODE)
                    break;

                throw;
            }
            LOG_DEBUG(log, "Removed {} old mutation entries: {} - {}",
                i + 1 - batch_start_i, entries[batch_start_i], entries[i]);
            batch_start_i = i + 1;
            ops.clear();
        }
    }

    return entries.size();
}

}
