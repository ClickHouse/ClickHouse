#include <Storages/MergeTree/ReplicatedMergeTreeCleanupThread.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Poco/Timestamp.h>

#include <random>
#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_FOUND_NODE;
    extern const int ALL_REPLICAS_LOST;
    extern const int REPLICA_STATUS_CHANGED;
}


ReplicatedMergeTreeCleanupThread::ReplicatedMergeTreeCleanupThread(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , log_name(storage.database_name + "." + storage.table_name + " (ReplicatedMergeTreeCleanupThread)")
    , log(&Logger::get(log_name))
{
    task = storage.global_context.getSchedulePool().createTask(log_name, [this]{ run(); });
}

void ReplicatedMergeTreeCleanupThread::run()
{
    const auto CLEANUP_SLEEP_MS = storage.settings.cleanup_delay_period * 1000
        + std::uniform_int_distribution<UInt64>(0, storage.settings.cleanup_delay_period_random_add * 1000)(rng);

    try
    {
        iterate();
    }
    catch (const Coordination::Exception & e)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

        if (e.code == Coordination::ZSESSIONEXPIRED)
            return;
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
    }

    task->scheduleAfter(CLEANUP_SLEEP_MS);
}


void ReplicatedMergeTreeCleanupThread::iterate()
{
    storage.clearOldPartsAndRemoveFromZK();

    {
        /// TODO: Implement tryLockStructureForShare.
        auto lock = storage.lockStructureForShare(false, "");
        storage.clearOldTemporaryDirectories();
    }

    /// This is loose condition: no problem if we actually had lost leadership at this moment
    ///  and two replicas will try to do cleanup simultaneously.
    if (storage.is_leader)
    {
        clearOldLogs();
        clearOldBlocks();
        clearOldMutations();
    }
}


void ReplicatedMergeTreeCleanupThread::clearOldLogs()
{
    auto zookeeper = storage.getZooKeeper();

    Coordination::Stat stat;
    if (!zookeeper->exists(storage.zookeeper_path + "/log", &stat))
        throw Exception(storage.zookeeper_path + "/log doesn't exist", ErrorCodes::NOT_FOUND_NODE);

    int children_count = stat.numChildren;

    /// We will wait for 1.1 times more records to accumulate than necessary.
    if (static_cast<double>(children_count) < storage.settings.min_replicated_logs_to_keep * 1.1)
        return;

    Strings replicas = zookeeper->getChildren(storage.zookeeper_path + "/replicas", &stat);

    /// We will keep logs after and including this threshold.
    UInt64 min_saved_log_pointer = std::numeric_limits<UInt64>::max();

    UInt64 min_log_pointer_lost_candidate = std::numeric_limits<UInt64>::max();

    Strings entries = zookeeper->getChildren(storage.zookeeper_path + "/log");

    if (entries.empty())
        return;

    std::sort(entries.begin(), entries.end());

    String min_saved_record_log_str = entries[
        entries.size() > storage.settings.max_replicated_logs_to_keep.value
            ? entries.size() - storage.settings.max_replicated_logs_to_keep.value
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
    entries.erase(entries.end() - std::min<UInt64>(entries.size(), storage.settings.min_replicated_logs_to_keep.value), entries.end());
    /// We will not touch records that are no less than `min_saved_log_pointer`.
    entries.erase(std::lower_bound(entries.begin(), entries.end(), "log-" + padIndex(min_saved_log_pointer)), entries.end());

    if (entries.empty())
        return;

    markLostReplicas(host_versions_lost_replicas, log_pointers_candidate_lost_replicas, replicas.size() - num_replicas_were_marked_is_lost, zookeeper);

    Coordination::Requests ops;
    for (size_t i = 0; i < entries.size(); ++i)
    {
        ops.emplace_back(zkutil::makeRemoveRequest(storage.zookeeper_path + "/log/" + entries[i], -1));

        if (ops.size() > 4 * zkutil::MULTI_BATCH_SIZE || i + 1 == entries.size())
        {
            /// We need to check this because the replica that was restored from one of the marked replicas does not copy a non-valid log_pointer.
            for (const auto & host_version : host_versions_lost_replicas)
                ops.emplace_back(zkutil::makeCheckRequest(storage.zookeeper_path + "/replicas/" + host_version.first + "/host", host_version.second));

            /// Simultaneously with clearing the log, we check to see if replica was added since we received replicas list.
            ops.emplace_back(zkutil::makeCheckRequest(storage.zookeeper_path + "/replicas", stat.version));
            zookeeper->multi(ops);
            ops.clear();
        }
    }

    LOG_DEBUG(log, "Removed " << entries.size() << " old log entries: " << entries.front() << " - " << entries.back());
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
        Coordination::Requests ops;
        /// If host changed version we can not mark replicas, because replica started to be active.
        ops.emplace_back(zkutil::makeCheckRequest(storage.zookeeper_path + "/replicas/" + replica + "/host", host_versions_lost_replicas.at(replica)));
        ops.emplace_back(zkutil::makeSetRequest(storage.zookeeper_path + "/replicas/" + replica + "/is_lost", "1", -1));
        candidate_lost_replicas.push_back(replica);
        requests.push_back(ops);
    }

    if (candidate_lost_replicas.size() == replicas_count)
        throw Exception("All replicas are stale: we won't mark any replica as lost", ErrorCodes::ALL_REPLICAS_LOST);

    std::vector<zkutil::ZooKeeper::FutureMulti> futures;
    for (size_t i = 0; i < candidate_lost_replicas.size(); ++i)
        futures.emplace_back(zookeeper->tryAsyncMulti(requests[i]));

    for (size_t i = 0; i < candidate_lost_replicas.size(); ++i)
    {
        auto multi_responses = futures[i].get();
        if (multi_responses.responses[0]->error == Coordination::Error::ZBADVERSION)
            throw Exception(candidate_lost_replicas[i] + " became active when we marked lost replicas.", DB::ErrorCodes::REPLICA_STATUS_CHANGED);
        zkutil::KeeperMultiException::check(multi_responses.error, requests[i], multi_responses.responses);
    }
}


struct ReplicatedMergeTreeCleanupThread::NodeWithStat
{
    String node;
    Int64 ctime = 0;

    NodeWithStat(String node_, Int64 ctime_) : node(std::move(node_)), ctime(ctime_) {}

    static bool greaterByTime(const NodeWithStat & lhs, const NodeWithStat & rhs)
    {
        return std::forward_as_tuple(lhs.ctime, lhs.node) > std::forward_as_tuple(rhs.ctime, rhs.node);
    }
};

void ReplicatedMergeTreeCleanupThread::clearOldBlocks()
{
    auto zookeeper = storage.getZooKeeper();

    std::vector<NodeWithStat> timed_blocks;
    getBlocksSortedByTime(*zookeeper, timed_blocks);

    if (timed_blocks.empty())
        return;

    /// Use ZooKeeper's first node (last according to time) timestamp as "current" time.
    Int64 current_time = timed_blocks.front().ctime;
    Int64 time_threshold = std::max(static_cast<Int64>(0), current_time - static_cast<Int64>(1000 * storage.settings.replicated_deduplication_window_seconds));

    /// Virtual node, all nodes that are "greater" than this one will be deleted
    NodeWithStat block_threshold{{}, time_threshold};

    size_t current_deduplication_window = std::min<size_t>(timed_blocks.size(), storage.settings.replicated_deduplication_window.value);
    auto first_outdated_block_fixed_threshold = timed_blocks.begin() + current_deduplication_window;
    auto first_outdated_block_time_threshold = std::upper_bound(timed_blocks.begin(), timed_blocks.end(), block_threshold, NodeWithStat::greaterByTime);
    auto first_outdated_block = std::min(first_outdated_block_fixed_threshold, first_outdated_block_time_threshold);

    zkutil::AsyncResponses<Coordination::RemoveResponse> try_remove_futures;
    for (auto it = first_outdated_block; it != timed_blocks.end(); ++it)
    {
        String path = storage.zookeeper_path + "/blocks/" + it->node;
        try_remove_futures.emplace_back(path, zookeeper->asyncTryRemove(path));
    }

    for (auto & pair : try_remove_futures)
    {
        const String & path = pair.first;
        int32_t rc = pair.second.get().error;
        if (rc == Coordination::ZNOTEMPTY)
        {
            /// Can happen if there are leftover block nodes with children created by previous server versions.
            zookeeper->removeRecursive(path);
        }
        else if (rc)
            LOG_WARNING(log,
                "Error while deleting ZooKeeper path `" << path << "`: " + zkutil::ZooKeeper::error2string(rc) << ", ignoring.");
    }

    auto num_nodes_to_delete = timed_blocks.end() - first_outdated_block;
    if (num_nodes_to_delete)
        LOG_TRACE(log, "Cleared " << num_nodes_to_delete << " old blocks from ZooKeeper");
}


void ReplicatedMergeTreeCleanupThread::getBlocksSortedByTime(zkutil::ZooKeeper & zookeeper, std::vector<NodeWithStat> & timed_blocks)
{
    timed_blocks.clear();

    Strings blocks;
    Coordination::Stat stat;
    if (zookeeper.tryGetChildren(storage.zookeeper_path + "/blocks", blocks, &stat))
        throw Exception(storage.zookeeper_path + "/blocks doesn't exist", ErrorCodes::NOT_FOUND_NODE);

    /// Clear already deleted blocks from the cache, cached_block_ctime should be subset of blocks
    {
        NameSet blocks_set(blocks.begin(), blocks.end());
        for (auto it = cached_block_stats.begin(); it != cached_block_stats.end();)
        {
            if (!blocks_set.count(it->first))
                it = cached_block_stats.erase(it);
            else
                ++it;
        }
    }

    auto not_cached_blocks = stat.numChildren - cached_block_stats.size();
    if (not_cached_blocks)
    {
        LOG_TRACE(log, "Checking " << stat.numChildren << " blocks (" << not_cached_blocks << " are not cached)"
                                   << " to clear old ones from ZooKeeper.");
    }

    zkutil::AsyncResponses<Coordination::ExistsResponse> exists_futures;
    for (const String & block : blocks)
    {
        auto it = cached_block_stats.find(block);
        if (it == cached_block_stats.end())
        {
            /// New block. Fetch its stat asynchronously.
            exists_futures.emplace_back(block, zookeeper.asyncExists(storage.zookeeper_path + "/blocks/" + block));
        }
        else
        {
            /// Cached block
            timed_blocks.emplace_back(block, it->second);
        }
    }

    /// Put fetched stats into the cache
    for (auto & elem : exists_futures)
    {
        auto status = elem.second.get();
        if (status.error != Coordination::ZNONODE)
        {
            cached_block_stats.emplace(elem.first, status.stat.ctime);
            timed_blocks.emplace_back(elem.first, status.stat.ctime);
        }
    }

    std::sort(timed_blocks.begin(), timed_blocks.end(), NodeWithStat::greaterByTime);
}


void ReplicatedMergeTreeCleanupThread::clearOldMutations()
{
    if (!storage.settings.finished_mutations_to_keep)
        return;

    if (storage.queue.countFinishedMutations() <= storage.settings.finished_mutations_to_keep)
    {
        /// Not strictly necessary, but helps to avoid unnecessary ZooKeeper requests.
        /// If even this replica hasn't finished enough mutations yet, then we don't need to clean anything.
        return;
    }

    auto zookeeper = storage.getZooKeeper();

    Coordination::Stat replicas_stat;
    Strings replicas = zookeeper->getChildren(storage.zookeeper_path + "/replicas", &replicas_stat);

    UInt64 min_pointer = std::numeric_limits<UInt64>::max();
    for (const String & replica : replicas)
    {
        String pointer;
        zookeeper->tryGet(storage.zookeeper_path + "/replicas/" + replica + "/mutation_pointer", pointer);
        if (pointer.empty())
            return; /// One replica hasn't done anything yet so we can't delete any mutations.
        min_pointer = std::min(parse<UInt64>(pointer), min_pointer);
    }

    Strings entries = zookeeper->getChildren(storage.zookeeper_path + "/mutations");
    std::sort(entries.begin(), entries.end());

    /// Do not remove entries that are greater than `min_pointer` (they are not done yet).
    entries.erase(std::upper_bound(entries.begin(), entries.end(), padIndex(min_pointer)), entries.end());
    /// Do not remove last `storage.settings.finished_mutations_to_keep` entries.
    if (entries.size() <= storage.settings.finished_mutations_to_keep)
        return;
    entries.erase(entries.end() - storage.settings.finished_mutations_to_keep, entries.end());

    if (entries.empty())
        return;

    Coordination::Requests ops;
    size_t batch_start_i = 0;
    for (size_t i = 0; i < entries.size(); ++i)
    {
        ops.emplace_back(zkutil::makeRemoveRequest(storage.zookeeper_path + "/mutations/" + entries[i], -1));

        if (ops.size() > 4 * zkutil::MULTI_BATCH_SIZE || i + 1 == entries.size())
        {
            /// Simultaneously with clearing the log, we check to see if replica was added since we received replicas list.
            ops.emplace_back(zkutil::makeCheckRequest(storage.zookeeper_path + "/replicas", replicas_stat.version));
            zookeeper->multi(ops);
            LOG_DEBUG(log, "Removed " << (i + 1 - batch_start_i) << " old mutation entries: " << entries[batch_start_i] << " - " << entries[i]);
            batch_start_i = i + 1;
            ops.clear();
        }
    }
}

}
