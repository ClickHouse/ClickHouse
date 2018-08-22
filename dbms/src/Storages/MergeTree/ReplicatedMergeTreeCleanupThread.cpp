#include <Storages/MergeTree/ReplicatedMergeTreeCleanupThread.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/setThreadName.h>
#include <Poco/Timestamp.h>

#include <random>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_FOUND_NODE;
    extern const int ALL_REPLICAS_LOST;
    extern const int REPLICA_IS_ACTIVE;
}


ReplicatedMergeTreeCleanupThread::ReplicatedMergeTreeCleanupThread(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , log_name(storage.database_name + "." + storage.table_name + " (ReplicatedMergeTreeCleanupThread)")
    , log(&Logger::get(log_name))
{
    task = storage.context.getSchedulePool().createTask(log_name, [this]{ run(); });
}

void ReplicatedMergeTreeCleanupThread::run()
{
    const auto CLEANUP_SLEEP_MS = storage.data.settings.cleanup_delay_period * 1000
        + std::uniform_int_distribution<UInt64>(0, storage.data.settings.cleanup_delay_period_random_add * 1000)(rng);

    try
    {
        iterate();
    }
    catch (const zkutil::KeeperException & e)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);

        if (e.code == ZooKeeperImpl::ZooKeeper::ZSESSIONEXPIRED)
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
    storage.data.clearOldTemporaryDirectories();

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

    zkutil::Stat stat;
    if (!zookeeper->exists(storage.zookeeper_path + "/log", &stat))
        throw Exception(storage.zookeeper_path + "/log doesn't exist", ErrorCodes::NOT_FOUND_NODE);

    int children_count = stat.numChildren;

    /// We will wait for 1.1 times more records to accumulate than necessary.
    if (static_cast<double>(children_count) < storage.data.settings.min_replicated_logs_to_keep * 1.1)
        return;

    Strings replicas = zookeeper->getChildren(storage.zookeeper_path + "/replicas", &stat);
    UInt64 min_pointer_active_replica = std::numeric_limits<UInt64>::max();

    Strings entries = zookeeper->getChildren(storage.zookeeper_path + "/log");

    if (entries.empty())
        return;

    std::sort(entries.begin(), entries.end());

    String min_saved_record_log_str = entries[entries.size() > storage.data.settings.max_replicated_logs_to_keep.value
                                              ? entries.size() - storage.data.settings.max_replicated_logs_to_keep.value
                                              : 0];
    String min_pointer_inactive_replica_str;

    std::unordered_map<String, UInt32> hosts_version;
    std::unordered_map<String, String> log_pointers_lost_replicas;

    for (const String & replica : replicas)
    {
        zkutil::Stat host_stat;
        zkutil::Stat log_pointer_stat;
        zookeeper->get(storage.zookeeper_path + "/replicas/" + replica + "/host", &host_stat);
        String pointer = zookeeper->get(storage.zookeeper_path + "/replicas/" + replica + "/log_pointer", &log_pointer_stat);
        if (pointer.empty())
            return;

        UInt32 log_pointer = parse<UInt64>(pointer);
        String log_pointer_str = "log-" + padIndex(log_pointer);

        /// Check status of replica (active or not).
        /// If replica was not active, we could check when it's log_pointer locates.
        if (zookeeper->exists(storage.zookeeper_path + "/replicas/" + replica + "/is_active") || !zookeeper->exists(storage.zookeeper_path + "/replicas/" + replica + "/is_lost"))
            min_pointer_active_replica = std::min(min_pointer_active_replica, log_pointer);
        else
        {
            /// We can not mark lost replicas.
            if (zookeeper->get(storage.zookeeper_path + "/replicas/" + replica + "/is_lost") == "0")
            {
                hosts_version[replica] = host_stat.version;
                log_pointers_lost_replicas[replica] = log_pointer_str;

                if (log_pointer_str >= min_saved_record_log_str)
                {
                    if (min_pointer_inactive_replica_str != "" && min_pointer_inactive_replica_str >= log_pointer_str)
                        min_pointer_inactive_replica_str = log_pointer_str;
                    else if (min_pointer_inactive_replica_str == "")
                        min_pointer_inactive_replica_str = log_pointer_str;
                }
            }
        }
    }

    String min_pointer_active_replica_str = "log-" + padIndex(min_pointer_active_replica);

    String min_pointer_replica_str = min_pointer_inactive_replica_str == ""
                                     ? min_pointer_active_replica_str
                                     : std::min(min_pointer_inactive_replica_str, min_pointer_active_replica_str);

    /// We will not touch the last `min_replicated_logs_to_keep` records.
    entries.erase(entries.end() - std::min(entries.size(), storage.data.settings.min_replicated_logs_to_keep.value), entries.end());
    /// We will not touch records that are no less than `min_pointer_active_replica`.
    entries.erase(std::lower_bound(entries.begin(), entries.end(), min_pointer_replica_str), entries.end());

    if (entries.empty())
        return;

    markLostReplicas(hosts_version, log_pointers_lost_replicas, entries.back(), zookeeper);

    zkutil::Requests ops;
    for (size_t i = 0; i < entries.size(); ++i)
    {
        ops.emplace_back(zkutil::makeRemoveRequest(storage.zookeeper_path + "/log/" + entries[i], -1));

        if (ops.size() > 4 * zkutil::MULTI_BATCH_SIZE || i + 1 == entries.size())
        {
            /// Simultaneously with clearing the log, we check to see if replica was added since we received replicas list.
            ops.emplace_back(zkutil::makeCheckRequest(storage.zookeeper_path + "/replicas", stat.version));
            zookeeper->multi(ops);
            ops.clear();
        }
    }

    LOG_DEBUG(log, "Removed " << entries.size() << " old log entries: " << entries.front() << " - " << entries.back());
}


void ReplicatedMergeTreeCleanupThread::markLostReplicas(const std::unordered_map<String, UInt32> & hosts_version,
                                                        const std::unordered_map<String, String> & log_pointers_lost_replicas,
                                                        const String & remove_border, const zkutil::ZooKeeperPtr & zookeeper)
{
    std::vector<zkutil::Requests> requests;
    std::vector<zkutil::ZooKeeper::FutureMulti> futures;

    for (auto pair : log_pointers_lost_replicas)
    {
        String replica = pair.first;
        if (pair.second <= remove_border)
        { 
            zkutil::Requests ops;
            /// If host changed version we can not mark replicas, because replica startes to be active.
            ops.emplace_back(zkutil::makeCheckRequest(storage.zookeeper_path + "/replicas/" + replica + "/host", hosts_version.at(replica)));
            ops.emplace_back(zkutil::makeSetRequest(storage.zookeeper_path + "/replicas/" + replica + "/is_lost", "1", -1));
            requests.push_back(ops);
        }
    }
    
    if (requests.size() == (zookeeper->getChildren(storage.zookeeper_path + "/replicas")).size())
        throw Exception("All replicas are lost", ErrorCodes::ALL_REPLICAS_LOST);
    
    for (auto & req : requests)
        futures.push_back(zookeeper->tryAsyncMulti(req));

    for (auto & future : futures)
    {
        auto res = future.get();
        if (res.error == ZooKeeperImpl::ZooKeeper::ZBADVERSION)
            throw Exception("One of the replicas became active, when we clear log", ErrorCodes::REPLICA_IS_ACTIVE);
        else if (res.error != ZooKeeperImpl::ZooKeeper::ZOK)
            throw;
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
    Int64 time_threshold = std::max(static_cast<Int64>(0), current_time - static_cast<Int64>(1000 * storage.data.settings.replicated_deduplication_window_seconds));

    /// Virtual node, all nodes that are "greater" than this one will be deleted
    NodeWithStat block_threshold{{}, time_threshold};

    size_t current_deduplication_window = std::min(timed_blocks.size(), storage.data.settings.replicated_deduplication_window.value);
    auto first_outdated_block_fixed_threshold = timed_blocks.begin() + current_deduplication_window;
    auto first_outdated_block_time_threshold = std::upper_bound(timed_blocks.begin(), timed_blocks.end(), block_threshold, NodeWithStat::greaterByTime);
    auto first_outdated_block = std::min(first_outdated_block_fixed_threshold, first_outdated_block_time_threshold);

    zkutil::AsyncResponses<zkutil::RemoveResponse> try_remove_futures;
    for (auto it = first_outdated_block; it != timed_blocks.end(); ++it)
    {
        String path = storage.zookeeper_path + "/blocks/" + it->node;
        try_remove_futures.emplace_back(path, zookeeper->asyncTryRemove(path));
    }

    for (auto & pair : try_remove_futures)
    {
        const String & path = pair.first;
        int32_t rc = pair.second.get().error;
        if (rc == ZooKeeperImpl::ZooKeeper::ZNOTEMPTY)
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
    zkutil::Stat stat;
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

    zkutil::AsyncResponses<zkutil::ExistsResponse> exists_futures;
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
        if (status.error != ZooKeeperImpl::ZooKeeper::ZNONODE)
        {
            cached_block_stats.emplace(elem.first, status.stat.ctime);
            timed_blocks.emplace_back(elem.first, status.stat.ctime);
        }
    }

    std::sort(timed_blocks.begin(), timed_blocks.end(), NodeWithStat::greaterByTime);
}


void ReplicatedMergeTreeCleanupThread::clearOldMutations()
{
    if (!storage.data.settings.finished_mutations_to_keep)
        return;

    if (storage.queue.countFinishedMutations() <= storage.data.settings.finished_mutations_to_keep)
    {
        /// Not strictly necessary, but helps to avoid unnecessary ZooKeeper requests.
        /// If even this replica hasn't finished enough mutations yet, then we don't need to clean anything.
        return;
    }

    auto zookeeper = storage.getZooKeeper();

    zkutil::Stat replicas_stat;
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
    /// Do not remove last `storage.data.settings.finished_mutations_to_keep` entries.
    if (entries.size() <= storage.data.settings.finished_mutations_to_keep)
        return;
    entries.erase(entries.end() - storage.data.settings.finished_mutations_to_keep, entries.end());

    if (entries.empty())
        return;

    zkutil::Requests ops;
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
