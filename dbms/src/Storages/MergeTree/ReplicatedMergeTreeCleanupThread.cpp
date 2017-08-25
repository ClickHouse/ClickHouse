#include <Storages/MergeTree/ReplicatedMergeTreeCleanupThread.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/setThreadName.h>
#include <Poco/Timestamp.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_FOUND_NODE;
}


ReplicatedMergeTreeCleanupThread::ReplicatedMergeTreeCleanupThread(StorageReplicatedMergeTree & storage_)
    : storage(storage_),
    log(&Logger::get(storage.database_name + "." + storage.table_name + " (StorageReplicatedMergeTree, CleanupThread)")),
    thread([this] { run(); }),
    cached_block_stats(std::make_unique<NodesStatCache>()) {}


void ReplicatedMergeTreeCleanupThread::run()
{
    setThreadName("ReplMTCleanup");

    const auto CLEANUP_SLEEP_MS = storage.data.settings.cleanup_delay_period * 1000;

    while (!storage.shutdown_called)
    {
        try
        {
            iterate();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        storage.shutdown_event.tryWait(CLEANUP_SLEEP_MS);
    }

    LOG_DEBUG(log, "Cleanup thread finished");
}


void ReplicatedMergeTreeCleanupThread::iterate()
{
    storage.clearOldPartsAndRemoveFromZK(log);
    storage.data.clearOldTemporaryDirectories();

    if (storage.is_leader_node)
    {
        clearOldLogs();
        clearOldBlocks();
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
    if (static_cast<double>(children_count) < storage.data.settings.replicated_logs_to_keep * 1.1)
        return;

    Strings replicas = zookeeper->getChildren(storage.zookeeper_path + "/replicas", &stat);
    UInt64 min_pointer = std::numeric_limits<UInt64>::max();
    for (const String & replica : replicas)
    {
        String pointer = zookeeper->get(storage.zookeeper_path + "/replicas/" + replica + "/log_pointer");
        if (pointer.empty())
            return;
        min_pointer = std::min(min_pointer, parse<UInt64>(pointer));
    }

    Strings entries = zookeeper->getChildren(storage.zookeeper_path + "/log");
    std::sort(entries.begin(), entries.end());

    /// We will not touch the last `replicated_logs_to_keep` records.
    entries.erase(entries.end() - std::min(entries.size(), storage.data.settings.replicated_logs_to_keep), entries.end());
    /// We will not touch records that are no less than `min_pointer`.
    entries.erase(std::lower_bound(entries.begin(), entries.end(), "log-" + padIndex(min_pointer)), entries.end());

    if (entries.empty())
        return;

    zkutil::Ops ops;
    for (size_t i = 0; i < entries.size(); ++i)
    {
        ops.emplace_back(std::make_unique<zkutil::Op::Remove>(storage.zookeeper_path + "/log/" + entries[i], -1));

        if (ops.size() > 4 * zkutil::MULTI_BATCH_SIZE || i + 1 == entries.size())
        {
            /// Simultaneously with clearing the log, we check to see if replica was added since we received replicas list.
            ops.emplace_back(std::make_unique<zkutil::Op::Check>(storage.zookeeper_path + "/replicas", stat.version));
            zookeeper->multi(ops);
            ops.clear();
        }
    }

    LOG_DEBUG(log, "Removed " << entries.size() << " old log entries: " << entries.front() << " - " << entries.back());
}


namespace
{

/// Just a subset of zkutil::Stat fields required for the cache
struct RequiredStat
{
    int64_t ctime = 0;
    int32_t numChildren = 0;

    RequiredStat() = default;
    RequiredStat(const RequiredStat &) = default;
    explicit RequiredStat(const zkutil::Stat & s) : ctime(s.ctime), numChildren(s.numChildren) {};
    explicit RequiredStat(Int64 ctime_) : ctime(ctime_) {}
};

}

/// Just a node name with its ZooKeeper's stat
struct ReplicatedMergeTreeCleanupThread::NodeWithStat
{
    String node;
    RequiredStat stat;

    NodeWithStat() = default;
    NodeWithStat(const String & node_, const RequiredStat & stat_) : node(node_), stat(stat_) {}

    static bool greaterByTime (const NodeWithStat & lhs, const NodeWithStat & rhs)
    {
        return std::greater<void>()(std::forward_as_tuple(lhs.stat.ctime, lhs.node), std::forward_as_tuple(rhs.stat.ctime, rhs.node));
    }
};

/// Use simple map node_name -> zkutil::Stat (only required fields) as the cache
/// It is not declared in the header explicitly to hide extra implementation dependent structs like RequiredStat
class ReplicatedMergeTreeCleanupThread::NodesStatCache : public std::map<String, RequiredStat> {};


void ReplicatedMergeTreeCleanupThread::clearOldBlocks()
{
    auto zookeeper = storage.getZooKeeper();

    std::vector<NodeWithStat> timed_blocks;
    getBlocksSortedByTime(zookeeper, timed_blocks);

    if (timed_blocks.empty())
        return;

    /// Use ZooKeeper's first node (last according to time) timestamp as "current" time.
    Int64 current_time = timed_blocks.front().stat.ctime;
    Int64 time_threshold = std::max(0L, current_time - static_cast<Int64>(1000 * storage.data.settings.replicated_deduplication_window_seconds));

    /// Virtual node, all nodes that are "greater" than this one will be deleted
    NodeWithStat block_threshold("", RequiredStat(time_threshold));

    size_t current_deduplication_window = std::min(timed_blocks.size(), storage.data.settings.replicated_deduplication_window);
    auto first_outdated_block_fixed_threshold = timed_blocks.begin() + current_deduplication_window;
    auto first_outdated_block_time_threshold = std::upper_bound(timed_blocks.begin(), timed_blocks.end(), block_threshold, NodeWithStat::greaterByTime);
    auto first_outdated_block = std::min(first_outdated_block_fixed_threshold, first_outdated_block_time_threshold);

    /// TODO After about half a year, we could remain only multi op, because there will be no obsolete children nodes.
    zkutil::Ops ops;
    for (auto it = first_outdated_block; it != timed_blocks.end(); ++it)
    {
        String path = storage.zookeeper_path + "/blocks/" + it->node;

        if (it->stat.numChildren == 0)
        {
            ops.emplace_back(new zkutil::Op::Remove(path, -1));
            if (ops.size() >= zkutil::MULTI_BATCH_SIZE)
            {
                zookeeper->multi(ops);
                ops.clear();
            }
        }
        else
            zookeeper->removeRecursive(path);
    }

    if (!ops.empty())
    {
        zookeeper->multi(ops);
        ops.clear();
    }

    auto num_nodes_to_delete = timed_blocks.end() - first_outdated_block;
    LOG_TRACE(log, "Cleared " << num_nodes_to_delete << " old blocks from ZooKeeper");
}


void ReplicatedMergeTreeCleanupThread::getBlocksSortedByTime(zkutil::ZooKeeperPtr & zookeeper, std::vector<NodeWithStat> & timed_blocks)
{
    timed_blocks.clear();

    Strings blocks;
    zkutil::Stat stat;
    if (ZOK != zookeeper->tryGetChildren(storage.zookeeper_path + "/blocks", blocks, &stat))
        throw Exception(storage.zookeeper_path + "/blocks doesn't exist", ErrorCodes::NOT_FOUND_NODE);

    /// Clear already deleted blocks from the cache, cached_block_ctime should be subset of blocks
    {
        NameSet blocks_set(blocks.begin(), blocks.end());
        for (auto it = cached_block_stats->begin(); it != cached_block_stats->end();)
        {
            if (!blocks_set.count(it->first))
                it = cached_block_stats->erase(it);
            else
                ++it;
        }
    }

    auto not_cached_blocks = stat.numChildren - cached_block_stats->size();
    LOG_TRACE(log, "Checking " << stat.numChildren << " blocks (" << not_cached_blocks << " are not cached)"
            << " to clear old ones from ZooKeeper. This might take several minutes.");

    std::vector<std::pair<String, zkutil::ZooKeeper::ExistsFuture>> exists_futures;
    for (const String & block : blocks)
    {
        auto it = cached_block_stats->find(block);
        if (it == cached_block_stats->end())
        {
            /// New block. Fetch its stat stat asynchronously
            exists_futures.emplace_back(block, zookeeper->asyncExists(storage.zookeeper_path + "/blocks/" + block));
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
        zkutil::ZooKeeper::StatAndExists status = elem.second.get();
        if (!status.exists)
            throw zkutil::KeeperException("A block node was suddenly deleted", ZNONODE);

        cached_block_stats->emplace(elem.first, RequiredStat(status.stat));
        timed_blocks.emplace_back(elem.first, RequiredStat(status.stat));
    }

    std::sort(timed_blocks.begin(), timed_blocks.end(), NodeWithStat::greaterByTime);
}


ReplicatedMergeTreeCleanupThread::~ReplicatedMergeTreeCleanupThread()
{
    if (thread.joinable())
        thread.join();
}

}
