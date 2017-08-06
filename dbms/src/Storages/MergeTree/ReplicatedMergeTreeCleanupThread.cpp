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
    thread([this] { run(); }) {}


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

        if (ops.size() > 400 || i + 1 == entries.size())
        {
            /// Simultaneously with clearing the log, we check to see if replica was added since we received replicas list.
            ops.emplace_back(std::make_unique<zkutil::Op::Check>(storage.zookeeper_path + "/replicas", stat.version));
            zookeeper->multi(ops);
            ops.clear();
        }
    }

    LOG_DEBUG(log, "Removed " << entries.size() << " old log entries: " << entries.front() << " - " << entries.back());
}


void ReplicatedMergeTreeCleanupThread::clearOldBlocks()
{
    auto zookeeper = storage.getZooKeeper();

    Strings blocks;
    zkutil::Stat stat;
    if (ZOK != zookeeper->tryGetChildren(storage.zookeeper_path + "/blocks", blocks, &stat))
        throw Exception(storage.zookeeper_path + "/blocks doesn't exist", ErrorCodes::NOT_FOUND_NODE);

    /// Clear already deleted blocks from the cache, cached_block_ctime should be subset of blocks
    {
        NameSet blocks_set(blocks.begin(), blocks.end());
        for (auto it = cached_block_ctime.begin(); it != cached_block_ctime.end();)
        {
            if (!blocks_set.count(it->first))
                it = cached_block_ctime.erase(it);
            else
                ++it;
        }
    }

    auto not_cached_blocks = stat.numChildren - cached_block_ctime.size();
    LOG_TRACE(log, "Checking " << stat.numChildren << " blocks  (" << not_cached_blocks << " are not cached)"
            << " to clear old ones from ZooKeeper. This might take several minutes.");

    /// Time -> block hash from ZooKeeper (from node name)
    using TimedBlock = std::pair<Int64, String>;
    using TimedBlocksComparator = std::greater<TimedBlock>;
    std::vector<TimedBlock> timed_blocks;

    for (const String & block : blocks)
    {
        auto it = cached_block_ctime.find(block);

        if (it == cached_block_ctime.end())
        {
            /// New block. Fetch its stat and put it into the cache
            zkutil::Stat block_stat;
            zookeeper->exists(storage.zookeeper_path + "/blocks/" + block, &block_stat);
            cached_block_ctime.emplace(block, block_stat.ctime);
            timed_blocks.emplace_back(block_stat.ctime, block);
        }
        else
        {
            /// Cached block
            timed_blocks.emplace_back(it->second, block);
        }
    }

    if (timed_blocks.empty())
        return;

    std::sort(timed_blocks.begin(), timed_blocks.end(), TimedBlocksComparator());

    /// Use ZooKeeper's first node (last according to time) timestamp as "current" time.
    Int64 current_time = timed_blocks.front().first;
    Int64 time_threshold = std::max(0L, current_time - static_cast<Int64>(storage.data.settings.replicated_deduplication_window_seconds));
    TimedBlock block_threshold(time_threshold, "");

    size_t current_deduplication_window = std::min(timed_blocks.size(), storage.data.settings.replicated_deduplication_window);
    auto first_outdated_block_fixed_threshold = timed_blocks.begin() + current_deduplication_window;
    auto first_outdated_block_time_threshold = std::upper_bound(timed_blocks.begin(), timed_blocks.end(), block_threshold, TimedBlocksComparator());
    auto first_outdated_block = std::min(first_outdated_block_fixed_threshold, first_outdated_block_time_threshold);

    for (auto it = first_outdated_block; it != timed_blocks.end(); ++it)
    {
        /// TODO After about half a year, we could replace this to multi op, because there will be no obsolete children nodes.
        zookeeper->removeRecursive(storage.zookeeper_path + "/blocks/" + it->second);
        cached_block_ctime.erase(it->second);
    }

    LOG_TRACE(log, "Cleared " << timed_blocks.end() - first_outdated_block << " old blocks from ZooKeeper");
}

}
