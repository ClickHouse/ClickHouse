#pragma once

#include <Coordination/Storage/BlockCache.h>
#include <Coordination/Storage/NodeRefCache.h>
#include <Coordination/Storage/SortedFile.h>
#include <Coordination/Storage/SortedRun.h>
#include <Coordination/Storage/Memtable.h>
#include <Common/SharedMutex.h>
#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>

namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

struct AsynchronousMetricValue;
using AsynchronousMetricValues = std::unordered_map<std::string, AsynchronousMetricValue>;
}

namespace Coordination::Storage
{

struct BackgroundWork;

/// Owns and manages all the node storage things: committed and uncommitted state, background
/// flushes and merges.
struct StorageState
{
    struct UncommittedMemtable
    {
        MemtablePtr memtable;
        NodeHashMap<NodeRef> nodes;
        /// Zxid of the latest appended node. When commit point reaches this zxid, this memtable can
        /// be deleted. May be overestimated if request with this zxid was rolled back; that's ok.
        int64_t max_zxid = 0;
    };

    DB::KeeperContextPtr keeper_context;
    LoggerPtr log;

    /// If true, don't do file IO and keep SortedFile blocks pinned in memory.
    bool memory_only = true;

    /// In memory-only mode stays null (blocks are owned by SortedFile-s instead).
    std::unique_ptr<BlockCache> block_cache;

    /// IO environment used for all file reads and writes. `disk` is assigned in startup and
    /// stays null in memory-only mode. Files live at the root of the disk.
    DB::DiskPtr disk;
    DB::ReadSettings read_settings;
    DB::WriteSettings write_settings;

    /// Protects committed state (`files`, `{mutable,immutable}_memtables`, `node_cache`, etc).
    DB::SharedMutex * storage_mutex = nullptr;

    /// Files and memtables containing committed nodes, listed in chronological order.
    /// All memtables come after all files.
    /// In this order, each path's sequence of NodeAction-s forms a valid history,
    /// e.g. [Create, Update, Remove, Create], never e.g. [Create, Create] or [Remove, ...].
    ///
    /// E.g. to find a node (without using the `node_cache` hash map), you'd need to search
    /// mutable_memtable, then immutable_memtables in reverse, then sorted_runs in reverse, stopping
    /// when the node (or its NodeAction::Remove tombstone) is found.
    ///
    /// When a merge is in progress, this may contain both a partial merge output (prefix) and
    /// truncated merge inputs (suffixes), with overlapping file_seqno ranges but nonoverlapping
    /// NodePath ranges. The merge is careful to keep all such published intermediate results
    /// consistent, such that the above paragraphs are always true, so readers don't need to think
    /// about merges.
    std::vector<SortedRunPtr> sorted_runs;
    std::vector<MemtablePtr> immutable_memtables;
    MemtablePtr mutable_memtable; // may be nullptr

    uint32_t next_file_seqno = 1;

    /// Latest occurrence of each node in files and memtables. Doesn't contain removed nodes.
    NodeRefCache node_cache;

    /// Uncommitted state, as an overlay on top of committed state.
    /// Contains all uncommitted changes and some recently committed changes (i.e. overlaps committed state).
    /// To find a node, search in these UncommittedMemtable-s in reverse, then in committed state
    /// if not found.
    /// Similarly to regular memtables, we create a new one when the latest one gets big enough.
    /// But these memtables are never flushed to files; instead, a memtable is simply deleted when
    /// its max_zxid gets committed. This vector usually has two elements.
    std::vector<UncommittedMemtable> uncommitted;

    /// Sum of Memtable::total_bytes across `uncommitted`. Duplicates information that's already
    /// in `uncommitted`, as an atomic, so that fillAsynchronousMetrics can read it without
    /// holding the mutex that protects uncommitted state.
    std::atomic<size_t> uncommitted_bytes{};

    std::unique_ptr<BackgroundWork> background;

    /// How long to sleep before each write, in microseconds.
    std::atomic<int64_t> write_throttling_us{};

    explicit StorageState(DB::KeeperContextPtr keeper_context_, DB::SharedMutex * storage_mutex_);
    ~StorageState(); // calls shutdown()

    /// Start and stop background threads.
    void startup();
    void shutdown();

    /// ========== Operations on committed state. ==========

    /// Caller must hold storage_mutex in shared mode (for const methods) or exclusive mode (for non-const).

    /// Node lookup in committed state.
    ///
    /// Returns NodeRef with action == Remove and block == nullptr if the node doesn't exist.
    /// If the node's block was evicted from the block cache, reloads it (through `block_cache`)
    /// and re-points the affected `node_cache` entries at the newly loaded block.
    NodeRef getCommittedNode(const NodePathWithHash & path) const;

    NodeRef appendCommittedNode(FullNode & node);

    /// Get children. The caller should ignore set entries with action == Remove.
    ///
    /// (Why not also have a function that outputs children's FullNode-s or at least NodeRef-s
    ///  along the way, instead of just names? After all, SortedFile has to iterate over whole nodes
    ///  just to extract their names; can't it read FullNode-s along the way? That would be
    ///  incorrect because when a node is Update-d by a memtable it's not added to the memtable's
    ///  children sets, so the FullNode we get from SortedFile may be outdated, even though its name
    ///  is still correct. We could parse a FullNode from SortedFile along the way, but then do a
    ///  NodeRefCache lookup, but it's unclear whether this is worth the trouble.)
    void listCommittedChildrenNames(const NodePathWithHash & path, ChildrenSet2 & out, DB::Arena & arena) const;

    /// Very minimal stats. Caller must hold storage_mutex.
    void getNodeCountAndDataSize(uint64_t & out_node_count, uint64_t & out_data_size) const;

    /// Various metrics. Caller must hold storage_mutex.
    void fillAsynchronousMetrics(DB::AsynchronousMetricValues & new_values) const;

    /// ========== Operations on uncommitted state. ==========

    /// May be called in parallel with operations on committed state, but not in parallel with each other.
    /// (I.e. caller should hold some mutex protecting uncommitted state.)
    /// Caller must *not* hold storage_mutex (these methods may lock+unlock it).

    /// Node lookup in committed+uncommitted state.
    /// Locks storage_mutex (shared) for committed state lookup if the node is not found in
    /// uncommitted state.
    NodeRef getUncommittedNode(const NodePathWithHash & path);

    NodeRef appendUncommittedNode(FullNode & node, int64_t zxid);

    /// Call periodically to remove obsolete UncommittedMemtable-s.
    void cleanupUncommittedState(int64_t committed_zxid);

    void listUncommittedChildrenNames(const NodePathWithHash & path, ChildrenSet2 & out, DB::Arena & arena) const;

    /// Sleeps for a short time if background work fell behind.
    /// Call before each write, with storage_mutex unlocked.
    void throttleWrite() const;

    /// TODO: listRecursive of some kind. It can be faster than normal tree traversal because
    ///       SortedFile can just do one range scan per depth. But it seems very tricky because
    ///       memtable can only list children of one node at a time, and also uncommitted state may
    ///       remove children (which may've been listed as part of SortedFile scan).

    /// ========== Private methods ==========

    /// Call when memtables or sorted runs were added or removed, with storage_mutex held.
    void recalculateWriteThrottling();

    /// Generates a unique file name for a new SortedFile. We don't rely on the file name format
    /// anywhere, the min_file_seqno/max_file_seqno/file_idx_in_run are just for debugging
    /// convenience; this function could equally well return a random string.
    std::string makeSortedFilePath(uint32_t min_file_seqno, uint32_t max_file_seqno, size_t file_idx_in_run) const;

    /// On startup we write a mostly useless file and read it back, to fail early if the disk
    /// doesn't work (e.g. no directory) or doesn't support positioned reads (readBigAt).
    /// Otherwise the problem would only be reported by background thread after filling a memtable
    /// and flushing it, potentially a long time after startup.
    void writeInfoFile();
};

}
