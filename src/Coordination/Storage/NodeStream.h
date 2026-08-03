#pragma once

#include <Coordination/Storage/SortedRun.h>
#include <Coordination/Storage/Memtable.h>
#include <Coordination/Storage/Node.h>
#include <Common/HashTable/HashSet.h>

#include <queue>

namespace Coordination::Storage
{

/// Produces a sequence of nodes one at a time.
/// Mainly needed for merging sorted runs: multiple SortedRunNodeStream-s feed into a MergingNodeStream.
/// Call next() to get the first node (or at_end == true), then to get each successive node.
struct NodeStream
{
    FullNode node; // valid if next() was called at least once and at_end is false
    bool at_end = false;

    NodeStream() = default;
    NodeStream(const NodeStream &) = default;
    NodeStream & operator=(const NodeStream &) = default;
    virtual ~NodeStream() = default;

    /// Either sets `at_end = true` or populates `node`.
    /// Invalidates previous `node` (and its `path` and `data`).
    virtual void next() = 0;
};

/// Reads from a series of files. Used by merges.
struct SortedRunNodeStream final : public NodeStream
{
private:
    SortedRunPtr sorted_run;
    BlockCache * block_cache = nullptr;

    size_t file_idx = 0; // where `node` is
    uint32_t next_block_idx = 0; // within the current file
    uint32_t offset = 0;
    BlockPtr block; // current block, if loaded
    std::string path_buf;

public:
    SortedRunNodeStream(SortedRunPtr sorted_run_, BlockCache * block_cache_);

    void next() override;

    /// Removes files that were fully consumed by this iterator (i.e. current `node` is in a later
    /// file) from sorted_run.
    void dropConsumedFiles(std::vector<SortedFilePtr> & dropped);
};

/// Iterates over Memtable in sorted order, by expensively listing and pre-sorting all nodes in advance.
/// Used by flush.
struct MemtableSortedNodeStream final : public NodeStream
{
private:
    struct NodeInfo
    {
        NodePath path;
        uint32_t block_idx = 0;
        uint32_t offset = 0;
        /// Node's index inside memtable. Nodes with equal path get combineActions-ed in order of
        /// increasing idx.
        uint32_t idx = 0;

        /// Compares <path, idx>.
        bool operator<(const NodeInfo & rhs) const;
    };

    MemtablePtr memtable;
    DB::Arena arena; // for paths
    std::vector<NodeInfo> sorted_nodes;
    size_t next_idx = 0;

public:
    /// Does the sorting, slow.
    explicit MemtableSortedNodeStream(MemtablePtr memtable_);

    void next() override;

    /// Peek the next node (in iteration order) and return true if its path is equal to `node.path`.
    bool isNextPathEqualToCurrent() const;
};

/// Iterates over Memtable in reverse order. Used by snapshotting.
struct MemtableReversedNodeStream final : public NodeStream
{
private:
    MemtablePtr memtable;
    /// We load one block at a time (in reverse order), iterate over the block to get node offsets,
    /// then iterate over those offsets in reverse and read the nodes.
    uint32_t block_idx = 0;
    std::vector<uint32_t> offsets; // node offsets in current block
    size_t idx = 0; // in offsets
    std::string path_buf;

public:
    explicit MemtableReversedNodeStream(MemtablePtr memtable_);

    void next() override;
};

/// Merges multiple sorted NodeStream-s into one.
/// Paths in each stream must be strictly increasing (no duplicates).
/// Nodes with equal paths from different streams are collapsed using combineActions.
struct MergingNodeStream final : public NodeStream
{
private:
    struct Substream
    {
        NodeStream * stream = nullptr;
        size_t idx = 0;

        bool operator<(const Substream & rhs) const; // for priority_queue
    };

    std::priority_queue<Substream> pq; // TODO: Something faster: heap that supports decreasing top key, or tournament tree
    /// Our `node` is copied from this substream's current `node`.
    std::optional<Substream> pinned_substream;
    size_t next_idx = 0;

    void advanceAndPush(Substream s);

public:
    /// Caller must add all substreams, in chronological order, before starting iteration.
    /// If no substreams are added, the first next() call will set at_end = true.
    void addSubstream(NodeStream * s);

    /// After next() call, one of the substreams is positioned on our `node`, other substreams are
    /// positioned on exactly the first node with higher path than our `node`.
    void next() override;
};

/// Iterates over a snapshot of the set of all committed nodes. Deduplicates/collapses nodes, so we
/// write out just the set of nodes with no NodeAction nonsense. Doesn't hold storage_mutex, except
/// during construction. Tries to not use a lot of memory.
/// The output is not fully sorted by path, but usually has a long sorted run, which snapshot reader
/// can then heuristically detect and put into a SortedRun instead of a Memtable.
struct SnapshotWriterNodeStream : public NodeStream
{
    BlockCache * block_cache = nullptr;

    std::vector<SortedRunPtr> sorted_runs;
    std::vector<MemtablePtr> memtables;

    HashSet<NodePathHash, UInt128TrivialHash> memtable_paths;

    /// First we read all memtables in reverse, deduplicating using memtable_paths set, and freeing
    /// finished memtables as we go.
    /// Then we merge all files the same way as background merges (but not writing output to files)
    /// and skip paths that were already seen in memtables (memtable_paths).
    /// (Alternatively, we could just merge everything, including memtables (MemtableSortedNodeStream),
    ///  but that would use more memory, and memory usage seems important here.)
    std::optional<MemtableReversedNodeStream> memtable_stream;
    std::vector<SortedRunNodeStream> run_streams;
    std::optional<MergingNodeStream> merging_stream;

    /// Caller must hold storage_mutex in shared or exclusive mode.
    explicit SnapshotWriterNodeStream(const StorageState & storage);

    size_t getNodeCount() const;

    void next() override;
};

}
