#pragma once

#include <Coordination/Storage/Common.h>

namespace Coordination::Storage
{

struct SortedFile;
using SortedFilePtr = std::shared_ptr<SortedFile>;
struct SortedRun;
using SortedRunPtr = std::shared_ptr<SortedRun>;

/// Sequence of sorted files with disjoint key (NodePath) ranges.
/// May consist of 0 files.
/// (We store a sorted run in multiple files to allow merges to incrementally publish outputs and
///  delete/evict inputs, to avoid 2x space/memory usage. Important in memory-only mode.)
struct SortedRun
{
    std::vector<SortedFilePtr> files;

    /// Ignore nodes with paths <= this (they've been merged into other SortedRun-s). nullopt means
    /// no cutoff, read everything. When set, points into min_path_buf.
    std::optional<NodePath> min_path_cutoff;
    std::string min_path_buf;

    /// Position in the chronological order of files and memtables. A sorted run covers a contiguous
    /// range of seqnos: a flushed run inherits the file_seqno of its memtable, a merged run covers
    /// the union of the (consecutive) ranges of its sources.
    uint32_t min_file_seqno = 0;
    uint32_t max_file_seqno = 0;

    size_t total_block_size = 0;
    size_t total_file_size = 0;

    /// How many nodes (+1) and tombstones (-1) this run "contributes" to the total.
    /// For files not involved in a merge, this is just the sum of node_count_delta in `files`.
    /// For live merges, the count may be distributed across inputs and outputs arbitrarily, it's
    /// only guaranteed that adding this up across all files and memtables gives correct node count.
    int64_t node_count_delta = 0;

    SortedRun() = default;
    SortedRun & operator=(const SortedRun &) = delete;

    /// === Reading ===

    /// Find and load the block that may contain the given path. nullptr if not found.
    BlockPtr getBlockCoveringPath(NodePath path, BlockCache * block_cache) const;

    /// Scan the nodes with range_start < path < range_end (both bounds exclusive) and report the
    /// last component of each path. The caller passes the range that selects exactly the direct
    /// children of some node.
    void listChildrenNames(NodePath range_start, NodePath range_end, ChildrenSet2 & out, DB::Arena & arena, BlockCache * block_cache) const;

    /// === Writing and merging ===

    SortedRun(uint32_t min_file_seqno_, uint32_t max_file_seqno_);

    /// Copies the metadata and the array of SortedFilePtr-s (sharing the same SortedFile-s), so the
    /// copy can be published while the original keeps being modified.
    SortedRunPtr shallowCopy() const;

    void setMinPathCutoff(std::optional<NodePath> new_cutoff);

private:
    SortedRun(const SortedRun &) = default; // for shallowCopy()
};

struct StorageState;

/// Writes to a series of files and to block cache. Used by flushes and merges.
struct SortedRunWriter
{
    StorageState * storage = nullptr;
    size_t target_block_size = 0;
    size_t target_file_uncompressed_size = 0;
    std::string file_path_prefix;

    SortedRunPtr sorted_run;

    SortedFilePtr file; // not added to sorted_run yet

    BlockPtr block; // not added to `file` yet
    NodePath block_min_path;
    NodePath block_max_path;
    std::string block_max_path_buf;

    SortedRunWriter(SortedRunPtr sorted_run_, StorageState * storage_);
    ~SortedRunWriter();

    /// Basic usage:
    ///   while (...) {
    ///     writer->appendNode(...);
    ///     writer->finishFileIfBigEnough();
    ///   }
    ///   result = writer->finish();

    void appendNode(FullNode & node);

    /// Returns true if a file was added to sorted_run, and sorted_run contains all nodes appended
    /// so far.
    bool finishFileIfBigEnough();

    /// If not called, all produced files are deleted in destructor.
    SortedRunPtr finish();

private:
    void finishBlock();
    void finishFile();
};

}
