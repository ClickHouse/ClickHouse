#pragma once

#include <Coordination/Storage/Common.h>

#include <memory>

namespace DB
{
class WriteBuffer;
}

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
    void listChildrenNames(NodePath range_start, NodePath range_end, UInt128 parent_path_hash, ChildrenSet2 & out, DB::Arena & arena, BlockCache * block_cache) const;

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

/// WriteBuffer wrapper that just passes data through to a given other WriteBuffer, without calling
/// destructor or finalize() on it. (Because for some reason compressing WriteBuffer implementations
/// insist on calling finalize() on the target buffer even if it's passed by plain pointer.)
class AppendWriteBuffer : public DB::WriteBuffer
{
public:
    WriteBuffer * out = nullptr;

    explicit AppendWriteBuffer(WriteBuffer * out_);
    void nextImpl() override;

    void flush() { out->position() = position(); }

    void finalizeImpl() override;
    ~AppendWriteBuffer() override;
};

/// Writes to a series of files and to block cache. Used by flushes and merges.
/// Output files have `delete_when_destroyed = true`; the caller should set it to false when
/// publishing a finished file.
struct SortedRunWriter
{
    SortedRunPtr sorted_run;

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
    StorageState * storage = nullptr;
    size_t target_block_size = 0;
    size_t target_block_group_compressed_size = 0;
    size_t target_file_uncompressed_size = 0;

    /// Current file, not added to sorted_run yet.
    SortedFilePtr file;
    /// Paths that have children added or removed in current `file`. For `parent_paths_filter`.
    std::vector<UInt128> parent_paths;

    /// Current block, not added to `file` yet.
    BlockPtr block;
    NodePath block_min_path;
    NodePath block_max_path;
    std::string block_max_path_buf;

    /// If nonzero, `block_max_path.str()[0:last_added_parent_path_len]` was added to `parent_paths`.
    size_t last_added_parent_path_len = 0;

    /// Current group of blocks. New `compressed_writer` is created for each group.
    /// Its blocks were added to `file` and were written to `compressed_writer`, but
    /// `group_compressed_size` is not known yet.
    std::optional<AppendWriteBuffer> file_appender; // awful adapter to make ZstdDeflatingWriteBuffer work
    std::unique_ptr<DB::WriteBuffer> compressed_writer;
    size_t group_start_block_idx = 0;
    size_t group_offset_in_file = 0;

    std::unique_ptr<DB::WriteBuffer> file_writer; // writes to `file`; null in memory-only mode

    void finishBlock();
    void finishGroup();
    void finishFile();

    void buildParentPathsFilter();
};

}
