#pragma once

#include <Core/Block_fwd.h>
#include <Core/Joins.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Processors/IProcessor.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <QueryPipeline/SizeLimits.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

namespace DB
{

class TemporaryDataOnDiskScope;
using TemporaryDataOnDiskScopePtr = std::shared_ptr<TemporaryDataOnDiskScope>;
class TemporaryBlockStreamHolder;
class TemporaryBlockStreamReaderHolder;

/// Whether the result still depends on which build rows matched once the probe phase is over:
/// `RIGHT` and `FULL` emit the build rows that matched nothing, and a right-driven `SEMI`/`ANTI`
/// result is made of build rows selected by whether they matched at all. The other kinds decide
/// every output row while the probe row that produced it is still at hand, so they keep no flags.
bool needsBuildSideMatchFlags(JoinKind kind, JoinStrictness strictness);

/// Whether a stage after the probe phase emits the build rows that no probe row matched, padded
/// with the probe side's column defaults.
bool keepsUnmatchedBuildRows(JoinKind kind, JoinStrictness strictness);

/// A build block ready for matching. Held by shared pointer because a block that was compressed or
/// spilled is decompressed or read back into one that only lives as long as the output rows still
/// pointing into it.
using BuildBlockPtr = std::shared_ptr<const StoredBlock>;

/// How the materialized build side is kept as it grows: compressed in memory, then streamed to disk.
struct BlockNestedLoopStoreSettings
{
    /// A stored block is compressed once the build side has passed either threshold, from
    /// `cross_join_min_rows_to_compress` and `cross_join_min_bytes_to_compress`.
    size_t min_rows_to_compress = 0;
    size_t min_bytes_to_compress = 0;
    /// The build side is streamed to disk once what it holds in memory passes this, from
    /// `max_bytes_before_external_join`; 0 leaves spilling to the query memory tracker alone.
    size_t max_bytes_in_memory = 0;
    /// Where the spilled blocks go. Without it the build side stays in memory whatever its size.
    TemporaryDataOnDiskScopePtr tmp_data;
};

/// The materialized build side of a block nested loop join, shared by every build and probe stream.
/// The build streams append to it concurrently until `finish`; from then on it is read-only, and
/// only then are the stored blocks and their global row numbering observable.
///
/// A stored block may be compressed, or written to a temporary file and dropped from memory. Its
/// row count is kept either way, so the global row numbering - and with it the indexing of the
/// match flags - does not change when a block moves out of memory. Blocks are read back through a
/// `BuildSideBlockReader`.
class BlockNestedLoopJoinData
{
public:
    BlockNestedLoopJoinData(
        SharedHeader build_header_,
        JoinKind kind_,
        JoinStrictness strictness_,
        const SizeLimits & size_limits_,
        BlockNestedLoopStoreSettings store_settings_ = {});
    ~BlockNestedLoopJoinData();

    /// Appends one build block; `num_rows` is authoritative, because a block with no columns still
    /// has rows. Thread-safe. Returns false when the size limits are exceeded under
    /// `join_overflow_mode = 'break'`, asking the caller to stop reading the build side.
    bool addBlock(Block block, size_t num_rows);

    /// Records the build side's `WITH TOTALS` row. It is not a build row: it never takes part in
    /// matching, it only contributes its columns to the joined totals row.
    void setBuildSideTotals(Block totals);
    /// The build side's totals row, or a block with no columns when it has none.
    /// Valid only after `finish`.
    const Block & getBuildSideTotals() const;

    /// Ends the build phase: assigns the global row numbers and makes the store read-only.
    /// Must be called exactly once, after every build stream is done appending.
    void finish();
    bool isFinished() const { return finished.load(std::memory_order_acquire); }

    /// The number of stored build blocks, and the rows of one of them. Both are known without
    /// reading the block back, so a consumer can decide to skip a block without paying for it.
    /// Valid only after `finish`.
    size_t getNumBlocks() const;
    size_t getBlockNumRows(size_t index) const;
    /// Global row number of the first row of block `i`, with a trailing entry equal to
    /// `getTotalRows()`. A global row number identifies a build row for the whole probe phase and
    /// stays stable when a block is moved out of memory. Valid only after `finish`.
    const std::vector<size_t> & getRowOffsets() const;

    /// Whether `BuildSideBlockReader::read` hands out the stored block itself rather than a copy of
    /// it. A compressed or spilled block is materialized anew on every read, so a consumer that
    /// keeps one alive holds memory neither the store nor its spilling accounts for.
    /// Valid only after `finish`.
    bool isBlockSharedInMemory(size_t index) const;

    /// Whether the build side can be moved out of memory at all. A build side of columnless rows
    /// cannot: the `Native` format has no way to persist a bare row count.
    bool canSpill() const;
    /// What the build side holds in memory right now, and the largest single block of it.
    size_t getInMemoryBytes() const { return in_memory_bytes.load(std::memory_order_relaxed); }
    size_t getMaxInMemoryBlockBytes() const { return max_in_memory_block_bytes.load(std::memory_order_relaxed); }
    size_t getNumSpilledBlocks() const { return num_spilled_blocks.load(std::memory_order_relaxed); }
    /// Streams every block that is still in memory out to the temporary file, and keeps the build
    /// side streaming from then on. Returns false when there is less than `min_bytes` to gain.
    /// Thread-safe, and called by the query memory tracker through the build transform.
    bool spillInMemoryBlocks(size_t min_bytes);

    /// Whether the match flags below are kept at all; decided by the kind and strictness.
    bool hasBuildSideMatchFlags() const { return needs_match_flags; }
    /// Records that some probe row matched the build row `global_row`. Called by every probe stream
    /// concurrently, only for a kind that keeps the flags.
    void setBuildRowMatched(size_t global_row);
    /// Takes the build row `global_row` for the caller, if no probe stream has taken it yet. This is
    /// how a right-driven `ANY`/`SEMI` gives a build row to exactly one probe row.
    bool claimBuildRow(size_t global_row);
    /// Whether any probe row matched the build row `global_row`. Meaningful only once every probe
    /// stream has finished.
    bool isBuildRowMatched(size_t global_row) const;

    size_t getTotalRows() const { return total_rows.load(std::memory_order_relaxed); }
    size_t getTotalBytes() const { return total_bytes.load(std::memory_order_relaxed); }

    const SharedHeader & getHeader() const { return build_header; }
    JoinKind getKind() const { return kind; }
    JoinStrictness getStrictness() const { return strictness; }

private:
    friend class BuildSideBlockReader;

    /// One stored build block: in memory, possibly compressed, or in the temporary file at
    /// `spill_ordinal`. `num_rows` is known either way.
    struct BuildBlockEntry
    {
        BuildBlockPtr block;
        size_t num_rows = 0;
        bool compressed = false;
        /// Position of the block in the temporary file; meaningful only when `block` is null.
        size_t spill_ordinal = 0;
    };

    void assertFinished(const char * what) const;

    /// Keeps the block in memory, compressing it if the build side has grown past the thresholds.
    void storeBlock(BuildBlockEntry & entry, size_t index, StoredBlock stored_block, size_t rows_in_join, size_t bytes_in_join)
        TSA_REQUIRES(mutex);
    /// Writes the block out and leaves `entry` pointing at it by its position in the file. Blocks
    /// are written in increasing index order, which is what makes one forward pass enough to read
    /// any of them back.
    void spillBlock(BuildBlockEntry & entry, const StoredBlock & stored_block) TSA_REQUIRES(mutex);
    /// Writes out every block that is still in memory, in index order.
    void spillInMemoryBlocksLocked() TSA_REQUIRES(mutex);

    /// The entry of block `index`. Needs no lock: the store is read-only by the time it is called.
    const BuildBlockEntry & getBlockEntry(size_t index) const;
    /// A fresh sequential reader over the spilled blocks, positioned at the first of them.
    TemporaryBlockStreamReaderHolder createSpillReadStream() const;

    const SharedHeader build_header;
    const JoinKind kind;
    const JoinStrictness strictness;
    const SizeLimits size_limits;
    const BlockNestedLoopStoreSettings store_settings;
    const bool needs_match_flags;

    mutable std::mutex mutex;
    std::vector<BuildBlockEntry> blocks TSA_GUARDED_BY(mutex);
    std::vector<size_t> row_offsets TSA_GUARDED_BY(mutex);
    Block build_side_totals TSA_GUARDED_BY(mutex);

    /// Exists from the first spill on; every later block is written to it as well, so that the
    /// build side never goes back to growing in memory once it has proved too large for it.
    std::unique_ptr<TemporaryBlockStreamHolder> tmp_stream TSA_GUARDED_BY(mutex);

    /// One flag per build row, indexed by global row number; allocated by `finish` and only for the
    /// kinds that need it.
    ///
    /// Relaxed is enough for the accesses themselves: a flag is only ever set, never cleared, so no
    /// write can be lost, and the happens-before edge that makes every write visible to the
    /// unmatched scan comes from the pipeline instead - a probe stream finishes its output port
    /// only after its last write, and `DelayedPortsProcessor` observes that finish before it lets
    /// the unmatched stage produce a single row. `finished` orders the allocation itself the same
    /// way it orders `blocks` and `row_offsets`.
    std::unique_ptr<std::atomic_bool[]> matched_flags;

    std::atomic<size_t> total_rows{0};
    /// What the build side would occupy in memory as a whole, spilled blocks included; this is what
    /// `max_bytes_in_join` limits, so that spilling does not quietly raise the limit.
    std::atomic<size_t> total_bytes{0};
    std::atomic<size_t> in_memory_bytes{0};
    std::atomic<size_t> max_in_memory_block_bytes{0};
    std::atomic<size_t> num_spilled_blocks{0};
    std::atomic<bool> finished{false};
};

using BlockNestedLoopJoinDataPtr = std::shared_ptr<BlockNestedLoopJoinData>;

/// Reads the stored build blocks back, one at a time and in index order. Spilling is sequential, so
/// the temporary file can only be read forward; a reader asked for an earlier block starts the file
/// over, which is what the probe does for every probe chunk. Not thread-safe: each probe stream and
/// each unmatched scan owns one.
class BuildSideBlockReader
{
public:
    explicit BuildSideBlockReader(BlockNestedLoopJoinDataPtr data_);
    ~BuildSideBlockReader();

    /// The block at `index`, decompressed or read back from disk as needed. It stays alive for as
    /// long as the returned pointer is held, which is what lets accumulated output rows point into
    /// a block the walk has already moved past.
    BuildBlockPtr read(size_t index);

private:
    BuildBlockPtr readSpilledBlock(size_t index, size_t spill_ordinal);

    BlockNestedLoopJoinDataPtr data;
    std::unique_ptr<TemporaryBlockStreamReaderHolder> spill_stream;
    /// Position of the file reader: the ordinal of the next spilled block it will hand out.
    size_t next_spill_ordinal = 0;
    /// The block handed out last, kept because one block is walked over several tiles.
    BuildBlockPtr current;
    size_t current_index = 0;
};

/// Fills `BlockNestedLoopJoinData` with the build side. Carries no data downstream: its output port
/// has an empty header and is finished once the whole build side is stored, which is how the probe
/// side learns that it may start.
class BlockNestedLoopBuildTransform final : public IProcessor
{
public:
    BlockNestedLoopBuildTransform(SharedHeader input_header, BlockNestedLoopJoinDataPtr data_, FinishCounterPtr finish_counter_);

    String getName() const override { return "BlockNestedLoopBuild"; }

    /// Routes the build side's `WITH TOTALS` row into the store. Only one build stream may carry
    /// it, so a pipeline with build-side totals uses a single build stream.
    InputPort * addTotalsPort();

    Status prepare() override;
    void work() override;

    ProcessorMemoryStats getMemoryStats() override;
    bool spillOnSize(size_t bytes) override;

private:
    /// Counts this stream out of the build phase, closing the store when it is the last one.
    void finishBuild();

    BlockNestedLoopJoinDataPtr data;
    FinishCounterPtr finish_counter;
    Chunk chunk;
    bool stop_reading = false;
    bool for_totals = false;
    bool build_finished = false;
};

}
