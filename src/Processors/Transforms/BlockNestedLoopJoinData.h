#pragma once

#include <Columns/IColumn.h>
#include <Core/Block_fwd.h>
#include <Core/Joins.h>
#include <Processors/IProcessor.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <QueryPipeline/SizeLimits.h>

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace DB
{

class TemporaryDataOnDiskScope;
using TemporaryDataOnDiskScopePtr = std::shared_ptr<TemporaryDataOnDiskScope>;
class TemporaryBlockStreamHolder;
using TemporaryBlockStreamHolderPtr = std::unique_ptr<TemporaryBlockStreamHolder>;
class TemporaryBlockStreamReaderHolder;

/// Whether the result still depends on which build rows matched once the probe phase is over:
/// `RIGHT` and `FULL` emit the build rows that matched nothing, and a right-driven `SEMI`/`ANTI`
/// result is made of build rows selected by whether they matched at all. The other kinds decide
/// every output row while the probe row that produced it is still at hand, so they keep no flags.
bool needsBuildSideMatchFlags(JoinKind kind, JoinStrictness strictness);

/// Whether a stage after the probe phase emits the build rows that no probe row matched, padded
/// with the probe side's column defaults.
bool keepsUnmatchedBuildRows(JoinKind kind, JoinStrictness strictness);

/// Whether the match flags, where they are kept, end up set for every build row that satisfied the
/// condition with some probe row - which is what makes counting them the build side's `matched`
/// number in `EXPLAIN ANALYZE`.
bool buildSideMatchFlagsCountEveryMatch(JoinKind kind, JoinStrictness strictness);

/// A build block ready for matching. The store never scatters a block, so a row's index into the
/// columns is also its position in the block - the equality the match flags and the
/// unmatched-build-rows scan are both indexed by.
struct BuildBlock
{
    Columns columns;
    size_t num_rows = 0;
    /// The block's position in the store, which the global row numbering is built from.
    size_t index = 0;

    size_t allocatedBytes() const;
};

/// Held by shared pointer because a block that was compressed or spilled is decompressed or read
/// back into one that only lives as long as the output rows still pointing into it.
using BuildBlockPtr = std::shared_ptr<const BuildBlock>;

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
    /// The store opens a child scope of its own on top of it, so that what it writes is accounted
    /// as a join's temporary data rather than as the query's in general.
    TemporaryDataOnDiskScopePtr tmp_data;
    /// How the spilled blocks are written, from `temporary_files_codec` and the buffer size the
    /// other joins use for theirs.
    String temporary_files_codec;
    UInt64 temporary_files_buffer_size = 0;
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
        BlockNestedLoopStoreSettings store_settings_ = {},
        size_t num_build_streams_ = 1);
    ~BlockNestedLoopJoinData();

    /// Appends one build block; `num_rows` is authoritative, because a block with no columns still
    /// has rows. Thread-safe. Returns false when the size limits are exceeded under
    /// `join_overflow_mode = 'break'`, asking the caller to stop reading the build side.
    /// `stream_index` names the calling build stream, which owns the temporary file its blocks go to;
    /// no two build streams may share one.
    bool addBlock(Block block, size_t num_rows, size_t stream_index);

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

    /// The materialized form of a compressed or spilled block, if some reader still holds one. Every
    /// probe stream walks the same blocks, so without this each of them would materialize the whole
    /// build side for itself.
    BuildBlockPtr findMaterializedBlock(size_t index) const;
    /// Publishes a block a reader has just materialized and returns what readers will use for that
    /// index from now on - the block itself, or the one another reader published first, in which case
    /// the caller's copy is the one to drop. The most recently published blocks are also held here
    /// past the reader that made them, bounded by `MAX_MATERIALIZED_WINDOW_BYTES`, so that a stream
    /// lagging behind the others still finds them.
    BuildBlockPtr publishMaterializedBlock(size_t index, BuildBlockPtr block);

    /// Whether the build side can be moved out of memory at all. A build side of columnless rows
    /// cannot: the `Native` format has no way to persist a bare row count.
    bool canSpill() const;
    /// What the build side holds in memory right now, and the largest single block of it.
    size_t getInMemoryBytes() const { return in_memory_bytes.load(std::memory_order_relaxed); }
    size_t getMaxInMemoryBlockBytes() const { return max_in_memory_block_bytes.load(std::memory_order_relaxed); }
    size_t getNumSpilledBlocks() const { return num_spilled_blocks.load(std::memory_order_relaxed); }
    /// Streams every block that is still in memory out to the temporary file of `stream_index`, and
    /// keeps the build side streaming from then on. Returns false when there is less than `min_bytes`
    /// to gain. Thread-safe, and called by the query memory tracker through the build transform.
    bool spillInMemoryBlocks(size_t min_bytes, size_t stream_index);

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

    /// What `EXPLAIN ANALYZE` reports about the build side beyond its row count: how many of its rows
    /// some probe row matched, the peak it occupied in memory, whether any of it was compressed, and
    /// what it wrote to disk.
    /// The count of matched rows is `nullopt` where the match flags answer nothing, and is meaningful
    /// only once every probe stream has finished - the same point at which the flags become readable.
    std::optional<UInt64> countMatchedBuildRows() const;
    size_t getPeakInMemoryBytes() const { return peak_in_memory_bytes.load(std::memory_order_relaxed); }
    bool hasCompressedBlocks() const { return has_compressed_blocks.load(std::memory_order_relaxed); }
    size_t getSpilledCompressedBytes() const;

    const SharedHeader & getHeader() const { return build_header; }
    JoinKind getKind() const { return kind; }
    JoinStrictness getStrictness() const { return strictness; }

private:
    friend class BuildSideBlockReader;

    /// One stored build block: in memory, possibly compressed, or in the temporary file of
    /// `sink_index`, at `spill_ordinal` in it. `num_rows` is known either way.
    struct BuildBlockEntry
    {
        BuildBlockPtr block;
        size_t num_rows = 0;
        bool compressed = false;
        /// Which temporary file holds the block, and where in that file - the position is assigned by
        /// `finish`. Both are meaningful only when `block` is null.
        size_t sink_index = 0;
        size_t spill_ordinal = 0;
    };

    /// What the store hands out once the build phase is over: the build phase's own members, moved
    /// here by `finish`, plus the row numbering it computes. Read with no mutex and no annotation -
    /// the release store to `finished` publishes it, and no writer is left after that edge.
    struct FinishedState
    {
        std::vector<BuildBlockEntry> blocks;
        std::vector<size_t> row_offsets;
        Block build_side_totals;
        std::vector<TemporaryBlockStreamHolderPtr> spill_sinks;
    };

    /// The published state; throws while the build phase is still going. The only way to read it,
    /// so the acquire side of the edge is crossed exactly once per access and in one place.
    const FinishedState & finishedState(const char * what) const;

    /// Whether a stored block may end up compressed or spilled, and so materialized anew on every
    /// read instead of shared between the readers.
    bool mayMaterializeBlocks() const
    {
        return store_settings.min_rows_to_compress != 0 || store_settings.min_bytes_to_compress != 0
            || (store_settings.max_bytes_in_memory != 0 && store_settings.tmp_data != nullptr);
    }

    /// What the whole build side amounted to once a block had been appended to it, which is what the
    /// size limits are checked against.
    struct StoreSize
    {
        size_t rows = 0;
        size_t bytes = 0;
    };

    /// Appends one block, as it arrived or as a slice of a larger one: accounts it, compresses it
    /// where the thresholds say so, and either keeps it in memory or hands it to the spill.
    StoreSize appendBlock(BuildBlock build_block, size_t stream_index);

    /// Keeps the block in memory. `uncompressed_bytes` is what it takes once decompressed, which is
    /// the shape the spill writes it out in.
    void storeBlock(BuildBlockEntry & entry, size_t index, BuildBlock build_block, bool compressed, size_t uncompressed_bytes)
        TSA_REQUIRES(mutex);
    /// Takes every block that is still in memory out of it, in index order, for the temporary file of
    /// `stream_index`. What it returns has to be written before that stream writes anything else.
    std::vector<BuildBlockPtr> takeInMemoryBlocksLocked(size_t stream_index) TSA_REQUIRES(mutex);
    /// Writes blocks to the temporary file of `stream_index`, opening it on the first ones. Called
    /// without the store mutex: serializing and compressing a block is what spilling costs, and a file
    /// has one writer only, so this is where the build streams stop queueing behind each other.
    void writeSpilledBlocks(size_t stream_index, std::vector<BuildBlockPtr> blocks_to_write);

    /// The entry of block `index`. Needs no lock: the store is read-only by the time it is called.
    const BuildBlockEntry & getBlockEntry(size_t index) const;
    /// A fresh sequential reader over one temporary file, positioned at the first block in it.
    TemporaryBlockStreamReaderHolder createSpillReadStream(size_t sink_index) const;

    const SharedHeader build_header;
    const JoinKind kind;
    const JoinStrictness strictness;
    const SizeLimits size_limits;
    const BlockNestedLoopStoreSettings store_settings;
    const bool needs_match_flags;

    /// The build phase's state, which every build stream appends to concurrently. `finish` moves all
    /// of it into `finished_state` and nothing reads it here afterwards, so the annotation covers
    /// every access there is and needs no exception anywhere.
    mutable std::mutex mutex;
    std::vector<BuildBlockEntry> blocks TSA_GUARDED_BY(mutex);
    Block build_side_totals TSA_GUARDED_BY(mutex);
    /// One temporary file per build stream, opened on that stream's first spilled block. Not guarded:
    /// a stream is the only writer of its own file - the bulk flush included, since the memory spill
    /// scheduler asks a processor to spill from that processor's own execution slot - and nothing reads
    /// them before `finish` has moved them into the published state.
    std::vector<TemporaryBlockStreamHolderPtr> spill_sinks;

    /// Written once by `finish`, under the mutex and before the release store to `finished`.
    std::unique_ptr<const FinishedState> finished_state;

    /// Where the readers share what they materialize, which a compressed block and a spilled one both
    /// are - the first decompressed anew, the second read back from its file. `materialized` is the
    /// lookup, weak so that a block no reader holds any longer costs nothing; `materialized_window` is
    /// what keeps the most recent ones reachable after their reader moved on. Allocated by `finish`, so
    /// a slot is only ever read or written once the block indexes are final.
    ///
    /// Nothing keeps the readers in step, and nothing has to: they walk the blocks in the same order
    /// and are released together, so they ask for the same ones unless their probe chunks drift far
    /// apart, and a reader that finds nothing here simply materializes its own. What a probe stream may
    /// hold is bounded either way; the sharing spares the work, it does not carry the bound.
    mutable std::mutex materialized_mutex;
    std::vector<std::weak_ptr<const BuildBlock>> materialized TSA_GUARDED_BY(materialized_mutex);
    std::deque<BuildBlockPtr> materialized_window TSA_GUARDED_BY(materialized_mutex);
    size_t materialized_window_bytes TSA_GUARDED_BY(materialized_mutex) = 0;

    /// One flag per build row, indexed by global row number; allocated by `finish` and only for the
    /// kinds that need it.
    ///
    /// Relaxed is enough for the accesses themselves: a flag is only ever set, never cleared, so no
    /// write can be lost, and the happens-before edge that makes every write visible to the
    /// unmatched scan comes from the pipeline instead - a probe stream finishes its output port
    /// only after its last write, and `DelayedPortsProcessor` observes that finish before it lets
    /// the unmatched stage produce a single row. `finished` orders the allocation itself the same
    /// way it orders `finished_state`.
    std::unique_ptr<std::atomic_bool[]> matched_flags;

    std::atomic<size_t> total_rows{0};
    /// What the build side would occupy in memory as a whole, spilled blocks included; this is what
    /// `max_bytes_in_join` limits, so that spilling does not quietly raise the limit.
    std::atomic<size_t> total_bytes{0};
    std::atomic<size_t> in_memory_bytes{0};
    /// The largest `in_memory_bytes` ever reached, which spilling does not lower: what the build side
    /// cost in memory is what it cost, whether or not it was later written out.
    std::atomic<size_t> peak_in_memory_bytes{0};
    std::atomic<size_t> max_in_memory_block_bytes{0};
    std::atomic<size_t> num_spilled_blocks{0};
    std::atomic<bool> has_compressed_blocks{false};
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

    /// Drops the cached block and the temporary-file reader. A block that was decompressed or read
    /// back from disk is a copy the store does not account for, so a reader that has nothing left
    /// to read must not keep it alive for as long as its processor sits in the finished pipeline.
    void release();

private:
    BuildBlockPtr readSpilledBlock(size_t index, size_t sink_index, size_t spill_ordinal);

    BlockNestedLoopJoinDataPtr data;
    std::unique_ptr<TemporaryBlockStreamReaderHolder> spill_stream;
    /// Which file the reader is on, and the position of the next block it will hand out from it.
    size_t spill_stream_sink = 0;
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
    BlockNestedLoopBuildTransform(
        SharedHeader input_header, BlockNestedLoopJoinDataPtr data_, FinishCounterPtr finish_counter_, size_t stream_index_);

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
    /// This stream's place among the build streams, and so the temporary file it spills to.
    const size_t stream_index;
    Chunk chunk;
    bool stop_reading = false;
    bool for_totals = false;
    /// Closing the store is asked for by `prepare` and done by `work`, which is where the work of
    /// this stream belongs; `build_finished` is what makes `prepare` ask for it exactly once.
    bool finish_build_requested = false;
    bool build_finished = false;
};

}
