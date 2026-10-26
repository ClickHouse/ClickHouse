#pragma once

#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/IProcessor.h>
#include <Processors/RowsBeforeStepCounter.h>
#include <Processors/Transforms/ChunkRowRange.h>
#include <Processors/Transforms/LimitByGroupMapping.h>
#include <Common/Logger.h>

#include <list>
#include <optional>

namespace DB
{

/// `LIMIT BY` for input where equal grouping keys are not guaranteed to be contiguous, with a bound on
/// the memory it may hold. It is the hash-based `LimitByTransform` until the query reaches the spill
/// threshold, and a sort/merge based `LIMIT BY` afterwards.
///
/// While in memory it behaves exactly like `LimitByTransform`: it keeps one counter per distinct group
/// and streams the surviving rows downstream in input order, so a query that never crosses the
/// threshold pays nothing for the spilling machinery.
///
/// Crossing the threshold, the grouping hash table is written out as one row per group carrying the key
/// and the number of rows seen for it, and then released. From that point every input row is buffered
/// with its arrival number and the buffer is written to disk as a sorted run whenever it grows too
/// large. At the end the runs are merged into one stream ordered by
/// `(grouping keys, is a group state row descending, arrival number)`, which puts each group's state row
/// first and its data rows in input order right after it, so a single pass over the merged stream can
/// resume the per-group counter and emit exactly the rows inside the `[offset, offset + length)` window.
///
/// The rows emitted before the spill keep their input order and precede every row that the merge emits,
/// but the merge emits its own rows in grouping key order rather than in input order. `LIMIT BY` does
/// not order its result, yet a step above it may rely on an order established below it, so the plan only
/// builds this transform when the step does not have to preserve the order of its input.
///
/// Extra memory: bounded by the spill threshold, plus one buffered run.
class ExternalLimitByTransform final : public IProcessor
{
public:
    ExternalLimitByTransform(
        SharedHeader header,
        UInt64 group_length_,
        UInt64 group_offset_,
        const Names & column_names,
        size_t max_bytes_in_state_before_external_limit_by_,
        size_t max_bytes_in_query_before_external_limit_by_,
        size_t max_block_size_,
        TemporaryDataOnDiskScopePtr tmp_data_,
        size_t min_free_disk_space_);

    ~ExternalLimitByTransform() override;

    String getName() const override { return "ExternalLimitByTransform"; }

    Status prepare() override;
    void work() override;
    PipelineUpdate updatePipeline() override;

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { rows_before_limit_at_least.swap(counter); }

private:
    enum class Stage : uint8_t
    {
        /// Reading the input: filtering it in memory, or buffering and spilling it after the threshold.
        Consume = 0,
        /// The input is done. Nothing left to do without a spill, otherwise merging the runs.
        Generate,
    };

    Status prepareConsume();
    Status prepareGenerate();

    void consume(Chunk chunk);
    void generate();

    /// The in-memory phase: counts the rows of each group and leaves the surviving ones in `chunk`.
    void filterChunkInMemory(Chunk & chunk);

    bool shouldSpill() const;

    /// Writes the grouping hash table out as group state rows and releases it.
    void convertGroupsToStateRows();

    void bufferChunkForSpill(Chunk chunk);
    void addSortedChunkToBuffer(Chunk chunk);
    void spillBufferedChunks();

    void buildMergingPipeline();

    /// One pass over the merged stream: resumes each group's counter from its state row and leaves only
    /// the rows of the `[offset, offset + length)` window, without the service columns.
    void filterMergedChunk(Chunk & chunk);

    void removeConstColumns(Chunk & chunk) const;
    void enrichChunkWithConstants(Chunk & chunk) const;

    Stage stage = Stage::Consume;

    /// Kept per-group interval is `[group_offset, group_limit_end)`.
    const UInt64 group_offset;
    const UInt64 group_limit_end;

    /// The spill starts when the grouping state alone crosses the first threshold, or when the whole
    /// query crosses the second one. Either being zero disables that half of the decision.
    const size_t max_bytes_in_state_before_external_limit_by;
    const size_t max_bytes_in_query_before_external_limit_by;
    const size_t max_block_size;
    TemporaryDataOnDiskScopePtr tmp_data;
    const size_t min_free_disk_space;

    /// Constants are dropped before spilling, because the Native format cannot carry them, and put back
    /// on every chunk this transform emits.
    Block header_without_constants;
    std::vector<bool> const_columns_to_remove;

    /// `header_without_constants` followed by the service columns that drive the merge.
    SharedHeader spill_header;
    size_t is_state_column_position = 0;
    size_t rows_seen_column_position = 0;
    size_t arrival_column_position = 0;

    /// Grouping keys, both by name and by position in `header_without_constants`.
    GroupingKeys grouping_keys;

    /// Orders the runs by `(grouping keys, is a group state row descending, arrival number)`.
    SortDescription run_description;

    /// The in-memory grouping state, released when the transform spills.
    std::optional<LimitByGroupMapping> mapping;

    /// The counter of the single group of a `LIMIT BY` whose every key is constant, which builds no
    /// hash table and therefore never spills.
    UInt64 trivial_group_rows_seen = 0;

    /// Slices of the chunk being filtered that will be emitted. A member so that its allocation is
    /// reused across chunks.
    std::vector<ChunkRowRange> output_slices;

    /// The number of input rows seen so far, which numbers the rows buffered for a spill.
    UInt64 arrival_counter = 0;

    /// Sorted chunks waiting to be written as one run, and the runs already written.
    Chunks buffered_chunks;
    size_t buffered_bytes = 0;
    std::list<TemporaryBlockStreamHolder> runs;

    /// The merge is driven by a sub-pipeline this transform adds once the input is done.
    Processors merging_processors;
    ProcessorPtr merging_transform;
    bool merging_pipeline_built = false;

    /// Set once the grouping hash table has been converted to state rows.
    bool spilled = false;

    /// The number of rows seen for the group the merge is in the middle of, and its key, which is needed
    /// because one group can span several merged chunks.
    UInt64 merged_group_rows_seen = 0;
    MutableColumns previous_merged_chunk_last_key_columns;

    Chunk current_chunk;
    Chunk merged_chunk;
    Chunk generated_chunk;

    RowsBeforeStepCounterPtr rows_before_limit_at_least;

    LoggerPtr log = getLogger("ExternalLimitByTransform");
};

}
