#pragma once

#include <Core/ColumnNumbers.h>
#include <Core/SortDescription.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/IProcessor.h>
#include <Processors/Transforms/DistinctSetFilter.h>
#include <QueryPipeline/SizeLimits.h>
#include <Common/logger_useful.h>

namespace DB
{

class MergeSorter;

/// Deduplicates a stream that is sorted by `description` over the distinct key columns: keeps the first
/// row of every range of rows that compare equal, drops the rest. Values are compared by the sort
/// comparison, so e.g. `0.` and `-0.` or NaNs with different payloads are deduplicated as one value -
/// the same equality that DISTINCT in order uses (the in-memory hash DISTINCT distinguishes such values
/// by their binary representation; both notions of equality are long-accepted DISTINCT behaviors, and
/// which one applies to a value depends on the path the value took - see ExternalDistinctTransform).
/// The stream may be split into chunks arbitrarily: the filter detects when a chunk continues the last
/// range of the previous one.
///
/// The last column of each chunk must be a UInt8 "already emitted" flag. If a range starts with a
/// flagged row, the whole range is dropped: such a value was already sent downstream before the spill.
/// The flagged rows are required to precede the equal rows without the flag (which the merge of the
/// spilled runs guarantees by the input-index tie-break, see ExternalDistinctTransform).
class DistinctSortedFilter
{
public:
    DistinctSortedFilter(ColumnNumbers key_columns_pos_, SortDescription description_, size_t flag_column_pos_);

    /// Forgets the state accumulated from previous chunks (to start filtering an unrelated stream).
    void reset();

    /// Filters the next chunk of the sorted stream. Returns a chunk with only the first row of each
    /// range of equal keys that does not start with an "already emitted" flag; may have zero rows.
    /// If `strip_flag` is set, the flag column is removed from the result.
    Chunk filter(Chunk chunk, bool strip_flag);

private:
    void saveLatestKey(const ColumnRawPtrs & key_columns, size_t row_pos);
    bool isLatestKeyFromPrevChunk(const ColumnRawPtrs & key_columns, size_t row_pos) const;

    const ColumnNumbers key_columns_pos;
    const SortDescription description;
    const size_t flag_column_pos;

    /// The key of the last row of the previous chunk, to detect that a chunk continues the previous range.
    MutableColumns prev_chunk_latest_key;
};

/// DISTINCT transform that can spill to disk ("external distinct"). Used for the final (single stream)
/// DISTINCT when `max_bytes_before_external_distinct`/`max_bytes_ratio_before_external_distinct` is set.
///
/// While the memory usage of the query is below the threshold, it behaves like DistinctTransform: keeps a
/// hash set of the seen keys and streams the first occurrence of each key downstream immediately. If a
/// spill happens, the set of the already emitted rows must be recoverable: for most set methods the keys
/// are extracted back from the hash table itself (DistinctSetFilter::extractKeyColumns) - no memory
/// overhead before the spill, one transient copy of the keys at the spill moment. When the extraction
/// cannot rebuild the rows - the chosen method is irreversible (`hashed` keeps just a 128-bit hash per
/// key), or a non-key column carries per-row data (see nonKeyColumnsAreRebuildable) - the transform
/// retains the emitted chunks in a buffer instead (only column pointers are copied, but the emitted
/// columns stay referenced in memory until the first spill).
///
/// When the memory usage of the query exceeds the threshold, the transform switches to the external mode:
///  - The already emitted rows (extracted from the set, or taken from the buffer when the extraction
///    cannot rebuild them) are sorted by the key columns and written to a temporary file as the first
///    "run", each row carrying an "already emitted" flag; the hash set (and the buffer) is freed. The
///    runs contain only the non-constant columns - the constant columns are re-attached from the header
///    after the merge.
///  - Nothing is emitted anymore until the input is exhausted. Incoming chunks are sorted and accumulated,
///    and written out as further runs (locally deduplicated, flag not set) each time the memory usage
///    exceeds the threshold again.
///  - At the end, all runs are merged by a MergingSortedTransform (plus the leftover in-memory chunks as
///    the last input), and DistinctSortedFilter keeps the first occurrence of each distinct key that
///    was not emitted before the spill. The first run is the merge input 0, and the merge breaks ties by
///    the input index, so flagged rows precede the equal rows from the later runs. This tie-break is
///    a correctness requirement: the filter looks only at the first row of each equal range, so if an
///    unflagged row could come first, a value emitted before the spill would be emitted again as a
///    duplicate (see the note on SortCursorHelper in Core/SortCursor.h).
class ExternalDistinctTransform final : public IProcessor
{
public:
    ExternalDistinctTransform(
        SharedHeader header_,
        const SizeLimits & set_size_limits_,
        UInt64 limit_hint_,
        const Names & columns_,
        size_t max_bytes_before_external_distinct_,
        TemporaryDataOnDiskScopePtr tmp_data_,
        size_t min_free_disk_space_,
        size_t max_block_size_rows_);

    ~ExternalDistinctTransform() override;

    String getName() const override { return "ExternalDistinctTransform"; }

    Status prepare() override;
    void work() override;
    PipelineUpdate updatePipeline() override;

private:
    enum class Stage : uint8_t
    {
        Consume = 0,
        Generate,
        Serialize,
    };

    Status prepareConsume();
    Status prepareSerialize();
    Status prepareGenerate();

    void consume(Chunk chunk);
    void serialize();
    void generate();

    /// Keeps only the spilled columns of an input-header chunk (drops the constant columns).
    Chunk stripConstantColumns(Chunk chunk) const;
    /// Takes a spill-layout chunk (see stripConstantColumns, buildChunkFromKeys), appends the
    /// "already emitted" flag column and sorts by the key columns.
    Chunk prepareSpillChunk(Chunk chunk, bool already_emitted) const;

    /// Whether the first run can be rebuilt from the set at spill time (then the emitted chunks do not
    /// have to be retained in memory). Meaningful once at least one chunk was filtered.
    bool firstRunFromExtraction() const;
    /// Assembles a spill-layout chunk (without the flag column) from the extracted key columns. Only
    /// used when every non-key column is a rebuildable constant, so the spilled columns are exactly
    /// the keys.
    Chunk buildChunkFromKeys(MutableColumns && key_columns) const;
    /// Re-attaches the constant columns that are not written to the spilled runs, turning a merged
    /// spill-layout chunk back into an input-header chunk.
    Chunk restoreConstantColumns(Chunk chunk) const;

    void startFirstSpill();
    void startSpillRun(Chunks run_chunks, size_t run_bytes, bool is_first_run);

    /// The pre-spill deduplication, shared with DistinctTransform (freed when the first spill happens).
    DistinctSetFilter distinct_set;
    /// All the non-key columns are constants with a known value (see firstRunFromExtraction).
    const bool non_key_columns_rebuildable;
    const UInt64 limit_hint;
    const SizeLimits set_size_limits;

    const size_t max_bytes_before_external_distinct;
    TemporaryDataOnDiskScopePtr tmp_data;
    const size_t min_free_disk_space;
    const size_t max_block_size_rows;

    /// Ascending sort over the (non-constant) key columns; defines the order of the spilled runs.
    SortDescription description;
    /// Positions (in the input header) of the columns written to the spilled runs: all the non-constant
    /// columns. The constant columns are re-attached from the header after the merge of the runs - their
    /// values carry no information, and the Native format of the temporary files could not keep them
    /// constant anyway.
    const ColumnNumbers spill_columns_pos;
    /// Positions of the key columns within the spill layout.
    const ColumnNumbers spill_key_columns_pos;
    /// Header of the spilled runs: the spilled columns plus the flag column.
    SharedHeader spill_header;

    /// Copies of the emitted chunks, the future first run (freed when the first spill happens).
    Chunks emitted_buffer;
    /// External mode: sorted flagless chunks accumulated since the last run was written.
    Chunks chunks;
    size_t sum_bytes_in_chunks = 0;

    size_t temporary_files_num = 0;
    std::unique_ptr<MergeSorter> merge_sorter;
    /// Local deduplication of the run that is currently being written (an I/O saver, not needed for
    /// correctness; the first run is unique by construction and bypasses it).
    DistinctSortedFilter run_dedup;
    bool current_run_is_first = false;
    /// Deduplication of the merged stream of runs in the Generate stage.
    DistinctSortedFilter merge_dedup;

    ProcessorPtr external_merging_sorted;
    Processors processors;

    Stage stage = Stage::Consume;
    bool spilled = false;
    bool generated_prefix = false;
    /// No more output is needed: the limit hint or a size limit (with the 'break' overflow mode) was
    /// reached. The counterpart of ISimpleTransform::stopReading.
    bool read_stopped = false;

    /// Distinct rows sent downstream (in both phases). Post-spill this is exactly what the cardinality
    /// of the DISTINCT hash set would have been, so the rows limit is enforced against it.
    size_t emitted_rows = 0;

    Chunk current_chunk;
    Chunk generated_chunk;

    LoggerPtr log = getLogger("ExternalDistinctTransform");
};

}
