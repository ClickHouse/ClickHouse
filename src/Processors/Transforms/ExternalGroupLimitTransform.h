#pragma once

#include <Core/Names.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/IProcessor.h>
#include <Processors/RowsBeforeStepCounter.h>
#include <Processors/SpillableAdapter.h>
#include <Processors/Transforms/DistinctSetMemoryTracker.h>
#include <Processors/Transforms/GroupLimitSpillLayout.h>
#include <Processors/Transforms/SeenGroupExtractor.h>
#include <Processors/Transforms/SortingTransform.h>
#include <QueryPipeline/SizeLimits.h>
#include <Common/logger_useful.h>

#include <optional>
#include <variant>

namespace DB
{

class BufferingFromFileSource;
class BufferingToFileSink;
class GroupLimitSortedTransform;

/// Parameters of a group limit: rows with equal keys form a group, and each group keeps the rows whose
/// arrival rank falls into a window: `[m, m + n)` for `LIMIT n OFFSET m BY keys`, `[0, 1)` for `DISTINCT`.
struct GroupLimitParams
{
    /// Key column names. `DISTINCT` treats an empty list as every column.
    Names key_columns;
    UInt64 group_offset = 0;
    /// Exclusive end of the kept window: the offset plus the length, saturated at the maximum `UInt64`.
    UInt64 group_limit_end = 1;
    /// Stops reading once this many result rows are produced; zero disables it.
    UInt64 limit_hint = 0;
    /// Limits on the in-memory state, and on result rows once spilled (`max_rows_in_distinct`,
    /// `max_bytes_in_distinct`).
    SizeLimits size_limits{};
    /// The number of transforms that split the groups of the operator between them, one per disjoint input
    /// stream. Each transform holds about `1 / num_streams` of the state, so it applies that share of the
    /// spill floor.
    size_t num_streams = 1;
};

/// The hash-based group limit (`DISTINCT` or `LIMIT BY`) that spills to disk. `Traits` supplies the in-memory
/// filter of the operator, the processor names shown to users, the operator name used in log messages, and
/// the operator's profile events.
///
/// The transform streams the rows of each group's window until tracked query memory exceeds its spill
/// threshold, projected growth and spill workspace exceed the remaining threshold budget, or the memory
/// scheduler requests a spill (see `spill`). The threshold triggers no spill while the state of the operator
/// stays below a run's worth of bytes (see `isBelowSpillFloor`). Its filter retains typed keys, or 128-bit
/// fingerprints of generic keys, which the spill runs compare instead of the keys.
///
/// At the first spill, the filter's groups become sorted seen runs carrying the number of input rows seen per
/// group. Extraction prepares one run at a time with a soft byte target and waits for its file to finish.
/// The extractor owns the retained grouping state and releases it as extraction finishes. A filter that
/// retains fingerprints can release its hash table before extracting them. Further input becomes sorted
/// runs that keep at most the window end of rows per group, and output waits until all input is consumed.
/// `GroupLimitSpillLayout` owns the column conversions.
///
/// Before merging, a conservative reader and output-workspace estimate determines how much of the
/// tail can stay in memory. Whole chunks are spilled in arrival order until the remaining suffix fits
/// the estimate or no tail remains.
/// `GroupLimitSortedTransform` merges the runs and the in-memory tail. Runs are ordered by key and then by
/// the seen count descending, placing seen rows first in each group so that their counts start the rank of
/// the group. Input runs follow in arrival order, keeping arrival precedence through input-index ties.
///
/// When input order must be preserved, `MergeSortingTransform` sorts the merged rows back into it and can
/// itself spill. It sorts by the columns of the input order when the caller provides them and the spilled
/// rows carry them, and otherwise by arrival numbers that `GroupLimitSpillLayout` attaches to spilled rows.
/// Rows that tie on the sort columns can then come out in a different order than in memory, which the sort
/// order allows. Without the order requirement, rows follow the spill comparison order, which is fingerprint
/// order for generic keys.
/// Scheduler requests synchronously write unconnected runs and release their memory before returning;
/// their readers are attached at the next pipeline update.
template <typename Traits>
class ExternalGroupLimitTransform final : public IProcessor
{
    using Filter = typename Traits::Filter;

public:
    /// `max_bytes_before_external_` is the spill threshold of query memory; zero leaves spilling to the
    /// memory scheduler's requests. A non-null `shared_filter_bytes_` reports memory snapshots to
    /// `DistinctLimitsCheckingTransform`, which enforces the limits across all streams of a parallel
    /// `DISTINCT`. In that mode, `params_.size_limits` must be unlimited. A non-empty
    /// `input_sort_description_` is the order of the input rows. With `preserve_input_order_`, a spill sorts
    /// its rows back by it when they carry its columns, and by arrival numbers otherwise.
    ExternalGroupLimitTransform(
        SharedHeader header_,
        const GroupLimitParams & params_,
        size_t max_bytes_before_external_,
        TemporaryDataOnDiskScopePtr tmp_data_,
        size_t min_free_disk_space_,
        size_t max_block_size_rows_,
        size_t preferred_block_bytes_,
        bool preserve_input_order_,
        DistinctSetMemoryTracker::SharedCounter shared_filter_bytes_ = nullptr,
        SortDescription input_sort_description_ = {});

    ~ExternalGroupLimitTransform() override;

    String getName() const override { return String(Traits::transform_name); }

    Status prepare() override;
    void work() override;
    PipelineUpdate updatePipeline() override;

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { rows_before_limit_at_least.swap(counter); }

    std::span<ISpillable * const> getSpillables() override { return {&spillable_ptr, 1}; }
    ProcessorMemoryStats getMemoryStats() const;
    size_t spill(size_t at_least_bytes);
    const TemporaryDataOnDiskScope * getSpillScope() const { return tmp_data.get(); }

private:
    SpillableAdapter<ExternalGroupLimitTransform> spillable{*this};
    ISpillable * const spillable_ptr = &spillable;

    struct Hashing
    {
        Hashing(const Block & header, const GroupLimitParams & limit_params, const IProcessor & owner)
            : filter(Traits::createFilter(header, limit_params, owner))
        {
        }

        /// Memory charged to the global `max_bytes_in_distinct` check. A filter without groups counts as empty,
        /// like in a serial `DISTINCT` that checks limits only after inserting keys: otherwise the
        /// preallocated hash table of a transform that spills before hashing would exceed tiny limits.
        size_t getRetainedFilterBytes() const { return filter.getTotalRowCount() ? filter.getTotalByteCount() : 0; }

        Filter filter;
        /// EOF is observed in `prepare`, but releasing the filter belongs to `work`.
        bool input_finished = false;
    };

    struct ExtractingSeenGroups
    {
        std::unique_ptr<SeenGroupExtractor> extractor;
    };

    struct PreparedMerge
    {
        std::shared_ptr<GroupLimitSortedTransform> merger;
        Processors order_restoration;
    };

    struct MergeRegistration
    {
        std::shared_ptr<GroupLimitSortedTransform> merger;
        InputPort & input;
    };

    /// Exhausting the merger and handing off its last chunk completes the producer, not the file.
    struct RunWriteProgress
    {
        std::unique_ptr<MergeSorter> merger;
        Chunk chunk;
        size_t max_block_bytes = 0;
    };

    struct PreparedRun
    {
        RunWriteProgress progress;
        std::shared_ptr<BufferingToFileSink> sink;
        std::shared_ptr<BufferingFromFileSource> source;
        std::optional<PreparedMerge> initial_merge;
    };

    struct ConnectingSeenRun
    {
        PreparedRun run;
        std::unique_ptr<SeenGroupExtractor> extractor;
    };

    struct WritingSeenRun
    {
        RunWriteProgress progress;
        OutputPort & output;
        std::unique_ptr<SeenGroupExtractor> extractor;
        InputPort & completion;
        OutputPort & readiness;
    };

    struct SortingUnit
    {
        Chunks chunks;
        size_t rows = 0;
        size_t allocated_bytes = 0;
    };

    struct CollectingInput
    {
        SortingUnit pending;
        Chunks sorted_chunks;
        size_t sorted_bytes = 0;
        size_t sorted_rows = 0;
    };

    struct ConnectingInputRun
    {
        PreparedRun run;
    };

    struct WritingInputRun
    {
        RunWriteProgress progress;
        OutputPort & output;
    };

    struct PreparingTail
    {
        CollectingInput collecting;
    };

    struct ConnectingTailRun
    {
        PreparedRun run;
        CollectingInput remaining;
    };

    struct WritingTailRun
    {
        RunWriteProgress progress;
        OutputPort & output;
        CollectingInput remaining;
        InputPort & completion;
        OutputPort & readiness;
    };

    struct ConnectingTail
    {
        std::shared_ptr<MergeSorterSource> source;
    };

    struct Merging
    {
        InputPort & input;
        Chunk chunk;
    };

    struct Finishing
    {
    };

    /// Connection states hand processors prepared by `work` to `updatePipeline`. Each writing state
    /// determines its own continuation and owns only the completion dependencies that it needs.
    using State = std::variant<
        Hashing,
        ExtractingSeenGroups,
        ConnectingSeenRun,
        WritingSeenRun,
        CollectingInput,
        ConnectingInputRun,
        WritingInputRun,
        PreparingTail,
        ConnectingTailRun,
        WritingTailRun,
        ConnectingTail,
        Merging,
        Finishing>;

    Status prepareInput();
    CollectingInput * getCollectingInput();
    const CollectingInput * getCollectingInput() const;
    Status prepareCollectingInput(CollectingInput & collecting);
    Status prepareRunWrite(RunWriteProgress & progress, OutputPort & output);
    Status prepareSeenRunWrite(WritingSeenRun & writing);
    Status prepareInputWrite(WritingInputRun & writing);
    Status prepareTailWrite(WritingTailRun & writing);
    Status prepareMergedOutput(Merging & merging);
    Status prepareFinish();
    Status finish();

    void consumeHashing(Hashing & hashing);
    void startSpilling(Hashing & hashing);
    void extractSeenRun(ExtractingSeenGroups & extracting);
    void collectInput(CollectingInput & collecting);
    void flushSortingUnit(CollectingInput & collecting);
    bool canAppendToSortingUnit(const SortingUnit & unit, size_t rows, size_t bytes) const;
    bool canStartCoalescing(size_t rows, size_t bytes) const;
    bool fitsSortingBudget(size_t rows, size_t column_bytes, size_t additional_input_bytes = 0) const;
    size_t maxRowsInSortingUnit() const;
    void readRun(RunWriteProgress & progress);
    void prepareTail(PreparingTail & tail);
    size_t estimateRunWriteMemory(size_t rows, size_t allocated_bytes) const;
    size_t selectTailSpillPrefix(const CollectingInput & collecting) const;
    void consumeMerged(Merging & merging);
    /// Counts result rows toward the hint, the size limits, and `rows_before_limit_at_least`.
    void addResultRows(size_t rows);

    PreparedRun prepareRun(
        SharedHeader header, Chunks chunks, size_t bytes, const SortDescription & description, MergeSorter::Mode mode);
    PreparedMerge prepareMerge();
    void connectMerge(PreparedMerge & prepared, Processors & processors);
    void connectRunSource(PreparedRun & prepared, Processors & processors);
    OutputPort & connectRun(PreparedRun & prepared, Processors & processors);
    /// Drain an unconnected run on the calling thread, transferring its finalized file to the reader.
    void spillRun(PreparedRun run);
    /// Returns the minimum run size, also used by the sort that restores input order and as the spill floor.
    size_t minBytesInRun() const;
    /// Whether a filter state of `state_bytes`, grown by `growth_bytes`, stays below this transform's share
    /// of the spill floor: `minBytesInRun`, divided among the transforms that split the groups of the
    /// operator. Spilling such a state frees almost nothing when other operators hold the query memory, but
    /// holds back the results until the input ends, which also defeats an early `LIMIT`.
    bool isBelowSpillFloor(size_t state_bytes, size_t growth_bytes) const;
    /// Whether spilled rows carry arrival numbers, which restore the input order when they do not carry its
    /// columns.
    bool attachesArrivalNumbers() const { return preserve_input_order && input_order_description.empty(); }

    const GroupLimitParams params;
    /// Outlives the state that owns the filter and its seen-group extractor.
    DistinctSetMemoryTracker filter_memory;
    State state;
    const size_t max_bytes_before_external;
    TemporaryDataOnDiskScopePtr tmp_data;
    const size_t min_free_disk_space;
    const size_t max_block_size_rows;
    const size_t preferred_block_bytes;
    const bool preserve_input_order;
    /// The order of the input by columns that spilled rows carry, which restores it after a spill. Empty when
    /// arrival numbers restore it, or when the order does not matter.
    const SortDescription input_order_description;
    /// Created at the first spill from the representation selected by the initialized filter.
    std::optional<GroupLimitSpillLayout> spill_layout;

    /// Tracks connected merge inputs until tail attachment or early termination closes registration.
    std::optional<MergeRegistration> merge_registration;
    /// Scheduler-driven spilling releases memory before returning; completed readers are connected later.
    std::vector<PreparedRun> spilled_runs;
    size_t temporary_files_num = 0;
    /// Run output sizes estimate the memory needed to read all files concurrently during merging.
    size_t estimated_file_read_memory = 0;
    /// Largest average row width observed before or after the group limit, including the seen count.
    size_t max_average_row_bytes = 0;

    /// Counts accepted input rows and provides the next arrival number.
    UInt64 consumed_rows = 0;
    /// Counts rows admitted to the result, before the pending output is pushed to its port.
    size_t result_rows = 0;
    RowsBeforeStepCounterPtr rows_before_limit_at_least;

    /// Unprocessed input, including a partially hashed chunk's suffix, remains here until seen-key
    /// extraction finishes.
    Chunk input_chunk;
    /// Both hashing and merging produce results here, independently of spill-writing progress.
    Chunk output_chunk;

    LoggerPtr log = getLogger(String(Traits::transform_name));
};

}
