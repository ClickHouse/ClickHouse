#pragma once

#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/IProcessor.h>
#include <Processors/Sources/ExternalMergeSource.h>
#include <Processors/Transforms/DistinctSetFilter.h>
#include <Processors/Transforms/DistinctSpillLayout.h>
#include <Processors/Transforms/SortingTransform.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/SizeLimits.h>
#include <Common/logger_useful.h>

#include <optional>
#include <variant>

namespace DB
{

class BufferingToFileSink;

/// The final hash-based `DISTINCT` streams first occurrences until tracked query memory exceeds its
/// spill threshold or projected growth and spill workspace exceed the remaining threshold budget.
/// Its set retains typed keys or the same generic-key fingerprints as ordinary `DISTINCT`.
///
/// At the first spill, the set's keys become sorted suppression runs carrying already-emitted flags.
/// Extraction prepares one run at a time with a soft byte target and waits for its file to finish.
/// The set is released after its last keys are extracted. Further input becomes sorted, locally deduplicated
/// runs, and output waits until all input is consumed. `DistinctSpillLayout` owns the column conversions.
///
/// `DistinctSortedTransform` merges the runs and the unique in-memory tail, removing duplicate keys
/// and keys emitted before spilling. Runs are ordered by key and then by the already-emitted flag
/// descending, placing suppression rows first. `ExternalMergeSource` bounds simultaneous file readers
/// by merging files in groups with matching layouts. It selects the smallest compressed files within
/// each group; the final merge combines both groups and the in-memory tail to apply suppression.
///
/// When input order must be preserved, `DistinctSpillLayout` attaches arrival numbers to ordinary rows
/// and constant zeros to suppression rows so both layouts support the same comparator. Comparing these
/// after the emitted flag retains the earliest ordinary row independently of merge order.
/// `MergeSortingTransform` restores arrival order after deduplication and can itself spill.
/// Otherwise, rows follow the spill comparison order, which is fingerprint order for generic keys.
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
        size_t max_block_size_rows_,
        bool preserve_input_order_,
        size_t max_external_merge_fan_in_);

    ~ExternalDistinctTransform() override;

    String getName() const override { return "ExternalDistinctTransform"; }

    Status prepare() override;
    void work() override;
    PipelineUpdate updatePipeline() override;

private:
    struct Hashing
    {
        Hashing(const Block & header, const Names & columns, const SizeLimits & limits)
            : set(header, columns, limits)
        {
        }

        DistinctSetFilter set;

        /// EOF is observed in `prepare`, but releasing the set belongs to `work`.
        bool input_finished = false;
    };

    struct ExtractingSuppression
    {
        std::unique_ptr<DistinctSetFilter::KeyExtractor> keys;
    };

    /// Exhausting the merger and handing off its last chunk completes the producer, not the file.
    struct RunWriteProgress
    {
        std::unique_ptr<MergeSorter> merger;
        Chunk chunk;
    };

    struct PreparedRun
    {
        RunWriteProgress progress;
        std::shared_ptr<BufferingToFileSink> sink;
    };

    struct ConnectingSuppressionRun
    {
        PreparedRun run;
        std::unique_ptr<DistinctSetFilter::KeyExtractor> keys;
    };

    struct WritingSuppressionRun
    {
        RunWriteProgress progress;
        OutputPort & output;
        std::unique_ptr<DistinctSetFilter::KeyExtractor> keys;
        InputPort & completion;
        std::shared_ptr<BufferingToFileSink> sink;
    };

    struct CollectingInput
    {
        Chunks chunks;
        size_t bytes = 0;
    };

    struct ConnectingInputRun
    {
        PreparedRun run;
    };

    struct WritingInputRun
    {
        RunWriteProgress progress;
        OutputPort & output;
        InputPort & completion;
        std::shared_ptr<BufferingToFileSink> sink;
    };

    struct PreparingTail
    {
        Chunks chunks;
    };

    struct ConnectingMerge
    {
        Pipe pipe;
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
        ExtractingSuppression,
        ConnectingSuppressionRun,
        WritingSuppressionRun,
        CollectingInput,
        ConnectingInputRun,
        WritingInputRun,
        PreparingTail,
        ConnectingMerge,
        Merging,
        Finishing>;

    Status prepareInput();
    Status prepareCollectingInput(CollectingInput & collecting);
    Status prepareRunWrite(RunWriteProgress & progress, OutputPort & output, InputPort & completion);
    Status prepareMergedOutput(Merging & merging);
    Status prepareFinish();
    Status finish();

    void consumeHashing(Hashing & hashing);
    void startSpilling(Hashing & hashing);
    void extractSuppressionRun(ExtractingSuppression & extracting);
    void collectInput(CollectingInput & collecting);
    void readRun(RunWriteProgress & progress);
    void prepareTail(PreparingTail & tail);
    void consumeMerged(Merging & merging);

    PreparedRun prepareRun(
        SharedHeader header, Chunks chunks, size_t bytes, const SortDescription & description, MergeSorter::Mode mode);
    Pipe createMergePipe(Chunks tail);
    void connectRun(PreparedRun & prepared, Processors & processors);

    /// Returns the minimum run size, also used by the sort that restores input order.
    size_t minBytesInRun() const;

    State state;
    const UInt64 limit_hint;
    const SizeLimits set_size_limits;
    const size_t max_bytes_before_external_distinct;
    TemporaryDataOnDiskScopePtr tmp_data;
    const size_t min_free_disk_space;
    const size_t max_block_size_rows;
    const bool preserve_input_order;
    const size_t max_external_merge_fan_in;

    /// Created at the first spill from the representation selected by the initialized set.
    std::optional<DistinctSpillLayout> spill_layout;

    ExternalMergeSource::Runs suppression_runs;
    ExternalMergeSource::Runs ordinary_runs;
    size_t temporary_files_num = 0;

    /// Counts accepted input rows and provides the next arrival number.
    UInt64 consumed_rows = 0;

    /// Counts rows admitted to the result, before the pending output is pushed to its port.
    size_t result_rows = 0;

    /// Input rejected before hash-table growth remains here until suppression extraction finishes.
    Chunk input_chunk;

    /// Both hashing and merging produce results here, independently of spill-writing progress.
    Chunk output_chunk;

    LoggerPtr log = getLogger("ExternalDistinctTransform");
};

}
