#pragma once

#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/IProcessor.h>
#include <Processors/Transforms/DistinctSetFilter.h>
#include <Processors/Transforms/DistinctSortedFilter.h>
#include <Processors/Transforms/DistinctSpillLayout.h>
#include <QueryPipeline/SizeLimits.h>
#include <Common/logger_useful.h>

#include <optional>

namespace DB
{

class MergeSorter;
class MergingSortedTransform;

/// The final hash-based `DISTINCT` streams first occurrences until tracked query memory exceeds its
/// spill threshold or projected table growth leaves insufficient user/server memory for spilling.
/// Its set retains extractable keys, including serialized keys when needed.
///
/// At the first spill, the set's keys become sorted suppression runs carrying already-emitted flags.
/// Extraction prepares one run at a time with a soft byte target and waits for its file to finish.
/// The set is released after its last keys are extracted. Further input becomes sorted, locally deduplicated
/// runs, and output waits until all input is consumed. `DistinctSpillLayout` owns the column conversions.
///
/// `MergingSortedTransform` merges the runs, including any in-memory tail, and `DistinctSortedFilter`
/// removes duplicate keys and keys emitted before spilling. Runs are ordered by key and then by the
/// already-emitted flag descending, placing suppression rows first in each equal-key range. Ordinary
/// runs follow in arrival order, preserving the first payload among equal keys through input-index ties.
///
/// When input order must be preserved, `DistinctSpillLayout` attaches arrival numbers to spilled rows.
/// After merging and deduplication, `MergeSortingTransform` restores that order and can itself spill.
/// Otherwise, the post-spill output is in distinct-key order.
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
        bool preserve_input_order_);

    ~ExternalDistinctTransform() override;

    String getName() const override { return "ExternalDistinctTransform"; }

    Status prepare() override;
    void work() override;
    PipelineUpdate updatePipeline() override;

private:
    enum class Stage : uint8_t
    {
        Consume = 0,
        ExtractSuppression,
        Generate,
        Serialize,
    };

    enum class RunKind : uint8_t
    {
        Input,
        Suppression,
    };

    enum class PipelineUpdateKind : uint8_t
    {
        InitializeMergeAndAddRun,
        AddRun,
        AddInMemoryTail,
    };

    struct PendingPipelineUpdate
    {
        PipelineUpdateKind kind;
        RunKind run_kind;
        ProcessorPtr sink;
        ProcessorPtr source;
        Processors merged_stream;
        Processors processors;
    };

    Status prepareConsume();
    Status prepareSerialize();
    Status prepareGenerate();

    void consume(Chunk chunk);
    void extractSuppressionRun();
    void serialize();
    void generate();

    /// Stably sorts suppression rows and stably sorts and deduplicates ordinary input rows.
    Chunk sortSpillChunk(Chunk chunk, RunKind kind) const;

    void startFirstSpill();
    void startSpillRun(Chunks run_chunks, size_t run_bytes, RunKind kind);
    void createMergedStream(PendingPipelineUpdate & update);
    void connectMergedStream(const Processors & merged_stream);
    void attachSpilledRun(const ProcessorPtr & source, const ProcessorPtr & sink, RunKind kind);
    void attachInMemoryTail(const ProcessorPtr & source);
    /// Returns the minimum run size, also used by the sort that restores input order.
    size_t minBytesInRun() const;

    /// Owns hashing state until the first spill. Resetting it permanently ends the hashing phase.
    std::optional<DistinctSetFilter> distinct_set;
    /// Owns the set and arena while successive suppression runs are extracted and written.
    std::unique_ptr<DistinctSetFilter::KeyExtractor> suppression_keys;
    const UInt64 limit_hint;
    const SizeLimits set_size_limits;

    const size_t max_bytes_before_external_distinct;
    TemporaryDataOnDiskScopePtr tmp_data;
    const size_t min_free_disk_space;
    const size_t max_block_size_rows;
    const DistinctSpillLayout spill_layout;

    /// Counts received rows and provides the next arrival number.
    UInt64 consumed_rows = 0;

    /// Accumulates sorted chunks with unset emitted flags until the next run is written.
    Chunks chunks;
    size_t sum_bytes_in_chunks = 0;

    size_t temporary_files_num = 0;
    std::unique_ptr<MergeSorter> merge_sorter;
    /// Removes duplicates across chunks while writing an ordinary run. Each chunk is already locally
    /// deduplicated by `sortSpillChunk`, so single-chunk runs and suppression rows bypass this filter.
    DistinctSortedFilter run_dedup;
    bool deduplicate_current_run = false;
    std::shared_ptr<MergingSortedTransform> external_merging_sorted;
    std::optional<PendingPipelineUpdate> pending_pipeline_update;

    InputPort * merged_input = nullptr;
    OutputPort * run_write_output = nullptr;
    InputPort * run_completion_input = nullptr;
    OutputPort * run_readiness_output = nullptr;

    Stage stage = Stage::Consume;
    /// The in-memory tail closes merge-input registration exactly once, even when it contains no rows.
    bool merge_inputs_finalized = false;
    /// No more output is needed: the limit hint or a size limit (with the 'break' overflow mode) was
    /// reached. The counterpart of `ISimpleTransform::stopReading`.
    bool read_stopped = false;

    /// Counts distinct rows sent downstream in both phases for limit hints and the row limit.
    size_t emitted_rows = 0;

    /// Retains unprocessed input while the existing set is extracted into suppression runs.
    Chunk pending_input;
    Chunk current_chunk;
    Chunk generated_chunk;

    LoggerPtr log = getLogger("ExternalDistinctTransform");
};

}
