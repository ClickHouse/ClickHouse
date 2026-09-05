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
/// external-memory threshold. Its set retains extractable keys, including serialized keys when needed.
///
/// At the first spill, the set's keys become a sorted suppression run carrying already-emitted flags.
/// The set is released after extraction. Further input becomes sorted, locally deduplicated runs, and
/// output waits until all input has been consumed. `DistinctSpillLayout` owns the column conversions.
///
/// `MergingSortedTransform` merges the runs, including any in-memory tail, and `DistinctSortedFilter`
/// removes duplicate keys and keys emitted before spilling. The suppression run is merge input zero:
/// the merge's input-index tie-break puts its flagged rows first in every equal-key range. Ordinary
/// runs follow in arrival order, preserving the first payload among equal keys.
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
        Generate,
        Serialize,
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
        ProcessorPtr sink;
        ProcessorPtr source;
        Processors merged_stream;
        Processors processors;
    };

    Status prepareConsume();
    Status prepareSerialize();
    Status prepareGenerate();

    void consume(Chunk chunk);
    void serialize();
    void generate();

    /// Stably sorts suppression rows and stably sorts and deduplicates ordinary input rows.
    Chunk sortSpillChunk(Chunk chunk, bool already_emitted) const;

    void startFirstSpill();
    void startSpillRun(Chunks run_chunks, size_t run_bytes, bool is_first_run);
    void createMergedStream(PendingPipelineUpdate & update);
    void connectMergedStream(const Processors & merged_stream);
    void attachSpilledRun(const ProcessorPtr & source, const ProcessorPtr & sink);
    void attachInMemoryTail(const ProcessorPtr & source);
    /// Returns the minimum run size, also used by the sort that restores input order.
    size_t minBytesInRun() const;

    /// Owns hashing state until the first spill. Resetting it permanently ends the hashing phase.
    std::optional<DistinctSetFilter> distinct_set;
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
    bool current_run_is_deduplicated = false;
    std::shared_ptr<MergingSortedTransform> external_merging_sorted;
    std::optional<PendingPipelineUpdate> pending_pipeline_update;

    Stage stage = Stage::Consume;
    /// The in-memory tail closes merge-input registration exactly once, even when it contains no rows.
    bool merge_inputs_finalized = false;
    /// No more output is needed: the limit hint or a size limit (with the 'break' overflow mode) was
    /// reached. The counterpart of `ISimpleTransform::stopReading`.
    bool read_stopped = false;

    /// Counts distinct rows sent downstream in both phases. Post-spill this is exactly what the cardinality
    /// of the `DISTINCT` hash set would have been, so the rows limit is enforced against it.
    size_t emitted_rows = 0;

    Chunk current_chunk;
    Chunk generated_chunk;

    LoggerPtr log = getLogger("ExternalDistinctTransform");
};

}
