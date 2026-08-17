#pragma once
#include <deque>
#include <optional>

#include <Compression/CompressedReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/Aggregator.h>
#include <Processors/Chunk.h>
#include <Processors/IAccumulatingTransform.h>
#include <Processors/RowsBeforeStepCounter.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>


namespace DB
{

class AggregatedChunkInfo final : public ChunkInfoCloneable<AggregatedChunkInfo>
{
public:
    bool is_overflows = false;
    Int32 bucket_num = -1;
    UInt64 chunk_num = 0; // chunk number in order of generation, used during memory bound merging to restore chunks order
    std::vector<Int32> out_of_order_buckets; // out of order buckets for two level aggregation
};

using AggregatorList = std::list<Aggregator>;
using AggregatorListPtr = std::shared_ptr<AggregatorList>;

class RuntimeDataflowStatisticsCacheUpdater;
using RuntimeDataflowStatisticsCacheUpdaterPtr = std::shared_ptr<RuntimeDataflowStatisticsCacheUpdater>;

struct AggregatingTransformParams
{
    Aggregator::Params params;

    /// Each params holds a list of aggregators which are used in query. It's needed because we need
    /// to use a pointer of aggregator to proper destroy complex aggregation states on exception
    /// (See comments in AggregatedDataVariants). However, this pointer might not be valid because
    /// we can have two different aggregators at the same time due to mixed pipeline of aggregate
    /// projections, and one of them might gets destroyed before used.
    AggregatorListPtr aggregator_list_ptr;
    Aggregator & aggregator;
    bool final;
    Block header;

    AggregatingTransformParams(SharedHeader header_, const Aggregator::Params & params_, bool final_)
        : params(params_)
        , aggregator_list_ptr(std::make_shared<AggregatorList>())
        , aggregator(*aggregator_list_ptr->emplace(aggregator_list_ptr->end(), *header_, params))
        , final(final_)
        , header(*header_)
    {
    }

    AggregatingTransformParams(
        const Block & header_, const Aggregator::Params & params_, const AggregatorListPtr & aggregator_list_ptr_, bool final_)
        : params(params_)
        , aggregator_list_ptr(aggregator_list_ptr_)
        , aggregator(*aggregator_list_ptr->emplace(aggregator_list_ptr->end(), header_, params))
        , final(final_)
        , header(header_)
    {
    }

    Block getHeader() const { return params.getHeader(header, final); }

    Block getCustomHeader(bool final_) const { return params.getHeader(header, final_); }
};

struct ManyAggregatedData
{
    ManyAggregatedDataVariants variants;
    std::atomic<UInt32> num_finished = 0;

    /// Set when the adaptive aggregation is enabled for this aggregation (see
    /// `AdaptiveAggregationSession`); shared by all the participating transforms.
    AdaptiveAggregationSessionPtr adaptive_session;

    explicit ManyAggregatedData(size_t num_threads = 0) : variants(num_threads)
    {
        for (auto & elem : variants)
            elem = std::make_shared<AggregatedDataVariants>();
    }

    ~ManyAggregatedData();
};

using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;
using ManyAggregatedDataPtr = std::shared_ptr<ManyAggregatedData>;

/// Carries one sealed staged chunk of the adaptive aggregation through the pipeline: the
/// producing AggregatingTransform emits it as a rowless chunk (the header's columns, all
/// empty) and the staged-chunk store absorbs it into the session backlog. The chunk is
/// mutable because the store finishes it (builds its instruction preparation) at absorption;
/// in flight it is exclusively owned.
class StagedChunkInfo final : public ChunkInfoCloneable<StagedChunkInfo>
{
public:
    explicit StagedChunkInfo(MutableStagedChunkPtr chunk_) : chunk(std::move(chunk_)) { }

    MutableStagedChunkPtr chunk;
};

/** Aggregates the stream of blocks using the specified key columns and aggregate functions.
  * Columns with aggregate functions adds to the end of the block.
  * If final = false, the aggregate functions are not finalized, that is, they are not replaced by their value, but contain an intermediate state of calculations.
  * This is necessary so that aggregation can continue (for example, by combining streams of partially aggregated data).
  *
  * For every separate stream of data separate AggregatingTransform is created.
  * Every AggregatingTransform reads data from the first port till is is not run out, or max_rows_to_group_by reached.
  * When the last AggregatingTransform finish reading, the result of aggregation is needed to be merged together.
  * This task is performed by ConvertingAggregatedToChunksTransform.
  * Last AggregatingTransform expands pipeline and adds second input port, which reads from ConvertingAggregated.
  *
  * Aggregation data is passed by ManyAggregatedData structure, which is shared between all aggregating transforms.
  * At aggregation step, every transform uses it's own AggregatedDataVariants structure.
  * At merging step, all structures pass to ConvertingAggregatedToChunksTransform.
  */
class AggregatingTransform final : public IProcessor
{
public:
    AggregatingTransform(SharedHeader header, AggregatingTransformParamsPtr params_, RuntimeDataflowStatisticsCacheUpdaterPtr updater_);

    /// For Parallel aggregating.
    AggregatingTransform(
        SharedHeader header,
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataPtr many_data,
        size_t current_variant,
        size_t max_threads,
        size_t temporary_data_merge_threads,
        bool should_produce_results_in_order_of_bucket_number_ = true,
        bool skip_merging_ = false,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater_ = nullptr);

    ~AggregatingTransform() override;

    String getName() const override { return "AggregatingTransform"; }
    Status prepare() override;
    void work() override;
    PipelineUpdate updatePipeline() override;
    void setRowsBeforeAggregationCounter(RowsBeforeStepCounterPtr counter) override { rows_before_aggregation.swap(counter); }
    void onCancel() noexcept override;

protected:
    void consume(Chunk chunk);

private:

    /// To read the data that was flushed into the temporary data file.
    Processors processors;

    AggregatingTransformParamsPtr params;
    LoggerPtr log = getLogger("AggregatingTransform");

    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns aggregate_columns;

    /** Used if there is a limit on the maximum number of rows in the aggregation,
     *   and if group_by_overflow_mode == ANY.
     *  In this case, new keys are not added to the set, but aggregation is performed only by
     *   keys that have already managed to get into the set.
     */
    bool no_more_keys = false;

    ManyAggregatedDataPtr many_data;
    AggregatedDataVariants & variants;

    /// Per-transform context of the adaptive aggregation; engaged when the shared state exists
    /// on `many_data`. Held by pointer: the producer's definition stays out of this widely
    /// included header (see `AdaptiveAggregationImpl.h`).
    std::unique_ptr<AdaptiveAggregationProducer> adaptive_context;

    size_t max_threads = 1;
    size_t temporary_data_merge_threads = 1;
    bool should_produce_results_in_order_of_bucket_number = true;
    /// If we aggregate partitioned data merging is not needed.
    bool skip_merging = false;

    /// TODO: calculate time only for aggregation.
    Stopwatch watch;

    UInt64 src_rows = 0;
    UInt64 src_bytes = 0;

    std::atomic_flag is_generate_initialized;
    bool is_consume_finished = false;
    bool is_pipeline_created = false;

    Chunk current_chunk;
    bool read_current_chunk = false;

    bool is_consume_started = false;

    RowsBeforeStepCounterPtr rows_before_aggregation;

    std::list<TemporaryBlockStreamHolder> tmp_files;

    RuntimeDataflowStatisticsCacheUpdaterPtr updater;

    /// The adaptive path: sealed staged chunks waiting to be pushed through the output port
    /// to the staged-chunk store (see `AdaptiveAggregationMergeTransform`), and the flag that
    /// this producer ran its finish work (flush, own spill, countdown). On this path the
    /// transform never assembles or forwards the merge - the store does.
    std::deque<Chunk> staged_outbox;
    bool adaptive_producer_finished = false;

    Status prepareAdaptive();
    void finishAdaptiveProducer();
    void logAggregatedAndSpillOwnVariants();

    void initGenerate();
};

/// The staged-chunk store and finish barrier of the adaptive aggregation. Every producing
/// AggregatingTransform emits its sealed staged chunks through its output port; this
/// processor absorbs them into the session backlog (it is the backlog's only writer, so
/// publication needs no cross-producer coordination). A producer closes its port only after
/// it flushed its staging and finished its local table, so once every input is finished the
/// aggregation is complete: the store then assembles the merge - exactly the work the
/// last-finishing AggregatingTransform does on the non-adaptive path - and forwards the
/// merged output through its own port.
class AdaptiveAggregationMergeTransform final : public IProcessor
{
public:
    AdaptiveAggregationMergeTransform(
        SharedHeader header,
        size_t num_inputs,
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataPtr many_data_,
        size_t max_threads_,
        size_t temporary_data_merge_threads_,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater_);

    String getName() const override { return "AdaptiveAggregationMerge"; }
    Status prepare() override;
    void work() override;
    PipelineUpdate updatePipeline() override;

private:
    void assembleMerge();

    /// The processors of the assembled merge, handed to the executor via `updatePipeline`.
    Processors processors;

    AggregatingTransformParamsPtr params;
    LoggerPtr log = getLogger("AdaptiveAggregationMergeTransform");

    ManyAggregatedDataPtr many_data;
    const size_t num_producers;
    size_t max_threads;
    size_t temporary_data_merge_threads;
    RuntimeDataflowStatisticsCacheUpdaterPtr updater;

    /// Keeps the spilled files of an external merge alive for the sources reading them.
    std::list<TemporaryBlockStreamHolder> tmp_files;

    Chunk chunk_to_absorb;
    bool has_chunk_to_absorb = false;
    bool merge_assembled = false;
    bool is_pipeline_created = false;
};

Chunk convertToChunk(const Block & block);

}
