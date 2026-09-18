#pragma once
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

    /// The number of producers that have to reach the finish barrier in
    /// `AggregatingTransform::initGenerate`, fixed at construction time.
    /// The size of `variants` cannot be used instead: the merge can append the adaptive aggregation's
    /// early-drain routing table while other producers are still returning from the barrier.
    const size_t num_producers;

    /// Set when the adaptive aggregation is enabled for this aggregation (see
    /// `AdaptiveAggregationSession`); shared by all the participating transforms.
    AdaptiveAggregationSessionPtr adaptive_session;

    explicit ManyAggregatedData(size_t num_threads = 0) : variants(num_threads), num_producers(num_threads)
    {
        for (auto & elem : variants)
            elem = std::make_shared<AggregatedDataVariants>();
    }

    ~ManyAggregatedData();
};

using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;
using ManyAggregatedDataPtr = std::shared_ptr<ManyAggregatedData>;

/** Aggregates the stream of blocks using the specified key columns and aggregate functions.
  * Columns with aggregate functions are added to the end of the block.
  * If `final = false`, the aggregate functions are not finalized: they are not replaced by their
  * values, but contain intermediate calculation states. This is necessary so that aggregation can
  * continue (for example, by combining streams of partially aggregated data).
  *
  * For every separate stream of data, a separate `AggregatingTransform` is created.
  * Every `AggregatingTransform` reads data from the first port until it runs out, or until
  * `max_rows_to_group_by` is exceeded with `group_by_overflow_mode = 'break'`.
  * When the last `AggregatingTransform` finishes reading and staging, the results must be merged.
  * For in-memory aggregation, this task is performed by `ConvertingAggregatedToChunksTransform`.
  * The last `AggregatingTransform` expands the pipeline and adds an input port, which reads
  * from the merge pipeline.
  *
  * Aggregation data is passed through `ManyAggregatedData`, shared between all aggregating transforms.
  * During aggregation, every transform uses its own `AggregatedDataVariants` structure.
  * During in-memory merging, all structures are passed to `ConvertingAggregatedToChunksTransform`.
  *
  * In adaptive mode, the first block needing staging creates partitioning, coalescing, and publication
  * processors. A separate output sends them aggregate arguments and recorded misses. The producer waits
  * for acknowledgement before checking memory and limits, and for publication to finish before entering
  * the shared finish barrier. The public output always carries aggregation results.
  */
class AggregatingTransform final : public IProcessor
{
public:
    AggregatingTransform(SharedHeader header, AggregatingTransformParamsPtr params_, RuntimeDataflowStatisticsCacheUpdaterPtr updater_);

    /// For parallel aggregation.
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

private:
    size_t getGeneratingStepGroup() const;
    void initGenerate();
    void consume(Chunk & chunk);
    void finishLocalAggregation();

    Status prepareAdaptive();
    void workAdaptive();
    void finishAdaptiveAggregation();

    AggregatingTransformParamsPtr params;
    ManyAggregatedDataPtr many_data;
    AggregatedDataVariants & variants;

    /** Used if there is a limit on the maximum number of rows in the aggregation,
      * and if `group_by_overflow_mode = 'any'`.
      * In this case, new keys are not added to the set, but aggregation is performed only for
      * keys that have already managed to get into the set.
      */
    bool no_more_keys = false;
    bool is_consume_finished = false;
    Chunk current_chunk;
    bool read_current_chunk = false;

    LoggerPtr log = getLogger("AggregatingTransform");
    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns aggregate_columns;
    /// TODO: Calculate time only for aggregation.
    Stopwatch watch;
    UInt64 src_rows = 0;
    UInt64 src_bytes = 0;
    bool is_consume_started = false;
    RowsBeforeStepCounterPtr rows_before_aggregation;

    /// Owns adaptive execution and the ports of the lazily created staging pipeline.
    struct AdaptiveState;
    std::unique_ptr<AdaptiveState> adaptive;

    /// Holds the merge processors, including readers for data flushed into temporary files,
    /// before they are added to the pipeline.
    Processors processors;
    size_t max_threads = 1;
    size_t temporary_data_merge_threads = 1;
    bool should_produce_results_in_order_of_bucket_number = true;
    /// If we aggregate partitioned data, merging is not needed.
    bool skip_merging = false;
    std::atomic_flag is_generate_initialized;
    bool is_pipeline_created = false;
    RuntimeDataflowStatisticsCacheUpdaterPtr updater;
};

Chunk convertToChunk(const Block & block);

}
