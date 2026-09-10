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

    /// Sets the merge transform's input count before final assembly can append the shared drain table.
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

/// Aggregates one input stream into its own variant in `ManyAggregatedData`. With `final = false`,
/// result columns hold aggregate states for subsequent merging.
///
/// Ordinary aggregation uses the last producer to assemble the merge pipeline and forward its
/// results through a second input. Adaptive producers instead emit owned staged payloads to a
/// dedicated admission transform and close their output after admission and local finishing.
/// `AdaptiveAggregationMergeTransform` owns final assembly after all admission streams finish.
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
    size_t getGeneratingStepGroup() const;

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
    /// Owns the outbox and the input storage retained while aggregation waits for admission.
    std::unique_ptr<AdaptiveAggregationExecution> adaptive_execution;
    /// Indexes the next prepared chunk to send through the admission port.
    size_t next_ready_chunk = 0;

    /// Final conversion starts after the last buffered chunks have been admitted.
    enum class AdaptiveFinishStage
    {
        NotStarted,
        AfterFinalFlush,
        Complete,
    };
    AdaptiveFinishStage adaptive_finish_stage = AdaptiveFinishStage::NotStarted;

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

    RuntimeDataflowStatisticsCacheUpdaterPtr updater;

    Status prepareAdaptive();
    void finishLocalAggregation();
    void finishAdaptiveAggregation();
    void initGenerate();
};

/// Assembles the in-memory or external merge after all producers finish. External reader
/// sources own their temporary files for the lifetime of the returned pipeline.
Processors createAggregationMergePipeline(
    const AggregatingTransformParamsPtr & params, const ManyAggregatedDataPtr & many_data,
    size_t max_threads, size_t temporary_data_merge_threads,
    bool should_produce_results_in_order_of_bucket_number, bool skip_merging,
    const RuntimeDataflowStatisticsCacheUpdaterPtr & updater);

Chunk convertToChunk(const Block & block);

}
