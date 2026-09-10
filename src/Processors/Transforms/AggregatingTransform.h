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

/// Shares local aggregation and final spilling between ordinary and adaptive producers.
class AggregatingTransformBase : public IProcessor
{
public:
    void setRowsBeforeAggregationCounter(RowsBeforeStepCounterPtr counter) override { rows_before_aggregation.swap(counter); }

protected:
    AggregatingTransformBase(
        SharedHeader input_header, SharedHeader output_header, AggregatingTransformParamsPtr params_,
        ManyAggregatedDataPtr many_data_, size_t current_variant);

    void consume(Chunk & chunk, AdaptiveAggregationExecution * execution = nullptr);
    void finishLocalAggregation();

    AggregatingTransformParamsPtr params;
    ManyAggregatedDataPtr many_data;
    AggregatedDataVariants & variants;

    /// Stops inserting new keys when the group limit is reached with overflow mode `ANY`.
    bool no_more_keys = false;
    bool is_consume_finished = false;
    Chunk current_chunk;
    bool read_current_chunk = false;

private:
    LoggerPtr log = getLogger("AggregatingTransformBase");
    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns aggregate_columns;
    Stopwatch watch;
    UInt64 src_rows = 0;
    UInt64 src_bytes = 0;
    bool is_consume_started = false;
    RowsBeforeStepCounterPtr rows_before_aggregation;
};

/// Aggregates one input stream into its own variant in `ManyAggregatedData`. The last producer
/// assembles the merge pipeline and forwards its results through a second input. With `final = false`,
/// result columns hold aggregate states for subsequent merging.
class AggregatingTransform final : public AggregatingTransformBase
{
public:
    AggregatingTransform(SharedHeader header, AggregatingTransformParamsPtr params_, RuntimeDataflowStatisticsCacheUpdaterPtr updater_);

    /// Aggregates one producer's input and participates in the shared final merge.
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

    String getName() const override { return "AggregatingTransform"; }
    Status prepare() override;
    void work() override;
    PipelineUpdate updatePipeline() override;

private:
    size_t getGeneratingStepGroup() const;
    void initGenerate();

    /// Holds the merge processors before they are added to the pipeline.
    Processors processors;
    size_t max_threads = 1;
    size_t temporary_data_merge_threads = 1;
    bool should_produce_results_in_order_of_bucket_number = true;
    /// Partitioned aggregation can produce its results without merging producer tables.
    bool skip_merging = false;
    std::atomic_flag is_generate_initialized;
    bool is_pipeline_created = false;
    RuntimeDataflowStatisticsCacheUpdaterPtr updater;
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
