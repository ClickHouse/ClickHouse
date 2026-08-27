#pragma once
#include <mutex>
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
    /// Shared state of the kept-keys cutoff (`Aggregator::Params::shared_kept_keys_for_overflow_any`).
    ///
    /// The streams aggregate completely normally until the first stream exceeds
    /// `max_rows_to_group_by` (`checkLimits` sets its `no_more_keys` in `ANY` mode). That stream
    /// publishes a `seed` block of exactly `max_rows_to_group_by` of its keys with empty aggregate
    /// states and sets `frozen`. Every stream then rebuilds its `AggregatedDataVariants` to exactly
    /// the kept key set — before consuming its next chunk, or at merge preparation for the streams
    /// that had already finished — keeping the states it accumulated for the kept keys and dropping
    /// the rest, and continues on the regular `no_more_keys` path against the rebuilt table.
    ///
    /// The merged values of the kept keys are exact: every row consumed before the stream applied
    /// the cutoff was aggregated normally, and every later row of a kept key finds the key in the
    /// rebuilt table. Rows of the dropped keys are irrelevant — the keys are absent from every
    /// rebuilt table, so they never reach the merged result (an unspecified subset of the groups
    /// is a valid result for the LIMIT-without-ORDER-BY queries this serves).
    struct SharedKeptKeys
    {
        std::mutex mutex;
        std::atomic<bool> frozen{false};
        /// Kept keys + empty aggregate states in the mergeable block layout.
        /// Written once under `mutex`; immutable after `frozen` is set (readers synchronize
        /// with an acquire load of `frozen`).
        Block seed;
        /// Per-variant: the variant was rebuilt to the kept key set. Written only by the variant's
        /// owning stream during consumption; read by the last finishing stream in `initGenerate`,
        /// synchronized via `num_finished`.
        std::vector<char> applied;
    };

    ManyAggregatedDataVariants variants;
    std::atomic<UInt32> num_finished = 0;
    std::shared_ptr<SharedKeptKeys> shared_kept_keys;

    /// The number of producers that have to reach the finish barrier in
    /// `AggregatingTransform::initGenerate`, fixed at construction time.
    /// `variants.size()` cannot be used instead: the last finisher appends the adaptive
    /// aggregation's early-drain routing table to `variants`, and reading the size of a vector
    /// that is concurrently grown is a data race.
    const size_t num_producers;

    /// Set when the adaptive aggregation is enabled for this aggregation (see
    /// `AdaptiveAggregationSession`); shared by all the participating transforms.
    AdaptiveAggregationSessionPtr adaptive_session;

    explicit ManyAggregatedData(size_t num_threads = 0) : variants(num_threads), num_producers(num_threads)
    {
        for (auto & elem : variants)
            elem = std::make_shared<AggregatedDataVariants>();
    }

    void enableSharedKeptKeys()
    {
        shared_kept_keys = std::make_shared<SharedKeptKeys>();
        shared_kept_keys->applied.resize(variants.size(), 0);
    }

    ~ManyAggregatedData();
};

using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;
using ManyAggregatedDataPtr = std::shared_ptr<ManyAggregatedData>;

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
    size_t getGeneratingStepGroup() const;

    /// Rebuilds `variants` to the shared kept key set (see ManyAggregatedData::SharedKeptKeys).
    /// With `may_freeze` (this stream has just exceeded `max_rows_to_group_by`), publishes the
    /// kept key set first unless another stream has already frozen it.
    void applySharedKeptKeysCutoff(bool may_freeze);

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
    /// Index of `variants` in `many_data->variants` (for `SharedKeptKeys::applied`).
    size_t variant_index = 0;

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

    void initGenerate();
};

Chunk convertToChunk(const Block & block);

}
