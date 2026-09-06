#pragma once
#include <algorithm>
#include <atomic>
#include <bit>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <unordered_map>

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
    /// Throttle between input chunks, never while a producer owns a shard. The limit is a
    /// high-water mark: producers may overshoot it by the chunks they are already processing.
    struct DictionaryAggregationBackpressure : std::enable_shared_from_this<DictionaryAggregationBackpressure>
    {
        struct DictionaryRetention
        {
            explicit DictionaryRetention(std::shared_ptr<DictionaryAggregationBackpressure> backpressure_)
                : backpressure(std::move(backpressure_))
            {
            }
            ~DictionaryRetention();
            DictionaryRetention(const DictionaryRetention &) = delete;
            DictionaryRetention & operator=(const DictionaryRetention &) = delete;

            std::shared_ptr<DictionaryAggregationBackpressure> backpressure;
            Columns dictionaries;
        };

        DictionaryAggregationBackpressure();

        std::shared_ptr<DictionaryRetention> retainDictionaries(const Columns & columns, const IColumn * grouping_dictionary);
        void add(size_t bytes)
        {
            outstanding_bytes.fetch_add(bytes, std::memory_order_relaxed);
        }
        void release(size_t bytes);
        bool wait();
        void cancel();

    private:
        void releaseDictionaries(Columns & dictionaries);
        struct DictionaryUsage
        {
            size_t references = 0;
            size_t bytes = 0;
        };
        std::unordered_map<const IColumn *, DictionaryUsage> dictionary_usage;

        /// Upper bound; `wait` also accounts for the query memory limit and current headroom.
        size_t high_watermark = 256 << 20;
        std::atomic<size_t> outstanding_bytes = 0;
        std::atomic<size_t> num_waiters = 0;
        std::atomic<bool> cancelled = false;
        std::mutex mutex;
        std::condition_variable cv;
    };

    struct DictionaryAggregationBlock
    {
        /// Shared by every fragment of one input chunk. Destroy columns before releasing its dictionaries.
        std::shared_ptr<DictionaryAggregationBackpressure::DictionaryRetention> retained_dictionaries;
        Columns columns;
        size_t rows = 0;
    };

    struct DictionaryAggregationShard;

    struct DictionaryAggregationLease
    {
        AggregatedDataVariantsPtr variants;
        DictionaryAggregationShard * shard = nullptr;
        std::vector<DictionaryAggregationBlock> blocks;
        size_t bytes = 0;

        explicit operator bool() const { return shard != nullptr; }
    };

    struct DictionaryAggregationShard
    {
        AggregatedDataVariantsPtr variants;
        /// Protects the queue and ownership flag, not aggregation itself. While `is_processing`
        /// is set, exactly one producer owns `variants` and drains everything queued here.
        /// The owner does not return from `consume` until the queue is empty, so the existing
        /// producer finish barrier also guarantees that no queued blocks remain.
        std::mutex mutex;
        std::vector<DictionaryAggregationBlock> pending_blocks;
        size_t pending_bytes = 0;
        bool is_processing = false;
    };

    struct DictionaryAggregationShards
    {
        DictionaryAggregationShards(
            ColumnPtr dictionary_, size_t max_shards, std::shared_ptr<std::atomic<size_t>> shard_budget_);
        ~DictionaryAggregationShards();

        ColumnPtr dictionary;
        /// Keep the slots stable while producers own leases. New slots are initialized
        /// before publishing a larger `num_shards`; unused slots remain null.
        std::vector<std::unique_ptr<DictionaryAggregationShard>> shards;
        std::atomic<size_t> num_shards = 1;
        /// Number of producers retaining this as their current dictionary. Protected by
        /// `dictionary_shards_mutex`; an owner keeps its registration until it drains all leases.
        size_t num_users = 0;

        /// Retain the reservation during retirement, until the tables have been destroyed.
        const std::shared_ptr<std::atomic<size_t>> shard_budget;
    };

    ManyAggregatedDataVariants variants;
    /// One lazy value-keyed table per producer for results from retired dictionaries.
    /// Keep it separate from the initial local index table so that table can still pre-merge
    /// by index if its dictionary reappears at the end of the input.
    ManyAggregatedDataVariants dictionary_aggregation_results;
    std::atomic<UInt32> num_finished = 0;

    /// The number of producers that have to reach the finish barrier in
    /// `AggregatingTransform::initGenerate`, fixed at construction time.
    /// `variants.size()` cannot be used instead: the last finisher appends dictionary tables
    /// and the adaptive aggregation's early-drain routing table.
    const size_t num_producers;

    /// Set when the adaptive aggregation is enabled for this aggregation (see
    /// `AdaptiveAggregationSession`); shared by all the participating transforms.
    AdaptiveAggregationSessionPtr adaptive_session;

    const std::shared_ptr<DictionaryAggregationBackpressure> dictionary_backpressure
        = std::make_shared<DictionaryAggregationBackpressure>();

    explicit ManyAggregatedData(size_t num_threads = 0)
        : variants(num_threads)
        , dictionary_aggregation_results(num_threads)
        , num_producers(num_threads)
        , max_shards_per_dictionary(std::bit_floor(std::max<size_t>(1, std::min(max_dictionary_shards, num_threads))))
        , dictionary_shard_budget(std::make_shared<std::atomic<size_t>>(max_shards_per_dictionary - 1))
    {
        for (auto & elem : variants)
            elem = std::make_shared<AggregatedDataVariants>();
    }

    DictionaryAggregationShards & acquireDictionaryAggregationShards(const ColumnPtr & dictionary);

    /// Drop a producer's registration after it has drained all its leases. The last user
    /// takes exclusive ownership and retires the dictionary outside the registry mutex.
    std::unique_ptr<DictionaryAggregationShards> releaseDictionaryAggregationShards(DictionaryAggregationShards & shards);

    /// Called only after the producer finish barrier or from the destructor.
    void collectDictionaryAggregationVariants();

    DictionaryAggregationLease enqueueDictionaryAggregationBlock(
        DictionaryAggregationShards & shards_for_dictionary,
        size_t shard,
        Columns columns,
        size_t rows,
        size_t bytes,
        std::shared_ptr<DictionaryAggregationBackpressure::DictionaryRetention> retained_dictionaries);

    bool continueDictionaryAggregationShard(DictionaryAggregationLease & lease);

    void releaseDictionaryAggregationShard(DictionaryAggregationLease & lease);

    /// Once shards have been created, their sizes cannot serve as per-producer size hints.
    std::atomic<bool> has_created_dictionary_shards = false;

    ~ManyAggregatedData();

private:
    static constexpr size_t max_dictionary_shards = 64;
    const size_t max_shards_per_dictionary;
    /// Each dictionary gets one shard. At most `num_producers` dictionaries are registered
    /// or being retired, and extra shards share this budget instead of multiplying by it.
    const std::shared_ptr<std::atomic<size_t>> dictionary_shard_budget;

    std::mutex dictionary_shards_mutex;
    std::unordered_map<const IColumn *, std::unique_ptr<DictionaryAggregationShards>> dictionary_shards;
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
  * At aggregation step, every transform normally uses its own AggregatedDataVariants structure.
  * A transform aggregates a single-part `LowCardinality` dictionary into its local variant until
  * its input switches dictionaries. It then retains the local variant and uses bounded shared
  * shards, independently of which transform receives later read tasks. Each producer registers
  * only its current dictionary; the last user of an abandoned dictionary merges its shards into
  * a producer-local value table and releases them. Final dictionaries join the parallel merge.
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
    bool executeDictionaryAggregationLease(ManyAggregatedData::DictionaryAggregationLease lease);
    bool retireCurrentDictionary();

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
    /// `onCancel` can run concurrently with the reset of `many_data` after aggregation.
    /// Keep only the wakeup state alive, not the aggregation tables.
    const std::shared_ptr<ManyAggregatedData::DictionaryAggregationBackpressure> dictionary_backpressure;
    /// Non-owning: completed transforms must not extend these tables' lifetimes after
    /// releasing `many_data`.
    AggregatedDataVariants & variants;
    AggregatedDataVariantsPtr & dictionary_aggregation_result;
    ManyAggregatedData::DictionaryAggregationShards * current_dictionary_shards = nullptr;
    const IColumn * previous_single_dictionary = nullptr;
    bool dictionary_sharding_enabled = false;
    size_t dictionary_shard_offset = 0;

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
