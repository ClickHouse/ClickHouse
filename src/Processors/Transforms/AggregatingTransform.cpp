#include <bit>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnDecimal.h>
#include <Processors/Transforms/AggregatingTransform.h>

#include <Interpreters/AdaptiveAggregationImpl.h>

#include <Common/CurrentThread.h>
#include <Core/ProtocolDefines.h>
#include <Formats/NativeReader.h>
#include <Processors/Chunk.h>
#include <Processors/ISource.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <Processors/Transforms/SquashingTransform.h>
#include <QueryPipeline/Pipe.h>
#include <base/types.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadPool.h>
#include <Processors/QueryPlan/AggregatingStep.h>

#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>

#include <algorithm>
#include <atomic>

namespace CurrentMetrics
{
    extern const Metric DestroyAggregatesThreads;
    extern const Metric DestroyAggregatesThreadsActive;
    extern const Metric DestroyAggregatesThreadsScheduled;
}

namespace ProfileEvents
{
    extern const Event ExternalAggregationMerge;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
    extern const int LOGICAL_ERROR;
}

ManyAggregatedData::~ManyAggregatedData()
{
    try
    {
        if (variants.size() <= 1)
            return;

        // Aggregation states destruction may be very time-consuming.
        // In the case of a query with LIMIT, most states won't be destroyed during conversion to blocks.
        // Without the following code, they would be destroyed in the destructor of AggregatedDataVariants in the current thread (i.e. sequentially).
        const auto pool = std::make_unique<ThreadPool>(
            CurrentMetrics::DestroyAggregatesThreads,
            CurrentMetrics::DestroyAggregatesThreadsActive,
            CurrentMetrics::DestroyAggregatesThreadsScheduled,
            variants.size());

        for (auto && variant : variants)
        {
            if (variant->size() < 100'000) // some seemingly reasonable constant
                continue;

            // It doesn't make sense to spawn a thread if the variant is not going to actually destroy anything.
            if (variant->aggregator)
            {
                pool->scheduleOrThrowOnError(
                    [my_variant = std::move(variant), thread_group = CurrentThread::getGroup()]() mutable
                    {
                        ThreadGroupSwitcher switcher(thread_group, ThreadName::AGGREGATOR_DESTRUCTION);
                        my_variant.reset();
                    });
            }
        }

        pool->wait();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

/// Convert block to chunk.
/// Adds additional info about aggregation.
Chunk convertToChunk(const Block & block)
{
    auto info = std::make_shared<AggregatedChunkInfo>();
    info->bucket_num = block.info.bucket_num;
    info->is_overflows = block.info.is_overflows;
    info->out_of_order_buckets = block.info.out_of_order_buckets;

    UInt64 num_rows = block.rows();
    Chunk chunk(block.getColumns(), num_rows);
    chunk.getChunkInfos().add(std::move(info));

    return chunk;
}

static Chunk convertToChunk(Aggregator::AggregatedChunk && agg_chunk)
{
    auto info = std::make_shared<AggregatedChunkInfo>();
    info->bucket_num = agg_chunk.bucket_num;
    info->is_overflows = agg_chunk.is_overflows;

    agg_chunk.chunk.getChunkInfos().add(std::move(info));
    return std::move(agg_chunk.chunk);
}

namespace
{
    const AggregatedChunkInfo * getInfoFromChunk(const Chunk & chunk)
    {
        auto agg_info = chunk.getChunkInfos().get<AggregatedChunkInfo>();
        if (!agg_info)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk should have AggregatedChunkInfo.");

        return agg_info.get();
    }

    /// Reads chunks from file in native format. Provide chunks with aggregation info.
    class SourceFromNativeStream final : public ISource
    {
    public:
        explicit SourceFromNativeStream(SharedHeader header, TemporaryBlockStreamReaderHolder tmp_stream_)
            : ISource(header)
            , tmp_stream(std::move(tmp_stream_))
        {}

        String getName() const override { return "SourceFromNativeStream"; }

        Chunk generate() override
        {
            if (!tmp_stream)
                return {};

            auto block = tmp_stream->read();
            if (block.empty())
            {
                tmp_stream.reset();
                return {};
            }
            return convertToChunk(block);
        }

        std::optional<ReadProgress> getReadProgress() override { return std::nullopt; }

    private:
        TemporaryBlockStreamReaderHolder tmp_stream;
    };
}

/// Worker which merges states for single-level aggregation of FixedHashMap.
/// Each worker is assigned to a subset of the keys, so that we can merge in-place without race conditions.
class ConvertingAggregatedToChunksWithMergingSourceForFixedHashMap final : public ISource
{
public:
    struct SharedData
    {
        std::atomic<bool> is_cancelled = false;
    };

    using SharedDataPtr = std::shared_ptr<SharedData>;

    ConvertingAggregatedToChunksWithMergingSourceForFixedHashMap(AggregatingTransformParamsPtr params_, ManyAggregatedDataVariantsPtr data_, UInt32 thread_index_, UInt32 num_threads_, Arena * arena_)
        : ISource(std::make_shared<const Block>(params_->getHeader()), false)
        , params(std::move(params_))
        , data(std::move(data_))
        , shared_data(std::make_shared<SharedData>())
        , thread_index(thread_index_)
        , num_threads(num_threads_)
        , arena(arena_)
    {
    }

    String getName() const override { return "ConvertingAggregatedToChunksWithMergingSourceForFixedHashMap"; }

protected:
    Chunk generate() override
    {
        params->aggregator.mergeSingleLevelDataImplFixedMap(*data, arena, thread_index, num_threads, shared_data->is_cancelled);

        finished = true;
        data.reset();
        return Chunk{};
    }

private:
    AggregatingTransformParamsPtr params;
    ManyAggregatedDataVariantsPtr data;
    SharedDataPtr shared_data;
    UInt32 thread_index;
    UInt32 num_threads;
    Arena * arena;
};

/// Worker which merges buckets for two-level aggregation.
/// Atomically increments bucket counter and returns merged result.
class ConvertingAggregatedToChunksWithMergingSource final : public ISource
{
public:
    static constexpr UInt32 NUM_BUCKETS = 256;

    struct SharedData
    {
        std::atomic<UInt32> next_bucket_to_merge = 0;
        std::array<std::atomic<bool>, NUM_BUCKETS> is_bucket_processed{};
        std::atomic<bool> is_cancelled = false;

        /// Rows merged so far across all partitions of the parallel single-level merge; the group
        /// limit is checked against this running total.
        std::atomic<size_t> single_level_merged_rows = 0;

        /// Groups the two-level bucket merge has converted so far; a throw-mode group limit is
        /// checked against this running total, taken from the bucket tables rather than from
        /// the converted chunks, which the bucket-local Top-K conversion truncates. For the
        /// adaptive aggregator this is the only enforcement the staged keys ever get: the
        /// frozen tables are bounded and the staged cardinality is unknown until the merge.
        /// The buckets partition the key space, so the sum counts every group exactly once.
        std::atomic<size_t> two_level_merged_groups = 0;

        SharedData()
        {
            for (auto & flag : is_bucket_processed)
                flag = false;
        }
    };

    using SharedDataPtr = std::shared_ptr<SharedData>;

    ConvertingAggregatedToChunksWithMergingSource(
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataVariantsPtr data_,
        SharedDataPtr shared_data_,
        Arena * arena_,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater_,
        AdaptiveAggregationSessionPtr adaptive_session_)
        : ISource(std::make_shared<const Block>(params_->getHeader()), false)
        , params(std::move(params_))
        , data(std::move(data_))
        , shared_data(std::move(shared_data_))
        , arena(arena_)
        , updater(std::move(updater_))
        , adaptive_session(std::move(adaptive_session_))
    {
    }

    String getName() const override { return "ConvertingAggregatedToChunksWithMergingSource"; }

    void cancel(CancelReason reason) noexcept override
    {
        /// When 2-level aggregation is being used ConvertingAggregatedToChunksTransform expects
        /// to receive data from all sources, so we do not need to stop the processor here.
        if (reason == CancelReason::PartialResult)
            return;

        ISource::cancel(reason);
    }

protected:
    Chunk generate() override
    {
        UInt32 bucket_num = shared_data->next_bucket_to_merge.fetch_add(1);

        if (bucket_num >= NUM_BUCKETS)
        {
            data.reset();
            return {};
        }

        /// The adaptive merge gives every bucket its own arena (see the setup in
        /// `createSources`), so a retired bucket's drained and merged states free with its
        /// slot instead of accumulating until the whole merge ends.
        Arena * bucket_arena = arena;
        if (adaptive_session)
        {
            bucket_arena = data->at(0)->adaptive_merge_bucket_arenas[bucket_num].get();
            params->aggregator.drainAdaptiveBucketForMerge(*data->at(0), bucket_arena, bucket_num, *adaptive_session, shared_data->is_cancelled);
        }

        /// The bucket's group count is taken from the table rather than from the chunk: the
        /// bucket-local Top-K conversion truncates the chunk to its n best groups, and the
        /// group-by limit must be enforced against the true cardinality.
        size_t full_group_count = 0;
        auto agg_chunk = params->aggregator.mergeAndConvertOneBucketToChunk(
            *data, bucket_arena, params->final, bucket_num, shared_data->is_cancelled, updater, &full_group_count);
        Chunk chunk = convertToChunk(std::move(agg_chunk));

        /// A throw-mode group limit is enforced against the merged totals for every run: the
        /// baseline producers' checks cannot see the merged cardinality (their tables are
        /// checked one by one), and the adaptive producers' checks cannot see the staged keys
        /// at all, so this is where the limit catches what they miss. The dropping modes keep
        /// the merge untouched: their contract is decided at the producers, and stopping the
        /// merge here would drop already-aggregated groups.
        if (params->params.max_rows_to_group_by != 0 && params->params.group_by_overflow_mode == OverflowMode::THROW
            && !shared_data->is_cancelled.load(std::memory_order_seq_cst))
        {
            bool no_more_keys = false;
            const size_t total = shared_data->two_level_merged_groups.fetch_add(full_group_count) + full_group_count;
            params->aggregator.checkLimits(total, no_more_keys);
        }

        /// Retire the bucket's working memory only after a successful conversion: the output
        /// chunk either copied the values out or captured the arena slot's ownership. A throw
        /// above or a cancellation skips retirement and leaves everything to the ordinary
        /// destruction of the variants, which still owns every non-retired slot.
        if (adaptive_session && !shared_data->is_cancelled.load(std::memory_order_seq_cst))
            params->aggregator.retireAdaptiveMergedBucket(*data->at(0), *adaptive_session, bucket_num);

        shared_data->is_bucket_processed[bucket_num] = true;

        return chunk;
    }

private:
    AggregatingTransformParamsPtr params;
    ManyAggregatedDataVariantsPtr data;
    SharedDataPtr shared_data;
    Arena * arena;
    RuntimeDataflowStatisticsCacheUpdaterPtr updater;
    AdaptiveAggregationSessionPtr adaptive_session;
};

/// Worker of the parallel single-level merge: atomically takes the next hash partition, merges it out of
/// every per-thread table and emits it as one chunk.
class ConvertingAggregatedToChunksByPartitionMergingSource final : public ISource
{
public:
    using SharedDataPtr = ConvertingAggregatedToChunksWithMergingSource::SharedDataPtr;

    ConvertingAggregatedToChunksByPartitionMergingSource(
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataVariantsPtr data_,
        SharedDataPtr shared_data_,
        UInt32 num_partitions_,
        size_t max_source_table_size_,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater_)
        : ISource(std::make_shared<const Block>(params_->getHeader()), false)
        , params(std::move(params_))
        , data(std::move(data_))
        , shared_data(std::move(shared_data_))
        , num_partitions(num_partitions_)
        , max_source_table_size(max_source_table_size_)
        , updater(std::move(updater_))
    {
    }

    String getName() const override { return "ConvertingAggregatedToChunksByPartitionMergingSource"; }

    void cancel(CancelReason reason) noexcept override
    {
        if (reason == CancelReason::PartialResult)
            return;

        ISource::cancel(reason);
    }

protected:
    Chunk generate() override
    {
        UInt32 partition = shared_data->next_bucket_to_merge.fetch_add(1);

        if (partition >= num_partitions)
        {
            data.reset();
            return {};
        }

        /// The serial merge stops merging further tables once the group limit breaks; the parallel
        /// equivalent is to stop taking further partitions once the merged total has crossed.
        bool no_more_keys = false;
        if (!params->aggregator.checkLimits(shared_data->single_level_merged_rows.load(), no_more_keys))
        {
            data.reset();
            return {};
        }

        auto agg_chunk = params->aggregator.mergeSingleLevelPartitionAndConvertToChunk(
            *data, params->final, partition, num_partitions, max_source_table_size, shared_data->is_cancelled, updater);

        /// Under the `throw` overflow mode this raises as soon as the running total exceeds the
        /// limit — the same condition on which the serial merge would have thrown between tables.
        const size_t total = shared_data->single_level_merged_rows.fetch_add(agg_chunk.chunk.getNumRows()) + agg_chunk.chunk.getNumRows();
        params->aggregator.checkLimits(total, no_more_keys);

        return convertToChunk(std::move(agg_chunk));
    }

private:
    AggregatingTransformParamsPtr params;
    ManyAggregatedDataVariantsPtr data;
    SharedDataPtr shared_data;
    UInt32 num_partitions;
    size_t max_source_table_size;
    RuntimeDataflowStatisticsCacheUpdaterPtr updater;
};

/// Asks Aggregator to convert accumulated aggregation state into blocks (without merging) and pushes them to later steps.
class ConvertingAggregatedToChunksSource final : public ISource
{
public:
    ConvertingAggregatedToChunksSource(AggregatingTransformParamsPtr params_, AggregatedDataVariantsPtr variant_)
        : ISource(std::make_shared<const Block>(params_->getHeader()), false), params(params_), variant(variant_)
    {
    }

    String getName() const override { return "ConvertingAggregatedToChunksSource"; }

protected:
    Chunk generate() override
    {
        if (variant->isTwoLevel())
        {
            if (current_bucket_num < NUM_BUCKETS)
            {
                Arena * arena = variant->aggregates_pool;
                auto agg_chunk = params->aggregator.convertOneBucketToChunk(*variant, arena, params->final, current_bucket_num++);
                return convertToChunk(std::move(agg_chunk));
            }
        }
        else if (!single_level_converted)
        {
            auto agg_chunk = params->aggregator.prepareChunkAndFillSingleLevel<true /* return_single_block */>(*variant, params->final);
            single_level_converted = true;
            return convertToChunk(std::move(agg_chunk));
        }

        variant.reset();

        return {};
    }

private:
    static constexpr UInt32 NUM_BUCKETS = 256;

    AggregatingTransformParamsPtr params;
    AggregatedDataVariantsPtr variant;

    UInt32 current_bucket_num = 0;
    bool single_level_converted = false;
};

/// Reads chunks from GroupingAggregatedTransform (stored in ChunksToMerge structure) and outputs them.
class FlattenChunksToMergeTransform final : public IProcessor
{
public:
    explicit FlattenChunksToMergeTransform(const Block & input_header, const Block & output_header)
        : IProcessor({input_header}, {output_header})
    {
    }

    String getName() const override { return "FlattenChunksToMergeTransform"; }

private:
    void work() override
    {
    }

    void process(Chunk && chunk)
    {
        auto chunks_to_merge = chunk.getChunkInfos().get<ChunksToMerge>();
        if (!chunks_to_merge)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected chunk with ChunksToMerge info in {}", getName());

        /// This transform drops the `ChunksToMerge` wrapper and emits the chunks it holds, so the ids of the
        /// buckets which `GroupingAggregatedTransform` still owes would be lost. It is used only over
        /// `ConvertingAggregatedToChunksSource`, which produces the buckets in order of their id-s and never
        /// delays any of them, so there is nothing to report and nothing to lose. If that ever changes, the
        /// chunks have to be stamped here with `chunks_to_merge->out_of_order_buckets`, otherwise the node
        /// which merges this result can finalize a bucket before all of its data is sent.
        chassert(chunks_to_merge->out_of_order_buckets.empty());

        if (chunks_to_merge->chunks)
            for (auto & cur_chunk : *chunks_to_merge->chunks)
                chunks.emplace_back(std::move(cur_chunk));
    }

    Status prepare() override
    {
        auto & input = inputs.front();
        auto & output = outputs.front();

        if (output.isFinished())
        {
            input.close();
            return Status::Finished;
        }

        if (!output.canPush())
        {
            input.setNotNeeded();
            return Status::PortFull;
        }

        if (!chunks.empty())
        {
            output.push(std::move(chunks.front()));
            chunks.pop_front();

            if (!chunks.empty())
                return Status::Ready;
        }

        if (input.isFinished() && chunks.empty())
        {
            output.finish();
            return Status::Finished;
        }

        if (input.isFinished())
            return Status::Ready;

        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        Chunk chunk = input.pull(true /* set_not_needed */);
        process(std::move(chunk));

        return Status::Ready;
    }

    std::list<Chunk> chunks;
};

/// Generates chunks with aggregated data.
/// In single level case, aggregates data itself.
/// In two-level case, creates `ConvertingAggregatedToChunksWithMergingSource` workers:
///
/// ConvertingAggregatedToChunksWithMergingSource ->
/// ConvertingAggregatedToChunksWithMergingSource -> ConvertingAggregatedToChunksTransform -> AggregatingTransform
/// ConvertingAggregatedToChunksWithMergingSource ->
///
/// Result chunks guaranteed to be sorted by bucket number.
class ConvertingAggregatedToChunksTransform final : public IProcessor
{
public:
    ConvertingAggregatedToChunksTransform(
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataVariantsPtr data_,
        size_t num_threads_,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater_,
        AdaptiveAggregationSessionPtr adaptive_session_)
        : IProcessor({}, {params_->getHeader()})
        , params(std::move(params_))
        , data(std::move(data_))
        , shared_data(std::make_shared<ConvertingAggregatedToChunksWithMergingSource::SharedData>())
        , num_threads(num_threads_)
        , updater(std::move(updater_))
        , adaptive_session(std::move(adaptive_session_))
    {
    }

    String getName() const override { return "ConvertingAggregatedToChunksTransform"; }

    void work() override
    {
        if (data->empty())
        {
            finished = true;
            return;
        }

        if (!is_initialized)
        {
            initialize();
            return;
        }

        if (data->at(0)->isTwoLevel())
        {
            /// In two-level case will only create sources.
            if (inputs.empty())
                createSources();
        }
        else if (parallelize_single_level_merge || worthParallelMergeSingleLevel())
        {
            if (!parallelize_single_level_merge)
            {
                parallelize_single_level_merge = true;
                LOG_TRACE(getLogger("AggregatingTransform"), "Use parallel merge for single level fixed hash map.");
            }
            if (inputs.empty())
                createSourcesForFixedHashMap();
            else
                mergeSingleLevel();
        }
        else if (parallel_partition_merge_started || worthParallelPartitionMergeSingleLevel())
        {
            if (!parallel_partition_merge_started)
            {
                parallel_partition_merge_started = true;
                LOG_TRACE(getLogger("AggregatingTransform"), "Use parallel hash-partition merge for single level aggregation data.");
            }
            if (inputs.empty())
                createSourcesForPartitionMerge();
        }
        else
        {
            mergeSingleLevel();
        }
    }

    PipelineUpdate updatePipeline() override
    {
        for (auto & source : processors)
        {
            auto & out = source->getOutputs().front();
            inputs.emplace_back(out.getHeader(), this);
            connect(out, inputs.back());
            inputs.back().setNeeded();
            source->inheritQueryPlanStepFromParent(*this, getQueryPlanStepGroup());
        }

        return PipelineUpdate{.to_add = std::move(processors), .to_remove = {}};
    }

    IProcessor::Status prepare() override
    {
        auto & output = outputs.front();

        if (finished && single_level_chunks.empty())
        {
            output.finish();
            return Status::Finished;
        }

        /// Check can output.
        if (output.isFinished())
        {
            for (auto & input : inputs)
                input.close();

            shared_data->is_cancelled.store(true, std::memory_order_seq_cst);

            return Status::Finished;
        }

        if (!output.canPush())
            return Status::PortFull;

        if (!is_initialized)
            return Status::Ready;

        if (!processors.empty())
            return Status::UpdatePipeline;

        if (!single_level_chunks.empty())
            return preparePushToOutput();

        /// Single level case.
        if (inputs.empty())
            return Status::Ready;
        else if (parallelize_single_level_merge)
            // Also single level, but need to check all input ports are finished.
            return prepareParallelizeSingleLevel();
        else if (parallel_partition_merge_started)
            // Also single level: the partition sources emit finished chunks, forward them as they come.
            return preparePartitionMerge();

        /// Two-level case.
        return prepareTwoLevel();
    }

    void onCancel() noexcept override
    {
        shared_data->is_cancelled.store(true, std::memory_order_seq_cst);
    }

private:
    bool worthParallelMergeSingleLevel()
    {
        if (num_threads <= 1)
            return false;

        if (!params->aggregator.isTypeFixedSize(*data))
            return false;

        return true;
    }

    bool worthParallelPartitionMergeSingleLevel()
    {
        if (!params->params.enable_parallel_single_level_merge)
            return false;

        if (num_threads <= 1 || data->size() <= 1)
            return false;

        /// The overflow row lives outside the partitioned cells, and under the `any` overflow mode
        /// the tables hold diverged key sets that the merge must reconcile key by key — both need
        /// the serial merge. Under `throw` and `break` the tables are ordinary, and the partition
        /// sources re-check the group limit against their shared running total.
        if (params->params.overflow_row)
            return false;
        if (params->params.max_rows_to_group_by != 0 && params->params.group_by_overflow_mode == OverflowMode::ANY)
            return false;

        if (!params->aggregator.canMergeSingleLevelInPartitions(*data->at(0)))
            return false;

        /// The largest source table is measured here, before any partition source runs: the
        /// merge mutates the source tables concurrently, so the workers must not read their
        /// sizes.
        max_source_table_size = 0;
        for (const auto & variants : *data)
            max_source_table_size = std::max(max_source_table_size, variants->sizeWithoutOverflowRow());

        /// A single partition would rebuild the whole result into a fresh table with no
        /// parallelism at all; the serial merge into the largest existing table is strictly
        /// cheaper, because that table's own cells do not move.
        partition_merge_num_partitions = singleLevelMergePartitionCount(max_source_table_size);
        return partition_merge_num_partitions > 1;
    }

    /// More partitions than workers, handed out dynamically, so a worker that got a light
    /// partition does not stay idle. Heavy per-key states always get the full partition count:
    /// their merge work dwarfs the partitioning overhead at any key count. Cheap states get
    /// fewer partitions for smaller merges, sized by the largest source table — a lower bound
    /// on the distinct-key count.
    size_t singleLevelMergePartitionCount(size_t max_table_size) const
    {
        const size_t max_partitions = std::bit_floor(std::min<size_t>(NUM_BUCKETS, num_threads * 2));

        const bool has_heavy_states
            = std::ranges::any_of(params->params.aggregates, [](const auto & aggregate) { return aggregate.function->sizeOfData() > 16; });
        if (has_heavy_states)
            return max_partitions;

        static constexpr size_t MIN_KEYS_PER_PARTITION = 512;
        return std::bit_floor(std::clamp<size_t>(max_table_size / MIN_KEYS_PER_PARTITION, 1, max_partitions));
    }

    /// The partition sources emit finished chunks in no particular order; forward them as they come.
    IProcessor::Status preparePartitionMerge()
    {
        auto & output = outputs.front();

        bool all_finished = true;
        for (auto & input : inputs)
        {
            if (input.isFinished())
                continue;
            all_finished = false;

            if (input.hasData())
            {
                auto chunk = input.pull();
                if (chunk.hasRows())
                {
                    output.push(std::move(chunk));
                    return Status::PortFull;
                }
            }
        }

        if (all_finished)
        {
            output.finish();
            return Status::Finished;
        }

        return Status::NeedData;
    }

    IProcessor::Status prepareParallelizeSingleLevel()
    {
        for (auto & input : inputs)
        {
            if (!input.isFinished())
                return Status::NeedData;
        }

        return Status::Ready;
    }

    IProcessor::Status preparePushToOutput()
    {
        if (single_level_chunks.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Some ready chunks expected");

        auto & output = outputs.front();
        auto chunk = std::move(single_level_chunks.back());
        single_level_chunks.pop_back();
        output.push(std::move(chunk));

        if (finished && single_level_chunks.empty())
        {
            output.finish();
            return Status::Finished;
        }

        return Status::PortFull;
    }

    /// Read all sources and try to push current bucket.
    IProcessor::Status prepareTwoLevel()
    {
        auto & output = outputs.front();

        for (auto & input : inputs)
        {
            if (!input.isFinished() && input.hasData())
            {
                auto chunk = input.pull();
                auto bucket = getInfoFromChunk(chunk)->bucket_num;
                two_level_chunks[bucket] = std::move(chunk);
            }
        }

        auto get_bucket_if_ready = [&](UInt32 bucket_num) -> Chunk
        {
            if (!shared_data->is_bucket_processed[bucket_num])
                return {};

            if (!two_level_chunks[bucket_num])
                return {};

            return std::move(two_level_chunks[bucket_num]);
        };

        auto get_ready_out_of_order_bucket = [&]() -> Chunk
        {
            for (auto it = out_of_order_buckets.begin(); it != out_of_order_buckets.end(); ++it)
            {
                if (auto chunk = get_bucket_if_ready(*it))
                {
                    out_of_order_buckets.erase(it);
                    return chunk;
                }
            }
            return {};
        };

        while (current_bucket_num < NUM_BUCKETS)
        {
            // Try find a ready bucket among out of order buckets first.
            Chunk chunk = get_ready_out_of_order_bucket();

            // Then try the current bucket.
            if (!chunk)
            {
                /// Try push the current bucket.
                if ((chunk = get_bucket_if_ready(current_bucket_num)))
                {
                    ++current_bucket_num;
                }
                else if (params->params.enable_producing_buckets_out_of_order_in_aggregation)
                {
                    /// Otherwise, if there is an empty slot, postpone the current bucket until it is ready.
                    if (out_of_order_buckets.size() < NUM_OOO_BUCKETS)
                    {
                        out_of_order_buckets.push_back(current_bucket_num);
                        chassert(std::ranges::is_sorted(out_of_order_buckets));
                        ++current_bucket_num;
                        continue;
                    }
                }
            }

            // No ready buckets.
            if (!chunk)
                return Status::NeedData;

            const auto has_rows = chunk.hasRows();
            if (has_rows)
            {
                chunk.getChunkInfos().get<AggregatedChunkInfo>()->out_of_order_buckets = out_of_order_buckets;
                output.push(std::move(chunk));
                return Status::PortFull;
            }
        }

        /// We want to prevent the following situation:
        /// 1. all inputs are finished and we tried to push all buckets (i.e., current_bucket_num == NUM_BUCKETS)
        /// 2. the next in order out of order bucket (and there are still some more) is empty, so we won't push it
        /// 3. if in that case we won't loop and make another `get_ready_out_of_order_bucket()`,
        ///    but proceed straight to `return NeedData`, we'll get `Pipeline stuck`, because, again, all inputs are finished
        while (auto chunk = get_ready_out_of_order_bucket())
        {
            if (chunk.hasRows())
            {
                chunk.getChunkInfos().template get<AggregatedChunkInfo>()->out_of_order_buckets = out_of_order_buckets;
                output.push(std::move(chunk));
                return Status::PortFull;
            }
        }

        if (!out_of_order_buckets.empty())
            return Status::NeedData;

        output.finish();
        /// Do not close inputs, they must be finished.
        return Status::Finished;
    }

    AggregatingTransformParamsPtr params;
    ManyAggregatedDataVariantsPtr data;
    ConvertingAggregatedToChunksWithMergingSource::SharedDataPtr shared_data;

    size_t num_threads;

    RuntimeDataflowStatisticsCacheUpdaterPtr updater;
    AdaptiveAggregationSessionPtr adaptive_session;

    bool is_initialized = false;
    bool finished = false;
    bool parallelize_single_level_merge = false;
    bool parallel_partition_merge_started = false;

    /// Set by the partition-merge gate for `createSourcesForPartitionMerge`; the sizes must be
    /// read before any partition source runs, because the merge mutates the source tables.
    size_t partition_merge_num_partitions = 0;
    size_t max_source_table_size = 0;

    Chunks single_level_chunks;

    UInt32 current_bucket_num = 0;
    static constexpr Int32 NUM_BUCKETS = 256;
    std::array<Chunk, NUM_BUCKETS> two_level_chunks;

    /// In principle we should produce buckets in order of their id-s for memory efficient merging.
    /// The problem is that on the initiator we cannot start merging buckets #(N+1) until we received all buckets #(<=N).
    /// Sometimes this dependency introduces a noticeable slowdown and in order to eliminate it we allow a few buckets
    /// to be delayed for a while and at that time merging still can be performed for some buckets with bigger id-s.
    /// It works because we don't actually require any specific order of buckets anywhere, we only need to make sure that
    /// `GroupingAggregatedTransform` will output all buckets (from all the nodes) with the same id together.
    static constexpr UInt32 NUM_OOO_BUCKETS = 4;
    std::vector<Int32> out_of_order_buckets;

    Processors processors;

    void initialize()
    {
        is_initialized = true;

        AggregatedDataVariantsPtr & first = data->at(0);

        /// At least we need one arena in first data item per thread
        if (num_threads > first->aggregates_pools.size())
        {
            Arenas & first_pool = first->aggregates_pools;
            for (size_t j = first_pool.size(); j < num_threads; ++j)
                first_pool.emplace_back(std::make_shared<Arena>());
        }

        if (first->type == AggregatedDataVariants::Type::without_key || params->params.overflow_row)
        {
            params->aggregator.mergeWithoutKeyDataImpl(*data, shared_data->is_cancelled);
            if (updater)
                updater->recordAggregationStateSizes(*first, /*bucket=*/-1);
            auto agg_chunk = params->aggregator.prepareChunkAndFillWithoutKey(
                *first, params->final, first->type != AggregatedDataVariants::Type::without_key);
            if (updater)
                updater->recordAggregationKeySizes(
                    agg_chunk.chunk, params->aggregator.getKeysPositions(), params->aggregator.getKeyTypes());

            if (agg_chunk.chunk.getNumRows() > 0)
                single_level_chunks.emplace_back(convertToChunk(std::move(agg_chunk)));
        }
    }

    void mergeSingleLevel()
    {
        AggregatedDataVariantsPtr & first = data->at(0);
        if (parallelize_single_level_merge)
        {
            params->aggregator.resetAggregatorExceptFirst(*data);

            /// We skip the `max_rows_to_group_by` limit check during the merge to avoid race condition.
            /// Therefore here we need to check additional after merges are completed from different threads.
            params->aggregator.ensureLimitsFixedMapMerge(first);
        }
        else
        {
            // In case of single threaded single level merge, we have to merge the data here before converting to blocks.
            if (current_bucket_num > 0 || first->type == AggregatedDataVariants::Type::without_key)
            {
                finished = true;
                return;
            }

            ++current_bucket_num;

#define M(NAME) \
    else if (first->type == AggregatedDataVariants::Type::NAME) \
    { \
        params->aggregator.mergeSingleLevelDataImpl<decltype(first->NAME)::element_type>(*data, shared_data->is_cancelled); \
        if (updater) \
            updater->recordAggregationStateSizes(*first, /*bucket=*/-1); \
    }
            if (false) {} // NOLINT
            APPLY_FOR_VARIANTS_SINGLE_LEVEL(M)
#undef M
            else
                throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");
        }

        auto agg_chunks = params->aggregator.prepareChunkAndFillSingleLevel</* return_single_block */ false>(*first, params->final);
        for (auto & agg_chunk : agg_chunks)
        {
            if (agg_chunk.chunk.getNumRows() > 0)
            {
                if (updater)
                    updater->recordAggregationKeySizes(
                        agg_chunk.chunk, params->aggregator.getKeysPositions(), params->aggregator.getKeyTypes());
                single_level_chunks.emplace_back(convertToChunk(std::move(agg_chunk)));
            }
        }

        finished = true;
        data.reset();
    }

    void createSources()
    {
        AggregatedDataVariantsPtr & first = data->at(0);

        if (adaptive_session)
        {
            /// The adaptive drain and merge create the destination's states in per-bucket
            /// arenas, for two reasons. Fresh arenas (rather than `pools[thread]`, typically a
            /// source local's arena) because with a zero-size aggregate state (`Nothing`) an
            /// arena returns one address for every allocation, so a drained state would alias
            /// that local's states and the bucket merge would see a state merged into itself.
            /// And per bucket (rather than per source) so a converted bucket's states free
            /// with its slot when the bucket retires. The slots live outside
            /// `aggregates_pools`, which every bucket's output columns capture wholesale;
            /// each conversion is handed its own slot instead.
            first->adaptive_merge_bucket_arenas.resize(ConvertingAggregatedToChunksWithMergingSource::NUM_BUCKETS);
            for (auto & slot : first->adaptive_merge_bucket_arenas)
                slot = std::make_shared<Arena>();
        }

        for (size_t thread = 0; thread < num_threads; ++thread)
        {
            /// Select Arena to avoid race conditions; the adaptive sources pick their arena
            /// per bucket instead.
            Arena * arena = adaptive_session ? nullptr : first->aggregates_pools.at(thread).get();
            auto source = std::make_shared<ConvertingAggregatedToChunksWithMergingSource>(
                params, data, shared_data, arena, updater, adaptive_session);

            processors.emplace_back(std::move(source));
        }

        data.reset();
    }

    void createSourcesForPartitionMerge()
    {
        /// Computed by the gate (`worthParallelPartitionMergeSingleLevel`), which engages this
        /// path only for more than one partition.
        const size_t num_partitions = partition_merge_num_partitions;
        chassert(num_partitions > 1);

        const size_t num_sources = std::min<size_t>(num_threads, num_partitions);
        for (size_t thread = 0; thread < num_sources; ++thread)
        {
            auto source = std::make_shared<ConvertingAggregatedToChunksByPartitionMergingSource>(
                params, data, shared_data, static_cast<UInt32>(num_partitions), max_source_table_size, updater);
            processors.emplace_back(std::move(source));
        }

        data.reset();
    }

    void createSourcesForFixedHashMap()
    {
        /// Disable min max optimization to avoid race condition.
        params->aggregator.disableMinMaxOptimizationForFixedHashMaps(*data);

        AggregatedDataVariantsPtr & first = data->at(0);
        for (size_t thread = 0; thread < num_threads; ++thread)
        {
            auto source = std::make_shared<ConvertingAggregatedToChunksWithMergingSourceForFixedHashMap>(params, data, thread, num_threads, first->aggregates_pools.at(thread).get());
            processors.emplace_back(std::move(source));
        }
    }
};

AggregatingTransform::AggregatingTransform(
    SharedHeader header, AggregatingTransformParamsPtr params_, RuntimeDataflowStatisticsCacheUpdaterPtr updater_)
    : AggregatingTransform(
          std::move(header),
          std::move(params_),
          std::make_unique<ManyAggregatedData>(1),
          0,
          1,
          1,
          true /* should_produce_results_in_order_of_bucket_number */,
          false /* skip_merging */,
          updater_)
{
}

AggregatingTransform::AggregatingTransform(
    SharedHeader header,
    AggregatingTransformParamsPtr params_,
    ManyAggregatedDataPtr many_data_,
    size_t current_variant,
    size_t max_threads_,
    size_t temporary_data_merge_threads_,
    bool should_produce_results_in_order_of_bucket_number_,
    bool skip_merging_,
    RuntimeDataflowStatisticsCacheUpdaterPtr updater_)
    : IProcessor({std::move(header)}, {params_->getHeader()})
    , params(std::move(params_))
    , key_columns(params->params.keys_size)
    , aggregate_columns(params->params.aggregates_size)
    , many_data(std::move(many_data_))
    , variants(*many_data->variants[current_variant])
    , max_threads(std::min(many_data->variants.size(), max_threads_))
    , temporary_data_merge_threads(temporary_data_merge_threads_)
    , should_produce_results_in_order_of_bucket_number(should_produce_results_in_order_of_bucket_number_)
    , skip_merging(skip_merging_)
    , updater(std::move(updater_))
{
    /// `AggregatingStep` leaves its engagement verdict in the flag. Without a producer nothing is ever
    /// staged, so the merge-time drains find empty backlogs and do nothing.
    if (many_data->adaptive_session && params->aggregator.getParams().enable_adaptive_aggregator)
        adaptive_context = std::make_unique<AdaptiveAggregationProducer>(many_data->adaptive_session);
}

AggregatingTransform::~AggregatingTransform() = default;

void AggregatingTransform::onCancel() noexcept
{
    /// A pressure sweep checks this between chunks and buckets: it can spill gigabytes to
    /// disk, and a cancelled query must not wait that out.
    if (adaptive_context)
        adaptive_context->session->cancel();
}

size_t AggregatingTransform::getGeneratingStepGroup() const
{
    /// After consumption finishes, this transform generates the child processors that perform
    /// the merge / final part of aggregation. Those children belong to the generating stage,
    /// not to the AggregatingTransform's own (partial) aggregation stage.
    return static_cast<size_t>(AggregatingStep::AggregatingStage::FinalAggregation);
}

IProcessor::Status AggregatingTransform::prepare()
{
    /// There are one or two input ports.
    /// The first one is used at aggregation step, the second one - while reading merged data from ConvertingAggregated

    auto & output = outputs.front();
    /// Last output is current. All other outputs should already be closed.
    auto & input = inputs.back();

    /// Check can output.
    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Finish data processing, prepare to generating.
    if (is_consume_finished && !is_generate_initialized.test())
    {
        /// Close input port in case max_rows_to_group_by was reached but not all data was read.
        inputs.front().close();

        return Status::Ready;
    }

    if (is_generate_initialized.test() && !is_pipeline_created && !processors.empty())
        return Status::UpdatePipeline;

    /// Only possible while consuming.
    if (read_current_chunk)
        return Status::Ready;

    /// Get chunk from input.
    if (input.isFinished())
    {
        if (is_consume_finished)
        {
            output.finish();
            /// input.isFinished() means that merging is done. Now we can release our reference to aggregation states.
            /// TODO: there is another case, when output port is getting closed first.
            /// E.g. `select ... group by x limit 10`, if it was two-level aggregation and first few buckets contained already enough rows
            /// limit will stop merging. It turned out to be not trivial to both release aggregation states and ensure that
            /// ManyAggregatedData holds the last references to them to trigger parallel destruction in its dtor. Will work on that.
            many_data.reset();
            return Status::Finished;
        }

        /// Finish data processing and create another pipe.
        is_consume_finished = true;
        return Status::Ready;
    }

    if (!input.hasData())
    {
        input.setNeeded();
        return Status::NeedData;
    }

    if (is_consume_finished)
        input.setNeeded();

    current_chunk = input.pull(/*set_not_needed = */ !is_consume_finished);
    read_current_chunk = true;

    if (is_consume_finished)
    {
        output.push(std::move(current_chunk));
        read_current_chunk = false;
        return Status::PortFull;
    }

    return Status::Ready;
}

void AggregatingTransform::work()
{
    if (is_consume_finished)
    {
        initGenerate();
    }
    else
    {
        consume(std::move(current_chunk));
        read_current_chunk = false;
    }
}

IProcessor::PipelineUpdate AggregatingTransform::updatePipeline()
{
    if (processors.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not updatePipeline in AggregatingTransform. This is a bug.");
    auto & out = processors.back()->getOutputs().front();
    inputs.emplace_back(out.getHeader(), this);
    connect(out, inputs.back());
    is_pipeline_created = true;
    for (auto & proc : processors)
        proc->inheritQueryPlanStepFromParent(*this, getGeneratingStepGroup());

    return PipelineUpdate{.to_add = std::move(processors), .to_remove = {}};
}

void AggregatingTransform::consume(Chunk chunk)
{
    const UInt64 num_rows = chunk.getNumRows();

    if (num_rows == 0 && params->params.empty_result_for_aggregation_by_empty_set)
        return;

    if (!is_consume_started)
    {
        LOG_TRACE(log, "Aggregating");
        is_consume_started = true;
    }
    if (rows_before_aggregation)
        rows_before_aggregation->add(num_rows);
    src_rows += num_rows;
    src_bytes += chunk.bytes();

    if (params->params.only_merge)
    {
        materializeChunk(chunk);
        if (!params->aggregator.mergeOnBlock(chunk.detachColumns(), num_rows, false, variants, no_more_keys, is_cancelled))
            is_consume_finished = true;
    }
    else
    {
        if (!params->aggregator.executeOnBlock(
                chunk.detachColumns(),
                0,
                num_rows,
                variants,
                key_columns,
                aggregate_columns,
                no_more_keys,
                adaptive_context.get()))
            is_consume_finished = true;
    }
}

void AggregatingTransform::initGenerate()
{
    if (is_generate_initialized.test_and_set())
        return;

    /// If there was no data, and we aggregate without keys, and we must return single row with the result of empty aggregation.
    /// To do this, we pass a block with zero rows to aggregate.
    if (variants.empty() && params->params.keys_size == 0 && !params->params.empty_result_for_aggregation_by_empty_set)
    {
        if (params->params.only_merge)
            params->aggregator.mergeOnBlock(getInputs().front().getHeader().getColumns(), 0, false, variants, no_more_keys, is_cancelled);
        else
            params->aggregator.executeOnBlock(
                getInputs().front().getHeader().getColumns(), 0, 0, variants, key_columns, aggregate_columns, no_more_keys,
                /* adaptive= */ nullptr);
    }

    double elapsed_seconds = watch.elapsedSeconds();
    size_t rows = variants.sizeWithoutOverflowRow();

    LOG_TRACE(log, "Aggregated. {} to {} rows (from {}) in {:.3f} sec. ({:.3f} rows/sec., {}/sec.)",
        src_rows, rows, ReadableSize(src_bytes),
        elapsed_seconds, static_cast<double>(src_rows) / elapsed_seconds,
        ReadableSize(static_cast<double>(src_bytes) / elapsed_seconds));

    if (params->aggregator.hasTemporaryData())
    {
        if (variants.isConvertibleToTwoLevel())
            variants.convertToTwoLevel();

        /// Flush data in the RAM to disk also. It's easier than merging on-disk and RAM data.
        /// A table that already spilled keeps its type with zero rows; writing it again would
        /// produce an empty part per producer.
        if (variants.hasData())
            params->aggregator.writeToTemporaryFile(variants);
    }

    bool adaptive_engaged = adaptive_context && adaptive_context->session->initialized.load(std::memory_order_acquire);
    if (adaptive_engaged)
    {
        /// Complete this thread's backlog contribution before the finish barrier below: the last
        /// finisher assembles the merge assuming every producer's staged records are enqueued.
        params->aggregator.flushPendingChunks(*adaptive_context);

        if (variants.isConvertibleToTwoLevel())
            variants.convertToTwoLevel();
    }

    if (many_data->num_finished.fetch_add(1) + 1 < many_data->num_producers)
    {
        /// Note: we reset aggregation state here to release memory earlier.
        /// It might cause extra memory usage for complex queries othervise.
        many_data.reset();
        return;
    }

    adaptive_engaged = adaptive_context && adaptive_context->session->initialized.load(std::memory_order_acquire);

    if (adaptive_engaged)
        LOG_TRACE(
            log,
            "Adaptive aggregation: {} delayed records queued for the merge-time drain",
            adaptive_context->session->backlog.undrainedRecords());

    /// In the case of two different aggregators existing simultaneously due to a mixed pipeline of aggregate projections,
    /// it is necessary to check whether any of the aggregators contains temporary data.
    auto aggregator_has_temporary_data = [&]()
    {
        return params->aggregator.hasTemporaryData()
            || std::any_of(
                params->aggregator_list_ptr->begin(),
                params->aggregator_list_ptr->end(),
                [](const Aggregator & aggregator) { return aggregator.hasTemporaryData(); });
    };

    if (adaptive_engaged)
    {
        auto & shared = *adaptive_context->session;

        /// The producers' final flushes run after their own spill checks, and a flush's seal
        /// copies can push memory over the external threshold with nothing re-checking. Re-check
        /// here, after every producer flushed and before the merge path is chosen: the sweep
        /// no-ops under the trigger, sheds staged records when over it, and spills the routing
        /// table if shedding is not enough - which makes the choice below go external.
        if (params->params.max_bytes_before_external_group_by)
            params->aggregator.drainStagedChunksUnderMemoryPressure(shared);

        if (aggregator_has_temporary_data())
        {
            /// A thawed or given-up producer spilled on the baseline path, so the merge goes
            /// external and the bucket-parallel drain will not run: put the backlogs into
            /// disk-mergeable form by draining everything into the routing table now (the
            /// finish barrier guarantees a quiescent, uncontended sweep). The external branch
            /// below flushes it together with the other still-in-memory variants.
            params->aggregator.drainStagedChunksAtFinish(shared);

            /// The external merge bypasses `prepareVariantsToMerge`, which is where the thaw
            /// verdict is normally recorded.
            params->aggregator.recordAdaptiveStagingVerdict(shared);
        }
        if (shared.early_drain_variants->hasData())
        {
            /// Early-drained records live in the routing table: it holds part of the result
            /// and joins the merge set like any other variant. Only the last finisher gets
            /// here, so growing `variants` is safe as long as nothing else reads it - hence
            /// the barrier above counts `num_producers` rather than the size of this vector.
            many_data->variants.push_back(shared.early_drain_variants);
        }
    }

    if (!aggregator_has_temporary_data())
    {
        if (!skip_merging)
        {
            auto prepared_data = params->aggregator.prepareVariantsToMerge(
                std::move(many_data->variants), adaptive_context ? adaptive_context->session.get() : nullptr);
            auto prepared_data_ptr = std::make_shared<ManyAggregatedDataVariants>(std::move(prepared_data));
            processors.emplace_back(std::make_shared<ConvertingAggregatedToChunksTransform>(
                params, std::move(prepared_data_ptr), max_threads, updater, adaptive_engaged ? adaptive_context->session : nullptr));
        }
        else
        {
            if (updater)
                updater->markUnsupportedCase();

            auto prepared_data = params->aggregator.prepareVariantsToMerge(std::move(many_data->variants), /*adaptive_session=*/nullptr);
            Pipes pipes;
            for (auto & variant : prepared_data)
            {
                /// Converts hash tables to blocks with data (finalized or not).
                pipes.emplace_back(std::make_shared<ConvertingAggregatedToChunksSource>(params, variant));
            }

            Pipe pipe = Pipe::unitePipes(std::move(pipes));
            if (!pipe.empty())
            {
                if (should_produce_results_in_order_of_bucket_number)
                {
                    /// Groups chunks with the same bucket_id and outputs them (as a vector of chunks) in order of bucket_id.
                    pipe.addTransform(std::make_shared<GroupingAggregatedTransform>(pipe.getHeader(), pipe.numOutputPorts(), params));
                    /// Outputs one chunk from group at a time in order of bucket_id.
                    pipe.addTransform(std::make_shared<FlattenChunksToMergeTransform>(pipe.getHeader(), params->getHeader()));
                }
                else
                {
                    /// If this is a final stage, we no longer have to keep chunks from different buckets into different chunks.
                    /// So now we can insert transform that will keep chunks size under control. It makes few times difference in exec time in some cases.
                    if (params->final)
                    {
                        pipe.addSimpleTransform(
                            [this](const SharedHeader & header)
                            {
                                /// Just a reasonable constant, matches default value for the setting `preferred_block_size_bytes`
                                static constexpr size_t oneMB = 1024 * 1024;
                                return std::make_shared<SimpleSquashingChunksTransform>(header, params->params.max_block_size, oneMB);
                            });
                    }
                    /// AggregatingTransform::updatePipeline expects single output port.
                    /// It's not a big problem because we do resize() to max_threads after AggregatingTransform.
                    pipe.resize(1);
                }
            }
            processors = Pipe::detachProcessors(std::move(pipe));
        }
    }
    else
    {
        if (updater)
            updater->markUnsupportedCase();

        /// If there are temporary files with partially-aggregated data on the disk,
        /// then read and merge them, spending the minimum amount of memory.

        ProfileEvents::increment(ProfileEvents::ExternalAggregationMerge);

        if (many_data->variants.size() > 1)
        {
            /// It may happen that some data has not yet been flushed,
            ///  because at the time thread has finished, no data has been flushed to disk, and then some were.
            for (auto & cur_variants : many_data->variants)
            {
                if (cur_variants->isConvertibleToTwoLevel())
                    cur_variants->convertToTwoLevel();

                if (cur_variants->hasData())
                    params->aggregator.writeToTemporaryFile(*cur_variants);
            }
        }

        size_t num_streams = 0;
        size_t compressed_size = 0;
        size_t uncompressed_size = 0;

        Pipes pipes;
        /// Merge external data from all aggregators used in query.
        for (auto & aggregator : *params->aggregator_list_ptr)
        {
            auto new_tmp_files = aggregator.detachTemporaryData();
            num_streams += new_tmp_files.size();

            for (auto & tmp_stream : new_tmp_files)
            {
                auto stat = tmp_stream.finishWriting();
                compressed_size += stat.compressed_size;
                uncompressed_size += stat.uncompressed_size;
                pipes.emplace_back(Pipe(std::make_unique<SourceFromNativeStream>(std::make_shared<const Block>(tmp_stream.getHeader()), tmp_stream.getReadStream())));
            }

            tmp_files.splice(tmp_files.end(), new_tmp_files);
        }

        LOG_DEBUG(
            log,
            "Will merge {} temporary files of size {} compressed, {} uncompressed.",
            num_streams,
            ReadableSize(compressed_size),
            ReadableSize(uncompressed_size));

        auto pipe = Pipe::unitePipes(std::move(pipes));
        addMergingAggregatedMemoryEfficientTransform(
            pipe, params, temporary_data_merge_threads, /*should_produce_results_in_order_of_bucket_number=*/true);

        processors = Pipe::detachProcessors(std::move(pipe));
    }
}

}
