#include <atomic>
#include <Processors/Transforms/AggregatingTransform.h>

#include <Formats/NativeReader.h>
#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <Core/ProtocolDefines.h>
#include <Common/logger_useful.h>
#include <Common/formatReadable.h>

#include <Processors/Transforms/SquashingTransform.h>


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

/// Convert block to chunk.
/// Adds additional info about aggregation.
Chunk convertToChunk(const Block & block)
{
    auto info = std::make_shared<AggregatedChunkInfo>();
    info->bucket_num = block.info.bucket_num;
    info->is_overflows = block.info.is_overflows;

    UInt64 num_rows = block.rows();
    Chunk chunk(block.getColumns(), num_rows);
    chunk.getChunkInfos().add(std::move(info));

    return chunk;
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
    class SourceFromNativeStream : public ISource
    {
    public:
        explicit SourceFromNativeStream(TemporaryFileStream * tmp_stream_)
            : ISource(tmp_stream_->getHeader())
            , tmp_stream(tmp_stream_)
        {}

        String getName() const override { return "SourceFromNativeStream"; }

        Chunk generate() override
        {
            if (!tmp_stream)
                return {};

            auto block = tmp_stream->read();
            if (!block)
            {
                tmp_stream = nullptr;
                return {};
            }
            return convertToChunk(block);
        }

        std::optional<ReadProgress> getReadProgress() override { return std::nullopt; }

    private:
        TemporaryFileStream * tmp_stream;
    };
}

/// Worker which merges buckets for two-level aggregation.
/// Atomically increments bucket counter and returns merged result.
class ConvertingAggregatedToChunksWithMergingSource : public ISource
{
public:
    static constexpr UInt32 NUM_BUCKETS = 256;

    struct SharedData
    {
        std::atomic<UInt32> next_bucket_to_merge = 0;
        std::array<std::atomic<bool>, NUM_BUCKETS> is_bucket_processed{};
        std::atomic<bool> is_cancelled = false;

        SharedData()
        {
            for (auto & flag : is_bucket_processed)
                flag = false;
        }
    };

    using SharedDataPtr = std::shared_ptr<SharedData>;

    ConvertingAggregatedToChunksWithMergingSource(
        AggregatingTransformParamsPtr params_, ManyAggregatedDataVariantsPtr data_, SharedDataPtr shared_data_, Arena * arena_)
        : ISource(params_->getHeader(), false)
        , params(std::move(params_))
        , data(std::move(data_))
        , shared_data(std::move(shared_data_))
        , arena(arena_)
    {
    }

    String getName() const override { return "ConvertingAggregatedToChunksWithMergingSource"; }

protected:
    Chunk generate() override
    {
        UInt32 bucket_num = shared_data->next_bucket_to_merge.fetch_add(1);

        if (bucket_num >= NUM_BUCKETS)
        {
            data.reset();
            return {};
        }

        Block block = params->aggregator.mergeAndConvertOneBucketToBlock(*data, arena, params->final, bucket_num, shared_data->is_cancelled);
        Chunk chunk = convertToChunk(block);

        shared_data->is_bucket_processed[bucket_num] = true;

        return chunk;
    }

private:
    AggregatingTransformParamsPtr params;
    ManyAggregatedDataVariantsPtr data;
    SharedDataPtr shared_data;
    Arena * arena;
};

/// Asks Aggregator to convert accumulated aggregation state into blocks (without merging) and pushes them to later steps.
class ConvertingAggregatedToChunksSource : public ISource
{
public:
    ConvertingAggregatedToChunksSource(AggregatingTransformParamsPtr params_, AggregatedDataVariantsPtr variant_)
        : ISource(params_->getHeader(), false), params(params_), variant(variant_)
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
                Block block = params->aggregator.convertOneBucketToBlock(*variant, arena, params->final, current_bucket_num++);
                return convertToChunk(block);
            }
        }
        else if (!single_level_converted)
        {
            Block block = params->aggregator.prepareBlockAndFillSingleLevel<true /* return_single_block */>(*variant, params->final);
            single_level_converted = true;
            return convertToChunk(block);
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
class FlattenChunksToMergeTransform : public IProcessor
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
class ConvertingAggregatedToChunksTransform : public IProcessor
{
public:
    ConvertingAggregatedToChunksTransform(AggregatingTransformParamsPtr params_, ManyAggregatedDataVariantsPtr data_, size_t num_threads_)
        : IProcessor({}, {params_->getHeader()})
        , params(std::move(params_))
        , data(std::move(data_))
        , shared_data(std::make_shared<ConvertingAggregatedToChunksWithMergingSource::SharedData>())
        , num_threads(num_threads_)
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
        else
        {
            mergeSingleLevel();
        }
    }

    Processors expandPipeline() override
    {
        for (auto & source : processors)
        {
            auto & out = source->getOutputs().front();
            inputs.emplace_back(out.getHeader(), this);
            connect(out, inputs.back());
            inputs.back().setNeeded();
        }

        return std::move(processors);
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
            return Status::ExpandPipeline;

        if (!single_level_chunks.empty())
            return preparePushToOutput();

        /// Single level case.
        if (inputs.empty())
            return Status::Ready;

        /// Two-level case.
        return prepareTwoLevel();
    }

    void onCancel() noexcept override
    {
        shared_data->is_cancelled.store(true, std::memory_order_seq_cst);
    }

private:
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

        while (current_bucket_num < NUM_BUCKETS)
        {
            if (!shared_data->is_bucket_processed[current_bucket_num])
                return Status::NeedData;

            if (!two_level_chunks[current_bucket_num])
                return Status::NeedData;

            auto chunk = std::move(two_level_chunks[current_bucket_num]);
            ++current_bucket_num;

            const auto has_rows = chunk.hasRows();
            if (has_rows)
            {
                output.push(std::move(chunk));
                return Status::PortFull;
            }
        }

        output.finish();
        /// Do not close inputs, they must be finished.
        return Status::Finished;
    }

    AggregatingTransformParamsPtr params;
    ManyAggregatedDataVariantsPtr data;
    ConvertingAggregatedToChunksWithMergingSource::SharedDataPtr shared_data;

    size_t num_threads;

    bool is_initialized = false;
    bool finished = false;

    Chunks single_level_chunks;

    UInt32 current_bucket_num = 0;
    static constexpr Int32 NUM_BUCKETS = 256;
    std::array<Chunk, NUM_BUCKETS> two_level_chunks;

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
            auto block = params->aggregator.prepareBlockAndFillWithoutKey(
                *first, params->final, first->type != AggregatedDataVariants::Type::without_key);

            if (block.rows() > 0)
                single_level_chunks.emplace_back(convertToChunk(block));
        }
    }

    void mergeSingleLevel()
    {
        AggregatedDataVariantsPtr & first = data->at(0);

        if (current_bucket_num > 0 || first->type == AggregatedDataVariants::Type::without_key)
        {
            finished = true;
            return;
        }

        ++current_bucket_num;

    #define M(NAME) \
                else if (first->type == AggregatedDataVariants::Type::NAME) \
                    params->aggregator.mergeSingleLevelDataImpl<decltype(first->NAME)::element_type>(*data);
        if (false) {} // NOLINT
        APPLY_FOR_VARIANTS_SINGLE_LEVEL(M)
    #undef M
        else
            throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");

        auto blocks = params->aggregator.prepareBlockAndFillSingleLevel</* return_single_block */ false>(*first, params->final);
        for (auto & block : blocks)
            if (block.rows() > 0)
                single_level_chunks.emplace_back(convertToChunk(block));

        finished = true;
        data.reset();
    }

    void createSources()
    {
        AggregatedDataVariantsPtr & first = data->at(0);
        processors.reserve(num_threads);

        for (size_t thread = 0; thread < num_threads; ++thread)
        {
            /// Select Arena to avoid race conditions
            Arena * arena = first->aggregates_pools.at(thread).get();
            auto source = std::make_shared<ConvertingAggregatedToChunksWithMergingSource>(params, data, shared_data, arena);

            processors.emplace_back(std::move(source));
        }

        data.reset();
    }
};

AggregatingTransform::AggregatingTransform(Block header, AggregatingTransformParamsPtr params_)
    : AggregatingTransform(
        std::move(header),
        std::move(params_),
        std::make_unique<ManyAggregatedData>(1),
        0,
        1,
        1,
        true /* should_produce_results_in_order_of_bucket_number */,
        false /* skip_merging */)
{
}

AggregatingTransform::AggregatingTransform(
    Block header,
    AggregatingTransformParamsPtr params_,
    ManyAggregatedDataPtr many_data_,
    size_t current_variant,
    size_t max_threads_,
    size_t temporary_data_merge_threads_,
    bool should_produce_results_in_order_of_bucket_number_,
    bool skip_merging_)
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
{
}

AggregatingTransform::~AggregatingTransform() = default;

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
        return Status::ExpandPipeline;

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
        else
        {
            /// Finish data processing and create another pipe.
            is_consume_finished = true;
            return Status::Ready;
        }
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

Processors AggregatingTransform::expandPipeline()
{
    if (processors.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not expandPipeline in AggregatingTransform. This is a bug.");
    auto & out = processors.back()->getOutputs().front();
    inputs.emplace_back(out.getHeader(), this);
    connect(out, inputs.back());
    is_pipeline_created = true;
    return std::move(processors);
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

    src_rows += num_rows;
    src_bytes += chunk.bytes();

    if (params->params.only_merge)
    {
        auto block = getInputs().front().getHeader().cloneWithColumns(chunk.detachColumns());
        block = materializeBlock(block);
        if (!params->aggregator.mergeOnBlock(block, variants, no_more_keys, is_cancelled))
            is_consume_finished = true;
    }
    else
    {
        if (!params->aggregator.executeOnBlock(chunk.detachColumns(), 0, num_rows, variants, key_columns, aggregate_columns, no_more_keys))
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
            params->aggregator.mergeOnBlock(getInputs().front().getHeader(), variants, no_more_keys, is_cancelled);
        else
            params->aggregator.executeOnBlock(getInputs().front().getHeader(), variants, key_columns, aggregate_columns, no_more_keys);
    }

    double elapsed_seconds = watch.elapsedSeconds();
    size_t rows = variants.sizeWithoutOverflowRow();

    LOG_TRACE(log, "Aggregated. {} to {} rows (from {}) in {} sec. ({:.3f} rows/sec., {}/sec.)",
        src_rows, rows, ReadableSize(src_bytes),
        elapsed_seconds, src_rows / elapsed_seconds,
        ReadableSize(src_bytes / elapsed_seconds));

    if (params->aggregator.hasTemporaryData())
    {
        if (variants.isConvertibleToTwoLevel())
            variants.convertToTwoLevel();

        /// Flush data in the RAM to disk also. It's easier than merging on-disk and RAM data.
        if (!variants.empty())
            params->aggregator.writeToTemporaryFile(variants);
    }

    if (many_data->num_finished.fetch_add(1) + 1 < many_data->variants.size())
    {
        /// Note: we reset aggregation state here to release memory earlier.
        /// It might cause extra memory usage for complex queries othervise.
        many_data.reset();
        return;
    }

    if (!params->aggregator.hasTemporaryData())
    {
        if (!skip_merging)
        {
            auto prepared_data = params->aggregator.prepareVariantsToMerge(std::move(many_data->variants));
            auto prepared_data_ptr = std::make_shared<ManyAggregatedDataVariants>(std::move(prepared_data));
            processors.emplace_back(
                std::make_shared<ConvertingAggregatedToChunksTransform>(params, std::move(prepared_data_ptr), max_threads));
        }
        else
        {
            auto prepared_data = params->aggregator.prepareVariantsToMerge(std::move(many_data->variants));
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
                            [this](const Block & header)
                            {
                                /// Just a reasonable constant, matches default value for the setting `preferred_block_size_bytes`
                                static constexpr size_t oneMB = 1024 * 1024;
                                return std::make_shared<SimpleSquashingChunksTransform>(header, params->params.max_block_size, oneMB);
                            });
                    }
                    /// AggregatingTransform::expandPipeline expects single output port.
                    /// It's not a big problem because we do resize() to max_threads after AggregatingTransform.
                    pipe.resize(1);
                }
            }
            processors = Pipe::detachProcessors(std::move(pipe));
        }
    }
    else
    {
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

                if (!cur_variants->empty())
                    params->aggregator.writeToTemporaryFile(*cur_variants);
            }
        }

        size_t num_streams = 0;
        size_t compressed_size = 0;
        size_t uncompressed_size = 0;

        Pipes pipes;
        /// Merge external data from all aggregators used in query.
        for (const auto & aggregator : *params->aggregator_list_ptr)
        {
            const auto & tmp_data = aggregator.getTemporaryData();
            for (auto * tmp_stream : tmp_data.getStreams())
                pipes.emplace_back(Pipe(std::make_unique<SourceFromNativeStream>(tmp_stream)));

            num_streams += tmp_data.getStreams().size();
            compressed_size += tmp_data.getStat().compressed_size;
            uncompressed_size += tmp_data.getStat().uncompressed_size;
        }

        LOG_DEBUG(
            log,
            "Will merge {} temporary files of size {} compressed, {} uncompressed.",
            num_streams,
            ReadableSize(compressed_size),
            ReadableSize(uncompressed_size));

        auto pipe = Pipe::unitePipes(std::move(pipes));
        addMergingAggregatedMemoryEfficientTransform(pipe, params, temporary_data_merge_threads);

        processors = Pipe::detachProcessors(std::move(pipe));
    }
}

}
