#include <Processors/Transforms/AggregatingTransform.h>

#include <Common/ClickHouseRevision.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>
#include <Processors/ISource.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>

namespace ProfileEvents
{
    extern const Event ExternalAggregationMerge;
}

namespace DB
{

/// Convert block to chunk.
/// Adds additional info about aggregation.
static Chunk convertToChunk(const Block & block)
{
    auto info = std::make_shared<AggregatedChunkInfo>();
    info->bucket_num = block.info.bucket_num;
    info->is_overflows = block.info.is_overflows;

    UInt64 num_rows = block.rows();
    Chunk chunk(block.getColumns(), num_rows);
    chunk.setChunkInfo(std::move(info));

    return chunk;
}

namespace
{
    /// Reads chunks from file in native format. Provide chunks with aggregation info.
    class SourceFromNativeStream : public ISource
    {
    public:
        SourceFromNativeStream(const Block & header, const std::string & path)
                : ISource(header), file_in(path), compressed_in(file_in),
                  block_in(std::make_shared<NativeBlockInputStream>(compressed_in, ClickHouseRevision::get()))
        {
            block_in->readPrefix();
        }

        String getName() const override { return "SourceFromNativeStream"; }

        Chunk generate() override
        {
            if (!block_in)
                return {};

            auto block = block_in->read();
            if (!block)
            {
                block_in->readSuffix();
                block_in.reset();
                return {};
            }

            return convertToChunk(block);
        }

    private:
        ReadBufferFromFile file_in;
        CompressedReadBuffer compressed_in;
        BlockInputStreamPtr block_in;
    };
}

/// Worker which merges buckets for two-level aggregation.
/// Atomically increments bucket counter and returns merged result.
class ConvertingAggregatedToChunksSource : public ISource
{
public:
    ConvertingAggregatedToChunksSource(
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataVariantsPtr data_,
        Arena * arena_,
        std::shared_ptr<std::atomic<UInt32>> next_bucket_to_merge_)
        : ISource(params_->getHeader())
        , params(std::move(params_))
        , data(std::move(data_))
        , next_bucket_to_merge(std::move(next_bucket_to_merge_))
        , arena(arena_)
        {}

    String getName() const override { return "ConvertingAggregatedToChunksSource"; }

protected:
    Chunk generate() override
    {
        UInt32 bucket_num = next_bucket_to_merge->fetch_add(1);

        if (bucket_num >= NUM_BUCKETS)
            return {};

        Block block = params->aggregator.mergeAndConvertOneBucketToBlock(*data, arena, params->final, bucket_num);

        return convertToChunk(block);
    }

private:
    AggregatingTransformParamsPtr params;
    ManyAggregatedDataVariantsPtr data;
    std::shared_ptr<std::atomic<UInt32>> next_bucket_to_merge;
    Arena * arena;

    static constexpr UInt32 NUM_BUCKETS = 256;
};

/// Generates chunks with aggregated data.
/// In single level case, aggregates data itself.
/// In two-level case, creates `ConvertingAggregatedToChunksSource` workers:
///
/// ConvertingAggregatedToChunksSource ->
/// ConvertingAggregatedToChunksSource -> ConvertingAggregatedToChunksTransform -> AggregatingTransform
/// ConvertingAggregatedToChunksSource ->
///
/// Result chunks guaranteed to be sorted by bucket number.
class ConvertingAggregatedToChunksTransform : public IProcessor
{
public:
    ConvertingAggregatedToChunksTransform(AggregatingTransformParamsPtr params_, ManyAggregatedDataVariantsPtr data_, size_t num_threads_)
        : IProcessor({}, {params_->getHeader()})
        , params(std::move(params_)), data(std::move(data_)), num_threads(num_threads_) {}

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
        }

        return std::move(processors);
    }

    IProcessor::Status prepare() override
    {
        auto & output = outputs.front();

        if (finished && !has_input)
        {
            output.finish();
            return Status::Finished;
        }

        /// Check can output.
        if (output.isFinished())
            return Status::Finished;

        if (!output.canPush())
            return Status::PortFull;

        if (!is_initialized)
            return Status::Ready;

        if (!processors.empty())
            return Status::ExpandPipeline;

        if (has_input)
            return preparePushToOutput();

        /// Single level case.
        if (inputs.empty())
            return Status::Ready;

        /// Two-level case.
        return preparePullFromInputs();
    }

private:
    IProcessor::Status preparePushToOutput()
    {
        auto & output = outputs.front();
        output.push(std::move(current_chunk));
        has_input = false;

        if (finished)
        {
            output.finish();
            return Status::Finished;
        }

        return Status::PortFull;
    }

    /// Read all sources and try to push current bucket.
    IProcessor::Status preparePullFromInputs()
    {
        bool all_inputs_are_finished = true;

        for (auto & input : inputs)
        {
            if (input.isFinished())
                continue;

            all_inputs_are_finished = false;

            input.setNeeded();

            if (input.hasData())
                ready_chunks.emplace_back(input.pull());
        }

        moveReadyChunksToMap();

        if (trySetCurrentChunkFromCurrentBucket())
            return preparePushToOutput();

        if (all_inputs_are_finished)
            throw Exception("All sources have finished before getting enough data in "
                            "ConvertingAggregatedToChunksTransform.", ErrorCodes::LOGICAL_ERROR);

        return Status::NeedData;
    }

private:
    AggregatingTransformParamsPtr params;
    ManyAggregatedDataVariantsPtr data;
    size_t num_threads;

    bool is_initialized = false;
    bool has_input = false;
    bool finished = false;

    Chunk current_chunk;
    Chunks ready_chunks;

    UInt32 current_bucket_num = 0;
    static constexpr Int32 NUM_BUCKETS = 256;
    std::map<UInt32, Chunk> bucket_to_chunk;

    Processors processors;

    static Int32 getBucketFromChunk(const Chunk & chunk)
    {
        auto & info = chunk.getChunkInfo();
        if (!info)
            throw Exception("Chunk info was not set for chunk in "
                            "ConvertingAggregatedToChunksTransform.", ErrorCodes::LOGICAL_ERROR);

        auto * agg_info = typeid_cast<const AggregatedChunkInfo *>(info.get());
        if (!agg_info)
            throw Exception("Chunk should have AggregatedChunkInfo in "
                            "ConvertingAggregatedToChunksTransform.", ErrorCodes::LOGICAL_ERROR);

        return agg_info->bucket_num;
    }

    void moveReadyChunksToMap()
    {
        for (auto & chunk : ready_chunks)
        {
            auto bucket = getBucketFromChunk(chunk);

            if (bucket < 0 || bucket >= NUM_BUCKETS)
                throw Exception("Invalid bucket number " + toString(bucket) + " in "
                                "ConvertingAggregatedToChunksTransform.", ErrorCodes::LOGICAL_ERROR);

            if (bucket_to_chunk.count(bucket))
                throw Exception("Found several chunks with the same bucket number in "
                                "ConvertingAggregatedToChunksTransform.", ErrorCodes::LOGICAL_ERROR);

            bucket_to_chunk[bucket] = std::move(chunk);
        }

        ready_chunks.clear();
    }

    void setCurrentChunk(Chunk chunk)
    {
        if (has_input)
            throw Exception("Current chunk was already set in "
                            "ConvertingAggregatedToChunksTransform.", ErrorCodes::LOGICAL_ERROR);

        has_input = true;
        current_chunk = std::move(chunk);
    }

    void initialize()
    {
        is_initialized = true;

        AggregatedDataVariantsPtr & first = data->at(0);

        /// At least we need one arena in first data item per thread
        if (num_threads > first->aggregates_pools.size())
        {
            Arenas & first_pool = first->aggregates_pools;
            for (size_t j = first_pool.size(); j < num_threads; j++)
                first_pool.emplace_back(std::make_shared<Arena>());
        }

        if (first->type == AggregatedDataVariants::Type::without_key || params->params.overflow_row)
        {
            params->aggregator.mergeWithoutKeyDataImpl(*data);
            auto block = params->aggregator.prepareBlockAndFillWithoutKey(
                    *first, params->final, first->type != AggregatedDataVariants::Type::without_key);

            setCurrentChunk(convertToChunk(block));
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
        if (false) {}
        APPLY_FOR_VARIANTS_SINGLE_LEVEL(M)
    #undef M
        else
            throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

        auto block = params->aggregator.prepareBlockAndFillSingleLevel(*first, params->final);

        setCurrentChunk(convertToChunk(block));
        finished = true;
    }

    void createSources()
    {
        AggregatedDataVariantsPtr & first = data->at(0);
        auto next_bucket_to_merge = std::make_shared<std::atomic<UInt32>>(0);

        for (size_t thread = 0; thread < num_threads; ++thread)
        {
            Arena * arena = first->aggregates_pools.at(thread).get();
            auto source = std::make_shared<ConvertingAggregatedToChunksSource>(
                    params, data, arena, next_bucket_to_merge);

            processors.emplace_back(std::move(source));
        }
    }

    bool trySetCurrentChunkFromCurrentBucket()
    {
        auto it = bucket_to_chunk.find(current_bucket_num);
        if (it != bucket_to_chunk.end())
        {
            setCurrentChunk(std::move(it->second));
            ++current_bucket_num;

            if (current_bucket_num == NUM_BUCKETS)
                finished = true;

            return true;
        }

        return false;
    }
};

AggregatingTransform::AggregatingTransform(Block header, AggregatingTransformParamsPtr params_)
    : AggregatingTransform(std::move(header), std::move(params_)
    , std::make_unique<ManyAggregatedData>(1), 0, 1, 1)
{
}

AggregatingTransform::AggregatingTransform(
    Block header, AggregatingTransformParamsPtr params_, ManyAggregatedDataPtr many_data_,
    size_t current_variant, size_t temporary_data_merge_threads_, size_t max_threads_)
    : IProcessor({std::move(header)}, {params_->getHeader()}), params(std::move(params_))
    , key_columns(params->params.keys_size)
    , aggregate_columns(params->params.aggregates_size)
    , many_data(std::move(many_data_))
    , variants(*many_data->variants[current_variant])
    , max_threads(std::min(many_data->variants.size(), max_threads_))
    , temporary_data_merge_threads(temporary_data_merge_threads_)
{
}

AggregatingTransform::~AggregatingTransform() = default;

IProcessor::Status AggregatingTransform::prepare()
{
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
    if (is_consume_finished && !is_generate_initialized)
        return Status::Ready;

    if (is_generate_initialized && !is_pipeline_created && !processors.empty())
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
            return Status::Finished;
        }
        else
        {
            /// Finish data processing and create another pipe.
            is_consume_finished = true;
            return Status::Ready;
        }
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    current_chunk = input.pull();
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
        initGenerate();
    else
    {
        consume(std::move(current_chunk));
        read_current_chunk = false;
    }
}

Processors AggregatingTransform::expandPipeline()
{
    auto & out = processors.back()->getOutputs().front();
    inputs.emplace_back(out.getHeader(), this);
    connect(out, inputs.back());
    is_pipeline_created = true;
    return std::move(processors);
}

void AggregatingTransform::consume(Chunk chunk)
{
    UInt64 num_rows = chunk.getNumRows();

    if (num_rows == 0 && params->params.empty_result_for_aggregation_by_empty_set)
        return;

    if (!is_consume_started)
    {
        LOG_TRACE(log, "Aggregating");
        is_consume_started = true;
    }

    src_rows += chunk.getNumRows();
    src_bytes += chunk.bytes();

    if (!params->aggregator.executeOnBlock(chunk.detachColumns(), num_rows, variants, key_columns, aggregate_columns, no_more_keys))
        is_consume_finished = true;
}

void AggregatingTransform::initGenerate()
{
    if (is_generate_initialized)
        return;

    is_generate_initialized = true;

    /// If there was no data, and we aggregate without keys, and we must return single row with the result of empty aggregation.
    /// To do this, we pass a block with zero rows to aggregate.
    if (variants.empty() && params->params.keys_size == 0 && !params->params.empty_result_for_aggregation_by_empty_set)
        params->aggregator.executeOnBlock(getInputs().front().getHeader(), variants, key_columns, aggregate_columns, no_more_keys);

    double elapsed_seconds = watch.elapsedSeconds();
    size_t rows = variants.sizeWithoutOverflowRow();
    LOG_TRACE(log, std::fixed << std::setprecision(3)
                              << "Aggregated. " << src_rows << " to " << rows << " rows (from " << src_bytes / 1048576.0 << " MiB)"
                              << " in " << elapsed_seconds << " sec."
                              << " (" << src_rows / elapsed_seconds << " rows/sec., " << src_bytes / elapsed_seconds / 1048576.0 << " MiB/sec.)");

    if (params->aggregator.hasTemporaryFiles())
    {
        if (variants.isConvertibleToTwoLevel())
            variants.convertToTwoLevel();

        /// Flush data in the RAM to disk also. It's easier than merging on-disk and RAM data.
        if (variants.size())
            params->aggregator.writeToTemporaryFile(variants);
    }

    if (many_data->num_finished.fetch_add(1) + 1 < many_data->variants.size())
        return;

    if (!params->aggregator.hasTemporaryFiles())
    {
        auto prepared_data = params->aggregator.prepareVariantsToMerge(many_data->variants);
        auto prepared_data_ptr = std::make_shared<ManyAggregatedDataVariants>(std::move(prepared_data));
        processors.emplace_back(std::make_shared<ConvertingAggregatedToChunksTransform>(params, std::move(prepared_data_ptr), max_threads));
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

                if (cur_variants->size())
                    params->aggregator.writeToTemporaryFile(*cur_variants);
            }
        }

        auto header = params->aggregator.getHeader(false);

        const auto & files = params->aggregator.getTemporaryFiles();
        BlockInputStreams input_streams;
        for (const auto & file : files.files)
            processors.emplace_back(std::make_unique<SourceFromNativeStream>(header, file->path()));

        LOG_TRACE(log, "Will merge " << files.files.size() << " temporary files of size "
                                     << (files.sum_size_compressed / 1048576.0) << " MiB compressed, "
                                     << (files.sum_size_uncompressed / 1048576.0) << " MiB uncompressed.");

        auto pipe = createMergingAggregatedMemoryEfficientPipe(
                header, params, files.files.size(), temporary_data_merge_threads);

        auto input = pipe.front()->getInputs().begin();
        for (auto & processor : processors)
            connect(processor->getOutputs().front(), *(input++));

        processors.insert(processors.end(), pipe.begin(), pipe.end());
    }
}

}
