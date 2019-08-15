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

namespace
{
    class SourceFromNativeStream : public ISource
    {
    public:
        SourceFromNativeStream(const Block & header, const std::string & path)
                : ISource(header), file_in(path), compressed_in(file_in)
                , block_in(std::make_shared<NativeBlockInputStream>(compressed_in, ClickHouseRevision::get()))
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

            auto info = std::make_shared<AggregatedChunkInfo>();
            info->bucket_num = block.info.bucket_num;
            info->is_overflows = block.info.is_overflows;

            UInt64 num_rows = block.rows();
            Chunk chunk(block.getColumns(), num_rows);
            chunk.setChunkInfo(std::move(info));

            return chunk;
        }

    private:
        ReadBufferFromFile file_in;
        CompressedReadBuffer compressed_in;
        BlockInputStreamPtr block_in;
    };

    class ConvertingAggregatedToBlocksTransform : public ISource
    {
    public:
        ConvertingAggregatedToBlocksTransform(Block header, AggregatingTransformParamsPtr params_, BlockInputStreamPtr stream_)
            : ISource(std::move(header)), params(std::move(params_)), stream(std::move(stream_)) {}

        String getName() const override { return "ConvertingAggregatedToBlocksTransform"; }

    protected:
        Chunk generate() override
        {
            auto block = stream->read();
            if (!block)
                return {};

            auto info = std::make_shared<AggregatedChunkInfo>();
            info->bucket_num = block.info.bucket_num;
            info->is_overflows = block.info.is_overflows;

            UInt64 num_rows = block.rows();
            Chunk chunk(block.getColumns(), num_rows);
            chunk.setChunkInfo(std::move(info));

            return chunk;
        }

    private:
        /// Store params because aggregator must be destroyed after stream. Order is important.
        AggregatingTransformParamsPtr params;
        BlockInputStreamPtr stream;
    };
}

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
    if (chunk.getNumRows() == 0 && params->params.empty_result_for_aggregation_by_empty_set)
        return;

    if (!is_consume_started)
    {
        LOG_TRACE(log, "Aggregating");
        is_consume_started = true;
    }

    src_rows += chunk.getNumRows();
    src_bytes += chunk.bytes();

    auto block = getInputs().front().getHeader().cloneWithColumns(chunk.detachColumns());

    if (!params->aggregator.executeOnBlock(block, variants, key_columns, aggregate_columns, no_more_keys))
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
        auto stream = params->aggregator.mergeAndConvertToBlocks(many_data->variants, params->final, max_threads);
        processors.emplace_back(std::make_shared<ConvertingAggregatedToBlocksTransform>(stream->getHeader(), params, std::move(stream)));
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
