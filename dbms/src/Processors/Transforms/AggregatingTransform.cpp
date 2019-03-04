#include <Processors/Transforms/AggregatingTransform.h>

#include <Common/ClickHouseRevision.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>

namespace ProfileEvents
{
    extern const Event ExternalAggregationMerge;
}

namespace DB
{

AggregatingTransform::AggregatingTransform(Block header, AggregatingTransformParamsPtr params_)
    : AggregatingTransform(std::move(header), std::move(params_)
    , std::make_unique<ManyAggregatedData>(1), 0, 1, 1)
{
}

AggregatingTransform::AggregatingTransform(
    Block header, AggregatingTransformParamsPtr params_, ManyAggregatedDataPtr many_data_,
    size_t current_variant, size_t temporary_data_merge_threads, size_t max_threads)
    : IAccumulatingTransform(std::move(header), params_->getHeader()), params(std::move(params_))
    , key(params->params.keys_size)
    , key_columns(params->params.keys_size)
    , aggregate_columns(params->params.aggregates_size)
    , many_data(std::move(many_data_))
    , variants(*many_data->variants[current_variant])
    , max_threads(std::min(many_data->variants.size(), max_threads))
    , temporary_data_merge_threads(temporary_data_merge_threads)
{
}

AggregatingTransform::~AggregatingTransform() = default;

void AggregatingTransform::consume(Chunk chunk)
{
    LOG_TRACE(log, "Aggregating");

    src_rows += chunk.getNumRows();
    src_bytes += chunk.bytes();

    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    if (!params->aggregator.executeOnBlock(block, variants, key_columns, aggregate_columns, key, no_more_keys))
        finishConsume();
}

void AggregatingTransform::initGenerate()
{
    if (is_generate_initialized)
        return;

    /// If there was no data, and we aggregate without keys, and we must return single row with the result of empty aggregation.
    /// To do this, we pass a block with zero rows to aggregate.
    if (variants.empty() && params->params.keys_size == 0 && !params->params.empty_result_for_aggregation_by_empty_set)
        params->aggregator.executeOnBlock(getInputPort().getHeader(), variants, key_columns, aggregate_columns, key, no_more_keys);

    double elapsed_seconds = watch.elapsedSeconds();
    size_t rows = variants.sizeWithoutOverflowRow();
    LOG_TRACE(log, std::fixed << std::setprecision(3)
                              << "Aggregated. " << src_rows << " to " << rows << " rows (from " << src_bytes / 1048576.0 << " MiB)"
                              << " in " << elapsed_seconds << " sec."
                              << " (" << src_rows / elapsed_seconds << " rows/sec., " << src_bytes / elapsed_seconds / 1048576.0 << " MiB/sec.)");

    if (params->aggregator.hasTemporaryFiles())
    {
        /// Flush data in the RAM to disk also. It's easier than merging on-disk and RAM data.
        if (many_data->variants.size() > 1 && variants.size())
            params->aggregator.writeToTemporaryFile(variants);

        if (variants.isConvertibleToTwoLevel())
            variants.convertToTwoLevel();
    }

    if (many_data->num_finished.fetch_add(1) + 1 < many_data->variants.size())
        return;

    if (!params->aggregator.hasTemporaryFiles())
    {
        impl = params->aggregator.mergeAndConvertToBlocks(many_data->variants, params->final, max_threads);
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

        const auto & files = params->aggregator.getTemporaryFiles();
        BlockInputStreams input_streams;
        for (const auto & file : files.files)
        {
            temporary_inputs.emplace_back(std::make_unique<TemporaryFileStream>(file->path()));
            input_streams.emplace_back(temporary_inputs.back()->block_in);
        }

        LOG_TRACE(log, "Will merge " << files.files.size() << " temporary files of size "
                                     << (files.sum_size_compressed / 1048576.0) << " MiB compressed, "
                                     << (files.sum_size_uncompressed / 1048576.0) << " MiB uncompressed.");

        impl = std::make_unique<MergingAggregatedMemoryEfficientBlockInputStream>(
                input_streams, params->params, params->final, temporary_data_merge_threads, temporary_data_merge_threads);
    }

    is_generate_initialized = true;
}

Chunk AggregatingTransform::generate()
{
    if (!is_generate_initialized)
        initGenerate();

    if (!impl)
        return {};

    auto block = impl->read();
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

AggregatingTransform::TemporaryFileStream::TemporaryFileStream(const std::string & path)
    : file_in(path), compressed_in(file_in),
      block_in(std::make_shared<NativeBlockInputStream>(compressed_in, ClickHouseRevision::get())) {}

}
