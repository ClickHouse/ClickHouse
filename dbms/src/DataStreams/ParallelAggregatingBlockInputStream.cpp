#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>
#include <DataStreams/ParallelAggregatingBlockInputStream.h>
#include <Common/ClickHouseRevision.h>


namespace ProfileEvents
{
    extern const Event ExternalAggregationMerge;
}


namespace DB
{


ParallelAggregatingBlockInputStream::ParallelAggregatingBlockInputStream(
    const BlockInputStreams & inputs, const BlockInputStreamPtr & additional_input_at_end,
    const Aggregator::Params & params_, bool final_, size_t max_threads_, size_t temporary_data_merge_threads_)
    : params(params_), aggregator(params),
    final(final_), max_threads(std::min(inputs.size(), max_threads_)), temporary_data_merge_threads(temporary_data_merge_threads_),
    keys_size(params.keys_size), aggregates_size(params.aggregates_size),
    handler(*this), processor(inputs, additional_input_at_end, max_threads, handler)
{
    children = inputs;
    if (additional_input_at_end)
        children.push_back(additional_input_at_end);
}


Block ParallelAggregatingBlockInputStream::getHeader() const
{
    return aggregator.getHeader(final);
}


void ParallelAggregatingBlockInputStream::cancel(bool kill)
{
    if (kill)
        is_killed = true;
    bool old_val = false;
    if (!is_cancelled.compare_exchange_strong(old_val, true, std::memory_order_seq_cst, std::memory_order_relaxed))
        return;

    if (!executed)
        processor.cancel(kill);
}


Block ParallelAggregatingBlockInputStream::readImpl()
{
    if (!executed)
    {
        Aggregator::CancellationHook hook = [&]() { return this->isCancelled(); };
        aggregator.setCancellationHook(hook);

        execute();

        if (isCancelledOrThrowIfKilled())
            return {};

        if (!aggregator.hasTemporaryFiles())
        {
            /** If all partially-aggregated data is in RAM, then merge them in parallel, also in RAM.
                */
            impl = aggregator.mergeAndConvertToBlocks(many_data, final, max_threads);
        }
        else
        {
            /** If there are temporary files with partially-aggregated data on the disk,
                *  then read and merge them, spending the minimum amount of memory.
                */

            ProfileEvents::increment(ProfileEvents::ExternalAggregationMerge);

            const auto & files = aggregator.getTemporaryFiles();
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
                input_streams, params, final, temporary_data_merge_threads, temporary_data_merge_threads);
        }

        executed = true;
    }

    Block res;
    if (isCancelledOrThrowIfKilled() || !impl)
        return res;

    return impl->read();
}


ParallelAggregatingBlockInputStream::TemporaryFileStream::TemporaryFileStream(const std::string & path)
    : file_in(path), compressed_in(file_in),
    block_in(std::make_shared<NativeBlockInputStream>(compressed_in, ClickHouseRevision::get())) {}



void ParallelAggregatingBlockInputStream::Handler::onBlock(Block & block, size_t thread_num)
{
    parent.aggregator.executeOnBlock(block, *parent.many_data[thread_num],
        parent.threads_data[thread_num].key_columns, parent.threads_data[thread_num].aggregate_columns,
        parent.threads_data[thread_num].key, parent.no_more_keys);

    parent.threads_data[thread_num].src_rows += block.rows();
    parent.threads_data[thread_num].src_bytes += block.bytes();
}

void ParallelAggregatingBlockInputStream::Handler::onFinishThread(size_t thread_num)
{
    if (!parent.isCancelled() && parent.aggregator.hasTemporaryFiles())
    {
        /// Flush data in the RAM to disk. So it's easier to unite them later.
        auto & data = *parent.many_data[thread_num];

        if (data.isConvertibleToTwoLevel())
            data.convertToTwoLevel();

        if (data.size())
            parent.aggregator.writeToTemporaryFile(data);
    }
}

void ParallelAggregatingBlockInputStream::Handler::onFinish()
{
    if (!parent.isCancelled() && parent.aggregator.hasTemporaryFiles())
    {
        /// It may happen that some data has not yet been flushed,
        ///  because at the time of `onFinishThread` call, no data has been flushed to disk, and then some were.
        for (auto & data : parent.many_data)
        {
            if (data->isConvertibleToTwoLevel())
                data->convertToTwoLevel();

            if (data->size())
                parent.aggregator.writeToTemporaryFile(*data);
        }
    }
}

void ParallelAggregatingBlockInputStream::Handler::onException(std::exception_ptr & exception, size_t thread_num)
{
    parent.exceptions[thread_num] = exception;
    parent.cancel(false);
}


void ParallelAggregatingBlockInputStream::execute()
{
    many_data.resize(max_threads);
    exceptions.resize(max_threads);

    for (size_t i = 0; i < max_threads; ++i)
        threads_data.emplace_back(keys_size, aggregates_size);

    LOG_TRACE(log, "Aggregating");

    Stopwatch watch;

    for (auto & elem : many_data)
        elem = std::make_shared<AggregatedDataVariants>();

    processor.process();
    processor.wait();

    rethrowFirstException(exceptions);

    if (isCancelledOrThrowIfKilled())
        return;

    double elapsed_seconds = watch.elapsedSeconds();

    size_t total_src_rows = 0;
    size_t total_src_bytes = 0;
    for (size_t i = 0; i < max_threads; ++i)
    {
        size_t rows = many_data[i]->size();
        LOG_TRACE(log, std::fixed << std::setprecision(3)
            << "Aggregated. " << threads_data[i].src_rows << " to " << rows << " rows"
                << " (from " << threads_data[i].src_bytes / 1048576.0 << " MiB)"
            << " in " << elapsed_seconds << " sec."
            << " (" << threads_data[i].src_rows / elapsed_seconds << " rows/sec., "
                << threads_data[i].src_bytes / elapsed_seconds / 1048576.0 << " MiB/sec.)");

        total_src_rows += threads_data[i].src_rows;
        total_src_bytes += threads_data[i].src_bytes;
    }
    LOG_TRACE(log, std::fixed << std::setprecision(3)
        << "Total aggregated. " << total_src_rows << " rows (from " << total_src_bytes / 1048576.0 << " MiB)"
        << " in " << elapsed_seconds << " sec."
        << " (" << total_src_rows / elapsed_seconds << " rows/sec., " << total_src_bytes / elapsed_seconds / 1048576.0 << " MiB/sec.)");

    /// If there was no data, and we aggregate without keys, we must return single row with the result of empty aggregation.
    /// To do this, we pass a block with zero rows to aggregate.
    if (total_src_rows == 0 && params.keys_size == 0 && !params.empty_result_for_aggregation_by_empty_set)
        aggregator.executeOnBlock(children.at(0)->getHeader(), *many_data[0],
            threads_data[0].key_columns, threads_data[0].aggregate_columns,
            threads_data[0].key, no_more_keys);
}

}
