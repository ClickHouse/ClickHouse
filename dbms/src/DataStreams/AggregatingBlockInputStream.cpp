#include <Common/ClickHouseRevision.h>

#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>
#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/NativeBlockInputStream.h>


namespace ProfileEvents
{
    extern const Event ExternalAggregationMerge;
}

namespace DB
{


Block AggregatingBlockInputStream::readImpl()
{
    if (!executed)
    {
        executed = true;
        AggregatedDataVariantsPtr data_variants = std::make_shared<AggregatedDataVariants>();

        Aggregator::CancellationHook hook = [&]() { return this->isCancelled(); };
        aggregator.setCancellationHook(hook);

        aggregator.execute(children.back(), *data_variants);

        if (!aggregator.hasTemporaryFiles())
        {
            ManyAggregatedDataVariants many_data { data_variants };
            impl = aggregator.mergeAndConvertToBlocks(many_data, final, 1);
        }
        else
        {
            /** If there are temporary files with partially-aggregated data on the disk,
              *  then read and merge them, spending the minimum amount of memory.
              */

            ProfileEvents::increment(ProfileEvents::ExternalAggregationMerge);

            if (!isCancelled())
            {
                /// Flush data in the RAM to disk also. It's easier.
                size_t rows = data_variants->sizeWithoutOverflowRow();
                if (rows)
                    aggregator.writeToTemporaryFile(*data_variants, rows);
            }

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

            impl = std::make_unique<MergingAggregatedMemoryEfficientBlockInputStream>(input_streams, params, final, 1, 1);
        }
    }

    Block res;
    if (isCancelled() || !impl)
        return res;

    return impl->read();
}


AggregatingBlockInputStream::TemporaryFileStream::TemporaryFileStream(const std::string & path)
    : file_in(path), compressed_in(file_in), block_in(std::make_shared<NativeBlockInputStream>(compressed_in, ClickHouseRevision::get())) {}


}
