#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <IO/WriteBuffer.h>

#include <Common/logger_useful.h>

namespace DB
{

MergingSortedTransform::MergingSortedTransform(
    const Block & header,
    size_t num_inputs,
    SortDescription  description_,
    size_t max_block_size,
    UInt64 limit_,
    WriteBuffer * out_row_sources_buf_,
    bool quiet_,
    bool use_average_block_sizes,
    bool have_all_inputs_)
    : IMergingTransform(
        num_inputs, header, header, have_all_inputs_, limit_,
        header,
        num_inputs,
        std::move(description_),
        max_block_size,
        limit_,
        out_row_sources_buf_,
        use_average_block_sizes)
    , quiet(quiet_)
{
}

void MergingSortedTransform::onNewInput()
{
    algorithm.addInput();
}

void MergingSortedTransform::onFinish()
{
    if (quiet)
        return;

    const auto & merged_data = algorithm.getMergedData();

    auto * log = &Poco::Logger::get("MergingSortedTransform");

    double seconds = total_stopwatch.elapsedSeconds();

    if (!seconds)
        LOG_DEBUG(log, "Merge sorted {} blocks, {} rows in 0 sec.", merged_data.totalChunks(), merged_data.totalMergedRows());
    else
        LOG_DEBUG(log, "Merge sorted {} blocks, {} rows in {} sec., {} rows/sec., {}/sec",
            merged_data.totalChunks(), merged_data.totalMergedRows(), seconds,
            merged_data.totalMergedRows() / seconds,
            ReadableSize(merged_data.totalAllocatedBytes() / seconds));
}

}
