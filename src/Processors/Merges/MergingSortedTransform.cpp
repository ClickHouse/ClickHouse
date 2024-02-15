#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <IO/WriteBuffer.h>

#include <Common/logger_useful.h>
#include <Common/formatReadable.h>

namespace DB
{

MergingSortedTransform::MergingSortedTransform(
    const Block & header,
    size_t num_inputs,
    const SortDescription & description_,
    size_t max_block_size_rows,
    size_t max_block_size_bytes,
    SortingQueueStrategy sorting_queue_strategy,
    UInt64 limit_,
    bool always_read_till_end_,
    WriteBuffer * out_row_sources_buf_,
    bool quiet_,
    bool use_average_block_sizes,
    bool have_all_inputs_)
    : IMergingTransform(
        num_inputs,
        header,
        header,
        have_all_inputs_,
        limit_,
        always_read_till_end_,
        header,
        num_inputs,
        description_,
        max_block_size_rows,
        max_block_size_bytes,
        sorting_queue_strategy,
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

    auto log = getLogger("MergingSortedTransform");

    double seconds = total_stopwatch.elapsedSeconds();

    if (seconds == 0.0)
        LOG_DEBUG(log, "Merge sorted {} blocks, {} rows in 0 sec.", merged_data.totalChunks(), merged_data.totalMergedRows());
    else
        LOG_DEBUG(log, "Merge sorted {} blocks, {} rows in {} sec., {} rows/sec., {}/sec",
            merged_data.totalChunks(), merged_data.totalMergedRows(), seconds,
            merged_data.totalMergedRows() / seconds,
            ReadableSize(merged_data.totalAllocatedBytes() / seconds));
}

}
