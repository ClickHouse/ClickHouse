#include <Processors/Merges/MergingSortedTransform.h>
#include <DataStreams/ColumnGathererStream.h>
#include <IO/WriteBuffer.h>
#include <DataStreams/materializeBlock.h>

#include <common/logger_useful.h>

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
        num_inputs, header, header, have_all_inputs_,
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

    auto * log = &Logger::get("MergingSortedTransform");

    double seconds = total_stopwatch.elapsedSeconds();

    std::stringstream message;
    message << std::fixed << std::setprecision(2)
            << "Merge sorted " << merged_data.totalChunks() << " blocks, " << merged_data.totalMergedRows() << " rows"
            << " in " << seconds << " sec.";

    if (seconds != 0)
        message << ", "
                << merged_data.totalMergedRows() / seconds << " rows/sec., "
                << merged_data.totalAllocatedBytes() / 1000000.0 / seconds << " MB/sec.";

    LOG_DEBUG(log, message.str());
}

}
