#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <IO/WriteBuffer.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
    extern const Event MergingSortedMilliseconds;
}

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
{
}

void MergingSortedTransform::onNewInput()
{
    algorithm.addInput();
}

void MergingSortedTransform::onFinish()
{
    logMergedStats(ProfileEvents::MergingSortedMilliseconds, "Merged sorted", getLogger("MergingSortedTransform"));
}

}
