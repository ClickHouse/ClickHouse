#include <IO/WriteBuffer.h>
#include <Processors/Merges/MergingShuffledTransform.h>
#include <Processors/Port.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
    extern const Event MergingShuffledMilliseconds;
}

namespace DB
{

MergingShuffledTransform::MergingShuffledTransform(
    SharedHeader header,
    size_t num_inputs,
    size_t max_block_size_rows,
    size_t max_block_size_bytes,
    UInt64 limit_,
    bool always_read_till_end_,
    WriteBuffer * out_row_sources_buf_,
    bool use_average_block_sizes,
    bool apply_virtual_row_conversions,
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
        max_block_size_rows,
        max_block_size_bytes,
        limit_,
        out_row_sources_buf_,
        use_average_block_sizes,
        apply_virtual_row_conversions)
{
    LOG_TRACE(getLogger("MergingShuffledTransform"), "MergingShuffledTransform");
}

void MergingShuffledTransform::onNewInput()
{
    algorithm.addInput();
}

void MergingShuffledTransform::onFinish()
{
    logMergedStats(ProfileEvents::MergingShuffledMilliseconds, "Merged Shuffled", getLogger("MergingShuffledTransform"));
}

}
