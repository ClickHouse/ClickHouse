#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/Algorithms/MergingSortedAlgorithm.h>


namespace DB
{

/// Implementation of IMergingTransform via MergingSortedAlgorithm.
class MergingSortedTransform final : public IMergingTransform<MergingSortedAlgorithm>
{
public:
    MergingSortedTransform(
        const Block & header,
        size_t num_inputs,
        const SortDescription & description,
        size_t max_block_size,
        SortingQueueStrategy sorting_queue_strategy,
        UInt64 limit_ = 0,
        WriteBuffer * out_row_sources_buf_ = nullptr,
        bool quiet_ = false,
        bool use_average_block_sizes = false,
        bool have_all_inputs_ = true);

    String getName() const override { return "MergingSortedTransform"; }

protected:
    void onNewInput() override;
    void onFinish() override;

private:
    bool quiet = false;
};

}
