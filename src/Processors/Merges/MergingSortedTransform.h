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
        SharedHeader header,
        size_t num_inputs,
        const SortDescription & description,
        size_t max_block_size_rows,
        size_t max_block_size_bytes,
        std::optional<size_t> max_dynamic_subcolumns_,
        SortingQueueStrategy sorting_queue_strategy,
        UInt64 limit_ = 0,
        bool always_read_till_end_ = false,
        WriteBuffer * out_row_sources_buf_ = nullptr,
        const std::optional<String> & filter_column_name_ = std::nullopt,
        bool use_average_block_sizes = false,
        bool apply_virtual_row_conversions = true,
        bool have_all_inputs_ = true);

    String getName() const override { return "MergingSortedTransform"; }

protected:
    void onNewInput() override;
    void onFinish() override;
};

}
