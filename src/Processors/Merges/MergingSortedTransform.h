#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/Algorithms/MergingSortedAlgorithm.h>


namespace DB
{

struct TopKThresholdTracker;
using TopKThresholdTrackerPtr = std::shared_ptr<TopKThresholdTracker>;

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
        size_t virtual_row_prefetch_window = 0,
        bool have_all_inputs_ = true);

    String getName() const override { return "MergingSortedTransform"; }

    /// TopN dynamic filtering: publish the value of the first sort column in the last row within the
    /// limit - the exact final threshold - once the merge gets there. The sorting transforms before the
    /// merge only publish the thresholds of their own streams, which can be much looser.
    void setTopKThresholdTracker(TopKThresholdTrackerPtr threshold_tracker_, const String & sort_column_name, UInt64 limit_);

protected:
    void onNewInput() override;
    void onFinish() override;
    void onOutputChunk(const Chunk & chunk) override;

private:
    TopKThresholdTrackerPtr threshold_tracker;
    UInt64 threshold_limit = 0;
    size_t threshold_sort_column_position = 0;
    UInt64 rows_before_threshold = 0;
    bool threshold_published = false;
};

}
