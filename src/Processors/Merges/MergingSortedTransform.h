#pragma once

#include <Processors/Merges/IMergingTransform.h>
#include <Processors/Merges/Algorithms/MergingSortedAlgorithm.h>

#include <memory>
#include <mutex>

namespace DB
{

/// Collects output bytes from all deferred materialization workers and emits the
/// usual merged-sorted diagnostic after both the merge and all workers finish.
class MergingSortedTransformStats
{
public:
    explicit MergingSortedTransformStats(size_t materializers_) : unfinished_materializers(materializers_) {}

    void finishMerge(IMergingAlgorithm::MergedStats stats, UInt64 elapsed_ns);
    void finishMaterializer(UInt64 materialized_bytes, UInt64 materialization_elapsed_ns);

private:
    void tryLog();

    std::mutex mutex;
    IMergingAlgorithm::MergedStats merged_stats;
    UInt64 merging_elapsed_ns = 0;
    UInt64 materialization_elapsed_ns = 0;
    UInt64 materialized_bytes = 0;
    size_t unfinished_materializers;
    bool merge_finished = false;
    bool logged = false;
};

using MergingSortedTransformStatsPtr = std::shared_ptr<MergingSortedTransformStats>;

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
        bool have_all_inputs_ = true,
        bool defer_materialization = false,
        MergingSortedTransformStatsPtr parallel_materialization_stats_ = {});

    String getName() const override { return "MergingSortedTransform"; }

protected:
    void onNewInput() override;
    void onFinish() override;

private:
    MergingSortedTransformStatsPtr parallel_materialization_stats;
};

}
