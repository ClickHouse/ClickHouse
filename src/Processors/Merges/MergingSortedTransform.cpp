#include <IO/WriteBuffer.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Port.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
    extern const Event MergingSortedMilliseconds;
    extern const Event MergingSortedMaterializationMilliseconds;
}

namespace DB
{

void MergingSortedTransformStats::finishMerge(IMergingAlgorithm::MergedStats stats, UInt64 elapsed_ns)
{
    std::lock_guard lock(mutex);
    chassert(!merge_finished);
    merged_stats = stats;
    merging_elapsed_ns = elapsed_ns;
    merge_finished = true;
    tryLog();
}

void MergingSortedTransformStats::finishMaterializer(UInt64 materialized_bytes_, UInt64 materialization_elapsed_ns_)
{
    std::lock_guard lock(mutex);
    chassert(unfinished_materializers > 0);
    materialized_bytes += materialized_bytes_;
    materialization_elapsed_ns += materialization_elapsed_ns_;
    --unfinished_materializers;
    tryLog();
}

void MergingSortedTransformStats::tryLog()
{
    if (logged || !merge_finished || unfinished_materializers)
        return;

    logged = true;
    merged_stats.bytes = materialized_bytes;
    /// Preserve the total semantics of `MergingSortedMilliseconds`: parallelization moves
    /// materialization to other processors, but it remains part of the ordered-merge cost.
    ProfileEvents::increment(
        ProfileEvents::MergingSortedMilliseconds,
        (merging_elapsed_ns + materialization_elapsed_ns) / 1000000ULL);
    ProfileEvents::increment(
        ProfileEvents::MergingSortedMaterializationMilliseconds,
        materialization_elapsed_ns / 1000000ULL);

    /// Don't print info for small parts (< 1M rows).
    if (merged_stats.rows < 1000000)
        return;

    const double merging_seconds = static_cast<double>(merging_elapsed_ns) / 1000000000ULL;
    const double materialization_seconds = static_cast<double>(materialization_elapsed_ns) / 1000000000ULL;
    auto log = getLogger("MergingSortedTransform");
    LOG_DEBUG(
        log,
        "Merged sorted, {} blocks, {} rows, {} bytes using {:.3f} sec. serial merge time and {:.3f} sec. total parallel materialization worker time.",
        merged_stats.blocks,
        merged_stats.rows,
        merged_stats.bytes,
        merging_seconds,
        materialization_seconds);
}

MergingSortedTransform::MergingSortedTransform(
    SharedHeader header,
    size_t num_inputs,
    const SortDescription & description_,
    size_t max_block_size_rows,
    size_t max_block_size_bytes,
    std::optional<size_t> max_dynamic_subcolumns_,
    SortingQueueStrategy sorting_queue_strategy,
    UInt64 limit_,
    bool always_read_till_end_,
    WriteBuffer * out_row_sources_buf_,
    const std::optional<String> & filter_column_name_,
    bool use_average_block_sizes,
    bool apply_virtual_row_conversions,
    size_t virtual_row_prefetch_window,
    bool have_all_inputs_,
    bool defer_materialization,
    MergingSortedTransformStatsPtr parallel_materialization_stats_)
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
        max_dynamic_subcolumns_,
        sorting_queue_strategy,
        limit_,
        out_row_sources_buf_,
        filter_column_name_,
        use_average_block_sizes,
        apply_virtual_row_conversions,
        virtual_row_prefetch_window,
        defer_materialization)
    , parallel_materialization_stats(std::move(parallel_materialization_stats_))
{
    chassert(defer_materialization == static_cast<bool>(parallel_materialization_stats));
}

void MergingSortedTransform::onNewInput()
{
    algorithm.addInput();
}

void MergingSortedTransform::onFinish()
{
    if (parallel_materialization_stats)
        parallel_materialization_stats->finishMerge(algorithm.getMergedStats(), merging_elapsed_ns);
    else
        logMergedStats(ProfileEvents::MergingSortedMilliseconds, "Merged sorted", getLogger("MergingSortedTransform"));
}

}
