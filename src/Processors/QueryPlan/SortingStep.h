#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>
#include <QueryPipeline/SizeLimits.h>
#include <Disks/IVolume.h>

namespace DB
{

/// Sort data stream
class SortingStep : public ITransformingStep
{
public:
    /// Full
    SortingStep(
        const DataStream & input_stream,
        SortDescription description_,
        size_t max_block_size_,
        UInt64 limit_,
        SizeLimits size_limits_,
        size_t max_bytes_before_remerge_,
        double remerge_lowered_memory_bytes_ratio_,
        size_t max_bytes_before_external_sort_,
        VolumePtr tmp_volume_,
        size_t min_free_disk_space_,
        bool optimize_sorting_by_input_stream_properties_);

    /// FinishSorting
    SortingStep(
        const DataStream & input_stream_,
        SortDescription prefix_description_,
        SortDescription result_description_,
        size_t max_block_size_,
        UInt64 limit_);

    /// MergingSorted
    SortingStep(
        const DataStream & input_stream,
        SortDescription sort_description_,
        size_t max_block_size_,
        UInt64 limit_ = 0);

    String getName() const override { return "Sorting"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    /// Add limit or change it to lower value.
    void updateLimit(size_t limit_);

    const SortDescription & getSortDescription() const { return result_description; }

    void convertToFinishSorting(SortDescription prefix_description);

private:
    void updateOutputStream() override;

    void mergingSorted(QueryPipelineBuilder & pipeline, const SortDescription & result_sort_desc, UInt64 limit_);
    void mergeSorting(QueryPipelineBuilder & pipeline, const SortDescription & result_sort_desc, UInt64 limit_);
    void finishSorting(
        QueryPipelineBuilder & pipeline, const SortDescription & input_sort_desc, const SortDescription & result_sort_desc, UInt64 limit_);
    void fullSort(
        QueryPipelineBuilder & pipeline,
        const SortDescription & result_sort_desc,
        UInt64 limit_,
        bool skip_partial_sort = false);

    enum class Type
    {
        Full,
        FinishSorting,
        MergingSorted,
    };

    Type type;

    SortDescription prefix_description;
    const SortDescription result_description;
    const size_t max_block_size;
    UInt64 limit;
    SizeLimits size_limits;

    size_t max_bytes_before_remerge = 0;
    double remerge_lowered_memory_bytes_ratio = 0;
    size_t max_bytes_before_external_sort = 0;
    VolumePtr tmp_volume;
    size_t min_free_disk_space = 0;
    const bool optimize_sorting_by_input_stream_properties = false;
};

}
