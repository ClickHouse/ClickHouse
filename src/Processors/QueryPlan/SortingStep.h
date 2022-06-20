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
        const SortDescription & description_,
        size_t max_block_size_,
        UInt64 limit_,
        SizeLimits size_limits_,
        size_t max_bytes_before_remerge_,
        double remerge_lowered_memory_bytes_ratio_,
        size_t max_bytes_before_external_sort_,
        VolumePtr tmp_volume_,
        size_t min_free_disk_space_);

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

    void convertToFinishSorting(SortDescription prefix_description);

private:

    enum class Type
    {
        Full,
        FinishSorting,
        MergingSorted,
    };

    Type type;

    SortDescription prefix_description;
    SortDescription result_description;
    size_t max_block_size;
    UInt64 limit;
    SizeLimits size_limits;

    size_t max_bytes_before_remerge = 0;
    double remerge_lowered_memory_bytes_ratio = 0;
    size_t max_bytes_before_external_sort = 0;
    VolumePtr tmp_volume;
    size_t min_free_disk_space = 0;
};

}
