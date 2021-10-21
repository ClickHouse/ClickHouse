#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>
#include <DataStreams/SizeLimits.h>
#include <Disks/IVolume.h>

namespace DB
{

/// Sorts stream of data. See MergeSortingTransform.
class MergeSortingStep : public ITransformingStep
{
public:
    explicit MergeSortingStep(
            const DataStream & input_stream,
            const SortDescription & description_,
            size_t max_merged_block_size_,
            UInt64 limit_,
            size_t max_bytes_before_remerge_,
            double remerge_lowered_memory_bytes_ratio_,
            size_t max_bytes_before_external_sort_,
            VolumePtr tmp_volume_,
            size_t min_free_disk_space_);

    String getName() const override { return "MergeSorting"; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    /// Add limit or change it to lower value.
    void updateLimit(size_t limit_);

private:
    SortDescription description;
    size_t max_merged_block_size;
    UInt64 limit;

    size_t max_bytes_before_remerge;
    double remerge_lowered_memory_bytes_ratio;
    size_t max_bytes_before_external_sort;
    VolumePtr tmp_volume;
    size_t min_free_disk_space;
};

}

