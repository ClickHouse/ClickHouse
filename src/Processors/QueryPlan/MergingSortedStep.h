#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>
#include <DataStreams/SizeLimits.h>
#include <Disks/IVolume.h>

namespace DB
{

class MergingSortedStep : public ITransformingStep
{
public:
    explicit MergingSortedStep(
        const DataStream & input_stream,
        SortDescription sort_description_,
        size_t max_block_size_,
        UInt64 limit_ = 0);

    String getName() const override { return "MergingSorted"; }

    void transformPipeline(QueryPipeline & pipeline) override;

private:
    SortDescription sort_description;
    size_t max_block_size;
    UInt64 limit;
};

}


