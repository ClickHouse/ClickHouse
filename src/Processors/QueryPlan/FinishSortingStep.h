#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>

namespace DB
{

/// Finish sorting of pre-sorted data. See FinishSortingTransform.
class FinishSortingStep : public ITransformingStep
{
public:
    FinishSortingStep(
        const DataStream & input_stream_,
        SortDescription prefix_description_,
        SortDescription result_description_,
        size_t max_block_size,
        UInt64 limit);

    String getName() const override { return "FinishSorting"; }

    void transformPipeline(QueryPipeline & pipeline) override;

private:
    SortDescription prefix_description;
    SortDescription result_description;
    size_t max_block_size;
    UInt64 limit;
};

}
