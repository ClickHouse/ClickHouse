#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

/// Merge streams of data into single sorted stream.
class MergingFinal : public ITransformingStep
{
public:
    explicit MergingFinal(
            const DataStream & input_stream,
            size_t num_output_streams_,
            SortDescription sort_description_,
            MergeTreeData::MergingParams params_,
            Names partition_key_columns_,
            size_t max_block_size_);

    String getName() const override { return "MergingFinal"; }

    void transformPipeline(QueryPipeline & pipeline) override;

    void describeActions(FormatSettings & settings) const override;

private:
    size_t num_output_streams;
    SortDescription sort_description;
    MergeTreeData::MergingParams merging_params;
    Names partition_key_columns;
    size_t max_block_size;
};

}
