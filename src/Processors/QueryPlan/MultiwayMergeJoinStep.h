#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <Core/Names.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/Transforms/MergeJoinTransform.h>

namespace DB
{

/// INNER ALL join of N inputs sorted by the same key, executed as one merge of all of them.
/// Built by `optimizeMultiwayMergeJoin` from a subtree of `full_sorting_merge` joins on that key.
class MultiwayMergeJoinStep : public IQueryPlanStep
{
public:
    MultiwayMergeJoinStep(
        SharedHeaders input_headers_,
        VectorWithMemoryTracking<Names> key_names_,
        SharedHeader output_header_,
        size_t max_block_size_,
        size_t max_streams_,
        int null_direction_);

    [[nodiscard]] String getName() const override { return "MultiwayMergeJoin"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;
    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    [[nodiscard]] QueryPlanStepPtr clone() const override;

    [[nodiscard]] const Names & getKeyNames(size_t input) const { return key_names.at(input); }

    /// Every input is split into the same primary key ranges, see `optimizeJoinByShards`.
    void enableJoinByLayers(JoinStep::PrimaryKeySharding sharding_) { sharding = std::move(sharding_); }

private:
    void updateOutputHeader() override;

    VectorWithMemoryTracking<Names> key_names;
    SharedHeader result_header;
    VectorWithMemoryTracking<MultiwayMergeJoinAlgorithm::OutputColumn> output_columns;
    size_t max_block_size;
    size_t max_streams;
    int null_direction;
    JoinStep::PrimaryKeySharding sharding;
};

}
