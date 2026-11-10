#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Sources/LazyFinalSharedState.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;
class LazyFinalKeyAnalysisStep;

class LazyReadReplacingFinalStep : public ISourceStep
{
public:
    LazyReadReplacingFinalStep(
        StorageMetadataPtr metadata_snapshot_,
        const MergeTreeData & data_,
        ContextPtr query_context_,
        LazyFinalSharedStatePtr shared_state_,
        LazyFinalKeyAnalysisStep * analysis_step_);

    enum class Stage : size_t
    {
        ReadAndAggregate = 0,
        ReplacingMerge = 1,
    };

    String getName() const override { return "LazyReadReplacingFinal"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;
    QueryPlanRawPtrs getChildPlans() override;

    std::vector<size_t> getStepGroups() const override;
    String getStepGroupName(size_t group) const override;

private:
    StorageMetadataPtr metadata_snapshot;
    const MergeTreeData & data;
    ContextPtr query_context;
    LazyFinalSharedStatePtr shared_state;
    LazyFinalKeyAnalysisStep * analysis_step;

    std::optional<QueryPlan> explain_plan;
};

}
