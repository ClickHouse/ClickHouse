#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Storages/MergeTree/RangesInDataPart.h>

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class LazyFinalKeyAnalysisStep : public ITransformingStep
{
public:
    LazyFinalKeyAnalysisStep(
        SharedHeader input_header_,
        FutureSetPtr future_set_,
        ContextPtr query_context_,
        StorageMetadataPtr metadata_snapshot_,
        RangesInDataParts ranges_);

    String getName() const override { return "LazyFinalKeyAnalysis"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    void updateOutputHeader() override {}

    FutureSetPtr future_set;
    ContextPtr query_context;
    StorageMetadataPtr metadata_snapshot;
    RangesInDataParts ranges;
};

}
