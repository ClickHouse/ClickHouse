#pragma once

#include <Processors/IProcessor.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/Sources/LazyFinalSharedState.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

class QueryPlan;
struct RangesInDataParts;

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class LazyReadReplacingFinalSource : public IProcessor
{
public:
    LazyReadReplacingFinalSource(
        StorageMetadataPtr metadata_snapshot_,
        const MergeTreeData & data_,
        ContextPtr query_context_,
        LazyFinalSharedStatePtr shared_state_);

    String getName() const override { return "LazyReadReplacingFinalSource"; }
    Status prepare() override;
    void work() override;
    PipelineUpdate updatePipeline() override;

    /// Build the aggregation plan (sorting key expr + tiebreaker + argMax + rename + is_deleted filter)
    /// on top of a ReadFromMergeTree step. Used both for execution and EXPLAIN.
    static QueryPlan buildPlanFromReadingStep(
        std::unique_ptr<ReadFromMergeTree> reading,
        const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeData & data,
        ContextPtr query_context);

private:
    OutputPort * pipeline_output = nullptr;
    const StorageMetadataPtr metadata_snapshot;
    const MergeTreeData & data;
    const ContextPtr query_context;
    LazyFinalSharedStatePtr shared_state;

    QueryPlanResourceHolder resources;
    Processors processors;
};

}
