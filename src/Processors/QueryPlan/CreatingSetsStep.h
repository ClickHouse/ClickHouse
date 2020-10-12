#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <DataStreams/SizeLimits.h>
#include <Interpreters/SubqueryForSet.h>

namespace DB
{

/// Creates sets for subqueries and JOIN. See CreatingSetsTransform.
class CreatingSetStep : public ITransformingStep
{
public:
    CreatingSetStep(
            const DataStream & input_stream_,
            Block header,
            String description_,
            SubqueryForSet subquery_for_set_,
            SizeLimits network_transfer_limits_,
            const Context & context_);

    String getName() const override { return "CreatingSet"; }

    void transformPipeline(QueryPipeline & pipeline) override;

    void describeActions(FormatSettings & settings) const override;

private:
    String description;
    SubqueryForSet subquery_for_set;
    SizeLimits network_transfer_limits;
    const Context & context;
};

class CreatingSetsStep : public IQueryPlanStep
{
public:
    CreatingSetsStep(DataStreams input_streams_);

    String getName() const override { return "CreatingSets"; }

    QueryPipelinePtr updatePipeline(QueryPipelines pipelines) override;

    void describePipeline(FormatSettings & settings) const override;

private:
    Processors processors;
};

void addCreatingSetsStep(
    QueryPlan & query_plan,
    SubqueriesForSets subqueries_for_sets,
    const SizeLimits & limits,
    const Context & context);

}
