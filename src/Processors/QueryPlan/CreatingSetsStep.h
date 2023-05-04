#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/PreparedSets.h>

namespace DB
{

/// Creates sets for subqueries and JOIN. See CreatingSetsTransform.
class CreatingSetStep : public ITransformingStep
{
public:
    CreatingSetStep(
            const DataStream & input_stream_,
            String description_,
            SubqueryForSet subquery_for_set_,
            SizeLimits network_transfer_limits_,
            ContextPtr context_);

    String getName() const override { return "CreatingSet"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputStream() override;

    String description;
    SubqueryForSet subquery_for_set;
    SizeLimits network_transfer_limits;
    ContextPtr context;
};

class CreatingSetsStep : public IQueryPlanStep
{
public:
    explicit CreatingSetsStep(DataStreams input_streams_);

    String getName() const override { return "CreatingSets"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;
};

class DelayedCreatingSetsStep final : public IQueryPlanStep
{
public:
    DelayedCreatingSetsStep(DataStream input_stream, PreparedSets::SubqueriesForSets subqueries_for_sets_, ContextPtr context_);

    String getName() const override { return "DelayedCreatingSets"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings &) override;

    static std::vector<std::unique_ptr<QueryPlan>> makePlansForSets(DelayedCreatingSetsStep && step);

private:
    PreparedSets::SubqueriesForSets subqueries_for_sets;
    ContextPtr context;
};

void addCreatingSetsStep(QueryPlan & query_plan, PreparedSets::SubqueriesForSets subqueries_for_sets, ContextPtr context);

void addCreatingSetsStep(QueryPlan & query_plan, PreparedSetsPtr prepared_sets, ContextPtr context);

}
