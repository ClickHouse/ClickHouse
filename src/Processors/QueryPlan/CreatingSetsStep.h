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
        const Header & input_header_,
        SetAndKeyPtr set_and_key_,
        StoragePtr external_table_,
        SizeLimits network_transfer_limits_,
        ContextPtr context_);

    String getName() const override { return "CreatingSet"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override;

    SetAndKeyPtr set_and_key;
    StoragePtr external_table;
    SizeLimits network_transfer_limits;
    ContextPtr context;
};

class CreatingSetsStep : public IQueryPlanStep
{
public:
    explicit CreatingSetsStep(Headers input_headers_);

    String getName() const override { return "CreatingSets"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override { output_header = getInputHeaders().front(); }
};

/// This is a temporary step which is converted to CreatingSetStep after plan optimization.
/// Can't be used by itself.
class DelayedCreatingSetsStep final : public IQueryPlanStep
{
public:
    DelayedCreatingSetsStep(Header input_header, PreparedSets::Subqueries subqueries_, ContextPtr context_);

    String getName() const override { return "DelayedCreatingSets"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings &) override;

    static std::vector<std::unique_ptr<QueryPlan>> makePlansForSets(DelayedCreatingSetsStep && step);

    ContextPtr getContext() const { return context; }
    PreparedSets::Subqueries detachSets() { return std::move(subqueries); }

private:
    void updateOutputHeader() override { output_header = getInputHeaders().front(); }

    PreparedSets::Subqueries subqueries;
    ContextPtr context;
};

void addCreatingSetsStep(QueryPlan & query_plan, PreparedSets::Subqueries subqueries, ContextPtr context);

void addCreatingSetsStep(QueryPlan & query_plan, PreparedSetsPtr prepared_sets, ContextPtr context);

QueryPipelineBuilderPtr addCreatingSetsTransform(QueryPipelineBuilderPtr pipeline, PreparedSets::Subqueries subqueries, ContextPtr context);

}
