#pragma once

#include <vector>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/PreparedSets.h>
#include "Interpreters/Context.h"
#include "Processors/QueryPlan/IQueryPlanStep.h"

namespace DB
{

/// Materializes CTE. See MaterializingCTETransform.
class MaterializingCTEStep : public ITransformingStep, public WithContext
{
public:
    MaterializingCTEStep(
        const DataStream & input_stream_,
        StoragePtr external_table_,
        String cte_table_name_,
        SizeLimits network_transfer_limits_,
        ContextPtr context_);

    String getName() const override { return "MaterializingCTE"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    void updateOutputStream() override;
    String cte_table_name;
    StoragePtr external_table;
    SizeLimits network_transfer_limits;
};

class MaterializingCTEsStep : public IQueryPlanStep, public WithContext
{
public:
    explicit MaterializingCTEsStep(ContextPtr context_, std::vector<QueryPlanPtr> && filling_cte_plans_, DataStream input_stream_);
    String getName() const override { return "DelayedMaterializingCTEs"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

private:
    std::vector<std::unique_ptr<QueryPlan>> materializing_cte_plans;
};

// class DelayedMaterializingCTEsStep final : public IQueryPlanStep
// {
// public:
//     DelayedMaterializingCTEsStep(DataStream input_stream, std::vector<QueryPlanPtr> materializing_cte_plans, ContextPtr context_);

//     String getName() const override { return "DelayedMaterializingCTEs"; }

//     QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings &) override;

//     ContextPtr getContext() const { return context; }

//     std::vector<QueryPlanPtr> detachPlans() { return std::move(materializing_cte_plans); }

// private:
//     std::vector<QueryPlanPtr> materializing_cte_plans;
//     ContextPtr context;
// };

}
