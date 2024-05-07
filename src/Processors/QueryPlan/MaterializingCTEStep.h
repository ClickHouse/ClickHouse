#pragma once

#include <vector>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/SizeLimits.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/PreparedSets.h>
#include "Interpreters/Context.h"
#include "Interpreters/MaterializedTableFromCTE.h"
#include "Processors/QueryPlan/IQueryPlanStep.h"

namespace DB
{

/// Materializes CTE. See MaterializingCTETransform.
class MaterializingCTEStep : public ITransformingStep, public WithContext
{
public:
    MaterializingCTEStep(
        const DataStream & input_stream_,
        FutureTableFromCTEPtr future_table_,
        SizeLimits network_transfer_limits_,
        ContextPtr context_);

    String getName() const override { return "MaterializingCTE"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    void updateOutputStream() override;
    FutureTableFromCTEPtr future_table;
    SizeLimits network_transfer_limits;
};

class MaterializingCTEsStep : public IQueryPlanStep, public WithContext
{
public:
    explicit MaterializingCTEsStep(ContextPtr context_, FutureTablesFromCTE && future_tables_, DataStream input_stream_);
    String getName() const override { return "DelayedMaterializingCTEs"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

private:
    FutureTablesFromCTE future_tables;
    std::vector<QueryPlanPtr> materializing_future_table_plans;
};

}
