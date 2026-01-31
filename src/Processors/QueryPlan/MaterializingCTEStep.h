#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

struct TemporaryTableHolder;
using TemporaryTableHolderPtr = std::shared_ptr<TemporaryTableHolder>;

class MaterializingCTEStep : public ITransformingStep
{
public:
    explicit MaterializingCTEStep(
        SharedHeader input_header_,
        TemporaryTableHolderPtr temporary_table_holder_
    );

    String getName() const override { return "MaterializingCTE"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:

    void updateOutputHeader() override {} // Output header should stay empty.

    TemporaryTableHolderPtr temporary_table_holder;
};


class MaterializingCTEsStep : public IQueryPlanStep
{
public:
    explicit MaterializingCTEsStep(SharedHeaders input_headers_);

    String getName() const override { return "MaterializingCTEs"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

private:
    void updateOutputHeader() override { output_header = getInputHeaders().front(); }
};


}
