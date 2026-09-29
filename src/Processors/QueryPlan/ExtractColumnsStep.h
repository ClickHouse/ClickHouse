#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

class ExtractColumnsStep : public ITransformingStep
{
public:

    explicit ExtractColumnsStep(SharedHeader input_header_, const NamesAndTypesList & requested_columns_);
    String getName() const override { return "ExtractColumns"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    void updateOutputHeader() override;

    NamesAndTypesList requested_columns;
};

}
