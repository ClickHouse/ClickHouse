#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>

namespace DB
{

class FillingStep : public ITransformingStep
{
public:
    FillingStep(const DataStream & input_stream_, SortDescription sort_description_);

    String getName() const override { return "Filling"; }

    void transformPipeline(QueryPipeline & pipeline) override;

    Strings describeActions() const override;

private:
    SortDescription sort_description;
};

}
