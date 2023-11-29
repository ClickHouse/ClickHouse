#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

class LazilyReadStep : public ITransformingStep
{
public:
    LazilyReadStep(
        const DataStream & input_stream_,
        const LazilyReadInfoPtr & lazily_read_info_);

    String getName() const override { return "LazilyRead"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputStream() override;

    LazilyReadInfoPtr lazily_read_info;
};

}
