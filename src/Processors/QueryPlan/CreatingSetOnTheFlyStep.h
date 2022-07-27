#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/CreatingSetsOnTheFlyTransform.h>


namespace DB
{

class CreatingSetOnTheFlyStep : public ITransformingStep
{
public:
    explicit CreatingSetOnTheFlyStep(
        const DataStream & input_stream_,
        const Names & column_names_,
        const SizeLimits & size_limits = {});

    String getName() const override { return "CreatingSetsOnTheFly"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    SetWithStatePtr getSet() const { return set; }

private:
    void updateOutputStream() override;

    Names column_names;
    SetWithStatePtr set;
};


class FilterBySetOnTheFlyStep : public ITransformingStep
{
public:
    FilterBySetOnTheFlyStep(
        const DataStream & input_stream_,
        const Names & column_names_,
        SetWithStatePtr set_);

    String getName() const override { return "FilterBySetOnTheFly"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputStream() override;

    Names column_names;
    SetWithStatePtr set;
};

}
