#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/Transforms/CreatingSetsOnTheFlyTransform.h>

#include <Interpreters/Set.h>


namespace DB
{

using SetPtr = std::shared_ptr<Set>;

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

    SetPtr getSet() const { return set; }

private:
    void updateOutputStream() override;

    Names column_names;
    SetPtr set;
};


class FilterBySetOnTheFlyStep : public ITransformingStep
{
public:
    FilterBySetOnTheFlyStep(
        const DataStream & input_stream_,
        const Names & column_names_,
        SetPtr set_);

    String getName() const override { return "FilterBySetOnTheFly"; }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputStream() override;

    Names column_names;
    SetPtr set;
};

}
