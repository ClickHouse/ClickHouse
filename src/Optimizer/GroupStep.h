#pragma once

#include <Optimizer/Group.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

class Group;

class GroupStep final : public IQueryPlanStep
{
public:
    explicit GroupStep(DataStream output_stream_, Group & group_);

    String getName() const override;

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    Group & getGroup();

private:
    Group & group;
};

}
