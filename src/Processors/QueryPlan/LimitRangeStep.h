#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

/** Executes LIMIT n AFTER expr [UNTIL expr]. See LimitRangeTransform. */
class LimitRangeStep : public ITransformingStep
{
public:
    LimitRangeStep(
        const SharedHeader & input_header_,
        std::optional<std::pair<ActionsDAG, String>> start_condition_,
        std::optional<std::pair<ActionsDAG, String>> end_condition_,
        size_t limit_);

    String getName() const override { return "LimitRange"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    std::optional<std::pair<ActionsDAG, String>> start_condition;
    std::optional<std::pair<ActionsDAG, String>> end_condition;
    size_t limit;
};

}
