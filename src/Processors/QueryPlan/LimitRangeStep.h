#pragma once

#include <optional>

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

/** Executes LIMIT [n] AFTER expr [UNTIL expr]. See LimitRangeTransform. */
class LimitRangeStep : public ITransformingStep
{
public:
    LimitRangeStep(
        const SharedHeader & input_header_,
        std::optional<std::pair<ActionsDAG, String>> start_condition_,
        std::optional<std::pair<ActionsDAG, String>> end_condition_,
        std::optional<UInt64> limit_);

    String getName() const override { return "LimitRange"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    bool hasCorrelatedExpressions() const override
    {
        return (start_condition && start_condition->first.hasCorrelatedColumns())
            || (end_condition && end_condition->first.hasCorrelatedColumns());
    }

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    std::optional<std::pair<ActionsDAG, String>> start_condition;
    std::optional<std::pair<ActionsDAG, String>> end_condition;
    std::optional<UInt64> limit;
};

}
