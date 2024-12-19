#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Interpreters/ActionsDAG.h>

namespace DB
{

class ExpressionTransform;
class JoiningTransform;

/// Calculates specified expression. See ExpressionTransform.
class ExpressionStep : public ITransformingStep
{
public:

    explicit ExpressionStep(const Header & input_header_, ActionsDAG actions_dag_, bool enable_adaptive_short_circuit_ = false);
    String getName() const override { return "Expression"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(FormatSettings & settings) const override;

    ActionsDAG & getExpression() { return actions_dag; }
    const ActionsDAG & getExpression() const { return actions_dag; }

    void describeActions(JSONBuilder::JSONMap & map) const override;

    void serialize(Serialization & ctx) const override;
    static std::unique_ptr<IQueryPlanStep> deserialize(Deserialization & ctx);
    bool enableAdaptiveShortCircuit() const { return enable_adaptive_short_circuit; }

private:
    void updateOutputHeader() override;
    ActionsDAG actions_dag;
    bool enable_adaptive_short_circuit;
};

}
