#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

class ExpressionTransform;
class JoiningTransform;

/// Calculates specified expression. See ExpressionTransform.
class ExpressionStep : public ITransformingStep
{
public:

    explicit ExpressionStep(const DataStream & input_stream_, ActionsDAGPtr actions_dag_);
    String getName() const override { return "Expression"; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings) override;

    void updateInputStream(DataStream input_stream, bool keep_header);

    void describeActions(FormatSettings & settings) const override;

    const ActionsDAGPtr & getExpression() const { return actions_dag; }

    void describeActions(JSONBuilder::JSONMap & map) const override;

private:
    ActionsDAGPtr actions_dag;
};

}
