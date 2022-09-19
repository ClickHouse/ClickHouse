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
    /// ExpressionStep is trying to preserve sorting if it is possible.
    /// Step can be sophisticated, so we can use explicit sort description in cases when we know result sorting.
    explicit ExpressionStep(const DataStream & input_stream_, const ActionsDAGPtr & actions_dag_,
                            const SortDescription & result_sort_desc_ = {});
    String getName() const override { return "Expression"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(FormatSettings & settings) const override;

    const ActionsDAGPtr & getExpression() const { return actions_dag; }

    void describeActions(JSONBuilder::JSONMap & map) const override;

private:
    void updateOutputStream() override;

    ActionsDAGPtr actions_dag;

    SortDescription result_sort_desc;
};

}
