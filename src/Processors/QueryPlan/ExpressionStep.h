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
    /// But changes can be pretty sophisticated, so we can pass perserve_sort_hint_ to specify columns that keep order.
    /// Because columns can be renamed, it accepts name mapping.
    explicit ExpressionStep(const DataStream & input_stream_, const ActionsDAGPtr & actions_dag_,
                            const NameToNameMap & perserve_sort_hint_ = {});
    String getName() const override { return "Expression"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    void describeActions(FormatSettings & settings) const override;

    const ActionsDAGPtr & getExpression() const { return actions_dag; }

    void describeActions(JSONBuilder::JSONMap & map) const override;

    void preserveSortingHint();

private:
    void updateOutputStream() override;

    ActionsDAGPtr actions_dag;

    NameToNameMap perserve_sort_hint;
};

}
