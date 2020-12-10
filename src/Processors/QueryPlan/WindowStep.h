#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

class WindowTransform;

class WindowStep : public ITransformingStep
{
public:
    using Transform = WindowTransform;

    explicit WindowStep(const DataStream & input_stream_, ActionsDAGPtr actions_dag_);
    String getName() const override { return "Expression"; }

    void transformPipeline(QueryPipeline & pipeline) override;

    void updateInputStream(DataStream input_stream, bool keep_header);

    void describeActions(FormatSettings & settings) const override;

    const ActionsDAGPtr & getExpression() const { return actions_dag; }

private:
    ActionsDAGPtr actions_dag;
};

}
