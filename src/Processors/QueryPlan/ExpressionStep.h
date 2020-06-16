#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ExpressionStep : public ITransformingStep
{
public:
    explicit ExpressionStep(const DataStream & input_stream_, ExpressionActionsPtr expression_);
    String getName() const override { return "Expression"; }

    void transformPipeline(QueryPipeline & pipeline) override;

private:
    ExpressionActionsPtr expression;
};

}
