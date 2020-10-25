#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ExpressionTransform;
class InflatingExpressionTransform;

/// Calculates specified expression. See ExpressionTransform.
class ExpressionStep : public ITransformingStep
{
public:
    using Transform = ExpressionTransform;

    explicit ExpressionStep(const DataStream & input_stream_, ExpressionActionsPtr expression_);
    String getName() const override { return "Expression"; }

    void transformPipeline(QueryPipeline & pipeline) override;

    void describeActions(FormatSettings & settings) const override;

private:
    ExpressionActionsPtr expression;
};

/// TODO: add separate step for join.
class InflatingExpressionStep : public ITransformingStep
{
public:
    using Transform = InflatingExpressionTransform;

    explicit InflatingExpressionStep(const DataStream & input_stream_, ExpressionActionsPtr expression_);
    String getName() const override { return "InflatingExpression"; }

    void transformPipeline(QueryPipeline & pipeline) override;

    void describeActions(FormatSettings & settings) const override;

private:
    ExpressionActionsPtr expression;
};

}
