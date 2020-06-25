#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ExpressionTransform;
class InflatingExpressionTransform;

class ExpressionStep : public ITransformingStep
{
public:
    using Transform = ExpressionTransform;

    explicit ExpressionStep(const DataStream & input_stream_, ExpressionActionsPtr expression_, bool default_totals_ = false);
    String getName() const override { return "Expression"; }

    void transformPipeline(QueryPipeline & pipeline) override;

private:
    ExpressionActionsPtr expression;
    bool default_totals; /// See ExpressionTransform
};

/// TODO: add separate step for join.
class InflatingExpressionStep : public ITransformingStep
{
public:
    using Transform = InflatingExpressionTransform;

    explicit InflatingExpressionStep(const DataStream & input_stream_, ExpressionActionsPtr expression_, bool default_totals_ = false);
    String getName() const override { return "Expression"; }

    void transformPipeline(QueryPipeline & pipeline) override;

private:
    ExpressionActionsPtr expression;
    bool default_totals; /// See ExpressionTransform
};

}
