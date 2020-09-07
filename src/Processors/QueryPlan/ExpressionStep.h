#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ExpressionStep : public ITransformingStep
{
public:
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
    explicit InflatingExpressionStep(const DataStream & input_stream_, ExpressionActionsPtr expression_, bool default_totals_ = false);
    String getName() const override { return "Expression"; }

    void transformPipeline(QueryPipeline & pipeline) override;

private:
    ExpressionActionsPtr expression;
    bool default_totals; /// See ExpressionTransform
};

}
