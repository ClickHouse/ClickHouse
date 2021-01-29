#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

class ExpressionTransform;
class JoiningTransform;

/// Calculates specified expression. See ExpressionTransform.
class ExpressionStep : public ITransformingStep
{
public:
    using Transform = ExpressionTransform;

    explicit ExpressionStep(const DataStream & input_stream_, ExpressionActionsPtr expression_);
    String getName() const override { return "Expression"; }

    void transformPipeline(QueryPipeline & pipeline) override;

    void updateInputStream(DataStream input_stream, bool keep_header);

    void describeActions(FormatSettings & settings) const override;

    const ExpressionActionsPtr & getExpression() const { return expression; }

private:
    ExpressionActionsPtr expression;
};

/// TODO: add separate step for join.
class JoinStep : public ITransformingStep
{
public:
    using Transform = JoiningTransform;

    explicit JoinStep(const DataStream & input_stream_, JoinPtr join_);
    String getName() const override { return "Join"; }

    void transformPipeline(QueryPipeline & pipeline) override;

private:
    JoinPtr join;
};

}
