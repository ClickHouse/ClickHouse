#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class ActionsDAG;
using ActionsDAGPtr = std::shared_ptr<ActionsDAG>;

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

class ExpressionTransform;
class JoiningTransform;

/// Calculates specified expression. See ExpressionTransform.
class ExpressionStep : public ITransformingStep
{
public:
    using Transform = ExpressionTransform;

    explicit ExpressionStep(const DataStream & input_stream_, ActionsDAGPtr actions_dag_);
    String getName() const override { return "Expression"; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & settings) override;

    void updateInputStream(DataStream input_stream, bool keep_header);

    void describeActions(FormatSettings & settings) const override;

    const ActionsDAGPtr & getExpression() const { return actions_dag; }

private:
    ActionsDAGPtr actions_dag;
};

/// TODO: add separate step for join.
class JoinStep : public ITransformingStep
{
public:
    using Transform = JoiningTransform;

    explicit JoinStep(const DataStream & input_stream_, JoinPtr join_, bool has_non_joined_rows_, size_t max_block_size_);
    String getName() const override { return "Join"; }

    void transformPipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    const JoinPtr & getJoin() const { return join; }

private:
    JoinPtr join;
    bool has_non_joined_rows;
    size_t max_block_size;
};

}
