#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Interpreters/JoinInfo.h>
#include <Processors/QueryPlan/JoinStep.h>

namespace DB
{

/** JoinStepLogical is a logical step for JOIN operation.
  * Doesn't contain any specific join algorithm or other execution details.
  * It's place holder for join operation with it's description that can be serialized.
  * Transformed to actual join step during plan optimization.
  */
class JoinStepLogical final : public IQueryPlanStep
{
public:
    JoinStepLogical(
        const Block & left_header_,
        const Block & right_header_,
        JoinInfo join_info_,
        JoinExpressionActions join_expression_actions_,
        Names required_output_columns_,
        ContextPtr context_);

    String getName() const override { return "JoinLogical"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &) override;

    void describePipeline(FormatSettings & settings) const override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    template <typename T>
    void setPreparedJoinStorage(T && storage) { prepared_join_storage = std::forward<T>(storage); }

    JoinPtr chooseJoinAlgorithm();

protected:
    void updateOutputHeader() override;

    JoinExpressionActions expression_actions;
    JoinInfo join_info;

    Names required_output_columns;
    ContextPtr query_context;

    std::variant<std::shared_ptr<StorageJoin>, std::shared_ptr<const IKeyValueEntity>> prepared_join_storage;
};

}
