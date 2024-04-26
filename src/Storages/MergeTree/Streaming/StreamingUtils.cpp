#include <algorithm>

#include <Processors/QueryPlan/ExpressionStep.h>

#include <Storages/MergeTree/Streaming/StreamingUtils.h>
#include <Storages/MergeTree/QueueModeColumns.h>

namespace DB
{

Names extendColumnsWithStreamingAux(const Names & columns_to_read)
{
    Names ext = columns_to_read;
    ext.push_back(QueuePartitionIdColumn::name);
    ext.push_back(QueueBlockNumberColumn::name);
    ext.push_back(QueueBlockOffsetColumn::name);

    std::sort(ext.begin(), ext.end());
    ext.erase(std::unique(ext.begin(), ext.end()), ext.end());

    return ext;
}

void addDropAuxColumnsStep(QueryPlan & query_plan, const Block & desired_header)
{
    if (blocksHaveEqualStructure(query_plan.getCurrentDataStream().header, desired_header))
      return;

    auto convert_actions_dag = ActionsDAG::makeConvertingActions(
        query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName(),
        desired_header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name);

    auto expression_step = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), convert_actions_dag);
    expression_step->setStepDescription("Drop auxiliary columns");
    query_plan.addStep(std::move(expression_step));
}

}
