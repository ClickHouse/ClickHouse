#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>

namespace DB
{

void addConvertingActions(QueryPlan & plan, const Block & header, bool has_missing_objects)
{
    if (blocksHaveEqualStructure(plan.getCurrentHeader(), header))
        return;

    auto mode = has_missing_objects ? ActionsDAG::MatchColumnsMode::Position : ActionsDAG::MatchColumnsMode::Name;

    auto get_converting_dag = [mode](const Block & block_, const Block & header_)
    {
        /// Convert header structure to expected.
        /// Also we ignore constants from result and replace it with constants from header.
        /// It is needed for functions like `now64()` or `randConstant()` because their values may be different.
        return ActionsDAG::makeConvertingActions(
            block_.getColumnsWithTypeAndName(),
            header_.getColumnsWithTypeAndName(),
            mode,
            true);
    };

    auto convert_actions_dag = get_converting_dag(plan.getCurrentHeader(), header);
    auto converting = std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(convert_actions_dag));
    plan.addStep(std::move(converting));
}

}
