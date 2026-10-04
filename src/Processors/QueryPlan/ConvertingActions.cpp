#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>

namespace DB
{

void addConvertingActions(QueryPlan & plan, const Block & header, const ContextPtr & context);
void addConvertingActions(QueryPlan & plan, const Block & header, const ContextPtr & context)
{
    if (blocksHaveEqualStructure(*plan.getCurrentHeader(), header))
        return;

    const auto & current_header = *plan.getCurrentHeader();
    /// Same-named columns (SELECT *, x + 1 AS x) cannot be told apart by name; the source produces them in header order.
    auto mode = header.getIndexByName().size() < header.columns() && current_header.columns() == header.columns()
        ? ActionsDAG::MatchColumnsMode::Position
        : ActionsDAG::MatchColumnsMode::Name;

    auto get_converting_dag = [mode, context](const Block & block_, const Block & header_)
    {
        /// Convert header structure to expected.
        /// Also we ignore constants from result and replace it with constants from header.
        /// It is needed for functions like `now64()` or `randConstant()` because their values may be different.
        return ActionsDAG::makeConvertingActions(
            block_.getColumnsWithTypeAndName(),
            header_.getColumnsWithTypeAndName(),
            mode,
            context,
            true);
    };

    auto convert_actions_dag = get_converting_dag(*plan.getCurrentHeader(), header);
    auto converting = std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(convert_actions_dag));
    plan.addStep(std::move(converting));
}

}
