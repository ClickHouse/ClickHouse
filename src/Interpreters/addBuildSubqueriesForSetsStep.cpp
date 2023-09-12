#include <Columns/ColumnSet.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/addBuildSubqueriesForSetsStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

void addBuildSubqueriesForSetsStep(
    QueryPlan & query_plan,
    ContextPtr context,
    PreparedSets & prepared_sets,
    const std::vector<ActionsDAGPtr> & result_actions_to_execute)
{
    PreparedSets::Subqueries subqueries;
    for (const auto & actions_to_execute : result_actions_to_execute)
    {
        if (!actions_to_execute)
            continue;

        for (const auto & node : actions_to_execute->getNodes())
        {
            const auto & set_column = node.column;

            for (const auto & future_set_from_subquery : prepared_sets.getSubqueries())
            {
                if (const auto * set_column_ptr = dynamic_cast<const ColumnSet *>(set_column.get()))
                {
                    const auto * set_from_subquery = dynamic_cast<const FutureSetFromSubquery *>(set_column_ptr->getData().get());
                    if (set_from_subquery->getSetAndKey()->key == future_set_from_subquery->getSetAndKey()->key)
                    {
                        subqueries.emplace_back(future_set_from_subquery);
                    }
                }
            }
        }
    }

    if (!subqueries.empty())
    {
        auto step = std::make_unique<DelayedCreatingSetsStep>(query_plan.getCurrentDataStream(), std::move(subqueries), context);

        query_plan.addStep(std::move(step));
    }
}

}
