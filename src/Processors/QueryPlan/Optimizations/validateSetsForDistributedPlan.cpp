#include <Interpreters/PreparedSets.h>
#include <Parsers/IAST.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

namespace QueryPlanOptimizations
{

namespace
{

String describeSet(const FutureSetFromSubquery & future_set)
{
    if (auto ast = future_set.getSourceAST())
        return ast->formatForErrorMessage();
    const auto hash = future_set.getHash();
    return fmt::format("with hash {}_{}", hash.low64, hash.high64);
}

}

void validateSetsForDistributedPlan(QueryPlan::Node & root)
{
    std::vector<QueryPlan::Node *> stack;
    stack.push_back(&root);
    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();
        if (!node || !node->step)
            continue;

        if (const auto * delayed = typeid_cast<const DelayedCreatingSetsStep *>(node->step.get()))
        {
            for (const auto & future_set : delayed->getSets())
            {
                if (future_set && future_set->hasExternalTable())
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                        "make_distributed_plan does not support sets backed by an external table "
                        "(`GLOBAL IN` / `GLOBAL JOIN`): IN-subquery {}", describeSet(*future_set));
            }
        }

        for (auto * child : node->children)
            stack.push_back(child);

        for (auto * child_plan : node->step->getChildPlans())
            if (child_plan && child_plan->getRootNode())
                stack.push_back(child_plan->getRootNode());
    }
}

}

}
