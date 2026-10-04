#include <Analyzer/moveNonAggregateHavingConjunctsToWhere.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

namespace DB
{

namespace
{

/** Classify a HAVING conjunct subtree for the `HAVING` -> `WHERE` rewrite.
  *
  * `AbortRewrite` outranks `KeepInHaving` outranks `Move`:
  * - any window function or stateful function -> `AbortRewrite` (matches legacy `return false`);
  * - else any aggregate function -> `KeepInHaving`;
  * - else any `grouping` function -> `KeepInHaving` (stricter than legacy; required because
  *   `validateAggregates` rejects `grouping` in `WHERE`);
  * - else any `arrayJoin` function -> `KeepInHaving` (before aggregation it would multiply the rows);
  * - else any non-deterministic function -> `KeepInHaving` (stricter than legacy, which moved them);
  * - else -> `Move`.
  */
enum class HavingConjunctMoveAction
{
    Move,
    KeepInHaving,
    AbortRewrite,
};

HavingConjunctMoveAction classifyHavingConjunctForMove(const QueryTreeNodePtr & node)
{
    HavingConjunctMoveAction verdict = HavingConjunctMoveAction::Move;

    QueryTreeNodes nodes_to_visit = {node};
    while (!nodes_to_visit.empty())
    {
        auto current = nodes_to_visit.back();
        nodes_to_visit.pop_back();

        auto current_type = current->getNodeType();
        if (current_type == QueryTreeNodeType::QUERY || current_type == QueryTreeNodeType::UNION)
            continue;

        if (auto * function_node = current->as<FunctionNode>())
        {
            if (function_node->isWindowFunction())
                return HavingConjunctMoveAction::AbortRewrite;

            if (function_node->isOrdinaryFunction())
            {
                if (auto function_base = function_node->getFunction())
                {
                    if (function_base->isStateful())
                        return HavingConjunctMoveAction::AbortRewrite;

                    if (!function_base->isDeterministicInScopeOfQuery() && verdict == HavingConjunctMoveAction::Move)
                        verdict = HavingConjunctMoveAction::KeepInHaving;
                }
            }

            if (function_node->isAggregateFunction() && verdict == HavingConjunctMoveAction::Move)
                verdict = HavingConjunctMoveAction::KeepInHaving;

            /// `GroupingFunctionsResolvePass` replaces `grouping` with its specializations, like `__groupingOrdinary`.
            const auto & function_name = function_node->getFunctionName();
            bool is_grouping_function = function_name == "grouping" || function_name.starts_with("__grouping");
            if ((is_grouping_function || function_name == "arrayJoin") && verdict == HavingConjunctMoveAction::Move)
                verdict = HavingConjunctMoveAction::KeepInHaving;
        }

        for (const auto & child : current->getChildren())
            if (child)
                nodes_to_visit.push_back(child);
    }

    return verdict;
}

}

void moveNonAggregateHavingConjunctsToWhere(QueryNode & query_node, const ContextPtr & context)
{
    if (!query_node.hasHaving())
        return;

    if (query_node.isGroupByWithCube()
        || query_node.isGroupByWithRollup()
        || query_node.isGroupByWithTotals()
        || query_node.isGroupByWithGroupingSets())
        return;

    auto & having_node = query_node.getHaving();

    /// The parser builds left-associative binary `and` trees, so `(a AND b) AND c`
    /// arrives as `and(and(a, b), c)`. Flatten the whole chain into atomic conjuncts,
    /// mirroring the legacy `splitConjunctionsAst` helper.
    /// Without this, a nested `and` containing an aggregate is classified as a single
    /// `KeepInHaving` conjunct and its non-aggregate siblings stay trapped in `HAVING`.
    QueryTreeNodes conjuncts;
    {
        QueryTreeNodes worklist{having_node};
        while (!worklist.empty())
        {
            auto current = std::move(worklist.back());
            worklist.pop_back();

            auto * current_function = current->as<FunctionNode>();
            if (current_function && current_function->getFunctionName() == "and")
            {
                const auto & args = current_function->getArguments().getNodes();
                /// Reverse-iterate into the LIFO worklist to preserve left-to-right order.
                for (auto it = args.rbegin(); it != args.rend(); ++it)
                    worklist.push_back(*it);
            }
            else
            {
                conjuncts.push_back(std::move(current));
            }
        }
    }

    std::vector<HavingConjunctMoveAction> classifications;
    classifications.reserve(conjuncts.size());
    for (const auto & conjunct : conjuncts)
    {
        auto action = classifyHavingConjunctForMove(conjunct);
        if (action == HavingConjunctMoveAction::AbortRewrite)
            return;
        classifications.push_back(action);
    }

    QueryTreeNodes keep_in_having;
    QueryTreeNodes move_to_where;
    keep_in_having.reserve(conjuncts.size());
    move_to_where.reserve(conjuncts.size());

    for (size_t i = 0; i < conjuncts.size(); ++i)
    {
        if (classifications[i] == HavingConjunctMoveAction::KeepInHaving)
            keep_in_having.push_back(std::move(conjuncts[i]));
        else
            move_to_where.push_back(std::move(conjuncts[i]));
    }

    if (move_to_where.empty())
        return;

    auto build_and = [&context](QueryTreeNodes && args) -> QueryTreeNodePtr
    {
        if (args.size() == 1)
            return std::move(args.front());
        auto and_function = std::make_shared<FunctionNode>("and");
        and_function->markAsOperator();
        and_function->getArguments().getNodes() = std::move(args);
        and_function->resolveAsFunction(FunctionFactory::instance().get("and", context));
        return and_function;
    };

    if (keep_in_having.empty())
        having_node = nullptr;
    else
        having_node = build_and(std::move(keep_in_having));

    QueryTreeNodes new_where_args;
    new_where_args.reserve(1 + move_to_where.size());
    if (query_node.hasWhere())
        new_where_args.push_back(query_node.getWhere());
    for (auto & moved : move_to_where)
        new_where_args.push_back(std::move(moved));

    query_node.getWhere() = build_and(std::move(new_where_args));
}

}
