#include <Analyzer/moveNonAggregateHavingConjunctsToWhere.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <TableFunctions/ITableFunction.h>

namespace DB
{

namespace
{

/// Whether the type is or contains a `DateTime` or `DateTime64` without an explicit time zone.
/// Such values are converted from and to their calendar representation in the time zone of the executing server.
bool hasImplicitTimeZone(const DataTypePtr & type)
{
    if (const auto * date_time = typeid_cast<const DataTypeDateTime *>(type.get()))
        return !date_time->hasExplicitTimeZone();
    if (const auto * date_time64 = typeid_cast<const DataTypeDateTime64 *>(type.get()))
        return !date_time64->hasExplicitTimeZone();
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(type.get()))
        return hasImplicitTimeZone(nullable->getNestedType());
    if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        return hasImplicitTimeZone(low_cardinality->getDictionaryType());
    if (const auto * array = typeid_cast<const DataTypeArray *>(type.get()))
        return hasImplicitTimeZone(array->getNestedType());
    if (const auto * map = typeid_cast<const DataTypeMap *>(type.get()))
        return hasImplicitTimeZone(map->getKeyType()) || hasImplicitTimeZone(map->getValueType());
    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(type.get()))
        return std::ranges::any_of(tuple->getElements(), hasImplicitTimeZone);
    if (const auto * variant = typeid_cast<const DataTypeVariant *>(type.get()))
        return std::ranges::any_of(variant->getVariants(), hasImplicitTimeZone);
    return false;
}

/** Classify a HAVING conjunct subtree for the `HAVING` -> `WHERE` rewrite.
  *
  * `AbortRewrite` outranks `KeepInHaving` outranks `Move`:
  * - any window function or stateful function -> `AbortRewrite` (matches legacy `return false`);
  * - any function with observable side effects (like `sleepEachRow`) -> `AbortRewrite` (stricter than legacy,
  *   which moved them): it must be evaluated once per group, and a sibling conjunct moved to `WHERE` would remove groups
  *   before it is evaluated;
  * - else any aggregate function -> `KeepInHaving`;
  * - else any `grouping` function -> `KeepInHaving` (stricter than legacy; required because
  *   `validateAggregates` rejects `grouping` in `WHERE`);
  * - else any `arrayJoin` function -> `KeepInHaving` (before aggregation it would multiply the rows);
  * - else, with `keep_index_hint_conjuncts`, any `indexHint` function -> `KeepInHaving` (before aggregation it would
  *   prune granules, leaving partial groups);
  * - else any non-deterministic function -> `KeepInHaving` (stricter than legacy, which moved them);
  * - else -> `Move`.
  *
  * Inside subqueries only the non-deterministic and stateful functions and the functions with observable side effects
  * are taken into account: aggregate, window and `grouping` functions there belong to the scope of the subquery.
  * A non-deterministic subquery, or one with a stateful function, keeps the conjunct in `HAVING`, because in `WHERE`
  * a correlated subquery would be evaluated per source row instead of per group. The same holds for a subquery reading
  * a non-deterministic table function, like `generateRandom`. A subquery with a function with observable side effects
  * (like `timeSeriesStoreTags`) cancels the rewrite, as at the top level.
  *
  * With `keep_server_dependent_conjuncts`, a conjunct also stays in `HAVING` if its result may differ between
  * servers: if it contains a function that is not deterministic at all (like `shardNum`, `hostName` or `dictGet`),
  * or reads a table or a table function, in a subquery or as the right-hand side of `IN`. A subquery without `FROM`,
  * which reads `system.one`, is still moved.
  * Such a conjunct also stays in `HAVING` if it has a value of type `DateTime` or `DateTime64` without an explicit time zone
  * (like `toHour(toDateTime(k))`) or uses `toUnixTimestamp` to parse a string: the result depends on the time zone of
  * the server, which may differ between the shards and the initiator.
  * The right-hand side of the `GLOBAL IN` family is an exception: the initiator computes the set once and sends the same
  * temporary table to every server, so it is not server-dependent.
  */
enum class HavingConjunctMoveAction
{
    Move,
    KeepInHaving,
    AbortRewrite,
};

HavingConjunctMoveAction classifyHavingConjunctForMove(
    const QueryTreeNodePtr & node, bool keep_server_dependent_conjuncts, bool keep_index_hint_conjuncts)
{
    HavingConjunctMoveAction verdict = HavingConjunctMoveAction::Move;

    auto keep_in_having = [&verdict]()
    {
        if (verdict == HavingConjunctMoveAction::Move)
            verdict = HavingConjunctMoveAction::KeepInHaving;
    };

    struct NodeToVisit
    {
        QueryTreeNodePtr node;
        bool inside_subquery = false;
        /// False for the right-hand side of the `GLOBAL IN` family, which is the same on every server.
        bool check_server_dependent = false;
    };

    std::vector<NodeToVisit> nodes_to_visit = {{node, false, keep_server_dependent_conjuncts}};
    while (!nodes_to_visit.empty())
    {
        auto [current, inside_subquery, check_server_dependent] = std::move(nodes_to_visit.back());
        nodes_to_visit.pop_back();

        auto current_type = current->getNodeType();
        if (current_type == QueryTreeNodeType::QUERY || current_type == QueryTreeNodeType::UNION)
            inside_subquery = true;

        if (check_server_dependent
            && (current_type == QueryTreeNodeType::CONSTANT || current_type == QueryTreeNodeType::COLUMN
                || current_type == QueryTreeNodeType::FUNCTION)
            && hasImplicitTimeZone(current->getResultType()))
            keep_in_having();

        if (auto * constant_node = current->as<ConstantNode>())
        {
            /// A secondary server folds functions like `shardNum` into constants.
            if (check_server_dependent && !constant_node->isDeterministic())
                keep_in_having();

            /// A constant folded from a function, like `toUnixTimestamp('2024-01-05 00:00:00')`, may still depend on
            /// the time zone of the server, so check the expression it was folded from.
            if (check_server_dependent && constant_node->hasSourceExpression()
                && constant_node->getSourceExpression()->getNodeType() == QueryTreeNodeType::FUNCTION)
                nodes_to_visit.push_back({constant_node->getSourceExpression(), inside_subquery, check_server_dependent});
        }
        else if (auto * table_function_node = current->as<TableFunctionNode>())
        {
            const auto & table_function = table_function_node->getTableFunction();
            if (check_server_dependent || !table_function || !table_function->isDeterministicInScopeOfQuery())
                keep_in_having();
        }
        else if (auto * table_node = current->as<TableNode>())
        {
            const auto & storage_id = table_node->getStorageID();
            bool is_system_one = storage_id.database_name == "system" && storage_id.table_name == "one";
            if (check_server_dependent && !is_system_one)
                keep_in_having();
        }
        else if (auto * function_node = current->as<FunctionNode>())
        {
            /// `toUnixTimestamp` parses a string in the time zone of the server.
            if (check_server_dependent && function_node->isOrdinaryFunction())
            {
                const auto & arguments = function_node->getArguments().getNodes();
                auto function_base = function_node->getFunction();
                if (function_base && function_base->getName() == "toUnixTimestamp" && arguments.size() == 1
                    && isStringOrFixedString(removeLowCardinalityAndNullable(arguments[0]->getResultType())))
                    keep_in_having();
            }

            if (inside_subquery)
            {
                if (function_node->isOrdinaryFunction())
                {
                    auto function_base = function_node->getFunction();
                    if (function_base && function_base->hasObservableSideEffects())
                        return HavingConjunctMoveAction::AbortRewrite;
                    if (function_base
                        && (function_base->isStateful()
                            || !function_base->isDeterministicInScopeOfQuery()
                            || (check_server_dependent && !function_base->isDeterministic())))
                        keep_in_having();
                }
            }
            else
            {
                if (function_node->isWindowFunction())
                    return HavingConjunctMoveAction::AbortRewrite;

                if (function_node->isOrdinaryFunction())
                {
                    if (auto function_base = function_node->getFunction())
                    {
                        /// Functions like `sleepEachRow` must be evaluated once per group, not per source row,
                        /// and a sibling conjunct moved to `WHERE` must not change the number of groups they see.
                        if (function_base->isStateful() || function_base->hasObservableSideEffects())
                            return HavingConjunctMoveAction::AbortRewrite;

                        if (!function_base->isDeterministicInScopeOfQuery()
                            || (check_server_dependent && !function_base->isDeterministic()))
                            keep_in_having();
                    }
                }

                if (function_node->isAggregateFunction())
                    keep_in_having();

                /// `GroupingFunctionsResolvePass` replaces `grouping` with its specializations, like `__groupingOrdinary`.
                const auto & function_name = function_node->getFunctionName();
                bool is_grouping_function = function_name == "grouping" || function_name.starts_with("__grouping");
                /// `arrayJoin` may also be written as its alias `unnest`.
                const auto & canonical_name = getFunctionCanonicalNameIfAny(function_name);
                if (is_grouping_function || canonical_name == "arrayJoin"
                    || (keep_index_hint_conjuncts && canonical_name == "indexHint"))
                    keep_in_having();
            }
        }

        const auto * current_function = current->as<FunctionNode>();
        if (current_function && isNameOfGlobalInFunction(current_function->getFunctionName()))
        {
            const auto & arguments = current_function->getArguments().getNodes();
            for (size_t i = 0; i < arguments.size(); ++i)
                if (arguments[i])
                    nodes_to_visit.push_back({arguments[i], inside_subquery, check_server_dependent && i != 1});
            continue;
        }

        for (const auto & child : current->getChildren())
            if (child)
                nodes_to_visit.push_back({child, inside_subquery, check_server_dependent});
    }

    return verdict;
}

}

void moveNonAggregateHavingConjunctsToWhere(
    QueryNode & query_node, const ContextPtr & context, bool keep_server_dependent_conjuncts, bool keep_index_hint_conjuncts)
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
        auto action = classifyHavingConjunctForMove(conjunct, keep_server_dependent_conjuncts, keep_index_hint_conjuncts);
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
