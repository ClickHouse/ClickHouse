#include <Analyzer/moveNonAggregateHavingConjunctsToWhere.h>

#include <Analyzer/AggregationUtils.h>
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

#include <unordered_set>

namespace DB
{

namespace
{

/// Whether the type is or may contain a `DateTime` or `DateTime64` without an explicit time zone.
/// Such values are converted from and to their calendar representation in the time zone of the executing server.
/// `Dynamic` and `JSON` may hold such a value at runtime, so they are treated as having one.
bool hasImplicitTimeZone(const DataTypePtr & type)
{
    if (isDynamic(type) || isObject(type))
        return true;
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

/// The type of a function argument, or `nullptr` for a subquery or a table, like the right-hand side of `IN`.
DataTypePtr getArgumentType(const QueryTreeNodePtr & argument)
{
    if (!argument || isTableExpressionNodeType(argument->getNodeType()))
        return nullptr;
    return removeLowCardinalityAndNullable(argument->getResultType());
}

/// Whether the optional time zone argument at `index` is omitted or is an empty string. In both cases the function
/// uses the time zone of the executing server.
bool isTimeZoneArgumentUnbound(const QueryTreeNodes & arguments, size_t index)
{
    if (arguments.size() <= index)
        return true;
    const auto * constant_node = arguments[index] ? arguments[index]->as<ConstantNode>() : nullptr;
    if (!constant_node)
        return false;
    const auto & value = constant_node->getValue();
    return value.getType() == Field::Types::String && value.safeGet<String>().empty();
}

/// Whether the function converts between a Unix timestamp and its calendar representation in the time zone
/// of the executing server, without a `DateTime` or `DateTime64` value with an implicit time zone in between.
bool dependsOnServerTimeZone(const FunctionNode & function_node)
{
    const auto & arguments = function_node.getArguments().getNodes();
    if (arguments.empty() || !arguments[0])
        return false;

    auto function_base = function_node.getFunction();
    if (!function_base)
        return false;

    const auto & function_name = function_base->getName();
    auto first_argument_type = getArgumentType(arguments[0]);
    bool returns_date = isDateOrDate32(removeLowCardinalityAndNullable(function_node.getResultType()));

    /// `toUnixTimestamp` parses a string in the time zone of the server.
    if (function_name == "toUnixTimestamp" && arguments.size() == 1 && first_argument_type
        && isStringOrFixedString(first_argument_type))
        return true;

    /// The `parseDateTime` family parses a string in the time zone of the server when the optional third argument
    /// is omitted or empty, and attaches the name of that time zone to the result type, so the type looks explicitly bound.
    if (function_name.starts_with("parseDateTime") && !function_name.starts_with("parseDateTimeBestEffort")
        && isTimeZoneArgumentUnbound(arguments, 2))
        return true;

    /// A `Date` or `Date32` is converted to a point in time at its midnight in the time zone of the server,
    /// like in `toRelativeSecondNum(d)` or `toUnixTimestamp(d)`. Only the functions that work with whole days,
    /// or do not look at the value at all, are known to give the same result on every server.
    if (!returns_date
        && std::ranges::any_of(arguments, [](const QueryTreeNodePtr & argument)
            {
                auto argument_type = getArgumentType(argument);
                return argument_type && isDateOrDate32(argument_type);
            }))
    {
        static const std::unordered_set<std::string_view> time_zone_independent_functions
        {
            "equals", "notEquals", "less", "greater", "lessOrEquals", "greaterOrEquals",
            "in", "notIn", "nullIn", "notNullIn", "globalIn", "globalNotIn", "globalNullIn", "globalNotNullIn",
            "has", "hasAny", "hasAll", "isNull", "isNotNull", "tuple", "array", "minus", "toString", "_CAST", "CAST",
            "toYear", "toQuarter", "toMonth", "toDayOfMonth", "toDayOfWeek", "toDayOfYear", "toISOYear", "toISOWeek",
            "toWeek", "toYearWeek", "toYYYYMM", "toYYYYMMDD", "toRelativeYearNum", "toRelativeQuarterNum",
            "toRelativeMonthNum", "toRelativeWeekNum", "toRelativeDayNum",
        };
        return !time_zone_independent_functions.contains(function_name);
    }

    if (!first_argument_type || !isNumber(first_argument_type))
        return false;

    /// `toDate(k)`, `toDate32(k)` and `CAST(k AS Date)` take a number as a Unix timestamp.
    if (returns_date)
        return true;

    /// `fromUnixTimestamp(k, format)` formats a Unix timestamp; the optional third argument is the time zone.
    return (function_name == "fromUnixTimestamp" || function_name == "fromUnixTimestampInJodaSyntax"
            || function_name == "formatDateTime" || function_name == "formatDateTimeInJodaSyntax")
        && arguments.size() < 3;
}

/// Whether the subtree contains a function whose result or effect depends on the rows it is evaluated on:
/// a stateful function, like `rowNumberInAllBlocks` or `runningDifference`, a function that is not deterministic
/// in the scope of the query, like `blockSize`, or a function with observable side effects, like `sleepEachRow`.
bool hasRowSensitiveFunction(const QueryTreeNodePtr & node)
{
    QueryTreeNodes nodes_to_visit{node};
    while (!nodes_to_visit.empty())
    {
        auto current = std::move(nodes_to_visit.back());
        nodes_to_visit.pop_back();

        if (const auto * function_node = current->as<FunctionNode>(); function_node && function_node->isOrdinaryFunction())
        {
            auto function_base = function_node->getFunction();
            if (function_base
                && (function_base->isStateful() || !function_base->isDeterministicInScopeOfQuery()
                    || function_base->hasObservableSideEffects()))
                return true;
        }

        for (const auto & child : current->getChildren())
            if (child)
                nodes_to_visit.push_back(child);
    }
    return false;
}

/** Classify a HAVING conjunct subtree for the `HAVING` -> `WHERE` rewrite.
  *
  * `AbortRewrite` outranks `KeepInHaving` outranks `Move`:
  * - any window function or stateful function -> `AbortRewrite` (matches legacy `return false`);
  * - with `abort_on_side_effects`, any function with observable side effects (like `sleepEachRow`), or that is not
  *   deterministic in the scope of the query (like `blockSize`), -> `AbortRewrite` (stricter than legacy, which moved
  *   them): it must be evaluated once per group on the original blocks of groups, and a sibling conjunct moved to `WHERE`
  *   would remove groups before it is evaluated. The compatibility rewrite of the analyzer must move the conjuncts with
  *   the columns that are not grouped, so it does not check it;
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
  * a non-deterministic table function, like `generateRandom`. With `abort_on_side_effects`, a subquery with a function
  * with observable side effects (like `timeSeriesStoreTags`) cancels the rewrite, as at the top level.
  *
  * Subqueries and table functions are inspected only with `inspect_subqueries`. The compatibility rewrite of the analyzer
  * skips them, like the legacy rewrite, because it must move the conjuncts with the columns that are not grouped.
  *
  * With `keep_server_dependent_conjuncts`, a conjunct also stays in `HAVING` if its result may differ between
  * servers: if it contains a function that is not deterministic at all (like `shardNum`, `hostName` or `dictGet`),
  * or reads a table or a table function, in a subquery or as the right-hand side of `IN`. A subquery without `FROM`,
  * which reads `system.one`, is still moved.
  * Such a conjunct also stays in `HAVING` if it has a value of type `DateTime` or `DateTime64` without an explicit time zone
  * (like `toHour(toDateTime(k))`), uses `toUnixTimestamp` or the `parseDateTime` family without the time zone argument
  * (or with an empty one) to parse a string, converts a number as a Unix timestamp
  * to a `Date` or `Date32` (like `toDate(k)`) or to a string (like `fromUnixTimestamp(k, '%F')`), or converts a `Date`
  * or `Date32` to a point in time (like `toRelativeSecondNum(d)`): the result depends
  * on the time zone of the server, which may differ between the shards and the initiator.
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
    const QueryTreeNodePtr & node,
    bool keep_server_dependent_conjuncts,
    bool keep_index_hint_conjuncts,
    bool abort_on_side_effects,
    bool inspect_subqueries)
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
        {
            if (!inspect_subqueries)
                continue;
            inside_subquery = true;
        }

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
            if (check_server_dependent
                || (inspect_subqueries && (!table_function || !table_function->isDeterministicInScopeOfQuery())))
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
            if (check_server_dependent && function_node->isOrdinaryFunction() && dependsOnServerTimeZone(*function_node))
                keep_in_having();

            if (inside_subquery)
            {
                if (function_node->isOrdinaryFunction())
                {
                    auto function_base = function_node->getFunction();
                    if (abort_on_side_effects && function_base && function_base->hasObservableSideEffects())
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
                        /// The same holds for functions depending on the block, like `blockSize`: a sibling conjunct
                        /// moved to `WHERE` would change the blocks of groups they are evaluated on.
                        if (function_base->isStateful()
                            || (abort_on_side_effects
                                && (function_base->hasObservableSideEffects() || !function_base->isDeterministicInScopeOfQuery())))
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
    QueryNode & query_node,
    const ContextPtr & context,
    bool keep_server_dependent_conjuncts,
    bool keep_index_hint_conjuncts,
    bool keep_pre_aggregation_expressions)
{
    if (!query_node.hasHaving())
        return;

    if (query_node.isGroupByWithCube()
        || query_node.isGroupByWithRollup()
        || query_node.isGroupByWithTotals()
        || query_node.isGroupByWithGroupingSets())
        return;

    /// A conjunct moved to `WHERE` removes rows before the expressions computed before aggregation, like the arguments of
    /// aggregate functions and the grouping keys. A stateful function there, like `sum(rowNumberInAllBlocks())`,
    /// or a function depending on the block, like `sum(blockSize())`, would see fewer rows and change the result even
    /// for the groups that pass the filter, and a function with observable side effects, like `sum(sleepEachRow(0.001))`,
    /// would be evaluated fewer times. Aggregate functions may appear in every section after `GROUP BY`,
    /// like `QUALIFY`, `LIMIT BY`, `LIMIT AFTER` and `LIMIT UNTIL`, so all of them are checked. In `HAVING` itself only
    /// the arguments of aggregate functions are computed before aggregation; the rest is classified per conjunct below.
    /// The join tree is checked as well: the plan optimizer may push the new `WHERE` below the expressions of `JOIN ON`
    /// and `ARRAY JOIN`, like `ARRAY JOIN range(blockSize())`.
    if (keep_pre_aggregation_expressions)
    {
        for (const auto & node : {query_node.getProjectionNode(), query_node.getJoinTreeNode(), query_node.getPrewhere(),
                 query_node.getWhere(), query_node.getGroupByNode(), query_node.getWindowNode(), query_node.getQualify(), query_node.getOrderByNode(),
                 query_node.getInterpolate(), query_node.getLimitByNode(), query_node.getLimitAfter(),
                 query_node.getLimitUntil()})
        {
            if (node && hasRowSensitiveFunction(node))
                return;
        }

        for (const auto & aggregate_function_node : collectAggregateFunctionNodes(query_node.getHaving()))
        {
            if (hasRowSensitiveFunction(aggregate_function_node))
                return;
        }
    }

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
        auto action = classifyHavingConjunctForMove(
            conjunct, keep_server_dependent_conjuncts, keep_index_hint_conjuncts,
            /*abort_on_side_effects=*/ keep_pre_aggregation_expressions,
            /*inspect_subqueries=*/ keep_pre_aggregation_expressions || keep_server_dependent_conjuncts);
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
