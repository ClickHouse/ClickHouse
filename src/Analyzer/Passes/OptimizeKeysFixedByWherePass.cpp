#include <Analyzer/Passes/OptimizeKeysFixedByWherePass.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/WindowFunctionsUtils.h>
#include <Analyzer/WindowNode.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/ComparisonParams.h>
#include <Functions/IFunction.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool empty_result_for_aggregation_by_constant_keys_on_empty_set;
    extern const SettingsBool optimize_keys_fixed_by_where;
}

namespace
{

/// The name of a column or a function at the root of an expression, otherwise empty.
std::string_view getRootName(const IQueryTreeNode & node)
{
    if (const auto * column = node.as<ColumnNode>())
        return column->getColumnName();
    if (const auto * function = node.as<FunctionNode>())
        return function->getFunctionName();
    return {};
}

/// Maps expressions with a column or a function at the root to nodes, such as the expressions that WHERE fixes to
/// constants of their types. Aliases are ignored, as the planner ignores them when it matches expressions to
/// aggregation keys: `GROUP BY tenant` must match `tenant AS t` in the projection. Then the columns of two table
/// expressions that differ only in their aliases compare equal, so such queries are skipped.
class ExpressionMap
{
public:
    void add(const QueryTreeNodePtr & expression, QueryTreeNodePtr node)
    {
        auto root_name = getRootName(*expression);
        if (root_name.empty())
            return;
        root_names.emplace(root_name);
        replacements.emplace(expression, std::move(node));
    }

    bool empty() const { return replacements.empty(); }

    const IQueryTreeNode * find(const QueryTreeNodePtr & node) const
    {
        /// Hashing a node hashes its whole subtree, so the name of its root is checked first.
        if (!root_names.contains(getRootName(*node)))
            return nullptr;
        auto it = replacements.find(node);
        return it == replacements.end() ? nullptr : it->second.get();
    }

private:
    std::unordered_set<std::string_view> root_names;
    QueryTreeNodePtrWithHashIgnoreAliasesMap<QueryTreeNodePtr> replacements;
};

/// Whether `expression = constant` holds for a single value of `expression`. It does not for the types
/// with different values that compare equal: floating-point (`-0.0 = 0.0`), `String` and `FixedString` with
/// trailing zero bytes, `Variant`, `Dynamic` and `JSON`. A `String` constant is accepted for the types that
/// `equals` parses it into.
bool isExactEquality(const DataTypePtr & expression_type, const DataTypePtr & constant_type)
{
    auto expression = removeLowCardinalityAndNullable(expression_type);
    auto constant = removeLowCardinalityAndNullable(constant_type);
    WhichDataType which_expression(expression);
    WhichDataType which_constant(constant);

    if (which_expression.isString())
        return which_constant.isString();
    if (which_expression.isInteger())
        return which_constant.isInteger();
    /// `equals` rescales the operand of the lower scale, which can overflow, so that has to be the constant.
    if (which_expression.isDecimal())
        return which_constant.isInteger() || (which_constant.isDecimal() && getDecimalScale(*constant) <= getDecimalScale(*expression));
    if (which_expression.isFixedString())
        return expression->equals(*constant);
    if (which_expression.isDateOrDate32OrDateTimeOrDateTime64() || which_expression.isEnum() || which_expression.isUUID()
        || which_expression.isIPv4() || which_expression.isIPv6())
        return which_constant.isString() || expression->equals(*constant);
    return false;
}

/// Whether the expression has the same value wherever it appears in the query.
bool isDeterministicExpression(const QueryTreeNodePtr & expression)
{
    std::vector<const IQueryTreeNode *> nodes{expression.get()};
    while (!nodes.empty())
    {
        const auto * node = nodes.back();
        nodes.pop_back();

        if (isQueryOrUnionNode(node))
            return false;

        if (const auto * function = node->as<FunctionNode>())
            if (!function->isOrdinaryFunction() || !function->getFunctionOrThrow()->isDeterministicInScopeOfQuery())
                return false;

        for (const auto & child : node->getChildren())
            if (child)
                nodes.push_back(child.get());
    }
    return true;
}

/// Collects the expressions that the conjuncts `expression = constant` of the condition fix.
void collectFixedExpressions(const QueryTreeNodePtr & condition, const FormatSettings & format_settings, ExpressionMap & fixed_expressions)
{
    QueryTreeNodes conjuncts{condition};
    while (!conjuncts.empty())
    {
        auto conjunct = std::move(conjuncts.back());
        conjuncts.pop_back();

        const auto * function = conjunct->as<FunctionNode>();
        if (!function)
            continue;

        const auto & arguments = function->getArguments().getNodes();
        if (function->getFunctionName() == "and")
        {
            conjuncts.insert(conjuncts.end(), arguments.begin(), arguments.end());
            continue;
        }

        if (function->getFunctionName() != "equals" || arguments.size() != 2)
            continue;

        size_t constant_index = arguments[0]->as<ConstantNode>() ? 0 : 1;
        const auto * constant = arguments[constant_index]->as<ConstantNode>();
        const auto & expression = arguments[1 - constant_index];
        if (!constant || getRootName(*expression).empty())
            continue;

        const auto & expression_type = expression->getResultType();
        const auto & constant_type = constant->getResultType();

        /// A server constant such as `hostName` differs between shards, and a non-deterministic one
        /// such as `now` may be evaluated again on them.
        if (!isExactEquality(expression_type, constant_type) || !constant->isDeterministic() || !isConstantSameOnAllShards(*constant)
            || !isDeterministicExpression(expression))
            continue;

        /// The replacement has the type of the expression, so the types of the query result do not change. A string is
        /// parsed with the format settings `equals` uses, as `cast_string_to_date_time_mode` changes its value.
        Field value = tryConvertFieldToTypeExact(constant->getValue(), *expression_type, constant_type.get(), format_settings);
        if (value.isNull())
            continue;

        fixed_expressions.add(expression, std::make_shared<ConstantNode>(std::move(value), expression_type));
    }
}

/// Replaces the fixed expressions in an expression evaluated after aggregation with `materialize(constant)`, which,
/// like the column, is evaluated for each row: a function of a constant is folded while planning, so `intDiv(1, x)`
/// would throw even when no row has `x = 0`. The arguments of aggregate functions are evaluated before the
/// aggregation, and subqueries and the expressions of columns, such as those of `JOIN USING`, belong to other steps.
/// An expression equal to a remaining GROUP BY key reads the key, which is computed once with the aggregate arguments
/// it shares, such as `arrayJoin`.
void replaceFixedExpressions(
    QueryTreeNodePtr & node, const ExpressionMap & fixed_expressions, const ExpressionMap & remaining_keys, const ContextPtr & context)
{
    if (!node)
        return;

    if (const auto * replacement = fixed_expressions.find(node))
    {
        node = createResolvedFunction(context, "materialize", {replacement->clone()});
        return;
    }

    if (node->getNodeType() == QueryTreeNodeType::CONSTANT || node->getNodeType() == QueryTreeNodeType::COLUMN || isQueryOrUnionNode(node)
        || remaining_keys.find(node) != nullptr)
        return;

    if (const auto * function = node->as<FunctionNode>(); function && function->isAggregateFunction())
        return;

    for (auto & child : node->getChildren())
        replaceFixedExpressions(child, fixed_expressions, remaining_keys, context);
}

class OptimizeKeysFixedByWhereVisitor : public InDepthQueryTreeVisitorWithContext<OptimizeKeysFixedByWhereVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<OptimizeKeysFixedByWhereVisitor>;
    using Base::Base;

    static bool needChildVisit(QueryTreeNodePtr & parent, QueryTreeNodePtr &)
    {
        return parent->getNodeType() != QueryTreeNodeType::TABLE_FUNCTION;
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_keys_fixed_by_where])
            return;

        /// The initiator of a distributed query sends the query already optimized, and expects the shards to return
        /// the columns of that query. An initiator without the optimization would not find the removed keys.
        if (getContext()->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
            return;

        auto * query = node->as<QueryNode>();
        if (!query || !query->hasWhere())
            return;

        /// The rows of totals and of the subtotals of ROLLUP, CUBE and GROUPING SETS have default values in place
        /// of the keys. WITH FILL inserts rows with other values of the sorting keys. The values of the columns
        /// from the outer query of a correlated subquery are not fixed by its WHERE.
        if (query->isGroupByWithTotals() || query->isGroupByWithRollup() || query->isGroupByWithCube()
            || query->isGroupByWithGroupingSets() || query->isCorrelated())
            return;

        for (const auto & sort_node : query->getOrderBy().getNodes())
            if (sort_node->as<SortNode &>().withFill())
                return;

        ExpressionMap fixed_expressions;
        collectFixedExpressions(query->getWhere(), ComparisonParams(getContext()).format_settings, fixed_expressions);
        if (fixed_expressions.empty())
            return;

        auto window_functions = collectWindowFunctionNodes(node);
        if (!query->hasGroupBy() && !query->hasOrderBy() && window_functions.empty())
            return;

        /// Two table expressions equal up to their aliases, as in `t AS a JOIN t AS b`.
        auto table_expressions = extractTableExpressions(query->getJoinTreeNodeTyped(), /*add_array_join=*/ true);
        if (table_expressions.size() > 1)
        {
            QueryTreeNodePtrWithHashIgnoreAliasesSet unique_table_expressions;
            for (const auto & table_expression : table_expressions)
                if (!unique_table_expressions.emplace(table_expression).second)
                    return;
        }

        /// Without any ORDER BY element the rows would not be read in the order of the table, which gives the elements
        /// fixed by WHERE an order in practice.
        auto & order_by = query->getOrderBy().getNodes();
        auto is_fixed_sort_node = [&](const QueryTreeNodePtr & sort_node)
        {
            return fixed_expressions.find(sort_node->as<SortNode &>().getExpression()) != nullptr;
        };
        if (!std::ranges::all_of(order_by, is_fixed_sort_node))
            std::erase_if(order_by, is_fixed_sort_node);

        auto remove_fixed_partition_keys = [&](WindowNode & window)
        {
            std::erase_if(
                window.getPartitionBy().getNodes(),
                [&](const QueryTreeNodePtr & partition_key) { return fixed_expressions.find(partition_key) != nullptr; });
        };
        for (const auto & window_function : window_functions)
            remove_fixed_partition_keys(window_function->as<FunctionNode &>().getWindowNode()->as<WindowNode &>());
        for (const auto & window : query->getWindow().getNodes())
            remove_fixed_partition_keys(window->as<WindowNode &>());

        optimizeGroupBy(*query, fixed_expressions);
    }

private:
    /// A fixed GROUP BY key is replaced with a constant, and its other uses with the same value. The planner removes
    /// constant keys, and returns an empty result for empty input if no key remains.
    void optimizeGroupBy(QueryNode & query, const ExpressionMap & fixed_expressions)
    {
        auto & keys = query.getGroupBy().getNodes();
        auto is_fixed = [&](const QueryTreeNodePtr & key) { return fixed_expressions.find(key) != nullptr; };
        if (std::ranges::none_of(keys, is_fixed))
            return;

        /// Without the setting, aggregation by constant keys returns a row for empty input, unlike the query.
        if (!getSettings()[Setting::empty_result_for_aggregation_by_constant_keys_on_empty_set]
            && std::ranges::all_of(keys, [&](const QueryTreeNodePtr & key) { return is_fixed(key) || key->getNodeType() == QueryTreeNodeType::CONSTANT; }))
            return;

        /// Every clause but these is evaluated after the aggregation.
        const QueryTreeNodePtr * before_aggregation[]
            = {&query.getWithNode(), &query.getJoinTreeNode(), &query.getPrewhere(), &query.getWhere(), &query.getGroupByNode()};
        std::vector<QueryTreeNodePtr *> after_aggregation;
        for (auto & child : query.getChildren())
            if (child && std::ranges::find(before_aggregation, &child) == std::end(before_aggregation))
                after_aggregation.push_back(&child);

        /// A correlated subquery reads the columns of this query, which the aggregation no longer outputs.
        for (const auto * expression : after_aggregation)
            if (containsCorrelatedSubquery(*expression))
                return;

        /// A key that is a constant is removed by the planner.
        ExpressionMap remaining_keys;
        for (auto & key : keys)
        {
            if (const auto * replacement = fixed_expressions.find(key))
                key = replacement->clone();
            else
                remaining_keys.add(key, key);
        }

        for (auto * expression : after_aggregation)
            replaceFixedExpressions(*expression, fixed_expressions, remaining_keys, getContext());
    }
};

}

void OptimizeKeysFixedByWherePass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    OptimizeKeysFixedByWhereVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
