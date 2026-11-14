#include <Analyzer/Passes/OptimizeKeyExpressionsUtils.h>

#include <algorithm>
#include <queue>
#include <unordered_set>
#include <vector>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/ValidationUtils.h>
#include <Common/Exception.h>
#include <Common/checkStackSize.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/TypeTree.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsBool group_by_use_nulls;
    extern const SettingsBool optimize_group_by_function_keys;
    extern const SettingsUInt64 distributed_group_by_no_merge;
}

namespace
{

struct NodeWithInfo
{
    QueryTreeNodePtr node;
    bool parents_are_only_deterministic = false;
};

bool isComparison(const String & name)
{
    return name == "equals" || name == "notEquals" || name == "less" || name == "greater" || name == "lessOrEquals"
        || name == "greaterOrEquals";
}

bool hasPreimage(const QueryTreeNodePtr & node)
{
    const auto * function = node->as<FunctionNode>();
    const auto function_base = function ? function->getFunction() : nullptr;
    return function_base && function_base->hasInformationAboutPreimage();
}

bool hasComparisonWithPreimage(const QueryTreeNodePtr & node)
{
    checkStackSize();

    const auto * function = node->as<FunctionNode>();
    if (function && isComparison(function->getFunctionName()) && std::ranges::any_of(function->getArguments().getNodes(), hasPreimage))
        return true;

    return std::ranges::any_of(node->getChildren(), [](const auto & child) { return child && hasComparisonWithPreimage(child); });
}

bool isReplaceableFunction(const QueryTreeNodePtr & node)
{
    const auto * function = node->as<FunctionNode>();
    if (!function)
        return false;

    /// `OptimizeDateOrDateTimeConverterWithPreimagePass`
    if (hasPreimage(node))
        return true;

    /// `InverseDictionaryLookupPass`
    const auto & name = function->getFunctionName();
    if (name.starts_with("dictGet"))
        return true;

    /// `ComparisonTupleEliminationPass`, `LogicalExpressionOptimizerPass` and `ConvertOrLikeChainPass`
    static const std::unordered_set<String> names = {
        "tuple", "and", "or", "not", "xor", "in", "notIn", "nullIn", "notNullIn", "globalIn", "globalNotIn", "globalNullIn",
        "globalNotNullIn", "like", "notLike", "ilike", "notILike", "match"};
    return isComparison(name) || names.contains(name);
}

/// `IfConstantConditionPass` replaces `if` with a branch.
bool isReplaceableFunctionOrBranch(const QueryTreeNodePtr & node)
{
    checkStackSize();

    if (isReplaceableFunction(node))
        return true;

    const auto * function = node->as<FunctionNode>();
    if (!function || (function->getFunctionName() != "if" && function->getFunctionName() != "multiIf"))
        return false;

    const auto & arguments = function->getArguments().getNodes();
    auto is_condition = [&](size_t i) { return i % 2 == 0 && i + 1 < arguments.size(); };

    bool has_constant_condition = false;
    for (size_t i = 0; i < arguments.size(); ++i)
        has_constant_condition |= is_condition(i) && arguments[i]->getNodeType() == QueryTreeNodeType::CONSTANT;

    if (!has_constant_condition)
        return false;

    for (size_t i = 0; i < arguments.size(); ++i)
        if (!is_condition(i) && isReplaceableFunctionOrBranch(arguments[i]))
            return true;

    return false;
}

bool keyArgumentTypesAreAllowed(const ColumnsWithTypeAndName & columns, bool allow_suspicious_types)
{
    if (allow_suspicious_types)
        return true;

    /// Dynamic and Variant types are not allowed in GROUP BY by default.
    auto is_suspicious = [](const IDataType & type) { return isDynamic(type) || isVariant(type); };

    for (const auto & column : columns)
        if (anyInTypeTree(*column.type, is_suspicious))
            return false;

    return true;
}

}

bool isReplaceableWithArguments(const QueryTreeNodePtr & node)
{
    /// `OptimizeDateOrDateTimeConverterWithPreimagePass` does not visit the arguments of comparisons, so it replaces a comparison in a
    /// key, but not in the same expression if that is compared.
    return isReplaceableFunctionOrBranch(node) || hasComparisonWithPreimage(node);
}

bool hasReplaceableWithArguments(const QueryTreeNodePtr & node)
{
    checkStackSize();

    /// `LikePerfectAffixRewritePass` and `ConvertOrLikeChainPass` replace `LIKE` with these.
    if (const auto * function = node->as<FunctionNode>())
    {
        const auto & name = function->getFunctionName();
        if (name == "startsWith" || name == "endsWith" || name.starts_with("multiSearchAny") || name == "multiMatchAny")
            return true;
    }

    return isReplaceableFunction(node)
        || std::ranges::any_of(node->getChildren(), [](const auto & child) { return child && hasReplaceableWithArguments(child); });
}

bool canRemoveGroupByKeysThatAreFunctionsOfOtherKeys(const QueryNode & query_node)
{
    const auto & settings = query_node.getContext()->getSettingsRef();
    if (!settings[Setting::optimize_group_by_function_keys])
        return false;

    /// When group_by_use_nulls = 1 removing keys from GROUP BY can lead
    /// to unexpected types in some functions.
    /// See example in https://github.com/ClickHouse/ClickHouse/pull/61567#issuecomment-2018007887
    if (settings[Setting::group_by_use_nulls])
        return false;

    if (!query_node.hasGroupBy())
        return false;

    /// Skip when a GROUP BY modifier produces rows where a grouping key is absent from the set
    /// being aggregated: CUBE/ROLLUP subtotals, GROUPING SETS non-member sets, and the WITH
    /// TOTALS row. In such a row the key is output as its column default. Dropping a key that is
    /// a function of other keys makes the output projection recompute it from those keys' totals
    /// defaults (e.g. toString(number) from number = 0 gives '0' instead of the required String
    /// default ''), which changes the result. See #110715.
    if (query_node.isGroupByWithCube() || query_node.isGroupByWithRollup()
        || query_node.isGroupByWithGroupingSets() || query_node.isGroupByWithTotals())
        return false;

    return true;
}

bool isExpressionFunctionOfKeys(
    const QueryTreeNodePtr & node, const QueryTreeNodePtrWithHashSet & keys, const QueryTreeNodes & aggregation_keys)
{
    auto is_aggregation_key = [&](const QueryTreeNodePtr & candidate)
    { return !aggregation_keys.empty() && isGroupByKeyAfterAggregation(candidate, aggregation_keys); };

    auto * function = node->as<FunctionNode>();
    if (!function || function->getArguments().getNodes().empty() || is_aggregation_key(node))
        return false;

    /// Aggregate and window functions are allowed as keys (for example in LIMIT BY), but they are
    /// not ordinary functions of their arguments, so such a key cannot be eliminated.
    const auto function_base = function->getFunction();
    if (!function_base)
        return false;

    std::vector<NodeWithInfo> candidates;
    auto & function_arguments = function->getArguments().getNodes();
    bool is_deterministic = function_base->isDeterministicInScopeOfQuery();
    for (auto it = function_arguments.rbegin(); it != function_arguments.rend(); ++it)
        candidates.push_back({ *it, is_deterministic });

    /// Using DFS we traverse function tree and try to find if it uses other keys as function arguments.
    bool found_at_least_one_usage = false;
    while (!candidates.empty())
    {
        auto [candidate, parents_are_only_deterministic] = candidates.back();
        candidates.pop_back();

        auto key = keys.find(candidate);
        bool found = key != keys.end();

        /// The expression is calculated from this key after the aggregation.
        if (found && !areConstantsWrittenAlike(candidate, key->node))
            return false;

        /// A key reached through a non-deterministic function is not determined by the keys, so the
        /// expression is not a function of them (for example rand64(g) takes the key g but is random).
        if (found && !parents_are_only_deterministic)
            return false;

        found_at_least_one_usage |= found;

        switch (candidate->getNodeType())
        {
            case QueryTreeNodeType::FUNCTION:
            {
                auto * func = candidate->as<FunctionNode>();
                auto & arguments = func->getArguments().getNodes();
                if (arguments.empty() || (!found && is_aggregation_key(candidate)))
                    return false;

                if (!found)
                {
                    const auto func_base = func->getFunction();
                    if (!func_base)
                        return false;

                    bool is_deterministic_function = parents_are_only_deterministic &&
                        func_base->isDeterministicInScopeOfQuery();
                    for (auto it = arguments.rbegin(); it != arguments.rend(); ++it)
                        candidates.push_back({ *it, is_deterministic_function });
                }
                break;
            }
            case QueryTreeNodeType::COLUMN:
                if (!found)
                    return false;
                break;
            case QueryTreeNodeType::CONSTANT:
                if (!parents_are_only_deterministic)
                    return false;
                break;
            default:
                return false;
        }
    }

    return found_at_least_one_usage;
}

void removeKeysThatAreFunctionsOfOtherKeys(
    QueryTreeNodes & keys,
    const QueryTreeNodes & aggregation_keys,
    UsableGroupByKeys usable_keys,
    const QueryTreeNodePtrWithHashIgnoreAliasesSet & keys_to_keep)
{
    QueryTreeNodePtrWithHashSet key_set;
    for (const auto & key : keys)
        if (usable_keys != UsableGroupByKeys::BEFORE_REPLACEMENTS || !isReplaceableWithArguments(key))
            key_set.insert(key);

    QueryTreeNodes new_keys;
    new_keys.reserve(keys.size());
    for (auto & key : keys)
    {
        const bool is_considered = !keys_to_keep.contains(key)
            && (usable_keys != UsableGroupByKeys::AFTER_REPLACEMENTS || hasReplaceableWithArguments(key));
        if (!is_considered || !isExpressionFunctionOfKeys(key, key_set, aggregation_keys))
            new_keys.push_back(key);
    }

    keys = std::move(new_keys);
}

QueryTreeNodes unwrapInjectiveFunctionsInKeys(
    const QueryTreeNodes & keys, bool allow_suspicious_types, const QueryTreeNodePtrWithHashIgnoreAliasesSet * keys_to_keep)
{
    QueryTreeNodes new_keys;
    new_keys.reserve(keys.size());
    for (const auto & key : keys)
    {
        std::queue<QueryTreeNodePtr> nodes_to_process;
        nodes_to_process.push(key);

        while (!nodes_to_process.empty())
        {
            auto node_to_process = nodes_to_process.front();
            nodes_to_process.pop();

            const auto * function_node = node_to_process->as<FunctionNode>();
            if (!function_node || (keys_to_keep && keys_to_keep->contains(node_to_process)))
            {
                // Constant aggregation keys are removed in PlannerExpressionAnalysis.cpp
                new_keys.push_back(node_to_process);
                continue;
            }

            /// Only ordinary functions can be injective. Aggregate and window functions, for which
            /// getFunction returns null, are allowed as keys (for example in LIMIT BY) and are kept as is.
            const auto function = function_node->getFunction();
            bool can_be_eliminated = false;
            if (function)
            {
                const auto arguments = function_node->getArgumentColumns();
                can_be_eliminated = function->isInjective(arguments) && keyArgumentTypesAreAllowed(arguments, allow_suspicious_types);
            }

            if (can_be_eliminated)
            {
                for (const auto & argument : function_node->getArguments())
                {
                    if (argument->getNodeType() != QueryTreeNodeType::CONSTANT)
                        nodes_to_process.push(argument);
                }
            }
            else
                new_keys.push_back(node_to_process);
        }
    }

    return new_keys;
}

bool isAggregateFunctionOfEqualValues(const FunctionNode & function)
{
    if (!function.isAggregateFunction())
        return false;

    /// Every aggregate here returns an actual element of its input column, so over a group where
    /// the argument is a GROUP BY key (constant within the group) the result equals that key and
    /// the aggregate can be dropped. Aliases (any_value / first_value -> any, last_value ->
    /// anyLast, *RespectNulls -> *_respect_nulls) are normalized to these canonical names by name
    /// resolution before this pass runs, so matching the canonical names covers them too.
    /// The same holds for the non-interpolating exact quantiles (quantileExact, its alias
    /// medianExact, quantileExactLow, quantileExactHigh), which return an element of the input
    /// at some position, and for the idempotent groupBitAnd / groupBitOr (x & x = x | x = x).
    /// The result type check below rejects any variant whose result type differs from the key.
    /// singleValueOrNull is excluded: it returns NULL unless the group has exactly one distinct
    /// value, so it is not value-preserving; groupBitXor is not idempotent; interpolating
    /// quantiles may return a value that is not an element; argMin/argMax are two-argument.
    static const std::unordered_set<String> names = {
        "min", "max", "any", "anyLast", "anyHeavy", "any_respect_nulls", "anyLast_respect_nulls", "quantileExact", "quantileExactLow",
        "quantileExactHigh", "groupBitAnd", "groupBitOr"};
    if (!names.contains(function.getFunctionName()))
        return false;

    const auto & arguments = function.getArguments().getNodes();
    if (arguments.size() != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected a single argument of function '{}' but received {}",
            function.getFunctionName(),
            arguments.size());

    /// Compare names, because types with a custom name, like `Bool`, are equal to the types without it.
    return recursiveRemoveLowCardinality(function.getResultType())->getName()
        == recursiveRemoveLowCardinality(arguments[0]->getResultType())->getName();
}

QueryTreeNodes getGroupByKeys(const QueryNode & query_node)
{
    QueryTreeNodes keys;
    if (!query_node.hasGroupBy())
        return keys;

    for (const auto & node : query_node.getGroupBy().getNodes())
    {
        if (query_node.isGroupByWithGroupingSets())
        {
            const auto & grouping_set_keys = node->as<ListNode &>().getNodes();
            keys.insert(keys.end(), grouping_set_keys.begin(), grouping_set_keys.end());
        }
        else
        {
            keys.push_back(node);
        }
    }
    return keys;
}

bool isGroupByKeyAfterAggregation(const QueryTreeNodePtr & node, const QueryTreeNodes & group_by_keys)
{
    if (const auto * function_node = node->as<FunctionNode>(); function_node && function_node->isConvertedToNullable())
        return true;
    return std::ranges::any_of(group_by_keys, [&](const auto & key) { return compareGroupByKeys(node, key); });
}

namespace
{

/// Whether `node` has an `arrayJoin`, and is not one of `calculated_nodes`. If it only has another name, it can be calculated again.
bool hasArrayJoinThatIsNotCalculated(const QueryTreeNodePtr & node, const QueryTreeNodes & calculated_nodes)
{
    return hasFunctionNode(node, "arrayJoin")
        && std::ranges::none_of(
               calculated_nodes,
               [&](const QueryTreeNodePtr & calculated_node)
               { return calculated_node->isEqual(*node) && areConstantsWrittenAlike(calculated_node, node); });
}

}

bool canHaveSeveralRowsPerGroup(const QueryNode & query_node)
{
    /// The groups of different shards are not merged.
    if (query_node.getContext()->getSettingsRef()[Setting::distributed_group_by_no_merge])
        return true;

    const auto & keys = query_node.getGroupBy().getNodes();
    auto adds_rows = [&](const QueryTreeNodePtr & node) { return hasArrayJoinThatIsNotCalculated(node, keys); };

    for (const auto & node : query_node.getOrderBy().getNodes())
    {
        const auto & sort_node = node->as<const SortNode &>();
        if (sort_node.withFill() || adds_rows(sort_node.getExpression()))
            return true;
    }

    if (query_node.hasHaving() && adds_rows(query_node.getHaving()))
        return true;

    if (query_node.hasQualify() && adds_rows(query_node.getQualify()))
        return true;

    return std::ranges::any_of(query_node.getLimitBy().getNodes(), adds_rows)
        || std::ranges::any_of(query_node.getProjection().getNodes(), adds_rows);
}

bool canHaveEqualRowsAfterDistinct(const QueryNode & query_node)
{
    /// The rows of different shards are not merged.
    if (query_node.getContext()->getSettingsRef()[Setting::distributed_group_by_no_merge])
        return true;

    /// Of the other clauses, only `LIMIT BY` and `WITH FILL` are done after `DISTINCT` and can add rows.
    if (std::ranges::any_of(
            query_node.getOrderBy().getNodes(), [](const QueryTreeNodePtr & node) { return node->as<const SortNode &>().withFill(); }))
        return true;

    const auto & projection = query_node.getProjection().getNodes();
    return std::ranges::any_of(
        query_node.getLimitBy().getNodes(),
        [&](const QueryTreeNodePtr & node) { return hasArrayJoinThatIsNotCalculated(node, projection); });
}

namespace
{

/// In which way the planner writes the constant in names.
bool hasCastInName(const ConstantNode & node)
{
    if (node.hasSourceExpression())
        return true;

    try
    {
        return ConstantNode::requiresCastCall(applyVisitor(FieldToDataType(), node.getValue()), node.getResultType());
    }
    catch (...)
    {
        /// `FieldToDataType` can throw for arrays of mixed types.
        return true;
    }
}

template <typename Predicate>
bool allConstantsInEqualNodes(const QueryTreeNodePtr & lhs, const QueryTreeNodePtr & rhs, const Predicate & predicate)
{
    checkStackSize();

    if (lhs == rhs)
        return true;

    return std::ranges::equal(
        lhs->getChildren(),
        rhs->getChildren(),
        [&](const auto & lhs_child, const auto & rhs_child)
        {
            if (!lhs_child || !rhs_child)
                return !lhs_child && !rhs_child;

            const auto * lhs_constant = lhs_child->template as<ConstantNode>();
            const auto * rhs_constant = rhs_child->template as<ConstantNode>();
            return lhs_constant && rhs_constant ? predicate(*lhs_constant, *rhs_constant)
                                                : allConstantsInEqualNodes(lhs_child, rhs_child, predicate);
        });
}

/// Equal constants can have types with different time zones, and can be 0 and -0.
bool haveSameValueName(const ConstantNode & lhs, const ConstantNode & rhs)
{
    return lhs.getResultType()->getName() == rhs.getResultType()->getName() && lhs.getValueName({}) == rhs.getValueName({});
}

}

bool areConstantsWrittenAlike(const QueryTreeNodePtr & lhs, const QueryTreeNodePtr & rhs)
{
    return allConstantsInEqualNodes(
        lhs,
        rhs,
        [](const ConstantNode & lhs_constant, const ConstantNode & rhs_constant)
        { return hasCastInName(lhs_constant) == hasCastInName(rhs_constant) && haveSameValueName(lhs_constant, rhs_constant); });
}

bool areConstantsEqual(const QueryTreeNodePtr & lhs, const QueryTreeNodePtr & rhs)
{
    return allConstantsInEqualNodes(lhs, rhs, haveSameValueName);
}

bool hasNonDeterministicFunction(const QueryTreeNodePtr & node)
{
    checkStackSize();

    if (const auto * function_node = node->as<FunctionNode>(); function_node && function_node->getFunctionName() != "arrayJoin")
        if (const auto function = function_node->getFunction())
            if (!function->isDeterministicInScopeOfQuery() || function->isStateful())
                return true;

    return std::ranges::any_of(node->getChildren(), [](const auto & child) { return child && hasNonDeterministicFunction(child); });
}

bool haveSameValue(const QueryTreeNodePtr & lhs, const QueryTreeNodePtr & rhs)
{
    return areConstantsWrittenAlike(lhs, rhs)
        || (areConstantsEqual(lhs, rhs) && !hasFunctionNode(lhs, "arrayJoin") && !hasNonDeterministicFunction(lhs));
}

bool canBeCalculatedFromKeys(const QueryTreeNodePtr & node, const QueryTreeNodes & group_by_keys)
{
    checkStackSize();

    const auto is_key = [&](const QueryTreeNodePtr & key) { return compareGroupByKeys(node, key) && areConstantsWrittenAlike(key, node); };
    if (std::ranges::any_of(group_by_keys, is_key))
        return true;

    switch (node->getNodeType())
    {
        case QueryTreeNodeType::COLUMN:
        {
            const auto source = node->as<ColumnNode &>().getColumnSourceOrNull();
            return source && source->getNodeType() == QueryTreeNodeType::LAMBDA_ARGS;
        }
        case QueryTreeNodeType::QUERY:
        case QueryTreeNodeType::UNION:
        case QueryTreeNodeType::TABLE:
        case QueryTreeNodeType::TABLE_FUNCTION:
            return true;
        case QueryTreeNodeType::FUNCTION:
        {
            if (node->as<FunctionNode &>().isAggregateFunction())
                return true;
            break;
        }
        default:
            break;
    }

    return std::ranges::all_of(
        node->getChildren(), [&](const auto & child) { return !child || canBeCalculatedFromKeys(child, group_by_keys); });
}

bool canEqualGroupByKeysDiffer(const QueryNode & query_node)
{
    if (!query_node.isGroupByWithRollup() && !query_node.isGroupByWithCube() && !query_node.isGroupByWithGroupingSets())
        return false;

    QueryTreeNodePtrWithHashSet unique_keys;
    return std::ranges::any_of(
        getGroupByKeys(query_node),
        [&](const QueryTreeNodePtr & key)
        {
            auto [unique_key, inserted] = unique_keys.emplace(key);
            return !inserted && !areConstantsWrittenAlike(unique_key->node, key);
        });
}

namespace
{

void collectGroupingFunctionArguments(const QueryTreeNodePtr & node, QueryTreeNodePtrWithHashIgnoreAliasesSet & arguments)
{
    checkStackSize();

    if (node->getNodeType() == QueryTreeNodeType::QUERY || node->getNodeType() == QueryTreeNodeType::UNION)
        return;

    /// The name after `GroupingFunctionsResolvePass`, if there are no subtotals.
    if (const auto * function_node = node->as<FunctionNode>(); function_node && function_node->getFunctionName() == "__groupingOrdinary")
        arguments.insert(function_node->getArguments().begin(), function_node->getArguments().end());

    for (const auto & child : node->getChildren())
        if (child)
            collectGroupingFunctionArguments(child, arguments);
}

}

QueryTreeNodePtrWithHashIgnoreAliasesSet getGroupingFunctionArguments(const QueryNode & query_node)
{
    QueryTreeNodePtrWithHashIgnoreAliasesSet arguments;
    for (const auto & clause : query_node.getChildren())
        if (clause && clause != query_node.getJoinTreeNode())
            collectGroupingFunctionArguments(clause, arguments);

    return arguments;
}

}
