#include <Analyzer/Passes/MergeHasChainToHasAnyPass.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/convertFieldToType.h>

#include <cmath>
#include <unordered_set>

namespace DB
{

namespace Setting
{
    extern const SettingsBool optimize_rewrite_has_chain_to_has_any;
    extern const SettingsUInt64 optimize_min_has_chain_length;
}

namespace
{

/// Two calls can only be merged if they see the same array values, so the array expression must be deterministic.
bool isExpressionNonDeterministic(const QueryTreeNodePtr & node)
{
    if (const auto * function = node->as<FunctionNode>(); function && function->isOrdinaryFunction()
        && !function->getFunctionOrThrow()->isDeterministicInScopeOfQuery())
        return true;

    for (const auto & child : node->getChildren())
        if (child && isExpressionNonDeterministic(child))
            return true;

    return false;
}

struct Member
{
    QueryTreeNodePtr array;
    Array elements;
    DataTypePtr element_type;
};

/// `has(arr, c)`, `hasAny(arr, [c, ...])` or `hasAll(arr, [c])` with constant elements of the element type of `arr`.
/// A `hasAll` with two or more elements is a conjunction, which no `hasAny` can take in.
std::optional<Member> matchMember(const QueryTreeNodePtr & node)
{
    const auto * function = node->as<FunctionNode>();
    if (!function)
        return {};

    const auto & name = function->getFunctionName();
    if (name != "has" && name != "hasAny" && name != "hasAll")
        return {};

    const auto & arguments = function->getArguments().getNodes();
    if (arguments.size() != 2)
        return {};

    const auto * constant = arguments[1]->as<ConstantNode>();
    const auto * array_type = typeid_cast<const DataTypeArray *>(arguments[0]->getResultType().get());
    if (!constant || !array_type)
        return {};

    Array elements;
    DataTypePtr constant_element_type;
    if (name == "has")
    {
        elements.push_back(constant->getValue());
        constant_element_type = constant->getResultType();
    }
    else
    {
        const auto * constant_array_type = typeid_cast<const DataTypeArray *>(constant->getResultType().get());
        if (!constant_array_type)
            return {};
        elements = constant->getValue().safeGet<Array>();
        constant_element_type = constant_array_type->getNestedType();
    }

    /// `hasAny(arr, [])` is false and `hasAll(arr, [])` is true whatever the array holds; neither has anything to merge.
    if (elements.empty() || isExpressionNonDeterministic(arguments[0]))
        return {};

    /// `has` compares the constant with the elements as values of the array's type, so the merged array keeps that
    /// type. A number of another width is accepted if it converts to that type and back without change: `has(ids, 5)`
    /// on an `Array(UInt64)` merges, `has(ids, -1)` does not. Strings, enums and fixed strings have to match exactly,
    /// because `has` and `hasAny` compare a `String` constant against an `Enum` or `FixedString` element differently.
    auto element_type = removeLowCardinalityAndNullable(array_type->getNestedType());
    auto constant_type = removeLowCardinalityAndNullable(constant_element_type);
    const bool convert = !constant_type->equals(*element_type);
    if (convert && (!isNumber(constant_type) || !isNumber(element_type)))
        return {};

    /// `hasAny` and `hasAll` need a common type of the two arrays and throw without one, for example for `[5.0]` against
    /// `Array(UInt64)`. Such a call must keep throwing on its own, so it is not merged into a chain that would run.
    if (convert && name != "has" && !tryGetLeastSupertype(DataTypes{array_type->getNestedType(), constant_element_type}))
        return {};

    for (auto & element : elements)
    {
        /// `has` finds a NaN needle in an `Array(LowCardinality(Float64))` by its bytes in the dictionary, while `hasAny`
        /// compares values and never matches NaN, so a NaN member stays as written.
        if (element.isNull() || (element.getType() == Field::Types::Float64 && std::isnan(element.safeGet<Float64>())))
            return {};
        if (convert)
        {
            element = tryConvertFieldToTypeExact(element, *element_type, constant_type.get());
            if (element.isNull())
                return {};
        }
    }

    if (name == "hasAll" && elements.size() > 1)
        return {};

    return Member{arguments[0], std::move(elements), std::move(element_type)};
}

/// `NOT has(arr, c)`, `NOT hasAny(arr, [c, ...])`, `NOT hasAll(arr, [c])` or `notHas(arr, c)`.
std::optional<Member> matchNegatedMember(const QueryTreeNodePtr & node)
{
    const auto * function = node->as<FunctionNode>();
    if (!function)
        return {};

    if (function->getFunctionName() == "not" && function->getArguments().getNodes().size() == 1)
        return matchMember(function->getArguments().getNodes()[0]);

    if (function->getFunctionName() != "notHas")
        return {};

    /// Look at `notHas(arr, c)` as `has(arr, c)` so the matcher above can be reused. This node is thrown away.
    auto has_node = std::make_shared<FunctionNode>("has");
    has_node->getArguments().getNodes() = function->getArguments().getNodes();
    return matchMember(has_node);
}

QueryTreeNodePtr makeHasAny(const QueryTreeNodePtr & array, Array elements, const DataTypePtr & element_type, bool negate, const ContextPtr & context)
{
    /// The constant is typed as the array's elements, so `hasAny` resolves without a cast to a common type.
    auto has_any = std::make_shared<FunctionNode>("hasAny");
    has_any->getArguments().getNodes()
        = {array, std::make_shared<ConstantNode>(Field(std::move(elements)), std::make_shared<DataTypeArray>(element_type))};
    resolveOrdinaryFunctionNodeByName(*has_any, "hasAny", context);
    if (!negate)
        return has_any;

    /// Marked as an operator so that `EXPLAIN` and the query sent to shards spell it `NOT`.
    auto not_node = std::make_shared<FunctionNode>("not");
    not_node->markAsOperator();
    not_node->getArguments().getNodes() = {std::move(has_any)};
    resolveOrdinaryFunctionNodeByName(*not_node, "not", context);
    return not_node;
}

/// The members of one `and`/`or` that can be merged, grouped by the array they test.
struct Group
{
    std::vector<size_t> positions;
    Array elements;
    DataTypePtr element_type;
};

/// Needles are sets, so a chain merges into the union of its needles: `has(a) OR hasAny([b, c])` is `hasAny([a, b, c])`,
/// and `NOT a AND NOT b` is `NOT (a OR b)`, so negated members merge under `and` and plain ones under `or`.
QueryTreeNodePtrWithHashMap<Group> collectGroups(const FunctionNode & function)
{
    const bool negated = function.getFunctionName() == "and";
    const auto & operands = function.getArguments().getNodes();

    QueryTreeNodePtrWithHashMap<Group> groups;
    for (size_t i = 0; i < operands.size(); ++i)
    {
        auto match = negated ? matchNegatedMember(operands[i]) : matchMember(operands[i]);
        if (!match)
            continue;

        auto & group = groups[match->array];
        group.positions.push_back(i);
        group.elements.insert(group.elements.end(), match->elements.begin(), match->elements.end());
        group.element_type = std::move(match->element_type);
    }
    return groups;
}

bool isChain(const QueryTreeNodePtr & node)
{
    const auto * function = node->as<FunctionNode>();
    return function && (function->getFunctionName() == "and" || function->getFunctionName() == "or");
}

bool hasMergeableGroup(const QueryTreeNodePtrWithHashMap<Group> & groups, size_t min_chain_length)
{
    for (const auto & entry : groups)
        if (entry.second.positions.size() >= min_chain_length)
            return true;
    return false;
}

/// Does this filter have a chain worth merging? Nested queries are handled when the visitor reaches them, and a node
/// shared through an alias is checked once.
bool containsMergeableChain(const QueryTreeNodePtr & node, size_t min_chain_length, std::unordered_set<const IQueryTreeNode *> & visited)
{
    if (!node || node->as<QueryNode>() || node->as<UnionNode>() || !visited.insert(node.get()).second)
        return false;

    if (isChain(node) && hasMergeableGroup(collectGroups(*node->as<FunctionNode>()), min_chain_length))
        return true;

    for (const auto & child : node->getChildren())
        if (containsMergeableChain(child, min_chain_length, visited))
            return true;
    return false;
}

/// Merges the chains of one filter in place. By the time this runs the filter is a private copy, see `rewriteFilter`.
class MergeChainsInFilterVisitor : public InDepthQueryTreeVisitor<MergeChainsInFilterVisitor>
{
public:
    MergeChainsInFilterVisitor(ContextPtr context_, size_t min_chain_length_)
        : context(std::move(context_)), min_chain_length(min_chain_length_)
    {
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child)
    {
        return !child->as<QueryNode>() && !child->as<UnionNode>();
    }

    void visitImpl(QueryTreeNodePtr & node)
    {
        if (!isChain(node))
            return;

        auto * function = node->as<FunctionNode>();
        const bool negated = function->getFunctionName() == "and";
        auto & operands = function->getArguments().getNodes();
        auto groups = collectGroups(*function);

        /// The merged call takes the place of the first member of its group.
        std::vector<QueryTreeNodePtr> replacement(operands.size());
        std::vector<bool> merged_away(operands.size(), false);
        bool changed = false;
        for (auto & [array, group] : groups)
        {
            if (group.positions.size() < min_chain_length)
                continue;

            replacement[group.positions.front()] = makeHasAny(array.node, std::move(group.elements), group.element_type, negated, context);
            for (size_t k = 1; k < group.positions.size(); ++k)
                merged_away[group.positions[k]] = true;
            changed = true;
        }

        if (!changed)
            return;

        QueryTreeNodes kept;
        for (size_t i = 0; i < operands.size(); ++i)
            if (!merged_away[i])
                kept.push_back(replacement[i] ? replacement[i] : operands[i]);

        /// `and` and `or` need two arguments, so a single survivor takes the slot itself.
        if (kept.size() == 1)
        {
            node = kept[0];
            return;
        }

        operands = std::move(kept);
        resolveOrdinaryFunctionNodeByName(*function, function->getFunctionName(), context);
    }

private:
    ContextPtr context;
    size_t min_chain_length;
};

class MergeHasChainToHasAnyVisitor : public InDepthQueryTreeVisitorWithContext<MergeHasChainToHasAnyVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<MergeHasChainToHasAnyVisitor>;
    using Base::Base;

    /// The merge is done in filters only, where a text index can use it: `WHERE`, `PREWHERE` and `JOIN ON`, of
    /// every query including subqueries. A chain in the projection, `GROUP BY`, `HAVING` or `ORDER BY` may be made
    /// of grouping keys, which have to stay as written to be found after the aggregation.
    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_rewrite_has_chain_to_has_any])
            return;

        if (auto * query = node->as<QueryNode>())
        {
            rewriteFilter(query->getWhere());
            rewriteFilter(query->getPrewhere());
        }
        else if (auto * join = node->as<JoinNode>())
        {
            rewriteFilter(join->getJoinExpression());
        }
    }

private:
    /// The analyzer gives every clause that uses an alias the same node, so the filter is rewritten on a copy and the
    /// projection and the `GROUP BY` keys keep the original. The copy is made only when there is a chain to merge.
    void rewriteFilter(QueryTreeNodePtr & filter)
    {
        if (!filter)
            return;

        /// Without a text index, `hasAny` with fewer than four elements is slower than the separate `has` calls,
        /// so short chains are left alone unless the setting asks for them. The length counts members, not needles.
        const size_t min_chain_length = std::max<size_t>(2, getSettings()[Setting::optimize_min_has_chain_length]);

        std::unordered_set<const IQueryTreeNode *> visited;
        if (!containsMergeableChain(filter, min_chain_length, visited))
            return;

        filter = filter->clone();
        MergeChainsInFilterVisitor visitor(getContext(), min_chain_length);
        visitor.visit(filter);
    }
};

}

void MergeHasChainToHasAnyPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    MergeHasChainToHasAnyVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
