#include <Analyzer/Passes/RewriteArrayJoinFilterToArrayFilterPass.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/WindowFunctionsUtils.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFunction.h>

#include <Functions/logical.h>

#include <Interpreters/ArrayJoinAction.h>

#include <Core/Settings.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool optimize_rewrite_array_join_filter_to_array_filter;
}

namespace
{

struct ArrayJoinProducerInfo
{
    enum class Kind
    {
        Function,
        ArrayJoinClause,
    };

    Kind kind = Kind::Function;
    QueryTreeNodePtr array_expression;
    ArrayJoinNode * array_join_node = nullptr;
    String array_join_column_name;
};

bool isArrayJoinFunction(const FunctionNode & function_node)
{
    return function_node.getFunctionName() == "arrayJoin";
}

std::optional<ArrayJoinProducerInfo> tryGetArrayJoinProducer(const QueryTreeNodePtr & node)
{
    if (const auto * function_node = node->as<FunctionNode>())
    {
        if (!isArrayJoinFunction(*function_node))
            return {};

        const auto & arguments = function_node->getArguments().getNodes();
        if (arguments.size() != 1 || !arguments[0])
            return {};

        ArrayJoinProducerInfo info;
        info.kind = ArrayJoinProducerInfo::Kind::Function;
        info.array_expression = arguments[0];
        return info;
    }

    const auto * column_node = node->as<ColumnNode>();
    if (!column_node)
        return {};

    auto source = column_node->getColumnSourceOrNull();
    if (!source)
        return {};

    auto * array_join_node = source->as<ArrayJoinNode>();
    if (!array_join_node)
        return {};

    /// LEFT ARRAY JOIN keeps a default row for empty arrays; filtering the array before
    /// expansion would drop that row and change semantics.
    if (array_join_node->isLeft())
        return {};

    const auto & join_expressions = array_join_node->getJoinExpressions().getNodes();
    /// Multi-array ARRAY JOIN uses aligned expansion; independent arrayFilter would break that.
    if (join_expressions.size() != 1)
        return {};

    auto * join_expression_column = join_expressions[0]->as<ColumnNode>();
    if (!join_expression_column || !join_expression_column->hasExpression())
        return {};

    ArrayJoinProducerInfo info;
    info.kind = ArrayJoinProducerInfo::Kind::ArrayJoinClause;
    info.array_expression = join_expression_column->getExpression();
    info.array_join_node = array_join_node;
    info.array_join_column_name = column_node->getColumnName();
    return info;
}

bool sameArrayJoinProducer(const ArrayJoinProducerInfo & lhs, const ArrayJoinProducerInfo & rhs)
{
    if (lhs.kind != rhs.kind)
        return false;

    if (lhs.kind == ArrayJoinProducerInfo::Kind::Function)
        return lhs.array_expression->isEqual(*rhs.array_expression, {.compare_aliases = false});

    return lhs.array_join_node == rhs.array_join_node
        && lhs.array_join_column_name == rhs.array_join_column_name;
}

void collectArrayJoinProducers(const QueryTreeNodePtr & node, std::vector<ArrayJoinProducerInfo> & producers)
{
    if (!node)
        return;

    if (auto producer = tryGetArrayJoinProducer(node))
    {
        producers.push_back(std::move(*producer));
        /// Do not look inside arrayJoin / ARRAY JOIN column references for nested producers.
        return;
    }

    /// Do not collect producers from subqueries nested inside the conjunct.
    if (isQueryOrUnionNode(node))
        return;

    for (const auto & child : node->getChildren())
        collectArrayJoinProducers(child, producers);
}

bool matchesArrayJoinProducer(const QueryTreeNodePtr & node, const ArrayJoinProducerInfo & producer)
{
    auto current = tryGetArrayJoinProducer(node);
    return current && sameArrayJoinProducer(*current, producer);
}

bool isDeterministicAndStateless(const QueryTreeNodePtr & node, const ArrayJoinProducerInfo & producer_to_ignore)
{
    std::vector<QueryTreeNodePtr> nodes_to_check = {node};

    while (!nodes_to_check.empty())
    {
        auto current = nodes_to_check.back();
        nodes_to_check.pop_back();

        if (!current)
            continue;

        if (matchesArrayJoinProducer(current, producer_to_ignore))
            continue;

        if (isQueryOrUnionNode(current))
            return false;

        if (const auto * function_node = current->as<FunctionNode>())
        {
            if (auto function_base = function_node->getFunction())
            {
                if (function_base->isStateful())
                    return false;
                if (!function_base->isDeterministicInScopeOfQuery())
                    return false;
            }
        }

        for (const auto & child : current->getChildren())
            nodes_to_check.push_back(child);
    }

    return true;
}

void extractTopLevelAndConjuncts(const QueryTreeNodePtr & node, QueryTreeNodes & conjuncts)
{
    const auto * function_node = node->as<FunctionNode>();
    if (function_node && function_node->getFunctionName() == "and")
    {
        for (const auto & argument : function_node->getArguments().getNodes())
            extractTopLevelAndConjuncts(argument, conjuncts);
        return;
    }

    conjuncts.push_back(node);
}

QueryTreeNodePtr makeConjunction(const QueryTreeNodes & nodes)
{
    if (nodes.empty())
        return nullptr;

    if (nodes.size() == 1)
        return nodes.front();

    auto function_node = std::make_shared<FunctionNode>("and");
    function_node->markAsOperator();
    for (const auto & node : nodes)
        function_node->getArguments().getNodes().push_back(node);

    const auto & function = createInternalFunctionAndOverloadResolver();
    function_node->resolveAsFunction(function->build(function_node->getArgumentColumns()));
    return function_node;
}

DataTypePtr getArrayJoinElementType(const QueryTreeNodePtr & array_expression)
{
    auto array_type = getArrayJoinDataType(array_expression->getResultType());
    if (!array_type)
        return {};
    return array_type->getNestedType();
}

class ReplaceArrayJoinProducerVisitor : public InDepthQueryTreeVisitor<ReplaceArrayJoinProducerVisitor>
{
public:
    ReplaceArrayJoinProducerVisitor(const ArrayJoinProducerInfo & producer_, QueryTreeNodePtr replacement_)
        : producer(producer_)
        , replacement(std::move(replacement_))
    {
    }

    static bool needChildVisit(QueryTreeNodePtr &, QueryTreeNodePtr & child)
    {
        return !isQueryOrUnionNode(child);
    }

    void visitImpl(QueryTreeNodePtr & node) const
    {
        if (matchesArrayJoinProducer(node, producer))
            node = replacement;
    }

private:
    const ArrayJoinProducerInfo & producer;
    QueryTreeNodePtr replacement;
};

QueryTreeNodePtr replaceProducerWithLambdaArgument(
    const QueryTreeNodePtr & expression,
    const ArrayJoinProducerInfo & producer,
    const QueryTreeNodePtr & lambda_argument)
{
    auto result = expression->clone();
    ReplaceArrayJoinProducerVisitor visitor(producer, lambda_argument);
    visitor.visit(result);
    return result;
}

QueryTreeNodePtr buildArrayFilter(
    const QueryTreeNodePtr & array_expression,
    const QueryTreeNodePtr & lambda_body,
    const LambdaArgumentsNodePtr & lambda_arguments,
    const ContextPtr & context)
{
    auto element_type = getArrayJoinElementType(array_expression);
    if (!element_type)
        return {};

    auto lambda_node = std::make_shared<LambdaNode>(lambda_arguments, lambda_body, false /*is_operator*/);
    lambda_node->resolve(std::make_shared<DataTypeFunction>(DataTypes{element_type}, lambda_body->getResultType()));

    auto array_filter = std::make_shared<FunctionNode>("arrayFilter");
    array_filter->getArguments().getNodes() = {lambda_node, array_expression};
    resolveOrdinaryFunctionNodeByName(*array_filter, "arrayFilter", context);
    return array_filter;
}

/** Count arrayJoin(E) nodes under root, without descending into nested queries/unions. */
class CountMatchingArrayJoinVisitor : public InDepthQueryTreeVisitor<CountMatchingArrayJoinVisitor>
{
public:
    explicit CountMatchingArrayJoinVisitor(const ArrayJoinProducerInfo & producer_)
        : producer(producer_)
    {
    }

    static bool needChildVisit(QueryTreeNodePtr &, QueryTreeNodePtr & child)
    {
        return !isQueryOrUnionNode(child);
    }

    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !isArrayJoinFunction(*function_node))
            return;

        const auto & arguments = function_node->getArguments().getNodes();
        if (arguments.size() != 1 || !arguments[0])
            return;

        if (arguments[0]->isEqual(*producer.array_expression, {.compare_aliases = false}))
            ++count;
    }

    size_t getCount() const { return count; }

private:
    const ArrayJoinProducerInfo & producer;
    size_t count = 0;
};

size_t countMatchingArrayJoinFunctions(QueryTreeNodePtr node, const ArrayJoinProducerInfo & producer)
{
    CountMatchingArrayJoinVisitor visitor(producer);
    visitor.visit(node);
    return visitor.getCount();
}

class WrapArrayJoinArgumentsVisitor : public InDepthQueryTreeVisitor<WrapArrayJoinArgumentsVisitor>
{
public:
    WrapArrayJoinArgumentsVisitor(const ArrayJoinProducerInfo & producer_, QueryTreeNodePtr filtered_array_, const ContextPtr & context_)
        : producer(producer_)
        , filtered_array(std::move(filtered_array_))
        , context(context_)
    {
    }

    static bool needChildVisit(QueryTreeNodePtr &, QueryTreeNodePtr & child)
    {
        /// Only rewrite arrayJoin nodes in the current query scope.
        return !isQueryOrUnionNode(child);
    }

    void visitImpl(QueryTreeNodePtr & node) const
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !isArrayJoinFunction(*function_node))
            return;

        auto & arguments = function_node->getArguments().getNodes();
        if (arguments.size() != 1 || !arguments[0])
            return;

        /// Already wrapped with this filtered array (e.g. shared node visited twice via WITH).
        if (arguments[0]->isEqual(*filtered_array, {.compare_aliases = false}))
            return;

        if (!arguments[0]->isEqual(*producer.array_expression, {.compare_aliases = false}))
            return;

        arguments[0] = filtered_array->clone();
        resolveOrdinaryFunctionNodeByName(*function_node, "arrayJoin", context);
    }

private:
    const ArrayJoinProducerInfo & producer;
    QueryTreeNodePtr filtered_array;
    ContextPtr context;
};

bool tryRewriteConjunctGroup(
    QueryTreeNodePtr & query_tree_node,
    const ArrayJoinProducerInfo & producer,
    const QueryTreeNodes & conjuncts,
    const ContextPtr & context)
{
    auto element_type = getArrayJoinElementType(producer.array_expression);
    if (!element_type)
        return false;

    static constexpr const char * lambda_argument_name = "__array_join_filter_x";

    auto lambda_arguments = std::make_shared<LambdaArgumentsNode>(Names{lambda_argument_name});
    lambda_arguments->resolve(DataTypes{element_type});
    auto lambda_argument = std::make_shared<ColumnNode>(
        NameAndTypePair{lambda_argument_name, element_type},
        lambda_arguments);

    QueryTreeNodes lambda_bodies;
    lambda_bodies.reserve(conjuncts.size());

    for (const auto & conjunct : conjuncts)
    {
        if (containsSubquery(conjunct))
            return false;

        if (hasAggregateFunctionNodes(conjunct) || hasWindowFunctionNodes(conjunct))
            return false;

        if (!isDeterministicAndStateless(conjunct, producer))
            return false;

        auto body = replaceProducerWithLambdaArgument(conjunct, producer, lambda_argument);
        if (matchesArrayJoinProducer(body, producer) || hasFunctionNode(body, "arrayJoin"))
            return false;

        lambda_bodies.push_back(std::move(body));
    }

    auto lambda_body = makeConjunction(lambda_bodies);
    if (!lambda_body)
        return false;

    auto filtered_array = buildArrayFilter(
        producer.array_expression,
        lambda_body,
        lambda_arguments,
        context);
    if (!filtered_array)
        return false;

    if (producer.kind == ArrayJoinProducerInfo::Kind::Function)
    {
        WrapArrayJoinArgumentsVisitor visitor(producer, filtered_array, context);
        visitor.visit(query_tree_node);
    }
    else
    {
        auto & join_expression_column = producer.array_join_node->getJoinExpressions().getNodes().front()->as<ColumnNode &>();
        join_expression_column.getExpressionOrThrow() = filtered_array;
    }

    return true;
}

class RewriteArrayJoinFilterToArrayFilterVisitor : public InDepthQueryTreeVisitorWithContext<RewriteArrayJoinFilterToArrayFilterVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<RewriteArrayJoinFilterToArrayFilterVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_rewrite_array_join_filter_to_array_filter])
            return;

        auto * query_node = node->as<QueryNode>();
        if (!query_node || !query_node->hasWhere())
            return;

        auto & where_node = query_node->getWhere();
        QueryTreeNodes conjuncts;
        extractTopLevelAndConjuncts(where_node, conjuncts);
        if (conjuncts.empty())
            return;

        struct Group
        {
            ArrayJoinProducerInfo producer;
            std::vector<size_t> conjunct_indexes;
        };

        std::vector<Group> groups;
        std::vector<char> rewritten(conjuncts.size(), false);

        for (size_t i = 0; i < conjuncts.size(); ++i)
        {
            if (containsSubquery(conjuncts[i]))
                continue;

            std::vector<ArrayJoinProducerInfo> producers;
            collectArrayJoinProducers(conjuncts[i], producers);
            if (producers.empty())
                continue;

            const auto & first = producers.front();
            bool all_same = true;
            for (size_t j = 1; j < producers.size(); ++j)
            {
                if (!sameArrayJoinProducer(first, producers[j]))
                {
                    all_same = false;
                    break;
                }
            }
            if (!all_same)
                continue;

            bool found_group = false;
            for (auto & group : groups)
            {
                if (sameArrayJoinProducer(group.producer, first))
                {
                    group.conjunct_indexes.push_back(i);
                    found_group = true;
                    break;
                }
            }
            if (!found_group)
                groups.push_back(Group{first, {i}});
        }

        bool any_dropped = false;
        for (const auto & group : groups)
        {
            QueryTreeNodes group_conjuncts;
            group_conjuncts.reserve(group.conjunct_indexes.size());
            for (size_t index : group.conjunct_indexes)
                group_conjuncts.push_back(conjuncts[index]);

            bool can_drop_conjuncts = true;
            if (group.producer.kind == ArrayJoinProducerInfo::Kind::Function)
            {
                /// Dropping is safe only if some arrayJoin(E) remains outside these conjuncts
                /// (e.g. in the projection). Otherwise WHERE is the sole expansion site.
                const size_t total_matches = countMatchingArrayJoinFunctions(node, group.producer);
                size_t matches_in_group = 0;
                for (const auto & conjunct : group_conjuncts)
                    matches_in_group += countMatchingArrayJoinFunctions(conjunct, group.producer);
                can_drop_conjuncts = total_matches > matches_in_group;
            }

            if (!tryRewriteConjunctGroup(node, group.producer, group_conjuncts, getContext()))
                continue;

            /// For ARRAY JOIN, expansion lives in the join tree, so dropping is always safe.
            /// For arrayJoin(), drop only if some arrayJoin(E) remains outside these conjuncts
            /// (typically the projection); otherwise WHERE is the sole expansion site.
            if (can_drop_conjuncts)
            {
                for (size_t index : group.conjunct_indexes)
                    rewritten[index] = true;
                any_dropped = true;
            }
        }

        if (!any_dropped)
            return;

        QueryTreeNodes remaining_conjuncts;
        remaining_conjuncts.reserve(conjuncts.size());
        for (size_t i = 0; i < conjuncts.size(); ++i)
        {
            if (!rewritten[i])
                remaining_conjuncts.push_back(conjuncts[i]);
        }

        where_node = makeConjunction(remaining_conjuncts);
    }
};

}

void RewriteArrayJoinFilterToArrayFilterPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    RewriteArrayJoinFilterToArrayFilterVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
