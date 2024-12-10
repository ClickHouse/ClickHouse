#include <Analyzer/Passes/CrossToInnerJoinPass.h>

#include <DataTypes/getLeastSupertype.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>

#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/Utils.h>

#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/logical.h>

#include <Common/logger_useful.h>
#include <Core/Settings.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 cross_to_inner_join_rewrite;
}

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

namespace
{

void extractJoinConditions(const QueryTreeNodePtr & node, QueryTreeNodes & equi_conditions, QueryTreeNodes & other)
{
    auto * func = node->as<FunctionNode>();
    if (!func)
    {
        other.push_back(node);
        return;
    }

    const auto & args = func->getArguments().getNodes();

    if (args.size() == 2 && func->getFunctionName() == "equals")
    {
        equi_conditions.push_back(node);
    }
    else if (func->getFunctionName() == "and")
    {
        for (const auto & arg : args)
            extractJoinConditions(arg, equi_conditions, other);
    }
    else
    {
        other.push_back(node);
    }
}

const QueryTreeNodePtr & getEquiArgument(const QueryTreeNodePtr & cond, size_t index)
{
    const auto * func = cond->as<FunctionNode>();
    chassert(func && func->getFunctionName() == "equals" && func->getArguments().getNodes().size() == 2);
    return func->getArguments().getNodes()[index];
}

bool findInTableExpression(const QueryTreeNodePtr & source, const QueryTreeNodePtr & table_expression)
{
    if (!source)
        return true;

    if (source->isEqual(*table_expression))
        return true;

    if (const auto * join_node = table_expression->as<JoinNode>())
    {
        return findInTableExpression(source, join_node->getLeftTableExpression())
            || findInTableExpression(source, join_node->getRightTableExpression());
    }

    return false;
}

void getJoinNodes(QueryTreeNodePtr & join_tree_node, std::vector<JoinNode *> & join_nodes)
{
    auto * join_node = join_tree_node->as<JoinNode>();
    if (!join_node)
        return;

    if (!isCrossOrComma(join_node->getKind()))
        return;

    join_nodes.push_back(join_node);
    getJoinNodes(join_node->getLeftTableExpression(), join_nodes);
    getJoinNodes(join_node->getRightTableExpression(), join_nodes);
}

class CrossToInnerJoinVisitor : public InDepthQueryTreeVisitorWithContext<CrossToInnerJoinVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<CrossToInnerJoinVisitor>;
    using Base::Base;

    struct JoinGraph
    {
        struct Edge
        {
            QueryTreeNodes equi_conditions;
        };

        struct Node
        {
            QueryTreeNodePtr table_node;
            /// JoinKind kind;
            std::unordered_map<size_t, Edge *> edges;
        };

        std::vector<Node> nodes;
        std::list<Edge> edges;
    };

    JoinGraph buildJoinGraph(const QueryTreeNodes & table_nodes, QueryTreeNodes & equi_conditions, QueryTreeNodes & other_conditions)
    {
        JoinGraph graph;

        graph.nodes.reserve(table_nodes.size());
        for (const auto & table_node : table_nodes)
            graph.nodes.emplace_back(JoinGraph::Node{table_node, {}});

        for (auto & condition : equi_conditions)
        {
            const auto & lhs_equi_argument = getEquiArgument(condition, 0);
            const auto & rhs_equi_argument = getEquiArgument(condition, 1);

            bool is_useful = false;

            auto left_src = getExpressionSource(lhs_equi_argument);
            auto right_src = getExpressionSource(rhs_equi_argument);

            if (left_src && right_src)
            {
                std::optional<size_t> lhs_index;
                std::optional<size_t> rhs_index;
                for (size_t i = 0; i < table_nodes.size(); ++i)
                {
                    const auto & table_node = table_nodes[i];
                    if (findInTableExpression(left_src, table_node))
                        lhs_index = i;
                    if (findInTableExpression(right_src, table_node))
                        rhs_index = i;
                }

                if (lhs_index != std::nullopt && rhs_index != std::nullopt && *lhs_index != *rhs_index)
                {
                    is_useful = true;
                    auto & edge = graph.nodes[*lhs_index].edges[*rhs_index];
                    if (!edge)
                    {
                        edge = &graph.edges.emplace_back();
                        graph.nodes[*rhs_index].edges[*lhs_index] = edge;
                    }

                    // std::cerr << "+ Cond " << *lhs_index << " " << *rhs_index << " : " << condition->dumpTree() << std::endl;
                    edge->equi_conditions.push_back(std::move(condition));
                }
            }

            if (!is_useful)
                other_conditions.push_back(std::move(condition));
        }

        return graph;
    }

    struct TableWithConditions
    {
        QueryTreeNodePtr table_node;
        QueryTreeNodes equi_conditions;
    };

    std::vector<TableWithConditions> buildJoinsChain(const JoinGraph & graph)
    {
        std::vector<TableWithConditions> res;
        std::set<size_t> active_set;
        std::vector<bool> visited(graph.nodes.size(), false);

        for (size_t i = 0; i < graph.nodes.size(); ++i)
        {
            if (visited[i])
            {
                // std::cerr << "Skipped : " << graph.nodes[i].table_node->dumpTree() << std::endl;
                continue;
            }

            active_set.insert(i);

            while (!active_set.empty())
            {
                size_t node = *active_set.begin();
                active_set.erase(active_set.begin());
                visited[node] = true;

                QueryTreeNodes equi_conditions;
                for (const auto & [dest, edge] : graph.nodes[node].edges)
                {
                    if (visited[dest])
                    {
                        std::cerr << node << " -> " << dest << std::endl;
                        equi_conditions.insert(equi_conditions.end(), edge->equi_conditions.begin(), edge->equi_conditions.end());
                    }
                    else
                        active_set.insert(dest);
                }

                // std::cerr << "Added : " << graph.nodes[node].table_node->dumpTree() << std::endl;
                // if (auto conj = makeConjunction(equi_conditions))
                //     std::cerr << "Cond : " << conj->dumpTree() << std::endl;
                res.emplace_back(graph.nodes[node].table_node, std::move(equi_conditions));
            }
        }

        return res;
    }

    QueryTreeNodePtr rebuildJoins(std::vector<TableWithConditions> tables)
    {
        QueryTreeNodes cross;
        QueryTreeNodePtr lhs;
        for (auto & table_with_condition : tables)
        {
            if (!lhs)
            {
                lhs = table_with_condition.table_node;
                continue;
            }

            auto join_node = std::make_shared<JoinNode>(
                std::move(lhs),
                std::move(table_with_condition.table_node),
                nullptr,
                JoinLocality::Unspecified,
                JoinStrictness::Unspecified,
                JoinKind::Cross,
                false);

            if (table_with_condition.equi_conditions.empty())
            {
                if (forceRewrite(join_node->getKind()))
                {
                    throw Exception(ErrorCodes::INCORRECT_QUERY,
                        "Failed to rewrite '{}' to INNER JOIN: "
                        "no equi-join conditions found in WHERE clause. "
                        "You may set setting `cross_to_inner_join_rewrite` to `1` to allow slow CROSS JOIN for this case",
                        join_node->formatASTForErrorMessage());
                }

                cross.push_back(std::move(join_node->getLeftTableExpression()));
                lhs = std::move(join_node->getRightTableExpression());
                continue;
            }

            join_node->crossToInner(makeConjunction(table_with_condition.equi_conditions));
            lhs = std::move(join_node);
        }

        if (cross.empty())
            return lhs;

        cross.push_back(lhs);
        return std::make_shared<CrossJoinNode>(std::move(cross));
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!isEnabled())
            return;

        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        auto & where_node = query_node->getWhere();
        if (!where_node)
            return;

        auto & join_tree_node = query_node->getJoinTree();
        if (!join_tree_node || join_tree_node->getNodeType() != QueryTreeNodeType::CROSS_JOIN)
            return;

        auto & cross_join_node = join_tree_node->as<CrossJoinNode &>();

        QueryTreeNodes equi_conditions;
        QueryTreeNodes other_conditions;
        extractJoinConditions(where_node, equi_conditions, other_conditions);
        if (equi_conditions.empty())
            return;

        auto join_graph = buildJoinGraph(cross_join_node.getTableExpressions(), equi_conditions, other_conditions);
        if (join_graph.edges.empty())
            return;

        auto tables_with_conditions = buildJoinsChain(join_graph);
        auto table_node = rebuildJoins(tables_with_conditions);
        query_node->getJoinTree() = table_node;

        if (!other_conditions.empty())
            where_node = makeConjunction(other_conditions);
    }

private:
    bool isEnabled() const { return getSettings()[Setting::cross_to_inner_join_rewrite]; }

    bool forceRewrite(JoinKind kind) const
    {
        if (kind == JoinKind::Cross)
            return false;
        /// Comma join can be forced to rewrite
        return getSettings()[Setting::cross_to_inner_join_rewrite] >= 2;
    }

    QueryTreeNodePtr makeConjunction(const QueryTreeNodes & nodes)
    {
        if (nodes.empty())
            return nullptr;

        if (nodes.size() == 1)
            return nodes.front();

        auto function_node = std::make_shared<FunctionNode>("and");
        for (const auto & node : nodes)
            function_node->getArguments().getNodes().push_back(node);

        const auto & function = createInternalFunctionAndOverloadResolver();
        function_node->resolveAsFunction(function->build(function_node->getArgumentColumns()));
        return function_node;
    }
};

}

void CrossToInnerJoinPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    CrossToInnerJoinVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
