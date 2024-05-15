#include <Analyzer/Passes/OptimizeGroupByToDistinctPass.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>

namespace DB
{

namespace
{

class GroupByToDistinctVisitor : public InDepthQueryTreeVisitorWithContext<GroupByToDistinctVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<GroupByToDistinctVisitor>;
    using Base::Base;

    bool hasAggregatesInProjection(const QueryNodePtr & query_node)
    {
        auto & projection_nodes = query_node->getProjection().getNodes();
        for (const auto & node : projection_nodes)
        {
            if (isAggregateFunctionNode(node))
                return true;
        }
        return false;
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * query_node = node->as<QueryNode>();

        // Проверяем, что запрос содержит только SELECT
        if (!query_node)
        return;

        if (!query_node->hasGroupBy() || hasAggregatesInProjection(query_node))
            return;

        if (!query_node->hasLimit())
            return;

        // Получаем список столбцов для группировки
        auto & group_by_columns = query_node->getGroupBy().getNodes();

        // Создаем новый узел запроса с оператором DISTINCT
        auto distinct_query_node = std::make_shared<QueryNode>(Context::createCopy(query_node->getContext()));
        distinct_query_node->setIsDistinct(true);

        distinct_query_node->getJoinTree() = query_node->getJoinTree();

        // Копируем столбцы для группировки в узел DISTINCT
        for (const auto & column_node : group_by_columns)
        {
            distinct_query_node->getProjection().getNodes().push_back(column_node);
        }

        // Копируем оператор LIMIT в узел DISTINCT
        distinct_query_node->getLimit() = query_node->getLimit();

        // Заменяем текущий узел запроса узлом с оператором DISTINCT
        query_node = distinct_query_node.get();
    }
};

}

void OptimizeGroupByToDistinctPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    GroupByToDistinctVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
