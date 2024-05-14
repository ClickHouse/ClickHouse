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

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings().group_by_to_distinct_optimization)
            return;

        auto * query_node = node->as<QueryNode>();

        // Проверяем, что запрос содержит только SELECT
        if (!query_node || (query_node->hasWith() || query_node->hasPrewhere() || query_node->hasWhere() || query_node->hasHaving() ||
            query_node->hasWindow() || query_node->hasOrderBy() || query_node->hasLimitByLimit() || query_node->hasLimitByOffset() ||
            query_node->hasLimitBy() || query_node->hasLimit() || query_node->hasOffset()))
            return;

        // Проверяем, что в запросе есть оператор GROUP BY
        if (!query_node->hasGroupBy())
            return;

        // Проверяем, что запрос имеет оператор LIMIT
        if (!query_node->hasLimit())
            return;

        // Получаем список столбцов для группировки
        auto & group_by_columns = query_node->getGroupBy().getNodes();

        // Создаем новый узел запроса с оператором DISTINCT
        auto distinct_query_node = std::make_shared<QueryNode>(Context::createCopy(query_node->getContext()));
        distinct_query_node->getJoinTree() = query_node->getJoinTree();

        // Копируем столбцы для группировки в узел DISTINCT
        for (const auto & column_node : group_by_columns)
        {
            distinct_query_node->getProjection().getNodes().push_back(column_node);
            distinct_query_node->getGroupBy().getNodes().push_back(column_node);
        }

        // Копируем оператор LIMIT в узел DISTINCT
        distinct_query_node->getLimit() = query_node->getLimit();

        // Заменяем текущий узел запроса узлом с оператором DISTINCT
        query_node->swap(*distinct_query_node);
    }
};

}

void OptimizeGroupByToDistinctPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    GroupByToDistinctVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
