#include <Planner/CollectTableExpressionData.h>

#include <Storages/IStorage.h>

#include <Analyzer/Utils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>

#include <Planner/PlannerContext.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNSUPPORTED_METHOD;
}

namespace
{

class CollectSourceColumnsVisitor : public InDepthQueryTreeVisitor<CollectSourceColumnsVisitor>
{
public:
    explicit CollectSourceColumnsVisitor(PlannerContext & planner_context_)
        : planner_context(planner_context_)
    {}

    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        auto column_source_node = column_node->getColumnSource();
        auto column_source_node_type = column_source_node->getNodeType();

        if (column_source_node_type == QueryTreeNodeType::ARRAY_JOIN ||
            column_source_node_type == QueryTreeNodeType::LAMBDA)
            return;

        /// JOIN using expression
        if (column_node->hasExpression() && column_source_node->getNodeType() == QueryTreeNodeType::JOIN)
            return;

        auto & table_expression_data = planner_context.getOrCreateTableExpressionData(column_source_node);

        if (column_node->hasExpression())
        {
            /// Replace ALIAS column with expression
            table_expression_data.addAliasColumnName(column_node->getColumnName());
            node = column_node->getExpression();
            visitImpl(node);
            return;
        }

        if (column_source_node_type != QueryTreeNodeType::TABLE &&
            column_source_node_type != QueryTreeNodeType::TABLE_FUNCTION &&
            column_source_node_type != QueryTreeNodeType::QUERY &&
            column_source_node_type != QueryTreeNodeType::UNION)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Expected table, table function, query or union column source. Actual {}",
                column_source_node->formatASTForErrorMessage());

        bool column_already_exists = table_expression_data.hasColumn(column_node->getColumnName());
        if (column_already_exists)
            return;

        auto column_identifier = planner_context.getGlobalPlannerContext()->createColumnIdentifier(node);
        table_expression_data.addColumn(column_node->getColumn(), column_identifier);
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node)
    {
        return !(child_node->getNodeType() == QueryTreeNodeType::QUERY || child_node->getNodeType() == QueryTreeNodeType::UNION);
    }

private:
    PlannerContext & planner_context;
};

}

void collectTableExpressionData(QueryTreeNodePtr & query_node, PlannerContext & planner_context)
{
    auto & query_node_typed = query_node->as<QueryNode &>();
    auto table_expressions_nodes = extractTableExpressions(query_node_typed.getJoinTree());

    for (auto & table_expression_node : table_expressions_nodes)
    {
        auto & table_expression_data = planner_context.getOrCreateTableExpressionData(table_expression_node);

        if (auto * table_node = table_expression_node->as<TableNode>())
        {
            bool storage_is_remote = table_node->getStorage()->isRemote();
            table_expression_data.setIsRemote(storage_is_remote);
        }
        else if (auto * table_function_node = table_expression_node->as<TableFunctionNode>())
        {
            bool storage_is_remote = table_function_node->getStorage()->isRemote();
            table_expression_data.setIsRemote(storage_is_remote);
        }

        if (table_expression_data.isRemote())
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Remote storages are not supported");
    }

    CollectSourceColumnsVisitor collect_source_columns_visitor(planner_context);
    collect_source_columns_visitor.visit(query_node);
}

}
