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

class CollectSourceColumnsMatcher
{
public:
    using Visitor = InDepthQueryTreeVisitor<CollectSourceColumnsMatcher, true, false>;

    struct Data
    {
        PlannerContext & planner_context;
    };

    static void visit(QueryTreeNodePtr & node, Data & data)
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

        auto & table_expression_node_to_data = data.planner_context.getTableExpressionNodeToData();

        auto [it, _] = table_expression_node_to_data.emplace(column_source_node, TableExpressionData());
        auto & table_expression_columns = it->second;

        if (column_node->hasExpression())
        {
            /// Replace ALIAS column with expression
            table_expression_columns.addAliasColumnName(column_node->getColumnName());
            node = column_node->getExpression();
            visit(node, data);
            return;
        }

        if (column_source_node_type != QueryTreeNodeType::TABLE &&
            column_source_node_type != QueryTreeNodeType::TABLE_FUNCTION &&
            column_source_node_type != QueryTreeNodeType::QUERY &&
            column_source_node_type != QueryTreeNodeType::UNION)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Expected table, table function, query or union column source. Actual {}",
                column_source_node->formatASTForErrorMessage());

        bool column_already_exists = table_expression_columns.hasColumn(column_node->getColumnName());

        if (!column_already_exists)
        {
            auto column_identifier = data.planner_context.getColumnUniqueIdentifier(column_source_node, column_node->getColumnName());
            data.planner_context.registerColumnNode(node, column_identifier);
            table_expression_columns.addColumn(column_node->getColumn(), column_identifier);
        }
        else
        {
            auto column_identifier = table_expression_columns.getColumnIdentifierOrThrow(column_node->getColumnName());
            data.planner_context.registerColumnNode(node, column_identifier);
        }
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node)
    {
        return !(child_node->getNodeType() == QueryTreeNodeType::QUERY || child_node->getNodeType() == QueryTreeNodeType::UNION);
    }
};

using CollectSourceColumnsVisitor = CollectSourceColumnsMatcher::Visitor;

}

void collectTableExpressionData(QueryTreeNodePtr & query_node, PlannerContext & planner_context)
{
    auto & query_node_typed = query_node->as<QueryNode &>();
    auto table_expressions_nodes = extractTableExpressions(query_node_typed.getJoinTree());
    auto & table_expression_node_to_data = planner_context.getTableExpressionNodeToData();

    for (auto & table_expression_node : table_expressions_nodes)
    {
        auto [it, _] = table_expression_node_to_data.emplace(table_expression_node, TableExpressionData());

        if (auto * table_node = table_expression_node->as<TableNode>())
        {
            bool storage_is_remote = table_node->getStorage()->isRemote();
            it->second.setIsRemote(storage_is_remote);
        }
        else if (auto * table_function_node = table_expression_node->as<TableFunctionNode>())
        {
            bool storage_is_remote = table_function_node->getStorage()->isRemote();
            it->second.setIsRemote(storage_is_remote);
        }

        if (it->second.isRemote())
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Remote storages are not supported");
    }

    CollectSourceColumnsVisitor::Data data {planner_context};
    CollectSourceColumnsVisitor collect_source_columns_visitor(data);
    collect_source_columns_visitor.visit(query_node);
}

}
