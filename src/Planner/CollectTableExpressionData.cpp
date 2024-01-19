#include <Planner/CollectTableExpressionData.h>

#include <Storages/IStorage.h>

#include <Analyzer/Utils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/ListNode.h>

#include <Planner/PlannerContext.h>
#include <Planner/PlannerActionsVisitor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_PREWHERE;
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
        /// Special case for USING clause which contains references to ALIAS columns.
        /// We can not modify such ColumnNode.
        if (auto * join_node = node->as<JoinNode>())
        {
            if (!join_node->isUsingJoinExpression())
                return;

            auto & using_list = join_node->getJoinExpression()->as<ListNode&>();
            for (auto & using_element : using_list)
            {
                auto & column_node = using_element->as<ColumnNode&>();
                /// This list contains column nodes from left and right tables.
                auto & columns_from_subtrees = column_node.getExpressionOrThrow()->as<ListNode&>().getNodes();

                /// Visit left table column node.
                visitUsingColumn(columns_from_subtrees[0]);
                /// Visit right table column node.
                visitUsingColumn(columns_from_subtrees[1]);
            }
            return;
        }

        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        if (column_node->getColumnName() == "__grouping_set")
            return;

        auto column_source_node = column_node->getColumnSource();
        auto column_source_node_type = column_source_node->getNodeType();

        if (column_source_node_type == QueryTreeNodeType::LAMBDA)
            return;

        /// JOIN using expression
        if (column_node->hasExpression() && column_source_node_type == QueryTreeNodeType::JOIN)
            return;

        auto & table_expression_data = planner_context.getOrCreateTableExpressionData(column_source_node);

        if (column_node->hasExpression() && column_source_node_type != QueryTreeNodeType::ARRAY_JOIN)
        {
            /// Replace ALIAS column with expression
            bool column_already_exists = table_expression_data.hasColumn(column_node->getColumnName());
            if (!column_already_exists)
            {
                auto column_identifier = planner_context.getGlobalPlannerContext()->createColumnIdentifier(node);
                table_expression_data.addAliasColumnName(column_node->getColumnName(), column_identifier);
            }

            node = column_node->getExpression();
            visitImpl(node);
            return;
        }

        /// JoinNode doesn't have its dedicated identifiers,
        /// we apply projection actions from outer scope to columns from JOIN
        if (column_source_node_type == QueryTreeNodeType::JOIN)
            return;

        if (column_source_node_type != QueryTreeNodeType::TABLE &&
            column_source_node_type != QueryTreeNodeType::TABLE_FUNCTION &&
            column_source_node_type != QueryTreeNodeType::QUERY &&
            column_source_node_type != QueryTreeNodeType::UNION &&
            column_source_node_type != QueryTreeNodeType::ARRAY_JOIN)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Expected table, table function, array join, query or union column source. Actual {}",
                column_source_node->formatASTForErrorMessage());

        bool column_already_exists = table_expression_data.hasColumn(column_node->getColumnName());
        if (column_already_exists)
            return;

        auto column_identifier = planner_context.getGlobalPlannerContext()->createColumnIdentifier(node);
        table_expression_data.addColumn(column_node->getColumn(), column_identifier);
    }

    static bool needChildVisit(const QueryTreeNodePtr & parent, const QueryTreeNodePtr & child_node)
    {
        if (auto * join_node = parent->as<JoinNode>())
        {
            if (join_node->getJoinExpression() == child_node && join_node->isUsingJoinExpression())
                return false;
        }
        auto child_node_type = child_node->getNodeType();
        return !(child_node_type == QueryTreeNodeType::QUERY || child_node_type == QueryTreeNodeType::UNION);
    }

private:

    void visitUsingColumn(QueryTreeNodePtr & node)
    {
        auto & column_node = node->as<ColumnNode &>();
        if (column_node.hasExpression())
        {
            auto & table_expression_data = planner_context.getOrCreateTableExpressionData(column_node.getColumnSource());
            bool column_already_exists = table_expression_data.hasColumn(column_node.getColumnName());
            if (column_already_exists)
                return;

            auto column_identifier = planner_context.getGlobalPlannerContext()->createColumnIdentifier(node);
            table_expression_data.addAliasColumnName(column_node.getColumnName(), column_identifier);

            visit(column_node.getExpressionOrThrow());
        }
        else
            visit(node);
    }

    PlannerContext & planner_context;
};

class CollectPrewhereTableExpressionVisitor : public ConstInDepthQueryTreeVisitor<CollectPrewhereTableExpressionVisitor>
{
public:
    explicit CollectPrewhereTableExpressionVisitor(const QueryTreeNodePtr & query_node_)
        : query_node(query_node_)
    {}

    const QueryTreeNodePtr & getPrewhereTableExpression() const
    {
        return table_expression;
    }

    void visitImpl(const QueryTreeNodePtr & node)
    {
        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        auto column_source = column_node->getColumnSourceOrNull();
        if (!column_source)
            throw Exception(ErrorCodes::ILLEGAL_PREWHERE,
                "Invalid column {} in PREWHERE. In query {}",
                column_node->formatASTForErrorMessage(),
                query_node->formatASTForErrorMessage());

        auto * table_column_source = column_source->as<TableNode>();
        auto * table_function_column_source = column_source->as<TableFunctionNode>();

        if (!table_column_source && !table_function_column_source)
            throw Exception(ErrorCodes::ILLEGAL_PREWHERE,
                "Invalid column {} in PREWHERE. Expected column source to be table or table function. Actual {}. In query {}",
                column_node->formatASTForErrorMessage(),
                column_source->formatASTForErrorMessage(),
                query_node->formatASTForErrorMessage());

        if (table_expression && table_expression.get() != column_source.get())
            throw Exception(ErrorCodes::ILLEGAL_PREWHERE,
                "Invalid column {} in PREWHERE. Expected columns from single table or table function {}. Actual {}. In query {}",
                column_node->formatASTForErrorMessage(),
                table_expression->formatASTForErrorMessage(),
                column_source->formatASTForErrorMessage(),
                query_node->formatASTForErrorMessage());

        if (!table_expression)
        {
            const auto & storage = table_column_source ? table_column_source->getStorage() : table_function_column_source->getStorage();
            if (!storage->supportsPrewhere())
                throw Exception(ErrorCodes::ILLEGAL_PREWHERE,
                    "Storage {} (table {}) does not support PREWHERE",
                    storage->getName(),
                    storage->getStorageID().getNameForLogs());

            table_expression = std::move(column_source);
            table_supported_prewhere_columns = storage->supportedPrewhereColumns();
        }

        if (table_supported_prewhere_columns && !table_supported_prewhere_columns->contains(column_node->getColumnName()))
            throw Exception(ErrorCodes::ILLEGAL_PREWHERE,
                "Table expression {} does not support column {} in PREWHERE. In query {}",
                table_expression->formatASTForErrorMessage(),
                column_node->formatASTForErrorMessage(),
                query_node->formatASTForErrorMessage());
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node)
    {
        auto child_node_type = child_node->getNodeType();
        return !(child_node_type == QueryTreeNodeType::QUERY || child_node_type == QueryTreeNodeType::UNION);
    }

private:
    QueryTreeNodePtr query_node;
    QueryTreeNodePtr table_expression;
    std::optional<NameSet> table_supported_prewhere_columns;
};

void checkStorageSupportPrewhere(const QueryTreeNodePtr & table_expression)
{
    if (auto * table_node = table_expression->as<TableNode>())
    {
        auto storage = table_node->getStorage();
        if (!storage->supportsPrewhere())
            throw Exception(ErrorCodes::ILLEGAL_PREWHERE,
                "Storage {} (table {}) does not support PREWHERE",
                storage->getName(),
                storage->getStorageID().getNameForLogs());
    }
    else if (auto * table_function_node = table_expression->as<TableFunctionNode>())
    {
        auto storage = table_function_node->getStorage();
        if (!storage->supportsPrewhere())
            throw Exception(ErrorCodes::ILLEGAL_PREWHERE,
                "Table function storage {} (table {}) does not support PREWHERE",
                storage->getName(),
                storage->getStorageID().getNameForLogs());
    }
    else
    {
        throw Exception(ErrorCodes::ILLEGAL_PREWHERE,
            "Subquery {} does not support PREWHERE",
            table_expression->formatASTForErrorMessage());
    }
}

}

void collectTableExpressionData(QueryTreeNodePtr & query_node, PlannerContextPtr & planner_context)
{
    auto & query_node_typed = query_node->as<QueryNode &>();
    auto table_expressions_nodes = extractTableExpressions(query_node_typed.getJoinTree());

    for (auto & table_expression_node : table_expressions_nodes)
    {
        auto & table_expression_data = planner_context->getOrCreateTableExpressionData(table_expression_node);

        if (auto * table_node = table_expression_node->as<TableNode>())
        {
            bool storage_is_remote = table_node->getStorage()->isRemote();
            bool storage_is_merge_tree = table_node->getStorage()->isMergeTree();
            table_expression_data.setIsRemote(storage_is_remote);
            table_expression_data.setIsMergeTree(storage_is_merge_tree);
        }
        else if (auto * table_function_node = table_expression_node->as<TableFunctionNode>())
        {
            bool storage_is_remote = table_function_node->getStorage()->isRemote();
            bool storage_is_merge_tree = table_function_node->getStorage()->isMergeTree();
            table_expression_data.setIsRemote(storage_is_remote);
            table_expression_data.setIsMergeTree(storage_is_merge_tree);
        }
    }

    CollectSourceColumnsVisitor collect_source_columns_visitor(*planner_context);

    /** Visit the join nodes first to ensure that all columns in subtrees are collected before the projection node is visited.
      * This is crucial for column nodes that originate from the JOIN USING clause.
      * For example:
      * SELECT a FROM t1 JOIN t2 USING a ORDER BY a;
      * In this case, if `a` is an ALIAS column, the expression for it will be calculated from the USING clause.
      * Therefore, the table expression data for t1 and t2 should be prepared before visiting the `a` column node in projection or ORDER BY.
      */
    auto table_expression_stack = buildTableExpressionsStack(query_node_typed.getJoinTree());
    for (auto & table_expression_node : table_expression_stack)
    {
        if (table_expression_node->getNodeType() == QueryTreeNodeType::JOIN)
            collect_source_columns_visitor.visit(table_expression_node);
    }

    for (auto & node : query_node_typed.getChildren())
    {
        if (!node || node == query_node_typed.getPrewhere())
            continue;

        auto node_type = node->getNodeType();
        if (node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION || node_type == QueryTreeNodeType::JOIN)
            continue;

        collect_source_columns_visitor.visit(node);
    }

    if (query_node_typed.hasPrewhere())
    {
        CollectPrewhereTableExpressionVisitor collect_prewhere_table_expression_visitor(query_node);
        collect_prewhere_table_expression_visitor.visit(query_node_typed.getPrewhere());

        auto prewhere_table_expression = collect_prewhere_table_expression_visitor.getPrewhereTableExpression();
        if (!prewhere_table_expression)
        {
            prewhere_table_expression = table_expressions_nodes[0];
            checkStorageSupportPrewhere(prewhere_table_expression);
        }

        auto & table_expression_data = planner_context->getOrCreateTableExpressionData(prewhere_table_expression);
        const auto & column_names = table_expression_data.getColumnNames();
        NameSet required_column_names_without_prewhere(column_names.begin(), column_names.end());

        collect_source_columns_visitor.visit(query_node_typed.getPrewhere());

        auto prewhere_actions_dag = std::make_shared<ActionsDAG>();

        PlannerActionsVisitor visitor(planner_context, false /*use_column_identifier_as_action_node_name*/);
        auto expression_nodes = visitor.visit(prewhere_actions_dag, query_node_typed.getPrewhere());
        if (expression_nodes.size() != 1)
            throw Exception(ErrorCodes::ILLEGAL_PREWHERE,
                "Invalid PREWHERE. Expected single boolean expression. In query {}",
                query_node->formatASTForErrorMessage());

        prewhere_actions_dag->getOutputs().push_back(expression_nodes[0]);

        for (const auto & prewhere_input_node : prewhere_actions_dag->getInputs())
            if (required_column_names_without_prewhere.contains(prewhere_input_node->result_name))
                prewhere_actions_dag->getOutputs().push_back(prewhere_input_node);

        table_expression_data.setPrewhereFilterActions(std::move(prewhere_actions_dag));
    }
}

void collectSourceColumns(QueryTreeNodePtr & expression_node, PlannerContextPtr & planner_context)
{
    CollectSourceColumnsVisitor collect_source_columns_visitor(*planner_context);
    collect_source_columns_visitor.visit(expression_node);
}

}
