#include <Planner/CollectTableExpressionData.h>

#include <Storages/IStorage.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>

#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerContext.h>
#include <Planner/PlannerCorrelatedSubqueries.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_PREWHERE;
}

namespace
{

class CollectSourceColumnsVisitor : public InDepthQueryTreeVisitorWithContext<CollectSourceColumnsVisitor>
{
public:
    explicit CollectSourceColumnsVisitor(PlannerContextPtr & planner_context_, bool keep_alias_columns_ = true)
        : InDepthQueryTreeVisitorWithContext(planner_context_->getQueryContext())
        , planner_context(planner_context_)
        , keep_alias_columns(keep_alias_columns_)
    {
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (isIndexHintFunction(node))
        {
            is_inside_index_hint_function = true;
            return;
        }

        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        if (column_node->getColumnName() == "__grouping_set")
            return;

        /// A special case for the "indexHint" function. We don't need its arguments for execution if column's source table is MergeTree.
        /// Instead, we prepare an ActionsDAG for its arguments and store it inside a function (see ActionsDAG::buildFilterActionsDAG).
        /// So this optimization allows not to read arguments of "indexHint" (if not needed in other contexts) but only to use index analysis for them.
        if (is_inside_index_hint_function && isColumnSourceMergeTree(*column_node))
            return;

        auto column_source_node = column_node->getColumnSource();
        auto column_source_node_type = column_source_node->getNodeType();

        if (column_source_node_type == QueryTreeNodeType::LAMBDA || column_source_node_type == QueryTreeNodeType::INTERPOLATE)
            return;

        /// JOIN using expression
        if (column_node->hasExpression() && column_source_node_type == QueryTreeNodeType::JOIN)
        {
            auto & columns_from_subtrees = column_node->getExpression()->as<ListNode &>().getNodes();
            visit(columns_from_subtrees[0]);
            visit(columns_from_subtrees[1]);
            return;
        }

        auto & table_expression_data = planner_context->getOrCreateTableExpressionData(column_source_node);

        if (isAliasColumn(node))
        {
            /// Column is an ALIAS column with expression
            bool column_already_exists = table_expression_data.hasColumn(column_node->getColumnName());
            if (!column_already_exists)
            {
                CollectSourceColumnsVisitor visitor_for_alias_column(planner_context);
                /// While we are processing expression of ALIAS columns we should not add source columns to selected.
                /// See also comment for `select_added_columns`
                visitor_for_alias_column.select_added_columns = false;
                visitor_for_alias_column.keep_alias_columns = keep_alias_columns;
                visitor_for_alias_column.visit(column_node->getExpression());

                if (!keep_alias_columns)
                {
                    /// For PREWHERE we can just replace ALIAS column with it's expression,
                    /// because ActionsDAG for PREWHERE applied right on top of table expression
                    /// and cannot affect subqueries or other table expressions.
                    node = column_node->getExpression();
                    return;
                }

                auto column_identifier = planner_context->getGlobalPlannerContext()->createColumnIdentifier(node);

                ActionsDAG alias_column_actions_dag;
                ColumnNodePtrWithHashSet empty_correlated_columns_set;
                PlannerActionsVisitor actions_visitor(planner_context, empty_correlated_columns_set, false);
                auto [outputs, correlated_subtrees] = actions_visitor.visit(alias_column_actions_dag, column_node->getExpression());
                if (outputs.size() != 1)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Expected single output in actions dag for alias column {}. Actual {}", column_node->dumpTree(), outputs.size());
                if (correlated_subtrees.notEmpty())
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Correlated subquery in alias column expression {}. Actual {}", column_node->dumpTree(), outputs.size());

                auto & alias_node = outputs[0];
                const auto & column_name = column_node->getColumnName();
                alias_node = &alias_column_actions_dag.addAlias(*alias_node, column_name);

                alias_column_actions_dag.getOutputs() = std::move(outputs);
                table_expression_data.addAliasColumn(column_node->getColumn(), column_identifier, std::move(alias_column_actions_dag), select_added_columns);
            }
            else
                table_expression_data.markSelectedColumn(column_node->getColumn().name);

            return;
        }

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
        {
            /// Column may be added when we collected data for ALIAS column
            /// But now we see it directly in the query, so make sure it's marked as selected
            if (select_added_columns)
                table_expression_data.markSelectedColumn(column_node->getColumnName());
            return;
        }

        auto column_identifier = planner_context->getGlobalPlannerContext()->createColumnIdentifier(node);
        table_expression_data.addColumn(column_node->getColumn(), column_identifier, select_added_columns);
    }

    void leaveImpl(QueryTreeNodePtr & node)
    {
        if (isIndexHintFunction(node))
        {
            is_inside_index_hint_function = false;
            return;
        }
    }

    static bool isAliasColumn(const QueryTreeNodePtr & node)
    {
        const auto * column_node = node->as<ColumnNode>();
        if (!column_node || !column_node->hasExpression())
            return false;
        const auto & column_source = column_node->getColumnSourceOrNull();
        if (!column_source)
            return false;
        return column_source->getNodeType() != QueryTreeNodeType::JOIN &&
               column_source->getNodeType() != QueryTreeNodeType::CROSS_JOIN &&
               column_source->getNodeType() != QueryTreeNodeType::ARRAY_JOIN;
    }

    /// Check if query node is a subquery and add used in correlated subquery columns to the table expression data.
    /// These columns can be used only by correlated subquery, but still they
    /// must be read by query plan for current query.
    ///
    /// Example: SELECT 1 FROM table as t WHERE EXISTS (SELECT * FROM numbers(10) WHERE t.id = number);
    bool checkSubquery(const QueryTreeNodePtr & node)
    {
        auto node_type = node->getNodeType();
        switch (node_type)
        {
            case QueryTreeNodeType::QUERY:
            {
                auto * query_node = node->as<QueryNode>();
                chassert(query_node != nullptr);

                /// Register correlated columns for the query or union node.
                visit(query_node->getCorrelatedColumnsNode());
                return true;
            }
            case QueryTreeNodeType::UNION:
            {
                auto * union_node = node->as<UnionNode>();
                chassert(union_node != nullptr);

                visit(union_node->getCorrelatedColumnsNode());
                return true;
            }
            default:
                return false;
        }
    }

    bool needChildVisit(const QueryTreeNodePtr & parent_node, const QueryTreeNodePtr & child_node)
    {
        return !(checkSubquery(child_node) || isAliasColumn(parent_node));
    }

    static bool isIndexHintFunction(const QueryTreeNodePtr & node)
    {
        return node->as<FunctionNode>() && node->as<FunctionNode>()->getFunctionName() == "indexHint";
    }

    static bool isColumnSourceMergeTree(const ColumnNode & node)
    {
        const auto * source_table = node.getColumnSource()->as<TableNode>();
        return source_table && source_table->getStorage()->isMergeTree();
    }

    void setKeepAliasColumns(bool keep_alias_columns_)
    {
        keep_alias_columns = keep_alias_columns_;
    }

private:
    PlannerContextPtr & planner_context;

    /// Replace ALIAS columns with their expressions or register them in table expression data.
    /// Usually we can replace them when we build some "local" actions DAG
    /// (for example Row Policy or PREWHERE) that is applied on top of the table expression.
    /// In other cases, we keep ALIAS columns as ColumnNode with an expression child node,
    /// and handle them in the Planner by inserting ActionsDAG to compute them after reading from storage.
    bool keep_alias_columns = true;

    /// Flag `select_added_columns` indicates if we should mark column as explicitly selected.
    /// For example, for table with columns (a Int32, b ALIAS a+1) and query SELECT b FROM table
    /// Column `b` is selected explicitly by user, but not `a` (that is also read though).
    /// Distinguishing such columns is important for checking access rights for ALIAS columns.
    bool select_added_columns = true;

    /// True if we are traversing arguments of function "indexHint".
    bool is_inside_index_hint_function = false;
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

        const auto & storage = table_column_source ? table_column_source->getStorage() : table_function_column_source->getStorage();
        const auto & storage_snapshot = table_column_source ? table_column_source->getStorageSnapshot() : table_function_column_source->getStorageSnapshot();

        if (!table_expression)
        {
            if (!storage->supportsPrewhere())
                throw Exception(ErrorCodes::ILLEGAL_PREWHERE,
                    "Storage {} (table {}) does not support PREWHERE",
                    storage->getName(),
                    storage->getStorageID().getNameForLogs());

            table_expression = std::move(column_source);
            table_supported_prewhere_columns = storage->supportedPrewhereColumns();
        }

        const bool has_table_virtual_column =
            column_node->getColumnName() == "_table" && storage->isVirtualColumn(column_node->getColumnName(), storage_snapshot->metadata);

        if ((table_supported_prewhere_columns && !table_supported_prewhere_columns->contains(column_node->getColumnName())) || has_table_virtual_column)
            throw Exception(ErrorCodes::ILLEGAL_PREWHERE,
                "Table expression {} does not support column {} in PREWHERE. In query {}",
                table_expression->formatASTForErrorMessage(),
                column_node->formatASTForErrorMessage(),
                query_node->formatASTForErrorMessage());
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node)
    {
        auto child_node_type = child_node->getNodeType();
        return child_node_type != QueryTreeNodeType::QUERY &&
               child_node_type != QueryTreeNodeType::UNION &&
               child_node_type != QueryTreeNodeType::LAMBDA;
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

    CollectSourceColumnsVisitor collect_source_columns_visitor(planner_context);
    for (auto & node : query_node_typed.getChildren())
    {
        if (!node || node == query_node_typed.getPrewhere())
            continue;

        auto node_type = node->getNodeType();
        if (node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION)
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
        const auto & read_column_names = table_expression_data.getColumnNames();
        NameSet required_column_names_without_prewhere(read_column_names.begin(), read_column_names.end());
        const auto & selected_column_names = table_expression_data.getSelectedColumnsNames();
        required_column_names_without_prewhere.insert(selected_column_names.begin(), selected_column_names.end());

        collect_source_columns_visitor.setKeepAliasColumns(false);
        collect_source_columns_visitor.visit(query_node_typed.getPrewhere());

        ActionsDAG prewhere_actions_dag;

        QueryTreeNodePtr query_tree_node = query_node_typed.getPrewhere();
        auto correlated_columns_set = query_node_typed.getCorrelatedColumnsSet();

        PlannerActionsVisitor visitor(planner_context, /*correlated_columns_set_=*/correlated_columns_set, false /*use_column_identifier_as_action_node_name*/);
        auto [expression_nodes, correlated_subtrees] = visitor.visit(prewhere_actions_dag, query_tree_node);
        if (expression_nodes.size() != 1)
            throw Exception(ErrorCodes::ILLEGAL_PREWHERE,
                "Invalid PREWHERE. Expected single boolean expression. In query {}",
                query_node->formatASTForErrorMessage());
        if (correlated_subtrees.notEmpty())
            throw Exception(ErrorCodes::ILLEGAL_PREWHERE,
                "Correlated subqueries are not allowed in PREWHERE expression. In query {}",
                query_node->formatASTForErrorMessage());

        prewhere_actions_dag.getOutputs().push_back(expression_nodes.back());

        for (const auto & prewhere_input_node : prewhere_actions_dag.getInputs())
            if (required_column_names_without_prewhere.contains(prewhere_input_node->result_name))
                prewhere_actions_dag.getOutputs().push_back(prewhere_input_node);

        table_expression_data.setPrewhereFilterActions(std::move(prewhere_actions_dag));
    }
}

void collectSourceColumns(QueryTreeNodePtr & expression_node, PlannerContextPtr & planner_context, bool keep_alias_columns)
{
    CollectSourceColumnsVisitor collect_source_columns_visitor(planner_context, keep_alias_columns);
    collect_source_columns_visitor.visit(expression_node);
}

}
