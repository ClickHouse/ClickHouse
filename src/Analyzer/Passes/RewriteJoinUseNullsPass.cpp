#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <Analyzer/Passes/RewriteJoinUseNullsPass.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Functions/FunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
namespace
{
class CollectTableRequiredColumnsVisitor : public InDepthQueryTreeVisitorWithContext<CollectTableRequiredColumnsVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<CollectTableRequiredColumnsVisitor>;
    using Base::Base;
    CollectTableRequiredColumnsVisitor(std::unordered_map<QueryTreeNodePtr, std::unordered_set<QueryTreeNodePtr>> & source_requiredColumns_, ContextPtr context_)
        : Base(context_)
        , source_requiredColumns(source_requiredColumns_)
    {
    }
    void enterImpl(VisitQueryTreeNodeType & node)
    {
        auto * col_node = node->as<ColumnNode>();
        if (!col_node)
            return;
        auto table_node = col_node->getColumnSource();
        source_requiredColumns[table_node].insert(node);
    }
private:
    std::unordered_map<QueryTreeNodePtr, std::unordered_set<QueryTreeNodePtr>> & source_requiredColumns;
};

// Add a projection of converting columns from left/right table to nullable.
class RewriteJoinUseNullsTableExpressionVisitor : public InDepthQueryTreeVisitorWithContext<RewriteJoinUseNullsTableExpressionVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<RewriteJoinUseNullsTableExpressionVisitor>;
    using Base::Base;
    RewriteJoinUseNullsTableExpressionVisitor(const std::unordered_map<QueryTreeNodePtr, std::unordered_set<QueryTreeNodePtr>> & source_requiredColumns_, ContextPtr context_)
        : Base(context_)
        , source_requiredColumns(source_requiredColumns_)
    {}

    void leaveImpl(VisitQueryTreeNodeType & node)
    {
        auto * join_node = node->as<JoinNode>();
        if (!join_node)
            return;

        if (join_node->getKind() == JoinKind::Full)
        {
            auto & left_table_expression = join_node->getLeftTableExpression();
            convertTableReturnColumnsToNullable(left_table_expression);
            auto & right_table_expression = join_node->getRightTableExpression();
            convertTableReturnColumnsToNullable(right_table_expression);
        }
        else if (join_node->getKind() == JoinKind::Left)
        {
            auto & right_table_expression = join_node->getRightTableExpression();
            convertTableReturnColumnsToNullable(right_table_expression);
        }
        else if (join_node->getKind() == JoinKind::Right)
        {
            auto & left_table_expression = join_node->getLeftTableExpression();
            convertTableReturnColumnsToNullable(left_table_expression);
        }
    }
private:
    const std::unordered_map<QueryTreeNodePtr, std::unordered_set<QueryTreeNodePtr>> & source_requiredColumns;

    void convertTableReturnColumnsToNullable(QueryTreeNodePtr & node)
    {
        if (auto * table_node = node->as<TableNode>())
        {
            convertTableNodeColumnsToNullable(node);
        }
        else if (auto * query_node = node->as<QueryNode>())
        {
            convertQueryColumnsToNullable(node);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknow table type in join: {}", node->dumpTree());
        }
    }

    void toNullableColumnProject(QueryTreeNodePtr & node, bool force)
    {
        auto type = node->getResultType();
        if (type->isNullable())
        {
            if (!force)
                return;
            type = typeid_cast<const DataTypeNullable *>(type.get())->getNestedType();
        }
        if (const auto * type_to_check_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type.get()))
            type = type_to_check_low_cardinality->getDictionaryType();

        if (type->canBeInsideNullable())
        {
            auto nullable_function_resolver = FunctionFactory::instance().get("toNullable", getContext());
            auto to_nullable = std::make_shared<FunctionNode>("toNullable");
            to_nullable->getArguments().getNodes().push_back(node);
            to_nullable->resolveAsFunction(nullable_function_resolver);
            to_nullable->convertToNullable();
            node = to_nullable;
        }
    }

    // If it's a table. collect all the required columns, and build a new subquery to project the required columns to nullable.
    void convertTableNodeColumnsToNullable(QueryTreeNodePtr & node)
    {
        auto * table_node = node->as<TableNode>();
        auto required_cols_it = source_requiredColumns.find(node);
        if (required_cols_it == source_requiredColumns.end())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Not found required columns for {}", table_node->getStorageID());
        }
        auto query_node = std::make_shared<QueryNode>(Context::createCopy(getContext()));
        query_node->setIsSubquery(true);
        auto & projected_cols = query_node->getProjection().getNodes();
        const std::unordered_set<QueryTreeNodePtr> & required_cols = required_cols_it->second;
        NamesAndTypes projectionColumns;
        std::unordered_set<String> has_projected_cols;
        for (auto node_it = required_cols.begin(); node_it != required_cols.end(); ++node_it)
        {
            auto * col_node = (*node_it)->as<ColumnNode>();
            if (!col_node)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Not a column node");
            col_node->setColumnSource(query_node);

            auto copy_node = col_node->clone();
            col_node->convertToNullable();

            if (has_projected_cols.contains(col_node->getColumnName()))
                continue;

            copy_node->as<ColumnNode>()->setColumnSource(node);
            toNullableColumnProject(copy_node, true);
            projected_cols.push_back(copy_node);
            projectionColumns.emplace_back(col_node->getColumnName(), copy_node->getResultType());

            has_projected_cols.insert(col_node->getColumnName());
        }
        query_node->resolveProjectionColumns(projectionColumns);
        query_node->setAlias(node->getAlias() + "nullable");
        query_node->getJoinTree() = node;
        node = query_node;
    }

    // If it's a subquery, only convert the non-nullable columns to nullable.
    void convertQueryColumnsToNullable(QueryTreeNodePtr & node)
    {
        auto * query_node = node->as<QueryNode>();
        auto & projection = query_node->getProjection();
        auto & projection_nodes = projection.getNodes();
        for (auto & projection_node : projection_nodes)
        {
            toNullableColumnProject(projection_node, false);
        }
    }

};
}

/**
  * Add a projection of converting columns from left/right table to nullable. For example
  *    SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON t1.a = t2.b Settings join_use_nulls = 1
  * will be rewritten to
  *    SELECT t1.*, t2.* FROM t1 LEFT JOIN (SELECT toNullable(t2.a) AS a, toNullable(t2.b) AS b FROM t2) AS t2 ON t1.a = t2.b Settings join_use_nulls = 1
  */
void RewriteJoinUseNullsPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    if (context->getSettingsRef().join_use_nulls)
    {
        LOG_TRACE(getLogger("RewriteJoinUseNullsPass"), "input query tree:\n{}", query_tree_node->dumpTree());
        std::unordered_map<QueryTreeNodePtr, std::unordered_set<QueryTreeNodePtr>> result_requiredColumns;
        /// Collect required columns for each table expression
        CollectTableRequiredColumnsVisitor table_required_columns_visitor(result_requiredColumns, context);
        table_required_columns_visitor.visit(query_tree_node);
        /// Rewrite table expressions to add a projection of converting columns to nullable if needed.
        RewriteJoinUseNullsTableExpressionVisitor table_expression_visitor(result_requiredColumns, context);
        table_expression_visitor.visit(query_tree_node);
        LOG_TRACE(getLogger("RewriteJoinUseNullsPass"), "Query tree after RewriteJoinUseNullsTableExpressionVisitor:\n{}", query_tree_node->dumpTree());
    }
}
}
