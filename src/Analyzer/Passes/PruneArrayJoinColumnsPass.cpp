#include <Analyzer/Passes/PruneArrayJoinColumnsPass.h>

#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>

#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>

namespace DB
{

namespace Setting
{

extern const SettingsBool enable_unaligned_array_join;

}

namespace
{

/// Per-expression usage state inside a single ARRAY JOIN node.
struct ExpressionUsage
{
    /// True when the expression is referenced directly (not via tupleElement),
    /// meaning all subcolumns are needed, or it has no nested() inner function.
    bool fully_used = false;

    /// Used subcolumn names (only meaningful for nested() expressions).
    std::unordered_set<std::string> used_subcolumns;

    /// Subcolumn names from the nested() first argument. Empty if not a nested() expression.
    std::vector<std::string> nested_subcolumn_names;

    bool hasNested() const { return !nested_subcolumn_names.empty(); }

    bool isUsed() const { return fully_used || !used_subcolumns.empty(); }
};

/// Key: (ArrayJoinNode raw ptr, column name) → ExpressionUsage.
using ArrayJoinUsageMap = std::unordered_map<const IQueryTreeNode *, std::unordered_map<std::string, ExpressionUsage>>;

/// Set of ArrayJoinNode raw pointers that we are tracking.
using ArrayJoinNodeSet = std::unordered_set<const IQueryTreeNode *>;

/// Map: (ArrayJoinNode raw ptr, column name) → post-pruning column DataType.
using UpdatedTypeMap = std::unordered_map<const IQueryTreeNode *, std::unordered_map<std::string, DataTypePtr>>;

/// Visitor that marks which ARRAY JOIN expressions and nested subcolumns are used.
class MarkUsedArrayJoinColumnsVisitor : public InDepthQueryTreeVisitorWithContext<MarkUsedArrayJoinColumnsVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<MarkUsedArrayJoinColumnsVisitor>;

    MarkUsedArrayJoinColumnsVisitor(ContextPtr context_, ArrayJoinUsageMap & usage_map_, const ArrayJoinNodeSet & tracked_nodes_)
        : Base(std::move(context_))
        , usage_map(usage_map_)
        , tracked_nodes(tracked_nodes_)
    {
    }

    bool needChildVisit(QueryTreeNodePtr & parent, QueryTreeNodePtr & child)
    {
        /// Skip visiting the join expressions list of a tracked ARRAY JOIN node —
        /// those are the expression definitions, not references.
        if (tracked_nodes.contains(parent.get()))
        {
            auto * array_join_node = parent->as<ArrayJoinNode>();
            if (array_join_node && child.get() == array_join_node->getJoinExpressionsNode().get())
                return false;
        }

        /// If the parent is tupleElement whose first argument is a column from a tracked
        /// ARRAY JOIN, skip visiting children — enterImpl already handles the tupleElement
        /// by marking only the specific subcolumn.
        auto * function_node = parent->as<FunctionNode>();
        if (function_node && function_node->getFunctionName() == "tupleElement")
        {
            const auto & arguments = function_node->getArguments().getNodes();
            if (arguments.size() >= 2)
            {
                if (auto * column_node = arguments[0]->as<ColumnNode>())
                {
                    auto source = column_node->getColumnSourceOrNull();
                    if (source && tracked_nodes.contains(source.get()))
                        return false;
                }
            }
        }

        return true;
    }

    void enterImpl(const QueryTreeNodePtr & node)
    {
        /// Case 1: tupleElement(array_join_col, 'subcolumn_name') — mark specific subcolumn.
        if (auto * function_node = node->as<FunctionNode>())
        {
            if (function_node->getFunctionName() != "tupleElement")
                return;

            const auto & arguments = function_node->getArguments().getNodes();
            if (arguments.size() < 2)
                return;

            auto * column_node = arguments[0]->as<ColumnNode>();
            auto * constant_node = arguments[1]->as<ConstantNode>();
            if (!column_node || !constant_node)
                return;

            auto source = column_node->getColumnSourceOrNull();
            if (!source || !tracked_nodes.contains(source.get()))
                return;

            auto map_it = usage_map.find(source.get());
            if (map_it == usage_map.end())
                return;

            auto expr_it = map_it->second.find(column_node->getColumnName());
            if (expr_it == map_it->second.end() || expr_it->second.fully_used)
                return;

            if (expr_it->second.hasNested())
            {
                const auto & value = constant_node->getValue();
                if (value.getType() == Field::Types::String)
                {
                    expr_it->second.used_subcolumns.insert(value.safeGet<String>());
                }
                else if (value.getType() == Field::Types::UInt64)
                {
                    /// tupleElement uses 1-based indexing.
                    UInt64 index = value.safeGet<UInt64>();
                    if (index >= 1 && index <= expr_it->second.nested_subcolumn_names.size())
                        expr_it->second.used_subcolumns.insert(expr_it->second.nested_subcolumn_names[index - 1]);
                    else
                        expr_it->second.fully_used = true;
                }
                else
                {
                    expr_it->second.fully_used = true;
                }
            }
            else
                expr_it->second.fully_used = true;

            return;
        }

        /// Case 2: direct reference to an ARRAY JOIN column — mark fully used.
        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        auto source = column_node->getColumnSourceOrNull();
        if (!source || !tracked_nodes.contains(source.get()))
            return;

        auto map_it = usage_map.find(source.get());
        if (map_it == usage_map.end())
            return;

        auto expr_it = map_it->second.find(column_node->getColumnName());
        if (expr_it != map_it->second.end())
            expr_it->second.fully_used = true;
    }

private:
    ArrayJoinUsageMap & usage_map;
    const ArrayJoinNodeSet & tracked_nodes;
};

void pruneNestedFunctionArguments(
    ColumnNode & column_node,
    FunctionNode & function_node,
    const ExpressionUsage & expr_usage,
    const ContextPtr & context)
{
    auto & nested_args = function_node.getArguments().getNodes();
    const auto & subcolumn_names = expr_usage.nested_subcolumn_names;
    size_t num_subcolumns = subcolumn_names.size();

    /// Find which indices to keep.
    std::vector<size_t> indices_to_keep;
    for (size_t i = 0; i < num_subcolumns; ++i)
    {
        if (expr_usage.used_subcolumns.contains(subcolumn_names[i]))
            indices_to_keep.push_back(i);
    }

    /// Nothing to prune.
    if (indices_to_keep.size() == num_subcolumns)
        return;

    /// Keep at least one subcolumn so the expression remains valid.
    if (indices_to_keep.empty())
        indices_to_keep.push_back(0);

    /// Build pruned names array and arguments.
    Array pruned_names_array;
    QueryTreeNodes pruned_args;
    pruned_names_array.reserve(indices_to_keep.size());
    pruned_args.reserve(indices_to_keep.size() + 1);

    for (size_t idx : indices_to_keep)
        pruned_names_array.push_back(subcolumn_names[idx]);

    auto pruned_names_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    pruned_args.push_back(std::make_shared<ConstantNode>(std::move(pruned_names_array), std::move(pruned_names_type)));

    for (size_t idx : indices_to_keep)
        pruned_args.push_back(nested_args[idx + 1]); /// +1: first arg is the names array.

    nested_args = std::move(pruned_args);

    /// Re-resolve the function to update its return type.
    auto nested_function = FunctionFactory::instance().get("nested", context);
    function_node.resolveAsFunction(nested_function->build(function_node.getArgumentColumns()));

    /// Update the ARRAY JOIN column node's type to match the new result.
    auto new_result_type = function_node.getResultType();
    auto new_column_type = assert_cast<const DataTypeArray &>(*new_result_type).getNestedType();
    column_node.setColumnType(std::move(new_column_type));
}

/// Visitor that updates reference ColumnNode types and re-resolves tupleElement functions
/// after nested() arguments have been pruned.
class UpdateArrayJoinReferenceTypesVisitor : public InDepthQueryTreeVisitorWithContext<UpdateArrayJoinReferenceTypesVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<UpdateArrayJoinReferenceTypesVisitor>;

    UpdateArrayJoinReferenceTypesVisitor(
        ContextPtr context_,
        const UpdatedTypeMap & updated_types_,
        const ArrayJoinNodeSet & tracked_nodes_)
        : Base(std::move(context_))
        , updated_types(updated_types_)
        , tracked_nodes(tracked_nodes_)
    {
    }

    void enterImpl(const QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "tupleElement")
            return;

        const auto & arguments = function_node->getArguments().getNodes();
        if (arguments.size() < 2)
            return;

        auto * column_node = arguments[0]->as<ColumnNode>();
        if (!column_node)
            return;

        auto source = column_node->getColumnSourceOrNull();
        if (!source || !tracked_nodes.contains(source.get()))
            return;

        auto node_it = updated_types.find(source.get());
        if (node_it == updated_types.end())
            return;

        auto type_it = node_it->second.find(column_node->getColumnName());
        if (type_it == node_it->second.end())
            return;

        const auto & new_type = type_it->second;
        if (column_node->getColumnType()->equals(*new_type))
            return;

        column_node->setColumnType(new_type);

        auto tuple_element_function = FunctionFactory::instance().get("tupleElement", getContext());
        function_node->resolveAsFunction(tuple_element_function->build(function_node->getArgumentColumns()));
    }

private:
    const UpdatedTypeMap & updated_types;
    const ArrayJoinNodeSet & tracked_nodes;
};

}

void PruneArrayJoinColumnsPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    auto * top_query_node = query_tree_node->as<QueryNode>();
    if (!top_query_node)
        return;

    const auto & settings = context->getSettingsRef();
    if (settings[Setting::enable_unaligned_array_join])
        return;

    /// Step 1: Find all ARRAY JOIN nodes and build the usage map.
    ArrayJoinUsageMap usage_map;
    ArrayJoinNodeSet tracked_nodes;

    auto table_expressions = extractTableExpressions(top_query_node->getJoinTree(), /*add_array_join=*/ true);
    for (const auto & table_expr : table_expressions)
    {
        auto * array_join_node = table_expr->as<ArrayJoinNode>();
        if (!array_join_node)
            continue;

        auto & expressions_usage = usage_map[table_expr.get()];

        for (const auto & join_expr : array_join_node->getJoinExpressions().getNodes())
        {
            auto * column_node = join_expr->as<ColumnNode>();
            if (!column_node || !column_node->hasExpression())
                continue;

            ExpressionUsage expr_usage;

            auto * function_node = column_node->getExpression()->as<FunctionNode>();
            if (function_node && function_node->getFunctionName() == "nested")
            {
                const auto & args = function_node->getArguments().getNodes();
                if (args.size() >= 2)
                {
                    if (auto * names_constant = args[0]->as<ConstantNode>())
                    {
                        const auto & names_array = names_constant->getValue().safeGet<Array>();
                        for (const auto & name : names_array)
                            expr_usage.nested_subcolumn_names.push_back(name.safeGet<String>());
                    }
                }
            }

            expressions_usage[column_node->getColumnName()] = std::move(expr_usage);
        }

        if (!expressions_usage.empty())
            tracked_nodes.insert(table_expr.get());
    }

    if (tracked_nodes.empty())
        return;

    /// Step 2: Mark used expressions and subcolumns.
    MarkUsedArrayJoinColumnsVisitor visitor(context, usage_map, tracked_nodes);
    visitor.visit(query_tree_node);

    /// Step 3: Prune.
    UpdatedTypeMap updated_types;

    for (auto & [node_ptr, expressions_usage] : usage_map)
    {
        /// Find the ArrayJoinNode among our table_expressions.
        ArrayJoinNode * array_join_node = nullptr;
        for (const auto & te : table_expressions)
        {
            if (te.get() == node_ptr)
            {
                array_join_node = te->as<ArrayJoinNode>();
                break;
            }
        }
        if (!array_join_node)
            continue;

        auto & join_expressions = array_join_node->getJoinExpressions().getNodes();

        /// 3a: Remove entire unused ARRAY JOIN expressions.
        {
            QueryTreeNodes kept;
            kept.reserve(join_expressions.size());

            for (auto & join_expr : join_expressions)
            {
                auto * column_node = join_expr->as<ColumnNode>();
                if (!column_node)
                {
                    kept.push_back(std::move(join_expr));
                    continue;
                }

                auto expr_it = expressions_usage.find(column_node->getColumnName());
                if (expr_it == expressions_usage.end() || expr_it->second.isUsed())
                    kept.push_back(std::move(join_expr));
            }

            /// Keep at least one expression to preserve row multiplication.
            if (kept.empty() && !join_expressions.empty())
                kept.push_back(std::move(join_expressions[0]));

            join_expressions = std::move(kept);
        }

        /// 3b: Prune unused nested() subcolumn arguments.
        for (auto & join_expr : join_expressions)
        {
            auto * column_node = join_expr->as<ColumnNode>();
            if (!column_node || !column_node->hasExpression())
                continue;

            auto expr_it = expressions_usage.find(column_node->getColumnName());
            if (expr_it == expressions_usage.end())
                continue;

            auto & expr_usage = expr_it->second;
            if (expr_usage.fully_used || !expr_usage.hasNested())
                continue;

            auto * function_node = column_node->getExpression()->as<FunctionNode>();
            if (!function_node)
                continue;

            pruneNestedFunctionArguments(*column_node, *function_node, expr_usage, context);
        }

        /// Collect post-pruning types for step 3c.
        auto & type_map = updated_types[node_ptr];
        for (const auto & join_expr : join_expressions)
        {
            auto * col_node = join_expr->as<ColumnNode>();
            if (!col_node)
                continue;
            type_map[col_node->getColumnName()] = col_node->getColumnType();
        }
    }

    /// 3c: Update types of reference ColumnNodes and re-resolve tupleElement functions.
    UpdateArrayJoinReferenceTypesVisitor type_updater(context, updated_types, tracked_nodes);
    type_updater.visit(query_tree_node);
}

}
