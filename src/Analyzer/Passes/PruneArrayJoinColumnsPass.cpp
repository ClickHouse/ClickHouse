#include <Analyzer/Passes/PruneArrayJoinColumnsPass.h>

#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

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

    /// Filled after pruning: the column type of the ARRAY JOIN expression, which references must adopt.
    DataTypePtr pruned_type;

    /// Filled after pruning: old 1-based tupleElement index -> new 1-based index.
    std::unordered_map<UInt64, UInt64> index_remap;

    bool hasNested() const { return !nested_subcolumn_names.empty(); }

    bool isUsed() const { return fully_used || !used_subcolumns.empty(); }
};

struct ArrayJoinUsage
{
    ArrayJoinNode * array_join_node = nullptr;

    /// Keyed by the ARRAY JOIN column name.
    std::unordered_map<std::string, ExpressionUsage> expressions;
};

/// Keyed by ArrayJoinNode raw pointer.
using ArrayJoinUsageMap = std::unordered_map<const IQueryTreeNode *, ArrayJoinUsage>;

/// Returns the usage entry of a ColumnNode that references a tracked ARRAY JOIN expression, or nullptr.
ExpressionUsage * findReferencedExpression(const ColumnNode & column_node, ArrayJoinUsageMap & usage_map)
{
    auto source = column_node.getColumnSourceOrNull();
    if (!source)
        return nullptr;

    auto usage_it = usage_map.find(source.get());
    if (usage_it == usage_map.end())
        return nullptr;

    auto expr_it = usage_it->second.expressions.find(column_node.getColumnName());
    if (expr_it == usage_it->second.expressions.end())
        return nullptr;

    return &expr_it->second;
}

/// Collects every ARRAY JOIN node of the tree, including those inside subqueries, CTEs and UNION branches.
class CollectArrayJoinNodesVisitor : public InDepthQueryTreeVisitorWithContext<CollectArrayJoinNodesVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<CollectArrayJoinNodesVisitor>;

    CollectArrayJoinNodesVisitor(ContextPtr context_, ArrayJoinUsageMap & usage_map_, std::unordered_set<const IQueryTreeNode *> & definition_nodes_)
        : Base(std::move(context_))
        , usage_map(usage_map_)
        , definition_nodes(definition_nodes_)
    {
    }

    void enterImpl(const QueryTreeNodePtr & node)
    {
        auto * array_join_node = node->as<ArrayJoinNode>();
        if (!array_join_node)
            return;

        /// Unaligned ARRAY JOIN pads shorter arrays, so removing an array can change the number of result rows.
        if (getSettings()[Setting::enable_unaligned_array_join])
            return;

        ArrayJoinUsage usage;
        usage.array_join_node = array_join_node;

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

            definition_nodes.insert(join_expr.get());
            usage.expressions[column_node->getColumnName()] = std::move(expr_usage);
        }

        if (!usage.expressions.empty())
            usage_map[node.get()] = std::move(usage);
    }

private:
    ArrayJoinUsageMap & usage_map;
    std::unordered_set<const IQueryTreeNode *> & definition_nodes;
};

/// Marks which ARRAY JOIN expressions and nested subcolumns are referenced anywhere in the tree.
class MarkUsedArrayJoinColumnsVisitor : public InDepthQueryTreeVisitorWithContext<MarkUsedArrayJoinColumnsVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<MarkUsedArrayJoinColumnsVisitor>;

    MarkUsedArrayJoinColumnsVisitor(ContextPtr context_, ArrayJoinUsageMap & usage_map_, const std::unordered_set<const IQueryTreeNode *> & definition_nodes_)
        : Base(std::move(context_))
        , usage_map(usage_map_)
        , definition_nodes(definition_nodes_)
    {
    }

    bool needChildVisit(QueryTreeNodePtr & parent, QueryTreeNodePtr & child [[maybe_unused]])
    {
        /// tupleElement(array_join_column, ...) is handled as a whole in enterImpl:
        /// visiting the column child would mark the expression as fully used.
        auto * function_node = parent->as<FunctionNode>();
        if (function_node && function_node->getFunctionName() == "tupleElement")
        {
            const auto & arguments = function_node->getArguments().getNodes();
            if (arguments.size() >= 2)
            {
                if (auto * column_node = arguments[0]->as<ColumnNode>())
                {
                    if (findReferencedExpression(*column_node, usage_map))
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

            auto * expr_usage = findReferencedExpression(*column_node, usage_map);
            if (!expr_usage || expr_usage->fully_used)
                return;

            if (expr_usage->hasNested())
            {
                const auto & value = constant_node->getValue();
                if (value.getType() == Field::Types::String)
                {
                    expr_usage->used_subcolumns.insert(value.safeGet<String>());
                }
                else if (value.getType() == Field::Types::UInt64)
                {
                    /// tupleElement uses 1-based indexing.
                    UInt64 index = value.safeGet<UInt64>();
                    if (index >= 1 && index <= expr_usage->nested_subcolumn_names.size())
                        expr_usage->used_subcolumns.insert(expr_usage->nested_subcolumn_names[index - 1]);
                    else
                        expr_usage->fully_used = true;
                }
                else
                {
                    expr_usage->fully_used = true;
                }
            }
            else
                expr_usage->fully_used = true;

            return;
        }

        /// Case 2: direct reference to an ARRAY JOIN column — mark fully used.
        /// The ARRAY JOIN expression itself is a ColumnNode with the same name and source, but it is
        /// the definition, not a reference. Its expression is still visited, because it may reference
        /// columns of a preceding ARRAY JOIN or contain a subquery with its own ARRAY JOIN.
        if (definition_nodes.contains(node.get()))
            return;

        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        if (auto * expr_usage = findReferencedExpression(*column_node, usage_map))
            expr_usage->fully_used = true;
    }

private:
    ArrayJoinUsageMap & usage_map;
    const std::unordered_set<const IQueryTreeNode *> & definition_nodes;
};

/// Prune unused subcolumn arguments from a nested() function and build an index remap
/// so that numeric tupleElement indices can be updated to reflect the new positions.
void pruneNestedFunctionArguments(
    ColumnNode & column_node,
    FunctionNode & function_node,
    ExpressionUsage & expr_usage,
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

    /// Build old 1-based index → new 1-based index remap.
    for (size_t new_idx = 0; new_idx < indices_to_keep.size(); ++new_idx)
        expr_usage.index_remap[indices_to_keep[new_idx] + 1] = new_idx + 1;

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

/// Updates reference ColumnNode types, rewrites stale numeric tupleElement
/// indices, and re-resolves tupleElement functions after nested() arguments have been pruned.
class UpdateArrayJoinReferenceTypesVisitor : public InDepthQueryTreeVisitorWithContext<UpdateArrayJoinReferenceTypesVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<UpdateArrayJoinReferenceTypesVisitor>;

    UpdateArrayJoinReferenceTypesVisitor(ContextPtr context_, ArrayJoinUsageMap & usage_map_)
        : Base(std::move(context_))
        , usage_map(usage_map_)
    {
    }

    void enterImpl(const QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "tupleElement")
            return;

        auto & arguments = function_node->getArguments().getNodes();
        if (arguments.size() < 2)
            return;

        auto * column_node = arguments[0]->as<ColumnNode>();
        if (!column_node)
            return;

        auto * expr_usage = findReferencedExpression(*column_node, usage_map);
        if (!expr_usage || !expr_usage->pruned_type)
            return;

        /// Rewrite numeric tupleElement index if pruning changed positions.
        auto * constant_node = arguments[1]->as<ConstantNode>();
        if (constant_node)
        {
            const auto & value = constant_node->getValue();
            if (value.getType() == Field::Types::UInt64)
            {
                UInt64 old_index = value.safeGet<UInt64>();
                auto remap_it = expr_usage->index_remap.find(old_index);
                if (remap_it != expr_usage->index_remap.end() && remap_it->second != old_index)
                    arguments[1] = std::make_shared<ConstantNode>(remap_it->second);
            }
        }

        if (column_node->getColumnType()->equals(*expr_usage->pruned_type))
            return;

        column_node->setColumnType(expr_usage->pruned_type);

        auto tuple_element_function = FunctionFactory::instance().get("tupleElement", getContext());
        function_node->resolveAsFunction(tuple_element_function->build(function_node->getArgumentColumns()));
    }

private:
    ArrayJoinUsageMap & usage_map;
};

}

void PruneArrayJoinColumnsPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    /// Step 1: Find all ARRAY JOIN nodes and build the usage map.
    ArrayJoinUsageMap usage_map;
    std::unordered_set<const IQueryTreeNode *> definition_nodes;

    CollectArrayJoinNodesVisitor collector(context, usage_map, definition_nodes);
    collector.visit(query_tree_node);

    if (usage_map.empty())
        return;

    /// Step 2: Mark used expressions and subcolumns.
    MarkUsedArrayJoinColumnsVisitor visitor(context, usage_map, definition_nodes);
    visitor.visit(query_tree_node);

    /// Step 3: Prune.
    for (auto & [_, usage] : usage_map)
    {
        auto & expressions_usage = usage.expressions;
        auto & join_expressions = usage.array_join_node->getJoinExpressions().getNodes();

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
        for (const auto & join_expr : join_expressions)
        {
            auto * col_node = join_expr->as<ColumnNode>();
            if (!col_node)
                continue;

            auto expr_it = expressions_usage.find(col_node->getColumnName());
            if (expr_it != expressions_usage.end())
                expr_it->second.pruned_type = col_node->getColumnType();
        }
    }

    /// 3c: Update types of reference ColumnNodes, rewrite numeric indices, and re-resolve tupleElement functions.
    UpdateArrayJoinReferenceTypesVisitor type_updater(context, usage_map);
    type_updater.visit(query_tree_node);
}

}
