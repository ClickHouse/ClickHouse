#include <Analyzer/Passes/HybridCastsPass.h>

#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/Utils.h>
#include <Analyzer/Resolve/IdentifierResolver.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

#include <Storages/IStorage.h>
#include <Storages/StorageDistributed.h>

#include <Core/Settings.h>
#include <Core/SettingsEnums.h>
#include <Common/Exception.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool hybrid_table_auto_cast_columns;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Collect Hybrid table expressions that require casts to normalize headers across segments.
///
/// Hybrid is currently exposed only as an engine (TableNode). If it ever gets a table function
/// wrapper, this visitor must also look at TableFunctionNode and unwrap to the underlying
/// StorageDistributed so cached casts can be picked up there as well.
class HybridCastTablesCollector : public InDepthQueryTreeVisitor<HybridCastTablesCollector>
{
public:
    explicit HybridCastTablesCollector(std::unordered_map<const IQueryTreeNode *, ColumnsDescription> & cast_map_)
        : cast_map(cast_map_)
    {}

    static bool needChildVisit(QueryTreeNodePtr &, QueryTreeNodePtr &) { return true; }

    void visitImpl(QueryTreeNodePtr & node)
    {
        const auto * table = node->as<TableNode>();
        if (!table)
            return;

        const auto * storage = table->getStorage().get();
        if (const auto * distributed = typeid_cast<const StorageDistributed *>(storage))
        {
            ColumnsDescription to_cast = distributed->getColumnsToCast();
            if (!to_cast.empty())
                cast_map.emplace(node.get(), std::move(to_cast)); // repeated table_expression can overwrite
        }
    }

private:
    std::unordered_map<const IQueryTreeNode *, ColumnsDescription> & cast_map;
};

// Visitor replaces all usages of the column with CAST(column, type) in the query tree.
class HybridCastVisitor : public InDepthQueryTreeVisitor<HybridCastVisitor>
{
public:
    HybridCastVisitor(
        const std::unordered_map<const IQueryTreeNode *, ColumnsDescription> & cast_map_,
        ContextPtr context_)
        : cast_map(cast_map_)
        , context(std::move(context_))
    {}

    bool shouldTraverseTopToBottom() const { return false; }

    static bool needChildVisit(QueryTreeNodePtr &, QueryTreeNodePtr & child)
    {
        /// Traverse all child nodes so casts also apply inside subqueries and UNION branches.
        (void)child;
        return true;
    }

    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        auto column_source = column_node->getColumnSourceOrNull();
        if (!column_source)
            return;

        auto it = cast_map.find(column_source.get());
        if (it == cast_map.end())
            return;

        const auto & column_name = column_node->getColumnName();
        auto expected_column_opt = it->second.tryGetPhysical(column_name);
        if (!expected_column_opt)
            return;

        auto column_clone = std::static_pointer_cast<ColumnNode>(column_node->clone());

        auto cast_node = buildCastFunction(column_clone, expected_column_opt->type, context);
        const auto & alias = node->getAlias();
        if (!alias.empty())
            cast_node->setAlias(alias);
        else
            cast_node->setAlias(expected_column_opt->name);

        node = cast_node;
    }

private:
    const std::unordered_map<const IQueryTreeNode *, ColumnsDescription> & cast_map;
    ContextPtr context;
};


} // namespace

void HybridCastsPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    const auto & settings = context->getSettingsRef();
    if (!settings[Setting::hybrid_table_auto_cast_columns])
        return;

    auto * query = query_tree_node->as<QueryNode>();
    if (!query)
        return;

    std::unordered_map<const IQueryTreeNode *, ColumnsDescription> cast_map;
    HybridCastTablesCollector collector(cast_map);
    collector.visit(query_tree_node);
    if (cast_map.empty())
        return;

    HybridCastVisitor visitor(cast_map, context);
    visitor.visit(query_tree_node);
}

}
