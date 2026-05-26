#include <Analyzer/Passes/InjectRandomOrderIfNoOrderByPass.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/UnionNode.h>
#include <Core/Settings.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool inject_random_order_for_select_without_order_by;
}

/// Append ORDER BY rand() to an order-by list
void addRandomOrderBy(ListNode & order_by_list_node, ContextPtr context)
{
    /// Build rand() node
    auto & function_factory = FunctionFactory::instance();
    auto resolver = function_factory.get("rand", context);
    auto rand_function_node = std::make_shared<FunctionNode>("rand");
    rand_function_node->resolveAsFunction(resolver);

    /// Create and add ORDER BY rand() node
    auto sort_node = std::make_shared<SortNode>(rand_function_node);
    order_by_list_node.getNodes().push_back(std::move(sort_node));
}

/// Wrap query_root in new QueryNode that includes a random order by
void wrapWithSelectOrderBy(QueryTreeNodePtr & query_root, ContextPtr context)
{
    auto * query_node = query_root->as<QueryNode>();

    /// Original projection columns (with the user-visible names like `number`).
    auto subquery_projection_columns = query_node->getProjectionColumns();

    /// Internal unique aliases used to reference the inner columns from the wrapper.
    /// One alias per projection column so multi-column SELECTs work, and aliases are
    /// kept internal so output names (CTAS, VIEW, JSONEachRow, etc.) stay intact.
    Names unique_column_names;
    unique_column_names.reserve(subquery_projection_columns.size());
    const String uuid_suffix = toString(UUIDHelpers::generateV4());
    for (size_t i = 0; i < subquery_projection_columns.size(); ++i)
        unique_column_names.push_back("__subquery_column_" + std::to_string(i) + "_" + uuid_suffix);

    /// Re-resolve inner query columns with the unique internal aliases.
    query_node->clearProjectionColumns();
    query_node->setProjectionAliasesToOverride(unique_column_names);
    query_node->resolveProjectionColumns(subquery_projection_columns);
    query_node->setIsSubquery(true);

    /// Wrapper: SELECT <inner columns referenced by UUID alias> FROM (inner) ORDER BY rand().
    /// The wrapper's projection columns keep the ORIGINAL names so CTAS/VIEW and named
    /// output formats produce user-expected column names.
    auto new_root = std::make_shared<QueryNode>(Context::createCopy(context));
    new_root->getJoinTree() = query_root;

    NamesAndTypes outer_projection_columns;
    outer_projection_columns.reserve(subquery_projection_columns.size());
    for (size_t i = 0; i < subquery_projection_columns.size(); ++i)
    {
        NameAndTypePair inner_ref{unique_column_names[i], subquery_projection_columns[i].type};
        new_root->getProjection().getNodes().push_back(std::make_shared<ColumnNode>(inner_ref, query_root));
        outer_projection_columns.emplace_back(subquery_projection_columns[i].name, subquery_projection_columns[i].type);
    }
    new_root->resolveProjectionColumns(std::move(outer_projection_columns));
    addRandomOrderBy(new_root->getOrderBy(), context);

    /// Replace old root with new wrapping query node
    query_root = new_root;
}

/// If inject_random_order_for_select_without_order_by = 1, wrap the query node into
/// SELECT * FROM <query_node> ORDER BY rand().

void InjectRandomOrderIfNoOrderByPass::run(QueryTreeNodePtr & root, ContextPtr context)
{
    const auto & settings = context->getSettingsRef();
    if (!settings[Setting::inject_random_order_for_select_without_order_by])
        return;

    /// Case 1: Top-level SELECT
    if (auto * query_node = root->as<QueryNode>())
    {
        if (!query_node->hasOrderBy())
            wrapWithSelectOrderBy(root, context);
        return;
    }

    /// Case 2: Top-level UNION - wrap each branch
    if (auto * union_node = root->as<UnionNode>())
    {
        auto & union_subqueries = union_node->getQueries();
        for (auto & union_subquery_node : union_subqueries.getNodes())
        {
            if (auto * node = union_subquery_node->as<QueryNode>())
                if (!node->hasOrderBy())
                    wrapWithSelectOrderBy(union_subquery_node, context);
        }
        return;
    }
}

}
