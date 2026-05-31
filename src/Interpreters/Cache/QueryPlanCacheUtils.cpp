#include <Interpreters/Cache/QueryPlanCacheUtils.h>

#include <Access/Common/AccessType.h>
#include <Access/Common/RowPolicyDefs.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Planner/PlannerContext.h>
#include <Storages/IStorage.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
}

namespace
{

class RemoveQueryPlanCacheIgnoredSettingsMatcher
{
public:
    struct Data {};

    static bool needChildVisit(ASTPtr &, const ASTPtr &)
    {
        return true;
    }

    static void visit(ASTPtr & ast, Data &)
    {
        if (auto * set_clause = ast->as<ASTSetQuery>())
        {
            chassert(!set_clause->is_standalone);

            auto is_ignored_setting = [](const auto & change)
            {
                return isSettingIgnoredInQueryPlanCache(change.name);
            };

            std::erase_if(set_clause->changes, is_ignored_setting);
        }
        else
        {
            ASTQueryWithOutput::resetOutputASTIfExist(*ast);
        }
    }
};

using RemoveQueryPlanCacheIgnoredSettingsVisitor = InDepthNodeVisitor<RemoveQueryPlanCacheIgnoredSettingsMatcher, true>;

ASTPtr normalizeASTForQueryPlanCache(ASTPtr ast)
{
    ASTPtr normalized_ast = ast->clone();

    RemoveQueryPlanCacheIgnoredSettingsMatcher::Data visitor_data;
    RemoveQueryPlanCacheIgnoredSettingsVisitor(visitor_data).visit(normalized_ast);

    return normalized_ast;
}

}

std::optional<QueryPlanCacheKey> tryBuildQueryPlanCacheKey(
    const ASTPtr & ast,
    const ContextPtr & context,
    UInt64 semantic_settings_hash)
{
    const ASTSelectQuery * select_query = ast->as<ASTSelectQuery>();
    if (!select_query)
    {
        const auto * union_query = ast->as<ASTSelectWithUnionQuery>();
        if (union_query && union_query->list_of_selects && union_query->list_of_selects->children.size() == 1)
            select_query = union_query->list_of_selects->children.front()->as<ASTSelectQuery>();
    }
    if (!select_query)
        return {};

    const auto * tables_in_select = select_query->tables()
        ? select_query->tables()->as<ASTTablesInSelectQuery>()
        : nullptr;
    if (!tables_in_select || tables_in_select->children.size() != 1)
        return {};

    const auto * elem = tables_in_select->children.front()->as<ASTTablesInSelectQueryElement>();
    if (!elem || !elem->table_expression)
        return {};

    const auto * table_expr = elem->table_expression->as<ASTTableExpression>();
    if (!table_expr || !table_expr->database_and_table_name
        || table_expr->table_function || table_expr->subquery)
        return {};

    StorageID storage_id(table_expr->database_and_table_name);
    storage_id = context->resolveStorageID(storage_id);

    auto storage = DatabaseCatalog::instance().tryGetTable(storage_id, context);
    if (!storage || storage->isRemote() || storage->isView())
        return {};

    if (storage_id.database_name == DatabaseCatalog::SYSTEM_DATABASE)
        return {};

    auto metadata = storage->getInMemoryMetadataPtr(context, false);
    std::map<String, Int64> table_metadata_versions;
    Int64 schema_version = metadata->getMetadataVersion();
    if (schema_version == 0)
        schema_version = computeSchemaHash(*metadata);
    table_metadata_versions[storage_id.getFullTableName()] = schema_version;

    IASTHash row_policy_hash{};
    auto row_policy = context->getRowPolicyFilter(storage_id.database_name, storage_id.table_name, RowPolicyFilterType::SELECT_FILTER);
    if (row_policy && !row_policy->isAlwaysTrue() && row_policy->expression)
        row_policy_hash = row_policy->expression->getTreeHash(/*ignore_aliases=*/false);

    auto roles = context->getCurrentRoles();
    std::sort(roles.begin(), roles.end());

    ASTPtr normalized_ast = normalizeASTForQueryPlanCache(ast);

    QueryPlanCacheKey key;
    key.ast_hash = normalized_ast->getTreeHash(/*ignore_aliases=*/false);
    key.semantic_settings_hash = semantic_settings_hash;
    key.table_metadata_versions = std::move(table_metadata_versions);
    key.storage_id = storage_id;
    key.row_policy_hash = row_policy_hash;
    key.user_id = context->getUserID();
    key.current_user_roles = std::move(roles);
    return key;
}

Names getSelectedColumnsForQueryPlanCacheEntry(const PlannerContextPtr & planner_context)
{
    if (!planner_context)
        return {};

    const auto & table_expression_data = planner_context->getTableExpressionNodeToData();
    /// `tryBuildQueryPlanCacheKey` only admits single-table queries, so this branch is never
    /// reached today. Throw rather than silently return an empty Names list so that any future
    /// extension to multi-table caching is forced to also extend the column-level access check.
    if (table_expression_data.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Query plan cache: expected exactly one table expression, got {}", table_expression_data.size());

    return table_expression_data.begin()->second.getSelectedColumnsNames();
}

void checkAccessForQueryPlanCacheHit(const ContextPtr & context, const StorageID & storage_id, const Names & selected_columns)
{
    auto storage = DatabaseCatalog::instance().tryGetTable(storage_id, context);
    if (!storage)
        throw Exception(ErrorCodes::UNKNOWN_TABLE,
            "Table {} no longer exists (stale query plan cache entry)",
            storage_id.getFullTableName());

    auto metadata = storage->getInMemoryMetadataPtr(context, false);

    if (selected_columns.empty() && metadata && !metadata->getColumns().empty())
    {
        auto access = context->getAccess();
        for (const auto & column : metadata->getColumns())
        {
            if (access->isGranted(AccessType::SELECT, storage_id.database_name, storage_id.table_name, column.name))
                return;
        }

        throw Exception(
            ErrorCodes::ACCESS_DENIED,
            "{}: Not enough privileges. To execute this query, it's necessary to have the grant SELECT for at least one column on {}",
            context->getUserName(),
            storage_id.getFullTableName());
    }

    context->checkAccess(AccessType::SELECT, storage_id, selected_columns);
}

}
