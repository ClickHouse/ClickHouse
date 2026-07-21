#include <Interpreters/Cache/QueryPlanCacheUtils.h>

#include <Access/Common/AccessType.h>
#include <Access/Common/RowPolicyDefs.h>
#include <Access/RowPolicy.h>
#include <Access/ContextAccess.h>
#include <Common/SipHash.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Analyzer/Utils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Planner/PlannerContext.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_TABLE;
}

namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsBool throw_on_unsupported_query_inside_transaction;
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

class HasInTableExpressionsMatcher
{
public:
    struct Data
    {
        bool has_in_table_expression = false;
    };

    static bool needChildVisit(const ASTPtr &, const ASTPtr &)
    {
        return true;
    }

    static void visit(const ASTPtr & ast, Data & data)
    {
        if (data.has_in_table_expression)
            return;

        const auto * function = ast->as<ASTFunction>();
        if (!function || !isNameOfInFunction(function->name) || !function->arguments)
            return;

        const auto & arguments = function->arguments->children;
        if (arguments.size() > 1 && (arguments[1]->as<ASTIdentifier>() || arguments[1]->as<ASTTableIdentifier>()))
            data.has_in_table_expression = true;
    }
};

using HasInTableExpressionsVisitor = InDepthNodeVisitor<HasInTableExpressionsMatcher, true>;

ASTPtr normalizeASTForQueryPlanCache(ASTPtr ast)
{
    ASTPtr normalized_ast = ast->clone();

    RemoveQueryPlanCacheIgnoredSettingsMatcher::Data visitor_data;
    RemoveQueryPlanCacheIgnoredSettingsVisitor(visitor_data).visit(normalized_ast);

    return normalized_ast;
}

std::map<String, Int64> getTableMetadataVersionsForQueryPlanCache(
    const StorageID & storage_id,
    const StorageMetadataPtr & metadata)
{
    std::map<String, Int64> table_metadata_versions;
    Int64 schema_version = metadata->getMetadataVersion();
    if (schema_version == 0)
        schema_version = computeSchemaHash(*metadata);
    table_metadata_versions[storage_id.getFullTableName()] = schema_version;
    return table_metadata_versions;
}

std::map<String, Int64> getTableMetadataVersionsForQueryPlanCache(
    const StorageID & storage_id,
    const StoragePtr & storage,
    const ContextPtr & context)
{
    auto metadata_snapshot = storage->getInMemoryMetadataPtr(context, false);
    return getTableMetadataVersionsForQueryPlanCache(storage_id, metadata_snapshot);
}

IASTHash getRowPolicyHashForQueryPlanCache(const ContextPtr & context, const StorageID & storage_id)
{
    IASTHash row_policy_hash{};
    auto row_policy = context->getRowPolicyFilter(storage_id.database_name, storage_id.table_name, RowPolicyFilterType::SELECT_FILTER);
    if (row_policy && !row_policy->isAlwaysTrue() && row_policy->expression)
        row_policy_hash = row_policy->expression->getTreeHash(/*ignore_aliases=*/false);
    return row_policy_hash;
}

UInt64 getRowPolicyNamesHashForQueryPlanCache(const ContextPtr & context, const StorageID & storage_id)
{
    SipHash hash;
    auto row_policy = context->getRowPolicyFilter(storage_id.database_name, storage_id.table_name, RowPolicyFilterType::SELECT_FILTER);
    if (!row_policy)
        return hash.get64();

    std::vector<String> policy_names;
    policy_names.reserve(row_policy->policies.size());
    for (const auto & policy : row_policy->policies)
        policy_names.push_back(policy->getFullName().toString());
    std::sort(policy_names.begin(), policy_names.end());
    for (const auto & policy_name : policy_names)
        hash.update(policy_name);
    return hash.get64();
}

}

bool astContainsInTableExpressionForQueryPlanCache(ASTPtr ast)
{
    HasInTableExpressionsMatcher::Data finder_data;
    HasInTableExpressionsVisitor(finder_data).visit(ast);
    return finder_data.has_in_table_expression;
}

std::optional<QueryPlanCacheLookupContext> tryBuildPreAnalysisQueryPlanCacheLookup(
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

    /// Reject `SELECT ... FROM t STREAM`. The `STREAM` cursor state lives in
    /// `TableExpressionModifiers::stream_settings`, which `ReadFromTableStep` does not
    /// serialize, so a cache hit would materialize a streaming read as a normal
    /// non-streaming one and change query semantics. Skip caching until the cursor
    /// state is part of the serialized plan.
    if (table_expr->stream_settings)
        return {};

    StorageID storage_id(table_expr->database_and_table_name);
    storage_id = context->resolveStorageID(storage_id);

    auto storage = DatabaseCatalog::instance().tryGetTable(storage_id, context);
    if (!storage || storage->isRemote() || storage->isView())
        return {};

    /// Restrict caching to MergeTree-family tables. Wrapper engines (StorageAlias,
    /// StorageBuffer, StorageMerge, ...) read through to other tables whose access
    /// rights and runtime data are not represented by the top-level `storage_id`
    /// stored in the cache key. Caching their plans would either bypass `SELECT`
    /// access checks on the underlying target (`StorageAlias` after `REVOKE` on
    /// the target) or skip wrapper-local state on hit (`StorageBuffer` in-memory
    /// rows). Until the cache key tracks every underlying `ReadFromTableStep`,
    /// only direct MergeTree reads are admitted.
    if (!dynamic_cast<const MergeTreeData *>(storage.get()))
        return {};

    if (storage_id.database_name == DatabaseCatalog::SYSTEM_DATABASE)
        return {};

    auto roles = context->getCurrentRoles();
    std::sort(roles.begin(), roles.end());

    ASTPtr normalized_ast = normalizeASTForQueryPlanCache(ast);

    QueryPlanCacheKey key;
    key.ast_hash = normalized_ast->getTreeHash(/*ignore_aliases=*/false);
    key.current_database = context->getCurrentDatabase();
    key.semantic_settings_hash = semantic_settings_hash;
    key.table_metadata_versions = getTableMetadataVersionsForQueryPlanCache(storage_id, storage, context);
    key.storage_id = storage_id;
    key.row_policy_hash = getRowPolicyHashForQueryPlanCache(context, storage_id);
    key.user_id = context->getUserID();
    key.current_user_roles = std::move(roles);

    QueryPlanCacheLookupContext lookup_context;
    lookup_context.key = std::move(key);
    lookup_context.storage_id = std::move(storage_id);
    return lookup_context;
}

Names getSelectedColumnsForQueryPlanCacheEntry(const PlannerContextPtr & planner_context)
{
    if (!planner_context)
        return {};

    const auto & table_expression_data = planner_context->getTableExpressionNodeToData();
    /// `tryBuildPreAnalysisQueryPlanCacheLookup` only admits single-table queries, so this branch is never
    /// reached today. Throw rather than silently return an empty Names list so that any future
    /// extension to multi-table caching is forced to also extend the column-level access check.
    if (table_expression_data.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Query plan cache: expected exactly one table expression, got {}", table_expression_data.size());

    return table_expression_data.begin()->second.getSelectedColumnsNames();
}

QueryPlanCacheDependencyFingerprint buildQueryPlanCacheDependencyFingerprint(
    const QueryPlanCacheLookupContext & lookup_context,
    const ContextPtr & context,
    const Names & selected_columns)
{
    auto storage = DatabaseCatalog::instance().tryGetTable(lookup_context.storage_id, context);
    if (!storage)
        throw Exception(ErrorCodes::UNKNOWN_TABLE,
            "Table {} no longer exists (stale query plan cache entry)",
            lookup_context.storage_id.getFullTableName());

    QueryPlanCacheDependencyFingerprint fingerprint;
    fingerprint.storage_id = lookup_context.storage_id;
    fingerprint.table_metadata_versions = getTableMetadataVersionsForQueryPlanCache(lookup_context.storage_id, storage, context);
    fingerprint.row_policy_hash = getRowPolicyHashForQueryPlanCache(context, lookup_context.storage_id);
    fingerprint.row_policy_names_hash = getRowPolicyNamesHashForQueryPlanCache(context, lookup_context.storage_id);
    fingerprint.semantic_settings_hash = lookup_context.key.semantic_settings_hash;
    fingerprint.selected_columns = selected_columns;
    return fingerprint;
}

static QueryPlanCacheDependencyFingerprint buildQueryPlanCacheDependencyFingerprint(
    const QueryPlanCacheLookupContext & lookup_context,
    const ContextPtr & context,
    const StorageMetadataPtr & metadata_snapshot,
    const Names & selected_columns)
{
    QueryPlanCacheDependencyFingerprint fingerprint;
    fingerprint.storage_id = lookup_context.storage_id;
    fingerprint.table_metadata_versions = getTableMetadataVersionsForQueryPlanCache(lookup_context.storage_id, metadata_snapshot);
    fingerprint.row_policy_hash = getRowPolicyHashForQueryPlanCache(context, lookup_context.storage_id);
    fingerprint.row_policy_names_hash = getRowPolicyNamesHashForQueryPlanCache(context, lookup_context.storage_id);
    fingerprint.semantic_settings_hash = lookup_context.key.semantic_settings_hash;
    fingerprint.selected_columns = selected_columns;
    return fingerprint;
}

std::optional<ValidatedQueryPlanCacheEntry> validateQueryPlanCacheEntryAndBuildSnapshot(
    const QueryPlanCacheLookupContext & lookup_context,
    const ContextPtr & context,
    const QueryPlanCacheEntry & entry)
{
    auto storage = DatabaseCatalog::instance().tryGetTable(lookup_context.storage_id, context);
    if (!storage)
        throw Exception(ErrorCodes::UNKNOWN_TABLE,
            "Table {} no longer exists (stale query plan cache entry)",
            lookup_context.storage_id.getFullTableName());

    auto table_lock = storage->lockForShare(
        context->getInitialQueryId(),
        context->getSettingsRef()[Setting::lock_acquire_timeout]);

    storage->updateExternalDynamicMetadataIfExists(context);
    auto metadata_snapshot = storage->getInMemoryMetadataPtr(context, false);
    auto storage_snapshot = storage->getStorageSnapshot(metadata_snapshot, context);

    if (entry.selected_columns != entry.dependencies.selected_columns)
        return {};

    auto fingerprint = buildQueryPlanCacheDependencyFingerprint(
        lookup_context,
        context,
        metadata_snapshot,
        entry.dependencies.selected_columns);

    if (!(entry.dependencies == fingerprint))
        return {};

    ValidatedQueryPlanCacheEntry result;
    result.storage_id = lookup_context.storage_id;
    result.selected_columns = entry.selected_columns;
    result.metadata_snapshot = metadata_snapshot;
    result.storage_bindings.push_back(QueryPlanStorageBinding{
        .table_name = lookup_context.storage_id.getFullTableName(),
        .storage = std::move(storage),
        .snapshot = std::move(storage_snapshot),
        .table_lock = std::move(table_lock),
    });
    return result;
}

void checkAccessForQueryPlanCacheHit(
    const ContextPtr & context,
    const StorageID & storage_id,
    const StorageMetadataPtr & metadata_snapshot,
    const Names & selected_columns)
{
    if (selected_columns.empty() && metadata_snapshot && !metadata_snapshot->getColumns().empty())
    {
        auto access = context->getAccess();
        for (const auto & column : metadata_snapshot->getColumns())
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

void checkStoragesSupportTransactionsForQueryPlanCacheHit(
    const ContextPtr & context,
    const std::vector<QueryPlanStorageBinding> & storage_bindings)
{
    if (!context->getSettingsRef()[Setting::throw_on_unsupported_query_inside_transaction])
        return;

    if (!context->getCurrentTransaction())
        return;

    for (const auto & binding : storage_bindings)
    {
        if (binding.storage && !binding.storage->supportsTransactions())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Storage {} (table {}) does not support transactions",
                binding.storage->getName(),
                binding.storage->getStorageID().getNameForLogs());
    }
}

}
