#include <Interpreters/Cache/QueryPlanCacheUtils.h>

#include <Access/Common/AccessType.h>
#include <Access/Common/RowPolicyDefs.h>
#include <Access/ContextAccess.h>
#include <Analyzer/Utils.h>
#include <Analyzer/TableNode.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Planner/PlannerContext.h>
#include <Processors/QueryPlan/ReadFromTableStep.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/Exception.h>

#include <algorithm>
#include <map>
#include <set>
#include <stack>

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
    struct Data
    {
    };

    static bool needChildVisit(ASTPtr &, const ASTPtr &) { return true; }

    static void visit(ASTPtr & ast, Data &)
    {
        if (auto * set_clause = ast->as<ASTSetQuery>())
        {
            chassert(!set_clause->is_standalone);
            std::erase_if(set_clause->changes, [](const auto & change) { return isSettingIgnoredInQueryPlanCache(change.name); });
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

    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }

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

ASTPtr normalizeASTForQueryPlanCache(const ASTPtr & ast)
{
    ASTPtr normalized_ast = ast->clone();
    RemoveQueryPlanCacheIgnoredSettingsMatcher::Data visitor_data;
    RemoveQueryPlanCacheIgnoredSettingsVisitor(visitor_data).visit(normalized_ast);
    return normalized_ast;
}

String formatKeyExpression(const ASTPtr & ast)
{
    return ast ? ast->formatWithSecretsOneLine() : String{};
}

std::optional<QueryPlanCacheStorageDependency> buildStorageDependency(
    const String & table_name, const StoragePtr & storage, const StorageMetadataPtr & metadata, const Names & column_names)
{
    QueryPlanCacheStorageDependency dependency;
    dependency.table_name = table_name;
    dependency.engine_name = storage->getName();
    dependency.sorting_key = formatKeyExpression(metadata->sorting_key.expression_list_ast);
    dependency.partition_key = formatKeyExpression(metadata->partition_key.expression_list_ast);
    dependency.primary_key = formatKeyExpression(metadata->primary_key.expression_list_ast);
    dependency.sampling_key = formatKeyExpression(metadata->sampling_key.expression_list_ast);
    dependency.sorting_key_reverse_flags = metadata->sorting_key.reverse_flags;

    std::set<String> unique_names(column_names.begin(), column_names.end());
    dependency.columns.reserve(unique_names.size());
    for (const auto & name : unique_names)
    {
        QueryPlanCacheColumnDependency column_dependency;
        if (auto column = metadata->columns.tryGetColumnOrSubcolumnDescription(GetColumnsOptions::All, name))
        {
            column_dependency.name = name;
            column_dependency.type = column->type->getName();
            column_dependency.default_kind = column->default_desc.kind;
            column_dependency.default_expression = formatKeyExpression(column->default_desc.expression);
            column_dependency.ephemeral_default = column->default_desc.ephemeral_default;
        }
        else if (auto virtual_column = metadata->virtuals.tryGet(name, VirtualsKind::All, VirtualsMaterializationPlace::All))
        {
            column_dependency.name = name;
            column_dependency.type = virtual_column->type->getName();
        }
        else
        {
            return {};
        }
        dependency.columns.push_back(std::move(column_dependency));
    }

    return dependency;
}

}

bool astContainsInTableExpressionForQueryPlanCache(ASTPtr ast)
{
    HasInTableExpressionsMatcher::Data finder_data;
    HasInTableExpressionsVisitor(finder_data).visit(ast);
    return finder_data.has_in_table_expression;
}

std::optional<QueryPlanCacheLookupContext>
tryBuildPreAnalysisQueryPlanCacheLookup(const ASTPtr & ast, const ContextPtr & context, UInt64 semantic_settings_hash)
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

    const auto * tables_in_select = select_query->tables() ? select_query->tables()->as<ASTTablesInSelectQuery>() : nullptr;
    if (!tables_in_select || tables_in_select->children.size() != 1)
        return {};

    const auto * elem = tables_in_select->children.front()->as<ASTTablesInSelectQueryElement>();
    if (!elem || !elem->table_expression)
        return {};

    const auto * table_expr = elem->table_expression->as<ASTTableExpression>();
    if (!table_expr || !table_expr->database_and_table_name || table_expr->table_function || table_expr->subquery)
        return {};

    if (table_expr->stream_settings)
        return {};

    StorageID storage_id(table_expr->database_and_table_name);
    storage_id = context->resolveStorageID(storage_id);

    auto storage = DatabaseCatalog::instance().tryGetTable(storage_id, context);
    if (!storage || storage->isRemote() || storage->isView())
        return {};

    /// Wrapper engines may reference storage and access-control dependencies that are not
    /// represented by the current single-table cache contract.
    if (!dynamic_cast<const MergeTreeData *>(storage.get()))
        return {};

    if (storage_id.database_name == DatabaseCatalog::SYSTEM_DATABASE)
        return {};

    /// Row policies are deliberately outside the first version of the cache contract. Any
    /// applicable policy, including an always-true policy, bypasses lookup and insertion.
    if (context->getRowPolicyFilter(storage_id.database_name, storage_id.table_name, RowPolicyFilterType::SELECT_FILTER))
        return {};

    ASTPtr normalized_ast = normalizeASTForQueryPlanCache(ast);

    QueryPlanCacheKey key;
    key.ast_hash = normalized_ast->getTreeHash(/*ignore_aliases=*/false);
    key.ast_identity = normalized_ast->formatWithSecretsOneLine();
    key.current_database = context->getCurrentDatabase();
    key.semantic_settings_hash = semantic_settings_hash;

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
    if (table_expression_data.size() != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Query plan cache: expected exactly one table expression, got {}", table_expression_data.size());

    return table_expression_data.begin()->second.getSelectedColumnsNames();
}

Names getReadColumnsForQueryPlanCacheEntry(const QueryPlan & plan)
{
    if (!plan.isInitialized())
        return {};

    std::set<String> read_columns;
    std::stack<const QueryPlan::Node *> stack;
    stack.push(plan.getRootNode());
    while (!stack.empty())
    {
        const auto * node = stack.top();
        stack.pop();
        for (const auto * child : node->children)
            stack.push(child);

        if (typeid_cast<const ReadFromTableStep *>(node->step.get()))
        {
            for (const auto & column : node->step->getOutputHeader()->getNames())
                read_columns.insert(column);
        }
    }

    return Names(read_columns.begin(), read_columns.end());
}

std::vector<QueryPlanCacheStorageDependency> buildQueryPlanCacheDependencies(
    const QueryPlanCacheLookupContext & lookup_context,
    const QueryPlan & plan,
    const PlannerContextPtr & planner_context,
    const Names & selected_columns)
{
    if (!plan.isInitialized())
        return {};

    if (!planner_context)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Query plan cache requires a planner context");

    std::map<String, std::set<String>> columns_by_table;
    std::stack<const QueryPlan::Node *> stack;
    stack.push(plan.getRootNode());
    while (!stack.empty())
    {
        const auto * node = stack.top();
        stack.pop();
        for (const auto * child : node->children)
            stack.push(child);

        if (const auto * read = typeid_cast<const ReadFromTableStep *>(node->step.get()))
        {
            auto & names = columns_by_table[read->getTable()];
            const auto output_names = node->step->getOutputHeader()->getNames();
            names.insert(output_names.begin(), output_names.end());
        }
    }

    const String expected_table_name = lookup_context.storage_id.getFullTableName();
    if (columns_by_table.size() != 1 || columns_by_table.begin()->first != expected_table_name)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Query plan cache only supports one direct table dependency");

    const auto & table_expression_data = planner_context->getTableExpressionNodeToData();
    if (table_expression_data.size() != 1)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Query plan cache only supports one planned table expression, got {}",
            table_expression_data.size());

    const auto * table_node = table_expression_data.begin()->first->as<TableNode>();
    if (!table_node)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Query plan cache requires a direct table node");

    const auto & storage = table_node->getStorage();
    const auto & storage_snapshot = table_node->getStorageSnapshot();
    if (!storage || !storage_snapshot)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Query plan cache requires a resolved storage snapshot");

    auto & dependency_columns = columns_by_table.begin()->second;
    dependency_columns.insert(selected_columns.begin(), selected_columns.end());
    Names names(dependency_columns.begin(), dependency_columns.end());
    auto dependency = buildStorageDependency(expected_table_name, storage, storage_snapshot->metadata, names);
    if (!dependency)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot capture all dependencies for table {}", expected_table_name);

    std::vector<QueryPlanCacheStorageDependency> result;
    result.push_back(std::move(*dependency));
    return result;
}

std::optional<ValidatedQueryPlanCacheEntry> validateQueryPlanCacheEntryAndBuildSnapshot(
    const QueryPlanCacheLookupContext & lookup_context, const ContextPtr & context, const QueryPlanCacheEntry & entry)
{
    if (entry.dependencies.size() != 1)
        return {};

    const auto & cached_dependency = entry.dependencies.front();
    if (cached_dependency.table_name != lookup_context.storage_id.getFullTableName())
        return {};

    auto storage = DatabaseCatalog::instance().tryGetTable(lookup_context.storage_id, context);
    if (!storage)
        throw Exception(
            ErrorCodes::UNKNOWN_TABLE,
            "Table {} no longer exists (stale query plan cache entry)",
            lookup_context.storage_id.getFullTableName());

    auto table_lock = storage->lockForShare(context->getInitialQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout]);

    storage->updateExternalDynamicMetadataIfExists(context);
    auto metadata_snapshot = storage->getInMemoryMetadataPtr(context, false);
    auto storage_snapshot = storage->getStorageSnapshot(metadata_snapshot, context);

    Names dependency_columns;
    dependency_columns.reserve(cached_dependency.columns.size());
    for (const auto & column : cached_dependency.columns)
        dependency_columns.push_back(column.name);

    auto current_dependency = buildStorageDependency(cached_dependency.table_name, storage, metadata_snapshot, dependency_columns);
    if (!current_dependency || *current_dependency != cached_dependency)
        return {};

    ValidatedQueryPlanCacheEntry result;
    result.storage_id = lookup_context.storage_id;
    result.table_name = cached_dependency.table_name;
    result.selected_columns = entry.selected_columns;
    result.read_columns = entry.read_columns;
    result.metadata_snapshot = metadata_snapshot;
    result.storage = std::move(storage);
    result.storage_snapshot = std::move(storage_snapshot);
    result.table_lock = std::move(table_lock);
    return result;
}

void checkAccessForQueryPlanCacheHit(
    const ContextPtr & context, const StorageID & storage_id, const StorageMetadataPtr & metadata_snapshot, const Names & selected_columns)
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

void checkStorageSupportsTransactionsForQueryPlanCacheHit(
    const ContextPtr & context, const StoragePtr & storage)
{
    if (!context->getSettingsRef()[Setting::throw_on_unsupported_query_inside_transaction])
        return;

    if (!context->getCurrentTransaction())
        return;

    if (storage && !storage->supportsTransactions())
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Storage {} (table {}) does not support transactions",
            storage->getName(),
            storage->getStorageID().getNameForLogs());
}

}
