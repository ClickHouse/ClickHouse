#include <Interpreters/Cache/QueryPlanCacheUtils.h>

#include <Access/Common/AccessType.h>
#include <Access/Common/RowPolicyDefs.h>
#include <Access/ContextAccess.h>
#include <Access/EnabledRowPolicies.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/Identifier.h>
#include <Analyzer/Resolve/IdentifierResolver.h>
#include <Analyzer/TableNode.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Core/ProtocolDefines.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromTableFunctionStep.h>
#include <Processors/QueryPlan/ReadFromTableStep.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryDescription.h>
#include <Storages/StorageMerge.h>

#include <algorithm>
#include <stack>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
}

/// Defined in Processors/QueryPlan/resolveStorages.cpp; reused so that dependency names are
/// parsed exactly the way `QueryPlan::resolveStorages` parses them on materialization.
Identifier parseTableIdentifier(const std::string & str, const ContextPtr & context);

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

IASTHash getRowPolicyHash(const ContextPtr & context, const String & database, const String & table)
{
    IASTHash row_policy_hash{};
    auto row_policy = context->getRowPolicyFilter(database, table, RowPolicyFilterType::SELECT_FILTER);
    if (row_policy && !row_policy->isAlwaysTrue() && row_policy->expression)
        row_policy_hash = row_policy->expression->getTreeHash(/*ignore_aliases=*/false);
    return row_policy_hash;
}

Int64 getMetadataVersionOrSchemaHash(const StorageMetadataPtr & metadata)
{
    Int64 version = metadata->getMetadataVersion();
    if (version == 0)
        version = computeSchemaHash(*metadata);
    return version;
}

bool isAllowedSystemTable(const String & database, const String & table)
{
    /// `system.one` backs FROM-less SELECTs; it is a constant single-row table.
    return database == DatabaseCatalog::SYSTEM_DATABASE && table == "one";
}

/// Fills a dependency record from a resolved storage. Returns false if the storage makes the
/// plan uncacheable.
bool fillDependency(QueryPlanCacheDependency & dep, const StoragePtr & storage, const ContextPtr & context)
{
    const auto & storage_id = storage->getStorageID();
    dep.database = storage_id.getDatabaseName();
    dep.table = storage_id.table_name;
    dep.uuid = storage_id.uuid;

    if (dep.database == DatabaseCatalog::TEMPORARY_DATABASE
        || (dep.database == DatabaseCatalog::SYSTEM_DATABASE && !isAllowedSystemTable(dep.database, dep.table))
        || storage->isRemote()
        || typeid_cast<const StorageMerge *>(storage.get()))
    {
        LOG_DEBUG(getLogger("QueryPlanCache"), "Not caching plan: dependency {}.{} is temporary, system, remote or Merge",
            dep.database, dep.table);
        return false;
    }

    auto metadata = storage->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/false);

    /// A DEFINER view executes its body under another user's rights; the cached plan would
    /// have to replay that user's access context, which the cache does not model.
    if (storage->isView() && metadata->sql_security_type && *metadata->sql_security_type == SQLSecurityType::DEFINER)
    {
        LOG_DEBUG(getLogger("QueryPlanCache"), "Not caching plan: view {}.{} has SQL SECURITY DEFINER", dep.database, dep.table);
        return false;
    }

    dep.metadata_version = getMetadataVersionOrSchemaHash(metadata);
    dep.row_policy_hash = getRowPolicyHash(context, dep.database, dep.table);
    dep.is_view = storage->isView();
    return true;
}

/// Returns true for functions whose results must not be frozen into a cached plan.
/// `arrayJoin` reports `isDeterministic() = false` because it is multi-valued, but it is
/// pure (same argument - same set of rows), so it is safe for the plan cache.
bool isFunctionUnsafeForPlanCache(const ASTFunction & function, const ContextPtr & context)
{
    if (function.name == "arrayJoin")
        return false;

    if (const auto func = FunctionFactory::instance().tryGet(function.name, context))
        return !func->isDeterministic();

    /// SQL-defined UDFs: determinism is unknown, assume the worst.
    if (UserDefinedSQLFunctionFactory::instance().tryGet(function.name))
        return true;

    if (const auto udf_executable = UserDefinedExecutableFunctionFactory::tryGet(function.name, context))
        return !udf_executable->isDeterministic();

    return false;
}

/// Function names whose subquery argument is a set, not a scalar (re-executed on every run).
bool isSetFunction(const String & name)
{
    return name == "in" || name == "notIn" || name == "globalIn" || name == "globalNotIn"
        || name == "nullIn" || name == "notNullIn" || name == "globalNullIn" || name == "globalNotNullIn";
}

/// Walks an AST and collects every table identifier used as a table expression, recursing
/// into view definitions. This catches dependencies that are invisible in the logical plan:
/// views (inlined during analysis) and tables referenced only from scalar subqueries
/// (evaluated to constants during analysis).
///
/// The same walk doubles as the eligibility check for expanded view bodies, which the
/// caller's query-tree checks cannot see: non-deterministic functions and (unless allowed)
/// scalar subqueries anywhere in the closure make the plan uncacheable.
class ASTDependencyCollector
{
public:
    ASTDependencyCollector(
        const ContextPtr & context_,
        std::vector<QueryPlanCacheDependency> & dependencies_,
        bool allow_scalar_subqueries_)
        : context(context_), dependencies(dependencies_), allow_scalar_subqueries(allow_scalar_subqueries_) {}

    bool collect(const IAST & ast, const String & default_database)
    {
        return collectImpl(ast, default_database, /*in_set_or_table_position=*/ false);
    }

private:
    bool collectImpl(const IAST & ast, const String & default_database, bool in_set_or_table_position)
    {
        if (const auto * table_expression = ast.as<ASTTableExpression>())
        {
            if (table_expression->table_function)
            {
                LOG_DEBUG(getLogger("QueryPlanCache"), "Not caching plan: query or view body uses a table function");
                return false;
            }
            if (table_expression->database_and_table_name)
                if (!visitTableIdentifier(*table_expression->database_and_table_name, default_database))
                    return false;

            /// Subqueries in FROM are re-executed on every run.
            in_set_or_table_position = true;
        }
        else if (const auto * function = ast.as<ASTFunction>())
        {
            if (isFunctionUnsafeForPlanCache(*function, context))
            {
                LOG_DEBUG(getLogger("QueryPlanCache"),
                    "Not caching plan: query or view body uses non-deterministic function {}", function->name);
                return false;
            }

            /// For `x IN (subquery)` only the right-hand argument is the set (re-executed on
            /// every run); the left-hand operand is an ordinary expression, so a subquery there
            /// is a scalar subquery (folded into a constant) and must be gated like any other
            /// scalar subquery. Marking every child as set-position would let a scalar subquery
            /// in the left operand escape the `query_plan_cache_allow_scalar_subqueries` gate.
            if (isSetFunction(function->name) && function->arguments && function->arguments->children.size() == 2)
            {
                const auto & args = function->arguments->children;
                return collectImpl(*args[0], default_database, /*in_set_or_table_position=*/ false)
                    && collectImpl(*args[1], default_database, /*in_set_or_table_position=*/ true);
            }

            in_set_or_table_position = false;
        }
        else if (ast.as<ASTSubquery>())
        {
            /// A subquery outside FROM / IN position is a scalar subquery: it is evaluated
            /// during analysis and its result is baked into the plan as a constant.
            if (!in_set_or_table_position && !allow_scalar_subqueries)
            {
                LOG_DEBUG(getLogger("QueryPlanCache"),
                    "Not caching plan: query or view body uses a scalar subquery "
                    "(see query_plan_cache_allow_scalar_subqueries)");
                return false;
            }
            in_set_or_table_position = false;
        }
        else if (!ast.as<ASTWithElement>())
        {
            /// `WITH name AS (subquery)` defines a CTE (table position); anything else resets
            /// the positional context.
            in_set_or_table_position = false;
        }

        for (const auto & child : ast.children)
            if (!collectImpl(*child, default_database, in_set_or_table_position))
                return false;

        return true;
    }

    bool visitTableIdentifier(const IAST & identifier_ast, const String & default_database)
    {
        const auto * identifier = identifier_ast.as<ASTTableIdentifier>();
        if (!identifier)
            return true;

        auto table_id = identifier->getTableId();
        if (table_id.database_name.empty())
            table_id.database_name = default_database;

        /// Unresolvable names are not necessarily errors: the identifier may refer to a CTE.
        /// Skipping them is safe - a CTE is not a storage, so it cannot go stale.
        auto storage = DatabaseCatalog::instance().tryGetTable(table_id, context);
        if (!storage)
            return true;

        const auto & resolved_id = storage->getStorageID();
        if (!visited.insert(resolved_id.getFullTableName()).second)
            return true;

        QueryPlanCacheDependency dep;
        if (!fillDependency(dep, storage, context))
            return false;
        dependencies.push_back(std::move(dep));

        /// Recurse into view bodies: nested views and their tables are dependencies too.
        /// Names inside a view body resolve against the view's own database.
        /// Normal views store their body in `select.inner_query`; materialized views in
        /// `select.select_query`.
        if (storage->isView())
        {
            auto metadata = storage->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/false);
            const auto & view_query = metadata->select.inner_query ? metadata->select.inner_query : metadata->select.select_query;
            if (view_query)
                if (!collectImpl(*view_query, resolved_id.getDatabaseName(), /*in_set_or_table_position=*/ false))
                    return false;
        }

        return true;
    }

    const ContextPtr & context;
    std::vector<QueryPlanCacheDependency> & dependencies;
    bool allow_scalar_subqueries = false;
    std::unordered_set<String> visited;
};

/// Collects dependencies from the logical plan's leaf `ReadFromTable` steps,
/// recursing into IN-subquery set sub-plans.
bool collectPlanDependencies(
    const QueryPlan & plan,
    const ContextPtr & context,
    std::vector<QueryPlanCacheDependency> & dependencies)
{
    if (!plan.isInitialized())
        return false;

    std::stack<const QueryPlan::Node *> stack;
    stack.push(plan.getRootNode());

    while (!stack.empty())
    {
        const auto * node = stack.top();
        stack.pop();

        if (const auto * delayed_creating_sets = typeid_cast<const DelayedCreatingSetsStep *>(node->step.get()))
        {
            for (const auto & set : delayed_creating_sets->getSets())
                if (const auto * sub_plan = set->getQueryPlan())
                    if (!collectPlanDependencies(*sub_plan, context, dependencies))
                        return false;
        }

        for (const auto * child : node->children)
            stack.push(child);

        if (!node->children.empty())
            continue;

        if (const auto * read_from_table = typeid_cast<const ReadFromTableStep *>(node->step.get()))
        {
            Identifier identifier = parseTableIdentifier(read_from_table->getTable(), context);
            auto table_node = IdentifierResolver::tryResolveTableIdentifier(identifier, context);
            if (!table_node)
            {
                LOG_DEBUG(getLogger("QueryPlanCache"), "Not caching plan: cannot resolve table {}", read_from_table->getTable());
                return false;
            }

            QueryPlanCacheDependency dep;
            if (!fillDependency(dep, table_node->getStorage(), context))
                return false;
            dep.columns = read_from_table->getOutputHeader()->getNames();
            dependencies.push_back(std::move(dep));
        }
        else
        {
            /// Unknown leaf type (table function, prepared source, ...): the plan cannot be
            /// revalidated against it, so it must not be cached.
            LOG_DEBUG(getLogger("QueryPlanCache"), "Not caching plan: leaf step {} is not a resolvable table read", node->step->getName());
            return false;
        }
    }

    return true;
}

}

bool astContainsFunctionsUnsafeForQueryPlanCache(const ASTPtr & ast, const ContextPtr & context)
{
    if (const auto * function = ast->as<ASTFunction>())
        if (isFunctionUnsafeForPlanCache(*function, context))
            return true;

    for (const auto & child : ast->children)
        if (astContainsFunctionsUnsafeForQueryPlanCache(child, context))
            return true;

    return false;
}

std::optional<QueryPlanCacheKey> tryBuildQueryPlanCacheKey(
    const ASTPtr & ast,
    const ContextPtr & context,
    UInt64 semantic_settings_hash)
{
    if (!ast->as<ASTSelectQuery>() && !ast->as<ASTSelectWithUnionQuery>())
        return {};

    auto roles = context->getCurrentRoles();
    std::sort(roles.begin(), roles.end());

    ASTPtr normalized_ast = normalizeASTForQueryPlanCache(ast);

    QueryPlanCacheKey key;
    key.ast_hash = normalized_ast->getTreeHash(/*ignore_aliases=*/false);
    key.semantic_settings_hash = semantic_settings_hash;
    key.current_database = context->getCurrentDatabase();
    key.user_id = context->getUserID();
    key.current_user_roles = std::move(roles);
    return key;
}

namespace
{

bool queryTreeIsEligibleImpl(const IQueryTreeNode & node, const ContextPtr & context, bool allow_scalar_subqueries)
{
    if (const auto * function_node = node.as<FunctionNode>())
    {
        if (function_node->isOrdinaryFunction())
        {
            const auto & function = function_node->getFunction();
            /// An unresolved ordinary function after analysis is unexpected; refuse to cache.
            if (!function || !function->isDeterministic())
                return false;
        }
    }
    else if (const auto * constant_node = node.as<ConstantNode>())
    {
        if (constant_node->hasSourceExpression())
        {
            const auto & source = constant_node->getSourceExpression();
            auto source_type = source->getNodeType();

            /// A constant whose source is a (scalar) subquery was evaluated during analysis;
            /// reusing the plan would reuse the value without re-reading the tables.
            if (source_type == QueryTreeNodeType::QUERY || source_type == QueryTreeNodeType::UNION)
            {
                if (!allow_scalar_subqueries)
                    return false;
            }
            else if (!queryTreeIsEligibleImpl(*source, context, allow_scalar_subqueries))
                return false;
        }
    }

    for (const auto & child : node.getChildren())
    {
        if (!child)
            continue;
        if (!queryTreeIsEligibleImpl(*child, context, allow_scalar_subqueries))
            return false;
    }

    return true;
}

}

bool queryTreeIsEligibleForPlanCache(const QueryTreeNodePtr & query_tree, const ContextPtr & context, bool allow_scalar_subqueries)
{
    return query_tree && queryTreeIsEligibleImpl(*query_tree, context, allow_scalar_subqueries);
}

std::optional<std::vector<QueryPlanCacheDependency>> collectQueryPlanCacheDependencies(
    const QueryPlan & plan,
    const ASTPtr & ast,
    const ContextPtr & context,
    bool allow_scalar_subqueries)
{
    std::vector<QueryPlanCacheDependency> dependencies;

    if (!collectPlanDependencies(plan, context, dependencies))
        return {};

    {
        ASTDependencyCollector collector(context, dependencies, allow_scalar_subqueries);
        if (!collector.collect(*ast, context->getCurrentDatabase()))
            return {};
    }

    /// Deduplicate by (database, table). Plan dependencies are collected first and carry
    /// the columns actually read; keep the first occurrence.
    std::sort(dependencies.begin(), dependencies.end(),
        [](const auto & lhs, const auto & rhs)
        {
            if (lhs.database != rhs.database)
                return lhs.database < rhs.database;
            if (lhs.table != rhs.table)
                return lhs.table < rhs.table;
            /// Entries with columns sort first so that dedup keeps them.
            return lhs.columns.size() > rhs.columns.size();
        });
    dependencies.erase(
        std::unique(dependencies.begin(), dependencies.end(),
            [](const auto & lhs, const auto & rhs) { return lhs.database == rhs.database && lhs.table == rhs.table; }),
        dependencies.end());

    return dependencies;
}

bool validateQueryPlanCacheEntry(const QueryPlanCacheEntry & entry, const ContextPtr & context)
{
    for (const auto & dep : entry.dependencies)
    {
        auto storage = DatabaseCatalog::instance().tryGetTable(StorageID{dep.database, dep.table}, context);
        if (!storage)
            return false;

        const auto & storage_id = storage->getStorageID();
        if (storage_id.uuid != dep.uuid)
            return false;

        auto metadata = storage->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/false);
        if (getMetadataVersionOrSchemaHash(metadata) != dep.metadata_version)
            return false;

        if (getRowPolicyHash(context, dep.database, dep.table) != dep.row_policy_hash)
            return false;
    }

    return true;
}

void checkAccessForQueryPlanCacheHit(const QueryPlanCacheEntry & entry, const ContextPtr & context)
{
    for (const auto & dep : entry.dependencies)
    {
        if (isAllowedSystemTable(dep.database, dep.table))
            continue;

        if (!dep.columns.empty())
        {
            context->checkAccess(AccessType::SELECT, StorageID{dep.database, dep.table}, dep.columns);
            continue;
        }

        /// No specific columns recorded (views, tables referenced only from scalar
        /// subqueries): require SELECT on at least one column, mirroring the access
        /// semantics of `SELECT count()`.
        auto storage = DatabaseCatalog::instance().tryGetTable(StorageID{dep.database, dep.table}, context);
        if (!storage)
            continue;
        auto metadata = storage->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/false);
        auto access = context->getAccess();
        bool any_column_granted = metadata->getColumns().empty();
        for (const auto & column : metadata->getColumns())
        {
            if (access->isGranted(AccessType::SELECT, dep.database, dep.table, column.name))
            {
                any_column_granted = true;
                break;
            }
        }
        if (!any_column_granted)
            throw Exception(
                ErrorCodes::ACCESS_DENIED,
                "{}: Not enough privileges. To execute this query, it's necessary to have the grant SELECT for at least one column on {}.{}",
                context->getUserName(),
                backQuoteIfNeed(dep.database),
                backQuoteIfNeed(dep.table));
    }
}

void addQueryAccessInfoForQueryPlanCacheHit(const QueryPlanCacheEntry & entry, const ContextPtr & context)
{
    if (!context->hasQueryContext())
        return;

    auto query_context = context->getQueryContext();
    for (const auto & dep : entry.dependencies)
    {
        StorageID storage_id{dep.database, dep.table};
        storage_id.uuid = dep.uuid;
        if (dep.is_view)
            query_context->addViewAccessInfo(storage_id.getFullTableName());
        else
            query_context->addQueryAccessInfo(storage_id, dep.columns);
    }
    for (const auto & row_policy : entry.used_row_policies)
        query_context->addUsedRowPolicy(row_policy);
}

QueryPlan materializeCachedQueryPlan(std::string_view serialized_plan, const ContextPtr & context)
{
    ReadBufferFromMemory in(serialized_plan.data(), serialized_plan.size());

    /// Reconstruct the logical plan skeleton; leaf nodes are storage-agnostic
    /// `ReadFromTable` placeholders.
    auto plan_and_sets = QueryPlan::deserialize(in, context);

    /// Rebuild `PreparedSet` objects for IN (...) subqueries embedded in the plan.
    auto plan = QueryPlan::makeSets(std::move(plan_and_sets), context);

    /// Replace `ReadFromTable` placeholders with storage-specific reads against the
    /// current data snapshots.
    plan.resolveStorages(context);

    return plan;
}

String serializeQueryPlanForCache(const QueryPlan & plan)
{
    WriteBufferFromOwnString out;
    plan.serialize(out, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    out.finalize();
    return out.str();
}

}
