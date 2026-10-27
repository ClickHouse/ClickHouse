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
#include <Common/FailPoint.h>
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
#include <Functions/UserDefined/UserDefinedWebAssembly.h>
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
#include <Storages/StorageAlias.h>
#include <Storages/StorageBuffer.h>
#include <Storages/StorageDictionary.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageMerge.h>
#include <Storages/StorageProxy.h>
#include <Storages/StorageView.h>

#include <base/scope_guard.h>

#include <algorithm>
#include <map>
#include <set>
#include <stack>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int NOT_IMPLEMENTED;
}

namespace FailPoints
{
    extern const char query_plan_cache_serialization_not_implemented[];
}

/// Defined in Processors/QueryPlan/resolveStorages.cpp; reused so that dependency names are
/// parsed exactly the way `QueryPlan::resolveStorages` parses them on materialization.
Identifier parseTableIdentifier(const std::string & str, const ContextPtr & context);

namespace
{

class RemoveQueryOutputMatcher
{
public:
    struct Data {};

    static bool needChildVisit(ASTPtr &, const ASTPtr &)
    {
        return true;
    }

    static void visit(ASTPtr & ast, Data &)
    {
        ASTQueryWithOutput::resetOutputASTIfExist(*ast);
    }
};

using RemoveQueryOutputVisitor = InDepthNodeVisitor<RemoveQueryOutputMatcher, true>;

/// The `SELECT` whose `SETTINGS` clause `InterpreterSetQuery::applySettingsFromQuery` applies to the
/// query context: the top-level `SELECT`, or the last `SELECT` of a top-level `UNION`.
ASTSelectQuery * getSelectWithQueryContextSettings(const IAST & ast)
{
    if (const auto * select = ast.as<ASTSelectQuery>())
        return const_cast<ASTSelectQuery *>(select);
    if (const auto * select_with_union = ast.as<ASTSelectWithUnionQuery>();
        select_with_union && select_with_union->list_of_selects && !select_with_union->list_of_selects->children.empty())
        return select_with_union->list_of_selects->children.back()->as<ASTSelectQuery>();
    return nullptr;
}

ASTPtr normalizeASTForQueryPlanCache(ASTPtr ast)
{
    ASTPtr normalized_ast = ast->clone();

    /// Strip the ignored settings only from the `SETTINGS` clause that is applied to the query context.
    /// Any other `SETTINGS` clause (of a subquery, of another `SELECT` of a `UNION`) is scoped to its
    /// part of the query, which a cached plan cannot reproduce, so `ASTDependencyCollector` makes such
    /// a query uncacheable. It must stay in the key as is: otherwise a query with an inner clause of
    /// only ignored settings would get the same key as the query without it and hit its cached plan.
    if (auto * select = getSelectWithQueryContextSettings(*normalized_ast))
    {
        if (auto settings = select->settings())
        {
            if (auto * set_query = settings->as<ASTSetQuery>())
            {
                std::erase_if(set_query->changes, [](const SettingChange & change) { return isSettingIgnoredInQueryPlanCache(change.name); });
                std::erase_if(set_query->default_settings, [](const String & name) { return isSettingIgnoredInQueryPlanCache(name); });
                if (set_query->changes.empty() && set_query->default_settings.empty() && set_query->query_parameters.empty())
                    select->setExpression(ASTSelectQuery::Expression::SETTINGS, {});
            }
        }
    }

    RemoveQueryOutputMatcher::Data visitor_data;
    RemoveQueryOutputVisitor(visitor_data).visit(normalized_ast);

    return normalized_ast;
}

/// A deserialized plan rebuilds every `ActionsDAG` function node through `FunctionFactory` alone
/// (see `ActionsDAG::deserialize`), while the analyzer resolves executable and WebAssembly UDFs
/// through their own factories first, before it ever looks into `FunctionFactory`. A cache hit on
/// a plan that uses such a UDF would therefore throw `UNKNOWN_FUNCTION`, or - for an executable UDF
/// declared in a configuration file, which unlike `CREATE FUNCTION` is not checked against the
/// builtin names - silently call the shadowed builtin instead. Both factories take precedence over
/// `FunctionFactory` during analysis, so this must be checked first, before the name is looked up
/// there.
bool isFunctionResolvedOutsideFunctionFactory(const String & name, const ContextPtr & context)
{
    return UserDefinedExecutableFunctionFactory::has(name, context)
        || UserDefinedWebAssemblyFunctionFactory::instance().has(name); /// NOLINT(readability-static-accessed-through-instance)
}

/// True for a function that checks access rights of the current user when it is created (introspection
/// functions, AI functions using a named collection, ...). A deterministic call with constant arguments
/// is folded into a constant during analysis, and the cached plan stores only the value: a hit never
/// creates the function again, so it would keep serving the value after the privilege was revoked. The
/// non-deterministic functions of this kind are rejected anyway; they are listed here so that the
/// guard does not depend on their determinism flag.
bool isFunctionCheckingAccessOnCreate(const String & name)
{
    static const std::unordered_set<std::string_view> functions = {
        "demangle",
        "addressToLine",
        "addressToLineWithInlines",
        "addressToSymbol",
        "getServerSetting",
        "getMergeTreeSetting",
        "getMacro",
        "hasColumnInTable",
        "joinGet",
        "joinGetOrNull",
        "file",
        "naiveBayesClassifier",
        "naiveBayesClassifierWithProb",
        "naiveBayesClassifierWithAllProbs",
        "predictXGBoost",
    };

    const String & effective_name = getFunctionCanonicalNameIfAny(name);
    return functions.contains(effective_name)
        /// The AI functions check access to the named collection with the credentials.
        || effective_name.starts_with("ai")
        /// The dictionary functions check `dictGet` on the dictionary.
        || effective_name.starts_with("dict");
}

/// What a row-policy filter AST contains that the plan cache cannot support.
struct RowPolicyFilterContents
{
    /// A subquery (a scalar `(SELECT ...)`, an `IN (SELECT ...)`, or a bare select node): it reads
    /// other tables that the cache can neither track nor revalidate.
    bool has_subquery = false;
    /// A call to an executable or WebAssembly UDF, which a deserialized plan cannot rebuild.
    bool has_function_resolved_outside_function_factory = false;
    /// A call to a SQL UDF (`CREATE FUNCTION`). The analyzer inlines its body into the filter with
    /// its own lookup in `UserDefinedSQLFunctionFactory`, so the body baked into the plan cannot be
    /// tied to the body any later check sees: a concurrent `CREATE OR REPLACE FUNCTION` between
    /// filter analysis and identity recording would pair the old body with a fingerprint of the new
    /// one, and every later validation would pass while the plan keeps enforcing the old body. SQL
    /// UDFs in the query itself make it uncacheable for a similar reason (see
    /// `isFunctionUnsafeForPlanCache`).
    bool has_sql_udf = false;
    /// A call to a name that no factory knows, e.g. a SQL UDF dropped after the filter was analyzed:
    /// the plan would keep its inlined body while the raw filter AST stays the same.
    bool has_unknown_function = false;
    /// A call to a non-deterministic function (e.g. `today`, `now`): the filter is planned into the
    /// cached plan itself, so its value could be frozen at the time the plan was analyzed - a hit
    /// would keep enforcing the analysis-time cutoff instead of the current one. This mirrors the
    /// check that `isFunctionUnsafeForPlanCache` applies to the visible query.
    bool has_non_deterministic_function = false;
    /// A call to a function that checks access rights when it is created (see
    /// `isFunctionCheckingAccessOnCreate`): a hit does not create it again, so the check is lost.
    bool has_function_checking_access = false;

    bool isCacheable() const
    {
        return !has_subquery && !has_function_resolved_outside_function_factory && !has_sql_udf && !has_unknown_function
            && !has_non_deterministic_function && !has_function_checking_access;
    }
};

void collectRowPolicyFilterContents(const IAST & ast, const ContextPtr & context, RowPolicyFilterContents & contents)
{
    if (ast.as<ASTSubquery>() || ast.as<ASTSelectQuery>() || ast.as<ASTSelectWithUnionQuery>())
        contents.has_subquery = true;

    if (const auto * function = ast.as<ASTFunction>())
    {
        if (isFunctionCheckingAccessOnCreate(function->name))
            contents.has_function_checking_access = true;

        if (isFunctionResolvedOutsideFunctionFactory(function->name, context))
            contents.has_function_resolved_outside_function_factory = true;
        else if (UserDefinedSQLFunctionFactory::instance().has(function->name))
            contents.has_sql_udf = true;
        else if (function->name != "lambda" && !FunctionFactory::instance().hasNameOrAlias(function->name))
            contents.has_unknown_function = true;
        /// `arrayJoin` reports `isDeterministic() = false` because it is multi-valued, but it is pure.
        else if (getFunctionCanonicalNameIfAny(function->name) != "arrayJoin")
        {
            if (const auto func = FunctionFactory::instance().tryGet(function->name, context); func && !func->isDeterministic())
                contents.has_non_deterministic_function = true;
        }
    }

    for (const auto & child : ast.children)
        collectRowPolicyFilterContents(*child, context, contents);
}

struct RowPolicyInfo
{
    /// Hash of the effective SELECT row-policy filter (empty when there is no restrictive policy).
    IASTHash hash{};
    /// What makes the plan uncacheable, if anything (see `RowPolicyFilterContents`).
    RowPolicyFilterContents contents;
};

RowPolicyInfo getRowPolicyInfo(const ContextPtr & context, const RowPolicyFilterPtr & row_policy)
{
    RowPolicyInfo info;
    if (row_policy && !row_policy->isAlwaysTrue() && row_policy->expression)
    {
        collectRowPolicyFilterContents(*row_policy->expression, context, info.contents);
        info.hash = row_policy->expression->getTreeHash(/*ignore_aliases=*/false);
    }
    return info;
}

RowPolicyInfo getRowPolicyInfo(const ContextPtr & context, const String & database, const String & table)
{
    return getRowPolicyInfo(context, context->getRowPolicyFilter(database, table, RowPolicyFilterType::SELECT_FILTER));
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

/// True if a cacheable logical plan inlines this storage's body instead of leaving a
/// `ReadFromTable` leaf for it. Only `StorageView` qualifies - `PlannerJoinTree` expands exactly
/// `typeid_cast<const StorageView *>(storage.get())` (see `expand_view_in_logical_plan`), while a
/// `StorageMaterializedView` stays a leaf and is executed by `StorageMaterializedView::readImpl`
/// against its target table, exactly as on the miss path. Keying the dependency walk off the
/// broader `isView` would treat a materialized view as expanded and therefore
/// (a) mark its dependency `columns_unknown`, discarding the exact leaf columns and upgrading the
/// hit recheck to table-level `SELECT` - a user holding only `SELECT(a)` on the view could run the
/// query on a miss and then get `ACCESS_DENIED` on the hit - and
/// (b) add the tables of the view's defining `SELECT` as dependencies, although reading a
/// materialized view does not read them.
bool isViewExpandedInCacheablePlan(const StoragePtr & storage)
{
    return typeid_cast<const StorageView *>(storage.get()) != nullptr;
}

/// True if the storage re-checks column-level `SELECT` access on *another* table while executing
/// its own `read`, using the very column names the plan reads: `StorageMaterializedView::readImpl`
/// checks them against the source table of its defining `SELECT` (and against an explicit `TO`
/// target), `StorageBuffer::read` checks them against the destination table. Such a check is not
/// replayable from the cached plan's dependencies, because the plan carries the column names that
/// planning chose at store time.
bool storageRechecksColumnAccessOnRead(const StoragePtr & storage)
{
    /// A lazily loaded table is reached through `StorageTableProxy`, which forwards `read` to the
    /// concrete engine but not its type, so test the engine behind the proxy.
    const auto * nested = unwrapStorageProxy(storage).get();
    return typeid_cast<const StorageMaterializedView *>(nested) != nullptr
        || typeid_cast<const StorageBuffer *>(nested) != nullptr;
}

/// True if the storage checks access to *another* object while executing its own `read`: besides
/// the engines of `storageRechecksColumnAccessOnRead`, a `Dictionary` table checks `SELECT` or
/// `dictGet` on the underlying dictionary, which can have a different name and is not a dependency
/// of its own. Such a check repeats from the plan leaf on every execution, but not for a table read
/// inside a scalar subquery, which is folded into a constant during analysis.
bool storageChecksAccessToAnotherObjectOnRead(const StoragePtr & storage)
{
    return storageRechecksColumnAccessOnRead(storage)
        || typeid_cast<const StorageDictionary *>(unwrapStorageProxy(storage).get()) != nullptr;
}

/// Returns false if the storage's engine or view security makes the plan uncacheable. This is
/// checked both when a dependency is recorded (`fillDependency`) and when a cache entry is
/// validated on a hit (`validateQueryPlanCacheEntry`): in UUID-less databases a `DROP`/`CREATE`
/// can swap in an unsupported engine (e.g. `Distributed`/`Merge`) or change a view's SQL
/// security while leaving the schema content hash unchanged, which a plain hash comparison would
/// miss, so the eligibility must be re-checked rather than trusted from store time.
bool isStorageEligibleForPlanCache(const StoragePtr & storage, const StorageMetadataPtr & metadata)
{
    const auto & storage_id = storage->getStorageID();
    const String & database = storage_id.getDatabaseName();
    const String & table = storage_id.table_name;

    if (database == DatabaseCatalog::TEMPORARY_DATABASE
        || (database == DatabaseCatalog::SYSTEM_DATABASE && !isAllowedSystemTable(database, table))
        || storage->isRemote()
        || typeid_cast<const StorageMerge *>(unwrapStorageProxy(storage).get()))
    {
        LOG_DEBUG(getLogger("QueryPlanCache"), "Not caching plan: dependency {}.{} is temporary, system, remote or Merge",
            database, table);
        return false;
    }

    /// An `Alias` forwards every read to its target table, and the semantics of that target are
    /// not representable in this dependency record, which describes the alias only:
    ///   - `PlannerJoinTree::getEffectiveRowPolicyFilter` bakes the *combination* of the alias's
    ///     and the target's `SELECT` row policies into the plan, while `dep.row_policy_hash`
    ///     fingerprints the alias's policy alone - tightening the target's policy would leave the
    ///     entry valid and a hit would keep returning rows the current policy hides. For the same
    ///     reason the store-time "row policy contains a subquery / calls an executable UDF" guard
    ///     never sees the target's policy.
    ///   - `StorageAlias::read` re-checks the plan's column names against the target table, so a
    ///     cached zero-column read replays the helper column chosen at store time (the divergence
    ///     `storageRechecksColumnAccessOnRead` describes for `MaterializedView` and `Buffer`).
    ///   - The target is not a dependency of its own, so neither its schema nor - for an alias
    ///     folded into a scalar subquery, where no plan leaf survives to re-check it - its column
    ///     grants are revalidated on a hit.
    /// Modelling all of this would mean resolving the target and recording it as a second
    /// dependency with the combined filter; until then, refuse to cache reads through an `Alias`.
    if (typeid_cast<const StorageAlias *>(unwrapStorageProxy(storage).get()))
    {
        LOG_DEBUG(getLogger("QueryPlanCache"),
            "Not caching plan: dependency {}.{} is an Alias, whose target table's row policies and column grants "
            "are not part of the dependency record", database, table);
        return false;
    }

    /// Every guard of the "analyzed storage == executed storage" invariant - the pre-store
    /// cross-check against the planning-time identities, the hit-path dependency validation and
    /// `QueryPlan::resolveStorages` - compares storage UUIDs. A table without a UUID (a table in
    /// an `Ordinary` or `Lazy` database) makes all of them vacuous: after a `DROP`/`CREATE` both
    /// the old and the new table carry `Nil`, so a swapped table would pass every comparison and
    /// a stale plan could execute the new table with the old table's baked row policies or view
    /// expansions. Such storages cannot be identity-bound, so they are not cacheable. The allowed
    /// system tables (`system.one`) are exempt: they are constant and cannot be replaced.
    if (storage_id.uuid == UUIDHelpers::Nil && !isAllowedSystemTable(database, table))
    {
        LOG_DEBUG(getLogger("QueryPlanCache"),
            "Not caching plan: dependency {}.{} has no UUID (a database engine that does not support atomic table replacement), "
            "so the plan cannot be bound to the analyzed table identity", database, table);
        return false;
    }

    /// A DEFINER view executes its body under the definer's rights; a NONE view executes it
    /// under the global context. Either way the cached plan resolves the expanded view leaves
    /// under the invoker context and cannot replay that overridden security context, so only
    /// `INVOKER` views (and views with no explicit security, which default to invoker) are
    /// eligible.
    if (storage->isView() && metadata->sql_security_type && *metadata->sql_security_type != SQLSecurityType::INVOKER)
    {
        LOG_DEBUG(getLogger("QueryPlanCache"), "Not caching plan: view {}.{} is not SQL SECURITY INVOKER", database, table);
        return false;
    }

    return true;
}

/// Table constraints are analyzed into the plan by the constraint-based optimizations
/// (`convert_query_to_cnf`, `optimize_using_constraints`, `optimize_substitute_columns`): their
/// expressions are folded and used to rewrite the query predicate, while they never appear in the
/// analyzed query tree checked by `queryTreeIsEligibleForPlanCache`. A constraint calling a
/// non-deterministic function (e.g. `getSetting`) would be frozen into the cached plan, and the
/// constraint text in the schema fingerprint does not capture the folded value.
bool constraintsContainFunctionsUnsafeForQueryPlanCache(const StorageInMemoryMetadata & metadata, const ContextPtr & context)
{
    for (const auto & constraint : metadata.getConstraints().getConstraints())
        if (astContainsFunctionsUnsafeForQueryPlanCache(constraint, context))
            return true;
    return false;
}

/// Fills a dependency record from a resolved storage. Returns false if the storage makes the
/// plan uncacheable.
bool fillDependency(QueryPlanCacheDependency & dep, const StoragePtr & storage, const ContextPtr & context)
{
    const auto & storage_id = storage->getStorageID();
    dep.database = storage_id.getDatabaseName();
    dep.table = storage_id.table_name;
    dep.uuid = storage_id.uuid;

    auto metadata = storage->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/false);

    if (!isStorageEligibleForPlanCache(storage, metadata))
        return false;

    dep.metadata_version = getMetadataVersionOrSchemaHash(metadata);

    /// A row-policy filter that contains a subquery reads other tables and bakes their results (a
    /// scalar boundary, or an `IN` set) into the plan as constants, without those tables ever
    /// becoming plan leaves or entries of the AST closure walked by `ASTDependencyCollector`. The
    /// dependency fingerprint would then stay unchanged when such a table changes, so a hit could
    /// keep enforcing a stale row boundary and expose or deny rows incorrectly; the filter also
    /// escapes `query_plan_cache_allow_scalar_subqueries`, which is applied only to the user query
    /// AST. Refuse to cache these plans.
    const auto row_policy_info = getRowPolicyInfo(context, dep.database, dep.table);
    if (row_policy_info.contents.has_subquery)
    {
        LOG_DEBUG(getLogger("QueryPlanCache"), "Not caching plan: row policy on {}.{} contains a subquery", dep.database, dep.table);
        return false;
    }
    /// A row-policy filter is applied during planning, so a UDF it calls never appears in the
    /// analyzed query tree checked by `queryTreeIsEligibleForPlanCache` - it has to be rejected here.
    if (row_policy_info.contents.has_function_resolved_outside_function_factory)
    {
        LOG_DEBUG(getLogger("QueryPlanCache"),
            "Not caching plan: row policy on {}.{} calls an executable or WebAssembly UDF", dep.database, dep.table);
        return false;
    }
    if (row_policy_info.contents.has_sql_udf)
    {
        LOG_DEBUG(getLogger("QueryPlanCache"), "Not caching plan: row policy on {}.{} calls a SQL UDF", dep.database, dep.table);
        return false;
    }
    if (row_policy_info.contents.has_unknown_function)
    {
        LOG_DEBUG(getLogger("QueryPlanCache"), "Not caching plan: row policy on {}.{} calls an unknown function", dep.database, dep.table);
        return false;
    }
    if (row_policy_info.contents.has_non_deterministic_function)
    {
        LOG_DEBUG(getLogger("QueryPlanCache"),
            "Not caching plan: row policy on {}.{} calls a non-deterministic function", dep.database, dep.table);
        return false;
    }
    if (row_policy_info.contents.has_function_checking_access)
    {
        LOG_DEBUG(getLogger("QueryPlanCache"),
            "Not caching plan: row policy on {}.{} calls a function that checks access rights", dep.database, dep.table);
        return false;
    }

    /// `ALIAS` column expressions are substituted into the query during analysis. For a table read
    /// directly by the query they appear in the analyzed query tree checked by
    /// `queryTreeIsEligibleForPlanCache`, but not for a table read inside an expanded view body, which
    /// is analyzed separately. A constant `ALIAS` expression is folded into the plan, so a
    /// non-deterministic function would be frozen and a function checking access rights would not
    /// repeat its check on a hit.
    for (const auto & column : metadata->getColumns())
    {
        if (column.default_desc.kind == ColumnDefaultKind::Alias && column.default_desc.expression
            && astContainsFunctionsUnsafeForQueryPlanCache(column.default_desc.expression, context))
        {
            LOG_DEBUG(getLogger("QueryPlanCache"),
                "Not caching plan: ALIAS column {} of {}.{} uses a function unsafe for the plan cache", column.name, dep.database, dep.table);
            return false;
        }
    }

    if (constraintsContainFunctionsUnsafeForQueryPlanCache(*metadata, context))
    {
        LOG_DEBUG(getLogger("QueryPlanCache"),
            "Not caching plan: a constraint of {}.{} uses a function unsafe for the plan cache", dep.database, dep.table);
        return false;
    }

    dep.row_policy_hash = row_policy_info.hash;
    /// Only an expanded view has no plan leaf of its own and is therefore reported as a view in the
    /// query-log access info; a materialized view is read like an ordinary table.
    dep.is_view = isViewExpandedInCacheablePlan(storage);
    return true;
}

/// Returns true for functions whose results must not be frozen into a cached plan.
/// `arrayJoin` reports `isDeterministic() = false` because it is multi-valued, but it is
/// pure (same argument - same set of rows), so it is safe for the plan cache.
bool isFunctionUnsafeForPlanCache(const ASTFunction & function, const ContextPtr & context)
{
    if (getFunctionCanonicalNameIfAny(function.name) == "arrayJoin")
        return false;

    if (isFunctionCheckingAccessOnCreate(function.name))
        return true;

    /// Checked before `FunctionFactory`, mirroring the resolution order of the analyzer.
    if (isFunctionResolvedOutsideFunctionFactory(function.name, context))
        return true;

    if (const auto func = FunctionFactory::instance().tryGet(function.name, context))
        return !func->isDeterministic();

    /// SQL-defined UDFs: determinism is unknown, assume the worst.
    if (UserDefinedSQLFunctionFactory::instance().tryGet(function.name))
        return true;

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
        /// The `SETTINGS` clause of the top-level `SELECT` (of the last `SELECT` of a top-level
        /// `UNION`) is applied to the query context by `InterpreterSetQuery::applySettingsFromQuery`
        /// before the query is interpreted, so it is part of the cache key and the plan is
        /// materialized with it.
        select_with_query_context_settings = getSelectWithQueryContextSettings(ast);

        return collectImpl(ast, default_database, /*in_set_or_table_position=*/ false, /*inside_scalar_subquery=*/ false);
    }

private:
    bool collectSelectQuery(const ASTSelectQuery & select, const String & default_database, bool inside_scalar_subquery)
    {
        /// Any other `SETTINGS` clause (of a subquery, of another `SELECT` of a `UNION`, of an expanded
        /// view body) is applied only to the context in which that part of the query is analyzed. The
        /// logical plan does not carry that context: `resolveStorages` builds every storage read, and
        /// the deserialization of a cached plan builds every function, with the context of the whole
        /// query, so the scoped settings would be lost (for example
        /// `do_not_merge_across_partitions_select_final` of a view body over a `FINAL` read).
        if (select.settings() && &select != select_with_query_context_settings)
        {
            LOG_DEBUG(getLogger("QueryPlanCache"),
                "Not caching plan: a subquery or view body has its own SETTINGS clause, which the logical plan does not preserve");
            return false;
        }

        cte_scopes.emplace_back();
        SCOPE_EXIT({ cte_scopes.pop_back(); });

        const auto with = select.with();
        if (with)
        {
            for (const auto & child : with->children)
            {
                if (const auto * cte = child->as<ASTWithElement>())
                    cte_scopes.back().emplace(cte->name, cte);
            }

            /// A `WITH <expression> AS <alias>` element is not a CTE: it is substituted at its
            /// expression use sites, which are plain identifiers that this walk cannot map back
            /// to the alias. A scalar subquery there (`WITH (SELECT max(v) FROM t) AS m`) is
            /// folded into a constant, so its tables must be recorded and the scalar-subquery
            /// gate applied. Walk these elements at the definition site, which is conservative
            /// for an alias that ends up unused.
            for (const auto & child : with->children)
            {
                if (child->as<ASTWithElement>())
                    continue;
                if (!collectImpl(*child, default_database, /*in_set_or_table_position=*/ false, inside_scalar_subquery))
                    return false;
            }
        }

        for (const auto & child : select.children)
        {
            if (child == with)
                continue;
            if (!collectImpl(*child, default_database, /*in_set_or_table_position=*/ false, inside_scalar_subquery))
                return false;
        }
        return true;
    }

    const ASTWithElement * findCTE(const StorageID & table_id) const
    {
        /// A qualified identifier always names a storage. CTE names are unqualified and shadow
        /// catalog names only in their lexical query scope.
        if (!table_id.database_name.empty())
            return nullptr;

        for (auto it = cte_scopes.rbegin(); it != cte_scopes.rend(); ++it)
            if (auto cte_it = it->find(table_id.table_name); cte_it != it->end())
                return cte_it->second;
        return nullptr;
    }

    bool collectCTE(const ASTWithElement & cte, const String & default_database, bool inside_scalar_subquery)
    {
        /// A CTE body is evaluated in the position where the CTE is referenced. This preserves
        /// the scalar-subquery access rule for a CTE used only as a scalar, while allowing a
        /// table CTE to be cached without the scalar-subquery opt-in.
        if (!visited_ctes.insert({&cte, inside_scalar_subquery}).second)
            return true;

        return cte.subquery
            && collectImpl(*cte.subquery, default_database, /*in_set_or_table_position=*/ true, inside_scalar_subquery);
    }

    bool collectImpl(const IAST & ast, const String & default_database, bool in_set_or_table_position, bool inside_scalar_subquery)
    {
        if (const auto * select = ast.as<ASTSelectQuery>())
            return collectSelectQuery(*select, default_database, inside_scalar_subquery);

        if (const auto * table_expression = ast.as<ASTTableExpression>())
        {
            if (table_expression->table_function)
            {
                LOG_DEBUG(getLogger("QueryPlanCache"), "Not caching plan: query or view body uses a table function");
                return false;
            }
            if (table_expression->database_and_table_name)
                if (!visitTableIdentifier(*table_expression->database_and_table_name, default_database, inside_scalar_subquery))
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
                if (!collectImpl(*args[0], default_database, /*in_set_or_table_position=*/ false, inside_scalar_subquery))
                    return false;

                /// `x IN name` names a table, a view or a `Set` storage directly. The parser emits a
                /// plain `ASTIdentifier` there, not an `ASTTableExpression`, so route it through
                /// `visitTableIdentifier` explicitly. Otherwise a view on the right side is expanded
                /// into the set sub-plan and only its body tables are recorded, so a hit would skip
                /// the revalidation and access recheck of the view itself, and a `Set` storage (which
                /// has no sub-plan at all) would not be recorded. Should the name actually resolve to
                /// something else, recording an existing table of the same name is only conservative.
                if (const auto * identifier = args[1]->as<ASTIdentifier>())
                {
                    if (auto table_identifier = identifier->createTable())
                        return visitTableIdentifier(*table_identifier, default_database, inside_scalar_subquery);
                    return true;
                }

                /// The set (right) side is re-executed each run and its tables become plan leaves
                /// in the IN-subquery sub-plan, so they are not folded; keep `inside_scalar_subquery`.
                return collectImpl(*args[1], default_database, /*in_set_or_table_position=*/ true, inside_scalar_subquery);
            }

            in_set_or_table_position = false;
        }
        else if (ast.as<ASTSubquery>())
        {
            /// A subquery outside FROM / IN position is a scalar subquery: it is evaluated
            /// during analysis and its result is baked into the plan as a constant.
            if (!in_set_or_table_position)
            {
                if (!allow_scalar_subqueries)
                {
                    LOG_DEBUG(getLogger("QueryPlanCache"),
                        "Not caching plan: query or view body uses a scalar subquery "
                        "(see query_plan_cache_allow_scalar_subqueries)");
                    return false;
                }
                /// Tables read inside this subquery are folded into a constant and have no plan
                /// leaf, so their exact columns cannot be recovered later.
                inside_scalar_subquery = true;
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
            if (!collectImpl(*child, default_database, in_set_or_table_position, inside_scalar_subquery))
                return false;

        return true;
    }

    bool visitTableIdentifier(const IAST & identifier_ast, const String & default_database, bool inside_scalar_subquery)
    {
        const auto * identifier = identifier_ast.as<ASTTableIdentifier>();
        if (!identifier)
            return true;

        auto table_id = identifier->getTableId();
        if (const auto * cte = findCTE(table_id))
            return collectCTE(*cte, default_database, inside_scalar_subquery);

        if (table_id.database_name.empty())
            table_id.database_name = default_database;

        /// Unresolvable names are not necessarily errors: the identifier may refer to a CTE.
        /// Skipping them is safe - a CTE is not a storage, so it cannot go stale.
        auto storage = DatabaseCatalog::instance().tryGetTable(table_id, context);
        if (!storage)
            return true;

        const auto & resolved_id = storage->getStorageID();
        /// Visit each storage once per scalar/non-scalar context. The same table read both inside
        /// a scalar subquery (folded, columns unknown) and in an ordinary position (a plan leaf
        /// with known columns) must contribute both occurrences, so that the merge in
        /// `collectQueryPlanCacheDependencies` unions them and the unknown flag wins. Keying only
        /// on the table name would let traversal order decide which flag survives.
        if (!visited.insert({resolved_id.getFullTableName(), inside_scalar_subquery}).second)
            return true;

        /// `MaterializedView` and `Buffer` check `SELECT` on another table (the defining source,
        /// the destination) inside their own `read`, and `Dictionary` checks access to the underlying
        /// dictionary. For an ordinary read that check replays from
        /// the plan leaf on every execution, but a scalar subquery is folded into a constant during
        /// analysis, so a hit would never repeat it and would keep serving the value after the grant
        /// on that other table was revoked. Such secondary requirements are not part of the
        /// dependency record, so do not cache them inside folded scalar subqueries.
        if (inside_scalar_subquery && storageChecksAccessToAnotherObjectOnRead(storage))
        {
            LOG_DEBUG(getLogger("QueryPlanCache"),
                "Not caching plan: scalar subquery reads {}, whose engine checks access to another object while reading",
                resolved_id.getNameForLogs());
            return false;
        }

        QueryPlanCacheDependency dep;
        if (!fillDependency(dep, storage, context))
            return false;
        /// A dependency whose exact selected columns cannot be recovered here must have a hit
        /// recheck table-level SELECT: a column-level "any column" recheck could otherwise pass
        /// after the actually-read column's grant was revoked, while the baked plan still returns
        /// that column. Two cases qualify:
        ///   - A table reached only through a scalar subquery is folded into a constant during
        ///     analysis, so it has no plan leaf.
        ///   - A view expanded into the plan has no plan leaf either (only its body tables do). The
        ///     miss path checks the precise selected view columns in
        ///     `prepareBuildQueryPlanForTableExpression` before expanding the view; recording
        ///     table-level here keeps a hit at least as strict (mirroring the scalar-subquery case,
        ///     and matching the conservative choice not to recover the exact view columns).
        /// The view's body tables are still visited below and recorded with their precise columns
        /// as ordinary plan leaves, so this only tightens the recheck of the view storage itself.
        /// A materialized view is not expanded (see `isViewExpandedInCacheablePlan`): it keeps its
        /// own plan leaf with the exact columns, so it must not be marked here.
        dep.columns_unknown = inside_scalar_subquery || isViewExpandedInCacheablePlan(storage);
        dependencies.push_back(std::move(dep));

        /// Recurse into the bodies of expanded views: nested views and their tables are inlined into
        /// the plan and are dependencies too. Names inside a view body resolve against the view's
        /// own database, and the body is stored in `select.inner_query`. A view referenced from a
        /// scalar subquery is itself folded, so propagate `inside_scalar_subquery` into its body.
        /// A materialized view's defining `SELECT` is deliberately not walked: reading the view
        /// reads its target table, not the sources of that `SELECT`.
        if (isViewExpandedInCacheablePlan(storage))
        {
            auto metadata = storage->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/false);
            const auto & view_query = metadata->select.inner_query;
            if (view_query)
            {
                /// A stored view has its own lexical scope. Its definition is analyzed in a
                /// fresh context, so an outer query's CTE names cannot shadow table names in
                /// the view body while collecting cache dependencies.
                auto outer_cte_scopes = std::move(cte_scopes);
                SCOPE_EXIT({ cte_scopes = std::move(outer_cte_scopes); });
                if (!collectImpl(*view_query, resolved_id.getDatabaseName(), /*in_set_or_table_position=*/ false, inside_scalar_subquery))
                    return false;
            }
        }

        return true;
    }

    const ContextPtr & context;
    std::vector<QueryPlanCacheDependency> & dependencies;
    bool allow_scalar_subqueries = false;
    /// The `SELECT` whose `SETTINGS` clause is applied to the query context - see `collect`.
    const ASTSelectQuery * select_with_query_context_settings = nullptr;
    std::vector<std::map<String, const ASTWithElement *>> cte_scopes;
    std::set<std::pair<const ASTWithElement *, bool>> visited_ctes;
    /// (full table name, was reached inside a scalar subquery) - see `visitTableIdentifier`.
    std::set<std::pair<String, bool>> visited;
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
            /// `ReadFromTableStep::serialize` does not carry the `STREAM` modifier, so a deserialized
            /// plan would silently turn a streaming read (cursor, subscription) into an ordinary
            /// snapshot read on a hit.
            if (read_from_table->getTableExpressionModifiers().hasStream())
            {
                LOG_DEBUG(getLogger("QueryPlanCache"), "Not caching plan: streaming read of {}", read_from_table->getTable());
                return false;
            }

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
            /// A zero-column read (`SELECT count() FROM t`, `SELECT 1 FROM t`) outputs a single
            /// helper column the planner injected among the *currently granted* ones purely to let
            /// the storage produce rows; it is not the query's access contract. Recording it would
            /// make a hit require SELECT on that particular column, while a miss re-plans and
            /// succeeds with any granted column. Leave the columns empty (and known): the hit
            /// re-check then applies the same "SELECT on at least one column" rule as planning.
            if (read_from_table->readsOnlyInjectedColumn())
            {
                /// The helper column is not part of the query's access contract, so the hit
                /// recheck deliberately falls back to the "SELECT on at least one column" rule
                /// used by planning (see below). A storage that re-checks the *plan's* column
                /// names against another table while reading escapes that relaxation: the cached
                /// plan replays the helper column chosen at store time, so a hit throws
                /// `ACCESS_DENIED` once that particular column is revoked, while a miss re-plans
                /// with another granted column and succeeds. Do not cache such reads.
                if (storageRechecksColumnAccessOnRead(table_node->getStorage()))
                {
                    LOG_DEBUG(getLogger("QueryPlanCache"),
                        "Not caching plan: zero-column read of {}.{} would replay the column chosen at planning time "
                        "into the engine's own access check", dep.database, dep.table);
                    return false;
                }
                /// The helper column is not recorded, but the columns resolved away before planning
                /// (`indexHint` arguments) are still required by a miss, so a hit must recheck them.
                dep.access_checked_columns = read_from_table->getAccessCheckedColumns();
            }
            else
            {
                dep.columns = read_from_table->getOutputHeader()->getNames();
                /// The access re-check must use the columns whose privileges planning verified,
                /// not the physical read set: they differ for `ALIAS` columns (see
                /// `ReadFromTableStep::getAccessCheckedColumns`).
                dep.access_checked_columns = read_from_table->getAccessCheckedColumns();
            }
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

static UInt64 combineSemanticsFingerprint(Int64 metadata_version, const IASTHash & row_policy_hash)
{
    SipHash hash;
    hash.update(metadata_version);
    hash.update(row_policy_hash.low64);
    hash.update(row_policy_hash.high64);
    return hash.get64();
}

UInt64 computeQueryPlanCacheSemanticsFingerprint(
    const StorageMetadataPtr & metadata, const RowPolicyFilterPtr & row_policy, const ContextPtr & context)
{
    return combineSemanticsFingerprint(getMetadataVersionOrSchemaHash(metadata), getRowPolicyInfo(context, row_policy).hash);
}

UInt64 computeQueryPlanCacheSemanticsFingerprint(const QueryPlanCacheDependency & dep)
{
    return combineSemanticsFingerprint(dep.metadata_version, dep.row_policy_hash);
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
            /// `arrayJoin` reports `isDeterministic() = false` because it is multi-valued, but it is
            /// pure (same argument - same set of rows), so it is safe for the plan cache. Mirror the
            /// AST-side exemption in `isFunctionUnsafeForPlanCache`; without it any query (or expanded
            /// view body) using `arrayJoin` would be wrongly rejected, contradicting the cache contract.
            if (function_node->getFunctionName() != "arrayJoin")
            {
                /// A function that `ActionsDAG::deserialize` cannot rebuild (an executable or a
                /// WebAssembly UDF) makes a hit throw or, worse, call a shadowed builtin instead.
                /// Mirrors `isFunctionUnsafeForPlanCache` on the AST side; the analyzed tree also
                /// covers calls that the query text does not show, such as one inlined from the
                /// body of a SQL UDF.
                if (isFunctionResolvedOutsideFunctionFactory(function_node->getFunctionName(), context))
                    return false;

                /// A folded call of a function that checks access rights when it is created would
                /// lose that check on a hit (see `isFunctionCheckingAccessOnCreate`).
                if (isFunctionCheckingAccessOnCreate(function_node->getFunctionName()))
                    return false;

                const auto & function = function_node->getFunction();
                /// An unresolved ordinary function after analysis is unexpected; refuse to cache.
                if (!function || !function->isDeterministic())
                    return false;
            }
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

    /// Merge dependencies that refer to the same (database, table). A storage can appear more
    /// than once - a self-join reads it through two plan leaves, and a table can be read both in
    /// the outer plan and inside a subquery or view body. The column requirements of every
    /// occurrence must be combined: keeping only one (as a plain dedup would) could drop a column
    /// that the cached plan still reads, letting a hit pass the access recheck after that
    /// column's grant was revoked. Unioning the column sets, and treating any occurrence with
    /// unknown columns as requiring table-level access, keeps the recheck at least as strict as
    /// normal planning.
    std::map<std::pair<String, String>, QueryPlanCacheDependency> merged;
    for (auto & dep : dependencies)
    {
        auto [it, inserted] = merged.try_emplace({dep.database, dep.table}, std::move(dep));
        if (inserted)
            continue;

        auto & target = it->second;
        /// uuid / metadata_version / row_policy_hash are identical across occurrences of the
        /// same storage; only the access-relevant fields differ and must be combined.
        target.columns_unknown = target.columns_unknown || dep.columns_unknown;
        target.is_view = target.is_view || dep.is_view;
        target.columns.insert(target.columns.end(), dep.columns.begin(), dep.columns.end());
        target.access_checked_columns.insert(
            target.access_checked_columns.end(), dep.access_checked_columns.begin(), dep.access_checked_columns.end());
    }

    std::vector<QueryPlanCacheDependency> result;
    result.reserve(merged.size());
    for (auto & [_, dep] : merged)
    {
        if (dep.columns_unknown)
        {
            /// Columns are rechecked at the table level; the partial column list is meaningless.
            dep.columns.clear();
        }
        else
        {
            std::sort(dep.columns.begin(), dep.columns.end());
            dep.columns.erase(std::unique(dep.columns.begin(), dep.columns.end()), dep.columns.end());
        }
        result.push_back(std::move(dep));
    }

    return result;
}

namespace
{

/// The metadata expressions of a dependency that are analyzed into a cacheable plan: the body of an
/// expanded view, the `ALIAS` column expressions and the constraints. They are checked with the same
/// predicate when the plan is stored (`ASTDependencyCollector` and `fillDependency`).
bool areMetadataExpressionsStillCacheable(const StoragePtr & storage, const StorageInMemoryMetadata & metadata, const ContextPtr & context)
{
    if (isViewExpandedInCacheablePlan(storage) && metadata.select.inner_query
        && astContainsFunctionsUnsafeForQueryPlanCache(metadata.select.inner_query, context))
        return false;

    for (const auto & column : metadata.getColumns())
    {
        if (column.default_desc.kind == ColumnDefaultKind::Alias && column.default_desc.expression
            && astContainsFunctionsUnsafeForQueryPlanCache(column.default_desc.expression, context))
            return false;
    }

    return !constraintsContainFunctionsUnsafeForQueryPlanCache(metadata, context);
}

}

bool validateQueryPlanCacheEntry(
    const QueryPlanCacheEntry & entry,
    const ContextPtr & context,
    QueryPlan::ExpectedStorageIdentities & validated_identities)
{
    validated_identities.clear();

    for (const auto & dep : entry.dependencies)
    {
        auto storage = DatabaseCatalog::instance().tryGetTable(StorageID{dep.database, dep.table}, context);
        if (!storage)
            return false;

        const auto & storage_id = storage->getStorageID();
        if (storage_id.uuid != dep.uuid)
            return false;

        auto metadata = storage->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/false);

        /// Re-check engine and view-security eligibility. In UUID-less databases a `DROP`/`CREATE`
        /// can replace a table with the same columns but an unsupported engine, or change a view's
        /// SQL security, while the schema content hash stays equal; without this re-check a hit
        /// could execute against a storage that a miss would have refused to cache.
        if (!isStorageEligibleForPlanCache(storage, metadata))
            return false;

        if (getMetadataVersionOrSchemaHash(metadata) != dep.metadata_version)
            return false;

        /// The schema fingerprint covers the metadata ASTs, not how their function names resolve.
        /// An executable or WebAssembly UDF loaded after the plan was stored can shadow a builtin
        /// used by an expanded view body or an `ALIAS` expression: the AST stays the same, but a
        /// fresh analysis would now call the UDF while the cached plan keeps calling the builtin.
        /// Re-walk these expressions on every hit, as the user query AST is re-walked in
        /// `astContainsFunctionsUnsafeForQueryPlanCache` before each lookup.
        if (!areMetadataExpressionsStillCacheable(storage, *metadata, context))
            return false;

        /// A policy that changed to contain a subquery, a non-deterministic function or a call to a UDF
        /// (or that changed in any other way) alters the hash and is rejected here; the explicit
        /// content guards are defense in depth, mirroring the store path in `fillDependency`. They
        /// also catch a change the raw filter AST does not show: a name of the filter that started
        /// resolving to a SQL UDF, or stopped resolving at all.
        const auto row_policy_info = getRowPolicyInfo(context, dep.database, dep.table);
        if (!row_policy_info.contents.isCacheable() || row_policy_info.hash != dep.row_policy_hash)
            return false;

        /// Record the identity that was just proven, so that materialization can bind its leaf
        /// reads to it instead of trusting a second, independent name resolution. The semantics
        /// fingerprint travels along: the UUID pins the table object, but only the fingerprint
        /// lets `resolveStorages` see a same-UUID in-place `ALTER` landing after this check.
        validated_identities[{storage_id.getDatabaseName(), storage_id.table_name}]
            = {storage_id.uuid, computeQueryPlanCacheSemanticsFingerprint(dep)};
    }

    return true;
}

void checkAccessForQueryPlanCacheHit(const QueryPlanCacheEntry & entry, const ContextPtr & context)
{
    for (const auto & dep : entry.dependencies)
    {
        if (isAllowedSystemTable(dep.database, dep.table))
            continue;

        /// Columns are unknown (a view expanded at plan time, or a scalar-subquery table folded
        /// into a constant - neither has a plan leaf whose precise columns could be recorded):
        /// the baked plan may read any column, so a column-level recheck is not enough. Require
        /// table-level SELECT, which is at least as strict as the per-column access a miss checks;
        /// a hit cannot recover the exact columns to recheck them precisely. The caller re-plans
        /// instead of hitting when this would fail (`hasTableLevelAccessForColumnsUnknownDependencies`),
        /// so that a hit is never stricter than a miss; this check stays as a safety net.
        if (dep.columns_unknown)
        {
            context->checkAccess(AccessType::SELECT, dep.database, dep.table);
            continue;
        }

        /// Recheck the columns planning checked (`ALIAS` columns as selected, not as read - see
        /// `QueryPlanCacheDependency::access_checked_columns`), so that a hit enforces exactly the
        /// grants a miss enforces.
        if (!dep.access_checked_columns.empty())
        {
            context->checkAccess(AccessType::SELECT, StorageID{dep.database, dep.table}, dep.access_checked_columns);
            continue;
        }

        /// No specific columns recorded and they are known (e.g. a `count()` over the table):
        /// require SELECT on at least one column, mirroring the access semantics of
        /// `SELECT count()`.
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

bool hasTableLevelAccessForColumnsUnknownDependencies(
    const std::vector<QueryPlanCacheDependency> & dependencies, const ContextPtr & context)
{
    auto access = context->getAccess();
    for (const auto & dep : dependencies)
    {
        if (!dep.columns_unknown || isAllowedSystemTable(dep.database, dep.table))
            continue;
        if (!access->isGranted(AccessType::SELECT, dep.database, dep.table))
            return false;
    }
    return true;
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
        {
            Names columns = dep.columns;
            if (dep.columns_unknown)
            {
                /// An AST-only dependency (for example, a scalar subquery folded into a
                /// constant) has no plan leaf from which to recover its exact output columns.
                /// Do not omit it from query-log metadata: conservatively report every current
                /// column that was validated for this hit.
                if (auto storage = DatabaseCatalog::instance().tryGetTable(storage_id, context))
                {
                    const auto metadata = storage->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/false);
                    columns = metadata->getColumns().getAll().getNames();
                }
            }
            query_context->addQueryAccessInfo(storage_id, columns);
        }

        /// Cached plans store row-policy expressions, but policy names are audit metadata rather
        /// than plan semantics. Re-read them on each hit so `system.query_log` reflects a
        /// same-expression policy rename or replacement that correctly passed validation.
        if (auto row_policy = context->getRowPolicyFilter(dep.database, dep.table, RowPolicyFilterType::SELECT_FILTER))
            for (const auto & policy : row_policy->policies)
                query_context->addUsedRowPolicy(policy->getFullName().toString());
    }
}

/// Drops the `max_threads` cap stored with a cached plan from the plan itself and from the source
/// plan of every `IN`-subquery set reachable from it (sets nest: a set source may contain further
/// sets), so the thread fan-out of a hit is decided by the current query, as on a miss.
static void resetStoredMaxThreads(QueryPlan & plan)
{
    plan.setMaxThreads(0);
    forEachSubquerySet(
        &plan,
        [](FutureSetFromSubquery & set)
        {
            if (auto * source = set.getQueryPlan())
                source->setMaxThreads(0);
            /// Descend into the source plan: the sets nested in it are live and rebuilt too.
            return true;
        });
}

QueryPlan materializeCachedQueryPlan(
    std::string_view serialized_plan,
    const ContextPtr & context,
    const QueryPlan::ExpectedStorageIdentities & validated_identities)
{
    ReadBufferFromMemory in(serialized_plan.data(), serialized_plan.size());

    /// Reconstruct the logical plan skeleton; leaf nodes are storage-agnostic
    /// `ReadFromTable` placeholders. The blob was produced by this same server in
    /// `serializeQueryPlanForCache`, so it is fully trusted and passes `max_type_complexity = 0`
    /// (unlimited), matching the trusted server-to-server path rather than untrusted client packets.
    auto plan_and_sets = QueryPlan::deserialize(in, context, /* max_type_complexity= */ 0);

    /// Rebuild `PreparedSet` objects for IN (...) subqueries embedded in the plan.
    auto plan = QueryPlan::makeSets(std::move(plan_and_sets), context);

    /// The blob also carries the plan-level execution limits, which `QueryPlan::buildQueryPipeline`
    /// applies through `limitMaxThreads`. They describe the execution the plan was stored for, not
    /// its structure, and `max_threads` is deliberately not part of the cache key (see
    /// `isSettingIgnoredInQueryPlanCache`), so replaying the stored value would let a warm-up with
    /// `SETTINGS max_threads = 1` cap every later hit. The planner stamps the limit on every plan it
    /// builds for a table expression (`PlannerJoinTree`), and an `IN (SELECT ...)` set source is a
    /// plan of its own that `QueryPlan::makeSets` has just rebuilt with its stored value and that
    /// `FutureSetFromSubquery::build` turns into a pipeline on its own, so clearing the outer plan
    /// alone would leave the set-building sub-pipelines pinned. The other limit,
    /// `concurrency_control`, is derived from `use_concurrency_control`, which is part of the key,
    /// so the stored value always equals the current one (and the outer plan's copy is re-derived
    /// anyway by `InterpreterSelectQueryFromPlan::execute`).
    resetStoredMaxThreads(plan);

    /// Replace `ReadFromTable` placeholders with storage-specific reads against the current data
    /// snapshots, requiring every leaf to resolve to a storage that `validateQueryPlanCacheEntry`
    /// has already proven. This closes the window between validation and execution: a concurrent
    /// `DROP`/`CREATE` in an `Atomic` database can no longer make the plan run against a storage
    /// (engine, schema, view security) that was never validated - resolution throws
    /// `INCORRECT_DATA` instead, and the caller falls back to normal planning.
    plan.resolveStorages(context, &validated_identities);

    return plan;
}

String serializeQueryPlanForCache(const QueryPlan & plan)
{
    /// Stands in for a plan step that does not implement serialization. Every step the analyzer can
    /// put into a cacheable logical plan is serializable today, so the fallback that keeps such a
    /// query on the ordinary interpreter is only reachable through this fail point (see test 04667).
    fiu_do_on(FailPoints::query_plan_cache_serialization_not_implemented,
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Injected: method serialize is not implemented for a step of this plan");
    });

    WriteBufferFromOwnString out;
    plan.serialize(out, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    out.finalize();
    return out.str();
}

}
