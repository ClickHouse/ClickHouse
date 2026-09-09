#pragma once

#include <Interpreters/Cache/QueryPlanCache.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

/// Returns true if the AST contains functions whose results must not be frozen into a
/// cached plan (non-deterministic functions; `arrayJoin` is exempt because it is pure).
bool astContainsFunctionsUnsafeForQueryPlanCache(const ASTPtr & ast, const ContextPtr & context);

/// Builds a cache lookup key for the given query AST without analyzing it.
/// Returns nullopt if the query shape is ineligible (not a SELECT).
std::optional<QueryPlanCacheKey> tryBuildQueryPlanCacheKey(
    const ASTPtr & ast,
    const ContextPtr & context,
    UInt64 semantic_settings_hash);

/// Post-analysis eligibility: walks the analyzed query tree and rejects plans that must not
/// be reused:
///   - non-deterministic functions anywhere (including expanded view bodies),
///   - scalar subqueries (their results are baked into the plan as constants), unless
///     `allow_scalar_subqueries` is set.
/// Returns false if the plan must not be stored.
bool queryTreeIsEligibleForPlanCache(const QueryTreeNodePtr & query_tree, const ContextPtr & context, bool allow_scalar_subqueries);

/// Collects all storages a cached plan depends on:
///   - leaf `ReadFromTable` steps of the logical plan (base tables read at runtime, including
///     sub-plans of IN-subquery sets),
///   - the AST closure over view definitions (views are inlined into the plan, and tables
///     referenced only from scalar subqueries do not appear in the plan at all).
/// Returns nullopt if any dependency makes the plan uncacheable (temporary tables, system
/// tables other than `system.one`, remote/Merge storages, table functions, DEFINER views).
std::optional<std::vector<QueryPlanCacheDependency>> collectQueryPlanCacheDependencies(
    const QueryPlan & plan,
    const ASTPtr & ast,
    const ContextPtr & context,
    bool allow_scalar_subqueries);

/// Fingerprint of the storage semantics a cacheable logical plan bakes in: the schema
/// (metadata version / schema hash) and the reading user's row-policy filter. It is recorded per
/// analyzed storage while the plan is built - from the analysis-time metadata snapshot - and
/// compared against the dependencies collected after the build, before an entry is stored. The
/// UUID comparison alone cannot detect an in-place `ALTER` (`MODIFY COLUMN`,
/// `ALTER VIEW ... MODIFY QUERY`, `CREATE`/`ALTER ROW POLICY`) between analysis and dependency
/// collection: the dependency would record the post-alter schema or row policy while the plan
/// carries the pre-alter semantics, producing an entry that validates successfully on every hit
/// yet keeps executing the stale semantics.
UInt64 computeQueryPlanCacheSemanticsFingerprint(
    const StorageMetadataPtr & metadata, const String & database, const String & table, const ContextPtr & context);

/// The same fingerprint computed from a collected dependency record, for comparison with the
/// analysis-time value above.
UInt64 computeQueryPlanCacheSemanticsFingerprint(const QueryPlanCacheDependency & dep);

/// Revalidates a cached entry against the current state of the database: every dependency
/// must still resolve to the same storage (UUID), with the same schema (metadata version /
/// schema hash) and the same row policy. Returns false if the entry is stale.
///
/// On success `validated_identities` receives the identity of every storage the validation
/// proved. It must be passed to `materializeCachedQueryPlan` so that plan materialization binds
/// its leaf reads to exactly these storages: validation and materialization resolve the table
/// names independently, and without this binding a concurrent `DROP`/`CREATE` between them could
/// validate one storage and execute against another.
bool validateQueryPlanCacheEntry(
    const QueryPlanCacheEntry & entry,
    const ContextPtr & context,
    QueryPlan::ExpectedStorageIdentities & validated_identities);

/// Re-checks SELECT access for every dependency of a cached plan. Permissions may have been
/// revoked after the plan was cached; throws ACCESS_DENIED in that case (the error propagates
/// to the user, it does not fall back to normal planning).
void checkAccessForQueryPlanCacheHit(const QueryPlanCacheEntry & entry, const ContextPtr & context);

/// Records the dependencies of a cached plan in the query context so that
/// `system.query_log.{query_databases,query_tables,query_columns,views}` stay populated
/// on cache hits (the planner that normally records them is skipped).
void addQueryAccessInfoForQueryPlanCacheHit(const QueryPlanCacheEntry & entry, const ContextPtr & context);

/// Reconstructs an executable plan from cached bytes: deserializes the logical plan,
/// rebuilds prepared sets, and resolves storage-agnostic `ReadFromTable` leaves into reads
/// against the current data snapshots (this is what makes cache hits see fresh data).
///
/// `validated_identities` comes from `validateQueryPlanCacheEntry` and pins resolution to the
/// storages that validation proved; a leaf that resolves to anything else throws `INCORRECT_DATA`.
QueryPlan materializeCachedQueryPlan(
    std::string_view serialized_plan,
    const ContextPtr & context,
    const QueryPlan::ExpectedStorageIdentities & validated_identities);

/// Serializes a logical plan (as produced by the planner's `build_logical_plan` mode) into
/// cacheable bytes.
String serializeQueryPlanForCache(const QueryPlan & plan);

}
