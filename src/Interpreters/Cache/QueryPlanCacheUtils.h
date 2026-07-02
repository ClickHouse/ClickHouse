#pragma once

#include <Interpreters/Cache/QueryPlanCache.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

class QueryPlan;

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

/// Revalidates a cached entry against the current state of the database: every dependency
/// must still resolve to the same storage (UUID), with the same schema (metadata version /
/// schema hash) and the same row policy. Returns false if the entry is stale.
bool validateQueryPlanCacheEntry(const QueryPlanCacheEntry & entry, const ContextPtr & context);

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
QueryPlan materializeCachedQueryPlan(std::string_view serialized_plan, const ContextPtr & context);

/// Serializes a logical plan (as produced by the planner's `build_logical_plan` mode) into
/// cacheable bytes.
String serializeQueryPlanForCache(const QueryPlan & plan);

}
