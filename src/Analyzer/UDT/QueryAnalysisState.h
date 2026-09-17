#pragma once

#include <Analyzer/IQueryTreeNode.h>

#include <Core/Types.h>
#include <Core/UUID.h>

#include <DataTypes/UDT/BoundObjectTypeReferences.h>
#include <DataTypes/UDT/ResourceLimits.h>

#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <unordered_map>
#include <utility>
#include <vector>

namespace DB
{

class FunctionNode;
class ColumnNode;
class ConstantNode;
class IDatabase;

namespace UDT
{

class BoundDeclaredTypeTree;
class QueryResourceLedger;
class ProspectiveResourceBudget;
class QueryTreeSemanticRoleGraph;
class SemanticRolePlanner;
enum class SemanticSinkKind : UInt8;
struct TypeAuthorityLimits;
class UDTTypeExpressionResolutionScope;

/// State allocated only after query analysis recognizes an eligible closed
/// UDT semantic sink. One per-database entry owns the database before its resolver, so
/// reverse destruction releases the resolver's borrowed authority first. The
/// database map is destroyed before the query-wide resource ledger.
struct QueryAnalysisState final
{
    struct SparseProjectionSource final
    {
        UInt32 ordinal = 0;
        bool unanimous_union = false;
        bool nullable_lift = false;
        std::vector<std::shared_ptr<IQueryTreeNode>> inputs;
    };

    struct PreboundContextualConstantCandidate final
    {
        BoundObjectTypeReferences::Ptr references;
        PersistedTypeOccurrencePath use_path;
        /// Populated only after the closed sink classifier selects this exact
        /// candidate. It is copied from a snapshot session for the owning
        /// database UUID; no catalog lookup occurs on the physical fast path.
        std::optional<EffectiveResourceLimits> effective_query_limits;
    };

    QueryAnalysisState();
    ~QueryAnalysisState();

    QueryAnalysisState(const QueryAnalysisState &) = delete;
    QueryAnalysisState & operator=(const QueryAnalysisState &) = delete;

    /// Call only after the first eligible semantic sink has passed the closed
    /// registry. Every additional authority joins its effective limits into
    /// the same monotonic query ledger before the borrowed budget is returned.
    ProspectiveResourceBudget & getOrCreateSemanticResourceBudget(const TypeAuthorityLimits & authority_limits);
    ProspectiveResourceBudget & getOrCreateSemanticResourceBudget(const EffectiveResourceLimits & effective_limits);

    /// Charges analyzer-side sparse discovery that precedes graph
    /// registration. The supplied values are monotonic query-wide totals, so
    /// repeated callbacks cannot charge the same work twice. Before an exact
    /// authority budget exists this is a no-op; the caller resubmits the same
    /// totals immediately after activating the first eligible sink.
    void chargeSemanticDiscoveryWork(UInt64 node_path_states, UInt64 inspected_edges, UInt64 scratch_bytes);

    bool hasSemanticResourceBudget() const noexcept { return semantic_resource_budget != nullptr; }

    /// A generation-changing QueryTree rewrite must publish its exact clone
    /// mapping while any retained node pointer can still be consumed by
    /// selected-output classification or semantic planning.
    bool hasQueryTreeRegistrations() const noexcept;

    /// Rebinds graph registrations and selected-output CAST targets from an
    /// already-resolved subtree to the exact analyzer-owned clone that replaces
    /// it. Stable semantic IDs remain unchanged; incompatible or colliding
    /// mappings fail closed before the replacement is published.
    void remapSemanticGenerationAfterQueryTreeReplacement(const IQueryTreeNode::CloneNodeMapping & clone_node_mapping);

    /// Performs the allocation-free closed-registry probe for the direct-root
    /// explicit CAST slice. Unsupported logical target shapes are rejected
    /// deterministically; an ineligible target does not allocate graph or
    /// planner state.
    bool isDirectExplicitCastEligible(const BoundDeclaredTypeTree & target) const;

    /// Registers one already-resolved public CAST and retains its exact bound
    /// target until semantic planning is sealed. Returns false only when the
    /// target is valid but ineligible for the explicit-CAST sink.
    bool registerResolvedDirectExplicitCast(
        std::shared_ptr<FunctionNode> function,
        std::shared_ptr<const BoundDeclaredTypeTree> target,
        const TypeAuthorityLimits & authority_limits,
        const EffectiveResourceLimits * exact_effective_query_limits = nullptr,
        const std::function<bool(const ColumnNode &)> & source_column_is_non_synthesizing = {},
        const std::function<std::optional<SparseProjectionSource>(const ColumnNode &)> & sparse_projection_source = {},
        const std::function<bool(const FunctionNode &)> & source_is_full_join_using_unanimous = {});

    /// Retains the exact declared target for DDL selected-output
    /// classification, including capability-free UDT aliases that do not
    /// activate semantic execution analysis. The key is the resolved
    /// FunctionNode owned by the current query generation.
    void rememberResolvedExplicitCastTarget(const FunctionNode * function, std::shared_ptr<const BoundDeclaredTypeTree> target);
    std::shared_ptr<const BoundDeclaredTypeTree> findResolvedExplicitCastTarget(const FunctionNode * function) const noexcept;

    /// Selects one trusted V2 View StoredExpression CAST endpoint by its
    /// runtime-only AST ordinal. Physical-only endpoints return null. A
    /// capability-bearing nested/stacked target is rejected instead of being
    /// silently treated as a direct-root CAST.
    static std::shared_ptr<const BoundDeclaredTypeTree>
    inspectPreboundStoredExplicitCast(const BoundObjectTypeReferences::Ptr & references, UInt64 stored_expression_ordinal);

    /// Allocation-free with respect to semantic query state. It consults only
    /// the already-selected direct column's immutable bound snapshot and its
    /// O(log D) runtime endpoint index. Pure aliases return no candidate and
    /// do not create QueryAnalysisState/graph/planner state.
    static std::optional<PreboundContextualConstantCandidate> inspectPreboundContextualConstant(
        const ColumnNode & expected_column, SemanticSinkKind kind, std::span<const UInt64> type_child_prefix = {});

    bool registerPreboundContextualConstant(
        std::shared_ptr<ColumnNode> expected_column,
        std::shared_ptr<ConstantNode> constant,
        SemanticSinkKind kind,
        PreboundContextualConstantCandidate candidate);

    /// Freezes the authoritative sink enumeration and runs the demand planner.
    /// Idempotent after a successful seal and also for a state with no eligible
    /// sink. No semantic graph may be registered after this barrier.
    void finalizeSemanticAnalysis();

    void finalizeSemanticGraphGeneration();

    struct DatabaseState final
    {
        String diagnostic_name;
        std::shared_ptr<IDatabase> database;
        std::shared_ptr<UDTTypeExpressionResolutionScope> resolver;
        std::pair<UInt64, UInt64> reported_catalog_events;
    };

    std::shared_ptr<QueryResourceLedger> resource_ledger;
    std::unordered_map<String, UUID> database_ids_by_name;
    std::unordered_map<UUID, DatabaseState> databases;
    std::unordered_map<const FunctionNode *, std::shared_ptr<const BoundDeclaredTypeTree>> resolved_explicit_cast_targets;
    /// Destroyed before resolver scopes and their shared ledger. Graph/planner
    /// instances borrowing this budget must have a strictly shorter lifetime.
    std::unique_ptr<ProspectiveResourceBudget> semantic_resource_budget;
    /// Declared after the borrowed budget so reverse destruction releases the
    /// planner, then graph, before their budget and resolver-owned handles.
    std::unique_ptr<QueryTreeSemanticRoleGraph> semantic_role_graph;
    std::unique_ptr<SemanticRolePlanner> semantic_role_planner;
    UInt64 next_semantic_node_id = 0;
    UInt32 next_semantic_path_id = 1;
    UInt64 semantic_literal_bytes = 0;
    UInt64 charged_semantic_discovery_node_path_states = 0;
    UInt64 charged_semantic_discovery_inspected_edges = 0;
    UInt64 charged_semantic_discovery_scratch_bytes = 0;
    bool semantic_analysis_finalized = false;
};

}
}
