#pragma once

#include <Analyzer/UDT/SemanticCacheDependencyDigest.h>
#include <Analyzer/UDT/SemanticSinkRegistry.h>
#include <Analyzer/UDT/SemanticTransferRegistry.h>

#include <DataTypes/UDT/CanonicalHash.h>

#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>

namespace DB::UDT
{

class ProspectiveResourceBudget;
class QueryTreeSemanticRoleGraph;

struct SemanticRolePlannerLimits
{
    UInt64 maximum_sinks = 16'384;
    UInt64 maximum_demanded_states = 131'072;
    UInt64 maximum_inspected_edges = 524'288;
    UInt64 maximum_logical_paths = 131'072;
    UInt64 maximum_shapes = 16'384;
    UInt64 maximum_roles = 16'384;
    UInt64 maximum_owned_definition_handles = 16'384;
    /// Iterative graph-DFS guard; this is independent of logical type-child
    /// path depth and deliberately remains a local implementation ceiling.
    UInt64 maximum_active_depth = 4'096;
    UInt64 maximum_shape_bytes = 64ULL << 20;
    /// One interner item guard; diagnostic payload limits are unrelated.
    UInt64 maximum_single_shape_bytes = 64ULL << 10;
    UInt64 maximum_role_argument_bytes = 64ULL << 20;
    UInt64 maximum_single_role_argument_bytes = 64ULL << 10;
    UInt64 maximum_combined_scratch_bytes = 64ULL << 20;
    UInt64 maximum_literal_bytes = 1ULL << 20;
    UInt64 maximum_conflicts = 32;
};

class SemanticRolePlannerError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        InvalidSink,
        InvalidGraph,
        StaleGeneration,
        MutableAfterSeal,
        NotSealed,
        LimitExceeded,
    };

    SemanticRolePlannerError(Code code_, std::string_view message);

    const Code code;
};

struct InternedLogicalRole
{
    DefinitionIdentity definition_identity;
    Digest definition_hash{};
    String canonical_arguments_encoding;
    Digest instantiation_semantic_hash{};
    LogicalShapeID shape = invalid_logical_shape_id;

    bool operator==(const InternedLogicalRole &) const = default;
};

enum class SemanticRoleConflictKind : UInt8
{
    Cycle,
    DistinctExactRoles,
};

struct SemanticRoleConflict
{
    SemanticRoleConflictKind kind = SemanticRoleConflictKind::Cycle;
    SemanticNodePath state;
    LogicalRoleID lhs = invalid_logical_role_id;
    LogicalRoleID rhs = invalid_logical_role_id;
};

enum class PlannedBoundaryKind : UInt8
{
    PhysicalOnly,
    PreserveSourceRole,
    ApplyExpectedRole,
    ObserveSourceRole,
    Conflict,
};

struct PlannedBoundary
{
    SemanticSinkID sink = invalid_semantic_sink_id;
    PlannedBoundaryKind kind = PlannedBoundaryKind::PhysicalOnly;
    RoleProof source_proof = RoleProof::noRole();
    LogicalRoleID expected_role = invalid_logical_role_id;
    QueryDefinitionHandleID retained_definition_handle = invalid_query_definition_handle_id;
};

struct SemanticRolePlannerStatistics
{
    UInt64 eligible_sinks = 0;
    UInt64 demanded_states = 0;
    UInt64 inspected_edges = 0;
    UInt64 logical_paths = 0;
    UInt64 shapes_interned = 0;
    UInt64 roles_interned = 0;
    UInt64 owned_definition_handles = 0;
    UInt64 prebound_schema_role_uses = 0;
    UInt64 conflicts = 0;
    UInt64 shape_bytes = 0;
    UInt64 role_argument_bytes = 0;
    UInt64 literal_bytes = 0;
    UInt64 semantic_scratch_bytes = 0;
};

/// Query-local, generation-scoped demand planner over one exact, already
/// sealed QueryTree graph instance. Every eligible sink in that graph is
/// imported at construction. The planner remains open only while proofs and
/// literal accounting are being prepared; `seal` plans every imported sink
/// and is the sole authority that can mint a semantic cache dependency.
class SemanticRolePlanner final
{
public:
    using Ptr = std::unique_ptr<SemanticRolePlanner>;

    static Ptr create(
        const QueryTreeSemanticRoleGraph & graph,
        ProspectiveResourceBudget & query_resource_budget,
        const SemanticRolePlannerLimits & limits = {});

    ~SemanticRolePlanner();

    SemanticRolePlanner(const SemanticRolePlanner &) = delete;
    SemanticRolePlanner & operator=(const SemanticRolePlanner &) = delete;
    SemanticRolePlanner(SemanticRolePlanner &&) = delete;
    SemanticRolePlanner & operator=(SemanticRolePlanner &&) = delete;

    /// Charges raw accepted literal bytes once per occurrence, before any
    /// exact literal/type grouping or deduplication and before folded-byte
    /// allocation.
    void chargeLiteralBytes(UInt64 bytes);

    /// Any failed non-cached proof attempt poisons this planner. The query must
    /// abandon it; accepted ledger charges remain monotonic and stale Visiting
    /// memo entries can never be reused.
    RoleProof prove(SemanticNodeID source, LogicalPathID path);
    RoleProof prove(const SemanticNodePath & source) { return prove(source.node, source.path); }

    PlannedBoundary satisfy(SemanticSinkID sink, const RoleProof & proof) const;
    PlannedBoundary planSink(SemanticSinkID sink);

    /// Plans every sink in the exact sealed graph enumeration, freezes this
    /// planner, and derives a sorted+unique safe over-approximation from all
    /// internally interned expected/proven roles and their owned shapes.
    /// Failure poisons the planner; a partial seal can never be retried.
    void seal(const SemanticCacheDependencyDigestLimits & digest_limits = {});
    bool isSealed() const noexcept;

    /// Sealed-result access revalidates the retained graph instance and its
    /// generation, including post-registration CAST mutation checks.
    const PlannedBoundary & getPlannedBoundary(SemanticSinkID sink) const;
    /// Revalidates once and exposes the complete immutable boundary set. This
    /// avoids a full graph revalidation for every sink at the final analyzer
    /// barrier while preserving the same fail-closed lifetime check.
    std::span<const PlannedBoundary> getPlannedBoundaries() const;
    const SemanticCacheDependencyDigest & getCacheDependencyDigest() const;

    UInt64 getGeneration() const noexcept;
    const SemanticRolePlannerStatistics & getStatistics() const noexcept;
    const InternedLogicalRole & getRole(LogicalRoleID role) const;
    std::string_view getShape(LogicalShapeID shape) const;
    const SemanticRoleConflict & getConflict(SemanticConflictID conflict) const;
    const InstantiatedTypeDescriptor::Ptr & getOwnedDefinitionHandle(QueryDefinitionHandleID handle) const;

private:
    struct Impl;

    std::optional<SemanticSinkID> registerSink(const SemanticSink & sink);

    SemanticRolePlanner(
        const QueryTreeSemanticRoleGraph & graph,
        ProspectiveResourceBudget & query_resource_budget,
        const SemanticRolePlannerLimits & limits,
        UInt64 admitted_base_scratch_bytes);

    std::unique_ptr<Impl> impl;
};

}
