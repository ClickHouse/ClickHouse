#pragma once

#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/ResourceLimits.h>
#include <DataTypes/UDT/SchemaObjectIdentity.h>

#include <Core/Types.h>
#include <Core/UUID.h>

#include <memory>
#include <mutex>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>


namespace DB::UDT
{

inline constexpr UInt16 schema_object_dependency_graph_format_version = 1;
inline constexpr UInt64 schema_object_dependency_graph_maximum_nodes = 200'000;
/// One edge carries a frozen 64-byte deterministic catalog charge. This count
/// therefore covers the complete 1 GiB implementation charge domain; the
/// aggregate catalog budget still rejects combinations with other charges.
inline constexpr UInt64 schema_object_dependency_graph_maximum_edges = 16'777'216;
/// Transaction-local delta materialization deliberately remains below the
/// durable graph decoder domain.
inline constexpr UInt64 schema_object_dependency_graph_maximum_mutation_edges = 8'388'608;

struct SchemaObjectDependencyNeighbor
{
    SchemaObjectID object;
    SchemaObjectDependencyEdgeKind kind{};

    bool operator==(const SchemaObjectDependencyNeighbor &) const = default;

    friend bool operator<(const SchemaObjectDependencyNeighbor & lhs, const SchemaObjectDependencyNeighbor & rhs) noexcept
    {
        if (lhs.object != rhs.object)
            return lhs.object < rhs.object;
        return static_cast<UInt8>(lhs.kind) < static_cast<UInt8>(rhs.kind);
    }
};

class SchemaObjectDependencyGraphError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        Truncated,
        UnsupportedVersion,
        InvalidValue,
        LimitExceeded,
        NonCanonical,
        TrailingData,
        DuplicateNode,
        DuplicateEdge,
        MissingNode,
        MissingEdge,
        ExistingNode,
        ExistingEdge,
        ConflictingMutation,
    };

    SchemaObjectDependencyGraphError(Code code_, std::string_view message);

    const Code code;
};

/// Frozen-format count limits plus tighter retained-state and transaction-local
/// limits. An immutable root retains its limits, so a later mutation cannot
/// silently widen them.
struct SchemaObjectDependencyGraphLimits
{
    UInt64 maximum_nodes = schema_object_dependency_graph_maximum_nodes;
    UInt64 maximum_edges = schema_object_dependency_graph_maximum_edges;
    UInt64 maximum_edges_per_node = 65'536;
    UInt64 maximum_mutation_nodes = 131'072;
    UInt64 maximum_mutation_edges = 131'072;
    UInt64 maximum_retained_bytes = resource_implementation_maximum_deterministic_catalog_bytes;

    bool operator==(const SchemaObjectDependencyGraphLimits &) const = default;
};

/// Exact node/edge delta for one database schema transaction. Every removal
/// must exist in the base root and every addition must be absent.
struct SchemaObjectDependencyGraphMutation
{
    std::vector<SchemaObjectID> node_additions;
    std::vector<SchemaObjectID> node_removals;
    std::vector<SchemaObjectDependencyEdge> edge_additions;
    std::vector<SchemaObjectDependencyEdge> edge_removals;
};

/// Exact work of a persistent graph edit. Trie depths are fixed by the
/// node/edge key layouts, so none of these counters depends on untouched
/// catalog size.
struct SchemaObjectDependencyGraphMutationStatistics
{
    UInt64 node_deltas_applied = 0;
    UInt64 edge_deltas_applied = 0;
    UInt64 set_nodes_visited = 0;
    UInt64 set_nodes_created = 0;
    UInt64 set_nodes_hashed = 0;
    UInt64 adjacency_nodes_visited = 0;
    UInt64 adjacency_nodes_created = 0;
    UInt64 neighbor_nodes_visited = 0;
    UInt64 neighbor_nodes_created = 0;
    /// Remains zero for persistent edits; retained for an explicit regression
    /// guard against reintroducing whole-adjacency vector copies.
    UInt64 adjacency_neighbors_copied = 0;
    UInt64 neighbors_materialized = 0;
    UInt64 snapshot_nodes_materialized = 0;
    UInt64 snapshot_edges_materialized = 0;
};

struct SchemaObjectDependencyGraphStorage;

/// Immutable compact forward/reverse graph. Nodes include isolated schema
/// objects. A mutation builds a complete replacement root; durable publication
/// remains owned by the database schema transaction.
class SchemaObjectDependencyGraph final
{
public:
    using Ptr = std::shared_ptr<const SchemaObjectDependencyGraph>;

    [[nodiscard]] static Ptr createEmpty(UUID database_uuid, const SchemaObjectDependencyGraphLimits & limits = {});
    [[nodiscard]] static Ptr build(
        UUID database_uuid,
        std::span<const SchemaObjectID> nodes,
        std::span<const SchemaObjectDependencyEdge> edges,
        const SchemaObjectDependencyGraphLimits & limits = {});
    [[nodiscard]] static Ptr decodeSnapshot(std::string_view bytes, const SchemaObjectDependencyGraphLimits & limits = {});
    [[nodiscard]] static Ptr applyMutation(
        const Ptr & base,
        const SchemaObjectDependencyGraphMutation & mutation,
        SchemaObjectDependencyGraphMutationStatistics * statistics = nullptr);

    [[nodiscard]] String encodeSnapshot() const;
    [[nodiscard]] Digest computeRoot() const noexcept { return merkle_root; }

    [[nodiscard]] UUID getDatabaseUUID() const noexcept { return database_uuid; }
    /// Snapshot/database-scope-only materialization. Point lookup, adjacency
    /// traversal and mutation never enumerate this vector.
    [[nodiscard]] std::span<const SchemaObjectID> getNodes() const;
    /// Allocation-free prospective boundary for `getDependencies`. The
    /// returned cardinality is retained in the immutable adjacency node.
    [[nodiscard]] UInt64 getDependencyCount(const SchemaObjectID & dependent) const noexcept;
    /// Allocation-free prospective boundary for `getDependents`. The returned
    /// cardinality is retained in the immutable adjacency node.
    [[nodiscard]] UInt64 getDependentCount(const SchemaObjectID & dependency) const noexcept;
    [[nodiscard]] std::span<const SchemaObjectDependencyNeighbor> getDependencies(const SchemaObjectID & dependent) const;
    [[nodiscard]] std::span<const SchemaObjectDependencyNeighbor> getDependents(const SchemaObjectID & dependency) const;

    [[nodiscard]] bool containsNode(const SchemaObjectID & node) const noexcept;
    [[nodiscard]] bool containsEdge(const SchemaObjectDependencyEdge & edge) const noexcept;

    [[nodiscard]] UInt64 getNodeCount() const noexcept { return node_count; }
    [[nodiscard]] UInt64 getEdgeCount() const noexcept { return edge_count; }
    [[nodiscard]] UInt64 getMaximumForwardDegree() const noexcept;
    [[nodiscard]] UInt64 getMaximumReverseDegree() const noexcept;
    [[nodiscard]] UInt64 getAccountedBytes() const noexcept { return accounted_bytes; }
    [[nodiscard]] const SchemaObjectDependencyGraphLimits & getLimits() const noexcept { return limits; }
    void validateAgainstLimits(const SchemaObjectDependencyGraphLimits & candidate_limits) const;

private:
    SchemaObjectDependencyGraph(
        UUID database_uuid_,
        SchemaObjectDependencyGraphLimits limits_,
        std::shared_ptr<const SchemaObjectDependencyGraphStorage> storage_,
        UInt64 node_count_,
        UInt64 edge_count_,
        UInt64 forward_group_count_,
        UInt64 reverse_group_count_,
        UInt64 accounted_bytes_,
        Digest merkle_root_,
        std::vector<SchemaObjectID> materialized_nodes_ = {});

    [[nodiscard]] static Ptr buildCanonical(
        UUID database_uuid,
        std::vector<SchemaObjectID> canonical_nodes,
        std::vector<SchemaObjectDependencyEdge> canonical_edges,
        const SchemaObjectDependencyGraphLimits & limits);
    const UUID database_uuid;
    const SchemaObjectDependencyGraphLimits limits;
    const std::shared_ptr<const SchemaObjectDependencyGraphStorage> storage;
    const UInt64 node_count;
    const UInt64 edge_count;
    const UInt64 forward_group_count;
    const UInt64 reverse_group_count;
    const UInt64 accounted_bytes;
    const Digest merkle_root;
    mutable std::once_flag materialize_nodes_once;
    mutable std::vector<SchemaObjectID> materialized_nodes;
};

}
