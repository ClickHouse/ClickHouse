#pragma once

#include <DataTypes/UDT/BoundObjectTypeReferences.h>

#include <Core/Types.h>

#include <limits>
#include <string_view>

namespace DB::UDT
{

using SemanticNodeID = UInt64;
using LogicalPathID = UInt32;
using LogicalShapeID = UInt32;
using LogicalRoleID = UInt32;
using SemanticConflictID = UInt32;
using SemanticSinkID = UInt32;
using QueryDefinitionHandleID = UInt32;

inline constexpr SemanticNodeID invalid_semantic_node_id = std::numeric_limits<SemanticNodeID>::max();
inline constexpr LogicalPathID invalid_logical_path_id = std::numeric_limits<LogicalPathID>::max();
inline constexpr LogicalShapeID invalid_logical_shape_id = std::numeric_limits<LogicalShapeID>::max();
inline constexpr LogicalRoleID invalid_logical_role_id = std::numeric_limits<LogicalRoleID>::max();
inline constexpr SemanticConflictID invalid_semantic_conflict_id = std::numeric_limits<SemanticConflictID>::max();
inline constexpr SemanticSinkID invalid_semantic_sink_id = std::numeric_limits<SemanticSinkID>::max();
inline constexpr QueryDefinitionHandleID invalid_query_definition_handle_id = std::numeric_limits<QueryDefinitionHandleID>::max();

/// Stable IDs are supplied by the analyzer adapter after its selected query
/// graph generation has stopped changing. Logical paths are adapter-owned,
/// normalized child-path IDs, not formatted column or type names.
struct SemanticNodePath
{
    SemanticNodeID node = invalid_semantic_node_id;
    LogicalPathID path = invalid_logical_path_id;

    bool isValid() const noexcept { return node != invalid_semantic_node_id && path != invalid_logical_path_id; }
    bool operator==(const SemanticNodePath &) const = default;
};

/// Canonical, binary-safe logical shape at one demanded path. The graph
/// adapter owns the bytes for the duration of one planner call; the planner
/// copies and interns them only after an eligible sink activates it.
struct LogicalShapeInput
{
    std::string_view canonical_encoding;

    bool isValid() const noexcept { return !canonical_encoding.empty(); }
};

/// Exact semantic identity is sourced only from an already-checked immutable
/// descriptor. No name, physical type equality, or row value can construct it.
struct LogicalRoleInput
{
    const PersistedTypeDescriptor * descriptor = nullptr;
    LogicalShapeInput shape;

    static LogicalRoleInput fromDescriptor(const InstantiatedTypeDescriptor & descriptor_, std::string_view canonical_logical_shape)
    {
        return {&descriptor_.getPersistedDescriptor(), {canonical_logical_shape}};
    }

    static LogicalRoleInput fromPersistedDescriptor(const PersistedTypeDescriptor & descriptor_, std::string_view canonical_logical_shape)
    {
        return {&descriptor_, {canonical_logical_shape}};
    }

    /// Borrows from the owning immutable bound-object snapshot without a new
    /// descriptor or definition-handle refcount operation.
    static LogicalRoleInput fromBoundObjectUse(
        const BoundObjectTypeReferences & references,
        const BoundObjectTypeReferenceUse & use,
        std::string_view canonical_logical_shape) noexcept
    {
        const auto descriptors = references.getDescriptors();
        const auto index = use.getDescriptorIndex();
        if (index >= descriptors.size() || !descriptors[index])
            return {};
        return fromDescriptor(*descriptors[index], canonical_logical_shape);
    }

    bool isValid() const noexcept { return descriptor && shape.isValid(); }
};

/// Compact source-proof lattice. Expected destinations are deliberately not
/// represented here and therefore cannot contaminate the proof memo key.
class RoleProof final
{
public:
    enum class Kind : UInt8
    {
        NoRole,
        NullOnly,
        Exact,
        Conflict,
    };

    static constexpr RoleProof noRole() noexcept { return RoleProof(Kind::NoRole, 0); }
    static constexpr RoleProof nullOnly() noexcept { return RoleProof(Kind::NullOnly, 0); }
    static constexpr RoleProof exact(LogicalRoleID role) noexcept { return RoleProof(Kind::Exact, role); }
    static constexpr RoleProof conflict(SemanticConflictID conflict_id) noexcept { return RoleProof(Kind::Conflict, conflict_id); }

    constexpr Kind getKind() const noexcept { return kind; }
    constexpr bool isExact() const noexcept { return kind == Kind::Exact; }
    constexpr bool isConflict() const noexcept { return kind == Kind::Conflict; }
    constexpr LogicalRoleID getExactRole() const noexcept { return isExact() ? payload : invalid_logical_role_id; }
    constexpr SemanticConflictID getConflict() const noexcept { return isConflict() ? payload : invalid_semantic_conflict_id; }

    bool operator==(const RoleProof &) const = default;

private:
    constexpr RoleProof(Kind kind_, UInt32 payload_) noexcept
        : payload(payload_)
        , kind(kind_)
    {
    }

    UInt32 payload = 0;
    Kind kind = Kind::NoRole;
};

static_assert(sizeof(RoleProof) <= 8);

}
