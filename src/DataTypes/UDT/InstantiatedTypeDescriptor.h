#pragma once

#include <DataTypes/IDataType_fwd.h>
#include <DataTypes/UDT/CanonicalTypeArguments.h>
#include <DataTypes/UDT/Definition.h>

#include <Core/Types.h>

#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB::UDT
{

/// The earlier checker-proof serialization included metadata that production
/// descriptors exclude, so this contract uses an additive domain/version
/// rather than reinterpreting the frozen V1 digest.
inline constexpr std::string_view instantiation_semantic_hash_domain = "ClickHouse UDT instantiation hash V2";

class DescriptorError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidDefinition,
        InvalidArguments,
        InvalidPhysicalType,
        InvalidPath,
        ConflictingIdentity,
        LimitExceeded,
    };

    DescriptorError(Code code_, std::string_view message);

    Code code;
};

/// These maxima are part of the frozen metadata contract. Callers may lower
/// them, but accepting a larger domain requires a compatible format revision.
struct TypeDescriptorLimits
{
    UInt64 maximum_canonical_arguments_bytes = 64ULL << 10;
    UInt64 maximum_canonical_physical_type_bytes = 64ULL << 10;
    UInt64 maximum_qualified_name_bytes = 4ULL << 10;
    /// Bounds the sparse physical node table; maximum_occurrences separately
    /// bounds raw logical-descriptor occurrences and their build scratch.
    UInt64 maximum_nodes = 1ULL << 20;
    UInt64 maximum_edges = 4ULL << 20;
    UInt64 maximum_path_depth = 64;
    UInt64 maximum_descriptors = 65'536;
    UInt64 maximum_occurrences = 65'536;
};

/// Non-mutating admission check shared by callers that must reject an invalid
/// composite limit profile before allocating descriptor/resolver state.
void validateTypeDescriptorLimits(const TypeDescriptorLimits & limits);

/// Complete checked field view for the allocation-free V2 identity codec.
/// V2 always commits a zero program count; supporting instantiated programs
/// requires a new codec version rather than extending this structure.
struct InstantiationSemanticHashInput
{
    DefinitionIdentity definition_identity;
    Digest definition_hash{};
    std::string_view canonical_arguments_encoding;
    std::string_view canonical_physical_type;
    Digest storage_fingerprint{};
    UInt16 checker_abi = 0;
    UInt16 checker_charge_abi = 0;
    UInt16 policy_abi = 0;
    UInt16 function_registry_abi = 0;
    Digest policy_semantic_hash{};
    SemanticCapabilityMask semantic_capabilities = 0;
};

/// Streams the exact V2 canonical frame into SHA-256 without materializing a
/// concatenated preimage. Admission/semantic validation belongs to the
/// descriptor builder; this codec preserves every byte supplied to it.
Digest computeInstantiationSemanticHash(const InstantiationSemanticHashInput & input);

/// Persistence-safe immutable value projection. In particular,
/// it contains no DataTypePtr, definition pointer, catalog root, or resolution
/// lease. Its exact field tuple is private in-memory state, not the
/// object-sidecar wire format.
class PersistedTypeDescriptor final
{
public:
    /// Reconstructs the immutable persistence projection without parsing a
    /// formatted type name. `canonical_arguments_encoding_` must be the exact
    /// opaque bytes previously produced by CanonicalTypeArguments::encoded();
    /// definition-aware recovery subsequently validates those bytes against
    /// the definition's formal parameters. This boundary validates every
    /// invariant available from the persisted tuple itself and recomputes its
    /// complete V2 semantic hash before constructing the value.
    static PersistedTypeDescriptor fromCanonicalPersistenceFields(
        DefinitionIdentity definition_identity_,
        Digest definition_hash_,
        String canonical_arguments_encoding_,
        String canonical_physical_type_,
        Digest instantiation_semantic_hash_,
        Digest storage_fingerprint_,
        UInt16 checker_abi_,
        UInt16 checker_charge_abi_,
        UInt16 policy_abi_,
        UInt16 function_registry_abi_,
        Digest policy_semantic_hash_,
        SemanticCapabilityMask semantic_capabilities_,
        String last_known_qualified_name_,
        const TypeDescriptorLimits & limits = {});

    const DefinitionIdentity & getDefinitionIdentity() const noexcept { return definition_identity; }
    const Digest & getDefinitionHash() const noexcept { return definition_hash; }
    const String & getCanonicalArgumentsEncoding() const noexcept { return canonical_arguments_encoding; }
    const String & getCanonicalPhysicalType() const noexcept { return canonical_physical_type; }
    const Digest & getInstantiationSemanticHash() const noexcept { return instantiation_semantic_hash; }
    const Digest & getStorageFingerprint() const noexcept { return storage_fingerprint; }
    UInt16 getCheckerABI() const noexcept { return checker_abi; }
    UInt16 getCheckerChargeABI() const noexcept { return checker_charge_abi; }
    UInt16 getPolicyABI() const noexcept { return policy_abi; }
    UInt16 getFunctionRegistryABI() const noexcept { return function_registry_abi; }
    const Digest & getPolicySemanticHash() const noexcept { return policy_semantic_hash; }
    SemanticCapabilityMask getSemanticCapabilities() const noexcept { return semantic_capabilities; }
    const String & getLastKnownQualifiedName() const noexcept { return last_known_qualified_name; }

    /// Rename changes only diagnostics, not instantiated identity.
    bool hasSameInstantiation(const PersistedTypeDescriptor & rhs) const noexcept;
    bool stableLess(const PersistedTypeDescriptor & rhs) const noexcept;
    bool operator==(const PersistedTypeDescriptor & rhs) const noexcept;

private:
    PersistedTypeDescriptor(
        DefinitionIdentity definition_identity_,
        Digest definition_hash_,
        String canonical_arguments_encoding_,
        String canonical_physical_type_,
        Digest instantiation_semantic_hash_,
        Digest storage_fingerprint_,
        UInt16 checker_abi_,
        UInt16 checker_charge_abi_,
        UInt16 policy_abi_,
        UInt16 function_registry_abi_,
        Digest policy_semantic_hash_,
        SemanticCapabilityMask semantic_capabilities_,
        String last_known_qualified_name_);

    DefinitionIdentity definition_identity;
    Digest definition_hash;
    String canonical_arguments_encoding;
    String canonical_physical_type;
    Digest instantiation_semantic_hash;
    Digest storage_fingerprint;
    UInt16 checker_abi;
    UInt16 checker_charge_abi;
    UInt16 policy_abi;
    UInt16 function_registry_abi;
    Digest policy_semantic_hash;
    SemanticCapabilityMask semantic_capabilities;
    String last_known_qualified_name;
};

/// One immutable concrete specialization. It deliberately owns only an
/// independent definition handle and concrete physical type; it cannot retain
/// the catalog generation/session from which the definition was found.
class InstantiatedTypeDescriptor final
{
public:
    using Ptr = std::shared_ptr<const InstantiatedTypeDescriptor>;

    static Ptr create(
        Definition::Ptr definition,
        CanonicalTypeArguments arguments,
        DataTypePtr physical_type,
        const TypeDescriptorLimits & limits = {});

    InstantiatedTypeDescriptor(const InstantiatedTypeDescriptor &) = delete;
    InstantiatedTypeDescriptor & operator=(const InstantiatedTypeDescriptor &) = delete;
    InstantiatedTypeDescriptor(InstantiatedTypeDescriptor &&) = delete;
    InstantiatedTypeDescriptor & operator=(InstantiatedTypeDescriptor &&) = delete;

    const Definition::Ptr & getDefinition() const noexcept { return definition; }
    const CanonicalTypeArguments & getCanonicalArguments() const noexcept { return arguments; }
    const DataTypePtr & getPhysicalType() const noexcept { return physical_type; }
    const PersistedTypeDescriptor & getPersistedDescriptor() const noexcept { return persisted; }

private:
    InstantiatedTypeDescriptor(
        Definition::Ptr definition_,
        CanonicalTypeArguments arguments_,
        DataTypePtr physical_type_,
        PersistedTypeDescriptor persisted_);

    Definition::Ptr definition;
    CanonicalTypeArguments arguments;
    DataTypePtr physical_type;
    PersistedTypeDescriptor persisted;
};

using BoundDeclaredTypeNodeID = UInt32;
inline constexpr BoundDeclaredTypeNodeID invalid_bound_declared_type_node_id = std::numeric_limits<BoundDeclaredTypeNodeID>::max();

/// Path identity uses normalized physical type-child ordinals only. Object
/// section/ordinal prefixes belong to the object-sidecar format.
struct BoundDeclaredTypeNodeInput
{
    /// TypeResolver must supply a normalized path and the exact physical
    /// subtree at that path. This compact builder validates path structure and
    /// logical/physical descriptor equality; it deliberately does not walk a
    /// second IDataType tree to rediscover parent-child relationships.
    std::vector<UInt32> type_child_ordinals;
    DataTypePtr physical_type;
};

/// A declared occurrence is separate from its physical node because transparent
/// aliases, dependency calls, and decreasing self-calls can all occupy the same
/// normalized physical path. logical_preorder is the stable outer-to-inner
/// identity assigned by the binder; occurrence records are never deduplicated.
struct BoundDeclaredTypeOccurrenceInput
{
    std::vector<UInt32> type_child_ordinals;
    InstantiatedTypeDescriptor::Ptr logical_descriptor;
    UInt32 logical_preorder = 0;
};

class BoundDeclaredTypeNode final
{
public:
    BoundDeclaredTypeNodeID getParent() const noexcept { return parent; }
    UInt32 getChildOrdinal() const noexcept { return child_ordinal; }
    const DataTypePtr & getPhysicalType() const noexcept { return physical_type; }
    SemanticCapabilityMask getOwnSemanticCapabilities() const noexcept { return own_semantic_capabilities; }
    SemanticCapabilityMask getSubtreeSemanticCapabilities() const noexcept { return subtree_semantic_capabilities; }
    UInt32 getChildCount() const noexcept { return child_count; }
    UInt32 getOccurrenceCount() const noexcept { return occurrence_count; }

private:
    friend class BoundDeclaredTypeTree;

    BoundDeclaredTypeNodeID parent = invalid_bound_declared_type_node_id;
    UInt32 child_ordinal = 0;
    UInt32 first_child = 0;
    UInt32 child_count = 0;
    UInt32 first_occurrence = 0;
    UInt32 occurrence_count = 0;
    SemanticCapabilityMask own_semantic_capabilities = 0;
    SemanticCapabilityMask subtree_semantic_capabilities = 0;
    DataTypePtr physical_type;
};

/// Compact immutable normalized declared-type tree. Nodes receive stable IDs
/// by lexicographic normalized type-child path, and every edge is represented
/// once in the flat child table.
class BoundDeclaredTypeTree final
{
public:
    using Ptr = std::shared_ptr<const BoundDeclaredTypeTree>;

    /// transitive_definition_handles are independent immutable handles reached
    /// during specialization but not necessarily attached to a logical node.
    /// Direct descriptor definitions are added automatically. No catalog root
    /// or resolution-session handle is accepted or retained. The raw supplied
    /// handle count is a work input and must fit maximum_descriptors before
    /// any combined scratch allocation; duplicates do not waive that bound.
    static Ptr build(
        std::vector<BoundDeclaredTypeNodeInput> nodes,
        std::vector<BoundDeclaredTypeOccurrenceInput> occurrences,
        std::vector<Definition::Ptr> transitive_definition_handles,
        const TypeDescriptorLimits & limits = {});

    BoundDeclaredTypeTree(const BoundDeclaredTypeTree &) = delete;
    BoundDeclaredTypeTree & operator=(const BoundDeclaredTypeTree &) = delete;
    BoundDeclaredTypeTree(BoundDeclaredTypeTree &&) = delete;
    BoundDeclaredTypeTree & operator=(BoundDeclaredTypeTree &&) = delete;

    const DataTypePtr & getPhysicalType() const noexcept { return nodes.front().physical_type; }
    const BoundDeclaredTypeNode & getNode(BoundDeclaredTypeNodeID node) const;
    std::optional<BoundDeclaredTypeNodeID> findNode(std::span<const UInt32> type_child_ordinals) const noexcept;
    std::span<const UInt32> getDescriptorIndices(BoundDeclaredTypeNodeID node) const;
    const std::vector<InstantiatedTypeDescriptor::Ptr> & getDescriptors() const noexcept { return descriptors; }
    const std::vector<Definition::Ptr> & getDefinitionHandles() const noexcept { return definition_handles; }
    SemanticCapabilityMask getSemanticCapabilities() const noexcept { return nodes.front().subtree_semantic_capabilities; }
    UInt64 getNodeCount() const noexcept { return nodes.size(); }
    UInt64 getEdgeCount() const noexcept { return children.size(); }
    UInt64 getOccurrenceCount() const noexcept { return descriptor_occurrences.size(); }

private:
    BoundDeclaredTypeTree(
        std::vector<BoundDeclaredTypeNode> nodes_,
        std::vector<BoundDeclaredTypeNodeID> children_,
        std::vector<UInt32> descriptor_occurrences_,
        std::vector<InstantiatedTypeDescriptor::Ptr> descriptors_,
        std::vector<Definition::Ptr> definition_handles_);

    std::vector<BoundDeclaredTypeNode> nodes;
    std::vector<BoundDeclaredTypeNodeID> children;
    std::vector<UInt32> descriptor_occurrences;
    std::vector<InstantiatedTypeDescriptor::Ptr> descriptors;
    std::vector<Definition::Ptr> definition_handles;
};

/// Sparse schema-binding result. Built-in-only declarations retain exactly the
/// physical type and no logical tree/handle allocation.
class BoundDeclaredTypeResult final
{
public:
    static BoundDeclaredTypeResult physicalOnly(DataTypePtr physical_type);
    static BoundDeclaredTypeResult withLogicalTree(BoundDeclaredTypeTree::Ptr logical_tree);

    const DataTypePtr & getPhysicalType() const noexcept { return physical_type; }
    const BoundDeclaredTypeTree::Ptr & getLogicalTree() const noexcept { return logical_tree; }
    bool hasLogicalTree() const noexcept { return static_cast<bool>(logical_tree); }

private:
    BoundDeclaredTypeResult(DataTypePtr physical_type_, BoundDeclaredTypeTree::Ptr logical_tree_);

    DataTypePtr physical_type;
    BoundDeclaredTypeTree::Ptr logical_tree;
};

}
