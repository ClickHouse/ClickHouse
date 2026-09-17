#pragma once

#include <DataTypes/IDataType_fwd.h>
#include <DataTypes/UDT/PersistedTypeReferences.h>
#include <DataTypes/UDT/TemplateSpecializer.h>

#include <Core/Types.h>

#include <memory>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB::UDT
{

/// One endpoint from an object-kind adapter's already-normalized physical
/// schema. The adapter computes the whole-schema fingerprint and supplies the
/// physical subtree at every persisted semantic path in canonical path order.
struct BoundObjectPhysicalOccurrence
{
    PersistedTypeOccurrencePath path;
    DataTypePtr physical_type;
    /// Runtime-only stable owner key (column/output/attribute name) rebuilt
    /// from the same normalized physical snapshot as `path`. It is deliberately
    /// absent from the durable V1 sidecar: rename/ALTER reconstructs it together
    /// with the new bound snapshot instead of carrying stale name state.
    String runtime_owner_key;
    /// Object-kind adapter classification for this exact semantic sink. It is
    /// derived from normalized schema structure and must be a subset of the
    /// referenced definition's checked capabilities.
    SemanticCapabilityMask selected_semantic_capabilities = 0;
};

struct BoundObjectPhysicalSchema
{
    SchemaObjectID object;
    UInt64 object_schema_revision = 0;
    Digest physical_schema_fingerprint{};
    std::vector<BoundObjectPhysicalOccurrence> occurrences;
};

struct BoundObjectTypeReferencesLimits
{
    PersistedTypeReferencesLimits persisted{
        .maximum_sidecar_bytes = 16ULL << 20,
        .maximum_descriptors = 4'096,
        .maximum_occurrence_paths = 65'536,
        .maximum_path_depth = 64,
        .maximum_canonical_arguments_bytes = 64ULL << 10,
        .maximum_canonical_physical_type_bytes = 64ULL << 10,
        .maximum_qualified_name_bytes = 4ULL << 10,
    };
    TypeDescriptorLimits descriptor;
    TemplateSpecializerLimits specializer;
    CanonicalTypeArgumentLimits type_arguments;
    UInt64 maximum_retained_path_components = 4ULL << 20;
    UInt64 maximum_single_runtime_owner_key_bytes = 1ULL << 20;
    UInt64 maximum_retained_runtime_owner_key_bytes = 64ULL << 20;
};

class BoundObjectTypeReferencesError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        ObjectMismatch,
        PathMismatch,
        DescriptorMismatch,
        PhysicalSchemaMismatch,
        AuthorityFailure,
        LimitExceeded,
    };

    BoundObjectTypeReferencesError(Code code_, std::string_view message);

    const Code code;
};

class BoundObjectTypeReferenceUse final
{
public:
    const PersistedTypeOccurrencePath & getPath() const noexcept { return path; }
    UInt32 getDescriptorIndex() const noexcept { return descriptor_index; }
    const DataTypePtr & getPhysicalType() const noexcept { return physical_type; }
    SemanticCapabilityMask getSemanticCapabilities() const noexcept { return semantic_capabilities; }
    const String & getRuntimeOwnerKey() const noexcept { return runtime_owner_key; }

private:
    BoundObjectTypeReferenceUse(
        PersistedTypeOccurrencePath path_,
        UInt32 descriptor_index_,
        DataTypePtr physical_type_,
        String runtime_owner_key_,
        SemanticCapabilityMask semantic_capabilities_);

    friend class BoundObjectTypeReferences;

    PersistedTypeOccurrencePath path;
    UInt32 descriptor_index;
    DataTypePtr physical_type;
    String runtime_owner_key;
    SemanticCapabilityMask semantic_capabilities;
};

struct BoundObjectTypeReferenceUseLookup
{
    const BoundObjectTypeReferenceUse * use = nullptr;
    bool ambiguous = false;
    SemanticCapabilityMask semantic_capabilities = 0;
};

/// Immutable load-time derivative of one canonical sidecar and one normalized
/// physical schema snapshot. Binding opens exactly one authority resolution
/// session, resolves every distinct canonical instantiation once, and retains
/// no catalog/session object. Ordinary consumers read the stored physical
/// pointers and compact path roles without reopening the authority.
class BoundObjectTypeReferences final
{
public:
    using Ptr = std::shared_ptr<const BoundObjectTypeReferences>;

    static Ptr bind(
        const PersistedTypeReferences & references,
        BoundObjectPhysicalSchema physical_schema,
        const IAuthorityAdapter & authority,
        const BoundObjectTypeReferencesLimits & limits = {});

    const SchemaObjectID & getObject() const noexcept { return object; }
    UInt16 getFormatVersion() const noexcept { return format_version; }
    UInt16 getPathDictionaryVersion() const noexcept { return path_dictionary_version; }
    UInt64 getObjectSchemaRevision() const noexcept { return object_schema_revision; }
    const Digest & getSidecarHash() const noexcept { return sidecar_hash; }
    const Digest & getPhysicalSchemaFingerprint() const noexcept { return physical_schema_fingerprint; }
    SemanticCapabilityMask getSemanticCapabilities() const noexcept { return semantic_capabilities; }

    std::span<const InstantiatedTypeDescriptor::Ptr> getDescriptors() const noexcept { return descriptors; }
    std::span<const Definition::Ptr> getDefinitionHandles() const noexcept { return definition_handles; }
    std::span<const BoundObjectTypeReferenceUse> getUses() const noexcept { return uses; }

    const BoundObjectTypeReferenceUse * findUse(const PersistedTypeOccurrencePath & path) const noexcept;

    /// Exact runtime lookup used only while an already-selected query column is
    /// visited. The precomputed index makes this O(log D), with no scan of the
    /// owning ColumnsDescription. Stacked logical applications at one physical
    /// endpoint are reported as ambiguous and never selected by physical type.
    BoundObjectTypeReferenceUseLookup findUniqueRuntimeUse(
        PersistedTypePathSection section, std::string_view runtime_owner_key, std::span<const UInt64> type_child_ordinals) const noexcept;
    BoundObjectTypeReferenceUseLookup findUniqueRuntimeUse(
        PersistedTypePathSection section,
        PersistedTypeOccurrenceSite site,
        std::string_view runtime_owner_key,
        std::span<const UInt64> type_child_ordinals) const noexcept;

    /// O(log D + R) exact declaration-slice lookup for DDL selected-output
    /// classification. The returned borrowed pointers follow canonical path /
    /// occurrence order and remain owned by this immutable snapshot.
    std::vector<const BoundObjectTypeReferenceUse *> findRuntimeUsesByPrefix(
        PersistedTypePathSection section, std::string_view runtime_owner_key, std::span<const UInt64> type_child_prefix) const;
    std::vector<const BoundObjectTypeReferenceUse *> findRuntimeUsesByPrefix(
        PersistedTypePathSection section,
        PersistedTypeOccurrenceSite site,
        std::string_view runtime_owner_key,
        std::span<const UInt64> type_child_prefix) const;

private:
    BoundObjectTypeReferences(
        UInt16 format_version_,
        UInt16 path_dictionary_version_,
        SchemaObjectID object_,
        UInt64 object_schema_revision_,
        Digest sidecar_hash_,
        Digest physical_schema_fingerprint_,
        std::vector<InstantiatedTypeDescriptor::Ptr> descriptors_,
        std::vector<Definition::Ptr> definition_handles_,
        std::vector<BoundObjectTypeReferenceUse> uses_,
        SemanticCapabilityMask semantic_capabilities_);

    const UInt16 format_version;
    const UInt16 path_dictionary_version;
    const SchemaObjectID object;
    const UInt64 object_schema_revision;
    const Digest sidecar_hash;
    const Digest physical_schema_fingerprint;
    const std::vector<InstantiatedTypeDescriptor::Ptr> descriptors;
    const std::vector<Definition::Ptr> definition_handles;
    const std::vector<BoundObjectTypeReferenceUse> uses;
    const std::vector<UInt32> runtime_use_indices;
    const SemanticCapabilityMask semantic_capabilities;
};

}
