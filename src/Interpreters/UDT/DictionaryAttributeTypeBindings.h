#pragma once

#include <DataTypes/UDT/BoundObjectTypeReferences.h>
#include <DataTypes/UDT/InstantiatedTypeDescriptor.h>
#include <DataTypes/UDT/SidecarExpectationRecord.h>

#include <Core/NamesAndTypes.h>

#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB::UDT
{

inline constexpr std::string_view dictionary_attribute_physical_schema_fingerprint_domain
    = "ClickHouse UDT dictionary attribute physical schema V1";

struct DictionaryAttributeTypeBindingInput
{
    String attribute_name;
    BoundDeclaredTypeResult attribute_type;
};

struct DictionaryAttributeTypeBindingLimits
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
    UInt64 maximum_attributes = 10'000;
    UInt64 maximum_total_attribute_name_bytes = 16ULL << 20;
    UInt64 maximum_bound_nodes = 1ULL << 20;
    UInt64 maximum_descriptor_occurrences = 65'536;
    UInt64 maximum_definition_handles = 65'536;
    UInt64 maximum_distinct_definition_handles = 1'024;
    UInt64 maximum_definition_dependencies = 262'144;
    UInt64 maximum_retained_path_components = 4ULL << 20;
    UInt64 maximum_single_runtime_owner_key_bytes = 1ULL << 20;
    UInt64 maximum_retained_runtime_owner_key_bytes = 64ULL << 20;
};

/// Pure pre-publication package for an already-identified durable Dictionary object.
/// The caller supplies exact attribute bindings retained
/// before semantic-role erasure. No parsing, authority lookup, persistent write,
/// or runtime publication is performed here.
///
/// Physical attributes are always present. Logical values are all absent for a
/// physical-only attribute schema. A database-owned mutation may consume the
/// logical values only as one indivisible package.
struct PreparedDictionaryAttributeTypeBindings
{
    NamesAndTypesList physical_attributes;
    Digest physical_schema_fingerprint{};
    std::optional<PersistedTypeReferences> persisted_references;
    std::optional<BoundObjectPhysicalSchema> bound_physical_schema;
    std::optional<SidecarExpectationRecord> sidecar_expectation;
    std::vector<SchemaObjectDependencyEdge> dependency_edges;
};

class DictionaryAttributeTypeBindingError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        InvalidObject,
        InvalidAttribute,
        CrossDatabaseReference,
        ConflictingDescriptor,
        SidecarMismatch,
        PhysicalSchemaMismatch,
        PathMismatch,
        LimitExceeded,
    };

    DictionaryAttributeTypeBindingError(Code code_, std::string_view message);

    const Code code;
};

/// Composes attribute-local BoundDeclaredTypeResult values into the reserved
/// DictionaryAttribute path section. Object ordinals are zero-based physical-attribute
/// ordinals; child ordinals come only from BoundDeclaredTypeTree. Multiple
/// logical applications at one physical endpoint retain their tree order as
/// consecutive occurrence ordinals and are never collapsed.
///
/// Attribute declarations are the sole allowed producer of DictionaryAttribute
/// paths.
[[nodiscard]] PreparedDictionaryAttributeTypeBindings prepareDictionaryAttributeTypeBindings(
    const SchemaObjectID & dictionary,
    UInt64 object_schema_revision,
    std::span<const DictionaryAttributeTypeBindingInput> attributes,
    const DictionaryAttributeTypeBindingLimits & limits = {});

/// Computes the fingerprint over the ordered physical attribute names and their
/// complete physical type encodings. A result header is not logical-authority
/// input; this helper is only for validating the physical half of a prepared or
/// loaded Dictionary snapshot.
[[nodiscard]] Digest computeDictionaryAttributePhysicalSchemaFingerprint(
    const NamesAndTypesList & physical_attributes, const DictionaryAttributeTypeBindingLimits & limits = {});

/// Reconstructs the load-time binding input for a Dictionary sidecar. It accepts
/// only attribute-declaration ordinals in the DictionaryAttribute section.
/// Every type-child path is resolved against the normalized physical attribute
/// tree; identity is never inferred from the resulting physical type.
[[nodiscard]] BoundObjectPhysicalSchema reconstructDictionaryAttributePhysicalSchema(
    const SchemaObjectID & expected_dictionary,
    UInt64 expected_object_schema_revision,
    const NamesAndTypesList & physical_attributes,
    const PersistedTypeReferences & persisted_references,
    const DictionaryAttributeTypeBindingLimits & limits = {});

}
