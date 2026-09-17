#pragma once

#include <Analyzer/UDT/SelectedOutputTypeBindings.h>

#include <DataTypes/UDT/BoundObjectTypeReferences.h>
#include <DataTypes/UDT/InstantiatedTypeDescriptor.h>
#include <DataTypes/UDT/SidecarExpectationRecord.h>
#include <DataTypes/UDT/TableColumnTypeBindings.h>

#include <Core/NamesAndTypes.h>

#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB::UDT
{

inline constexpr std::string_view view_output_physical_schema_fingerprint_domain = "ClickHouse UDT view output physical schema V1";

struct ViewOutputTypeBindingInput
{
    String output_name;
    BoundDeclaredTypeResult output_type;
};

/// Exact non-declaration endpoint owned by a View/MV CREATE AST. The owner
/// walker assigns a deterministic ordinal within `site`; the runtime key is
/// rebuilt from the same canonical owner snapshot and is never persisted as
/// an independent identity.
struct ViewAuxiliaryTypeBindingInput
{
    PersistedTypeOccurrenceSite site = PersistedTypeOccurrenceSite::StoredExpression;
    UInt64 object_ordinal = 0;
    String runtime_owner_key;
    BoundDeclaredTypeResult endpoint_type;
};

/// Load/rename counterpart of ViewAuxiliaryTypeBindingInput. Callers parse the
/// trusted physical CREATE metadata and provide only the exact endpoint roots
/// requested by the mixed sidecar; this binder performs no catalog lookup.
struct ViewAuxiliaryPhysicalTypeBindingInput
{
    PersistedTypeOccurrenceSite site = PersistedTypeOccurrenceSite::StoredExpression;
    UInt64 object_ordinal = 0;
    String runtime_owner_key;
    DataTypePtr physical_type;
};

struct ViewOutputTypeBindingLimits
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
    UInt64 maximum_outputs = 10'000;
    UInt64 maximum_total_output_name_bytes = 16ULL << 20;
    UInt64 maximum_bound_nodes = 1ULL << 20;
    UInt64 maximum_descriptor_occurrences = 65'536;
    UInt64 maximum_definition_handles = 65'536;
    UInt64 maximum_distinct_definition_handles = 1'024;
    UInt64 maximum_definition_dependencies = 262'144;
    UInt64 maximum_retained_path_components = 4ULL << 20;
    UInt64 maximum_single_runtime_owner_key_bytes = 1ULL << 20;
    UInt64 maximum_retained_runtime_owner_key_bytes = 64ULL << 20;
};

/// Pure pre-publication package for an already-identified durable View object.
/// Ordinary versus materialized View admission remains the caller's closed
/// classification decision. The caller supplies exact output bindings retained
/// before semantic-role erasure. No parsing, authority lookup, persistent write,
/// or runtime publication is performed here.
///
/// Physical outputs are always present. Logical values are all absent for a
/// physical-only output schema. A database-owned mutation may consume the
/// logical values only as one indivisible package.
struct PreparedViewOutputTypeBindings
{
    NamesAndTypesList physical_outputs;
    Digest physical_schema_fingerprint{};
    std::optional<PersistedTypeReferences> persisted_references;
    std::optional<BoundObjectPhysicalSchema> bound_physical_schema;
    std::optional<SidecarExpectationRecord> sidecar_expectation;
    std::vector<SchemaObjectDependencyEdge> dependency_edges;
};

class ViewOutputTypeBindingError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        InvalidObject,
        InvalidOutput,
        CrossDatabaseReference,
        ConflictingDescriptor,
        SidecarMismatch,
        PhysicalSchemaMismatch,
        PathMismatch,
        LimitExceeded,
    };

    ViewOutputTypeBindingError(Code code_, std::string_view message);

    const Code code;
};

/// Composes output-local BoundDeclaredTypeResult values into the reserved
/// ViewExpression path section. Object ordinals are zero-based physical-output
/// ordinals; child ordinals come only from BoundDeclaredTypeTree. Multiple
/// logical applications at one physical endpoint retain their tree order as
/// consecutive occurrence ordinals and are never collapsed.
///
/// Declaration-only packages remain canonical V1.
[[nodiscard]] PreparedViewOutputTypeBindings prepareViewOutputTypeBindings(
    const SchemaObjectID & view,
    UInt64 object_schema_revision,
    std::span<const ViewOutputTypeBindingInput> outputs,
    const ViewOutputTypeBindingLimits & limits = {});

/// Merges declaration endpoints with exact StoredExpression/SchemaString
/// endpoints. It emits V2 iff at least one non-declaration logical occurrence
/// exists; descriptor/path dictionaries, uses, dependencies, bound schema,
/// physical fingerprint, and expectation are rebuilt as one canonical package.
[[nodiscard]] PreparedViewOutputTypeBindings prepareViewMixedTypeBindings(
    const SchemaObjectID & view,
    UInt64 object_schema_revision,
    std::span<const ViewOutputTypeBindingInput> outputs,
    std::span<const ViewAuxiliaryTypeBindingInput> auxiliary_endpoints,
    const ViewOutputTypeBindingLimits & limits = {});

/// Consumes the compact DDL-local analyzer classification. Exact explicit and
/// prebound slices are materialized into transient BoundDeclaredTypeResult
/// values under the View limits, then merged with auxiliary V2 endpoints.
/// Physical sample/header types never establish a descriptor.
[[nodiscard]] PreparedViewOutputTypeBindings prepareViewSelectedOutputTypeBindings(
    const SchemaObjectID & view,
    UInt64 object_schema_revision,
    std::span<const SelectedOutputTypeBinding> selected_outputs,
    std::span<const ViewAuxiliaryTypeBindingInput> auxiliary_endpoints,
    const ViewOutputTypeBindingLimits & limits = {});

/// Table AS/EMPTY SELECT counterpart. It consumes the same analyzer proof,
/// materializes exact declaration slices without consulting a sample header,
/// and delegates canonical V1 column binding to the table adapter.
[[nodiscard]] PreparedTableColumnTypeBindings prepareTableSelectedOutputTypeBindings(
    const SchemaObjectID & table,
    UInt64 object_schema_revision,
    std::span<const SelectedOutputTypeBinding> selected_outputs,
    const TableColumnTypeBindingLimits & limits = {});

/// Computes the fingerprint over the ordered physical output names and their
/// complete physical type encodings. A result header is not logical-authority
/// input; this helper is only for validating the physical half of a prepared or
/// loaded View snapshot.
[[nodiscard]] Digest
computeViewOutputPhysicalSchemaFingerprint(const NamesAndTypesList & physical_outputs, const ViewOutputTypeBindingLimits & limits = {});

/// V2 fingerprint over ordered output declarations plus the canonical exact
/// non-declaration endpoint table.
[[nodiscard]] Digest computeViewMixedPhysicalSchemaFingerprint(
    const NamesAndTypesList & physical_outputs,
    std::span<const ViewAuxiliaryPhysicalTypeBindingInput> auxiliary_endpoints,
    const ViewOutputTypeBindingLimits & limits = {});

/// Reconstructs the load-time binding input for an output-only View sidecar.
/// It accepts only output ordinals. This is authoritative while output
/// declarations remain the sole allowed ViewExpression producer; a combined
/// stored-expression sidecar must not be routed here.
/// Every type-child path is resolved against the normalized physical output
/// tree; identity is never inferred from the resulting physical type.
[[nodiscard]] BoundObjectPhysicalSchema reconstructViewOutputPhysicalSchema(
    const SchemaObjectID & expected_view,
    UInt64 expected_object_schema_revision,
    const NamesAndTypesList & physical_outputs,
    const PersistedTypeReferences & persisted_references,
    const ViewOutputTypeBindingLimits & limits = {});


/// Mixed V2 load/rename/startup binder. Declaration paths are resolved from
/// physical_outputs; other sites require an exact owner endpoint supplied by
/// the trusted CREATE-metadata walker. Missing, duplicate, or extra endpoints
/// fail closed, and no descriptor is inferred from physical equality.
[[nodiscard]] BoundObjectPhysicalSchema reconstructViewMixedPhysicalSchema(
    const SchemaObjectID & expected_view,
    UInt64 expected_object_schema_revision,
    const NamesAndTypesList & physical_outputs,
    std::span<const ViewAuxiliaryPhysicalTypeBindingInput> auxiliary_endpoints,
    const PersistedTypeReferences & persisted_references,
    const ViewOutputTypeBindingLimits & limits = {});
}
