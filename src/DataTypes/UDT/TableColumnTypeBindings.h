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

inline constexpr std::string_view table_column_physical_schema_fingerprint_domain = "ClickHouse UDT table column physical schema V1";

struct TableColumnTypeBindingInput
{
    String column_name;
    BoundDeclaredTypeResult declared_type;
};

struct TableColumnTypeBindingLimits
{
    /// These are implementation maxima. Callers may lower them but cannot
    /// widen the permanent V1 admission domain.
    PersistedTypeReferencesLimits persisted{
        .maximum_sidecar_bytes = 16ULL << 20,
        .maximum_descriptors = 4'096,
        .maximum_occurrence_paths = 65'536,
        .maximum_path_depth = 64,
        .maximum_canonical_arguments_bytes = 64ULL << 10,
        .maximum_canonical_physical_type_bytes = 64ULL << 10,
        .maximum_qualified_name_bytes = 4ULL << 10,
    };
    UInt64 maximum_columns = 10'000;
    UInt64 maximum_total_column_name_bytes = 16ULL << 20;
    UInt64 maximum_bound_nodes = 1ULL << 20;
    UInt64 maximum_descriptor_occurrences = 65'536;
    UInt64 maximum_definition_handles = 65'536;
    UInt64 maximum_distinct_definition_handles = 1'024;
    UInt64 maximum_retained_path_components = 4ULL << 20;
    UInt64 maximum_single_runtime_owner_key_bytes = 1ULL << 20;
    UInt64 maximum_retained_runtime_owner_key_bytes = 64ULL << 20;
};

/// Pure, pre-persistence result for one already-identified Atomic table. The
/// caller supplies fully resolved declared types; this component performs no
/// parsing, catalog lookup, access check, storage mutation, or publication.
///
/// The physical column list is always present. Logical state is sparse: all
/// optional values are absent, and dependency_edges is empty, when every input
/// is physical-only. A later database-owned mutation may consume the four
/// logical outputs only as one indivisible package.
struct PreparedTableColumnTypeBindings
{
    NamesAndTypesList physical_columns;
    Digest physical_schema_fingerprint{};
    std::optional<PersistedTypeReferences> persisted_references;
    std::optional<BoundObjectPhysicalSchema> bound_physical_schema;
    std::optional<SidecarExpectationRecord> sidecar_expectation;
    std::vector<SchemaObjectDependencyEdge> dependency_edges;
};

class TableColumnTypeBindingError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        InvalidObject,
        InvalidColumn,
        CrossDatabaseReference,
        ConflictingDescriptor,
        SidecarMismatch,
        PhysicalSchemaMismatch,
        PathMismatch,
        LimitExceeded,
    };

    TableColumnTypeBindingError(Code code_, std::string_view message);

    const Code code;
};

/// Composes column-local BoundDeclaredTypeResult values into the permanent V1
/// table occurrence paths. Object ordinals are zero-based physical-column
/// ordinals; child ordinals come only from BoundDeclaredTypeTree. Multiple
/// logical applications at one physical node receive consecutive occurrence
/// ordinals and are never collapsed.
[[nodiscard]] PreparedTableColumnTypeBindings prepareTableColumnTypeBindings(
    const SchemaObjectID & table,
    UInt64 object_schema_revision,
    std::span<const TableColumnTypeBindingInput> columns,
    const TableColumnTypeBindingLimits & limits = {});

/// Computes the permanent V1 fingerprint over an ordered, normalized table
/// column schema. The input is the ordinary + materialized physical column set
/// used by StorageInMemoryMetadata; aliases and ephemeral columns are outside
/// this adapter's schema.
[[nodiscard]] Digest computeTableColumnPhysicalSchemaFingerprint(
    const NamesAndTypesList & physical_columns, const TableColumnTypeBindingLimits & limits = {});

/// Reconstructs the table-kind adapter input for load-time binding from the
/// normalized physical columns and their permanent V1 sidecar. The expected
/// identity and revision come from the owning Atomic metadata snapshot; they
/// must match the sidecar exactly. Every persisted column/type-child path is
/// resolved against the physical tree without consulting names, a catalog, or
/// a definition authority.
///
/// The returned occurrences preserve canonical sidecar order and select no
/// semantic-policy capabilities. Feed this value directly to
/// BoundObjectTypeReferences::bind after the database has enabled
/// dependent-object admission for the same snapshot.
[[nodiscard]] BoundObjectPhysicalSchema reconstructTableColumnPhysicalSchema(
    const SchemaObjectID & expected_table,
    UInt64 expected_object_schema_revision,
    const NamesAndTypesList & physical_columns,
    const PersistedTypeReferences & persisted_references,
    const TableColumnTypeBindingLimits & limits = {});

}
