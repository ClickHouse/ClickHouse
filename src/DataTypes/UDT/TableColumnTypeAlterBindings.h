#pragma once

#include <DataTypes/UDT/BoundObjectTypeReferences.h>
#include <DataTypes/UDT/SidecarExpectationRecord.h>
#include <DataTypes/UDT/TableColumnTypeBindings.h>
#include <DataTypes/IDataType_fwd.h>

#include <Core/NamesAndTypes.h>

#include <memory>
#include <mutex>
#include <optional>
#include <span>

namespace DB::UDT
{

class AuthorityVerificationStamp;

enum class TableColumnTypeAlterOperationKind : UInt8
{
    Replace = 1,
    Drop = 2,
    Rename = 3,
};

/// One already-prepared logical effect of an ALTER command. A Replace with no
/// replacement sidecar deliberately makes the target column physical-only.
/// A replacement sidecar is a one-column fragment produced by the ordinary
/// table-column binder; its object ordinal is rebased by the composer.
struct TableColumnTypeAlterOperation
{
    TableColumnTypeAlterOperationKind kind{};
    String column_name;
    String target_name;
    DataTypePtr replacement_physical_type;
    std::optional<PersistedTypeReferences> replacement_column_references;
    /// Exact physical schema after this command was applied. Replace fragments
    /// are materialized against this intermediate image; later commands may
    /// legitimately drop or rename those columns before the final schema.
    NamesAndTypesList physical_columns_after_operation;
};

struct CompletedTableColumnTypeAlterPublication
{
    BoundObjectTypeReferences::Ptr bound_references;
    std::optional<SidecarExpectationRecord> expectation;
    std::shared_ptr<const AuthorityVerificationStamp> verification_stamp;
};

/// Immutable desired sidecar plus a one-shot completion cell. Table-column
/// ALTER preparation owns the column-path composition; mapped View/Dictionary
/// ALTER preparation supplies an already-complete object-kind-specific
/// successor sidecar. DatabaseAtomic completes the same cell only after the
/// exact desired sidecar has been rebound, committed, and published with its
/// durable expectation. The storage's later metadata publication consumes that
/// completed package instead of losing provenance.
class PreparedTableColumnTypeAlter final
{
public:
    PreparedTableColumnTypeAlter(const PreparedTableColumnTypeAlter &) = delete;
    PreparedTableColumnTypeAlter & operator=(const PreparedTableColumnTypeAlter &) = delete;

    const SchemaObjectID & getObject() const noexcept { return object; }
    UInt64 getBeforeObjectSchemaRevision() const noexcept { return before_object_schema_revision; }
    const std::optional<PersistedTypeReferences> & getDesiredReferences() const noexcept { return desired_references; }
    const NamesAndTypesList & getAfterPhysicalColumns() const noexcept { return after_physical_columns; }

    void completePublication(
        BoundObjectTypeReferences::Ptr bound_references,
        std::optional<SidecarExpectationRecord> expectation,
        std::shared_ptr<const AuthorityVerificationStamp> verification_stamp);
    [[nodiscard]] std::optional<CompletedTableColumnTypeAlterPublication> getCompletedPublication() const;

private:
    PreparedTableColumnTypeAlter(
        SchemaObjectID object_,
        UInt64 before_object_schema_revision_,
        std::optional<PersistedTypeReferences> desired_references_,
        NamesAndTypesList after_physical_columns_);

    friend std::shared_ptr<PreparedTableColumnTypeAlter> prepareTableColumnTypeAlter(
        const NamesAndTypesList &,
        const BoundObjectTypeReferences &,
        const SidecarExpectationRecord &,
        const NamesAndTypesList &,
        std::span<const TableColumnTypeAlterOperation>,
        const TableColumnTypeBindingLimits &);

    friend std::shared_ptr<PreparedTableColumnTypeAlter> prepareInitialTableColumnTypeAlter(
        const NamesAndTypesList &,
        const NamesAndTypesList &,
        std::span<const TableColumnTypeAlterOperation>,
        const TableColumnTypeBindingLimits &);

    friend std::shared_ptr<PreparedTableColumnTypeAlter> prepareStoredObjectTypeAlter(
        const BoundObjectTypeReferences &,
        const SidecarExpectationRecord &,
        const NamesAndTypesList &,
        std::optional<PersistedTypeReferences>,
        const PersistedTypeReferencesLimits &);

    const SchemaObjectID object;
    const UInt64 before_object_schema_revision;
    const std::optional<PersistedTypeReferences> desired_references;
    const NamesAndTypesList after_physical_columns;

    mutable std::mutex completion_mutex;
    bool completion_ready = false;
    CompletedTableColumnTypeAlterPublication completion;
};

/// Reconstructs the exact current sidecar from the immutable bound index,
/// applies the explicit column-level logical operations by column name, then
/// re-canonicalizes descriptor/path dictionaries against the final ordered
/// physical columns. Unexpected path loss or a non-rebindable physical change
/// is rejected before database metadata I/O.
[[nodiscard]] std::shared_ptr<PreparedTableColumnTypeAlter> prepareTableColumnTypeAlter(
    const NamesAndTypesList & before_physical_columns,
    const BoundObjectTypeReferences & before_bound_references,
    const SidecarExpectationRecord & before_expectation,
    const NamesAndTypesList & after_physical_columns,
    std::span<const TableColumnTypeAlterOperation> operations,
    const TableColumnTypeBindingLimits & limits = {});

/// Composes the first logical binding on an existing physical-only table. The
/// table identity and semantic-extension domain are taken from the exact
/// operation-bound binder fragments. Revision 1 denotes the durable ordinary
/// metadata Before image; the desired mapped image is revision 2.
[[nodiscard]] std::shared_ptr<PreparedTableColumnTypeAlter> prepareInitialTableColumnTypeAlter(
    const NamesAndTypesList & before_physical_columns,
    const NamesAndTypesList & after_physical_columns,
    std::span<const TableColumnTypeAlterOperation> operations,
    const TableColumnTypeBindingLimits & limits = {});

/// Builds the common one-shot publication handoff for an already mapped
/// View/MaterializedView/Dictionary successor. `desired_references` is either
/// a complete object-kind-specific sidecar for revision N+1 or empty when the
/// ALTER deliberately removes the last logical occurrence. Physical-schema
/// reconstruction stays in the closed database-owned adapter in the mutation
/// planner; this boundary only proves exact before identity, revision and
/// canonical sidecar ownership.
[[nodiscard]] std::shared_ptr<PreparedTableColumnTypeAlter> prepareStoredObjectTypeAlter(
    const BoundObjectTypeReferences & before_bound_references,
    const SidecarExpectationRecord & before_expectation,
    const NamesAndTypesList & after_physical_columns,
    std::optional<PersistedTypeReferences> desired_references,
    const PersistedTypeReferencesLimits & limits = {});

/// Metadata-only mapped stored-object ALTER counterpart. Reconstructs the
/// exact retained canonical sidecar from the immutable bound snapshot, checks
/// its hash against the durable expectation, and advances only the object
/// schema revision. The object-kind adapter later revalidates the unchanged
/// physical fingerprint against the successor canonical metadata.
[[nodiscard]] PersistedTypeReferences rebaseBoundStoredObjectTypeReferences(
    const BoundObjectTypeReferences & retained_bound_references,
    const SidecarExpectationRecord & retained_expectation,
    const PersistedTypeReferencesLimits & limits = {});

/// Rebase a fresh CREATE binding package through ordinary physical-column
/// normalization (currently root-level Nested flattening). `operations` are
/// one-column binder fragments for the pre-normalized declarations; the same
/// occurrence composer used by ALTER maps them onto `normalized_physical_columns`.
/// The table identity and schema revision remain unchanged.
[[nodiscard]] PreparedTableColumnTypeBindings rebaseInitialTableColumnTypeBindingsAfterNormalization(
    PreparedTableColumnTypeBindings initial_bindings,
    const NamesAndTypesList & normalized_physical_columns,
    std::span<const TableColumnTypeAlterOperation> operations,
    const TableColumnTypeBindingLimits & limits = {});

/// Builds an exact desired sidecar for an internal durable rollback from a
/// previously retained bound snapshot. Its object revision is advanced from
/// `successor_of_revision`, never copied backwards from the retained snapshot.
[[nodiscard]] std::optional<PersistedTypeReferences> rebaseBoundTableColumnTypeReferences(
    const NamesAndTypesList & physical_columns,
    const BoundObjectTypeReferences & retained_bound_references,
    const SidecarExpectationRecord & retained_expectation,
    UInt64 successor_of_revision,
    const TableColumnTypeBindingLimits & limits = {});

}
