#pragma once

#include <Databases/DatabaseSchemaWAL.h>
#include <Databases/UDT/AuthorityRoot.h>
#include <Databases/UDT/AuthorityVerificationStamp.h>

#include <DataTypes/UDT/TableColumnTypeBindings.h>

#include <Core/Types.h>

#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace DB::UDT
{

struct DependentObjectAdmissionPlannerLimits
{
    DependentObjectAdmissionPlannerLimits();

    AuthorityRootBuildLimits authority_root;
    DatabaseSchemaWALLimits schema_wal;
    TableColumnTypeBindingLimits table_columns;
    BoundObjectTypeReferencesLimits bound_references;
};

class ValidatedDependentTableMetadata final
{
public:
    ValidatedDependentTableMetadata(const ValidatedDependentTableMetadata &) = delete;
    ValidatedDependentTableMetadata & operator=(const ValidatedDependentTableMetadata &) = delete;
    ValidatedDependentTableMetadata(ValidatedDependentTableMetadata &&) noexcept = default;
    ValidatedDependentTableMetadata & operator=(ValidatedDependentTableMetadata &&) noexcept = default;

    const SchemaObjectID & getObject() const noexcept { return object; }
    UInt64 getObjectSchemaRevision() const noexcept { return object_schema_revision; }
    const String & getObjectName() const noexcept { return object_name; }
    const Digest & getPhysicalSchemaFingerprint() const noexcept { return physical_schema_fingerprint; }
    const Digest & getCanonicalMetadataHash() const noexcept { return canonical_metadata_hash; }
    String releaseCanonicalMetadataBytes() noexcept { return std::move(canonical_metadata_bytes); }

private:
    ValidatedDependentTableMetadata(
        SchemaObjectID object_,
        UInt64 object_schema_revision_,
        String object_name_,
        Digest physical_schema_fingerprint_,
        String canonical_metadata_bytes_,
        Digest canonical_metadata_hash_);

    friend class IDependentTableMetadataValidator;

    SchemaObjectID object;
    UInt64 object_schema_revision = 0;
    String object_name;
    Digest physical_schema_fingerprint{};
    String canonical_metadata_bytes;
    Digest canonical_metadata_hash{};
};

/// Database-engine boundary for the ordinary table metadata image. A
/// production implementation must parse the candidate bytes, verify the exact
/// table UUID/revision and ordered physical schema, and return its canonical
/// encoding. Admission never treats caller-provided metadata bytes as trusted.
class IDependentTableMetadataValidator
{
public:
    virtual ~IDependentTableMetadataValidator() = default;

    [[nodiscard]] ValidatedDependentTableMetadata validateAndCanonicalize(
        const SidecarExpectationRecord & expectation,
        std::string_view candidate_metadata_bytes,
        std::string_view canonical_sidecar_bytes) const;

protected:
    struct DecodedTableMetadata
    {
        SchemaObjectID object;
        UInt64 object_schema_revision = 0;
        String object_name;
        Digest sidecar_hash{};
        Digest physical_schema_fingerprint{};
        String canonical_metadata_bytes;
    };

    virtual DecodedTableMetadata
    decodeAndCanonicalize(std::string_view candidate_metadata_bytes, std::string_view canonical_sidecar_bytes) const = 0;
};

class DependentObjectAdmissionPlannerError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        InvalidRequest,
        DatabaseMismatch,
        ExpectedEpochMismatch,
        InvalidRevision,
        InvalidBindings,
        InvalidMetadata,
        DefinitionNotFound,
        DefinitionMismatch,
        DependencyMismatch,
        ObjectAlreadyExists,
        IntegrityMismatch,
        LimitExceeded,
        InvalidBase,
        InvalidTransition,
    };

    DependentObjectAdmissionPlannerError(Code code_, std::string_view message);

    const Code code;
};

/// Pure pre-I/O result for one create-only table admission. The rebound
/// references are derived from the exact caller-pinned root and may later be
/// published with the table metadata only by a database-owned transaction.
class PreparedDependentObjectAdmission final
{
public:
    PreparedDependentObjectAdmission(const PreparedDependentObjectAdmission &) = delete;
    PreparedDependentObjectAdmission & operator=(const PreparedDependentObjectAdmission &) = delete;
    PreparedDependentObjectAdmission(PreparedDependentObjectAdmission &&) noexcept = default;
    PreparedDependentObjectAdmission & operator=(PreparedDependentObjectAdmission &&) noexcept = default;

    const AuthorityRoot & getReplacementRoot() const noexcept { return *replacement_root; }
    const DatabaseSchemaWALValidatedTransition & getValidatedTransition() const noexcept { return transition; }
    const BoundObjectTypeReferences::Ptr & getBoundUDTReferences() const noexcept { return bound_references; }
    const AuthorityVerificationStamp::Ptr & getVerificationStamp() const noexcept { return verification_stamp; }
    bool wasPlannedFrom(const AuthorityRoot & root) const noexcept { return planning_root == &root; }
    AuthorityRoot::Ptr releaseReplacementRoot() noexcept { return std::move(replacement_root); }

private:
    PreparedDependentObjectAdmission(
        const AuthorityRoot * planning_root_,
        AuthorityRoot::Ptr replacement_root_,
        DatabaseSchemaWALValidatedTransition transition_,
        BoundObjectTypeReferences::Ptr bound_references_,
        AuthorityVerificationStamp::Ptr verification_stamp_);

    friend class DependentObjectAdmissionPlanner;

    const AuthorityRoot * planning_root;
    AuthorityRoot::Ptr replacement_root;
    DatabaseSchemaWALValidatedTransition transition;
    BoundObjectTypeReferences::Ptr bound_references;
    AuthorityVerificationStamp::Ptr verification_stamp;
};

class DependentObjectAdmissionPlanner final
{
public:
    /// Plans only the first durable admission of a logically bound Atomic
    /// table. The transition carries the exact database-local installation
    /// mapping consumed by DatabaseAtomic's registered metadata-path adapter.
    [[nodiscard]] static PreparedDependentObjectAdmission planTableCreate(
        const AuthorityRoot & current_root,
        UInt64 transaction_id,
        UInt64 expected_database_catalog_epoch,
        const PreparedTableColumnTypeBindings & table_bindings,
        String candidate_metadata_bytes,
        const IDependentTableMetadataValidator & metadata_validator,
        const DependentObjectAdmissionPlannerLimits & limits = {});

    [[nodiscard]] static PreparedDependentObjectAdmission planTableCreate(
        const AuthorityRoot & current_root,
        UInt64 transaction_id,
        UInt64 expected_database_catalog_epoch,
        const PreparedTableColumnTypeBindings & table_bindings,
        std::span<const SchemaObjectID> object_dependencies,
        String candidate_metadata_bytes,
        const IDependentTableMetadataValidator & metadata_validator,
        const DependentObjectAdmissionPlannerLimits & limits = {});

private:
    DependentObjectAdmissionPlanner() = delete;
};

}
