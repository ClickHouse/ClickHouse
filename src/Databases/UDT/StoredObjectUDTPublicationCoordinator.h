#pragma once

#include <Databases/DatabaseSchemaMutationTransaction.h>
#include <Databases/UDT/AuthorityVerificationStamp.h>
#include <Databases/UDT/StoredObjectUDTPublicationPackage.h>

#include <DataTypes/UDT/BoundObjectTypeReferences.h>

#include <optional>
#include <vector>

namespace DB::UDT
{

struct StoredObjectUDTPublicationCoordinatorLimits
{
    StoredObjectUDTMetadataValidationLimits metadata_validation;
    StoredObjectUDTPublicationPackageLimits publication_package;
    BoundObjectTypeReferencesLimits bound_references;
    DatabaseSchemaWALLimits schema_wal;
};

class CommittedStoredObjectUDTPublication final
{
public:
    CommittedStoredObjectUDTPublication(const CommittedStoredObjectUDTPublication &) = delete;
    CommittedStoredObjectUDTPublication & operator=(const CommittedStoredObjectUDTPublication &) = delete;
    CommittedStoredObjectUDTPublication(CommittedStoredObjectUDTPublication &&) noexcept = default;
    CommittedStoredObjectUDTPublication & operator=(CommittedStoredObjectUDTPublication &&) = delete;

    const DatabaseSchemaWALCommit & getCommit() const noexcept { return commit; }
    const ValidatedStoredObjectUDTMetadata & getValidatedMetadata() const noexcept { return validated_metadata; }
    const PersistedTypeReferences & getPersistedReferences() const noexcept { return persisted_references; }
    const BoundObjectTypeReferences::Ptr & getBoundUDTReferences() const noexcept { return bound_references; }
    const SidecarExpectationRecord & getExpectationRecord() const noexcept { return expectation_record; }
    const AuthorityVerificationStamp::Ptr & getVerificationStamp() const noexcept { return verification_stamp; }
    const StoredObjectUDTPublicationPackageStatistics & getPackageStatistics() const noexcept { return package_statistics; }
    const AtomicAuthorityPublicationStatistics & getAuthorityPublicationStatistics() const noexcept
    {
        return authority_publication_statistics;
    }

private:
    CommittedStoredObjectUDTPublication(
        DatabaseSchemaWALCommit commit_,
        ValidatedStoredObjectUDTMetadata validated_metadata_,
        PersistedTypeReferences persisted_references_,
        BoundObjectTypeReferences::Ptr bound_references_,
        SidecarExpectationRecord expectation_record_,
        AuthorityVerificationStamp::Ptr verification_stamp_,
        StoredObjectUDTPublicationPackageStatistics package_statistics_,
        AtomicAuthorityPublicationStatistics authority_publication_statistics_) noexcept;

    friend class StoredObjectUDTPublicationCoordinator;

    DatabaseSchemaWALCommit commit;
    ValidatedStoredObjectUDTMetadata validated_metadata;
    PersistedTypeReferences persisted_references;
    BoundObjectTypeReferences::Ptr bound_references;
    SidecarExpectationRecord expectation_record;
    AuthorityVerificationStamp::Ptr verification_stamp;
    StoredObjectUDTPublicationPackageStatistics package_statistics;
    AtomicAuthorityPublicationStatistics authority_publication_statistics;
};

/// Fully fallible pre-durability image for one View or Dictionary CREATE. The
/// matching live object must be preallocated before this image is committed.
class PreparedStoredObjectUDTPublicationCommit final
{
public:
    PreparedStoredObjectUDTPublicationCommit(const PreparedStoredObjectUDTPublicationCommit &) = delete;
    PreparedStoredObjectUDTPublicationCommit & operator=(const PreparedStoredObjectUDTPublicationCommit &) = delete;
    PreparedStoredObjectUDTPublicationCommit(PreparedStoredObjectUDTPublicationCommit &&) noexcept = default;
    PreparedStoredObjectUDTPublicationCommit & operator=(PreparedStoredObjectUDTPublicationCommit &&) = delete;

    UInt64 getTransactionID() const noexcept { return transaction_id; }
    const DatabaseSchemaWALValidatedTransition & getRecoveryTransition() const noexcept { return recovery_transition; }
    const ValidatedStoredObjectUDTMetadata & getValidatedMetadata() const noexcept { return publication_package.getValidatedMetadata(); }
    const PersistedTypeReferences & getPersistedReferences() const noexcept { return publication_package.getPersistedReferences(); }
    const BoundObjectTypeReferences::Ptr & getBoundUDTReferences() const noexcept { return bound_references; }
    const SidecarExpectationRecord & getExpectationRecord() const noexcept { return publication_package.getExpectationRecord(); }
    const AuthorityVerificationStamp::Ptr & getVerificationStamp() const noexcept { return verification_stamp; }

private:
    PreparedStoredObjectUDTPublicationCommit(
        UInt64 transaction_id_,
        DatabaseSchemaWALValidatedTransition recovery_transition_,
        PreparedDatabaseSchemaMutationExecution execution_,
        AtomicAuthority::PreparedPublication authority_publication_,
        StoredObjectUDTPublicationPackage publication_package_,
        BoundObjectTypeReferences::Ptr bound_references_,
        AuthorityVerificationStamp::Ptr verification_stamp_,
        AtomicAuthorityPublicationStatistics authority_publication_statistics_) noexcept;

    friend class StoredObjectUDTPublicationCoordinator;

    UInt64 transaction_id;
    DatabaseSchemaWALValidatedTransition recovery_transition;
    PreparedDatabaseSchemaMutationExecution execution;
    AtomicAuthority::PreparedPublication authority_publication;
    StoredObjectUDTPublicationPackage publication_package;
    BoundObjectTypeReferences::Ptr bound_references;
    AuthorityVerificationStamp::Ptr verification_stamp;
    AtomicAuthorityPublicationStatistics authority_publication_statistics;
};

/// Durable but deliberately unpublished authority image. DatabaseAtomic must
/// publish it in the same no-fail window as the matching live object.
class DurablyCommittedStoredObjectUDTPublication final
{
public:
    DurablyCommittedStoredObjectUDTPublication(const DurablyCommittedStoredObjectUDTPublication &) = delete;
    DurablyCommittedStoredObjectUDTPublication & operator=(const DurablyCommittedStoredObjectUDTPublication &) = delete;
    DurablyCommittedStoredObjectUDTPublication(DurablyCommittedStoredObjectUDTPublication &&) noexcept = default;
    DurablyCommittedStoredObjectUDTPublication & operator=(DurablyCommittedStoredObjectUDTPublication &&) = delete;

    const DatabaseSchemaWALCommit & getCommit() const noexcept { return commit; }
    const ValidatedStoredObjectUDTMetadata & getValidatedMetadata() const noexcept { return publication_package.getValidatedMetadata(); }
    const PersistedTypeReferences & getPersistedReferences() const noexcept { return publication_package.getPersistedReferences(); }
    const BoundObjectTypeReferences::Ptr & getBoundUDTReferences() const noexcept { return bound_references; }
    const SidecarExpectationRecord & getExpectationRecord() const noexcept { return publication_package.getExpectationRecord(); }
    const AuthorityVerificationStamp::Ptr & getVerificationStamp() const noexcept { return verification_stamp; }

private:
    DurablyCommittedStoredObjectUDTPublication(
        DatabaseSchemaWALCommit commit_,
        AtomicAuthority::PreparedPublication authority_publication_,
        StoredObjectUDTPublicationPackage publication_package_,
        BoundObjectTypeReferences::Ptr bound_references_,
        AuthorityVerificationStamp::Ptr verification_stamp_,
        AtomicAuthorityPublicationStatistics authority_publication_statistics_) noexcept;

    friend class StoredObjectUDTPublicationCoordinator;

    DatabaseSchemaWALCommit commit;
    AtomicAuthority::PreparedPublication authority_publication;
    StoredObjectUDTPublicationPackage publication_package;
    BoundObjectTypeReferences::Ptr bound_references;
    AuthorityVerificationStamp::Ptr verification_stamp;
    AtomicAuthorityPublicationStatistics authority_publication_statistics;
};

/// Durable transaction coordinator for the closed View/Dictionary UDT
/// admission path. The caller retains the owning database schema lock through
/// prepare, commit/recovery, live-object publication, and authority publication.
class StoredObjectUDTPublicationCoordinator final
{
public:
    [[nodiscard]] static PreparedStoredObjectUDTPublicationCommit prepareCreateCommit(
        AtomicAuthority::RootSnapshot planning_root,
        AtomicAuthority & authority,
        IDatabaseSchemaMutationDurableStorage & storage,
        DatabaseSchemaMutationGuard & mutation_guard,
        UInt64 transaction_id,
        StoredObjectUDTPublicationAdmissionProof admission_proof,
        BoundObjectPhysicalSchema physical_schema,
        String candidate_metadata_bytes,
        String canonical_sidecar_bytes,
        SidecarExpectationRecord expected_expectation,
        std::vector<SchemaObjectID> object_dependencies,
        const IStoredObjectUDTMetadataValidator & metadata_validator,
        const StoredObjectUDTPublicationCoordinatorLimits & limits = {});

    [[nodiscard]] static DurablyCommittedStoredObjectUDTPublication commitPreparedCreateDurably(
        IDatabaseSchemaMutationDurableStorage & storage,
        DatabaseSchemaMutationGuard & mutation_guard,
        PreparedStoredObjectUDTPublicationCommit && prepared);

    [[nodiscard]] static std::optional<DurablyCommittedStoredObjectUDTPublication> recoverPreparedCreateDurably(
        IDatabaseSchemaMutationDurableStorage & storage,
        DatabaseSchemaMutationGuard & mutation_guard,
        PreparedStoredObjectUDTPublicationCommit && prepared,
        const std::optional<DatabaseSchemaWALCommit> & commit);

    [[nodiscard]] static CommittedStoredObjectUDTPublication
    publishDurablyCommittedCreate(AtomicAuthority & authority, DurablyCommittedStoredObjectUDTPublication committed) noexcept;

private:
    StoredObjectUDTPublicationCoordinator() = delete;
};

}
