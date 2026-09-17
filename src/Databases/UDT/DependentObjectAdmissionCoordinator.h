#pragma once

#include <Databases/DatabaseSchemaMutationTransaction.h>
#include <Databases/UDT/AtomicAuthority.h>
#include <Databases/UDT/DependentObjectAdmissionPlanner.h>

#include <optional>
#include <utility>

namespace DB::UDT
{

struct DependentObjectAdmissionCoordinatorLimits
{
    DatabaseSchemaWALLimits schema_wal;
};

class CommittedDependentObjectAdmission final
{
public:
    CommittedDependentObjectAdmission(const CommittedDependentObjectAdmission &) = delete;
    CommittedDependentObjectAdmission & operator=(const CommittedDependentObjectAdmission &) = delete;
    CommittedDependentObjectAdmission(CommittedDependentObjectAdmission &&) noexcept = default;
    CommittedDependentObjectAdmission & operator=(CommittedDependentObjectAdmission &&) noexcept = default;

    const DatabaseSchemaWALCommit & getCommit() const noexcept { return commit; }
    const BoundObjectTypeReferences::Ptr & getBoundUDTReferences() const noexcept { return bound_references; }
    const SidecarExpectationRecord & getSidecarExpectation() const noexcept { return expectation; }
    const AuthorityVerificationStamp::Ptr & getVerificationStamp() const noexcept { return verification_stamp; }
    const AtomicAuthorityPublicationStatistics & getPublicationStatistics() const noexcept { return publication_statistics; }
    BoundObjectTypeReferences::Ptr releaseBoundUDTReferences() noexcept { return std::move(bound_references); }

private:
    CommittedDependentObjectAdmission(
        DatabaseSchemaWALCommit commit_,
        BoundObjectTypeReferences::Ptr bound_references_,
        SidecarExpectationRecord expectation_,
        AuthorityVerificationStamp::Ptr verification_stamp_,
        AtomicAuthorityPublicationStatistics publication_statistics_) noexcept
        : commit(std::move(commit_))
        , bound_references(std::move(bound_references_))
        , expectation(std::move(expectation_))
        , verification_stamp(std::move(verification_stamp_))
        , publication_statistics(publication_statistics_)
    {
    }

    friend class DependentObjectAdmissionCoordinator;

    DatabaseSchemaWALCommit commit;
    BoundObjectTypeReferences::Ptr bound_references;
    SidecarExpectationRecord expectation;
    AuthorityVerificationStamp::Ptr verification_stamp;
    AtomicAuthorityPublicationStatistics publication_statistics;
};

/// Fully fallible pre-durability image for one mapped-table CREATE. Besides
/// preparing and validating the WAL execution, this retains the exact old
/// authority root and the already-prepared replacement publication. The
/// owning DatabaseAtomic can therefore finish storage startup and preallocate
/// its live catalog publication before the first durable write.
class PreparedDependentObjectAdmissionCommit final
{
public:
    PreparedDependentObjectAdmissionCommit(const PreparedDependentObjectAdmissionCommit &) = delete;
    PreparedDependentObjectAdmissionCommit & operator=(const PreparedDependentObjectAdmissionCommit &) = delete;
    PreparedDependentObjectAdmissionCommit(PreparedDependentObjectAdmissionCommit &&) noexcept = default;
    PreparedDependentObjectAdmissionCommit & operator=(PreparedDependentObjectAdmissionCommit &&) = delete;

    UInt64 getTransactionID() const noexcept { return transaction_id; }
    const DatabaseSchemaWALValidatedTransition & getRecoveryTransition() const noexcept { return recovery_transition; }
    const BoundObjectTypeReferences::Ptr & getBoundUDTReferences() const noexcept { return bound_references; }
    const SidecarExpectationRecord & getSidecarExpectation() const noexcept { return expectation; }
    const AuthorityVerificationStamp::Ptr & getVerificationStamp() const noexcept { return verification_stamp; }
    const AtomicAuthorityPublicationStatistics & getPublicationStatistics() const noexcept { return publication_statistics; }

private:
    PreparedDependentObjectAdmissionCommit(
        UInt64 transaction_id_,
        AtomicAuthority::RootSnapshot planning_root_,
        DatabaseSchemaWALValidatedTransition recovery_transition_,
        PreparedDatabaseSchemaMutationExecution execution_,
        AtomicAuthority::PreparedPublication publication_,
        BoundObjectTypeReferences::Ptr bound_references_,
        SidecarExpectationRecord expectation_,
        AuthorityVerificationStamp::Ptr verification_stamp_,
        AtomicAuthorityPublicationStatistics publication_statistics_) noexcept;

    friend class DependentObjectAdmissionCoordinator;

    UInt64 transaction_id;
    AtomicAuthority::RootSnapshot planning_root;
    DatabaseSchemaWALValidatedTransition recovery_transition;
    PreparedDatabaseSchemaMutationExecution execution;
    AtomicAuthority::PreparedPublication publication;
    BoundObjectTypeReferences::Ptr bound_references;
    SidecarExpectationRecord expectation;
    AuthorityVerificationStamp::Ptr verification_stamp;
    AtomicAuthorityPublicationStatistics publication_statistics;
};

/// Durable, still-unpublished mapped-table CREATE. Keeping this state distinct
/// is intentional: DatabaseAtomic must make the matching live table visible in
/// one no-fail publication window, rather than exposing the replacement
/// authority from inside the I/O coordinator.
class DurablyCommittedDependentObjectAdmission final
{
public:
    DurablyCommittedDependentObjectAdmission(const DurablyCommittedDependentObjectAdmission &) = delete;
    DurablyCommittedDependentObjectAdmission & operator=(const DurablyCommittedDependentObjectAdmission &) = delete;
    DurablyCommittedDependentObjectAdmission(DurablyCommittedDependentObjectAdmission &&) noexcept = default;
    DurablyCommittedDependentObjectAdmission & operator=(DurablyCommittedDependentObjectAdmission &&) = delete;

    const DatabaseSchemaWALCommit & getCommit() const noexcept { return commit; }
    const BoundObjectTypeReferences::Ptr & getBoundUDTReferences() const noexcept { return bound_references; }
    const SidecarExpectationRecord & getSidecarExpectation() const noexcept { return expectation; }
    const AuthorityVerificationStamp::Ptr & getVerificationStamp() const noexcept { return verification_stamp; }
    const AtomicAuthorityPublicationStatistics & getPublicationStatistics() const noexcept { return publication_statistics; }

private:
    DurablyCommittedDependentObjectAdmission(
        DatabaseSchemaWALCommit commit_,
        AtomicAuthority::RootSnapshot planning_root_,
        AtomicAuthority::PreparedPublication publication_,
        BoundObjectTypeReferences::Ptr bound_references_,
        SidecarExpectationRecord expectation_,
        AuthorityVerificationStamp::Ptr verification_stamp_,
        AtomicAuthorityPublicationStatistics publication_statistics_) noexcept;

    friend class DependentObjectAdmissionCoordinator;

    DatabaseSchemaWALCommit commit;
    AtomicAuthority::RootSnapshot planning_root;
    AtomicAuthority::PreparedPublication publication;
    BoundObjectTypeReferences::Ptr bound_references;
    SidecarExpectationRecord expectation;
    AuthorityVerificationStamp::Ptr verification_stamp;
    AtomicAuthorityPublicationStatistics publication_statistics;
};

/// Feature-inert execution kernel for an already planned dependent-object
/// CREATE. The caller must retain the owning database's exclusive schema
/// mutation serialization for the complete call. The durable storage kernel
/// can install ordinary Table metadata through its content-addressed mapping,
/// but production Table CREATE must not call this coordinator until preflight,
/// in-memory attach, startup binding and physicalization share that recovery
/// protocol. The coordinator consumes the exact planning-root pin and
/// releases it before returning the indivisible refs+expectation publication
/// package. The caller must retain the owning schema lock through the call;
/// after releasing that lock it must run
/// AtomicAuthority::scanRetired().
class DependentObjectAdmissionCoordinator final
{
public:
    /// Completes every allocation and fallible publication/storage preflight,
    /// but performs no durable write and publishes no authority state.
    [[nodiscard]] static PreparedDependentObjectAdmissionCommit prepareTableCreateCommit(
        AtomicAuthority::RootSnapshot planning_root,
        AtomicAuthority & authority,
        IDatabaseSchemaMutationDurableStorage & storage,
        DatabaseSchemaMutationGuard & mutation_guard,
        PreparedDependentObjectAdmission prepared,
        const DependentObjectAdmissionCoordinatorLimits & limits = {});

    /// Executes the already-preflighted durable transaction without publishing
    /// the replacement authority. On an indeterminate write, the ordinary
    /// schema-mutation fail-stop contract remains in force.
    [[nodiscard]] static DurablyCommittedDependentObjectAdmission commitPreparedTableCreateDurably(
        IDatabaseSchemaMutationDurableStorage & storage,
        DatabaseSchemaMutationGuard & mutation_guard,
        PreparedDependentObjectAdmissionCommit && prepared);

    /// Resolves an indeterminate durable write against the exact retained
    /// transition. A committed image returns the same still-unpublished
    /// package as the ordinary durable path; a rolled-back image is retired
    /// and returns nullopt. Any replay conflict remains fail-stop territory for
    /// the owning database.
    [[nodiscard]] static std::optional<DurablyCommittedDependentObjectAdmission> recoverPreparedTableCreateDurably(
        IDatabaseSchemaMutationDurableStorage & storage,
        DatabaseSchemaMutationGuard & mutation_guard,
        PreparedDependentObjectAdmissionCommit && prepared,
        const std::optional<DatabaseSchemaWALCommit> & commit);

    /// The only authority-publication step. All inputs are already owned and
    /// publication itself is the authority's no-throw pointer/ownership swap.
    [[nodiscard]] static CommittedDependentObjectAdmission
    publishDurablyCommittedTableCreate(AtomicAuthority & authority, DurablyCommittedDependentObjectAdmission committed) noexcept;

    [[nodiscard]] static CommittedDependentObjectAdmission commitPreparedTableCreate(
        AtomicAuthority::RootSnapshot planning_root,
        AtomicAuthority & authority,
        IDatabaseSchemaMutationDurableStorage & storage,
        DatabaseSchemaMutationGuard & mutation_guard,
        PreparedDependentObjectAdmission prepared,
        const DependentObjectAdmissionCoordinatorLimits & limits = {});

private:
    DependentObjectAdmissionCoordinator() = delete;
};

} // namespace DB::UDT
