#pragma once

#include <Databases/DatabaseSchemaMutationTransaction.h>
#include <Databases/UDT/AtomicAuthority.h>
#include <Databases/UDT/DependentObjectMutationPlanner.h>

#include <optional>

namespace DB::UDT
{

class PreparedDependentObjectMutationCommit final
{
public:
    PreparedDependentObjectMutationCommit(const PreparedDependentObjectMutationCommit &) = delete;
    PreparedDependentObjectMutationCommit & operator=(const PreparedDependentObjectMutationCommit &) = delete;
    PreparedDependentObjectMutationCommit(PreparedDependentObjectMutationCommit &&) noexcept = default;
    PreparedDependentObjectMutationCommit & operator=(PreparedDependentObjectMutationCommit &&) = delete;

    UInt64 getTransactionID() const noexcept { return transaction_id; }
    const DatabaseSchemaWALValidatedTransition & getRecoveryTransition() const noexcept { return recovery_transition; }

private:
    PreparedDependentObjectMutationCommit(
        DependentObjectMutationKind kind_,
        UInt64 transaction_id_,
        AtomicAuthority::RootSnapshot planning_root_,
        DatabaseSchemaWALValidatedTransition recovery_transition_,
        PreparedDatabaseSchemaMutationExecution execution_,
        AtomicAuthority::PreparedPublication publication_,
        BoundObjectTypeReferences::Ptr bound_references_,
        std::optional<SidecarExpectationRecord> expectation_,
        AuthorityVerificationStamp::Ptr verification_stamp_) noexcept;

    friend class DependentObjectMutationCoordinator;

    DependentObjectMutationKind kind;
    UInt64 transaction_id;
    AtomicAuthority::RootSnapshot planning_root;
    DatabaseSchemaWALValidatedTransition recovery_transition;
    PreparedDatabaseSchemaMutationExecution execution;
    AtomicAuthority::PreparedPublication publication;
    BoundObjectTypeReferences::Ptr bound_references;
    std::optional<SidecarExpectationRecord> expectation;
    AuthorityVerificationStamp::Ptr verification_stamp;
};

class DurablyCommittedDependentObjectMutation final
{
public:
    DurablyCommittedDependentObjectMutation(const DurablyCommittedDependentObjectMutation &) = delete;
    DurablyCommittedDependentObjectMutation & operator=(const DurablyCommittedDependentObjectMutation &) = delete;
    DurablyCommittedDependentObjectMutation(DurablyCommittedDependentObjectMutation &&) noexcept = default;
    DurablyCommittedDependentObjectMutation & operator=(DurablyCommittedDependentObjectMutation &&) = delete;

private:
    DurablyCommittedDependentObjectMutation(
        DependentObjectMutationKind kind_,
        DatabaseSchemaWALCommit commit_,
        AtomicAuthority::RootSnapshot planning_root_,
        AtomicAuthority::PreparedPublication publication_,
        BoundObjectTypeReferences::Ptr bound_references_,
        std::optional<SidecarExpectationRecord> expectation_,
        AuthorityVerificationStamp::Ptr verification_stamp_) noexcept;

    friend class DependentObjectMutationCoordinator;

    DependentObjectMutationKind kind;
    DatabaseSchemaWALCommit commit;
    AtomicAuthority::RootSnapshot planning_root;
    AtomicAuthority::PreparedPublication publication;
    BoundObjectTypeReferences::Ptr bound_references;
    std::optional<SidecarExpectationRecord> expectation;
    AuthorityVerificationStamp::Ptr verification_stamp;
};

struct PublishedDependentObjectMutation
{
    DependentObjectMutationKind kind{};
    DatabaseSchemaWALCommit commit;
    BoundObjectTypeReferences::Ptr bound_references;
    std::optional<SidecarExpectationRecord> expectation;
    AuthorityVerificationStamp::Ptr verification_stamp;
};

class DependentObjectMutationCoordinator final
{
public:
    [[nodiscard]] static PreparedDependentObjectMutationCommit prepareCommit(
        AtomicAuthority::RootSnapshot planning_root,
        AtomicAuthority & authority,
        IDatabaseSchemaMutationDurableStorage & storage,
        DatabaseSchemaMutationGuard & mutation_guard,
        PreparedDependentObjectMutation prepared,
        const DatabaseSchemaWALLimits & limits = {});

    [[nodiscard]] static DurablyCommittedDependentObjectMutation commitDurably(
        IDatabaseSchemaMutationDurableStorage & storage,
        DatabaseSchemaMutationGuard & mutation_guard,
        PreparedDependentObjectMutationCommit && prepared);

    [[nodiscard]] static std::optional<DurablyCommittedDependentObjectMutation> recoverDurably(
        IDatabaseSchemaMutationDurableStorage & storage,
        DatabaseSchemaMutationGuard & mutation_guard,
        PreparedDependentObjectMutationCommit && prepared,
        const std::optional<DatabaseSchemaWALCommit> & commit);

    [[nodiscard]] static PublishedDependentObjectMutation
    publish(AtomicAuthority & authority, DurablyCommittedDependentObjectMutation committed) noexcept;

private:
    DependentObjectMutationCoordinator() = delete;
};

}
