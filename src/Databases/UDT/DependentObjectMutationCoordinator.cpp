#include <Databases/UDT/DependentObjectMutationCoordinator.h>

#include <exception>
#include <stdexcept>
#include <utility>

namespace DB::UDT
{
namespace
{

void discardIfUnprepared(IDatabaseSchemaMutationDurableStorage & storage, DatabaseSchemaMutationGuard & guard, UInt64 transaction_id)
{
    if (guard.getState() == DatabaseSchemaMutationGuard::State::Ready)
        discardUnpreparedDatabaseSchemaMutationStaging(storage, guard, transaction_id);
}

}

PreparedDependentObjectMutationCommit::PreparedDependentObjectMutationCommit(
    DependentObjectMutationKind kind_,
    UInt64 transaction_id_,
    AtomicAuthority::RootSnapshot planning_root_,
    DatabaseSchemaWALValidatedTransition recovery_transition_,
    PreparedDatabaseSchemaMutationExecution execution_,
    AtomicAuthority::PreparedPublication publication_,
    BoundObjectTypeReferences::Ptr bound_references_,
    std::optional<SidecarExpectationRecord> expectation_,
    AuthorityVerificationStamp::Ptr verification_stamp_) noexcept
    : kind(kind_)
    , transaction_id(transaction_id_)
    , planning_root(std::move(planning_root_))
    , recovery_transition(std::move(recovery_transition_))
    , execution(std::move(execution_))
    , publication(std::move(publication_))
    , bound_references(std::move(bound_references_))
    , expectation(std::move(expectation_))
    , verification_stamp(std::move(verification_stamp_))
{
}

DurablyCommittedDependentObjectMutation::DurablyCommittedDependentObjectMutation(
    DependentObjectMutationKind kind_,
    DatabaseSchemaWALCommit commit_,
    AtomicAuthority::RootSnapshot planning_root_,
    AtomicAuthority::PreparedPublication publication_,
    BoundObjectTypeReferences::Ptr bound_references_,
    std::optional<SidecarExpectationRecord> expectation_,
    AuthorityVerificationStamp::Ptr verification_stamp_) noexcept
    : kind(kind_)
    , commit(std::move(commit_))
    , planning_root(std::move(planning_root_))
    , publication(std::move(publication_))
    , bound_references(std::move(bound_references_))
    , expectation(std::move(expectation_))
    , verification_stamp(std::move(verification_stamp_))
{
}

PreparedDependentObjectMutationCommit DependentObjectMutationCoordinator::prepareCommit(
    AtomicAuthority::RootSnapshot planning_root,
    AtomicAuthority & authority,
    IDatabaseSchemaMutationDurableStorage & storage,
    DatabaseSchemaMutationGuard & mutation_guard,
    PreparedDependentObjectMutation prepared,
    const DatabaseSchemaWALLimits & limits)
{
    if (!planning_root || !prepared.wasPlannedFrom(planning_root.get()))
        throw std::logic_error("dependent-object mutation was not planned from the retained authority root");
    const UInt64 transaction_id = prepared.getValidatedTransition().getPrepare().transaction_id;
    std::vector<String> recovery_bytes(
        prepared.getValidatedTransition().getStagedArtifactBytes().begin(),
        prepared.getValidatedTransition().getStagedArtifactBytes().end());
    auto recovery_transition = DatabaseSchemaWALTransitionBuilder::validateDecoded(
        prepared.getValidatedTransition().getPrepare(),
        {
            .authority_state = planning_root.get().getAuthorityState(),
            .authority_inventory = planning_root.get().pinAuthorityInventory(),
            .schema_graph = planning_root.get().pinSchemaObjectDependencyGraph(),
        },
        std::move(recovery_bytes),
        limits);
    auto execution = prepareDatabaseSchemaMutationExecution(prepared.getValidatedTransition(), limits);
    auto publication = authority.preparePublication(prepared.releaseReplacementRoot());
    auto bound_references = prepared.getBoundUDTReferences();
    auto expectation = prepared.getSidecarExpectation();
    auto verification_stamp = prepared.getVerificationStamp();
    if (static_cast<bool>(bound_references) != static_cast<bool>(expectation)
        || static_cast<bool>(bound_references) != static_cast<bool>(verification_stamp))
        throw std::logic_error("dependent-object mutation has an incomplete verified publication package");
    validatePreparedDatabaseSchemaMutationExecution(storage, mutation_guard, execution);
    return PreparedDependentObjectMutationCommit(
        prepared.getKind(),
        transaction_id,
        std::move(planning_root),
        std::move(recovery_transition),
        std::move(execution),
        std::move(publication),
        std::move(bound_references),
        std::move(expectation),
        std::move(verification_stamp));
}

DurablyCommittedDependentObjectMutation DependentObjectMutationCoordinator::commitDurably(
    IDatabaseSchemaMutationDurableStorage & storage,
    DatabaseSchemaMutationGuard & mutation_guard,
    PreparedDependentObjectMutationCommit && prepared)
{
    DatabaseSchemaWALCommit commit;
    try
    {
        commit = executePreparedDatabaseSchemaMutation(storage, mutation_guard, std::move(prepared.execution));
    }
    catch (...)
    {
        const auto original = std::current_exception();
        discardIfUnprepared(storage, mutation_guard, prepared.transaction_id);
        std::rethrow_exception(original);
    }
    return DurablyCommittedDependentObjectMutation(
        prepared.kind,
        std::move(commit),
        std::move(prepared.planning_root),
        std::move(prepared.publication),
        std::move(prepared.bound_references),
        std::move(prepared.expectation),
        std::move(prepared.verification_stamp));
}

std::optional<DurablyCommittedDependentObjectMutation> DependentObjectMutationCoordinator::recoverDurably(
    IDatabaseSchemaMutationDurableStorage & storage,
    DatabaseSchemaMutationGuard & mutation_guard,
    PreparedDependentObjectMutationCommit && prepared,
    const std::optional<DatabaseSchemaWALCommit> & commit)
{
    const auto decision = recoverDatabaseSchemaMutation(storage, mutation_guard, prepared.recovery_transition, commit);
    if (decision == DatabaseSchemaWALRecoveryDecision::RollBackPrepared)
    {
        retireRolledBackDatabaseSchemaMutation(storage, mutation_guard, prepared.transaction_id);
        return std::nullopt;
    }
    if (!commit)
        throw std::logic_error("committed dependent-object mutation recovery has no Commit marker");
    return DurablyCommittedDependentObjectMutation(
        prepared.kind,
        *commit,
        std::move(prepared.planning_root),
        std::move(prepared.publication),
        std::move(prepared.bound_references),
        std::move(prepared.expectation),
        std::move(prepared.verification_stamp));
}

PublishedDependentObjectMutation
DependentObjectMutationCoordinator::publish(AtomicAuthority & authority, DurablyCommittedDependentObjectMutation committed) noexcept
{
    authority.publish(std::move(committed.publication));
    return {
        .kind = committed.kind,
        .commit = std::move(committed.commit),
        .bound_references = std::move(committed.bound_references),
        .expectation = std::move(committed.expectation),
        .verification_stamp = std::move(committed.verification_stamp),
    };
}

}
