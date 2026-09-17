#include <Databases/UDT/DependentObjectAdmissionCoordinator.h>

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

} // namespace

PreparedDependentObjectAdmissionCommit::PreparedDependentObjectAdmissionCommit(
    UInt64 transaction_id_,
    AtomicAuthority::RootSnapshot planning_root_,
    DatabaseSchemaWALValidatedTransition recovery_transition_,
    PreparedDatabaseSchemaMutationExecution execution_,
    AtomicAuthority::PreparedPublication publication_,
    BoundObjectTypeReferences::Ptr bound_references_,
    SidecarExpectationRecord expectation_,
    AuthorityVerificationStamp::Ptr verification_stamp_,
    AtomicAuthorityPublicationStatistics publication_statistics_) noexcept
    : transaction_id(transaction_id_)
    , planning_root(std::move(planning_root_))
    , recovery_transition(std::move(recovery_transition_))
    , execution(std::move(execution_))
    , publication(std::move(publication_))
    , bound_references(std::move(bound_references_))
    , expectation(std::move(expectation_))
    , verification_stamp(std::move(verification_stamp_))
    , publication_statistics(publication_statistics_)
{
}

DurablyCommittedDependentObjectAdmission::DurablyCommittedDependentObjectAdmission(
    DatabaseSchemaWALCommit commit_,
    AtomicAuthority::RootSnapshot planning_root_,
    AtomicAuthority::PreparedPublication publication_,
    BoundObjectTypeReferences::Ptr bound_references_,
    SidecarExpectationRecord expectation_,
    AuthorityVerificationStamp::Ptr verification_stamp_,
    AtomicAuthorityPublicationStatistics publication_statistics_) noexcept
    : commit(std::move(commit_))
    , planning_root(std::move(planning_root_))
    , publication(std::move(publication_))
    , bound_references(std::move(bound_references_))
    , expectation(std::move(expectation_))
    , verification_stamp(std::move(verification_stamp_))
    , publication_statistics(publication_statistics_)
{
}

PreparedDependentObjectAdmissionCommit DependentObjectAdmissionCoordinator::prepareTableCreateCommit(
    AtomicAuthority::RootSnapshot planning_root,
    AtomicAuthority & authority,
    IDatabaseSchemaMutationDurableStorage & storage,
    DatabaseSchemaMutationGuard & mutation_guard,
    PreparedDependentObjectAdmission prepared,
    const DependentObjectAdmissionCoordinatorLimits & limits)
{
    if (!planning_root || !prepared.wasPlannedFrom(planning_root.get()))
        throw std::logic_error(
            "dependent-object admission was not planned from "
            "the retained authority root");

    const UInt64 transaction_id = prepared.getValidatedTransition().getPrepare().transaction_id;
    auto bound_references = prepared.getBoundUDTReferences();
    auto verification_stamp = prepared.getVerificationStamp();
    if (!bound_references || !verification_stamp)
        throw std::logic_error("dependent-object admission has no verified rebound publication package");
    const auto * expectation = prepared.getReplacementRoot().findExpectationRecord(bound_references->getObject());
    if (!expectation || expectation->object_schema_revision != bound_references->getObjectSchemaRevision()
        || expectation->sidecar_hash != bound_references->getSidecarHash()
        || expectation->physical_schema_fingerprint != bound_references->getPhysicalSchemaFingerprint())
    {
        throw std::logic_error(
            "dependent-object admission has no exact "
            "refs+expectation publication package");
    }
    SidecarExpectationRecord retained_expectation = *expectation;

    std::vector<String> recovery_staged_artifact_bytes(
        prepared.getValidatedTransition().getStagedArtifactBytes().begin(),
        prepared.getValidatedTransition().getStagedArtifactBytes().end());
    auto recovery_transition = DatabaseSchemaWALTransitionBuilder::validateDecoded(
        prepared.getValidatedTransition().getPrepare(),
        {
            .authority_state = planning_root.get().getAuthorityState(),
            .authority_inventory = planning_root.get().pinAuthorityInventory(),
            .schema_graph = planning_root.get().pinSchemaObjectDependencyGraph(),
        },
        std::move(recovery_staged_artifact_bytes),
        limits.schema_wal);
    auto execution = prepareDatabaseSchemaMutationExecution(prepared.getValidatedTransition(), limits.schema_wal);
    auto publication = authority.preparePublication(prepared.releaseReplacementRoot());
    const auto publication_statistics = publication.getStatistics();
    validatePreparedDatabaseSchemaMutationExecution(storage, mutation_guard, execution);

    return PreparedDependentObjectAdmissionCommit(
        transaction_id,
        std::move(planning_root),
        std::move(recovery_transition),
        std::move(execution),
        std::move(publication),
        std::move(bound_references),
        std::move(retained_expectation),
        std::move(verification_stamp),
        publication_statistics);
}

DurablyCommittedDependentObjectAdmission DependentObjectAdmissionCoordinator::commitPreparedTableCreateDurably(
    IDatabaseSchemaMutationDurableStorage & storage,
    DatabaseSchemaMutationGuard & mutation_guard,
    PreparedDependentObjectAdmissionCommit && prepared)
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

    return DurablyCommittedDependentObjectAdmission(
        std::move(commit),
        std::move(prepared.planning_root),
        std::move(prepared.publication),
        std::move(prepared.bound_references),
        std::move(prepared.expectation),
        std::move(prepared.verification_stamp),
        prepared.publication_statistics);
}

std::optional<DurablyCommittedDependentObjectAdmission> DependentObjectAdmissionCoordinator::recoverPreparedTableCreateDurably(
    IDatabaseSchemaMutationDurableStorage & storage,
    DatabaseSchemaMutationGuard & mutation_guard,
    PreparedDependentObjectAdmissionCommit && prepared,
    const std::optional<DatabaseSchemaWALCommit> & commit)
{
    const auto decision = recoverDatabaseSchemaMutation(storage, mutation_guard, prepared.recovery_transition, commit);
    if (decision == DatabaseSchemaWALRecoveryDecision::RollBackPrepared)
    {
        retireRolledBackDatabaseSchemaMutation(storage, mutation_guard, prepared.transaction_id);
        return std::nullopt;
    }
    if (!commit)
        throw std::logic_error("committed dependent-object admission recovery has no Commit marker");

    return DurablyCommittedDependentObjectAdmission(
        *commit,
        std::move(prepared.planning_root),
        std::move(prepared.publication),
        std::move(prepared.bound_references),
        std::move(prepared.expectation),
        std::move(prepared.verification_stamp),
        prepared.publication_statistics);
}

CommittedDependentObjectAdmission DependentObjectAdmissionCoordinator::publishDurablyCommittedTableCreate(
    AtomicAuthority & authority, DurablyCommittedDependentObjectAdmission committed) noexcept
{
    authority.publish(std::move(committed.publication));
    return CommittedDependentObjectAdmission(
        std::move(committed.commit),
        std::move(committed.bound_references),
        std::move(committed.expectation),
        std::move(committed.verification_stamp),
        committed.publication_statistics);
}

CommittedDependentObjectAdmission DependentObjectAdmissionCoordinator::commitPreparedTableCreate(
    AtomicAuthority::RootSnapshot planning_root,
    AtomicAuthority & authority,
    IDatabaseSchemaMutationDurableStorage & storage,
    DatabaseSchemaMutationGuard & mutation_guard,
    PreparedDependentObjectAdmission prepared,
    const DependentObjectAdmissionCoordinatorLimits & limits)
{
    auto commit = prepareTableCreateCommit(std::move(planning_root), authority, storage, mutation_guard, std::move(prepared), limits);
    auto durable = commitPreparedTableCreateDurably(storage, mutation_guard, std::move(commit));
    return publishDurablyCommittedTableCreate(authority, std::move(durable));
}

} // namespace DB::UDT
