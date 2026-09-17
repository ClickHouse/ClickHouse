#include <Databases/UDT/StoredObjectUDTPublicationCoordinator.h>

#include <Databases/UDT/AuthorityVerificationStampPublication.h>

#include <exception>
#include <stdexcept>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

using PackageError = StoredObjectUDTPublicationPackageError;

[[noreturn]] void fail(PackageError::Code code, std::string_view message)
{
    throw PackageError(code, message);
}

void discardIfUnprepared(IDatabaseSchemaMutationDurableStorage & storage, DatabaseSchemaMutationGuard & guard, UInt64 transaction_id)
{
    if (guard.getState() == DatabaseSchemaMutationGuard::State::Ready)
        discardUnpreparedDatabaseSchemaMutationStaging(storage, guard, transaction_id);
}

class RootBoundAuthorityAdapter final : public IAuthorityAdapter
{
public:
    RootBoundAuthorityAdapter(const AuthorityRoot & root_, TypeAuthorityCapabilities capabilities_) noexcept
        : root(root_)
        , capabilities(std::move(capabilities_))
    {
    }

    const TypeAuthorityCapabilities & getCapabilities() const noexcept override { return capabilities; }
    UUID getDatabaseUUID() const noexcept override { return root.getDatabaseUUID(); }

    ResolutionSession beginResolutionSession() const override
    {
        return makeSnapshotResolutionSession(
            &root,
            {
                .find_by_identity = findByIdentity,
                .find_by_name = findByName,
                .get_generation = getGeneration,
                .get_effective_resource_limits = getEffectiveResourceLimits,
            });
    }

    void requireCapabilities(TypeAuthorityCapabilityMask required, std::string_view) const override
    {
        if (!capabilities.containsAll(required))
            fail(PackageError::Code::InvalidBase, "stored-object publication root lacks required transient capabilities");
    }

private:
    static Definition::Ptr findByIdentity(const void * view, const DefinitionIdentity & identity)
    {
        return static_cast<const AuthorityRoot *>(view)->findByIdentity(identity);
    }

    static Definition::Ptr findByName(const void * view, std::string_view local_name)
    {
        return static_cast<const AuthorityRoot *>(view)->findByName(local_name);
    }

    static UInt64 getGeneration(const void * view) noexcept { return static_cast<const AuthorityRoot *>(view)->getTypeIndexGeneration(); }
    static const EffectiveResourceLimits * getEffectiveResourceLimits(const void * view) noexcept
    {
        return &static_cast<const AuthorityRoot *>(view)->getDatabaseResourceQuota().getLimits();
    }

    const AuthorityRoot & root;
    const TypeAuthorityCapabilities capabilities;
};

BoundObjectTypeReferences::Ptr bindExactReferences(
    const StoredObjectUDTPublicationPackage & publication_package,
    BoundObjectPhysicalSchema physical_schema,
    const TypeAuthorityCapabilities & capabilities,
    const BoundObjectTypeReferencesLimits & limits)
{
    const auto & references = publication_package.getPersistedReferences();
    const auto & expectation = publication_package.getExpectationRecord();
    if (physical_schema.object != references.object || physical_schema.object_schema_revision != references.object_schema_revision
        || physical_schema.physical_schema_fingerprint != references.physical_schema_fingerprint
        || physical_schema.occurrences.size() != references.occurrence_paths.size()
        || references.uses.size() != references.occurrence_paths.size())
    {
        fail(PackageError::Code::IntegrityMismatch, "stored-object physical schema differs from its exact canonical sidecar");
    }
    for (size_t index = 0; index < physical_schema.occurrences.size(); ++index)
    {
        const auto & use = references.uses[index];
        if (use.path_id != index || use.descriptor_id >= references.descriptors.size()
            || physical_schema.occurrences[index].selected_semantic_capabilities
                != references.descriptors[static_cast<size_t>(use.descriptor_id)].getSemanticCapabilities())
        {
            fail(
                PackageError::Code::IntegrityMismatch,
                "stored-object occurrence does not retain its exact descriptor semantic capabilities");
        }
    }
    try
    {
        RootBoundAuthorityAdapter authority(publication_package.getPlanningRoot(), capabilities);
        auto result = BoundObjectTypeReferences::bind(references, std::move(physical_schema), authority, limits);
        if (!result || result->getObject() != expectation.object || result->getObjectSchemaRevision() != expectation.object_schema_revision
            || result->getSidecarHash() != expectation.sidecar_hash
            || result->getPhysicalSchemaFingerprint() != expectation.physical_schema_fingerprint)
        {
            fail(PackageError::Code::IntegrityMismatch, "stored-object rebound references differ from their durable expectation");
        }
        return result;
    }
    catch (const PackageError &)
    {
        throw;
    }
    catch (const BoundObjectTypeReferencesError & error)
    {
        if (error.code == BoundObjectTypeReferencesError::Code::InvalidConfiguration)
            fail(PackageError::Code::InvalidConfiguration, "stored-object bound-reference limits are invalid");
        if (error.code == BoundObjectTypeReferencesError::Code::LimitExceeded)
            fail(PackageError::Code::LimitExceeded, "stored-object rebound references exceed their limit");
        if (error.code == BoundObjectTypeReferencesError::Code::AuthorityFailure)
            fail(PackageError::Code::DefinitionMismatch, "stored-object sidecar cannot bind against its exact authority root");
        fail(PackageError::Code::IntegrityMismatch, "stored-object sidecar differs from its physical schema");
    }
}

DatabaseSchemaWALAuthorityRecordState makeAuthorityRecordState(const AuthorityInventoryLeaf & leaf)
{
    return {
        .object_revision = leaf.object_revision,
        .canonical_record_hash = leaf.canonical_record_hash,
    };
}

DatabaseSchemaWALValidatedTransition buildTransition(
    UInt64 transaction_id, const StoredObjectUDTPublicationPackage & publication_package, const DatabaseSchemaWALLimits & limits)
{
    if (!transaction_id)
        fail(PackageError::Code::InvalidTransition, "stored-object publication transaction ID must be nonzero");

    std::vector<DatabaseSchemaWALAuthorityRecordDelta> authority_deltas;
    authority_deltas.reserve(publication_package.getInventoryLeafDeltas().size());
    for (const auto & delta : publication_package.getInventoryLeafDeltas())
    {
        if ((delta.before && delta.before->key != delta.key) || (delta.after && delta.after->key != delta.key))
            fail(PackageError::Code::InvalidTransition, "stored-object publication inventory delta has a mismatched key");
        authority_deltas.push_back({
            .key = delta.key,
            .before = delta.before ? std::optional{makeAuthorityRecordState(*delta.before)} : std::nullopt,
            .after = delta.after ? std::optional{makeAuthorityRecordState(*delta.after)} : std::nullopt,
        });
    }

    const auto & metadata = publication_package.getValidatedMetadata();
    const auto & expectation = publication_package.getExpectationRecord();
    std::vector<DatabaseSchemaWALDependentObjectDelta> dependent_deltas{{
        .object = metadata.getObject(),
        .before = std::nullopt,
        .after = DatabaseSchemaWALDependentObjectState{
            .object_schema_revision = metadata.getObjectSchemaRevision(),
            .metadata_hash = metadata.getCanonicalMetadataHash(),
            .sidecar_record_hash = expectation.sidecar_hash,
            .expectation_record_hash = publication_package.getExpectationRecordHash(),
        },
    }};
    std::vector<DatabaseSchemaWALStagedArtifact> staged_artifacts{
        {
            .kind = DatabaseSchemaWALStagedArtifactKind::SidecarExpectationRecord,
            .image = DatabaseSchemaWALStagedArtifactImage::After,
            .object = metadata.getObject(),
            .revision = metadata.getObjectSchemaRevision(),
            .canonical_bytes = publication_package.getCanonicalExpectationRecordBytes(),
        },
        {
            .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata,
            .image = DatabaseSchemaWALStagedArtifactImage::After,
            .object = metadata.getObject(),
            .revision = metadata.getObjectSchemaRevision(),
            .canonical_bytes = metadata.getCanonicalMetadataBytes(),
        },
        {
            .kind = DatabaseSchemaWALStagedArtifactKind::PersistedTypeReferencesSidecar,
            .image = DatabaseSchemaWALStagedArtifactImage::After,
            .object = metadata.getObject(),
            .revision = metadata.getObjectSchemaRevision(),
            .canonical_bytes = publication_package.getCanonicalSidecarBytes(),
        },
        {
            .kind = DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadataInstallationRecord,
            .image = DatabaseSchemaWALStagedArtifactImage::After,
            .object = metadata.getObject(),
            .revision = metadata.getObjectSchemaRevision(),
            .canonical_bytes = publication_package.getCanonicalInstallationRecordBytes(),
        },
    };

    const AuthorityRoot & planning_root = publication_package.getPlanningRoot();
    DatabaseSchemaWALValidatedTransition transition = [&]
    {
        try
        {
            return DatabaseSchemaWALTransitionBuilder::build(
                transaction_id,
                {
                    .authority_state = publication_package.getBeforeAuthorityState(),
                    .authority_inventory = planning_root.pinAuthorityInventory(),
                    .schema_graph = planning_root.pinSchemaObjectDependencyGraph(),
                },
                publication_package.getAfterAuthorityState(),
                std::move(authority_deltas),
                std::move(dependent_deltas),
                publication_package.getSchemaGraphDelta(),
                std::move(staged_artifacts),
                limits);
        }
        catch (const DatabaseSchemaWALError & error)
        {
            if (error.code == DatabaseSchemaWALError::Code::InvalidConfiguration)
                fail(PackageError::Code::InvalidConfiguration, "stored-object publication WAL limits are invalid");
            if (error.code == DatabaseSchemaWALError::Code::LimitExceeded)
                fail(PackageError::Code::LimitExceeded, "stored-object publication WAL transition exceeds its limit");
            fail(PackageError::Code::InvalidTransition, "stored-object publication WAL transition is invalid");
        }
    }();

    if (transition.getPrepare().before_authority_state != std::optional{publication_package.getBeforeAuthorityState()}
        || transition.getPrepare().after_authority_state != publication_package.getAfterAuthorityState()
        || transition.getAfterInventory().getSummary() != publication_package.getAfterInventory().getSummary()
        || transition.getAfterGraph().computeRoot() != publication_package.getAfterSchemaGraph().computeRoot())
    {
        fail(PackageError::Code::InvalidTransition, "stored-object publication WAL transition differs from its replacement root");
    }
    return transition;
}

}

CommittedStoredObjectUDTPublication::CommittedStoredObjectUDTPublication(
    DatabaseSchemaWALCommit commit_,
    ValidatedStoredObjectUDTMetadata validated_metadata_,
    PersistedTypeReferences persisted_references_,
    BoundObjectTypeReferences::Ptr bound_references_,
    SidecarExpectationRecord expectation_record_,
    AuthorityVerificationStamp::Ptr verification_stamp_,
    StoredObjectUDTPublicationPackageStatistics package_statistics_,
    AtomicAuthorityPublicationStatistics authority_publication_statistics_) noexcept
    : commit(std::move(commit_))
    , validated_metadata(std::move(validated_metadata_))
    , persisted_references(std::move(persisted_references_))
    , bound_references(std::move(bound_references_))
    , expectation_record(std::move(expectation_record_))
    , verification_stamp(std::move(verification_stamp_))
    , package_statistics(package_statistics_)
    , authority_publication_statistics(authority_publication_statistics_)
{
}

PreparedStoredObjectUDTPublicationCommit::PreparedStoredObjectUDTPublicationCommit(
    UInt64 transaction_id_,
    DatabaseSchemaWALValidatedTransition recovery_transition_,
    PreparedDatabaseSchemaMutationExecution execution_,
    AtomicAuthority::PreparedPublication authority_publication_,
    StoredObjectUDTPublicationPackage publication_package_,
    BoundObjectTypeReferences::Ptr bound_references_,
    AuthorityVerificationStamp::Ptr verification_stamp_,
    AtomicAuthorityPublicationStatistics authority_publication_statistics_) noexcept
    : transaction_id(transaction_id_)
    , recovery_transition(std::move(recovery_transition_))
    , execution(std::move(execution_))
    , authority_publication(std::move(authority_publication_))
    , publication_package(std::move(publication_package_))
    , bound_references(std::move(bound_references_))
    , verification_stamp(std::move(verification_stamp_))
    , authority_publication_statistics(authority_publication_statistics_)
{
}

DurablyCommittedStoredObjectUDTPublication::DurablyCommittedStoredObjectUDTPublication(
    DatabaseSchemaWALCommit commit_,
    AtomicAuthority::PreparedPublication authority_publication_,
    StoredObjectUDTPublicationPackage publication_package_,
    BoundObjectTypeReferences::Ptr bound_references_,
    AuthorityVerificationStamp::Ptr verification_stamp_,
    AtomicAuthorityPublicationStatistics authority_publication_statistics_) noexcept
    : commit(std::move(commit_))
    , authority_publication(std::move(authority_publication_))
    , publication_package(std::move(publication_package_))
    , bound_references(std::move(bound_references_))
    , verification_stamp(std::move(verification_stamp_))
    , authority_publication_statistics(authority_publication_statistics_)
{
}

PreparedStoredObjectUDTPublicationCommit StoredObjectUDTPublicationCoordinator::prepareCreateCommit(
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
    const StoredObjectUDTPublicationCoordinatorLimits & limits)
{
    if (!planning_root)
        fail(PackageError::Code::InvalidBase, "stored-object publication coordinator has no pinned authority root");
    if (!transaction_id)
        fail(PackageError::Code::InvalidTransition, "stored-object publication coordinator transaction ID must be nonzero");
    {
        auto current_root = authority.acquireCurrentRoot();
        if (!current_root || current_root.operator->() != planning_root.operator->())
            fail(PackageError::Code::InvalidBase, "stored-object publication coordinator root belongs to another authority state");
    }
    if (mutation_guard.getState() != DatabaseSchemaMutationGuard::State::Ready)
        fail(PackageError::Code::InvalidTransition, "stored-object publication mutation guard is not ready");
    if (mutation_guard.getDatabaseUUID() != planning_root->getDatabaseUUID())
        fail(PackageError::Code::DatabaseMismatch, "stored-object publication mutation guard belongs to another database");

    auto validated_metadata = metadata_validator.validateAndCanonicalize(
        expected_expectation, candidate_metadata_bytes, canonical_sidecar_bytes, limits.metadata_validation);
    auto publication_package = StoredObjectUDTPublicationPackage::prepareCreate(
        std::move(planning_root),
        std::move(admission_proof),
        std::move(validated_metadata),
        std::move(canonical_sidecar_bytes),
        std::move(expected_expectation),
        object_dependencies,
        limits.publication_package);
    auto bound_references
        = bindExactReferences(publication_package, std::move(physical_schema), authority.getCapabilities(), limits.bound_references);
    AuthorityVerificationStamp::Ptr verification_stamp;
    try
    {
        verification_stamp = verifyAndCreateAuthorityVerificationStamp(
            publication_package.getReplacementRoot(),
            publication_package.getExpectationRecord(),
            publication_package.getCanonicalSidecarBytes(),
            *bound_references);
    }
    catch (const AuthorityVerificationStampError & error)
    {
        if (error.code == AuthorityVerificationStampError::Code::InvalidConfiguration)
            fail(PackageError::Code::InvalidConfiguration, "stored-object verification-stamp limits are invalid");
        if (error.code == AuthorityVerificationStampError::Code::LimitExceeded
            || error.code == AuthorityVerificationStampError::Code::ArithmeticOverflow)
            fail(PackageError::Code::LimitExceeded, "stored-object verification stamp exceeds its limit");
        fail(PackageError::Code::IntegrityMismatch, "stored object failed its mandatory post-DDL verification");
    }
    auto transition = buildTransition(transaction_id, publication_package, limits.schema_wal);

    std::vector<String> recovery_staged_artifact_bytes(
        transition.getStagedArtifactBytes().begin(), transition.getStagedArtifactBytes().end());
    auto recovery_transition = DatabaseSchemaWALTransitionBuilder::validateDecoded(
        transition.getPrepare(),
        {
            .authority_state = publication_package.getBeforeAuthorityState(),
            .authority_inventory = publication_package.getPlanningRoot().pinAuthorityInventory(),
            .schema_graph = publication_package.getPlanningRoot().pinSchemaObjectDependencyGraph(),
        },
        std::move(recovery_staged_artifact_bytes),
        limits.schema_wal);
    auto execution = prepareDatabaseSchemaMutationExecution(transition, limits.schema_wal);
    auto authority_publication = authority.preparePublication(publication_package.releaseReplacementRoot());
    const auto authority_publication_statistics = authority_publication.getStatistics();
    validatePreparedDatabaseSchemaMutationExecution(storage, mutation_guard, execution);

    return PreparedStoredObjectUDTPublicationCommit(
        transaction_id,
        std::move(recovery_transition),
        std::move(execution),
        std::move(authority_publication),
        std::move(publication_package),
        std::move(bound_references),
        std::move(verification_stamp),
        authority_publication_statistics);
}

DurablyCommittedStoredObjectUDTPublication StoredObjectUDTPublicationCoordinator::commitPreparedCreateDurably(
    IDatabaseSchemaMutationDurableStorage & storage,
    DatabaseSchemaMutationGuard & mutation_guard,
    PreparedStoredObjectUDTPublicationCommit && prepared)
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

    return DurablyCommittedStoredObjectUDTPublication(
        std::move(commit),
        std::move(prepared.authority_publication),
        std::move(prepared.publication_package),
        std::move(prepared.bound_references),
        std::move(prepared.verification_stamp),
        prepared.authority_publication_statistics);
}

std::optional<DurablyCommittedStoredObjectUDTPublication> StoredObjectUDTPublicationCoordinator::recoverPreparedCreateDurably(
    IDatabaseSchemaMutationDurableStorage & storage,
    DatabaseSchemaMutationGuard & mutation_guard,
    PreparedStoredObjectUDTPublicationCommit && prepared,
    const std::optional<DatabaseSchemaWALCommit> & commit)
{
    const auto decision = recoverDatabaseSchemaMutation(storage, mutation_guard, prepared.recovery_transition, commit);
    if (decision == DatabaseSchemaWALRecoveryDecision::RollBackPrepared)
    {
        retireRolledBackDatabaseSchemaMutation(storage, mutation_guard, prepared.transaction_id);
        return std::nullopt;
    }
    if (!commit)
        throw std::logic_error("committed stored-object publication recovery has no Commit marker");

    return DurablyCommittedStoredObjectUDTPublication(
        *commit,
        std::move(prepared.authority_publication),
        std::move(prepared.publication_package),
        std::move(prepared.bound_references),
        std::move(prepared.verification_stamp),
        prepared.authority_publication_statistics);
}

CommittedStoredObjectUDTPublication StoredObjectUDTPublicationCoordinator::publishDurablyCommittedCreate(
    AtomicAuthority & authority, DurablyCommittedStoredObjectUDTPublication committed) noexcept
{
    /// Move every value needed by the live object out before publication. The
    /// remaining package is destroyed on return, releasing its planning-root
    /// hazard instead of retaining one scarce hazard slot per stored object.
    CommittedStoredObjectUDTPublication result(
        std::move(committed.commit),
        std::move(committed.publication_package.validated_metadata),
        std::move(committed.publication_package.persisted_references),
        std::move(committed.bound_references),
        std::move(committed.publication_package.expectation_record),
        std::move(committed.verification_stamp),
        committed.publication_package.statistics,
        committed.authority_publication_statistics);
    authority.publish(std::move(committed.authority_publication));
    return result;
}

}
