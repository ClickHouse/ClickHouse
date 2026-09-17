#include <Databases/UDT/PhysicalizationApplyCoordinator.h>

#include <exception>
#include <utility>

namespace DB::UDT
{
namespace
{

using Error = PhysicalizationApplyCoordinatorError;

[[noreturn]] void fail(Error::Code code, std::string_view message)
{
    throw Error(code, message);
}

void discardIfUnprepared(IDatabaseSchemaMutationDurableStorage & storage, DatabaseSchemaMutationGuard & guard, UInt64 transaction_id)
{
    if (guard.getState() == DatabaseSchemaMutationGuard::State::Ready)
        discardUnpreparedDatabaseSchemaMutationStaging(storage, guard, transaction_id);
}

}

PhysicalizationApplyCoordinatorError::PhysicalizationApplyCoordinatorError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

DatabaseSchemaWALCommit PhysicalizationApplyCoordinator::apply(
    const AuthorityRoot & current_root,
    AtomicAuthority & authority,
    IDatabaseSchemaMutationDurableStorage & storage,
    DatabaseSchemaMutationGuard & mutation_guard,
    PhysicalizationTokenStore & token_store,
    std::string_view opaque_token,
    UUID authenticated_principal_uuid,
    UInt64 now_microseconds,
    UInt64 transaction_id,
    const IPhysicalizationObjectProvider & object_provider,
    const IPhysicalizationRewriteAdapter & rewrite_adapter,
    const IPhysicalizationApplyAuthorization & authorization,
    const PhysicalizationApplyLimits & limits,
    const PhysicalizationMonotonicClock & point_of_no_return_clock)
{
    if (transaction_id == 0 || authenticated_principal_uuid == UUIDHelpers::Nil || opaque_token.empty())
        fail(Error::Code::InvalidRequest, "physicalization apply identity is incomplete");
    if (current_root.getPersistentCapabilityMask() != dependent_object_authority_capability_mask)
        fail(Error::Code::InvalidRequest, "physicalization apply requires an exact dependent-object-capable authority root");

    authorization.checkCancellation();
    const auto inspected = token_store.inspectForApply(opaque_token, authenticated_principal_uuid, now_microseconds);
    const auto plan = PhysicalizationPlanner::build(current_root, inspected.getSelector(), object_provider, limits.plan);
    if (!point_of_no_return_clock)
    {
        for (const auto & object : plan.getObjects())
        {
            if (object.object.kind != SchemaObjectKind::SyntheticTestObject)
                fail(Error::Code::InvalidRequest, "production physicalization apply requires a point-of-no-return monotonic clock");
        }
    }
    if (!inspected.matches(plan))
    {
        token_store.discard(opaque_token, authenticated_principal_uuid);
        PhysicalizationTokenRouter::unregisterToken(opaque_token, current_root.getDatabaseUUID());
        fail(Error::Code::StaleToken, "physicalization apply token does not match the freshly recomputed plan");
    }

    for (const auto & object : plan.getObjects())
    {
        authorization.checkCancellation();
        authorization.requireObjectRewrite(object);
    }
    for (const auto & definition : plan.getDefinitions())
    {
        authorization.checkCancellation();
        if (definition.selected_for_drop)
            authorization.requireDefinitionDrop(definition);
    }

    auto rewrite_images = rewrite_adapter.prepareRewriteImages(plan);
    authorization.checkCancellation();
    auto prepared = PhysicalizationMutationPlanner::plan(
        current_root, plan, transaction_id, current_root.getDatabaseCatalogEpoch(), rewrite_images, limits.mutation);

    auto execution = prepareDatabaseSchemaMutationExecution(prepared.getValidatedTransition(), limits.mutation.schema_wal);
    auto publication = authority.preparePublication(prepared.releaseReplacementRoot());
    validatePreparedDatabaseSchemaMutationExecution(storage, mutation_guard, execution);
    /// Token consumption is the point of no return. From here through durable
    /// Commit and the two runtime publications the operation is deliberately
    /// non-cancellable so a killed query cannot strand a half-published root.
    authorization.checkCancellation();
    const auto consumed = point_of_no_return_clock
        ? token_store.consumeForApply(opaque_token, authenticated_principal_uuid, inspected.getOperationID(), point_of_no_return_clock)
        : token_store.consumeForApply(opaque_token, authenticated_principal_uuid, inspected.getOperationID(), now_microseconds);
    if (!consumed.matches(plan) || consumed.getOperationID() != inspected.getOperationID())
        std::terminate();
    PhysicalizationTokenRouter::unregisterToken(opaque_token, current_root.getDatabaseUUID());

    DatabaseSchemaWALCommit commit;
    try
    {
        commit = executePreparedDatabaseSchemaMutation(storage, mutation_guard, std::move(execution));
    }
    catch (const DatabaseSchemaMutationIndeterminateDurabilityError & error)
    {
        const auto original = std::current_exception();
        const auto recovered_commit
            = rewrite_adapter.recoverIndeterminateRewrite(storage, mutation_guard, prepared.getValidatedTransition(), error);
        if (!recovered_commit)
            std::rethrow_exception(original);
        commit = *recovered_commit;
    }
    catch (...)
    {
        const auto original = std::current_exception();
        discardIfUnprepared(storage, mutation_guard, transaction_id);
        std::rethrow_exception(original);
    }

    rewrite_adapter.publishCommittedRewrite();
    authority.publish(std::move(publication));
    return commit;
}

DatabaseSchemaWALCommit PhysicalizationApplyCoordinator::activateDependentObjectAuthority(
    AtomicAuthority & authority,
    IDatabaseSchemaMutationDurableStorage & storage,
    DatabaseSchemaMutationGuard & mutation_guard,
    PreparedDependentObjectActivation prepared,
    const DependentObjectActivationPlannerLimits & limits)
{
    const UInt64 transaction_id = prepared.getValidatedTransition().getPrepare().transaction_id;
    auto execution = prepareDatabaseSchemaMutationExecution(prepared.getValidatedTransition(), limits.schema_wal);
    auto publication = authority.preparePublication(prepared.releaseReplacementRoot());
    validatePreparedDatabaseSchemaMutationExecution(storage, mutation_guard, execution);

    DatabaseSchemaWALCommit commit;
    try
    {
        commit = executePreparedDatabaseSchemaMutation(storage, mutation_guard, std::move(execution));
    }
    catch (...)
    {
        const auto original = std::current_exception();
        discardIfUnprepared(storage, mutation_guard, transaction_id);
        std::rethrow_exception(original);
    }

    authority.publish(std::move(publication));
    return commit;
}

}
