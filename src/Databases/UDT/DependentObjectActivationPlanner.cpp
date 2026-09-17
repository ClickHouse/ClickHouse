#include <Databases/UDT/DependentObjectActivationPlanner.h>

#include <DataTypes/UDT/AuthorityState.h>

namespace DB::UDT
{
namespace
{

using Error = DependentObjectActivationPlannerError;

[[noreturn]] void fail(Error::Code code, std::string_view message)
{
    throw Error(code, message);
}

}

DependentObjectActivationPlannerError::DependentObjectActivationPlannerError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

PreparedDependentObjectActivation::PreparedDependentObjectActivation(
    AuthorityRoot::Ptr replacement_root_, DatabaseSchemaWALValidatedTransition transition_)
    : replacement_root(std::move(replacement_root_))
    , transition(std::move(transition_))
{
}

PreparedDependentObjectActivation DependentObjectActivationPlanner::plan(
    const AuthorityRoot & current_root,
    UInt64 transaction_id,
    std::optional<UInt64> expected_database_catalog_epoch,
    const DependentObjectActivationPlannerLimits & limits)
{
    if (transaction_id == 0)
        fail(Error::Code::InvalidRequest, "dependent-object-capable activation transaction ID must be nonzero");
    if (expected_database_catalog_epoch && *expected_database_catalog_epoch != current_root.getDatabaseCatalogEpoch())
        fail(Error::Code::ExpectedEpochMismatch, "dependent-object-capable activation expected catalog epoch is stale");
    if (current_root.getPersistentCapabilityMask() != definition_authority_capability_mask)
        fail(Error::Code::CapabilityMismatch, "dependent-object-capable activation requires an exact definition-only authority root");

    DatabaseSchemaWALValidatedTransition transition = [&]
    {
        try
        {
            return DatabaseSchemaWALTransitionBuilder::buildDependentObjectActivation(transaction_id, current_root, limits.schema_wal);
        }
        catch (const DatabaseSchemaWALError & error)
        {
            if (error.code == DatabaseSchemaWALError::Code::LimitExceeded)
                fail(Error::Code::LimitExceeded, "dependent-object-capable activation WAL transition exceeds its limit");
            fail(Error::Code::InvalidTransition, "dependent-object-capable activation WAL transition is invalid");
        }
    }();

    AuthorityRoot::Ptr replacement;
    try
    {
        replacement
            = current_root.cloneWithAuthorityState(transition.getPrepare().after_authority_state, limits.schema_wal.authority_state);
    }
    catch (const AuthorityRootError & error)
    {
        if (error.code == AuthorityRootError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, error.what());
        fail(Error::Code::InvalidTransition, "dependent-object-capable activation replacement root is invalid");
    }

    if (!current_root.sharesContentPayloadWith(*replacement)
        || replacement->getTypeIndexGeneration() != current_root.getTypeIndexGeneration()
        || replacement->getTypeIndexContentDigest() != current_root.getTypeIndexContentDigest())
    {
        fail(Error::Code::InvalidTransition, "dependent-object-capable activation changed the immutable authority content payload");
    }

    return PreparedDependentObjectActivation(std::move(replacement), std::move(transition));
}

}
