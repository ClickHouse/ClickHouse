#pragma once

#include <Databases/DatabaseSchemaMutationTransaction.h>
#include <Databases/UDT/AtomicAuthority.h>
#include <Databases/UDT/DependentObjectActivationPlanner.h>
#include <Databases/UDT/PhysicalizationMutationPlanner.h>
#include <Databases/UDT/PhysicalizationTokenStore.h>

#include <Core/Types.h>

#include <chrono>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB::UDT
{

class IPhysicalizationApplyAuthorization
{
public:
    virtual ~IPhysicalizationApplyAuthorization() = default;

    /// The coordinator treats token consumption as the point of no return.
    /// This hook is checked throughout preparation and once immediately
    /// before consume; the post-consume durable section is non-cancellable.
    virtual void checkCancellation() const { }

    /// One query-wide deadline is used while acquiring the selected tables'
    /// ALTER locks before the database schema-mutation mutex.
    virtual std::chrono::milliseconds getTableAlterLockAcquireTimeout() const { return std::chrono::milliseconds::zero(); }

    /// Preflight forms used before hidden durable records are reconciled.
    virtual void requireObjectRewriteIdentity(const SchemaObjectID &, std::string_view) const { }
    /// Database-wide rewrite privilege required before exposing catalog-
    /// integrity diagnostics for an object whose exact name cannot be trusted.
    virtual void requireDatabaseObjectRewriteDiagnostics() const { }
    /// Database-wide DROP TYPE privilege required before reconciling a
    /// definition-only plan whose exact removal manifest is not built yet.
    virtual void requireDatabaseDefinitionDrop() const { }

    /// Rechecked for every object in the freshly recomputed rewrite scope.
    virtual void requireObjectRewrite(const PhysicalizationManifestObject & object) const = 0;
    /// Rechecked only for definitions the fresh plan will actually remove.
    virtual void requireDefinitionDrop(const PhysicalizationManifestDefinition & definition) const = 0;
};

class IPhysicalizationRewriteAdapter
{
public:
    virtual ~IPhysicalizationRewriteAdapter() = default;

    /// Produces every canonical before/after metadata image without I/O while
    /// the caller still pins the exact authority root used by `plan`.
    virtual std::vector<PhysicalizationRewriteImage> prepareRewriteImages(const PhysicalizationPlan & plan) const = 0;

    /// Publishes the already prepared physical-only runtime images after the
    /// durable transaction commits but before the replacement authority root
    /// becomes visible. Implementations must not fail: after Commit there is
    /// no rollback-safe state to return to.
    virtual void publishCommittedRewrite() const noexcept { }

    /// Resolves an indeterminate post-consume durability result against the
    /// exact retained transition. A returned Commit means the After image is
    /// durably selected; nullopt means no runtime After publication is safe
    /// and the original execution error must be returned. Production
    /// implementations must either restore Before or retain a fail-stop latch
    /// when exact online recovery cannot be proved.
    virtual std::optional<DatabaseSchemaWALCommit> recoverIndeterminateRewrite(
        IDatabaseSchemaMutationDurableStorage &,
        DatabaseSchemaMutationGuard &,
        const DatabaseSchemaWALValidatedTransition &,
        const DatabaseSchemaMutationIndeterminateDurabilityError &) const noexcept
    {
        return std::nullopt;
    }
};

struct PhysicalizationApplyLimits
{
    PhysicalizationPlanLimits plan;
    PhysicalizationMutationPlannerLimits mutation;
};

class PhysicalizationApplyCoordinatorError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidRequest,
        StaleToken,
    };

    PhysicalizationApplyCoordinatorError(Code code_, std::string_view message);

    const Code code;
};

/// Internal dependent-object-capable execution boundary. The caller owns the database schema
/// mutation guard and pins `current_root` for the complete call. All planning,
/// authorization, rewrite construction and publication preparation finish
/// before the token is consumed and before the WAL transaction starts.
class PhysicalizationApplyCoordinator final
{
public:
    [[nodiscard]] static DatabaseSchemaWALCommit apply(
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
        const PhysicalizationApplyLimits & limits = {},
        /// Empty is retained only for deterministic SyntheticTestObject unit
        /// adapters; production object kinds fail closed without a fresh clock.
        const PhysicalizationMonotonicClock & point_of_no_return_clock = {});

    /// Executes the content-neutral capability transition through the same
    /// durable transaction/publication order used by definition and object
    /// mutations. The prepared value must have been built from the currently
    /// pinned definition-only root while the same schema guard is held.
    [[nodiscard]] static DatabaseSchemaWALCommit activateDependentObjectAuthority(
        AtomicAuthority & authority,
        IDatabaseSchemaMutationDurableStorage & storage,
        DatabaseSchemaMutationGuard & mutation_guard,
        PreparedDependentObjectActivation prepared,
        const DependentObjectActivationPlannerLimits & limits = {});

private:
    PhysicalizationApplyCoordinator() = delete;
};

}
