#pragma once

#include <Databases/UDT/ILifecycleAdapter.h>

namespace DB
{
class ASTCreateQuery;
class DatabaseAtomic;
}

namespace DB::UDT
{

class AtomicAuthority;
class AtomicDatabaseSchemaMutationStorage;
class AuthorityRoot;
class BoundObjectTypeReferences;
class PhysicalizationTokenStore;
class EffectiveResourceLimits;
struct PersistedTypeReferences;
struct DefinitionMutationRequest;

/// DatabaseAtomic-owned lifecycle boundary. The object itself is in-memory
/// only; durable authority storage remains absent until a material mutation.
class AtomicLifecycleAdapter final : public ILifecycleAdapter
{
public:
    explicit AtomicLifecycleAdapter(DatabaseAtomic & database_);
    ~AtomicLifecycleAdapter() override;

    const TypeAuthorityCapabilities & getCapabilities() const noexcept override;
    UUID getDatabaseUUID() const noexcept override;
    void requireCapabilities(TypeAuthorityCapabilityMask required, std::string_view operation) const override;

    std::shared_ptr<void> acquireTableIntrospectionLease(
        const StoragePtr & table, std::chrono::milliseconds timeout, std::function<void()> check_cancellation) const override;
    std::unique_ptr<const ILifecycleSnapshot> acquireSnapshot() const override;

    void createOrAttach(const ASTCreateTypeQuery & query, const LifecycleActor & actor) override;
    void rename(const ASTRenameTypeQuery & query, const LifecycleActor & actor) override;
    void comment(const ASTAlterTypeCommentQuery & query, const LifecycleActor & actor) override;
    void dropRestrict(const ASTDropTypeQuery & query, const LifecycleActor & actor) override;
    PhysicalizationDryRunResult physicalizationDryRun(
        PhysicalizationSelector selector, const LifecycleActor & actor, const IPhysicalizationDryRunAuthorization & authorization) override;
    void physicalizationApply(
        std::string_view opaque_token, const LifecycleActor & actor, const IPhysicalizationApplyAuthorization & authorization) override;
    void discardPhysicalizationToken(std::string_view opaque_token, const LifecycleActor & actor) noexcept override;

private:
    friend class DB::DatabaseAtomic;

    /// DatabaseAtomic calls this after initial config resolution and again,
    /// before publication/query admission, after active-startup durable policy
    /// reconciliation. No token can exist at either boundary.
    void configureEffectiveDatabaseResourceLimitsForStartup(const EffectiveResourceLimits & effective_limits);
    AtomicAuthority * executeMutationLocked(const AuthorityRoot * current_root, DefinitionMutationRequest request);

    DatabaseAtomic & database;
    std::unique_ptr<PhysicalizationTokenStore> physicalization_tokens;
};

}
