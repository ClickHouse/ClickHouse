#pragma once

#include <DataTypes/UDT/IAuthorityAdapter.h>
#include <DataTypes/UDT/Record.h>

#include <Databases/UDT/AtomicAuthorityStartupStatus.h>
#include <Databases/UDT/PhysicalizationPlan.h>

#include <Core/Types.h>
#include <Core/UUID.h>
#include <Storages/IStorage_fwd.h>

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string_view>

namespace DB
{
class ASTCreateTypeQuery;
class ASTDropTypeQuery;
class ASTAlterTypeCommentQuery;
class ASTRenameTypeQuery;
}

namespace DB::UDT
{

struct MonomorphicProjection
{
    String canonical_physical_type;
    Digest storage_fingerprint{};

    bool operator==(const MonomorphicProjection &) const = default;
};

/// Principal data captured once at the lifecycle operation boundary. The
/// durable adapter chooses the commit timestamp; callers cannot supply one.
struct LifecycleActor
{
    UUID principal_uuid = UUIDHelpers::Nil;
    String principal_display_name;
    bool internal_query = false;
};

class IPhysicalizationApplyAuthorization;

class IPhysicalizationDryRunAuthorization
{
public:
    virtual ~IPhysicalizationDryRunAuthorization() = default;

    /// Cooperative query-cancellation checkpoint. Implementations without a
    /// query lifetime may retain the default no-op behavior.
    virtual void checkCancellation() const { }

    /// Coarse database privilege checked before selecting a closure or
    /// decoding authority records.
    virtual void requireDatabaseVisibility() const { }
    /// Exact selected identity/name privilege checked before durable metadata
    /// or sidecar reconciliation.
    virtual void requireObjectIdentityVisibility(const SchemaObjectID &, std::string_view) const { }
    /// Database-wide visibility required before exposing catalog-integrity
    /// diagnostics for an object whose exact current name cannot be trusted.
    virtual void requireDatabaseObjectDiagnosticsVisibility() const { }
    /// Database-wide type visibility required before reconciling a
    /// definition-only DROP UNUSED plan whose exact manifest is not built yet.
    virtual void requireDatabaseDefinitionVisibility() const { }

    /// Dry run must not disclose a partial loss/validation closure. Every
    /// selected object and every definition retained in the manifest is
    /// checked before a token is issued.
    virtual void requireObjectVisibility(const PhysicalizationManifestObject & object) const = 0;
    virtual void requireDefinitionVisibility(const PhysicalizationManifestDefinition & definition) const = 0;
};

struct PhysicalizationDryRunResult
{
    String opaque_token;
    PhysicalizationPlan plan;
};

/// One immutable, backend-owned view used by SHOW/DESCRIBE. A durable Atomic
/// implementation keeps its composite-root hazard for this object's lifetime;
/// no record span may outlive the snapshot.
class ILifecycleSnapshot
{
public:
    virtual ~ILifecycleSnapshot() = default;

    virtual UUID getDatabaseUUID() const noexcept = 0;
    virtual UInt64 getDatabaseCatalogEpoch() const noexcept = 0;
    virtual std::span<const Record> getDefinitionRecords() const noexcept = 0;
    virtual const Record * findDefinitionRecordByLocalName(std::string_view normalized_local_name) const noexcept = 0;
    virtual Definition::Ptr findCheckedDefinitionByIdentity(const DefinitionIdentity &) const noexcept { return {}; }

    /// Status is derived from the same immutable authority/runtime view as the
    /// record. A degraded startup has no executable records and exposes only
    /// unavailable diagnostics; callers must never treat those rows as a
    /// resolution catalog.
    virtual AuthorityDefinitionStatus getDefinitionStatus(const DefinitionIdentity &) const noexcept
    {
        return AuthorityDefinitionStatus::Active;
    }
    virtual std::string_view getDefinitionLastError(const DefinitionIdentity &) const noexcept { return {}; }
    virtual std::span<const AtomicAuthorityStartupDefinitionDiagnostic> getUnavailableDefinitionDiagnostics() const noexcept { return {}; }

    /// Exact dependent-object and resolution views owned by this same
    /// immutable snapshot. Introspection must not reopen the database's live
    /// authority after it has pinned lifecycle names/records: physicalization
    /// can publish a newer root in between and make old mapped metadata appear
    /// to belong to that newer authority value.
    virtual const SidecarExpectationRecord * findSidecarExpectation(const SchemaObjectID &) const noexcept { return nullptr; }
    virtual const IAuthorityAdapter * getResolutionAuthorityAdapter() const noexcept { return nullptr; }

    /// Derived against this snapshot's exact immutable authority value.
    /// Parameterized definitions have no single physical projection.
    virtual std::optional<MonomorphicProjection> getMonomorphicProjection(const DefinitionIdentity &) const { return std::nullopt; }
};

/// Database-owned lifecycle boundary. It is deliberately separate from the
/// read-hot resolution adapter: implementations serialize the whole
/// no-op/recheck/plan/durable-commit/publication sequence inside each method,
/// including the physicalization provenance-erasure path. The process-stable default
/// implementation advertises no capabilities and rejects before storage or
/// catalog mutation.
class ILifecycleAdapter
{
public:
    virtual ~ILifecycleAdapter() = default;

    virtual const TypeAuthorityCapabilities & getCapabilities() const noexcept = 0;
    virtual UUID getDatabaseUUID() const noexcept = 0;
    virtual void requireCapabilities(TypeAuthorityCapabilityMask required, std::string_view operation) const = 0;

    /// Serializes a table-metadata read with dependent-object authority
    /// publication. Callers retain the table share lock while acquiring this
    /// lease. Proven physical tables take only the database schema mutex and
    /// may expose their last published image while an ordinary ALTER computes
    /// its successor. Mapped tables retain share -> ALTER -> database-schema;
    /// a physical-to-mapped gap detected by database-owned UUID state releases
    /// the schema mutex before switching to that full order. Waiting is bounded
    /// by the caller's ordinary query lock timeout and periodically invokes
    /// check_cancellation. Backends without a durable dependent-object
    /// authority need no lease.
    virtual std::shared_ptr<void>
    acquireTableIntrospectionLease(const StoragePtr &, std::chrono::milliseconds, std::function<void()> check_cancellation) const
    {
        if (check_cancellation)
            check_cancellation();
        return {};
    }
    virtual std::unique_ptr<const ILifecycleSnapshot> acquireSnapshot() const = 0;

    virtual void createOrAttach(const ASTCreateTypeQuery & query, const LifecycleActor & actor) = 0;
    virtual void rename(const ASTRenameTypeQuery & query, const LifecycleActor & actor) = 0;
    virtual void comment(const ASTAlterTypeCommentQuery & query, const LifecycleActor & actor) = 0;
    virtual void dropRestrict(const ASTDropTypeQuery & query, const LifecycleActor & actor) = 0;

    virtual PhysicalizationDryRunResult physicalizationDryRun(
        PhysicalizationSelector selector, const LifecycleActor & actor, const IPhysicalizationDryRunAuthorization & authorization) = 0;
    virtual void physicalizationApply(
        std::string_view opaque_token, const LifecycleActor & actor, const IPhysicalizationApplyAuthorization & authorization) = 0;
    /// Best-effort cleanup when a dry-run result cannot be delivered to its
    /// caller. Tokens are process-local, so no durable work is required.
    virtual void discardPhysicalizationToken(std::string_view, const LifecycleActor &) noexcept { }
};

ILifecycleAdapter & getUnsupportedLifecycleAdapter() noexcept;

}
