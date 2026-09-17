#pragma once

#include <DataTypes/UDT/IAuthorityAdapter.h>
#include <DataTypes/UDT/Record.h>

#include <Databases/UDT/AtomicAuthorityStartupStatus.h>

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
    virtual std::unique_ptr<const ILifecycleSnapshot> acquireSnapshot() const = 0;

    virtual void createOrAttach(const ASTCreateTypeQuery & query, const LifecycleActor & actor) = 0;
    virtual void rename(const ASTRenameTypeQuery & query, const LifecycleActor & actor) = 0;
    virtual void comment(const ASTAlterTypeCommentQuery & query, const LifecycleActor & actor) = 0;
    virtual void dropRestrict(const ASTDropTypeQuery & query, const LifecycleActor & actor) = 0;
};

ILifecycleAdapter & getUnsupportedLifecycleAdapter() noexcept;

}
