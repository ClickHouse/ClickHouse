#pragma once

#include <DataTypes/UDT/AuthorityInventory.h>
#include <DataTypes/UDT/Record.h>

#include <Core/Types.h>
#include <Core/UUID.h>

#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace DB::UDT
{

class AuthorityRoot;

/// Stable public values used by system.user_defined_types. Do not persist the
/// compiler enum representation; durable authority state remains the canonical
/// records/inventory/WAL rather than this process-local diagnostic snapshot.
enum class AuthorityDefinitionStatus : UInt8
{
    Active = 1,
    Conflicted = 2,
    Invalid = 3,
    Incomplete = 4,
    Quarantined = 5,
    OverQuota = 6,
};

inline constexpr UInt64 atomic_authority_startup_maximum_last_error_bytes = 1ULL << 10;

[[nodiscard]] std::string_view getAuthorityDefinitionStatusName(AuthorityDefinitionStatus status) noexcept;

/// One fail-closed startup row. `record` is retained only when its standalone
/// V1 envelope, identity and anchored leaf hash were decoded exactly. A row
/// without a record still exposes its durable UUID/revision without inventing
/// a name or executable definition.
struct AtomicAuthorityStartupDefinitionDiagnostic
{
    AuthorityInventoryKey key;
    UInt64 revision = 0;
    std::optional<Record> record;
    AuthorityDefinitionStatus status = AuthorityDefinitionStatus::Incomplete;
    String last_error;
};

/// Trusted metadata identity decoded from the exact reconciled installation
/// image. Names are canonical object names, not filenames or display strings.
struct AtomicAuthorityStartupDependentObjectIdentity
{
    UUID object_uuid = UUIDHelpers::Nil;
    String object_name;
};

/// Whether startup recovered the complete anchored set of dependent-object
/// identities.  Exact permits object-local isolation; Unknown requires the
/// owning database to skip every persistent storage-object metadata file
/// before parsing because no canonical name can be proved unrelated to the
/// unavailable authority.
enum class AtomicAuthorityStartupDependentObjectScope : UInt8
{
    Exact = 1,
    Unknown = 2,
};

/// Immutable database-local diagnostic image used only when no AuthorityRoot
/// was safe to activate. It never supplies definitions to resolution. The
/// Exact expectation UUID/name mappings come only from fully reconciled
/// installation images. They let metadata loading skip mapped files before
/// parsing while unrelated physical tables remain loadable.
class AtomicAuthorityStartupStatusSnapshot final
{
public:
    using Ptr = std::shared_ptr<const AtomicAuthorityStartupStatusSnapshot>;

    [[nodiscard]] static Ptr create(
        UUID database_uuid,
        std::vector<AtomicAuthorityStartupDefinitionDiagnostic> diagnostics,
        std::vector<AtomicAuthorityStartupDependentObjectIdentity> expected_dependent_objects,
        String global_last_error,
        AtomicAuthorityStartupDependentObjectScope dependent_object_scope = AtomicAuthorityStartupDependentObjectScope::Exact);

    /// Precomputes the allocation-owning diagnostic image while a recovered
    /// root is still private. A later deterministic mapped-object startup
    /// failure can then switch to fail-closed degraded mode without allocating
    /// or attempting to reconstruct identities from the filesystem.
    [[nodiscard]] static Ptr createForUnavailableRoot(
        const AuthorityRoot & root,
        std::span<const AtomicAuthorityStartupDependentObjectIdentity> expected_dependent_objects,
        String stable_error);

    UUID getDatabaseUUID() const noexcept { return database_uuid; }
    std::span<const AtomicAuthorityStartupDefinitionDiagnostic> getDefinitionDiagnostics() const noexcept { return diagnostics; }
    bool containsExpectedDependentObject(UUID object_uuid) const noexcept;
    const AtomicAuthorityStartupDependentObjectIdentity * findExpectedDependentObject(UUID object_uuid) const noexcept;
    const AtomicAuthorityStartupDependentObjectIdentity * findExpectedDependentObject(std::string_view object_name) const noexcept;
    bool hasUnknownDependentObjectScope() const noexcept
    {
        return dependent_object_scope == AtomicAuthorityStartupDependentObjectScope::Unknown;
    }
    AuthorityDefinitionStatus getGlobalStatus() const noexcept { return global_status; }
    std::string_view getGlobalLastError() const noexcept { return global_last_error; }

private:
    AtomicAuthorityStartupStatusSnapshot(
        UUID database_uuid_,
        std::vector<AtomicAuthorityStartupDefinitionDiagnostic> diagnostics_,
        std::vector<AtomicAuthorityStartupDependentObjectIdentity> expected_dependent_objects_,
        std::vector<size_t> expected_dependent_objects_by_name_,
        AtomicAuthorityStartupDependentObjectScope dependent_object_scope_,
        AuthorityDefinitionStatus global_status_,
        String global_last_error_) noexcept;

    const UUID database_uuid;
    const std::vector<AtomicAuthorityStartupDefinitionDiagnostic> diagnostics;
    const std::vector<AtomicAuthorityStartupDependentObjectIdentity> expected_dependent_objects;
    const std::vector<size_t> expected_dependent_objects_by_name;
    const AtomicAuthorityStartupDependentObjectScope dependent_object_scope;
    const AuthorityDefinitionStatus global_status;
    const String global_last_error;
};

}
