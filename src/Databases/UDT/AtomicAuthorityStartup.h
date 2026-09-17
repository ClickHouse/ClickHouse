#pragma once

#include <Databases/UDT/AtomicAuthorityStartupStatus.h>
#include <Databases/UDT/AtomicDatabaseSchemaMutationStorage.h>
#include <Databases/UDT/AuthorityRecovery.h>

#include <functional>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB::UDT
{

struct AtomicAuthorityStartupLimits
{
    DatabaseSchemaWALLimits wal;
    AuthorityRecoveryLimits recovery;
};

class AtomicAuthorityStartupError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidDurableSequence,
        AuthorityStateMismatch,
        IncompleteRecovery,
    };

    AtomicAuthorityStartupError(Code code_, std::string_view message);

    const Code code;
};

struct AtomicAuthorityPendingTable
{
    SidecarExpectationRecord expectation;
    String object_name;
    String canonical_metadata_bytes;
    String canonical_sidecar_bytes;
};

/// Exact terminal mapped-table DROP whose tombstone ownership must be checked
/// after the server-wide dropped-table scan. `tombstone_replayed` distinguishes
/// a tombstone recreated in this recovery pass from one that may already have
/// been consumed by completed background cleanup.
struct AtomicAuthorityRecoveredDroppedTable
{
    UUID table_uuid = UUIDHelpers::Nil;
    String table_name;
    bool tombstone_replayed = false;
};

/// Result of the complete durable recovery boundary. A null root means the
/// backend proved and restored the exact never-enabled layout. Pending IStorage
/// objects remain in canonical reconciliation order and are bounded by the
/// configured recovery expectation-record limit. The legacy member/type name
/// is retained to avoid widening the startup API mechanically.
struct AtomicAuthorityStartupResult
{
    AuthorityRoot::Ptr authority_root;
    /// Present only when durable authority bytes were found but no root was
    /// safe to activate. The owning database may expose this immutable image
    /// for diagnostics, but must not use it for resolution or mapped objects.
    AtomicAuthorityStartupStatusSnapshot::Ptr degraded_status;
    std::vector<AtomicAuthorityPendingTable> pending_tables;
    std::vector<AtomicAuthorityRecoveredDroppedTable> recovered_dropped_tables;
    UInt64 completed_transactions = 0;
    UInt64 rolled_back_transactions = 0;
    UInt64 swept_unprepared_transactions = 0;
    UInt64 swept_retired_transactions = 0;
};

/// Shared typed boundary for startup callers that must distinguish durable
/// corruption from configuration, platform, control-flow, and injected
/// failures. Only the former may be projected as an unavailable INCOMPLETE
/// authority instead of aborting startup.
[[nodiscard]] bool isDegradableAtomicAuthorityStartupStorageError(AtomicDatabaseSchemaMutationStorageError::Code code) noexcept;

/// Payload-free database-wide fallback used when durable preflight cannot
/// recover an exact inventory/graph scope. It is diagnostic-only and never an
/// executable authority root.
[[nodiscard]] AtomicAuthorityStartupStatusSnapshot::Ptr
makeGlobalIncompleteAtomicAuthorityStartupStatus(UUID database_uuid, String stable_error);

enum class AtomicAuthorityDependentObjectValidationState : UInt8
{
    Validated,
    PendingTable,
};

struct AtomicAuthorityValidatedDependentObject
{
    SchemaObjectID object;
    UInt64 object_schema_revision = 0;
    Digest physical_schema_fingerprint{};
    AtomicAuthorityDependentObjectValidationState state = AtomicAuthorityDependentObjectValidationState::Validated;
};

using AtomicAuthorityDependentObjectValidator = std::function<AtomicAuthorityValidatedDependentObject(
    const SidecarExpectationRecord &, std::string_view canonical_metadata_bytes, std::string_view canonical_sidecar_bytes)>;

/// Recovers checkpoint plus the strictly ordered live WAL tail. Committed
/// transitions are validated without reinstalling obsolete images. The sole
/// exception is an exact terminal mapped-table DROP without a recovery marker:
/// its committed After image is replayed idempotently so a crash between Commit
/// and tombstone publication cannot lose cleanup ownership. A terminal
/// unresolved Prepare is physically rolled back.
/// Reconciliation then requires a registered kind-specific metadata/sidecar
/// validator for every dependent expectation before reconstructing one
/// immutable root. A validator may defer Table/View/Dictionary binding; the
/// exact reconciled IStorage image is then returned in `pending_tables`.
/// The caller must serialize this function with every database schema mutation.
[[nodiscard]] AtomicAuthorityStartupResult recoverAtomicAuthorityAtStartup(
    AtomicDatabaseSchemaMutationStorage & storage,
    const AtomicAuthorityStartupLimits & limits = {},
    const AtomicAuthorityDependentObjectValidator & dependent_object_validator = {});

/// Production Atomic startup boundary. SyntheticTestObject is validated
/// immediately; mapped Table/View/Dictionary objects are returned as pending
/// for later IStorage binding; all other dependent kinds remain fail-closed. The function then
/// durably upgrades an exact definition-only root with the content-neutral capability
/// transaction. An already-dependent-object-capable root is returned without another mutation.
[[nodiscard]] AtomicAuthorityStartupResult
recoverAndActivateAtomicAuthorityAtStartup(AtomicDatabaseSchemaMutationStorage & storage, const AtomicAuthorityStartupLimits & limits = {});
}
