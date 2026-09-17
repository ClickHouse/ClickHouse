#pragma once

#include <DataTypes/UDT/AuthorityState.h>
#include <DataTypes/UDT/PersistedTypeReferences.h>
#include <DataTypes/UDT/Record.h>
#include <DataTypes/UDT/SidecarExpectationRecord.h>

#include <Core/Types.h>

#include <span>
#include <stdexcept>
#include <string_view>

namespace DB::UDT
{

class AuthorityRoot;

/// Limits one exact object verification. Canonical-byte and work budgets are
/// totals; transient bytes bound canonical payloads and retained element
/// charges prospectively, excluding allocator slack covered by MemoryTracker.
struct AuthorityIntegrityVerifierLimits
{
    AuthorityStateLimits authority_state;
    PersistedTypeReferencesLimits persisted_references;
    RecordLimits definition_record;
    UInt64 maximum_required_definitions = 65'536;
    UInt64 maximum_outgoing_dependencies = 65'536;
    UInt64 maximum_canonical_bytes_hashed = 256ULL << 20;
    UInt64 maximum_work_units = 8'388'608;
    UInt64 maximum_transient_bytes = 64ULL << 20;
};

struct AuthorityIntegrityVerificationStatistics
{
    UInt64 inventory_lookups = 0;
    UInt64 descriptors_verified = 0;
    UInt64 definition_records_verified = 0;
    UInt64 graph_edges_inspected = 0;
    UInt64 canonical_bytes_hashed = 0;
    UInt64 work_units = 0;
    UInt64 peak_transient_bytes = 0;

    bool operator==(const AuthorityIntegrityVerificationStatistics &) const = default;
};

/// Compact immutable proof identity. It retains no authority root, record,
/// sidecar, schema object, or definition handle.
struct VerifiedAuthorityObjectIntegrity
{
    UUID database_uuid = UUIDHelpers::Nil;
    UInt64 database_catalog_epoch = 0;
    Digest authority_anchor{};
    SchemaObjectID object;
    UInt64 object_schema_revision = 0;
    Digest sidecar_hash{};
    Digest physical_schema_fingerprint{};
    Digest required_definitions_digest{};
    UInt64 required_definition_count = 0;
    AuthorityIntegrityVerificationStatistics statistics;

    bool operator==(const VerifiedAuthorityObjectIntegrity &) const = default;
};

class AuthorityIntegrityVerifierError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        LimitExceeded,
        InvalidAuthorityState,
        InventoryMismatch,
        ExpectationMismatch,
        SidecarMismatch,
        DefinitionMismatch,
        GraphMismatch,
    };

    AuthorityIntegrityVerifierError(Code code_, std::string_view message, AuthorityIntegrityVerificationStatistics statistics_ = {});

    const Code code;
    /// Deterministic charge accumulated before a data or limit failure. An
    /// invalid configuration is rejected before execution and carries zeros.
    const AuthorityIntegrityVerificationStatistics statistics;
};

/// Rejects an invalid execution configuration before any target I/O or work
/// is admitted. Data-dependent verification failures use the measured API
/// below instead of being conflated with a bad caller configuration.
void validateAuthorityIntegrityVerifierLimits(const AuthorityIntegrityVerifierLimits & limits);

/// Domain-separated digest of the exact strictly sorted, unique definition
/// identities and revisions covered by one successful object verification.
/// The item bound is admitted before hashing; canonical validation remains
/// the caller's responsibility.
[[nodiscard]] Digest computeVerifiedRequiredDefinitionsDigest(
    std::span<const DefinitionIdentity> sorted_unique_definitions, UInt64 maximum_required_definitions);

/// Verifies one current stored-object image against one already pinned,
/// immutable authority root. It performs no I/O, repair, quarantine, cache
/// publication, root mutation, or background scheduling.
[[nodiscard]] VerifiedAuthorityObjectIntegrity verifyAuthorityObjectIntegrity(
    const AuthorityRoot & authority,
    const SidecarExpectationRecord & expectation,
    const PersistedTypeReferences & references,
    std::string_view canonical_sidecar_bytes,
    UInt64 current_object_schema_revision,
    const Digest & current_physical_schema_fingerprint,
    const AuthorityIntegrityVerifierLimits & limits = {});

}
