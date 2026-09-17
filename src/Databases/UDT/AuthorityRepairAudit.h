#pragma once

#include <Databases/UDT/AtomicAuthority.h>
#include <Databases/UDT/AuthorityRepairPlan.h>

#include <DataTypes/UDT/DependentObjectMetadataInstallationRecord.h>

#include <Core/Types.h>

#include <chrono>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <stop_token>
#include <string_view>
#include <vector>

namespace DB::UDT
{

class AuthorityRepairAuditBuildContinuation;

inline constexpr UInt16 authority_repair_audit_format = 1;
inline constexpr UInt16 authority_repair_audit_work_charge_abi = 2;

/// Complete exact-root audit inputs include the durable records, the
/// persisted sidecar, and an independently observed current schema-object
/// image for every rooted expectation. The first three tags deliberately
/// match AuthorityRepairArtifactKind; the remaining durable-chain and image
/// observations are diagnostic-only and cannot be repaired by replacing an
/// authority artifact.
enum class AuthorityRepairAuditArtifactKind : UInt8
{
    DefinitionRecord = 1,
    SidecarExpectationRecord = 2,
    PersistedTypeReferencesSidecar = 3,
    DependentObjectMetadataInstallationRecord = 4,
    DependentObjectMetadata = 5,
    StoredObjectImage = 6,
};

enum class AuthorityRepairObservationState : UInt8
{
    Missing = 1,
    Present = 2,
};

/// One storage-adapter observation. The audit derives every identity and the
/// exact ordered observation set from its pinned root; callers cannot add or
/// omit an artifact. Byte-artifact observations use `artifact_bytes` and leave
/// the image fields zero. StoredObjectImage observations have no bytes and
/// carry the independently observed object identity, current revision, and
/// fingerprint. The adapter must keep the complete batch and all referenced
/// bytes immutable for the duration of build(); no observation storage is
/// retained by the result. Storage-snapshot atomicity remains an adapter
/// property: concurrent drift can only produce a rooted fail-closed finding,
/// and no audit result can authorize publication or quarantine release.
struct AuthorityRepairObservation
{
    AuthorityRepairAuditArtifactKind artifact_kind{};
    AuthorityInventoryKey authority_key;
    AuthorityRepairObservationState state{};
    std::string_view artifact_bytes;
    SchemaObjectID object;
    UInt64 object_schema_revision = 0;
    Digest physical_schema_fingerprint{};
};

enum class AuthorityRepairFindingKind : UInt8
{
    Missing = 1,
    Malformed = 2,
    IdentityMismatch = 3,
    RevisionMismatch = 4,
    PhysicalSchemaMismatch = 5,
    CanonicalHashMismatch = 6,
    AuthenticationUnavailable = 7,
};

/// Canonical bounded damage description. `observed_digest` is a
/// domain-separated digest of the raw bytes or current object image; an
/// explicit Missing observation stores the zero value. A repair target is
/// present only when replacement of one of the three authority artifacts can
/// address the finding; all diagnostic-only artifacts remain fail-closed.
struct AuthorityRepairFinding
{
    AuthorityRepairFindingKind finding_kind{};
    AuthorityRepairAuditArtifactKind artifact_kind{};
    SchemaObjectID object;
    AuthorityInventoryKey authority_key;
    UInt64 expected_object_revision = 0;
    Digest expected_canonical_hash{};
    Digest expected_physical_schema_fingerprint{};
    AuthorityRepairObservationState observed_state{};
    UInt64 observed_object_revision = 0;
    Digest observed_physical_schema_fingerprint{};
    UInt64 observed_bytes = 0;
    Digest observed_digest{};
    std::optional<AuthorityRepairTarget> repair_target;
};

struct AuthorityRepairAuditLimits
{
    AuthorityIntegrityVerifierLimits object_verifier;
    AuthorityQuarantinePlanLimits quarantine;
    DependentObjectMetadataInstallationRecordLimits installation_record;
    UInt64 maximum_canonical_metadata_bytes = 16ULL << 20;
    UInt64 maximum_observations = 600'000;
    UInt64 maximum_findings = 600'000;
    UInt64 maximum_total_observed_bytes = 1ULL << 30;
    UInt64 maximum_work_units = 4ULL << 30;
    UInt64 maximum_scratch_bytes = 128ULL << 20;
    /// Findings, targets, root/manifest identity, and any persistent-root
    /// canonical caches materialized by the audit. The independently bounded
    /// quarantine plan accounts its own retention.
    UInt64 maximum_retained_canonical_bytes = 256ULL << 20;
    /// Ephemeral cooperative run controls. They are deliberately not part of
    /// the canonical audit identity or persisted scheduler configuration.
    std::stop_token cancellation;
    std::optional<std::chrono::steady_clock::time_point> monotonic_deadline;
    std::optional<UInt64> thread_cpu_deadline_nanoseconds;
};

struct AuthorityRepairAuditStatistics
{
    UInt64 inventory_leaves = 0;
    UInt64 expected_artifacts = 0;
    UInt64 observed_artifacts = 0;
    UInt64 clean_artifacts = 0;
    UInt64 findings = 0;
    UInt64 repair_targets = 0;
    UInt64 unrepairable_findings = 0;
    UInt64 failing_seeds = 0;
    UInt64 quarantined_objects = 0;
    UInt64 observed_bytes = 0;
    UInt64 work_units = 0;
    UInt64 scratch_bytes = 0;
    UInt64 retained_canonical_bytes = 0;
    UInt64 root_cache_bytes_materialized = 0;
    UInt64 manifest_bytes_hashed = 0;

    bool operator==(const AuthorityRepairAuditStatistics &) const = default;
};

class AuthorityRepairAuditError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        LimitExceeded,
        ExecutionBudgetExceeded,
        ArithmeticOverflow,
        InvalidRoot,
        InventoryMismatch,
        GraphMismatch,
        RecordStoreMismatch,
        NonCanonicalObservationSet,
        InvalidObservation,
        QuarantineFailure,
    };

    AuthorityRepairAuditError(Code code_, std::string_view message);

    const Code code;
};

/// Immutable short-lived exact-root damage audit. Construction consumes the
/// move-only AtomicAuthority hazard pin, validates the complete anchored
/// inventory/record/graph view, derives every required observation, collects
/// all bounded artifact findings, and builds quarantine from the resulting
/// seeds. A persisted-sidecar target always takes its nonzero expected hash
/// from the pinned root's exact expectation record, never from observed or
/// candidate bytes. It retains no observed bytes and has no publication or
/// release API.
/// Destroy it promptly once its caller has captured the same expected root
/// for a repair transaction; it deliberately occupies one bounded hazard slot.
class AuthorityRepairAudit final
{
public:
    using Ptr = std::unique_ptr<const AuthorityRepairAudit>;

    AuthorityRepairAudit(const AuthorityRepairAudit &) = delete;
    AuthorityRepairAudit & operator=(const AuthorityRepairAudit &) = delete;
    AuthorityRepairAudit(AuthorityRepairAudit &&) = delete;
    AuthorityRepairAudit & operator=(AuthorityRepairAudit &&) = delete;
    ~AuthorityRepairAudit() = default;

    /// Observations are strictly ordered by AuthorityInventoryKey and then by
    /// AuthorityRepairAuditArtifactKind. A definition leaf contributes one
    /// DefinitionRecord observation. An expectation leaf always contributes
    /// SidecarExpectationRecord, PersistedTypeReferencesSidecar, and
    /// StoredObjectImage observations. If its exact root record names a
    /// metadata-installation hash, it additionally contributes
    /// DependentObjectMetadataInstallationRecord and DependentObjectMetadata
    /// before StoredObjectImage. Missing artifacts remain explicit entries, so
    /// neither omission nor an unrooted extra is accepted. This convenience
    /// entry point runs the same cooperative engine to completion, so its
    /// acceptance, manifest, and statistics are identical to resume().
    [[nodiscard]] static Ptr build(
        AtomicAuthority::RootSnapshot && pinned_root,
        std::span<const AuthorityRepairObservation> exact_ordered_observations,
        const AuthorityRepairAuditLimits & limits = {});

    /// Resumes a cooperative exact-root audit. Every completed inventory leaf
    /// and manifest/closure item is sealed in `continuation` before a
    /// cancellation or deadline check. ExecutionBudgetExceeded leaves that
    /// sealed prefix resumable with a newly pinned copy of the same immutable
    /// root and the same owned observation batch. When supplied, the immutable
    /// preserved quarantine must belong to that root and its exact sorted seed
    /// image is unioned into the new closure. It does not create repair targets:
    /// only current observations do. This prevents a partial audit from
    /// narrowing an already published fail-closed boundary; old clean seeds can
    /// be released only by subsequent full-root re-verification.
    [[nodiscard]] static Ptr resume(
        AuthorityRepairAuditBuildContinuation & continuation,
        AtomicAuthority::RootSnapshot && pinned_root,
        std::span<const AuthorityRepairObservation> exact_ordered_observations,
        const AuthorityRepairAuditLimits & limits = {},
        AuthorityQuarantinePlan::Ptr preserved_quarantine = {});

    const AuthorityRootGraphIdentity & getRoot() const noexcept { return root; }
    const AuthorityInventorySummary & getInventorySummary() const noexcept { return inventory_summary; }
    std::span<const AuthorityRepairFinding> getFindings() const noexcept { return findings; }
    const AuthorityQuarantinePlan * getQuarantinePlan() const noexcept { return quarantine.get(); }
    AuthorityQuarantinePlan::Ptr pinQuarantinePlan() const noexcept { return quarantine; }
    const Digest & getDamagedArtifactManifestDigest() const noexcept { return damaged_artifact_manifest_digest; }
    UInt64 getDamagedArtifactCount() const noexcept { return statistics.findings; }
    const AuthorityRepairAuditStatistics & getStatistics() const noexcept { return statistics; }

    bool hasDamage() const noexcept { return !findings.empty(); }
    bool hasCompleteRepairTargetSet() const noexcept { return !findings.empty() && repair_targets.size() == findings.size(); }
    /// Partial automatic repair is never exposed. Individual findings retain
    /// diagnostic mappings, but source selection receives targets only when
    /// every finding is addressable by the exact authority-artifact model.
    std::span<const AuthorityRepairTarget> getCompleteRepairTargets() const noexcept
    {
        if (!hasCompleteRepairTargetSet())
            return {};
        return repair_targets;
    }

    /// No result of this audit or of source selection can clear quarantine.
    /// A separate verifier must check the complete closure against the new
    /// exact root after repair and before any future release coordinator runs.
    static constexpr bool requiresFullClosureReverificationBeforeRelease() noexcept { return true; }

private:
    AuthorityRepairAudit(
        AtomicAuthority::RootSnapshot && pinned_root_,
        AuthorityRootGraphIdentity root_,
        AuthorityInventorySummary inventory_summary_,
        std::vector<AuthorityRepairFinding> findings_,
        std::vector<AuthorityRepairTarget> repair_targets_,
        AuthorityQuarantinePlan::Ptr quarantine_,
        Digest damaged_artifact_manifest_digest_,
        AuthorityRepairAuditStatistics statistics_) noexcept;

    AtomicAuthority::RootSnapshot pinned_root;
    const AuthorityRootGraphIdentity root;
    const AuthorityInventorySummary inventory_summary;
    const std::vector<AuthorityRepairFinding> findings;
    const std::vector<AuthorityRepairTarget> repair_targets;
    const AuthorityQuarantinePlan::Ptr quarantine;
    const Digest damaged_artifact_manifest_digest;
    const AuthorityRepairAuditStatistics statistics;
};

class AuthorityRepairAuditBuildContinuation final
{
public:
    class Impl;

    AuthorityRepairAuditBuildContinuation();
    AuthorityRepairAuditBuildContinuation(const AuthorityRepairAuditBuildContinuation &) = delete;
    AuthorityRepairAuditBuildContinuation & operator=(const AuthorityRepairAuditBuildContinuation &) = delete;
    ~AuthorityRepairAuditBuildContinuation();

    void reset() noexcept;

private:
    friend class AuthorityRepairAudit;
    std::unique_ptr<Impl> impl;
};

static_assert(
    static_cast<UInt8>(AuthorityRepairAuditArtifactKind::DefinitionRecord)
    == static_cast<UInt8>(AuthorityRepairArtifactKind::DefinitionRecord));
static_assert(
    static_cast<UInt8>(AuthorityRepairAuditArtifactKind::SidecarExpectationRecord)
    == static_cast<UInt8>(AuthorityRepairArtifactKind::SidecarExpectationRecord));
static_assert(
    static_cast<UInt8>(AuthorityRepairAuditArtifactKind::PersistedTypeReferencesSidecar)
    == static_cast<UInt8>(AuthorityRepairArtifactKind::PersistedTypeReferencesSidecar));
static_assert(static_cast<UInt8>(AuthorityRepairAuditArtifactKind::DependentObjectMetadataInstallationRecord) == 4);
static_assert(static_cast<UInt8>(AuthorityRepairAuditArtifactKind::DependentObjectMetadata) == 5);
static_assert(static_cast<UInt8>(AuthorityRepairAuditArtifactKind::StoredObjectImage) == 6);
static_assert(static_cast<UInt8>(AuthorityRepairObservationState::Missing) == 1);
static_assert(static_cast<UInt8>(AuthorityRepairObservationState::Present) == 2);
static_assert(static_cast<UInt8>(AuthorityRepairFindingKind::Missing) == 1);
static_assert(static_cast<UInt8>(AuthorityRepairFindingKind::Malformed) == 2);
static_assert(static_cast<UInt8>(AuthorityRepairFindingKind::IdentityMismatch) == 3);
static_assert(static_cast<UInt8>(AuthorityRepairFindingKind::RevisionMismatch) == 4);
static_assert(static_cast<UInt8>(AuthorityRepairFindingKind::PhysicalSchemaMismatch) == 5);
static_assert(static_cast<UInt8>(AuthorityRepairFindingKind::CanonicalHashMismatch) == 6);
static_assert(static_cast<UInt8>(AuthorityRepairFindingKind::AuthenticationUnavailable) == 7);

}
