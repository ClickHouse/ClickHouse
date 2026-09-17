#pragma once

#include <Databases/UDT/AuthorityQuarantinePlan.h>

#include <DataTypes/UDT/AuthorityInventory.h>
#include <DataTypes/UDT/PersistedTypeReferences.h>
#include <DataTypes/UDT/Record.h>

#include <Core/Types.h>

#include <memory>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB::UDT
{

class AuthorityRepairAudit;
class AuthorityRepairPlanBuildContinuation;

inline constexpr UInt16 authority_repair_plan_work_charge_abi = 2;

enum class AuthorityRepairSource : UInt8
{
    LocalSchemaWAL = 1,
    ReplicatedAuthority = 2,
    VerifiedBackup = 3,
};

/// Local planner tags for the three independently replaceable authority
/// artifacts. `PersistedTypeReferencesSidecar` is addressed through the
/// matching sidecar-expectation inventory key, but remains a separate target.
enum class AuthorityRepairArtifactKind : UInt8
{
    DefinitionRecord = 1,
    SidecarExpectationRecord = 2,
    PersistedTypeReferencesSidecar = 3,
};

/// Exact audit-derived repair target emitted from a complete pinned-root
/// integrity audit. Callers cannot construct the plan's target set. The
/// expected hash is anchored independently from every repair
/// source. It may be zero only for a persisted sidecar paired with an
/// expectation-record target in the same plan; the planner then derives the
/// sidecar hash from the first domain-exact expectation source.
struct AuthorityRepairTarget
{
    AuthorityRepairArtifactKind artifact_kind{};
    SchemaObjectID object;
    AuthorityInventoryKey authority_key;
    UInt64 object_revision = 0;
    Digest expected_canonical_hash{};
    Digest physical_schema_fingerprint{};
};

/// Already loaded candidate image. There is deliberately no candidate hash:
/// exact selection independently decodes and domain-hashes the complete bytes.
/// `source_reference` is bounded opaque canonical provenance owned by the
/// source adapter; the pure planner performs no lookup or I/O.
struct AuthorityRepairCandidate
{
    AuthorityRepairSource source{};
    AuthorityRepairArtifactKind artifact_kind{};
    SchemaObjectID object;
    AuthorityInventoryKey authority_key;
    UInt64 object_revision = 0;
    Digest physical_schema_fingerprint{};
    std::string_view canonical_bytes;
    std::string_view source_reference;
};

struct AuthorityRepairPlanLimits
{
    UInt64 maximum_targets = 100'000;
    UInt64 maximum_candidates = 300'000;
    UInt64 maximum_source_reference_bytes = 4ULL << 10;
    UInt64 maximum_total_input_bytes = 512ULL << 20;
    UInt64 maximum_work_units = 2ULL << 30;
    UInt64 maximum_scratch_bytes = 64ULL << 20;
    UInt64 maximum_retained_plan_bytes = 512ULL << 20;
};

struct AuthorityRepairPlanStatistics
{
    UInt64 targets = 0;
    UInt64 candidates = 0;
    UInt64 candidate_groups = 0;
    UInt64 exact_candidates = 0;
    UInt64 input_bytes = 0;
    UInt64 work_units = 0;
    UInt64 scratch_bytes = 0;
    UInt64 retained_plan_bytes = 0;

    bool operator==(const AuthorityRepairPlanStatistics &) const = default;
};

class AuthorityRepairPlanError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidConfiguration,
        LimitExceeded,
        ArithmeticOverflow,
        EmptyTargetSet,
        IncompleteAuditTargetSet,
        InvalidTarget,
        NonCanonicalTargetSet,
        TargetOutsideQuarantine,
        UnpairedPersistedSidecar,
        InconsistentSidecarPair,
        InvalidCandidateSet,
        UnexpectedCandidate,
        ConflictingCandidate,
        ExactSourceMissing,
    };

    AuthorityRepairPlanError(Code code_, std::string_view message);

    const Code code;
};

struct AuthorityRepairSelection
{
    AuthorityRepairArtifactKind artifact_kind{};
    SchemaObjectID object;
    AuthorityInventoryKey authority_key;
    UInt64 object_revision = 0;
    Digest canonical_hash{};
    Digest physical_schema_fingerprint{};
    AuthorityRepairSource source{};
    String canonical_bytes;
    String source_reference;

    bool operator==(const AuthorityRepairSelection &) const = default;
};

/// Immutable all-or-none source-selection result. Entries remain in the exact
/// canonical target order and own independently decoded, domain-hashed source
/// bytes plus bounded provenance.
class AuthorityRepairPlan final
{
public:
    using Ptr = std::shared_ptr<const AuthorityRepairPlan>;

    /// Consumes only internally derived targets from one exact-root audit; a
    /// caller cannot substitute or omit damaged artifacts. The audit must have
    /// a complete repairable target set and an exact quarantine closure. This
    /// feature-inert result selects sources but still cannot authorize repair
    /// publication or quarantine release. Targets are ordered by authority key
    /// and artifact tag; candidates use the same order and then `LocalSchemaWAL`,
    /// `ReplicatedAuthority`, `VerifiedBackup`. Byte-identical duplicates from
    /// one source are accepted as one bounded source group; conflicting copies
    /// fail closed.
    [[nodiscard]] static Ptr build(
        const AuthorityRepairAudit & audit,
        std::span<const AuthorityRepairCandidate> ordered_candidates,
        const AuthorityRepairPlanLimits & limits = {});

    /// Cooperative fast path for the canonical automatic-repair candidate
    /// image: at most one already priority-selected candidate per target.
    /// Each call seals at most `maximum_work_items` complete target/file
    /// identity or decisions and returns null until the all-or-none plan is
    /// complete. The caller keeps the candidate array and every referenced
    /// byte string immutable and at the same address until completion; the
    /// continuation seals a digest per candidate and rechecks it before use.
    [[nodiscard]] static Ptr resumeExactCandidateSet(
        AuthorityRepairPlanBuildContinuation & continuation,
        const AuthorityRepairAudit & audit,
        std::span<const AuthorityRepairCandidate> ordered_candidates,
        UInt64 maximum_work_items,
        const AuthorityRepairPlanLimits & limits = {});

    const AuthorityRootGraphIdentity & getRoot() const noexcept { return root; }
    const Digest & getDamagedArtifactManifestDigest() const noexcept { return damaged_artifact_manifest_digest; }
    UInt64 getDamagedArtifactCount() const noexcept { return damaged_artifact_count; }
    std::span<const AuthorityRepairSelection> getSelections() const noexcept { return selections; }
    const AuthorityRepairPlanStatistics & getStatistics() const noexcept { return statistics; }

private:
    AuthorityRepairPlan(
        AuthorityRootGraphIdentity root_,
        Digest damaged_artifact_manifest_digest_,
        UInt64 damaged_artifact_count_,
        std::vector<AuthorityRepairSelection> selections_,
        AuthorityRepairPlanStatistics statistics_) noexcept;

    const AuthorityRootGraphIdentity root;
    const Digest damaged_artifact_manifest_digest;
    const UInt64 damaged_artifact_count;
    const std::vector<AuthorityRepairSelection> selections;
    const AuthorityRepairPlanStatistics statistics;
};

class AuthorityRepairPlanBuildContinuation final
{
public:
    class Impl;

    AuthorityRepairPlanBuildContinuation();
    AuthorityRepairPlanBuildContinuation(const AuthorityRepairPlanBuildContinuation &) = delete;
    AuthorityRepairPlanBuildContinuation & operator=(const AuthorityRepairPlanBuildContinuation &) = delete;
    ~AuthorityRepairPlanBuildContinuation();

private:
    friend class AuthorityRepairPlan;
    std::unique_ptr<Impl> impl;
};

static_assert(static_cast<UInt8>(AuthorityRepairSource::LocalSchemaWAL) == 1);
static_assert(static_cast<UInt8>(AuthorityRepairSource::ReplicatedAuthority) == 2);
static_assert(static_cast<UInt8>(AuthorityRepairSource::VerifiedBackup) == 3);
static_assert(static_cast<UInt8>(AuthorityRepairArtifactKind::DefinitionRecord) == 1);
static_assert(static_cast<UInt8>(AuthorityRepairArtifactKind::SidecarExpectationRecord) == 2);
static_assert(static_cast<UInt8>(AuthorityRepairArtifactKind::PersistedTypeReferencesSidecar) == 3);

}
