#include <Databases/UDT/AuthorityRepairPlan.h>

#include <Databases/UDT/AuthorityRepairAudit.h>

#include <DataTypes/UDT/SidecarExpectationRecord.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <limits>
#include <optional>
#include <utility>

namespace DB::UDT
{
namespace
{

using PlanError = AuthorityRepairPlanError;

constexpr UInt64 canonical_uuid_bytes = 16;
constexpr UInt64 authority_root_graph_identity_canonical_bytes = canonical_uuid_bytes + sizeof(UInt64) + 2 * sizeof(Digest);
constexpr UInt64 schema_object_identity_canonical_bytes = sizeof(UInt8) + 2 * canonical_uuid_bytes;
constexpr UInt64 inventory_key_canonical_bytes = sizeof(UInt16) + sizeof(UInt8) + canonical_uuid_bytes;
constexpr UInt64 target_view_base_canonical_bytes
    = sizeof(UInt8) + schema_object_identity_canonical_bytes + inventory_key_canonical_bytes + sizeof(UInt64) + 2 * sizeof(Digest);
constexpr UInt64 candidate_view_base_canonical_bytes = 2 * sizeof(UInt8) + schema_object_identity_canonical_bytes
    + inventory_key_canonical_bytes + sizeof(UInt64) + sizeof(Digest) + 2 * sizeof(UInt64);
constexpr UInt64 selection_base_canonical_bytes = sizeof(UInt8) + schema_object_identity_canonical_bytes + inventory_key_canonical_bytes
    + sizeof(UInt64) + 2 * sizeof(Digest) + sizeof(UInt8) + 2 * sizeof(UInt64);
constexpr UInt64 repair_plan_base_canonical_bytes = authority_root_graph_identity_canonical_bytes + sizeof(Digest) + 2 * sizeof(UInt64);
constexpr std::string_view repair_candidate_identity_domain = "ClickHouse UDT repair candidate identity V1";
constexpr std::string_view repair_candidate_batch_domain = "ClickHouse UDT repair candidate batch V1";

[[noreturn]] void fail(PlanError::Code code, std::string_view message)
{
    throw PlanError(code, message);
}

UInt64 toUInt64(size_t value) noexcept
{
    static_assert(sizeof(size_t) <= sizeof(UInt64));
    return static_cast<UInt64>(value);
}

UInt64 checkedAdd(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        fail(PlanError::Code::ArithmeticOverflow, message);
    return lhs + rhs;
}

UInt64 checkedMultiply(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (lhs != 0 && rhs > std::numeric_limits<UInt64>::max() / lhs)
        fail(PlanError::Code::ArithmeticOverflow, message);
    return lhs * rhs;
}

void admit(UInt64 value, UInt64 maximum, std::string_view message)
{
    if (value > maximum)
        fail(PlanError::Code::LimitExceeded, message);
}

bool isKnownArtifactKind(AuthorityRepairArtifactKind kind) noexcept
{
    switch (kind)
    {
        case AuthorityRepairArtifactKind::DefinitionRecord:
        case AuthorityRepairArtifactKind::SidecarExpectationRecord:
        case AuthorityRepairArtifactKind::PersistedTypeReferencesSidecar: return true;
    }
    return false;
}

bool isSidecarObjectKind(SchemaObjectKind kind) noexcept
{
    return kind == SchemaObjectKind::Table || kind == SchemaObjectKind::View || kind == SchemaObjectKind::Dictionary
        || kind == SchemaObjectKind::SyntheticTestObject;
}

bool isValidArtifactKey(const AuthorityInventoryKey & key, AuthorityRepairArtifactKind kind) noexcept
{
    if (!isKnownArtifactKind(kind) || key.format_version != authority_inventory_format_version || key.object_uuid == UUIDHelpers::Nil)
        return false;

    switch (kind)
    {
        case AuthorityRepairArtifactKind::DefinitionRecord: return key.record_kind == AuthorityInventoryRecordKind::TypeDefinition;
        case AuthorityRepairArtifactKind::SidecarExpectationRecord:
        case AuthorityRepairArtifactKind::PersistedTypeReferencesSidecar:
            return key.record_kind == AuthorityInventoryRecordKind::SidecarExpectation;
    }
    return false;
}

bool sameArtifactIdentity(
    const AuthorityInventoryKey & lhs_key,
    AuthorityRepairArtifactKind lhs_kind,
    const AuthorityInventoryKey & rhs_key,
    AuthorityRepairArtifactKind rhs_kind) noexcept
{
    return lhs_key == rhs_key && lhs_kind == rhs_kind;
}

bool artifactIdentityLess(
    const AuthorityInventoryKey & lhs_key,
    AuthorityRepairArtifactKind lhs_kind,
    const AuthorityInventoryKey & rhs_key,
    AuthorityRepairArtifactKind rhs_kind) noexcept
{
    if (authorityInventoryKeyLess(lhs_key, rhs_key))
        return true;
    if (authorityInventoryKeyLess(rhs_key, lhs_key))
        return false;
    return static_cast<UInt8>(lhs_kind) < static_cast<UInt8>(rhs_kind);
}

UInt8 sourceRank(AuthorityRepairSource source)
{
    switch (source)
    {
        case AuthorityRepairSource::LocalSchemaWAL: return 1;
        case AuthorityRepairSource::ReplicatedAuthority: return 2;
        case AuthorityRepairSource::VerifiedBackup: return 3;
    }
    fail(PlanError::Code::InvalidCandidateSet, "authority repair candidate source is unknown");
}

UInt64 artifactByteLimit(AuthorityRepairArtifactKind kind, PlanError::Code invalid_kind_code)
{
    switch (kind)
    {
        case AuthorityRepairArtifactKind::DefinitionRecord: return RecordLimits{}.maximum_record_bytes;
        case AuthorityRepairArtifactKind::SidecarExpectationRecord: return toUInt64(sidecar_expectation_record_extended_encoded_bytes);
        case AuthorityRepairArtifactKind::PersistedTypeReferencesSidecar: return PersistedTypeReferencesLimits{}.maximum_sidecar_bytes;
    }
    fail(invalid_kind_code, "authority repair artifact kind is unknown");
}

void updateUInt8(CanonicalHasher & hasher, UInt8 value)
{
    const std::array<CanonicalByte, 1> bytes{value};
    hasher.update(bytes);
}

void updateUInt16(CanonicalHasher & hasher, UInt16 value)
{
    std::array<CanonicalByte, sizeof(UInt16)> bytes{};
    for (size_t index = 0; index < bytes.size(); ++index)
        bytes[index] = static_cast<CanonicalByte>(value >> (8 * index));
    hasher.update(bytes);
}

void updateUInt64(CanonicalHasher & hasher, UInt64 value)
{
    std::array<CanonicalByte, sizeof(UInt64)> bytes{};
    for (size_t index = 0; index < bytes.size(); ++index)
        bytes[index] = static_cast<CanonicalByte>(value >> (8 * index));
    hasher.update(bytes);
}

Digest computeCandidateIdentity(const AuthorityRepairCandidate & candidate)
{
    CanonicalHasher hasher(repair_candidate_identity_domain);
    updateUInt8(hasher, static_cast<UInt8>(candidate.source));
    updateUInt8(hasher, static_cast<UInt8>(candidate.artifact_kind));
    updateUInt8(hasher, static_cast<UInt8>(candidate.object.kind));
    hasher.updateUUID(candidate.object.database_uuid);
    hasher.updateUUID(candidate.object.object_uuid);
    updateUInt16(hasher, candidate.authority_key.format_version);
    updateUInt8(hasher, static_cast<UInt8>(candidate.authority_key.record_kind));
    hasher.updateUUID(candidate.authority_key.object_uuid);
    updateUInt64(hasher, candidate.object_revision);
    hasher.update(candidate.physical_schema_fingerprint);
    updateUInt64(hasher, toUInt64(candidate.canonical_bytes.size()));
    hasher.update(candidate.canonical_bytes);
    updateUInt64(hasher, toUInt64(candidate.source_reference.size()));
    hasher.update(candidate.source_reference);
    return hasher.finalize();
}

UInt64 candidateDecodeScratchUpperBound(AuthorityRepairArtifactKind kind, UInt64 bytes)
{
    UInt64 result = checkedMultiply(bytes, 2, "authority repair candidate decode scratch overflows UInt64");
    switch (kind)
    {
        case AuthorityRepairArtifactKind::DefinitionRecord: {
            const RecordLimits limits;
            result = checkedAdd(
                result,
                checkedAdd(
                    checkedMultiply(
                        std::min(bytes, limits.maximum_parameter_count),
                        sizeof(Parameter),
                        "authority repair record decode scratch overflows UInt64"),
                    checkedMultiply(
                        std::min(bytes, limits.maximum_dependency_count),
                        sizeof(DefinitionDependency),
                        "authority repair record decode scratch overflows UInt64"),
                    "authority repair record decode scratch overflows UInt64"),
                "authority repair record decode scratch overflows UInt64");
            break;
        }
        case AuthorityRepairArtifactKind::SidecarExpectationRecord:
            result = checkedAdd(result, sizeof(SidecarExpectationRecord), "authority repair expectation decode scratch overflows UInt64");
            break;
        case AuthorityRepairArtifactKind::PersistedTypeReferencesSidecar: {
            const PersistedTypeReferencesLimits limits;
            const UInt64 descriptor_count = std::min(bytes, limits.maximum_descriptors);
            const UInt64 path_count = std::min(bytes, limits.maximum_occurrence_paths);
            const UInt64 ordinal_count = std::min(
                bytes,
                checkedMultiply(
                    limits.maximum_occurrence_paths, limits.maximum_path_depth, "authority repair occurrence scratch overflows UInt64"));
            result = checkedAdd(
                result,
                checkedAdd(
                    checkedMultiply(
                        descriptor_count, sizeof(PersistedTypeDescriptor), "authority repair descriptor scratch overflows UInt64"),
                    checkedAdd(
                        checkedMultiply(
                            path_count,
                            sizeof(PersistedTypeOccurrencePath) + sizeof(PersistedTypeOccurrenceUse),
                            "authority repair occurrence scratch overflows UInt64"),
                        checkedMultiply(ordinal_count, sizeof(UInt64), "authority repair occurrence scratch overflows UInt64"),
                        "authority repair occurrence scratch overflows UInt64"),
                    "authority repair sidecar decode scratch overflows UInt64"),
                "authority repair sidecar decode scratch overflows UInt64");
            break;
        }
    }
    return result;
}

void validateLimits(const AuthorityRepairPlanLimits & limits)
{
    constexpr AuthorityRepairPlanLimits maxima;
    const auto valid = [](UInt64 value, UInt64 maximum) { return value != 0 && value <= maximum; };

    if (!valid(limits.maximum_targets, maxima.maximum_targets) || !valid(limits.maximum_candidates, maxima.maximum_candidates)
        || !valid(limits.maximum_source_reference_bytes, maxima.maximum_source_reference_bytes)
        || !valid(limits.maximum_total_input_bytes, maxima.maximum_total_input_bytes)
        || !valid(limits.maximum_work_units, maxima.maximum_work_units)
        || !valid(limits.maximum_scratch_bytes, maxima.maximum_scratch_bytes)
        || !valid(limits.maximum_retained_plan_bytes, maxima.maximum_retained_plan_bytes))
        fail(PlanError::Code::InvalidConfiguration, "authority repair-plan limits are invalid");
}

void validateTargetMapping(const AuthorityRepairTarget & target, const UUID & database_uuid)
{
    if (!target.object.isValid() || target.object.database_uuid != database_uuid
        || !isValidArtifactKey(target.authority_key, target.artifact_kind) || target.authority_key.object_uuid != target.object.object_uuid
        || target.object_revision == 0)
        fail(PlanError::Code::InvalidTarget, "authority repair target identity is invalid");

    switch (target.artifact_kind)
    {
        case AuthorityRepairArtifactKind::DefinitionRecord:
            if (target.object.kind != SchemaObjectKind::TypeDefinition || target.physical_schema_fingerprint != Digest{}
                || target.expected_canonical_hash == Digest{})
                fail(PlanError::Code::InvalidTarget, "definition repair target metadata is invalid");
            return;
        case AuthorityRepairArtifactKind::SidecarExpectationRecord:
            if (!isSidecarObjectKind(target.object.kind))
                fail(PlanError::Code::InvalidTarget, "sidecar repair target has an invalid object kind");
            if (target.expected_canonical_hash == Digest{})
                fail(PlanError::Code::InvalidTarget, "expectation repair target has no anchored record hash");
            return;
        case AuthorityRepairArtifactKind::PersistedTypeReferencesSidecar:
            if (!isSidecarObjectKind(target.object.kind))
                fail(PlanError::Code::InvalidTarget, "sidecar repair target has an invalid object kind");
            return;
    }
    fail(PlanError::Code::InvalidTarget, "authority repair target artifact kind is unknown");
}

struct CandidateFacts
{
    Digest canonical_hash{};
    std::optional<Digest> expected_sidecar_hash;
};

std::optional<CandidateFacts>
inspectCandidate(const AuthorityRepairCandidate & candidate, const AuthorityRepairTarget & target, const Digest & expected_hash)
{
    try
    {
        if (candidate.artifact_kind != target.artifact_kind || candidate.object != target.object
            || candidate.authority_key != target.authority_key || candidate.object_revision != target.object_revision
            || candidate.physical_schema_fingerprint != target.physical_schema_fingerprint || candidate.canonical_bytes.empty()
            || candidate.source_reference.empty())
            return std::nullopt;

        CandidateFacts facts;
        switch (candidate.artifact_kind)
        {
            case AuthorityRepairArtifactKind::DefinitionRecord: {
                const Record record = decodeRecord(candidate.canonical_bytes);
                if (record.identity.database_uuid != candidate.object.database_uuid
                    || record.identity.type_uuid != candidate.object.object_uuid || record.identity.revision != candidate.object_revision)
                    return std::nullopt;
                facts.canonical_hash = computeRecordHash(record);
                break;
            }
            case AuthorityRepairArtifactKind::SidecarExpectationRecord: {
                const SidecarExpectationRecord expectation = decodeSidecarExpectationRecord(candidate.canonical_bytes);
                if (expectation.object != candidate.object || expectation.object_schema_revision != candidate.object_revision
                    || expectation.physical_schema_fingerprint != candidate.physical_schema_fingerprint)
                    return std::nullopt;
                facts.canonical_hash = computeSidecarExpectationRecordHash(expectation);
                facts.expected_sidecar_hash = expectation.sidecar_hash;
                break;
            }
            case AuthorityRepairArtifactKind::PersistedTypeReferencesSidecar: {
                const PersistedTypeReferences references = decodePersistedTypeReferences(candidate.canonical_bytes);
                if (references.object != candidate.object || references.object_schema_revision != candidate.object_revision
                    || references.physical_schema_fingerprint != candidate.physical_schema_fingerprint)
                    return std::nullopt;
                facts.canonical_hash = computePersistedTypeReferencesSidecarHash(references);
                break;
            }
        }
        if (facts.canonical_hash != expected_hash)
            return std::nullopt;
        return facts;
    }
    catch (const RecordError &)
    {
        return std::nullopt;
    }
    catch (const SidecarExpectationRecordError &)
    {
        return std::nullopt;
    }
    catch (const PersistedTypeReferencesError &)
    {
        return std::nullopt;
    }
}

bool sameCandidate(const AuthorityRepairCandidate & lhs, const AuthorityRepairCandidate & rhs) noexcept
{
    return lhs.source == rhs.source && lhs.artifact_kind == rhs.artifact_kind && lhs.object == rhs.object
        && lhs.authority_key == rhs.authority_key && lhs.object_revision == rhs.object_revision
        && lhs.physical_schema_fingerprint == rhs.physical_schema_fingerprint && lhs.canonical_bytes == rhs.canonical_bytes
        && lhs.source_reference == rhs.source_reference;
}

}

class AuthorityRepairPlanBuildContinuation::Impl final
{
public:
    void clear() noexcept
    {
        audit = nullptr;
        root = {};
        manifest = {};
        damaged_artifacts = 0;
        candidate_data = nullptr;
        candidate_size = 0;
        candidate_validation_index = 0;
        candidate_identity_hasher.reset();
        candidate_batch_digest.reset();
        candidate_digests = std::vector<Digest>{};
        target_index = 0;
        candidate_index = 0;
        membership_work_per_target = 0;
        maximum_source_reference_bytes_seen = 0;
        preceding_expectation.reset();
        selected_expectation_sidecar_hash.reset();
        selections = std::vector<AuthorityRepairSelection>{};
        statistics = {};
        initialized = false;
    }

    const AuthorityRepairAudit * audit = nullptr;
    AuthorityRootGraphIdentity root;
    Digest manifest{};
    UInt64 damaged_artifacts = 0;
    const AuthorityRepairCandidate * candidate_data = nullptr;
    size_t candidate_size = 0;
    size_t candidate_validation_index = 0;
    std::unique_ptr<CanonicalHasher> candidate_identity_hasher;
    std::optional<Digest> candidate_batch_digest;
    std::vector<Digest> candidate_digests;
    size_t target_index = 0;
    size_t candidate_index = 0;
    UInt64 membership_work_per_target = 0;
    UInt64 maximum_source_reference_bytes_seen = 0;
    std::optional<AuthorityRepairTarget> preceding_expectation;
    std::optional<Digest> selected_expectation_sidecar_hash;
    std::vector<AuthorityRepairSelection> selections;
    AuthorityRepairPlanStatistics statistics;
    bool initialized = false;
};

AuthorityRepairPlanBuildContinuation::AuthorityRepairPlanBuildContinuation()
    : impl(std::make_unique<Impl>())
{
}

AuthorityRepairPlanBuildContinuation::~AuthorityRepairPlanBuildContinuation() = default;

AuthorityRepairPlanError::AuthorityRepairPlanError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

AuthorityRepairPlan::AuthorityRepairPlan(
    AuthorityRootGraphIdentity root_,
    Digest damaged_artifact_manifest_digest_,
    UInt64 damaged_artifact_count_,
    std::vector<AuthorityRepairSelection> selections_,
    AuthorityRepairPlanStatistics statistics_) noexcept
    : root(std::move(root_))
    , damaged_artifact_manifest_digest(std::move(damaged_artifact_manifest_digest_))
    , damaged_artifact_count(damaged_artifact_count_)
    , selections(std::move(selections_))
    , statistics(std::move(statistics_))
{
}

AuthorityRepairPlan::Ptr AuthorityRepairPlan::resumeExactCandidateSet(
    AuthorityRepairPlanBuildContinuation & continuation,
    const AuthorityRepairAudit & audit,
    std::span<const AuthorityRepairCandidate> ordered_candidates,
    UInt64 maximum_work_items,
    const AuthorityRepairPlanLimits & limits)
{
    validateLimits(limits);
    if (maximum_work_items == 0)
        fail(PlanError::Code::InvalidConfiguration, "authority repair cooperative plan pass must admit at least one work item");
    if (!audit.hasDamage())
        fail(PlanError::Code::EmptyTargetSet, "authority repair audit contains no damaged artifact");
    if (!audit.hasCompleteRepairTargetSet() || !audit.getQuarantinePlan())
        fail(PlanError::Code::IncompleteAuditTargetSet, "authority repair audit contains damage without a complete repairable target set");
    const auto targets = audit.getCompleteRepairTargets();
    const auto & quarantine = *audit.getQuarantinePlan();
    auto & progress = *continuation.impl;

    if (!progress.initialized)
    {
        const UInt64 target_count = toUInt64(targets.size());
        const UInt64 candidate_count = toUInt64(ordered_candidates.size());
        if (target_count == 0)
            fail(PlanError::Code::EmptyTargetSet, "authority repair requires at least one exact target");
        admit(target_count, limits.maximum_targets, "authority repair target count exceeds its limit");
        admit(candidate_count, limits.maximum_candidates, "authority repair candidate count exceeds its limit");
        if (candidate_count > target_count)
            fail(
                PlanError::Code::InvalidCandidateSet,
                "cooperative automatic repair requires at most one priority-selected candidate per exact target");

        progress.audit = std::addressof(audit);
        progress.root = audit.getRoot();
        progress.manifest = audit.getDamagedArtifactManifestDigest();
        progress.damaged_artifacts = audit.getDamagedArtifactCount();
        progress.candidate_data = ordered_candidates.data();
        progress.candidate_size = ordered_candidates.size();
        progress.candidate_identity_hasher = std::make_unique<CanonicalHasher>(repair_candidate_batch_domain);
        updateUInt64(*progress.candidate_identity_hasher, candidate_count);
        progress.statistics.targets = target_count;
        progress.statistics.candidates = candidate_count;

        UInt64 comparisons = 1;
        for (UInt64 closure_items = toUInt64(quarantine.getQuarantinedObjects().size()); closure_items != 0; closure_items >>= 1)
            comparisons = checkedAdd(comparisons, 1, "authority repair membership work overflows UInt64");
        progress.membership_work_per_target = comparisons;
        progress.statistics.input_bytes = checkedAdd(
            checkedMultiply(target_count, target_view_base_canonical_bytes, "authority repair target input bytes overflow UInt64"),
            checkedMultiply(candidate_count, candidate_view_base_canonical_bytes, "authority repair candidate input bytes overflow UInt64"),
            "authority repair fixed input bytes overflow UInt64");
        admit(
            progress.statistics.input_bytes, limits.maximum_total_input_bytes, "authority repair input bytes exceed their aggregate limit");
        progress.statistics.work_units = checkedAdd(
            checkedMultiply(
                checkedAdd(target_count, candidate_count, "authority repair item count overflows UInt64"),
                8,
                "authority repair item work overflows UInt64"),
            checkedMultiply(target_count, comparisons, "authority repair membership work overflows UInt64"),
            "authority repair work overflows UInt64");
        admit(progress.statistics.work_units, limits.maximum_work_units, "authority repair work exceeds its limit");
        progress.statistics.scratch_bytes
            = checkedMultiply(candidate_count, sizeof(Digest), "authority repair candidate identity scratch bytes overflow UInt64");
        admit(progress.statistics.scratch_bytes, limits.maximum_scratch_bytes, "authority repair selection scratch exceeds its byte limit");
        progress.statistics.retained_plan_bytes = checkedAdd(
            repair_plan_base_canonical_bytes,
            checkedMultiply(target_count, selection_base_canonical_bytes, "authority repair retained bytes overflow UInt64"),
            "authority repair retained bytes overflow UInt64");
        admit(
            progress.statistics.retained_plan_bytes,
            limits.maximum_retained_plan_bytes,
            "authority repair fixed plan retention exceeds its byte limit");
        progress.candidate_digests.reserve(ordered_candidates.size());
        progress.selections.reserve(targets.size());
        progress.initialized = true;
    }
    else if (
        progress.audit != std::addressof(audit) || progress.root != audit.getRoot()
        || progress.manifest != audit.getDamagedArtifactManifestDigest() || progress.damaged_artifacts != audit.getDamagedArtifactCount()
        || progress.candidate_data != ordered_candidates.data() || progress.candidate_size != ordered_candidates.size())
    {
        fail(PlanError::Code::InvalidCandidateSet, "authority repair cooperative plan continuation changed its exact inputs");
    }
    admit(progress.statistics.targets, limits.maximum_targets, "authority repair continued target count exceeds its current limit");
    admit(
        progress.statistics.candidates, limits.maximum_candidates, "authority repair continued candidate count exceeds its current limit");
    admit(progress.statistics.input_bytes, limits.maximum_total_input_bytes, "authority repair continued input exceeds its current limit");
    admit(progress.statistics.work_units, limits.maximum_work_units, "authority repair continued work exceeds its current limit");
    admit(progress.statistics.scratch_bytes, limits.maximum_scratch_bytes, "authority repair continued scratch exceeds its current limit");
    admit(
        progress.statistics.retained_plan_bytes,
        limits.maximum_retained_plan_bytes,
        "authority repair continued retention exceeds its current limit");
    admit(
        progress.maximum_source_reference_bytes_seen,
        limits.maximum_source_reference_bytes,
        "authority repair continued provenance exceeds its current limit");

    UInt64 completed = 0;
    while (progress.candidate_validation_index < ordered_candidates.size() && completed < maximum_work_items)
    {
        const auto & candidate = ordered_candidates[progress.candidate_validation_index];
        static_cast<void>(sourceRank(candidate.source));
        if (!candidate.object.isValid() || !isValidArtifactKey(candidate.authority_key, candidate.artifact_kind)
            || candidate.authority_key.object_uuid != candidate.object.object_uuid || candidate.object_revision == 0
            || candidate.canonical_bytes.empty() || candidate.source_reference.empty())
        {
            fail(PlanError::Code::InvalidCandidateSet, "authority repair candidate identity or payload is invalid");
        }
        const UInt64 bytes = toUInt64(candidate.canonical_bytes.size());
        const UInt64 provenance = toUInt64(candidate.source_reference.size());
        admit(
            bytes,
            artifactByteLimit(candidate.artifact_kind, PlanError::Code::InvalidCandidateSet),
            "authority repair candidate artifact exceeds its implementation byte limit");
        admit(provenance, limits.maximum_source_reference_bytes, "authority repair candidate provenance exceeds its byte limit");
        progress.maximum_source_reference_bytes_seen = std::max(progress.maximum_source_reference_bytes_seen, provenance);
        progress.statistics.input_bytes = checkedAdd(
            progress.statistics.input_bytes,
            checkedAdd(bytes, provenance, "authority repair candidate input bytes overflow UInt64"),
            "authority repair candidate input bytes overflow UInt64");
        admit(
            progress.statistics.input_bytes, limits.maximum_total_input_bytes, "authority repair input bytes exceed their aggregate limit");
        const UInt64 identity_scratch = checkedMultiply(
            toUInt64(progress.candidate_size), sizeof(Digest), "authority repair candidate identity scratch bytes overflow UInt64");
        progress.statistics.scratch_bytes = std::max(
            progress.statistics.scratch_bytes,
            checkedAdd(
                identity_scratch,
                candidateDecodeScratchUpperBound(candidate.artifact_kind, bytes),
                "authority repair candidate scratch bytes overflow UInt64"));
        admit(progress.statistics.scratch_bytes, limits.maximum_scratch_bytes, "authority repair candidate scratch exceeds its byte limit");
        progress.statistics.work_units = checkedAdd(
            progress.statistics.work_units,
            checkedAdd(bytes, provenance, "authority repair candidate identity work overflows UInt64"),
            "authority repair candidate identity work overflows UInt64");
        admit(progress.statistics.work_units, limits.maximum_work_units, "authority repair work exceeds its limit");
        const Digest identity = computeCandidateIdentity(candidate);
        progress.candidate_digests.push_back(identity);
        progress.candidate_identity_hasher->update(identity);
        ++progress.candidate_validation_index;
        ++completed;
    }
    if (progress.candidate_validation_index != ordered_candidates.size())
        return {};
    if (!progress.candidate_batch_digest)
    {
        progress.candidate_batch_digest = progress.candidate_identity_hasher->finalize();
        progress.candidate_identity_hasher.reset();
    }

    while (progress.target_index < targets.size() && completed < maximum_work_items)
    {
        const auto & target = targets[progress.target_index];
        validateTargetMapping(target, quarantine.getRoot().authority_root.database_uuid);
        if (progress.target_index != 0
            && !artifactIdentityLess(
                targets[progress.target_index - 1].authority_key,
                targets[progress.target_index - 1].artifact_kind,
                target.authority_key,
                target.artifact_kind))
            fail(PlanError::Code::NonCanonicalTargetSet, "authority repair targets are not strictly artifact-sorted and unique");
        if (!quarantine.contains(target.object))
            fail(PlanError::Code::TargetOutsideQuarantine, "authority repair target is outside the exact quarantine closure");

        if (progress.target_index == 0 || targets[progress.target_index - 1].authority_key != target.authority_key)
        {
            progress.preceding_expectation.reset();
            progress.selected_expectation_sidecar_hash.reset();
        }
        if (target.artifact_kind == AuthorityRepairArtifactKind::SidecarExpectationRecord)
            progress.preceding_expectation = target;
        else if (target.artifact_kind == AuthorityRepairArtifactKind::PersistedTypeReferencesSidecar)
        {
            if (target.expected_canonical_hash == Digest{} && !progress.preceding_expectation)
                fail(
                    PlanError::Code::UnpairedPersistedSidecar,
                    "persisted sidecar repair target has neither an anchored hash nor its expectation-record target");
            if (progress.preceding_expectation
                && (progress.preceding_expectation->object != target.object
                    || progress.preceding_expectation->object_revision != target.object_revision
                    || progress.preceding_expectation->physical_schema_fingerprint != target.physical_schema_fingerprint))
                fail(
                    PlanError::Code::InconsistentSidecarPair,
                    "persisted sidecar repair target identity disagrees with its expectation-record target");
        }

        if (progress.candidate_index >= ordered_candidates.size())
            fail(PlanError::Code::ExactSourceMissing, "authority repair target has no domain-exact source candidate");
        const auto & candidate = ordered_candidates[progress.candidate_index];
        const UInt64 bytes = toUInt64(candidate.canonical_bytes.size());
        const UInt64 provenance = toUInt64(candidate.source_reference.size());
        if (bytes == 0 || provenance == 0)
            fail(PlanError::Code::InvalidCandidateSet, "authority repair candidate identity or payload is invalid");
        admit(
            bytes,
            artifactByteLimit(candidate.artifact_kind, PlanError::Code::InvalidCandidateSet),
            "authority repair candidate artifact exceeds its implementation byte limit");
        admit(provenance, limits.maximum_source_reference_bytes, "authority repair candidate provenance exceeds its byte limit");
        if (progress.candidate_index >= progress.candidate_digests.size()
            || computeCandidateIdentity(candidate) != progress.candidate_digests[progress.candidate_index])
            fail(PlanError::Code::InvalidCandidateSet, "authority repair candidate changed after its exact batch was sealed");
        if (artifactIdentityLess(candidate.authority_key, candidate.artifact_kind, target.authority_key, target.artifact_kind))
            fail(PlanError::Code::UnexpectedCandidate, "authority repair candidate has no exact target artifact");
        if (!sameArtifactIdentity(candidate.authority_key, candidate.artifact_kind, target.authority_key, target.artifact_kind))
            fail(PlanError::Code::ExactSourceMissing, "authority repair target has no domain-exact source candidate");
        if (progress.candidate_index + 1 < ordered_candidates.size()
            && sameArtifactIdentity(
                candidate.authority_key,
                candidate.artifact_kind,
                ordered_candidates[progress.candidate_index + 1].authority_key,
                ordered_candidates[progress.candidate_index + 1].artifact_kind))
            fail(
                PlanError::Code::InvalidCandidateSet,
                "cooperative automatic repair received more than one priority-selected candidate for a target");
        static_cast<void>(sourceRank(candidate.source));
        if (!isValidArtifactKey(candidate.authority_key, candidate.artifact_kind) || candidate.canonical_bytes.empty()
            || candidate.source_reference.empty())
            fail(PlanError::Code::InvalidCandidateSet, "authority repair candidate identity or payload is invalid");
        progress.maximum_source_reference_bytes_seen = std::max(progress.maximum_source_reference_bytes_seen, provenance);
        const UInt64 input_increment = checkedAdd(bytes, provenance, "authority repair candidate input bytes overflow UInt64");
        const UInt64 byte_work = checkedAdd(
            checkedMultiply(bytes, 4, "authority repair candidate byte work overflows UInt64"),
            checkedMultiply(provenance, 2, "authority repair provenance work overflows UInt64"),
            "authority repair candidate work overflows UInt64");
        progress.statistics.work_units = checkedAdd(progress.statistics.work_units, byte_work, "authority repair work overflows UInt64");
        admit(progress.statistics.work_units, limits.maximum_work_units, "authority repair work exceeds its limit");
        progress.statistics.retained_plan_bytes
            = checkedAdd(progress.statistics.retained_plan_bytes, input_increment, "authority repair retained bytes overflow UInt64");
        admit(
            progress.statistics.retained_plan_bytes,
            limits.maximum_retained_plan_bytes,
            "authority repair retained plan exceeds its byte limit");

        Digest expected_hash = target.expected_canonical_hash;
        if (target.artifact_kind == AuthorityRepairArtifactKind::PersistedTypeReferencesSidecar)
        {
            if (progress.selected_expectation_sidecar_hash)
            {
                if (expected_hash != Digest{} && expected_hash != *progress.selected_expectation_sidecar_hash)
                    fail(
                        PlanError::Code::InconsistentSidecarPair,
                        "persisted sidecar repair hash disagrees with its selected exact expectation record");
                expected_hash = *progress.selected_expectation_sidecar_hash;
            }
            if (expected_hash == Digest{})
                fail(
                    PlanError::Code::UnpairedPersistedSidecar,
                    "persisted sidecar repair has no exact expected hash after expectation selection");
        }
        const auto facts = inspectCandidate(candidate, target, expected_hash);
        if (!facts)
            fail(PlanError::Code::ExactSourceMissing, "authority repair target has no domain-exact source candidate");
        ++progress.statistics.candidate_groups;
        ++progress.statistics.exact_candidates;
        if (target.artifact_kind == AuthorityRepairArtifactKind::SidecarExpectationRecord)
        {
            progress.selected_expectation_sidecar_hash = facts->expected_sidecar_hash;
            if (!progress.selected_expectation_sidecar_hash)
                fail(PlanError::Code::InconsistentSidecarPair, "selected expectation source lost its exact sidecar hash");
        }
        progress.selections.push_back({
            .artifact_kind = target.artifact_kind,
            .object = target.object,
            .authority_key = target.authority_key,
            .object_revision = target.object_revision,
            .canonical_hash = facts->canonical_hash,
            .physical_schema_fingerprint = target.physical_schema_fingerprint,
            .source = candidate.source,
            .canonical_bytes = String(candidate.canonical_bytes),
            .source_reference = String(candidate.source_reference),
        });
        ++progress.target_index;
        ++progress.candidate_index;
        ++completed;
    }

    if (progress.target_index != targets.size())
        return {};
    if (progress.candidate_index != ordered_candidates.size())
        fail(PlanError::Code::UnexpectedCandidate, "authority repair candidate has no exact target artifact");
    if (!progress.candidate_batch_digest)
        fail(PlanError::Code::InvalidCandidateSet, "authority repair candidate batch completed without a sealed identity");
    auto result = Ptr(new AuthorityRepairPlan(
        progress.root, progress.manifest, progress.damaged_artifacts, std::move(progress.selections), progress.statistics));
    progress.clear();
    return result;
}

AuthorityRepairPlan::Ptr AuthorityRepairPlan::build(
    const AuthorityRepairAudit & audit,
    std::span<const AuthorityRepairCandidate> ordered_candidates,
    const AuthorityRepairPlanLimits & limits)
{
    validateLimits(limits);
    if (!audit.hasDamage())
        fail(PlanError::Code::EmptyTargetSet, "authority repair audit contains no damaged artifact");
    if (!audit.hasCompleteRepairTargetSet() || !audit.getQuarantinePlan())
        fail(PlanError::Code::IncompleteAuditTargetSet, "authority repair audit contains damage without a complete repairable target set");
    const auto & quarantine = *audit.getQuarantinePlan();
    const auto sorted_unique_complete_targets = audit.getCompleteRepairTargets();
    const UInt64 target_count = toUInt64(sorted_unique_complete_targets.size());
    const UInt64 candidate_count = toUInt64(ordered_candidates.size());
    if (target_count == 0)
        fail(PlanError::Code::EmptyTargetSet, "authority repair requires at least one exact target");
    admit(target_count, limits.maximum_targets, "authority repair target count exceeds its limit");
    admit(candidate_count, limits.maximum_candidates, "authority repair candidate count exceeds its limit");

    UInt64 candidate_bytes = 0;
    UInt64 candidate_provenance_bytes = 0;
    UInt64 maximum_candidate_decode_scratch = 0;
    for (const auto & target : sorted_unique_complete_targets)
        static_cast<void>(artifactByteLimit(target.artifact_kind, PlanError::Code::InvalidTarget));
    for (const auto & candidate : ordered_candidates)
    {
        static_cast<void>(sourceRank(candidate.source));
        if (!isValidArtifactKey(candidate.authority_key, candidate.artifact_kind))
            fail(PlanError::Code::InvalidCandidateSet, "authority repair candidate artifact key is invalid");
        const UInt64 bytes = toUInt64(candidate.canonical_bytes.size());
        const UInt64 provenance = toUInt64(candidate.source_reference.size());
        if (bytes == 0 || provenance == 0)
            fail(PlanError::Code::InvalidCandidateSet, "authority repair candidate bytes and provenance must be nonempty");
        admit(
            bytes,
            artifactByteLimit(candidate.artifact_kind, PlanError::Code::InvalidCandidateSet),
            "authority repair candidate artifact exceeds its implementation byte limit");
        admit(provenance, limits.maximum_source_reference_bytes, "authority repair candidate provenance exceeds its byte limit");
        candidate_bytes = checkedAdd(candidate_bytes, bytes, "authority repair candidate bytes overflow UInt64");
        candidate_provenance_bytes
            = checkedAdd(candidate_provenance_bytes, provenance, "authority repair provenance bytes overflow UInt64");
        maximum_candidate_decode_scratch
            = std::max(maximum_candidate_decode_scratch, candidateDecodeScratchUpperBound(candidate.artifact_kind, bytes));
        static_cast<void>(computeCandidateIdentity(candidate));
    }
    const UInt64 input_item_bytes = checkedAdd(
        checkedMultiply(target_count, target_view_base_canonical_bytes, "authority repair target input bytes overflow UInt64"),
        checkedMultiply(candidate_count, candidate_view_base_canonical_bytes, "authority repair candidate input bytes overflow UInt64"),
        "authority repair fixed input bytes overflow UInt64");
    const UInt64 input_bytes = checkedAdd(
        input_item_bytes,
        checkedAdd(candidate_bytes, candidate_provenance_bytes, "authority repair input bytes overflow UInt64"),
        "authority repair input bytes overflow UInt64");
    admit(input_bytes, limits.maximum_total_input_bytes, "authority repair input bytes exceed their aggregate limit");

    const UInt64 item_work = checkedMultiply(
        checkedAdd(target_count, candidate_count, "authority repair item count overflows UInt64"),
        8,
        "authority repair item work overflows UInt64");
    UInt64 membership_comparisons_per_target = 1;
    for (UInt64 closure_items = toUInt64(quarantine.getQuarantinedObjects().size()); closure_items != 0; closure_items >>= 1)
        membership_comparisons_per_target
            = checkedAdd(membership_comparisons_per_target, 1, "authority repair membership work overflows UInt64");
    const UInt64 membership_work
        = checkedMultiply(target_count, membership_comparisons_per_target, "authority repair membership work overflows UInt64");
    const UInt64 byte_work = checkedAdd(
        checkedMultiply(candidate_bytes, 5, "authority repair candidate byte work overflows UInt64"),
        checkedMultiply(candidate_provenance_bytes, 3, "authority repair provenance work overflows UInt64"),
        "authority repair candidate work overflows UInt64");
    const UInt64 work_units = checkedAdd(
        checkedAdd(item_work, membership_work, "authority repair work overflows UInt64"),
        byte_work,
        "authority repair work overflows UInt64");
    admit(work_units, limits.maximum_work_units, "authority repair work exceeds its limit");

    const UInt64 fixed_retained_plan_bytes = checkedAdd(
        repair_plan_base_canonical_bytes,
        checkedMultiply(target_count, selection_base_canonical_bytes, "authority repair retained bytes overflow UInt64"),
        "authority repair retained bytes overflow UInt64");
    admit(fixed_retained_plan_bytes, limits.maximum_retained_plan_bytes, "authority repair fixed plan retention exceeds its byte limit");
    const UInt64 scratch_bytes = checkedAdd(
        checkedMultiply(target_count, sizeof(size_t) + sizeof(CandidateFacts), "authority repair selection scratch bytes overflow UInt64"),
        maximum_candidate_decode_scratch,
        "authority repair selection scratch bytes overflow UInt64");
    admit(scratch_bytes, limits.maximum_scratch_bytes, "authority repair selection scratch exceeds its byte limit");

    const UUID & database_uuid = quarantine.getRoot().authority_root.database_uuid;
    const AuthorityRepairTarget * preceding_expectation = nullptr;
    for (size_t index = 0; index < sorted_unique_complete_targets.size(); ++index)
    {
        const auto & target = sorted_unique_complete_targets[index];
        validateTargetMapping(target, database_uuid);
        if (index != 0
            && !artifactIdentityLess(
                sorted_unique_complete_targets[index - 1].authority_key,
                sorted_unique_complete_targets[index - 1].artifact_kind,
                target.authority_key,
                target.artifact_kind))
            fail(PlanError::Code::NonCanonicalTargetSet, "authority repair targets are not strictly artifact-sorted and unique");
        if (!quarantine.contains(target.object))
            fail(PlanError::Code::TargetOutsideQuarantine, "authority repair target is outside the exact quarantine closure");

        if (index == 0 || sorted_unique_complete_targets[index - 1].authority_key != target.authority_key)
            preceding_expectation = nullptr;
        if (target.artifact_kind == AuthorityRepairArtifactKind::SidecarExpectationRecord)
            preceding_expectation = &target;
        else if (target.artifact_kind == AuthorityRepairArtifactKind::PersistedTypeReferencesSidecar)
        {
            if (target.expected_canonical_hash == Digest{} && preceding_expectation == nullptr)
                fail(
                    PlanError::Code::UnpairedPersistedSidecar,
                    "persisted sidecar repair target has neither an anchored hash nor its expectation-record target");
            if (preceding_expectation
                && (preceding_expectation->object != target.object || preceding_expectation->object_revision != target.object_revision
                    || preceding_expectation->physical_schema_fingerprint != target.physical_schema_fingerprint))
                fail(
                    PlanError::Code::InconsistentSidecarPair,
                    "persisted sidecar repair target identity disagrees with its expectation-record target");
        }
    }

    for (size_t index = 0; index < ordered_candidates.size(); ++index)
    {
        const auto & candidate = ordered_candidates[index];
        const UInt8 rank = sourceRank(candidate.source);
        if (index == 0)
            continue;
        const auto & previous = ordered_candidates[index - 1];
        if (artifactIdentityLess(candidate.authority_key, candidate.artifact_kind, previous.authority_key, previous.artifact_kind))
            fail(PlanError::Code::InvalidCandidateSet, "authority repair candidates are not artifact-ordered");
        if (sameArtifactIdentity(previous.authority_key, previous.artifact_kind, candidate.authority_key, candidate.artifact_kind)
            && sourceRank(previous.source) > rank)
            fail(PlanError::Code::InvalidCandidateSet, "authority repair candidates are not source-priority ordered");
    }

    constexpr size_t no_selection = std::numeric_limits<size_t>::max();
    std::vector<size_t> selected_candidate_indexes(sorted_unique_complete_targets.size(), no_selection);
    std::vector<CandidateFacts> selected_candidate_facts(sorted_unique_complete_targets.size());
    AuthorityRepairPlanStatistics statistics{
        .targets = target_count,
        .candidates = candidate_count,
        .candidate_groups = 0,
        .exact_candidates = 0,
        .input_bytes = input_bytes,
        .work_units = work_units,
        .scratch_bytes = scratch_bytes,
        .retained_plan_bytes = 0,
    };

    size_t candidate_index = 0;
    std::optional<Digest> selected_expectation_sidecar_hash;
    for (size_t target_index = 0; target_index < sorted_unique_complete_targets.size(); ++target_index)
    {
        const auto & target = sorted_unique_complete_targets[target_index];
        if (target_index == 0 || sorted_unique_complete_targets[target_index - 1].authority_key != target.authority_key)
            selected_expectation_sidecar_hash.reset();

        Digest expected_hash = target.expected_canonical_hash;
        if (target.artifact_kind == AuthorityRepairArtifactKind::PersistedTypeReferencesSidecar)
        {
            if (selected_expectation_sidecar_hash)
            {
                if (expected_hash != Digest{} && expected_hash != *selected_expectation_sidecar_hash)
                    fail(
                        PlanError::Code::InconsistentSidecarPair,
                        "persisted sidecar repair hash disagrees with its selected exact expectation record");
                expected_hash = *selected_expectation_sidecar_hash;
            }
            if (expected_hash == Digest{})
                fail(
                    PlanError::Code::UnpairedPersistedSidecar,
                    "persisted sidecar repair has no exact expected hash after expectation selection");
        }

        if (candidate_index < ordered_candidates.size()
            && artifactIdentityLess(
                ordered_candidates[candidate_index].authority_key,
                ordered_candidates[candidate_index].artifact_kind,
                target.authority_key,
                target.artifact_kind))
            fail(PlanError::Code::UnexpectedCandidate, "authority repair candidate has no exact target artifact");

        while (candidate_index < ordered_candidates.size()
               && sameArtifactIdentity(
                   ordered_candidates[candidate_index].authority_key,
                   ordered_candidates[candidate_index].artifact_kind,
                   target.authority_key,
                   target.artifact_kind))
        {
            const size_t group_begin = candidate_index;
            const auto & first = ordered_candidates[group_begin];
            const UInt8 rank = sourceRank(first.source);
            ++candidate_index;
            while (candidate_index < ordered_candidates.size()
                   && sameArtifactIdentity(
                       ordered_candidates[candidate_index].authority_key,
                       ordered_candidates[candidate_index].artifact_kind,
                       first.authority_key,
                       first.artifact_kind)
                   && sourceRank(ordered_candidates[candidate_index].source) == rank)
            {
                if (!sameCandidate(first, ordered_candidates[candidate_index]))
                    fail(PlanError::Code::ConflictingCandidate, "authority repair source contains conflicting duplicate candidates");
                ++candidate_index;
            }
            ++statistics.candidate_groups;

            const auto facts = inspectCandidate(first, target, expected_hash);
            if (!facts)
                continue;
            ++statistics.exact_candidates;
            if (selected_candidate_indexes[target_index] == no_selection)
            {
                selected_candidate_indexes[target_index] = group_begin;
                selected_candidate_facts[target_index] = *facts;
            }
        }

        if (selected_candidate_indexes[target_index] == no_selection)
            fail(PlanError::Code::ExactSourceMissing, "authority repair target has no domain-exact source candidate");
        if (target.artifact_kind == AuthorityRepairArtifactKind::SidecarExpectationRecord)
        {
            selected_expectation_sidecar_hash = selected_candidate_facts[target_index].expected_sidecar_hash;
            if (!selected_expectation_sidecar_hash)
                fail(PlanError::Code::InconsistentSidecarPair, "selected expectation source lost its exact sidecar hash");
        }
    }
    if (candidate_index != ordered_candidates.size())
        fail(PlanError::Code::UnexpectedCandidate, "authority repair candidate has no exact target artifact");

    UInt64 retained_plan_bytes = fixed_retained_plan_bytes;
    for (size_t index = 0; index < sorted_unique_complete_targets.size(); ++index)
    {
        const auto & candidate = ordered_candidates[selected_candidate_indexes[index]];
        retained_plan_bytes = checkedAdd(
            retained_plan_bytes, toUInt64(candidate.canonical_bytes.size()), "authority repair retained bytes overflow UInt64");
        retained_plan_bytes = checkedAdd(
            retained_plan_bytes, toUInt64(candidate.source_reference.size()), "authority repair retained bytes overflow UInt64");
    }
    admit(retained_plan_bytes, limits.maximum_retained_plan_bytes, "authority repair retained plan exceeds its byte limit");
    statistics.retained_plan_bytes = retained_plan_bytes;

    std::vector<AuthorityRepairSelection> selections;
    selections.reserve(sorted_unique_complete_targets.size());
    for (size_t index = 0; index < sorted_unique_complete_targets.size(); ++index)
    {
        const auto & target = sorted_unique_complete_targets[index];
        const auto & candidate = ordered_candidates[selected_candidate_indexes[index]];
        selections.push_back({
            .artifact_kind = target.artifact_kind,
            .object = target.object,
            .authority_key = target.authority_key,
            .object_revision = target.object_revision,
            .canonical_hash = selected_candidate_facts[index].canonical_hash,
            .physical_schema_fingerprint = target.physical_schema_fingerprint,
            .source = candidate.source,
            .canonical_bytes = String(candidate.canonical_bytes),
            .source_reference = String(candidate.source_reference),
        });
    }
    return Ptr(new AuthorityRepairPlan(
        audit.getRoot(), audit.getDamagedArtifactManifestDigest(), audit.getDamagedArtifactCount(), std::move(selections), statistics));
}

}
