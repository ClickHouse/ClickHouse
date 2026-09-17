#include <Databases/UDT/AuthorityRepairAudit.h>

#include <Databases/DatabaseSchemaWAL.h>
#include <Databases/UDT/AuthorityRoot.h>

#include <DataTypes/UDT/AuthorityState.h>
#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/PersistedTypeReferences.h>
#include <DataTypes/UDT/Record.h>
#include <DataTypes/UDT/ResourceLimits.h>
#include <DataTypes/UDT/SidecarExpectationRecord.h>

#include <Common/Stopwatch.h>

#include <algorithm>
#include <array>
#include <limits>
#include <optional>
#include <set>
#include <utility>

namespace DB::UDT
{
namespace
{

using AuditError = AuthorityRepairAuditError;

constexpr UInt64 maximum_audit_observations = 600'000;
constexpr UInt64 maximum_audit_findings = 600'000;
constexpr UInt64 maximum_audit_observed_bytes = 1ULL << 30;
constexpr UInt64 maximum_audit_work_units = 4ULL << 30;
constexpr UInt64 maximum_audit_scratch_bytes = 128ULL << 20;
constexpr UInt64 maximum_audit_retained_canonical_bytes = 256ULL << 20;
constexpr UInt64 maximum_canonical_metadata_bytes = 16ULL << 20;
constexpr UInt64 maximum_installation_record_bytes = 16ULL << 20;
constexpr UInt64 maximum_installation_object_name_bytes = 4ULL << 10;
constexpr UInt64 maximum_object_verifier_work_units = 67'108'864;
constexpr UInt64 maximum_object_verifier_transient_bytes = 1ULL << 30;
constexpr UInt64 rooted_canonical_artifact_work_multiplier = 4;
constexpr UInt64 observed_canonical_artifact_work_multiplier = 8;
constexpr UInt64 canonical_uuid_bytes = 16;
constexpr UInt64 schema_object_identity_canonical_bytes = sizeof(UInt8) + 2 * canonical_uuid_bytes;
constexpr UInt64 inventory_key_canonical_bytes = sizeof(UInt16) + sizeof(UInt8) + canonical_uuid_bytes;
/// V1 has 38 key nibbles and at most 16 canonical children per radix branch.
/// Indexed lookup validates a branch once and scans it once more to descend.
constexpr UInt64 authority_inventory_index_lookup_work_units = 2 * inventory_key_canonical_bytes * (2 * 16 + 8);
constexpr UInt64 repair_target_canonical_bytes
    = sizeof(UInt8) + schema_object_identity_canonical_bytes + inventory_key_canonical_bytes + sizeof(UInt64) + 2 * sizeof(Digest);
constexpr UInt64 repair_finding_canonical_bytes = 2 * sizeof(UInt8) + schema_object_identity_canonical_bytes + inventory_key_canonical_bytes
    + 3 * sizeof(UInt64) + 5 * sizeof(Digest) + 2 * sizeof(UInt8);
constexpr UInt64 repair_audit_base_canonical_bytes = canonical_uuid_bytes + 17 * sizeof(UInt64) + 4 * sizeof(Digest);
constexpr UInt64 resumable_set_item_scratch_bytes = sizeof(SchemaObjectID) + 6 * sizeof(void *);
constexpr std::string_view observed_artifact_digest_domain = "ClickHouse UDT observed authority artifact V1";
constexpr std::string_view observed_object_image_digest_domain = "ClickHouse UDT observed schema object image V1";
constexpr std::string_view damaged_artifact_manifest_domain = "ClickHouse UDT damaged authority artifact manifest V1";

[[noreturn]] void fail(AuditError::Code code, std::string_view message)
{
    throw AuditError(code, message);
}

UInt64 toUInt64(size_t value) noexcept
{
    static_assert(sizeof(size_t) <= sizeof(UInt64));
    return static_cast<UInt64>(value);
}

UInt64 checkedAdd(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        fail(AuditError::Code::ArithmeticOverflow, message);
    return lhs + rhs;
}

UInt64 checkedMultiply(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (lhs != 0 && rhs > std::numeric_limits<UInt64>::max() / lhs)
        fail(AuditError::Code::ArithmeticOverflow, message);
    return lhs * rhs;
}

bool isZeroDigest(const Digest & digest) noexcept
{
    return std::all_of(digest.begin(), digest.end(), [](CanonicalByte byte) { return byte == 0; });
}

bool isEmptySchemaObjectID(const SchemaObjectID & object) noexcept
{
    return object == SchemaObjectID{};
}

bool isKnownObservationState(AuthorityRepairObservationState state) noexcept
{
    return state == AuthorityRepairObservationState::Missing || state == AuthorityRepairObservationState::Present;
}

bool isKnownAuditArtifactKind(AuthorityRepairAuditArtifactKind kind) noexcept
{
    switch (kind)
    {
        case AuthorityRepairAuditArtifactKind::DefinitionRecord:
        case AuthorityRepairAuditArtifactKind::SidecarExpectationRecord:
        case AuthorityRepairAuditArtifactKind::PersistedTypeReferencesSidecar:
        case AuthorityRepairAuditArtifactKind::DependentObjectMetadataInstallationRecord:
        case AuthorityRepairAuditArtifactKind::DependentObjectMetadata:
        case AuthorityRepairAuditArtifactKind::StoredObjectImage: return true;
    }
    return false;
}

bool isSidecarObjectKind(SchemaObjectKind kind) noexcept
{
    return kind == SchemaObjectKind::Table || kind == SchemaObjectKind::View || kind == SchemaObjectKind::Dictionary
        || kind == SchemaObjectKind::SyntheticTestObject;
}

UInt64 sortWorkBound(UInt64 items)
{
    UInt64 levels = 0;
    for (UInt64 remaining = items; remaining > 1; remaining = (remaining + 1) / 2)
        ++levels;
    return checkedMultiply(
        items,
        checkedAdd(levels, 1, "authority repair-audit sort work overflows UInt64"),
        "authority repair-audit sort work overflows UInt64");
}

void validateObjectVerifierLimits(const AuthorityIntegrityVerifierLimits & limits)
{
    constexpr AuthorityStateLimits authority_maxima;
    constexpr PersistedTypeReferencesLimits persisted_maxima;
    constexpr RecordLimits record_maxima;
    constexpr SchemaObjectDependencyGraphLimits graph_maxima;
    const auto valid = [](UInt64 value, UInt64 maximum) { return value != 0 && value <= maximum; };

    if (!valid(limits.authority_state.maximum_leaves, authority_maxima.maximum_leaves)
        || !valid(limits.authority_state.maximum_encoded_bytes, authority_maxima.maximum_encoded_bytes))
        fail(AuditError::Code::InvalidConfiguration, "authority repair-audit state limits are invalid");

    const auto & persisted = limits.persisted_references;
    if (!valid(persisted.maximum_sidecar_bytes, persisted_maxima.maximum_sidecar_bytes)
        || !valid(persisted.maximum_descriptors, persisted_maxima.maximum_descriptors)
        || !valid(persisted.maximum_occurrence_paths, persisted_maxima.maximum_occurrence_paths)
        || !valid(persisted.maximum_path_depth, persisted_maxima.maximum_path_depth)
        || !valid(persisted.maximum_canonical_arguments_bytes, persisted_maxima.maximum_canonical_arguments_bytes)
        || !valid(persisted.maximum_canonical_physical_type_bytes, persisted_maxima.maximum_canonical_physical_type_bytes)
        || !valid(persisted.maximum_qualified_name_bytes, persisted_maxima.maximum_qualified_name_bytes)
        || !valid(persisted.maximum_text_bytes, persisted_maxima.maximum_text_bytes))
        fail(AuditError::Code::InvalidConfiguration, "authority repair-audit sidecar limits are invalid");

    const auto & record = limits.definition_record;
    if (!valid(record.maximum_record_bytes, record_maxima.maximum_record_bytes)
        || !valid(record.maximum_name_bytes, record_maxima.maximum_name_bytes)
        || !valid(record.maximum_parameter_count, record_maxima.maximum_parameter_count)
        || !valid(record.maximum_parameter_name_bytes, record_maxima.maximum_parameter_name_bytes)
        || !valid(record.maximum_canonical_sql_bytes, record_maxima.maximum_canonical_sql_bytes)
        || !valid(record.maximum_template_ir_bytes, record_maxima.maximum_template_ir_bytes)
        || !valid(record.maximum_dependency_count, record_maxima.maximum_dependency_count)
        || !valid(record.maximum_checker_certificate_bytes, record_maxima.maximum_checker_certificate_bytes)
        || !valid(record.maximum_owner_display_name_bytes, record_maxima.maximum_owner_display_name_bytes)
        || !valid(record.maximum_comment_bytes, record_maxima.maximum_comment_bytes))
        fail(AuditError::Code::InvalidConfiguration, "authority repair-audit definition-record limits are invalid");

    const UInt64 maximum_canonical_bytes = getResourceImplementationLimits().get(ResourceLimit::DeterministicCatalogBytesPerDatabase);
    if (!valid(limits.maximum_required_definitions, persisted_maxima.maximum_descriptors)
        || !valid(limits.maximum_outgoing_dependencies, graph_maxima.maximum_edges_per_node)
        || !valid(limits.maximum_canonical_bytes_hashed, maximum_canonical_bytes)
        || !valid(limits.maximum_work_units, maximum_object_verifier_work_units)
        || !valid(limits.maximum_transient_bytes, maximum_object_verifier_transient_bytes))
        fail(AuditError::Code::InvalidConfiguration, "authority repair-audit object-verifier limits are invalid");
}

void validateQuarantineLimits(const AuthorityQuarantinePlanLimits & limits)
{
    constexpr AuthorityQuarantinePlanLimits maxima;
    const auto valid = [](UInt64 value, UInt64 maximum) { return value != 0 && value <= maximum; };
    if (!valid(limits.maximum_seed_objects, maxima.maximum_seed_objects)
        || !valid(limits.maximum_closure_objects, maxima.maximum_closure_objects)
        || !valid(limits.maximum_reverse_edges_per_object, maxima.maximum_reverse_edges_per_object)
        || !valid(limits.maximum_walked_edges, maxima.maximum_walked_edges) || !valid(limits.maximum_work_units, maxima.maximum_work_units)
        || !valid(limits.maximum_retained_canonical_bytes, maxima.maximum_retained_canonical_bytes)
        || limits.maximum_seed_objects > limits.maximum_closure_objects
        || limits.maximum_reverse_edges_per_object > limits.maximum_walked_edges)
        fail(AuditError::Code::InvalidConfiguration, "authority repair-audit quarantine limits are invalid");
}

void validateLimits(const AuthorityRepairAuditLimits & limits)
{
    validateObjectVerifierLimits(limits.object_verifier);
    validateQuarantineLimits(limits.quarantine);
    const auto valid = [](UInt64 value, UInt64 maximum) { return value != 0 && value <= maximum; };
    if (!valid(limits.installation_record.maximum_encoded_bytes, maximum_installation_record_bytes)
        || !valid(limits.installation_record.maximum_object_name_bytes, maximum_installation_object_name_bytes)
        || !valid(limits.maximum_canonical_metadata_bytes, maximum_canonical_metadata_bytes)
        || !valid(limits.maximum_observations, maximum_audit_observations) || !valid(limits.maximum_findings, maximum_audit_findings)
        || !valid(limits.maximum_total_observed_bytes, maximum_audit_observed_bytes)
        || !valid(limits.maximum_work_units, maximum_audit_work_units) || !valid(limits.maximum_scratch_bytes, maximum_audit_scratch_bytes)
        || !valid(limits.maximum_retained_canonical_bytes, maximum_audit_retained_canonical_bytes)
        || limits.maximum_findings > limits.maximum_observations)
        fail(AuditError::Code::InvalidConfiguration, "authority repair-audit aggregate limits are invalid");
}

class AuditBudget final
{
public:
    AuditBudget(const AuthorityRepairAuditLimits & limits_, AuthorityRepairAuditStatistics & statistics_)
        : limits(limits_)
        , statistics(statistics_)
    {
        if (statistics.work_units > limits.maximum_work_units || statistics.observed_bytes > limits.maximum_total_observed_bytes
            || statistics.scratch_bytes > limits.maximum_scratch_bytes
            || statistics.retained_canonical_bytes > limits.maximum_retained_canonical_bytes)
            fail(AuditError::Code::LimitExceeded, "authority repair audit continuation exceeds its current effective limits");
        checkControl();
    }

    void checkControl() const
    {
        if (control_suppressed)
            return;
        if (limits.cancellation.stop_requested())
            fail(AuditError::Code::ExecutionBudgetExceeded, "authority repair audit was cancelled");
        if (limits.monotonic_deadline && std::chrono::steady_clock::now() >= *limits.monotonic_deadline)
            fail(AuditError::Code::ExecutionBudgetExceeded, "authority repair audit exceeded its wall-time budget");
        if (limits.thread_cpu_deadline_nanoseconds && clock_gettime_ns(CLOCK_THREAD_CPUTIME_ID) >= *limits.thread_cpu_deadline_nanoseconds)
            fail(AuditError::Code::ExecutionBudgetExceeded, "authority repair audit exceeded its CPU-time budget");
    }

    void chargeWork(UInt64 amount)
    {
        checkControl();
        statistics.work_units
            = admit(statistics.work_units, amount, limits.maximum_work_units, "authority repair audit exceeds its work limit");
    }

    UInt64 availableWork() const noexcept { return limits.maximum_work_units - statistics.work_units; }

    /// One bounded artifact/root lookup may finish atomically once admitted.
    /// Its cursor is committed before endAtomicUnit() performs the post-work
    /// wall/CPU/cancellation checkpoint.
    void beginAtomicUnit()
    {
        checkControl();
        control_suppressed = true;
    }

    void endAtomicUnit()
    {
        control_suppressed = false;
        checkControl();
    }

    void abandonAtomicUnit() noexcept { control_suppressed = false; }

    void chargeObservedBytes(UInt64 amount)
    {
        checkControl();
        statistics.observed_bytes = admit(
            statistics.observed_bytes,
            amount,
            limits.maximum_total_observed_bytes,
            "authority repair audit exceeds its observed-byte limit");
    }

    void admitScratch(UInt64 amount)
    {
        checkControl();
        if (amount > limits.maximum_scratch_bytes)
            fail(AuditError::Code::LimitExceeded, "authority repair audit exceeds its scratch-byte limit");
        statistics.scratch_bytes = std::max(statistics.scratch_bytes, amount);
    }

    void retain(UInt64 amount)
    {
        checkControl();
        statistics.retained_canonical_bytes = admit(
            statistics.retained_canonical_bytes,
            amount,
            limits.maximum_retained_canonical_bytes,
            "authority repair audit exceeds its retained canonical-byte limit");
    }

    void chargeManifestBytes(UInt64 amount)
    {
        statistics.manifest_bytes_hashed
            = checkedAdd(statistics.manifest_bytes_hashed, amount, "authority repair-audit manifest bytes overflow UInt64");
        chargeWork(amount);
    }

private:
    static UInt64 admit(UInt64 current, UInt64 amount, UInt64 maximum, std::string_view message)
    {
        if (current > maximum || amount > maximum - current)
            fail(AuditError::Code::LimitExceeded, message);
        return current + amount;
    }

    const AuthorityRepairAuditLimits & limits;
    AuthorityRepairAuditStatistics & statistics;
    bool control_suppressed = false;
};

class AtomicAuditUnit final
{
public:
    explicit AtomicAuditUnit(AuditBudget & budget_)
        : budget(budget_)
    {
        budget.beginAtomicUnit();
    }

    AtomicAuditUnit(const AtomicAuditUnit &) = delete;
    AtomicAuditUnit & operator=(const AtomicAuditUnit &) = delete;

    ~AtomicAuditUnit()
    {
        if (active)
            budget.abandonAtomicUnit();
    }

    void seal()
    {
        active = false;
        budget.endAtomicUnit();
    }

private:
    AuditBudget & budget;
    bool active = true;
};

UInt64 observationByteLimit(AuthorityRepairAuditArtifactKind kind, const AuthorityRepairAuditLimits & limits)
{
    switch (kind)
    {
        case AuthorityRepairAuditArtifactKind::DefinitionRecord: return limits.object_verifier.definition_record.maximum_record_bytes;
        case AuthorityRepairAuditArtifactKind::SidecarExpectationRecord: return toUInt64(sidecar_expectation_record_extended_encoded_bytes);
        case AuthorityRepairAuditArtifactKind::PersistedTypeReferencesSidecar:
            return limits.object_verifier.persisted_references.maximum_sidecar_bytes;
        case AuthorityRepairAuditArtifactKind::DependentObjectMetadataInstallationRecord:
            return limits.installation_record.maximum_encoded_bytes;
        case AuthorityRepairAuditArtifactKind::DependentObjectMetadata: return limits.maximum_canonical_metadata_bytes;
        case AuthorityRepairAuditArtifactKind::StoredObjectImage: return 0;
    }
    fail(AuditError::Code::InvalidObservation, "authority repair observation has an unknown artifact kind");
}

void validateObservationShape(const AuthorityRepairObservation & observation, const AuthorityRepairAuditLimits & limits)
{
    if (!isKnownAuditArtifactKind(observation.artifact_kind) || !isKnownObservationState(observation.state))
        fail(AuditError::Code::InvalidObservation, "authority repair observation has an unknown tag");
    if (observation.authority_key.format_version != authority_inventory_format_version
        || observation.authority_key.object_uuid == UUIDHelpers::Nil)
        fail(AuditError::Code::InvalidObservation, "authority repair observation has an invalid authority key");

    if (observation.artifact_kind == AuthorityRepairAuditArtifactKind::StoredObjectImage)
    {
        if (!observation.artifact_bytes.empty())
            fail(AuditError::Code::InvalidObservation, "stored-object image observation cannot contain canonical artifact bytes");
        if (observation.state == AuthorityRepairObservationState::Missing
            && (!isEmptySchemaObjectID(observation.object) || observation.object_schema_revision != 0
                || !isZeroDigest(observation.physical_schema_fingerprint)))
            fail(AuditError::Code::InvalidObservation, "missing stored-object image observation contains an image value");
        if (observation.state == AuthorityRepairObservationState::Present
            && (!observation.object.isValid() || !isSidecarObjectKind(observation.object.kind)))
            fail(AuditError::Code::InvalidObservation, "present stored-object image observation has an invalid identity");
        return;
    }

    if (!isEmptySchemaObjectID(observation.object) || observation.object_schema_revision != 0
        || !isZeroDigest(observation.physical_schema_fingerprint))
        fail(AuditError::Code::InvalidObservation, "authority-record observation contains stored-object image fields");
    if (observation.state == AuthorityRepairObservationState::Missing && !observation.artifact_bytes.empty())
        fail(AuditError::Code::InvalidObservation, "missing authority artifact observation contains bytes");
    if (toUInt64(observation.artifact_bytes.size()) > observationByteLimit(observation.artifact_kind, limits))
        fail(AuditError::Code::LimitExceeded, "authority repair observation exceeds its artifact byte limit");
}

AuthorityRepairArtifactKind toRepairArtifactKind(AuthorityRepairAuditArtifactKind kind)
{
    switch (kind)
    {
        case AuthorityRepairAuditArtifactKind::DefinitionRecord: return AuthorityRepairArtifactKind::DefinitionRecord;
        case AuthorityRepairAuditArtifactKind::SidecarExpectationRecord: return AuthorityRepairArtifactKind::SidecarExpectationRecord;
        case AuthorityRepairAuditArtifactKind::PersistedTypeReferencesSidecar:
            return AuthorityRepairArtifactKind::PersistedTypeReferencesSidecar;
        case AuthorityRepairAuditArtifactKind::DependentObjectMetadataInstallationRecord:
        case AuthorityRepairAuditArtifactKind::DependentObjectMetadata:
        case AuthorityRepairAuditArtifactKind::StoredObjectImage:
            fail(AuditError::Code::InvalidObservation, "diagnostic-only artifact cannot become an authority repair target");
    }
    fail(AuditError::Code::InvalidObservation, "authority repair observation has an unknown artifact kind");
}

struct ExpectedArtifact
{
    AuthorityRepairAuditArtifactKind artifact_kind{};
    SchemaObjectID object;
    AuthorityInventoryKey authority_key;
    UInt64 object_revision = 0;
    Digest canonical_hash{};
    Digest physical_schema_fingerprint{};
};

AuthorityRepairTarget makeRepairTarget(const ExpectedArtifact & expected)
{
    return {
        .artifact_kind = toRepairArtifactKind(expected.artifact_kind),
        .object = expected.object,
        .authority_key = expected.authority_key,
        .object_revision = expected.object_revision,
        .expected_canonical_hash = expected.canonical_hash,
        .physical_schema_fingerprint = expected.physical_schema_fingerprint,
    };
}

bool isRepairableArtifact(AuthorityRepairAuditArtifactKind kind) noexcept
{
    return kind == AuthorityRepairAuditArtifactKind::DefinitionRecord || kind == AuthorityRepairAuditArtifactKind::SidecarExpectationRecord
        || kind == AuthorityRepairAuditArtifactKind::PersistedTypeReferencesSidecar;
}

Digest hashObservedObjectImage(const SchemaObjectID & object, UInt64 revision, const Digest & fingerprint)
{
    CanonicalHasher hasher(observed_object_image_digest_domain);
    const std::array<CanonicalByte, 1> kind{static_cast<CanonicalByte>(object.kind)};
    hasher.update(kind);
    hasher.updateUUID(object.database_uuid);
    hasher.updateUUID(object.object_uuid);
    std::array<CanonicalByte, sizeof(UInt64)> revision_bytes{};
    for (size_t index = 0; index < revision_bytes.size(); ++index)
        revision_bytes[index] = static_cast<CanonicalByte>(revision >> (8 * index));
    hasher.update(revision_bytes);
    hasher.update(fingerprint);
    return hasher.finalize();
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

void updateObject(CanonicalHasher & hasher, const SchemaObjectID & object)
{
    updateUInt8(hasher, static_cast<UInt8>(object.kind));
    hasher.updateUUID(object.database_uuid);
    hasher.updateUUID(object.object_uuid);
}

void updateInventoryKey(CanonicalHasher & hasher, const AuthorityInventoryKey & key)
{
    updateUInt16(hasher, key.format_version);
    updateUInt8(hasher, static_cast<UInt8>(key.record_kind));
    hasher.updateUUID(key.object_uuid);
}

constexpr UInt64 manifest_header_bytes = 2 * sizeof(UInt16) + canonical_uuid_bytes + 6 * sizeof(UInt64) + 3 * sizeof(Digest);
constexpr UInt64 manifest_count_bytes = sizeof(UInt64);
constexpr UInt64 manifest_object_bytes = schema_object_identity_canonical_bytes;
constexpr UInt64 manifest_finding_bytes = 2 * sizeof(UInt8) + schema_object_identity_canonical_bytes + inventory_key_canonical_bytes
    + 3 * sizeof(UInt64) + 4 * sizeof(Digest) + 2 * sizeof(UInt8);
constexpr UInt64 manifest_target_bytes
    = sizeof(UInt8) + schema_object_identity_canonical_bytes + inventory_key_canonical_bytes + sizeof(UInt64) + 2 * sizeof(Digest);
constexpr std::string_view repair_observation_batch_domain = "ClickHouse UDT repair observation batch V1";
constexpr std::string_view repair_observation_identity_domain = "ClickHouse UDT repair observation identity V1";
constexpr std::string_view repair_preserved_seed_batch_domain = "ClickHouse UDT repair preserved quarantine seeds V1";
constexpr std::string_view repair_audit_resource_limits_domain = "ClickHouse UDT repair audit resource limits V1";

Digest computeRepairObservationIdentity(const AuthorityRepairObservation & observation)
{
    CanonicalHasher hasher(repair_observation_identity_domain);
    updateUInt8(hasher, static_cast<UInt8>(observation.artifact_kind));
    updateInventoryKey(hasher, observation.authority_key);
    updateUInt8(hasher, static_cast<UInt8>(observation.state));
    updateUInt64(hasher, toUInt64(observation.artifact_bytes.size()));
    hasher.update(observation.artifact_bytes);
    updateObject(hasher, observation.object);
    updateUInt64(hasher, observation.object_schema_revision);
    hasher.update(observation.physical_schema_fingerprint);
    return hasher.finalize();
}

Digest computeRepairAuditResourceLimitsIdentity(const AuthorityRepairAuditLimits & limits)
{
    CanonicalHasher hasher(repair_audit_resource_limits_domain);
    const auto add = [&](UInt64 value) { updateUInt64(hasher, value); };
    add(limits.object_verifier.authority_state.maximum_leaves);
    add(limits.object_verifier.authority_state.maximum_encoded_bytes);
    add(limits.object_verifier.persisted_references.maximum_sidecar_bytes);
    add(limits.object_verifier.persisted_references.maximum_descriptors);
    add(limits.object_verifier.persisted_references.maximum_occurrence_paths);
    add(limits.object_verifier.persisted_references.maximum_path_depth);
    add(limits.object_verifier.persisted_references.maximum_canonical_arguments_bytes);
    add(limits.object_verifier.persisted_references.maximum_canonical_physical_type_bytes);
    add(limits.object_verifier.persisted_references.maximum_qualified_name_bytes);
    add(limits.object_verifier.persisted_references.maximum_text_bytes);
    add(limits.object_verifier.definition_record.maximum_record_bytes);
    add(limits.object_verifier.definition_record.maximum_name_bytes);
    add(limits.object_verifier.definition_record.maximum_parameter_count);
    add(limits.object_verifier.definition_record.maximum_parameter_name_bytes);
    add(limits.object_verifier.definition_record.maximum_canonical_sql_bytes);
    add(limits.object_verifier.definition_record.maximum_template_ir_bytes);
    add(limits.object_verifier.definition_record.maximum_dependency_count);
    add(limits.object_verifier.definition_record.maximum_checker_certificate_bytes);
    add(limits.object_verifier.definition_record.maximum_owner_display_name_bytes);
    add(limits.object_verifier.definition_record.maximum_comment_bytes);
    add(limits.object_verifier.maximum_required_definitions);
    add(limits.object_verifier.maximum_outgoing_dependencies);
    add(limits.object_verifier.maximum_canonical_bytes_hashed);
    add(limits.object_verifier.maximum_work_units);
    add(limits.object_verifier.maximum_transient_bytes);
    add(limits.quarantine.maximum_seed_objects);
    add(limits.quarantine.maximum_closure_objects);
    add(limits.quarantine.maximum_reverse_edges_per_object);
    add(limits.quarantine.maximum_walked_edges);
    add(limits.quarantine.maximum_work_units);
    add(limits.quarantine.maximum_retained_canonical_bytes);
    add(limits.installation_record.maximum_encoded_bytes);
    add(limits.installation_record.maximum_object_name_bytes);
    add(limits.maximum_canonical_metadata_bytes);
    add(limits.maximum_observations);
    add(limits.maximum_findings);
    add(limits.maximum_total_observed_bytes);
    add(limits.maximum_work_units);
    add(limits.maximum_scratch_bytes);
    add(limits.maximum_retained_canonical_bytes);
    return hasher.finalize();
}

UInt64 quarantineLookupWorkBound(UInt64 maximum_objects)
{
    UInt64 levels = 0;
    for (UInt64 remaining = maximum_objects; remaining != 0; remaining >>= 1)
        ++levels;
    return checkedAdd(
        checkedMultiply(levels, 2, "authority repair-audit quarantine lookup work overflows UInt64"),
        8,
        "authority repair-audit quarantine lookup work overflows UInt64");
}

}

class AuthorityRepairAuditBuildContinuation::Impl final
{
public:
    enum class LeafPhase : UInt8
    {
        RootedRecord,
        DefinitionObservation,
        ExpectationRecordObservation,
        SidecarObservation,
        InstallationObservation,
        MetadataObservation,
        ObjectObservation,
        IntegrityVerification,
    };

    enum class Phase : UInt8
    {
        Initialize,
        InventoryPreflight,
        ObservationPreflight,
        PreserveQuarantineSeeds,
        AuditLeaves,
        MaterializeSeeds,
        InitializeQuarantine,
        SeedQuarantine,
        WalkQuarantine,
        MaterializeQuarantine,
        ManifestHeader,
        ManifestSeeds,
        ManifestClosureCount,
        ManifestClosure,
        ManifestFindings,
        ManifestTargets,
        ManifestFinalize,
        Complete,
    };

    void clear() noexcept
    {
        pinned_root.reset();
        inventory.reset();
        graph.reset();
        root_identity = {};
        inventory_summary = {};
        statistics = {};
        resource_limits_identity.reset();
        phase = Phase::Initialize;
        observation_data = nullptr;
        observation_size = 0;
        observation_identity_hasher.reset();
        observation_batch_digest.reset();
        observation_digests = std::vector<Digest>{};
        preserved_quarantine.reset();
        preserved_seed_data = nullptr;
        preserved_seed_size = 0;
        preserved_seed_index = 0;
        preserved_seed_hasher.reset();
        preserved_seed_batch_digest.reset();
        inventory_index = 0;
        observation_validation_index = 0;
        observation_index = 0;
        definition_count = 0;
        expectation_count = 0;
        installation_count = 0;
        expected_artifacts = 0;
        rooted_expectations_seen = 0;
        object_verifier_transient_limit = 0;
        observation_identity_scratch_bytes = 0;
        maximum_decode_scratch_bytes = 0;
        leaf_phase = LeafPhase::RootedRecord;
        leaf_exact_sidecar = false;
        leaf_exact_object = false;
        leaf_expected_metadata_hash.reset();
        findings = std::vector<AuthorityRepairFinding>{};
        repair_targets = std::vector<AuthorityRepairTarget>{};
        seed_set.clear();
        seeds = std::vector<SchemaObjectID>{};
        quarantine_statistics = {};
        quarantine_closure.clear();
        quarantine_pending = std::vector<SchemaObjectID>{};
        quarantine_seed_index = 0;
        quarantine_pending_index = 0;
        quarantine_dependent_index = 0;
        quarantine_adjacency_active = false;
        quarantine_lookup_work = 0;
        quarantined_objects = std::vector<SchemaObjectID>{};
        quarantine.reset();
        manifest_hasher.reset();
        manifest_seed_index = 0;
        manifest_closure_index = 0;
        manifest_finding_index = 0;
        manifest_target_index = 0;
        manifest_digest = {};
    }

    std::optional<AtomicAuthority::RootSnapshot> pinned_root;
    AuthorityInventory::Ptr inventory;
    SchemaObjectDependencyGraph::Ptr graph;
    AuthorityRootGraphIdentity root_identity;
    AuthorityInventorySummary inventory_summary;
    AuthorityRepairAuditStatistics statistics;
    std::optional<Digest> resource_limits_identity;
    Phase phase = Phase::Initialize;
    const AuthorityRepairObservation * observation_data = nullptr;
    size_t observation_size = 0;
    std::unique_ptr<CanonicalHasher> observation_identity_hasher;
    std::optional<Digest> observation_batch_digest;
    std::vector<Digest> observation_digests;
    AuthorityQuarantinePlan::Ptr preserved_quarantine;
    const SchemaObjectID * preserved_seed_data = nullptr;
    size_t preserved_seed_size = 0;
    size_t preserved_seed_index = 0;
    std::unique_ptr<CanonicalHasher> preserved_seed_hasher;
    std::optional<Digest> preserved_seed_batch_digest;
    UInt64 inventory_index = 0;
    size_t observation_validation_index = 0;
    size_t observation_index = 0;
    UInt64 definition_count = 0;
    UInt64 expectation_count = 0;
    UInt64 installation_count = 0;
    UInt64 expected_artifacts = 0;
    UInt64 rooted_expectations_seen = 0;
    UInt64 object_verifier_transient_limit = 0;
    UInt64 observation_identity_scratch_bytes = 0;
    UInt64 maximum_decode_scratch_bytes = 0;
    LeafPhase leaf_phase = LeafPhase::RootedRecord;
    bool leaf_exact_sidecar = false;
    bool leaf_exact_object = false;
    std::optional<Digest> leaf_expected_metadata_hash;
    std::vector<AuthorityRepairFinding> findings;
    std::vector<AuthorityRepairTarget> repair_targets;
    std::set<SchemaObjectID> seed_set;
    std::vector<SchemaObjectID> seeds;
    AuthorityQuarantinePlanStatistics quarantine_statistics;
    std::set<SchemaObjectID> quarantine_closure;
    std::vector<SchemaObjectID> quarantine_pending;
    size_t quarantine_seed_index = 0;
    size_t quarantine_pending_index = 0;
    size_t quarantine_dependent_index = 0;
    bool quarantine_adjacency_active = false;
    UInt64 quarantine_lookup_work = 0;
    std::vector<SchemaObjectID> quarantined_objects;
    AuthorityQuarantinePlan::Ptr quarantine;
    std::unique_ptr<CanonicalHasher> manifest_hasher;
    size_t manifest_seed_index = 0;
    size_t manifest_closure_index = 0;
    size_t manifest_finding_index = 0;
    size_t manifest_target_index = 0;
    Digest manifest_digest{};
};

namespace
{
bool processOneResumedAuditWorkItem(
    AuthorityRepairAuditBuildContinuation::Impl & progress,
    std::span<const AuthorityRepairObservation> observations,
    const AuthorityRepairAuditLimits & limits,
    AuditBudget & budget,
    UInt64 object_verifier_transient_limit);

}

AuthorityRepairAudit::Ptr AuthorityRepairAudit::resume(
    AuthorityRepairAuditBuildContinuation & continuation,
    AtomicAuthority::RootSnapshot && pinned_root,
    std::span<const AuthorityRepairObservation> exact_ordered_observations,
    const AuthorityRepairAuditLimits & limits,
    AuthorityQuarantinePlan::Ptr preserved_quarantine)
{
    validateLimits(limits);
    const Digest resource_limits_identity = computeRepairAuditResourceLimitsIdentity(limits);
    auto & progress = *continuation.impl;
    if (progress.phase != AuthorityRepairAuditBuildContinuation::Impl::Phase::Initialize)
    {
        if (!progress.resource_limits_identity || *progress.resource_limits_identity != resource_limits_identity)
            fail(AuditError::Code::InvalidConfiguration, "authority repair audit continuation changed its deterministic resource limits");
        if (!pinned_root || !progress.pinned_root || std::addressof(pinned_root.get()) != std::addressof(progress.pinned_root->get())
            || pinned_root->getAuthorityState() != progress.pinned_root->get().getAuthorityState()
            || pinned_root->getInventorySummary() != progress.inventory_summary
            || pinned_root->getAuthorityState().schema_graph_root != progress.root_identity.schema_graph_root)
            fail(AuditError::Code::InvalidRoot, "authority repair audit continuation changed its exact pinned root");
        if (exact_ordered_observations.data() != progress.observation_data
            || exact_ordered_observations.size() != progress.observation_size)
            fail(AuditError::Code::NonCanonicalObservationSet, "authority repair audit continuation changed its observation batch");
        const auto preserved_seeds = preserved_quarantine ? preserved_quarantine->getFailingSeeds() : std::span<const SchemaObjectID>{};
        if (preserved_quarantine.get() != progress.preserved_quarantine.get() || preserved_seeds.data() != progress.preserved_seed_data
            || preserved_seeds.size() != progress.preserved_seed_size)
        {
            fail(AuditError::Code::QuarantineFailure, "authority repair audit continuation changed its preserved quarantine seeds");
        }
    }

    AuditBudget budget(limits, progress.statistics);
    using Phase = AuthorityRepairAuditBuildContinuation::Impl::Phase;
    while (true)
    {
        switch (progress.phase)
        {
            case Phase::Initialize: {
                AtomicAuditUnit unit(budget);
                if (!pinned_root)
                    fail(AuditError::Code::InvalidRoot, "authority repair audit requires a nonempty Atomic root snapshot");
                progress.pinned_root.emplace(std::move(pinned_root));
                progress.resource_limits_identity = resource_limits_identity;
                const AuthorityRoot & authority = progress.pinned_root->get();
                const AuthorityState & state = authority.getAuthorityState();
                if (state.database_uuid == UUIDHelpers::Nil || state.database_uuid != authority.getDatabaseUUID()
                    || state.database_catalog_epoch == 0 || isZeroDigest(state.inventory_root) || isZeroDigest(state.schema_graph_root)
                    || isZeroDigest(state.anchor_hash)
                    || (state.persistent_capability_mask != definition_authority_capability_mask
                        && state.persistent_capability_mask != dependent_object_authority_capability_mask))
                    fail(AuditError::Code::InvalidRoot, "authority repair audit received an invalid authority-root identity");
                budget.retain(repair_audit_base_canonical_bytes);
                try
                {
                    budget.admitScratch(limits.object_verifier.authority_state.maximum_encoded_bytes);
                    budget.chargeWork(checkedMultiply(
                        limits.object_verifier.authority_state.maximum_encoded_bytes,
                        rooted_canonical_artifact_work_multiplier,
                        "authority repair-audit root-state work overflows UInt64"));
                    static_cast<void>(encodeAuthorityState(state, limits.object_verifier.authority_state));
                    if (computeAuthorityStateAnchor(state, limits.object_verifier.authority_state) != state.anchor_hash)
                        fail(AuditError::Code::InvalidRoot, "authority repair audit root has an invalid authority anchor");
                }
                catch (const AuthorityRepairAuditError &)
                {
                    throw;
                }
                catch (const AuthorityStateError & error)
                {
                    if (error.code == AuthorityStateError::Code::LimitExceeded)
                        fail(AuditError::Code::LimitExceeded, "authority repair audit root state exceeds its limit");
                    fail(AuditError::Code::InvalidRoot, "authority repair audit root state is invalid");
                }
                progress.inventory = authority.pinAuthorityInventory();
                progress.graph = authority.pinSchemaObjectDependencyGraph();
                if (!progress.inventory || !progress.graph)
                    fail(AuditError::Code::InvalidRoot, "authority repair audit root has no inventory or schema graph");
                progress.inventory_summary = progress.inventory->getSummary();
                if (progress.inventory_summary.leaf_count != state.leaf_count
                    || progress.inventory_summary.merkle_radix_root != state.inventory_root)
                    fail(AuditError::Code::InventoryMismatch, "authority repair audit inventory differs from its exact root anchor");
                if (progress.graph->getDatabaseUUID() != state.database_uuid || progress.graph->computeRoot() != state.schema_graph_root)
                    fail(AuditError::Code::GraphMismatch, "authority repair audit graph differs from its exact root anchor");
                if (progress.inventory_summary.leaf_count > limits.object_verifier.authority_state.maximum_leaves)
                    fail(AuditError::Code::LimitExceeded, "authority repair audit inventory exceeds its leaf limit");
                progress.root_identity = {
                    .authority_root = {
                        .database_uuid = state.database_uuid,
                        .database_catalog_epoch = state.database_catalog_epoch,
                        .authority_anchor = state.anchor_hash,
                    },
                    .schema_graph_root = state.schema_graph_root,
                };
                if (preserved_quarantine && preserved_quarantine->getRoot() != progress.root_identity)
                    fail(AuditError::Code::QuarantineFailure, "authority repair audit preserved quarantine belongs to another exact root");
                progress.statistics.inventory_leaves = progress.inventory_summary.leaf_count;
                budget.chargeWork(progress.inventory_summary.leaf_count);
                progress.observation_data = exact_ordered_observations.data();
                progress.observation_size = exact_ordered_observations.size();
                progress.observation_identity_hasher = std::make_unique<CanonicalHasher>(repair_observation_batch_domain);
                updateUInt64(*progress.observation_identity_hasher, toUInt64(exact_ordered_observations.size()));
                budget.chargeWork(sizeof(UInt64));
                progress.preserved_quarantine = std::move(preserved_quarantine);
                const auto preserved_seeds
                    = progress.preserved_quarantine ? progress.preserved_quarantine->getFailingSeeds() : std::span<const SchemaObjectID>{};
                if (toUInt64(preserved_seeds.size()) > limits.quarantine.maximum_seed_objects)
                    fail(AuditError::Code::LimitExceeded, "authority repair audit preserved quarantine exceeds its seed limit");
                progress.preserved_seed_data = preserved_seeds.data();
                progress.preserved_seed_size = preserved_seeds.size();
                progress.preserved_seed_hasher = std::make_unique<CanonicalHasher>(repair_preserved_seed_batch_domain);
                updateUInt64(*progress.preserved_seed_hasher, toUInt64(preserved_seeds.size()));
                budget.chargeWork(sizeof(UInt64));
                progress.phase = Phase::InventoryPreflight;
                unit.seal();
                continue;
            }
            case Phase::InventoryPreflight: {
                if (progress.inventory_index < progress.inventory_summary.leaf_count)
                {
                    AtomicAuditUnit unit(budget);
                    budget.chargeWork(authority_inventory_index_lookup_work_units);
                    const auto * leaf_ptr = progress.inventory->getLeafByCanonicalIndex(progress.inventory_index);
                    if (!leaf_ptr)
                        fail(AuditError::Code::InventoryMismatch, "authority repair audit lost its canonical inventory preflight cursor");
                    const auto & leaf = *leaf_ptr;
                    if (leaf.key.format_version != authority_inventory_format_version || leaf.key.object_uuid == UUIDHelpers::Nil
                        || leaf.object_revision == 0 || isZeroDigest(leaf.canonical_record_hash))
                        fail(AuditError::Code::InventoryMismatch, "authority repair audit inventory contains an invalid leaf");
                    if (progress.inventory_index != 0)
                    {
                        budget.chargeWork(authority_inventory_index_lookup_work_units);
                        const auto * previous = progress.inventory->getLeafByCanonicalIndex(progress.inventory_index - 1);
                        if (!previous || !authorityInventoryKeyLess(previous->key, leaf.key))
                            fail(
                                AuditError::Code::InventoryMismatch,
                                "authority repair audit inventory leaves are not canonical and unique");
                    }
                    UInt64 artifact_count = 1;
                    if (leaf.key.record_kind == AuthorityInventoryRecordKind::TypeDefinition)
                    {
                        ++progress.definition_count;
                    }
                    else if (leaf.key.record_kind == AuthorityInventoryRecordKind::SidecarExpectation)
                    {
                        ++progress.expectation_count;
                        const auto & authority = progress.pinned_root->get();
                        const auto * expectation = authority.findExpectationRecord(leaf.key.object_uuid);
                        if (!expectation || !expectation->object.isValid() || !isSidecarObjectKind(expectation->object.kind)
                            || expectation->object.database_uuid != authority.getDatabaseUUID()
                            || expectation->object.object_uuid != leaf.key.object_uuid
                            || expectation->object_schema_revision != leaf.object_revision || isZeroDigest(expectation->sidecar_hash)
                            || isZeroDigest(expectation->physical_schema_fingerprint) || !progress.graph->containsNode(expectation->object))
                            fail(
                                AuditError::Code::RecordStoreMismatch,
                                "authority repair audit root contains an invalid expectation record");
                        artifact_count = expectation->installation_record_hash ? 5 : 3;
                        if (expectation->installation_record_hash)
                        {
                            if (isZeroDigest(*expectation->installation_record_hash))
                                fail(
                                    AuditError::Code::RecordStoreMismatch,
                                    "authority repair audit root contains a zero installation-record hash");
                            ++progress.installation_count;
                        }
                    }
                    else
                    {
                        fail(AuditError::Code::InventoryMismatch, "authority repair audit inventory contains an unknown record kind");
                    }
                    progress.expected_artifacts = checkedAdd(
                        progress.expected_artifacts, artifact_count, "authority repair-audit expected artifact count overflows UInt64");
                    if (progress.expected_artifacts > limits.maximum_observations)
                        fail(AuditError::Code::LimitExceeded, "authority repair audit exceeds its observation-count limit");
                    ++progress.inventory_index;
                    unit.seal();
                    continue;
                }

                AtomicAuditUnit unit(budget);
                const auto & authority = progress.pinned_root->get();
                const auto & state = authority.getAuthorityState();
                if (authority.getDefinitionRecordCount() != progress.definition_count
                    || authority.getExpectationRecordCount() != progress.expectation_count)
                    fail(
                        AuditError::Code::RecordStoreMismatch,
                        "authority repair audit record-store cardinality differs from the inventory");
                if (state.persistent_capability_mask == definition_authority_capability_mask && progress.expectation_count != 0)
                    fail(AuditError::Code::InvalidRoot, "definition-only authority root contains sidecar expectations");
                if (toUInt64(exact_ordered_observations.size()) != progress.expected_artifacts)
                    fail(
                        AuditError::Code::NonCanonicalObservationSet,
                        "authority repair observations do not exactly cover the root-derived artifact set");
                progress.statistics.expected_artifacts = progress.expected_artifacts;
                progress.statistics.observed_artifacts = progress.expected_artifacts;
                const UInt64 observation_digest_bytes = checkedMultiply(
                    progress.expected_artifacts, sizeof(Digest), "authority repair observation identity retention overflows UInt64");
                progress.observation_identity_scratch_bytes = observation_digest_bytes;

                const UInt64 record_decode_scratch = progress.definition_count == 0
                    ? 0
                    : checkedAdd(
                          checkedMultiply(
                              limits.object_verifier.definition_record.maximum_record_bytes,
                              2,
                              "authority repair-audit record scratch overflows UInt64"),
                          checkedAdd(
                              checkedMultiply(
                                  limits.object_verifier.definition_record.maximum_parameter_count,
                                  sizeof(Parameter),
                                  "authority repair-audit parameter scratch overflows UInt64"),
                              checkedMultiply(
                                  limits.object_verifier.definition_record.maximum_dependency_count,
                                  sizeof(DefinitionDependency),
                                  "authority repair-audit dependency scratch overflows UInt64"),
                              "authority repair-audit record scratch overflows UInt64"),
                          "authority repair-audit record scratch overflows UInt64");
                const UInt64 sidecar_decode_scratch = progress.expectation_count == 0
                    ? 0
                    : checkedAdd(
                          checkedMultiply(
                              limits.object_verifier.persisted_references.maximum_sidecar_bytes,
                              2,
                              "authority repair-audit sidecar scratch overflows UInt64"),
                          checkedAdd(
                              checkedMultiply(
                                  limits.object_verifier.persisted_references.maximum_descriptors,
                                  sizeof(PersistedTypeDescriptor),
                                  "authority repair-audit descriptor scratch overflows UInt64"),
                              checkedMultiply(
                                  limits.object_verifier.persisted_references.maximum_occurrence_paths,
                                  sizeof(PersistedTypeOccurrencePath) + sizeof(PersistedTypeOccurrenceUse),
                                  "authority repair-audit occurrence scratch overflows UInt64"),
                              "authority repair-audit sidecar scratch overflows UInt64"),
                          "authority repair-audit sidecar scratch overflows UInt64");
                const UInt64 installation_decode_scratch = progress.installation_count == 0
                    ? 0
                    : checkedAdd(
                          limits.installation_record.maximum_encoded_bytes,
                          limits.installation_record.maximum_object_name_bytes,
                          "authority repair-audit installation-record scratch overflows UInt64");
                const UInt64 metadata_hash_scratch = progress.installation_count == 0
                    ? 0
                    : checkedAdd(
                          limits.maximum_canonical_metadata_bytes, 16, "authority repair-audit metadata hash scratch overflows UInt64");
                const UInt64 decode_scratch = checkedAdd(
                    observation_digest_bytes,
                    std::max({record_decode_scratch, sidecar_decode_scratch, installation_decode_scratch, metadata_hash_scratch}),
                    "authority repair-audit continuation scratch overflows UInt64");
                progress.maximum_decode_scratch_bytes
                    = std::max({record_decode_scratch, sidecar_decode_scratch, installation_decode_scratch, metadata_hash_scratch});
                budget.admitScratch(decode_scratch);
                progress.object_verifier_transient_limit = limits.object_verifier.maximum_transient_bytes;
                if (progress.expectation_count != 0)
                {
                    const UInt64 sidecar_and_identity_scratch = checkedAdd(
                        observation_digest_bytes, sidecar_decode_scratch, "authority repair-audit verifier base scratch overflows UInt64");
                    if (sidecar_and_identity_scratch >= limits.maximum_scratch_bytes)
                        fail(
                            AuditError::Code::LimitExceeded, "authority repair audit has no scratch budget for rooted object verification");
                    progress.object_verifier_transient_limit
                        = std::min(progress.object_verifier_transient_limit, limits.maximum_scratch_bytes - sidecar_and_identity_scratch);
                    progress.maximum_decode_scratch_bytes = std::max(
                        progress.maximum_decode_scratch_bytes,
                        checkedAdd(
                            sidecar_decode_scratch,
                            progress.object_verifier_transient_limit,
                            "authority repair-audit verifier transient scratch overflows UInt64"));
                    budget.admitScratch(checkedAdd(
                        sidecar_and_identity_scratch,
                        progress.object_verifier_transient_limit,
                        "authority repair-audit verifier scratch overflows UInt64"));
                }
                progress.observation_digests.reserve(exact_ordered_observations.size());
                progress.inventory_index = 0;
                progress.phase = Phase::ObservationPreflight;
                unit.seal();
                continue;
            }
            case Phase::ObservationPreflight: {
                if (progress.observation_validation_index < exact_ordered_observations.size())
                {
                    AtomicAuditUnit unit(budget);
                    const auto & observation = exact_ordered_observations[progress.observation_validation_index];
                    validateObservationShape(observation, limits);
                    budget.chargeObservedBytes(toUInt64(observation.artifact_bytes.size()));
                    const UInt64 identity_bytes = checkedAdd(
                        schema_object_identity_canonical_bytes + inventory_key_canonical_bytes + 2 * sizeof(UInt8) + 2 * sizeof(UInt64)
                            + sizeof(Digest),
                        toUInt64(observation.artifact_bytes.size()),
                        "authority repair observation identity bytes overflow UInt64");
                    budget.chargeWork(checkedAdd(identity_bytes, 1, "authority repair observation work overflows UInt64"));
                    const Digest observation_digest = computeRepairObservationIdentity(observation);
                    progress.observation_digests.push_back(observation_digest);
                    progress.observation_identity_hasher->update(observation_digest);
                    ++progress.observation_validation_index;
                    unit.seal();
                    continue;
                }
                AtomicAuditUnit unit(budget);
                progress.observation_batch_digest = progress.observation_identity_hasher->finalize();
                progress.observation_identity_hasher.reset();
                progress.phase = Phase::PreserveQuarantineSeeds;
                unit.seal();
                continue;
            }
            case Phase::PreserveQuarantineSeeds: {
                const auto preserved_seeds
                    = progress.preserved_quarantine ? progress.preserved_quarantine->getFailingSeeds() : std::span<const SchemaObjectID>{};
                if (preserved_seeds.data() != progress.preserved_seed_data || preserved_seeds.size() != progress.preserved_seed_size)
                {
                    fail(AuditError::Code::QuarantineFailure, "authority repair audit lost its exact preserved quarantine seed image");
                }
                if (progress.preserved_seed_index < preserved_seeds.size())
                {
                    AtomicAuditUnit unit(budget);
                    const auto & seed = preserved_seeds[progress.preserved_seed_index];
                    if (!seed.isValid() || seed.database_uuid != progress.root_identity.authority_root.database_uuid
                        || !progress.graph->containsNode(seed)
                        || (progress.preserved_seed_index != 0 && !(preserved_seeds[progress.preserved_seed_index - 1] < seed)))
                    {
                        fail(
                            AuditError::Code::QuarantineFailure,
                            "authority repair audit preserved quarantine seeds are not exact canonical graph objects");
                    }
                    const UInt64 seed_scratch = checkedMultiply(
                        checkedAdd(toUInt64(progress.seed_set.size()), 1, "authority repair-audit seed count overflows UInt64"),
                        resumable_set_item_scratch_bytes,
                        "authority repair-audit seed continuation scratch overflows UInt64");
                    budget.admitScratch(checkedAdd(
                        progress.observation_identity_scratch_bytes,
                        checkedAdd(
                            progress.maximum_decode_scratch_bytes,
                            seed_scratch,
                            "authority repair-audit seed continuation scratch overflows UInt64"),
                        "authority repair-audit seed continuation scratch overflows UInt64"));
                    const auto [_, inserted] = progress.seed_set.insert(seed);
                    if (!inserted)
                        fail(AuditError::Code::QuarantineFailure, "authority repair audit preserved quarantine seeds are not unique");
                    updateObject(*progress.preserved_seed_hasher, seed);
                    budget.chargeWork(schema_object_identity_canonical_bytes);
                    ++progress.preserved_seed_index;
                    unit.seal();
                    continue;
                }
                AtomicAuditUnit unit(budget);
                progress.preserved_seed_batch_digest = progress.preserved_seed_hasher->finalize();
                progress.preserved_seed_hasher.reset();
                progress.phase = Phase::AuditLeaves;
                unit.seal();
                continue;
            }
            case Phase::AuditLeaves: {
                if (progress.inventory_index < progress.inventory_summary.leaf_count)
                {
                    AtomicAuditUnit unit(budget);
                    const bool leaf_complete = processOneResumedAuditWorkItem(
                        progress, exact_ordered_observations, limits, budget, progress.object_verifier_transient_limit);
                    if (leaf_complete)
                    {
                        ++progress.inventory_index;
                        progress.leaf_phase = AuthorityRepairAuditBuildContinuation::Impl::LeafPhase::RootedRecord;
                        progress.leaf_exact_sidecar = false;
                        progress.leaf_exact_object = false;
                        progress.leaf_expected_metadata_hash.reset();
                    }
                    unit.seal();
                    continue;
                }
                AtomicAuditUnit unit(budget);
                if (progress.observation_index != exact_ordered_observations.size())
                    fail(
                        AuditError::Code::NonCanonicalObservationSet,
                        "authority repair observation set contains an unrooted extra artifact");
                if (progress.rooted_expectations_seen != progress.expectation_count)
                    fail(
                        AuditError::Code::RecordStoreMismatch,
                        "authority repair audit expectation store contains an unrooted extra record");
                progress.statistics.findings = toUInt64(progress.findings.size());
                progress.statistics.repair_targets = toUInt64(progress.repair_targets.size());
                progress.statistics.unrepairable_findings = progress.statistics.findings - progress.statistics.repair_targets;
                const UInt64 seed_count = toUInt64(progress.seed_set.size());
                const UInt64 seed_set_scratch = checkedMultiply(
                    seed_count, resumable_set_item_scratch_bytes, "authority repair-audit seed-set scratch overflows UInt64");
                const UInt64 seed_vector_scratch
                    = checkedMultiply(seed_count, sizeof(SchemaObjectID), "authority repair-audit seed-vector scratch overflows UInt64");
                budget.admitScratch(checkedAdd(
                    progress.observation_identity_scratch_bytes,
                    checkedAdd(
                        seed_set_scratch, seed_vector_scratch, "authority repair-audit seed materialization scratch overflows UInt64"),
                    "authority repair-audit seed materialization scratch overflows UInt64"));
                budget.chargeWork(sortWorkBound(progress.statistics.findings));
                progress.seeds.reserve(progress.seed_set.size());
                progress.phase = Phase::MaterializeSeeds;
                unit.seal();
                continue;
            }
            case Phase::MaterializeSeeds: {
                if (!progress.seed_set.empty())
                {
                    AtomicAuditUnit unit(budget);
                    const auto it = progress.seed_set.begin();
                    progress.seeds.push_back(*it);
                    progress.seed_set.erase(it);
                    unit.seal();
                    continue;
                }
                AtomicAuditUnit unit(budget);
                progress.statistics.failing_seeds = toUInt64(progress.seeds.size());
                progress.phase = progress.seeds.empty() ? Phase::ManifestHeader : Phase::InitializeQuarantine;
                unit.seal();
                continue;
            }
            case Phase::InitializeQuarantine: {
                AtomicAuditUnit unit(budget);
                const UInt64 seed_count = toUInt64(progress.seeds.size());
                const auto & qlimits = limits.quarantine;
                if (seed_count == 0 || seed_count > qlimits.maximum_seed_objects)
                    fail(AuditError::Code::LimitExceeded, "authority repair audit quarantine exceeds its failing-seed limit");
                progress.quarantine_statistics.seed_objects = seed_count;
                progress.quarantine_statistics.closure_objects = seed_count;
                progress.quarantine_statistics.work_units = checkedAdd(
                    4,
                    checkedMultiply(seed_count, 3, "authority repair-audit quarantine seed work overflows UInt64"),
                    "authority repair-audit quarantine work overflows UInt64");
                constexpr UInt64 quarantine_base_bytes = canonical_uuid_bytes + sizeof(UInt64) + 2 * sizeof(Digest) + 2 * sizeof(UInt64);
                progress.quarantine_statistics.retained_canonical_bytes = checkedAdd(
                    quarantine_base_bytes,
                    checkedMultiply(
                        checkedMultiply(seed_count, 2, "authority repair-audit quarantine seed bytes overflow UInt64"),
                        schema_object_identity_canonical_bytes,
                        "authority repair-audit quarantine seed bytes overflow UInt64"),
                    "authority repair-audit quarantine retained bytes overflow UInt64");
                if (progress.quarantine_statistics.closure_objects > qlimits.maximum_closure_objects
                    || progress.quarantine_statistics.work_units > qlimits.maximum_work_units
                    || progress.quarantine_statistics.retained_canonical_bytes > qlimits.maximum_retained_canonical_bytes)
                    fail(AuditError::Code::LimitExceeded, "authority repair audit quarantine seeds exceed their effective limits");
                const UInt64 quarantine_seed_vector_bytes = checkedMultiply(
                    seed_count, sizeof(SchemaObjectID), "authority repair-audit quarantine seed scratch overflows UInt64");
                const UInt64 quarantine_pending_bytes = checkedMultiply(
                    qlimits.maximum_closure_objects,
                    sizeof(SchemaObjectID),
                    "authority repair-audit quarantine pending scratch overflows UInt64");
                const UInt64 quarantine_set_bytes = checkedMultiply(
                    qlimits.maximum_closure_objects,
                    resumable_set_item_scratch_bytes,
                    "authority repair-audit quarantine set scratch overflows UInt64");
                const UInt64 quarantine_materialization_bytes = checkedMultiply(
                    qlimits.maximum_closure_objects,
                    sizeof(SchemaObjectID),
                    "authority repair-audit quarantine materialization scratch overflows UInt64");
                budget.admitScratch(checkedAdd(
                    progress.observation_identity_scratch_bytes,
                    checkedAdd(
                        quarantine_seed_vector_bytes,
                        checkedAdd(
                            quarantine_pending_bytes,
                            checkedAdd(
                                quarantine_set_bytes,
                                quarantine_materialization_bytes,
                                "authority repair-audit quarantine continuation scratch overflows UInt64"),
                            "authority repair-audit quarantine continuation scratch overflows UInt64"),
                        "authority repair-audit quarantine continuation scratch overflows UInt64"),
                    "authority repair-audit quarantine continuation scratch overflows UInt64"));
                progress.quarantine_lookup_work
                    = quarantineLookupWorkBound(std::min(qlimits.maximum_closure_objects, progress.graph->getNodeCount()));
                progress.quarantine_pending.reserve(static_cast<size_t>(qlimits.maximum_closure_objects));
                progress.phase = Phase::SeedQuarantine;
                unit.seal();
                continue;
            }
            case Phase::SeedQuarantine: {
                if (progress.quarantine_seed_index < progress.seeds.size())
                {
                    AtomicAuditUnit unit(budget);
                    const auto & seed = progress.seeds[progress.quarantine_seed_index];
                    if (!seed.isValid() || seed.database_uuid != progress.root_identity.authority_root.database_uuid
                        || !progress.graph->containsNode(seed))
                        fail(AuditError::Code::QuarantineFailure, "authority repair audit has an invalid quarantine seed");
                    progress.quarantine_closure.emplace_hint(progress.quarantine_closure.end(), seed);
                    progress.quarantine_pending.push_back(seed);
                    ++progress.quarantine_seed_index;
                    unit.seal();
                    continue;
                }
                AtomicAuditUnit unit(budget);
                progress.phase = Phase::WalkQuarantine;
                unit.seal();
                continue;
            }
            case Phase::WalkQuarantine: {
                if (progress.quarantine_pending_index >= progress.quarantine_pending.size())
                {
                    AtomicAuditUnit unit(budget);
                    const UInt64 closure_size = toUInt64(progress.quarantine_closure.size());
                    if (closure_size > limits.quarantine.maximum_work_units - progress.quarantine_statistics.work_units)
                        fail(AuditError::Code::LimitExceeded, "authority repair audit quarantine closure exceeds its work limit");
                    progress.quarantine_statistics.work_units += closure_size;
                    progress.quarantined_objects.reserve(progress.quarantine_closure.size());
                    progress.phase = Phase::MaterializeQuarantine;
                    unit.seal();
                    continue;
                }
                const auto dependency = progress.quarantine_pending[progress.quarantine_pending_index];
                const auto dependents = progress.graph->getDependents(dependency);
                if (!progress.quarantine_adjacency_active)
                {
                    AtomicAuditUnit unit(budget);
                    const UInt64 dependent_count = progress.graph->getDependentCount(dependency);
                    if (toUInt64(dependents.size()) != dependent_count)
                        fail(AuditError::Code::GraphMismatch, "pinned schema graph reverse cardinality changed during immutable traversal");
                    if (dependent_count > limits.quarantine.maximum_reverse_edges_per_object)
                        fail(AuditError::Code::LimitExceeded, "authority repair audit quarantine reverse adjacency exceeds its edge limit");
                    if (progress.quarantine_statistics.reverse_adjacencies_read >= limits.quarantine.maximum_closure_objects
                        || dependent_count
                            > limits.quarantine.maximum_walked_edges - progress.quarantine_statistics.reverse_edges_inspected)
                        fail(AuditError::Code::LimitExceeded, "authority repair audit quarantine exceeds its reverse-edge limits");
                    const UInt64 edge_work = checkedMultiply(
                        dependent_count,
                        checkedAdd(progress.quarantine_lookup_work, 1, "authority repair-audit quarantine edge work overflows UInt64"),
                        "authority repair-audit quarantine edge work overflows UInt64");
                    const UInt64 added_work = checkedAdd(1, edge_work, "authority repair-audit quarantine work overflows UInt64");
                    if (added_work > limits.quarantine.maximum_work_units - progress.quarantine_statistics.work_units)
                        fail(AuditError::Code::LimitExceeded, "authority repair audit quarantine exceeds its work limit");
                    ++progress.quarantine_statistics.reverse_adjacencies_read;
                    progress.quarantine_statistics.reverse_edges_inspected += dependent_count;
                    progress.quarantine_statistics.work_units += added_work;
                    progress.quarantine_adjacency_active = true;
                    progress.quarantine_dependent_index = 0;
                    unit.seal();
                    continue;
                }
                if (progress.quarantine_dependent_index < dependents.size())
                {
                    AtomicAuditUnit unit(budget);
                    const auto & dependent = dependents[progress.quarantine_dependent_index];
                    if (!dependent.object.isValid() || dependent.object.database_uuid != progress.root_identity.authority_root.database_uuid
                        || !isKnownSchemaObjectDependencyEdgeKind(dependent.kind))
                        fail(AuditError::Code::GraphMismatch, "pinned schema graph contains an invalid reverse dependency");
                    const auto [_, inserted] = progress.quarantine_closure.insert(dependent.object);
                    if (inserted)
                    {
                        if (progress.quarantine_statistics.closure_objects >= limits.quarantine.maximum_closure_objects
                            || schema_object_identity_canonical_bytes > limits.quarantine.maximum_retained_canonical_bytes
                                    - progress.quarantine_statistics.retained_canonical_bytes)
                            fail(AuditError::Code::LimitExceeded, "authority repair audit quarantine closure exceeds its effective limits");
                        ++progress.quarantine_statistics.closure_objects;
                        progress.quarantine_statistics.retained_canonical_bytes += schema_object_identity_canonical_bytes;
                        progress.quarantine_pending.push_back(dependent.object);
                    }
                    ++progress.quarantine_dependent_index;
                    unit.seal();
                    continue;
                }
                AtomicAuditUnit unit(budget);
                progress.quarantine_adjacency_active = false;
                progress.quarantine_dependent_index = 0;
                ++progress.quarantine_pending_index;
                unit.seal();
                continue;
            }
            case Phase::MaterializeQuarantine: {
                if (progress.quarantined_objects.size() < progress.quarantine_closure.size())
                {
                    AtomicAuditUnit unit(budget);
                    const auto it = progress.quarantined_objects.empty()
                        ? progress.quarantine_closure.begin()
                        : progress.quarantine_closure.upper_bound(progress.quarantined_objects.back());
                    if (it == progress.quarantine_closure.end())
                        fail(AuditError::Code::QuarantineFailure, "authority repair audit lost its quarantine-closure cursor");
                    progress.quarantined_objects.push_back(*it);
                    unit.seal();
                    continue;
                }
                AtomicAuditUnit unit(budget);
                if (progress.quarantine_statistics.work_units > budget.availableWork())
                    fail(AuditError::Code::LimitExceeded, "authority repair audit has no work budget for quarantine closure");
                budget.chargeWork(progress.quarantine_statistics.work_units);
                progress.statistics.quarantined_objects = toUInt64(progress.quarantined_objects.size());
                progress.quarantine = AuthorityQuarantinePlan::Ptr(new AuthorityQuarantinePlan(
                    progress.root_identity,
                    std::move(progress.seeds),
                    std::move(progress.quarantined_objects),
                    progress.quarantine_statistics));
                progress.phase = Phase::ManifestHeader;
                unit.seal();
                continue;
            }
            case Phase::ManifestHeader: {
                AtomicAuditUnit unit(budget);
                progress.manifest_hasher = std::make_unique<CanonicalHasher>(damaged_artifact_manifest_domain);
                budget.chargeManifestBytes(manifest_header_bytes + manifest_count_bytes);
                updateUInt16(*progress.manifest_hasher, authority_repair_audit_format);
                updateUInt16(*progress.manifest_hasher, authority_repair_audit_work_charge_abi);
                progress.manifest_hasher->updateUUID(progress.root_identity.authority_root.database_uuid);
                updateUInt64(*progress.manifest_hasher, progress.root_identity.authority_root.database_catalog_epoch);
                progress.manifest_hasher->update(progress.root_identity.authority_root.authority_anchor);
                updateUInt64(*progress.manifest_hasher, progress.inventory_summary.leaf_count);
                progress.manifest_hasher->update(progress.inventory_summary.merkle_radix_root);
                progress.manifest_hasher->update(progress.root_identity.schema_graph_root);
                updateUInt64(*progress.manifest_hasher, progress.expected_artifacts);
                updateUInt64(*progress.manifest_hasher, toUInt64(progress.findings.size()));
                updateUInt64(*progress.manifest_hasher, toUInt64(progress.repair_targets.size()));
                updateUInt64(*progress.manifest_hasher, toUInt64(progress.findings.size() - progress.repair_targets.size()));
                const auto seeds = progress.quarantine ? progress.quarantine->getFailingSeeds() : std::span<const SchemaObjectID>{};
                updateUInt64(*progress.manifest_hasher, toUInt64(seeds.size()));
                progress.phase = Phase::ManifestSeeds;
                unit.seal();
                continue;
            }
            case Phase::ManifestSeeds: {
                const auto seeds = progress.quarantine ? progress.quarantine->getFailingSeeds() : std::span<const SchemaObjectID>{};
                if (progress.manifest_seed_index < seeds.size())
                {
                    AtomicAuditUnit unit(budget);
                    budget.chargeManifestBytes(manifest_object_bytes);
                    updateObject(*progress.manifest_hasher, seeds[progress.manifest_seed_index]);
                    ++progress.manifest_seed_index;
                    unit.seal();
                    continue;
                }
                progress.phase = Phase::ManifestClosureCount;
                continue;
            }
            case Phase::ManifestClosureCount: {
                AtomicAuditUnit unit(budget);
                budget.chargeManifestBytes(manifest_count_bytes);
                const auto closure = progress.quarantine ? progress.quarantine->getQuarantinedObjects() : std::span<const SchemaObjectID>{};
                updateUInt64(*progress.manifest_hasher, toUInt64(closure.size()));
                progress.phase = Phase::ManifestClosure;
                unit.seal();
                continue;
            }
            case Phase::ManifestClosure: {
                const auto closure = progress.quarantine ? progress.quarantine->getQuarantinedObjects() : std::span<const SchemaObjectID>{};
                if (progress.manifest_closure_index < closure.size())
                {
                    AtomicAuditUnit unit(budget);
                    budget.chargeManifestBytes(manifest_object_bytes);
                    updateObject(*progress.manifest_hasher, closure[progress.manifest_closure_index]);
                    ++progress.manifest_closure_index;
                    unit.seal();
                    continue;
                }
                progress.phase = Phase::ManifestFindings;
                continue;
            }
            case Phase::ManifestFindings: {
                if (progress.manifest_finding_index < progress.findings.size())
                {
                    AtomicAuditUnit unit(budget);
                    const auto & finding = progress.findings[progress.manifest_finding_index];
                    budget.chargeManifestBytes(manifest_finding_bytes);
                    updateUInt8(*progress.manifest_hasher, static_cast<UInt8>(finding.finding_kind));
                    updateUInt8(*progress.manifest_hasher, static_cast<UInt8>(finding.artifact_kind));
                    updateObject(*progress.manifest_hasher, finding.object);
                    updateInventoryKey(*progress.manifest_hasher, finding.authority_key);
                    updateUInt64(*progress.manifest_hasher, finding.expected_object_revision);
                    progress.manifest_hasher->update(finding.expected_canonical_hash);
                    progress.manifest_hasher->update(finding.expected_physical_schema_fingerprint);
                    updateUInt8(*progress.manifest_hasher, static_cast<UInt8>(finding.observed_state));
                    updateUInt64(*progress.manifest_hasher, finding.observed_object_revision);
                    progress.manifest_hasher->update(finding.observed_physical_schema_fingerprint);
                    updateUInt64(*progress.manifest_hasher, finding.observed_bytes);
                    progress.manifest_hasher->update(finding.observed_digest);
                    updateUInt8(*progress.manifest_hasher, finding.repair_target ? 1 : 0);
                    ++progress.manifest_finding_index;
                    unit.seal();
                    continue;
                }
                progress.phase = Phase::ManifestTargets;
                continue;
            }
            case Phase::ManifestTargets: {
                if (progress.manifest_target_index < progress.repair_targets.size())
                {
                    AtomicAuditUnit unit(budget);
                    const auto & target = progress.repair_targets[progress.manifest_target_index];
                    budget.chargeManifestBytes(manifest_target_bytes);
                    updateUInt8(*progress.manifest_hasher, static_cast<UInt8>(target.artifact_kind));
                    updateObject(*progress.manifest_hasher, target.object);
                    updateInventoryKey(*progress.manifest_hasher, target.authority_key);
                    updateUInt64(*progress.manifest_hasher, target.object_revision);
                    progress.manifest_hasher->update(target.expected_canonical_hash);
                    progress.manifest_hasher->update(target.physical_schema_fingerprint);
                    ++progress.manifest_target_index;
                    unit.seal();
                    continue;
                }
                progress.phase = Phase::ManifestFinalize;
                continue;
            }
            case Phase::ManifestFinalize: {
                AtomicAuditUnit unit(budget);
                progress.manifest_digest = progress.manifest_hasher->finalize();
                progress.manifest_hasher.reset();
                progress.phase = Phase::Complete;
                unit.seal();
                continue;
            }
            case Phase::Complete: {
                if (!progress.pinned_root || !static_cast<bool>(*progress.pinned_root) || !progress.observation_batch_digest
                    || !progress.preserved_seed_batch_digest)
                    fail(AuditError::Code::InvalidRoot, "authority repair audit continuation completed without sealed exact inputs");
                auto result = Ptr(new AuthorityRepairAudit(
                    std::move(*progress.pinned_root),
                    progress.root_identity,
                    progress.inventory_summary,
                    std::move(progress.findings),
                    std::move(progress.repair_targets),
                    std::move(progress.quarantine),
                    progress.manifest_digest,
                    progress.statistics));
                progress.pinned_root.reset();
                return result;
            }
        }
    }
}

AuthorityRepairAuditError::AuthorityRepairAuditError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

AuthorityRepairAudit::AuthorityRepairAudit(
    AtomicAuthority::RootSnapshot && pinned_root_,
    AuthorityRootGraphIdentity root_,
    AuthorityInventorySummary inventory_summary_,
    std::vector<AuthorityRepairFinding> findings_,
    std::vector<AuthorityRepairTarget> repair_targets_,
    AuthorityQuarantinePlan::Ptr quarantine_,
    Digest damaged_artifact_manifest_digest_,
    AuthorityRepairAuditStatistics statistics_) noexcept
    : pinned_root(std::move(pinned_root_))
    , root(std::move(root_))
    , inventory_summary(std::move(inventory_summary_))
    , findings(std::move(findings_))
    , repair_targets(std::move(repair_targets_))
    , quarantine(std::move(quarantine_))
    , damaged_artifact_manifest_digest(std::move(damaged_artifact_manifest_digest_))
    , statistics(std::move(statistics_))
{
}

AuthorityRepairAuditBuildContinuation::AuthorityRepairAuditBuildContinuation()
    : impl(std::make_unique<Impl>())
{
}

AuthorityRepairAuditBuildContinuation::~AuthorityRepairAuditBuildContinuation() = default;

void AuthorityRepairAuditBuildContinuation::reset() noexcept
{
    impl->clear();
}

AuthorityRepairAudit::Ptr AuthorityRepairAudit::build(
    AtomicAuthority::RootSnapshot && pinned_root,
    std::span<const AuthorityRepairObservation> exact_ordered_observations,
    const AuthorityRepairAuditLimits & limits)
{
    AuthorityRepairAuditBuildContinuation continuation;
    return resume(continuation, std::move(pinned_root), exact_ordered_observations, limits);
}
namespace
{

const AuthorityRepairObservation & getResumedObservation(
    AuthorityRepairAuditBuildContinuation::Impl & progress,
    std::span<const AuthorityRepairObservation> observations,
    const ExpectedArtifact & expected,
    AuditBudget & budget)
{
    if (progress.observation_index >= observations.size())
        fail(AuditError::Code::NonCanonicalObservationSet, "authority repair observation set ended before the expected artifact set");
    if (progress.observation_index >= progress.observation_digests.size())
        fail(AuditError::Code::NonCanonicalObservationSet, "authority repair observation identity prefix is incomplete");
    const auto & observation = observations[progress.observation_index];
    budget.chargeWork(checkedAdd(
        schema_object_identity_canonical_bytes + inventory_key_canonical_bytes + 2 * sizeof(UInt8) + 2 * sizeof(UInt64) + sizeof(Digest),
        toUInt64(observation.artifact_bytes.size()),
        "authority repair observation identity work overflows UInt64"));
    if (computeRepairObservationIdentity(observation) != progress.observation_digests[progress.observation_index])
        fail(AuditError::Code::NonCanonicalObservationSet, "authority repair observation changed after its exact batch was sealed");
    ++progress.observation_index;
    if (observation.artifact_kind != expected.artifact_kind || observation.authority_key != expected.authority_key)
        fail(
            AuditError::Code::NonCanonicalObservationSet, "authority repair observations are not in the exact root-derived artifact order");
    return observation;
}

void appendResumedFinding(
    AuthorityRepairAuditBuildContinuation::Impl & progress,
    AuditBudget & budget,
    const AuthorityRepairAuditLimits & limits,
    AuthorityRepairFindingKind finding_kind,
    const ExpectedArtifact & expected,
    const AuthorityRepairObservation & observation,
    UInt64 observed_revision,
    const Digest & observed_fingerprint,
    const Digest & observed_digest)
{
    if (toUInt64(progress.findings.size()) >= limits.maximum_findings)
        fail(AuditError::Code::LimitExceeded, "authority repair audit exceeds its finding-count limit");
    const bool repairable = isRepairableArtifact(expected.artifact_kind);
    budget.retain(repair_finding_canonical_bytes);
    std::optional<AuthorityRepairTarget> target;
    if (repairable)
    {
        target = makeRepairTarget(expected);
        budget.retain(checkedMultiply(2, repair_target_canonical_bytes, "authority repair-audit target bytes overflow UInt64"));
        progress.repair_targets.push_back(*target);
    }
    progress.findings.push_back({
        .finding_kind = finding_kind,
        .artifact_kind = expected.artifact_kind,
        .object = expected.object,
        .authority_key = expected.authority_key,
        .expected_object_revision = expected.object_revision,
        .expected_canonical_hash = expected.canonical_hash,
        .expected_physical_schema_fingerprint = expected.physical_schema_fingerprint,
        .observed_state = observation.state,
        .observed_object_revision = observed_revision,
        .observed_physical_schema_fingerprint = observed_fingerprint,
        .observed_bytes = toUInt64(observation.artifact_bytes.size()),
        .observed_digest = observed_digest,
        .repair_target = std::move(target),
    });
    const auto seed_position = progress.seed_set.lower_bound(expected.object);
    if (seed_position == progress.seed_set.end() || *seed_position != expected.object)
    {
        if (toUInt64(progress.seed_set.size()) >= limits.quarantine.maximum_seed_objects)
            fail(AuditError::Code::LimitExceeded, "authority repair audit exceeds its quarantine failing-seed limit");
        const UInt64 seed_scratch = checkedMultiply(
            checkedAdd(toUInt64(progress.seed_set.size()), 1, "authority repair-audit seed count overflows UInt64"),
            resumable_set_item_scratch_bytes,
            "authority repair-audit seed continuation scratch overflows UInt64");
        budget.admitScratch(checkedAdd(
            progress.observation_identity_scratch_bytes,
            checkedAdd(
                progress.maximum_decode_scratch_bytes, seed_scratch, "authority repair-audit seed continuation scratch overflows UInt64"),
            "authority repair-audit seed continuation scratch overflows UInt64"));
        progress.seed_set.emplace_hint(seed_position, expected.object);
    }
}

Digest computeResumedObservedBytesDigest(const AuthorityRepairObservation & observation, AuditBudget & budget)
{
    const UInt64 bytes = toUInt64(observation.artifact_bytes.size());
    budget.chargeWork(
        checkedMultiply(bytes, observed_canonical_artifact_work_multiplier, "authority repair-audit observed-byte work overflows UInt64"));
    return hashFramedDomainSeparated(observed_artifact_digest_domain, observation.artifact_bytes);
}

bool processOneResumedAuditWorkItem(
    AuthorityRepairAuditBuildContinuation::Impl & progress,
    std::span<const AuthorityRepairObservation> observations,
    const AuthorityRepairAuditLimits & limits,
    AuditBudget & budget,
    UInt64 object_verifier_transient_limit)
{
    using LeafPhase = AuthorityRepairAuditBuildContinuation::Impl::LeafPhase;
    const AuthorityRoot & authority = progress.pinned_root->get();
    budget.chargeWork(authority_inventory_index_lookup_work_units);
    const auto * leaf_ptr = progress.inventory->getLeafByCanonicalIndex(progress.inventory_index);
    if (!leaf_ptr)
        fail(AuditError::Code::InventoryMismatch, "authority repair audit lost its canonical inventory cursor");
    const auto & leaf = *leaf_ptr;
    const auto & state = authority.getAuthorityState();
    const bool definition = leaf.key.record_kind == AuthorityInventoryRecordKind::TypeDefinition;
    const auto * expectation = definition ? nullptr : authority.findExpectationRecord(leaf.key.object_uuid);

    if (progress.leaf_phase == LeafPhase::RootedRecord)
    {
        if (definition)
        {
            budget.chargeWork(3);
            const Record * expected_record = authority.findDefinitionRecord(leaf.key.object_uuid);
            const SchemaObjectID object{
                .kind = SchemaObjectKind::TypeDefinition,
                .database_uuid = state.database_uuid,
                .object_uuid = leaf.key.object_uuid,
            };
            if (!expected_record || expected_record->identity.database_uuid != state.database_uuid
                || expected_record->identity.type_uuid != leaf.key.object_uuid || expected_record->identity.revision != leaf.object_revision
                || !progress.graph->containsNode(object))
                fail(AuditError::Code::RecordStoreMismatch, "authority repair audit definition record differs from its inventory identity");
            try
            {
                if (budget.availableWork() < rooted_canonical_artifact_work_multiplier)
                    fail(AuditError::Code::LimitExceeded, "authority repair audit has no work budget for its rooted definition");
                RecordLimits rooted_limits = limits.object_verifier.definition_record;
                rooted_limits.maximum_record_bytes
                    = std::min(rooted_limits.maximum_record_bytes, budget.availableWork() / rooted_canonical_artifact_work_multiplier);
                const String canonical = encodeRecord(*expected_record, rooted_limits);
                budget.chargeWork(checkedMultiply(
                    toUInt64(canonical.size()),
                    rooted_canonical_artifact_work_multiplier,
                    "authority repair-audit rooted definition work overflows UInt64"));
                if (computeRecordHash(*expected_record, rooted_limits) != leaf.canonical_record_hash)
                    fail(AuditError::Code::RecordStoreMismatch, "authority repair audit definition record differs from its inventory hash");
            }
            catch (const AuthorityRepairAuditError &)
            {
                throw;
            }
            catch (const RecordError & error)
            {
                if (error.code == RecordError::Code::LimitExceeded)
                    fail(AuditError::Code::LimitExceeded, "authority repair audit rooted definition exceeds its limit");
                fail(AuditError::Code::RecordStoreMismatch, "authority repair audit rooted definition record is invalid");
            }
            progress.leaf_phase = LeafPhase::DefinitionObservation;
            return false;
        }

        if (!expectation || expectation->object_schema_revision != leaf.object_revision)
            fail(AuditError::Code::RecordStoreMismatch, "authority repair audit expectation record differs from its inventory identity");
        ++progress.rooted_expectations_seen;
        try
        {
            const UInt64 expectation_bytes = expectation->installation_record_hash
                ? toUInt64(sidecar_expectation_record_extended_encoded_bytes)
                : toUInt64(sidecar_expectation_record_encoded_bytes);
            budget.chargeWork(checkedMultiply(
                expectation_bytes,
                rooted_canonical_artifact_work_multiplier,
                "authority repair-audit rooted expectation work overflows UInt64"));
            const String canonical = encodeSidecarExpectationRecord(*expectation);
            if (toUInt64(canonical.size()) != expectation_bytes
                || computeSidecarExpectationRecordHash(*expectation) != leaf.canonical_record_hash)
                fail(AuditError::Code::RecordStoreMismatch, "authority repair audit expectation record differs from its inventory hash");
        }
        catch (const AuthorityRepairAuditError &)
        {
            throw;
        }
        catch (const SidecarExpectationRecordError &)
        {
            fail(AuditError::Code::RecordStoreMismatch, "authority repair audit rooted expectation record is invalid");
        }
        progress.leaf_phase = LeafPhase::ExpectationRecordObservation;
        return false;
    }

    if (progress.leaf_phase == LeafPhase::DefinitionObservation)
    {
        const SchemaObjectID object{
            .kind = SchemaObjectKind::TypeDefinition,
            .database_uuid = state.database_uuid,
            .object_uuid = leaf.key.object_uuid,
        };
        const ExpectedArtifact expected{
            .artifact_kind = AuthorityRepairAuditArtifactKind::DefinitionRecord,
            .object = object,
            .authority_key = leaf.key,
            .object_revision = leaf.object_revision,
            .canonical_hash = leaf.canonical_record_hash,
            .physical_schema_fingerprint = {},
        };
        const auto & observation = getResumedObservation(progress, observations, expected, budget);
        if (observation.state == AuthorityRepairObservationState::Missing)
            appendResumedFinding(progress, budget, limits, AuthorityRepairFindingKind::Missing, expected, observation, 0, {}, {});
        else
        {
            const Digest digest = computeResumedObservedBytesDigest(observation, budget);
            try
            {
                const Record observed = decodeRecord(observation.artifact_bytes, limits.object_verifier.definition_record);
                if (observed.identity.database_uuid != object.database_uuid || observed.identity.type_uuid != object.object_uuid)
                    appendResumedFinding(
                        progress,
                        budget,
                        limits,
                        AuthorityRepairFindingKind::IdentityMismatch,
                        expected,
                        observation,
                        observed.identity.revision,
                        {},
                        digest);
                else if (observed.identity.revision != leaf.object_revision)
                    appendResumedFinding(
                        progress,
                        budget,
                        limits,
                        AuthorityRepairFindingKind::RevisionMismatch,
                        expected,
                        observation,
                        observed.identity.revision,
                        {},
                        digest);
                else if (computeRecordHash(observed, limits.object_verifier.definition_record) != leaf.canonical_record_hash)
                    appendResumedFinding(
                        progress,
                        budget,
                        limits,
                        AuthorityRepairFindingKind::CanonicalHashMismatch,
                        expected,
                        observation,
                        observed.identity.revision,
                        {},
                        digest);
                else
                    ++progress.statistics.clean_artifacts;
            }
            catch (const RecordError &)
            {
                appendResumedFinding(progress, budget, limits, AuthorityRepairFindingKind::Malformed, expected, observation, 0, {}, digest);
            }
        }
        return true;
    }

    if (!expectation || expectation->object_schema_revision != leaf.object_revision)
        fail(AuditError::Code::RecordStoreMismatch, "authority repair audit expectation record changed during its pinned scan");

    if (progress.leaf_phase == LeafPhase::ExpectationRecordObservation)
    {
        const ExpectedArtifact expected{
            .artifact_kind = AuthorityRepairAuditArtifactKind::SidecarExpectationRecord,
            .object = expectation->object,
            .authority_key = leaf.key,
            .object_revision = leaf.object_revision,
            .canonical_hash = leaf.canonical_record_hash,
            .physical_schema_fingerprint = expectation->physical_schema_fingerprint,
        };
        const auto & observation = getResumedObservation(progress, observations, expected, budget);
        if (observation.state == AuthorityRepairObservationState::Missing)
            appendResumedFinding(progress, budget, limits, AuthorityRepairFindingKind::Missing, expected, observation, 0, {}, {});
        else
        {
            const Digest digest = computeResumedObservedBytesDigest(observation, budget);
            try
            {
                const auto observed = decodeSidecarExpectationRecord(observation.artifact_bytes);
                if (observed.object != expectation->object)
                    appendResumedFinding(
                        progress,
                        budget,
                        limits,
                        AuthorityRepairFindingKind::IdentityMismatch,
                        expected,
                        observation,
                        observed.object_schema_revision,
                        observed.physical_schema_fingerprint,
                        digest);
                else if (observed.object_schema_revision != expectation->object_schema_revision)
                    appendResumedFinding(
                        progress,
                        budget,
                        limits,
                        AuthorityRepairFindingKind::RevisionMismatch,
                        expected,
                        observation,
                        observed.object_schema_revision,
                        observed.physical_schema_fingerprint,
                        digest);
                else if (observed.physical_schema_fingerprint != expectation->physical_schema_fingerprint)
                    appendResumedFinding(
                        progress,
                        budget,
                        limits,
                        AuthorityRepairFindingKind::PhysicalSchemaMismatch,
                        expected,
                        observation,
                        observed.object_schema_revision,
                        observed.physical_schema_fingerprint,
                        digest);
                else if (computeSidecarExpectationRecordHash(observed) != leaf.canonical_record_hash)
                    appendResumedFinding(
                        progress,
                        budget,
                        limits,
                        AuthorityRepairFindingKind::CanonicalHashMismatch,
                        expected,
                        observation,
                        observed.object_schema_revision,
                        observed.physical_schema_fingerprint,
                        digest);
                else
                    ++progress.statistics.clean_artifacts;
            }
            catch (const SidecarExpectationRecordError &)
            {
                appendResumedFinding(progress, budget, limits, AuthorityRepairFindingKind::Malformed, expected, observation, 0, {}, digest);
            }
        }
        progress.leaf_phase = LeafPhase::SidecarObservation;
        return false;
    }

    if (progress.leaf_phase == LeafPhase::SidecarObservation)
    {
        const ExpectedArtifact expected{
            .artifact_kind = AuthorityRepairAuditArtifactKind::PersistedTypeReferencesSidecar,
            .object = expectation->object,
            .authority_key = leaf.key,
            .object_revision = leaf.object_revision,
            .canonical_hash = expectation->sidecar_hash,
            .physical_schema_fingerprint = expectation->physical_schema_fingerprint,
        };
        const auto & observation = getResumedObservation(progress, observations, expected, budget);
        if (observation.state == AuthorityRepairObservationState::Missing)
            appendResumedFinding(progress, budget, limits, AuthorityRepairFindingKind::Missing, expected, observation, 0, {}, {});
        else
        {
            const Digest digest = computeResumedObservedBytesDigest(observation, budget);
            try
            {
                const auto observed
                    = decodePersistedTypeReferences(observation.artifact_bytes, limits.object_verifier.persisted_references);
                if (observed.object != expectation->object)
                    appendResumedFinding(
                        progress,
                        budget,
                        limits,
                        AuthorityRepairFindingKind::IdentityMismatch,
                        expected,
                        observation,
                        observed.object_schema_revision,
                        observed.physical_schema_fingerprint,
                        digest);
                else if (observed.object_schema_revision != expectation->object_schema_revision)
                    appendResumedFinding(
                        progress,
                        budget,
                        limits,
                        AuthorityRepairFindingKind::RevisionMismatch,
                        expected,
                        observation,
                        observed.object_schema_revision,
                        observed.physical_schema_fingerprint,
                        digest);
                else if (observed.physical_schema_fingerprint != expectation->physical_schema_fingerprint)
                    appendResumedFinding(
                        progress,
                        budget,
                        limits,
                        AuthorityRepairFindingKind::PhysicalSchemaMismatch,
                        expected,
                        observation,
                        observed.object_schema_revision,
                        observed.physical_schema_fingerprint,
                        digest);
                else if (
                    computePersistedTypeReferencesSidecarHash(observed, limits.object_verifier.persisted_references)
                    != expectation->sidecar_hash)
                    appendResumedFinding(
                        progress,
                        budget,
                        limits,
                        AuthorityRepairFindingKind::CanonicalHashMismatch,
                        expected,
                        observation,
                        observed.object_schema_revision,
                        observed.physical_schema_fingerprint,
                        digest);
                else
                {
                    ++progress.statistics.clean_artifacts;
                    progress.leaf_exact_sidecar = true;
                }
            }
            catch (const PersistedTypeReferencesError &)
            {
                appendResumedFinding(progress, budget, limits, AuthorityRepairFindingKind::Malformed, expected, observation, 0, {}, digest);
            }
        }
        progress.leaf_phase = expectation->installation_record_hash ? LeafPhase::InstallationObservation : LeafPhase::ObjectObservation;
        return false;
    }

    if (progress.leaf_phase == LeafPhase::InstallationObservation)
    {
        if (!expectation->installation_record_hash)
            fail(AuditError::Code::RecordStoreMismatch, "authority repair audit lost its rooted installation-record identity");
        const ExpectedArtifact expected{
            .artifact_kind = AuthorityRepairAuditArtifactKind::DependentObjectMetadataInstallationRecord,
            .object = expectation->object,
            .authority_key = leaf.key,
            .object_revision = leaf.object_revision,
            .canonical_hash = *expectation->installation_record_hash,
            .physical_schema_fingerprint = expectation->physical_schema_fingerprint,
        };
        const auto & observation = getResumedObservation(progress, observations, expected, budget);
        if (observation.state == AuthorityRepairObservationState::Missing)
            appendResumedFinding(progress, budget, limits, AuthorityRepairFindingKind::Missing, expected, observation, 0, {}, {});
        else
        {
            const Digest digest = computeResumedObservedBytesDigest(observation, budget);
            try
            {
                const auto observed
                    = decodeDependentObjectMetadataInstallationRecord(observation.artifact_bytes, limits.installation_record);
                if (observed.object != expectation->object)
                    appendResumedFinding(
                        progress,
                        budget,
                        limits,
                        AuthorityRepairFindingKind::IdentityMismatch,
                        expected,
                        observation,
                        observed.object_schema_revision,
                        {},
                        digest);
                else if (observed.object_schema_revision != expectation->object_schema_revision)
                    appendResumedFinding(
                        progress,
                        budget,
                        limits,
                        AuthorityRepairFindingKind::RevisionMismatch,
                        expected,
                        observation,
                        observed.object_schema_revision,
                        {},
                        digest);
                else if (isZeroDigest(observed.metadata_artifact_hash))
                    appendResumedFinding(
                        progress,
                        budget,
                        limits,
                        AuthorityRepairFindingKind::Malformed,
                        expected,
                        observation,
                        observed.object_schema_revision,
                        {},
                        digest);
                else if (
                    computeDependentObjectMetadataInstallationRecordHash(observed, limits.installation_record)
                    != *expectation->installation_record_hash)
                    appendResumedFinding(
                        progress,
                        budget,
                        limits,
                        AuthorityRepairFindingKind::CanonicalHashMismatch,
                        expected,
                        observation,
                        observed.object_schema_revision,
                        {},
                        digest);
                else
                {
                    ++progress.statistics.clean_artifacts;
                    progress.leaf_expected_metadata_hash = observed.metadata_artifact_hash;
                }
            }
            catch (const DependentObjectMetadataInstallationRecordError &)
            {
                appendResumedFinding(progress, budget, limits, AuthorityRepairFindingKind::Malformed, expected, observation, 0, {}, digest);
            }
        }
        progress.leaf_phase = LeafPhase::MetadataObservation;
        return false;
    }

    if (progress.leaf_phase == LeafPhase::MetadataObservation)
    {
        const ExpectedArtifact expected{
            .artifact_kind = AuthorityRepairAuditArtifactKind::DependentObjectMetadata,
            .object = expectation->object,
            .authority_key = leaf.key,
            .object_revision = leaf.object_revision,
            .canonical_hash = progress.leaf_expected_metadata_hash.value_or(Digest{}),
            .physical_schema_fingerprint = expectation->physical_schema_fingerprint,
        };
        const auto & observation = getResumedObservation(progress, observations, expected, budget);
        if (observation.state == AuthorityRepairObservationState::Missing)
            appendResumedFinding(progress, budget, limits, AuthorityRepairFindingKind::Missing, expected, observation, 0, {}, {});
        else
        {
            const Digest digest = computeResumedObservedBytesDigest(observation, budget);
            if (!progress.leaf_expected_metadata_hash)
                appendResumedFinding(
                    progress, budget, limits, AuthorityRepairFindingKind::AuthenticationUnavailable, expected, observation, 0, {}, digest);
            else if (observation.artifact_bytes.empty())
                appendResumedFinding(progress, budget, limits, AuthorityRepairFindingKind::Malformed, expected, observation, 0, {}, digest);
            else if (
                computeDatabaseSchemaWALStagedArtifactHash(
                    DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, observation.artifact_bytes)
                != *progress.leaf_expected_metadata_hash)
                appendResumedFinding(
                    progress, budget, limits, AuthorityRepairFindingKind::CanonicalHashMismatch, expected, observation, 0, {}, digest);
            else
                ++progress.statistics.clean_artifacts;
        }
        progress.leaf_phase = LeafPhase::ObjectObservation;
        return false;
    }

    if (progress.leaf_phase == LeafPhase::ObjectObservation)
    {
        const ExpectedArtifact expected{
            .artifact_kind = AuthorityRepairAuditArtifactKind::StoredObjectImage,
            .object = expectation->object,
            .authority_key = leaf.key,
            .object_revision = leaf.object_revision,
            .canonical_hash = {},
            .physical_schema_fingerprint = expectation->physical_schema_fingerprint,
        };
        const auto & observation = getResumedObservation(progress, observations, expected, budget);
        if (observation.state == AuthorityRepairObservationState::Missing)
            appendResumedFinding(progress, budget, limits, AuthorityRepairFindingKind::Missing, expected, observation, 0, {}, {});
        else
        {
            budget.chargeWork(schema_object_identity_canonical_bytes + sizeof(UInt64) + sizeof(Digest));
            const Digest digest
                = hashObservedObjectImage(observation.object, observation.object_schema_revision, observation.physical_schema_fingerprint);
            if (observation.object != expectation->object)
                appendResumedFinding(
                    progress,
                    budget,
                    limits,
                    AuthorityRepairFindingKind::IdentityMismatch,
                    expected,
                    observation,
                    observation.object_schema_revision,
                    observation.physical_schema_fingerprint,
                    digest);
            else if (observation.object_schema_revision != expectation->object_schema_revision)
                appendResumedFinding(
                    progress,
                    budget,
                    limits,
                    AuthorityRepairFindingKind::RevisionMismatch,
                    expected,
                    observation,
                    observation.object_schema_revision,
                    observation.physical_schema_fingerprint,
                    digest);
            else if (observation.physical_schema_fingerprint != expectation->physical_schema_fingerprint)
                appendResumedFinding(
                    progress,
                    budget,
                    limits,
                    AuthorityRepairFindingKind::PhysicalSchemaMismatch,
                    expected,
                    observation,
                    observation.object_schema_revision,
                    observation.physical_schema_fingerprint,
                    digest);
            else
            {
                ++progress.statistics.clean_artifacts;
                progress.leaf_exact_object = true;
            }
        }
        if (progress.leaf_exact_sidecar && progress.leaf_exact_object)
        {
            progress.leaf_phase = LeafPhase::IntegrityVerification;
            return false;
        }
        return true;
    }

    if (progress.leaf_phase != LeafPhase::IntegrityVerification || progress.observation_index == 0)
        fail(AuditError::Code::InvalidRoot, "authority repair audit has an invalid leaf continuation state");
    const size_t object_observation_index = progress.observation_index - 1;
    const size_t sidecar_distance = expectation->installation_record_hash ? 3 : 1;
    if (object_observation_index < sidecar_distance)
        fail(AuditError::Code::NonCanonicalObservationSet, "authority repair audit lost its persisted-sidecar observation cursor");
    const size_t sidecar_index = object_observation_index - sidecar_distance;
    const auto & sidecar_observation = observations[sidecar_index];
    budget.chargeWork(checkedAdd(
        schema_object_identity_canonical_bytes + inventory_key_canonical_bytes + 2 * sizeof(UInt8) + 2 * sizeof(UInt64) + sizeof(Digest),
        toUInt64(sidecar_observation.artifact_bytes.size()),
        "authority repair sidecar identity work overflows UInt64"));
    if (sidecar_index >= progress.observation_digests.size()
        || computeRepairObservationIdentity(sidecar_observation) != progress.observation_digests[sidecar_index])
        fail(AuditError::Code::NonCanonicalObservationSet, "authority repair exact sidecar changed before integrity verification");
    try
    {
        const auto references
            = decodePersistedTypeReferences(sidecar_observation.artifact_bytes, limits.object_verifier.persisted_references);
        const UInt64 available = budget.availableWork();
        if (available < 2)
            fail(AuditError::Code::LimitExceeded, "authority repair audit has no work budget for rooted object verification");
        AuthorityIntegrityVerifierLimits verifier_limits = limits.object_verifier;
        const UInt64 dimension = available / 2;
        verifier_limits.maximum_work_units = std::min(verifier_limits.maximum_work_units, dimension);
        verifier_limits.maximum_canonical_bytes_hashed = std::min(verifier_limits.maximum_canonical_bytes_hashed, dimension);
        verifier_limits.maximum_transient_bytes = object_verifier_transient_limit;
        const auto verification = verifyAuthorityObjectIntegrity(
            authority,
            *expectation,
            references,
            sidecar_observation.artifact_bytes,
            expectation->object_schema_revision,
            expectation->physical_schema_fingerprint,
            verifier_limits);
        budget.chargeWork(checkedAdd(
            verification.statistics.work_units,
            verification.statistics.canonical_bytes_hashed,
            "authority repair-audit verifier work overflows UInt64"));
    }
    catch (const PersistedTypeReferencesError &)
    {
        fail(AuditError::Code::RecordStoreMismatch, "authority repair audit exact sidecar changed before integrity verification");
    }
    catch (const AuthorityIntegrityVerifierError & error)
    {
        if (error.code == AuthorityIntegrityVerifierError::Code::InvalidConfiguration)
            fail(AuditError::Code::InvalidConfiguration, "authority repair audit object-verifier configuration is invalid");
        if (error.code == AuthorityIntegrityVerifierError::Code::LimitExceeded)
            fail(AuditError::Code::LimitExceeded, "authority repair audit object verification exceeds its remaining budget");
        if (error.code == AuthorityIntegrityVerifierError::Code::GraphMismatch)
            fail(AuditError::Code::GraphMismatch, "authority repair audit found an inconsistency inside its pinned graph");
        if (error.code == AuthorityIntegrityVerifierError::Code::InventoryMismatch)
            fail(AuditError::Code::InventoryMismatch, "authority repair audit found an inconsistency inside its pinned inventory");
        fail(AuditError::Code::RecordStoreMismatch, "authority repair audit found an inconsistency inside its pinned root");
    }
    return true;
}

}
}
