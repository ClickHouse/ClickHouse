#include <Databases/UDT/AuthorityQuarantineAdmission.h>

#include <DataTypes/UDT/CanonicalHash.h>

#include <algorithm>
#include <limits>

namespace DB::UDT
{
namespace
{

using AdmissionStatus = AuthorityQuarantineAdmissionStatus;

constexpr UInt64 canonical_uuid_bytes = 16;
constexpr UInt64 schema_object_identity_canonical_bytes = sizeof(UInt8) + 2 * canonical_uuid_bytes;
constexpr UInt64 definition_identity_canonical_bytes = 2 * canonical_uuid_bytes + sizeof(UInt64);
constexpr UInt64 authority_root_identity_canonical_bytes = canonical_uuid_bytes + sizeof(UInt64) + sizeof(Digest);
constexpr UInt64 authority_root_graph_identity_canonical_bytes = authority_root_identity_canonical_bytes + sizeof(Digest);
constexpr UInt64 object_image_identity_canonical_bytes = schema_object_identity_canonical_bytes + sizeof(UInt64) + 2 * sizeof(Digest);
constexpr UInt64 verification_stamp_base_canonical_bytes
    = authority_root_identity_canonical_bytes + object_image_identity_canonical_bytes + sizeof(Digest) + sizeof(UInt64);
constexpr UInt64 operation_base_canonical_bytes
    = 2 * sizeof(UInt8) + authority_root_graph_identity_canonical_bytes + 2 * sizeof(UInt8) + 2 * sizeof(UInt64);
constexpr UInt64 continuation_proof_base_canonical_bytes = object_image_identity_canonical_bytes + sizeof(UInt8) + sizeof(UInt64);

UInt64 toUInt64(size_t value) noexcept
{
    static_assert(sizeof(size_t) <= sizeof(UInt64));
    return static_cast<UInt64>(value);
}

bool checkedAdd(UInt64 lhs, UInt64 rhs, UInt64 & result) noexcept
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        return false;
    result = lhs + rhs;
    return true;
}

bool checkedMultiply(UInt64 lhs, UInt64 rhs, UInt64 & result) noexcept
{
    if (lhs != 0 && rhs > std::numeric_limits<UInt64>::max() / lhs)
        return false;
    result = lhs * rhs;
    return true;
}

bool addProduct(UInt64 & current, UInt64 lhs, UInt64 rhs) noexcept
{
    UInt64 product = 0;
    return checkedMultiply(lhs, rhs, product) && checkedAdd(current, product, current);
}

bool isZeroDigest(const Digest & digest) noexcept
{
    return std::all_of(digest.begin(), digest.end(), [](CanonicalByte byte) { return byte == 0; });
}

bool isValidRootIdentity(const AuthorityRootIdentity & root) noexcept
{
    return root.database_uuid != UUIDHelpers::Nil && root.database_catalog_epoch != 0 && !isZeroDigest(root.authority_anchor);
}

bool isValidRootIdentity(const AuthorityRootGraphIdentity & root) noexcept
{
    return isValidRootIdentity(root.authority_root) && !isZeroDigest(root.schema_graph_root);
}

bool isValidOperationKind(AuthorityQuarantineOperationKind kind) noexcept
{
    switch (kind)
    {
        case AuthorityQuarantineOperationKind::Read:
        case AuthorityQuarantineOperationKind::Write:
        case AuthorityQuarantineOperationKind::Mutation:
        case AuthorityQuarantineOperationKind::DDL:
        case AuthorityQuarantineOperationKind::Attach: return true;
    }
    return false;
}

bool isValidOperationTiming(AuthorityQuarantineOperationTiming timing) noexcept
{
    switch (timing)
    {
        case AuthorityQuarantineOperationTiming::New:
        case AuthorityQuarantineOperationTiming::StartedBeforeQuarantine: return true;
    }
    return false;
}

bool isSidecarObject(const SchemaObjectID & object) noexcept
{
    if (!object.isValid())
        return false;
    return object.kind == SchemaObjectKind::Table || object.kind == SchemaObjectKind::View || object.kind == SchemaObjectKind::Dictionary
        || object.kind == SchemaObjectKind::SyntheticTestObject;
}

int compareUUID(const UUID & lhs, const UUID & rhs) noexcept
{
    const auto lhs_bytes = uuidToCanonicalBytes(lhs);
    const auto rhs_bytes = uuidToCanonicalBytes(rhs);
    if (lhs_bytes == rhs_bytes)
        return 0;
    return lhs_bytes < rhs_bytes ? -1 : 1;
}

int compareDefinitionIdentity(const DefinitionIdentity & lhs, const DefinitionIdentity & rhs) noexcept
{
    if (const int database = compareUUID(lhs.database_uuid, rhs.database_uuid))
        return database;
    if (const int type = compareUUID(lhs.type_uuid, rhs.type_uuid))
        return type;
    if (lhs.revision == rhs.revision)
        return 0;
    return lhs.revision < rhs.revision ? -1 : 1;
}

bool hasCanonicalDefinitionClosure(std::span<const DefinitionIdentity> definitions, const UUID & database_uuid) noexcept
{
    for (size_t index = 0; index < definitions.size(); ++index)
    {
        const auto & definition = definitions[index];
        if (definition.database_uuid != database_uuid || definition.database_uuid == UUIDHelpers::Nil
            || definition.type_uuid == UUIDHelpers::Nil || definition.revision == 0)
            return false;
        if (index != 0
            && (definitions[index - 1].type_uuid == definition.type_uuid
                || compareDefinitionIdentity(definitions[index - 1], definition) >= 0))
            return false;
    }
    return true;
}

bool isValidObjectImage(const AuthorityObjectImageIdentity & image, const UUID & database_uuid) noexcept
{
    return isSidecarObject(image.object) && image.object.database_uuid == database_uuid && image.object_schema_revision != 0
        && !isZeroDigest(image.sidecar_hash) && !isZeroDigest(image.physical_schema_fingerprint);
}

bool validateLimits(const AuthorityQuarantineAdmissionLimits & limits) noexcept
{
    constexpr AuthorityQuarantineAdmissionLimits maxima;
    const auto valid = [](UInt64 value, UInt64 maximum) { return value != 0 && value <= maximum; };
    return valid(limits.maximum_touched_objects, maxima.maximum_touched_objects)
        && valid(limits.maximum_continuation_proofs, maxima.maximum_continuation_proofs)
        && valid(limits.maximum_required_definitions_per_proof, maxima.maximum_required_definitions_per_proof)
        && valid(limits.maximum_inspected_definition_items, maxima.maximum_inspected_definition_items)
        && valid(limits.maximum_work_units, maxima.maximum_work_units)
        && valid(limits.maximum_evidence_canonical_bytes, maxima.maximum_evidence_canonical_bytes);
}

UInt64 treeLookupWorkBound(UInt64 maximum_objects) noexcept
{
    UInt64 levels = 0;
    while (maximum_objects != 0)
    {
        ++levels;
        maximum_objects >>= 1;
    }
    return 2 * levels + 4;
}

bool hasExpectedStampStatistics(const AuthorityVerificationStamp & stamp, UInt64 definition_count) noexcept
{
    UInt64 expected_work = 0;
    UInt64 expected_retained_bytes = verification_stamp_base_canonical_bytes;
    if (!checkedMultiply(definition_count, 4, expected_work) || !checkedAdd(expected_work, 1, expected_work)
        || !addProduct(expected_retained_bytes, definition_count, definition_identity_canonical_bytes))
        return false;

    const auto & statistics = stamp.getConstructionStatistics();
    return statistics.required_definition_items == definition_count && statistics.touched_authority_key_items == 0
        && statistics.work_units == expected_work && statistics.retained_canonical_bytes == expected_retained_bytes;
}

bool prepareProspectiveStatistics(
    const AuthorityQuarantinePlan & quarantine,
    const AuthorityQuarantineOperationView & operation,
    const AuthorityQuarantineAdmissionLimits & limits,
    AuthorityQuarantineAdmissionStatistics & statistics,
    AdmissionStatus & failure) noexcept
{
    statistics.quarantined_objects = toUInt64(quarantine.getQuarantinedObjects().size());
    statistics.touched_objects = toUInt64(operation.sorted_unique_touched_objects.size());
    statistics.continuation_proofs = toUInt64(operation.sorted_unique_continuation_proofs.size());
    if (statistics.quarantined_objects > AuthorityQuarantinePlanLimits{}.maximum_closure_objects
        || statistics.touched_objects > limits.maximum_touched_objects
        || statistics.continuation_proofs > limits.maximum_continuation_proofs)
    {
        failure = AdmissionStatus::LimitExceeded;
        return false;
    }

    UInt64 nonnull_stamps = 0;
    for (const auto & proof : operation.sorted_unique_continuation_proofs)
    {
        const UInt64 current_count = toUInt64(proof.sorted_unique_current_required_definitions.size());
        if (current_count > limits.maximum_required_definitions_per_proof
            || !checkedAdd(statistics.current_definition_items, current_count, statistics.current_definition_items))
        {
            failure = current_count > limits.maximum_required_definitions_per_proof ? AdmissionStatus::LimitExceeded
                                                                                    : AdmissionStatus::ArithmeticOverflow;
            return false;
        }

        if (!proof.last_verification_stamp)
            continue;
        ++nonnull_stamps;
        const UInt64 stamped_count = toUInt64(proof.last_verification_stamp->getRequiredDefinitions().size());
        if (stamped_count > limits.maximum_required_definitions_per_proof
            || !checkedAdd(statistics.stamped_definition_items, stamped_count, statistics.stamped_definition_items))
        {
            failure = stamped_count > limits.maximum_required_definitions_per_proof ? AdmissionStatus::LimitExceeded
                                                                                    : AdmissionStatus::ArithmeticOverflow;
            return false;
        }
    }

    UInt64 inspected_definition_items = 0;
    if (!checkedAdd(statistics.current_definition_items, statistics.stamped_definition_items, inspected_definition_items))
    {
        failure = AdmissionStatus::ArithmeticOverflow;
        return false;
    }
    if (inspected_definition_items > limits.maximum_inspected_definition_items)
    {
        failure = AdmissionStatus::LimitExceeded;
        return false;
    }

    statistics.evidence_canonical_bytes = operation_base_canonical_bytes;
    if (!addProduct(statistics.evidence_canonical_bytes, statistics.touched_objects, schema_object_identity_canonical_bytes)
        || !addProduct(statistics.evidence_canonical_bytes, statistics.continuation_proofs, continuation_proof_base_canonical_bytes)
        || !addProduct(statistics.evidence_canonical_bytes, statistics.current_definition_items, definition_identity_canonical_bytes)
        || !addProduct(statistics.evidence_canonical_bytes, nonnull_stamps, verification_stamp_base_canonical_bytes)
        || !addProduct(statistics.evidence_canonical_bytes, statistics.stamped_definition_items, definition_identity_canonical_bytes))
    {
        failure = AdmissionStatus::ArithmeticOverflow;
        return false;
    }
    if (statistics.evidence_canonical_bytes > limits.maximum_evidence_canonical_bytes)
    {
        failure = AdmissionStatus::LimitExceeded;
        return false;
    }

    statistics.work_units = 4;
    const UInt64 lookup_work = treeLookupWorkBound(statistics.quarantined_objects);
    if (!addProduct(statistics.work_units, statistics.touched_objects, lookup_work + 4)
        || !addProduct(statistics.work_units, statistics.continuation_proofs, 12)
        || !addProduct(statistics.work_units, statistics.current_definition_items, 3)
        || !addProduct(statistics.work_units, statistics.stamped_definition_items, 2))
    {
        failure = AdmissionStatus::ArithmeticOverflow;
        return false;
    }
    if (statistics.work_units > limits.maximum_work_units)
    {
        failure = AdmissionStatus::LimitExceeded;
        return false;
    }
    return true;
}

AdmissionStatus validateCanonicalEvidence(
    const AuthorityQuarantinePlan & quarantine,
    const AuthorityQuarantineOperationView & operation,
    AuthorityQuarantineAdmissionStatistics & statistics) noexcept
{
    const auto & database_uuid = quarantine.getRoot().authority_root.database_uuid;
    for (size_t index = 0; index < operation.sorted_unique_touched_objects.size(); ++index)
    {
        const auto & object = operation.sorted_unique_touched_objects[index];
        if (!object.isValid() || object.database_uuid != database_uuid
            || (index != 0 && !(operation.sorted_unique_touched_objects[index - 1] < object)))
            return AdmissionStatus::NonCanonicalTouchSet;
        if (quarantine.contains(object))
            ++statistics.quarantined_touched_objects;
    }

    for (size_t index = 0; index < operation.sorted_unique_continuation_proofs.size(); ++index)
    {
        const auto & proof = operation.sorted_unique_continuation_proofs[index];
        if (!isValidObjectImage(proof.current_object, database_uuid)
            || !hasCanonicalDefinitionClosure(proof.sorted_unique_current_required_definitions, database_uuid))
            return AdmissionStatus::NonCanonicalContinuationEvidence;
        if (index != 0 && !(operation.sorted_unique_continuation_proofs[index - 1].current_object.object < proof.current_object.object))
            return AdmissionStatus::NonCanonicalContinuationEvidence;
    }
    return AdmissionStatus::AllowedUnaffected;
}

AdmissionStatus
validateReadContinuation(const AuthorityQuarantinePlan & quarantine, const AuthorityQuarantineOperationView & operation) noexcept
{
    if (!operation.continuation_proof_set_is_complete
        || operation.sorted_unique_continuation_proofs.size() != operation.sorted_unique_touched_objects.size())
        return AdmissionStatus::IncompleteContinuationEvidence;
    if (!isValidRootIdentity(operation.pinned_root) || operation.pinned_root != quarantine.getRoot())
        return AdmissionStatus::ContinuationRootMismatch;

    for (size_t index = 0; index < operation.sorted_unique_touched_objects.size(); ++index)
    {
        const auto & touched_object = operation.sorted_unique_touched_objects[index];
        const auto & proof = operation.sorted_unique_continuation_proofs[index];
        if (proof.current_object.object != touched_object)
            return AdmissionStatus::IncompleteContinuationEvidence;
        if (!proof.last_verification_stamp)
            return AdmissionStatus::IncompleteContinuationEvidence;

        const auto & stamp = *proof.last_verification_stamp;
        if (stamp.getVerifiedRoot() != operation.pinned_root.authority_root || stamp.getVerifiedObject() != proof.current_object)
            return AdmissionStatus::ContinuationStampMismatch;

        const auto stamped_definitions = stamp.getRequiredDefinitions();
        const auto current_definitions = proof.sorted_unique_current_required_definitions;
        if (!hasCanonicalDefinitionClosure(stamped_definitions, operation.pinned_root.authority_root.database_uuid)
            || stamped_definitions.size() != current_definitions.size()
            || !std::equal(current_definitions.begin(), current_definitions.end(), stamped_definitions.begin())
            || !hasExpectedStampStatistics(stamp, toUInt64(stamped_definitions.size())))
            return AdmissionStatus::ContinuationDependencyMismatch;
    }
    return AdmissionStatus::AllowedReadContinuation;
}

}

AuthorityQuarantineAdmissionDecision decideAuthorityQuarantineAdmission(
    const AuthorityQuarantinePlan & quarantine,
    const AuthorityQuarantineOperationView & operation,
    const AuthorityQuarantineAdmissionLimits & limits) noexcept
{
    AuthorityQuarantineAdmissionDecision result;
    if (!validateLimits(limits))
        return result;
    if (!isValidOperationKind(operation.kind))
    {
        result.status = AdmissionStatus::InvalidOperationKind;
        return result;
    }
    if (!isValidOperationTiming(operation.timing))
    {
        result.status = AdmissionStatus::InvalidOperationTiming;
        return result;
    }

    const auto & quarantine_root = quarantine.getRoot();
    if (!isValidRootIdentity(quarantine_root.authority_root) || isZeroDigest(quarantine_root.schema_graph_root))
    {
        result.status = AdmissionStatus::InvalidQuarantineIdentity;
        return result;
    }
    /// Membership in a quarantine closure is meaningful only for the exact
    /// authority and dependency-graph roots from which that closure was
    /// computed. Even an apparently unaffected object must not be admitted
    /// against a different root without first rebuilding the quarantine plan.
    if (!isValidRootIdentity(operation.pinned_root) || operation.pinned_root != quarantine_root)
    {
        result.status = AdmissionStatus::OperationRootMismatch;
        return result;
    }
    if (!operation.touch_set_is_complete)
    {
        result.status = AdmissionStatus::IncompleteTouchSet;
        return result;
    }

    AdmissionStatus prospective_failure = AdmissionStatus::InvalidConfiguration;
    if (!prepareProspectiveStatistics(quarantine, operation, limits, result.statistics, prospective_failure))
    {
        result.status = prospective_failure;
        return result;
    }

    const AdmissionStatus evidence_status = validateCanonicalEvidence(quarantine, operation, result.statistics);
    if (evidence_status != AdmissionStatus::AllowedUnaffected)
    {
        result.status = evidence_status;
        return result;
    }
    if (result.statistics.quarantined_touched_objects == 0)
    {
        result.status = AdmissionStatus::AllowedUnaffected;
        return result;
    }
    if (operation.timing == AuthorityQuarantineOperationTiming::New)
    {
        result.status = AdmissionStatus::NewOperationTouchesQuarantine;
        return result;
    }
    if (operation.kind != AuthorityQuarantineOperationKind::Read)
    {
        result.status = AdmissionStatus::NonReadContinuationRejected;
        return result;
    }

    result.status = validateReadContinuation(quarantine, operation);
    return result;
}

}
