#include <Databases/UDT/AuthorityVerificationStamp.h>

#include <DataTypes/UDT/CanonicalHash.h>

#include <algorithm>
#include <limits>
#include <utility>

namespace DB::UDT
{
namespace
{

using StampError = AuthorityVerificationStampError;

constexpr UInt64 canonical_uuid_bytes = 16;
constexpr UInt64 authority_root_identity_canonical_bytes = canonical_uuid_bytes + sizeof(UInt64) + sizeof(Digest);
constexpr UInt64 schema_object_identity_canonical_bytes = sizeof(UInt8) + 2 * canonical_uuid_bytes;
constexpr UInt64 object_image_identity_canonical_bytes = schema_object_identity_canonical_bytes + sizeof(UInt64) + 2 * sizeof(Digest);
constexpr UInt64 definition_identity_canonical_bytes = 2 * canonical_uuid_bytes + sizeof(UInt64);
constexpr UInt64 stamp_count_canonical_bytes = sizeof(UInt64);
constexpr UInt64 stamp_base_canonical_bytes
    = authority_root_identity_canonical_bytes + object_image_identity_canonical_bytes + sizeof(Digest) + stamp_count_canonical_bytes;

[[noreturn]] void fail(StampError::Code code, std::string_view message)
{
    throw StampError(code, message);
}

UInt64 toUInt64(size_t value) noexcept
{
    static_assert(sizeof(size_t) <= sizeof(UInt64));
    return static_cast<UInt64>(value);
}

UInt64 checkedAdd(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        fail(StampError::Code::ArithmeticOverflow, message);
    return lhs + rhs;
}

UInt64 checkedMultiply(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (lhs != 0 && rhs > std::numeric_limits<UInt64>::max() / lhs)
        fail(StampError::Code::ArithmeticOverflow, message);
    return lhs * rhs;
}

bool isZeroDigest(const Digest & digest) noexcept
{
    return std::all_of(digest.begin(), digest.end(), [](CanonicalByte byte) { return byte == 0; });
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

bool isValidDefinitionIdentity(const DefinitionIdentity & identity, const UUID & database_uuid) noexcept
{
    return identity.database_uuid == database_uuid && identity.database_uuid != UUIDHelpers::Nil && identity.type_uuid != UUIDHelpers::Nil
        && identity.revision != 0;
}

bool hasCanonicalDefinitionClosure(std::span<const DefinitionIdentity> definitions, const UUID & database_uuid) noexcept
{
    for (size_t index = 0; index < definitions.size(); ++index)
    {
        if (!isValidDefinitionIdentity(definitions[index], database_uuid))
            return false;
        if (index == 0)
            continue;
        if (definitions[index - 1].type_uuid == definitions[index].type_uuid
            || compareDefinitionIdentity(definitions[index - 1], definitions[index]) >= 0)
            return false;
    }
    return true;
}

bool isValidAuthorityKey(const AuthorityInventoryKey & key) noexcept
{
    if (key.format_version != authority_inventory_format_version || key.object_uuid == UUIDHelpers::Nil)
        return false;
    return key.record_kind == AuthorityInventoryRecordKind::TypeDefinition
        || key.record_kind == AuthorityInventoryRecordKind::SidecarExpectation;
}

bool hasCanonicalTouchedKeys(std::span<const AuthorityInventoryKey> keys) noexcept
{
    for (size_t index = 0; index < keys.size(); ++index)
    {
        if (!isValidAuthorityKey(keys[index]))
            return false;
        if (index != 0 && !authorityInventoryKeyLess(keys[index - 1], keys[index]))
            return false;
    }
    return true;
}

void validateLimits(const AuthorityVerificationStampLimits & limits)
{
    constexpr AuthorityVerificationStampLimits maxima;
    const auto valid = [](UInt64 value, UInt64 maximum) { return value != 0 && value <= maximum; };
    if (!valid(limits.maximum_required_definitions, maxima.maximum_required_definitions)
        || !valid(limits.maximum_touched_authority_keys, maxima.maximum_touched_authority_keys)
        || !valid(limits.maximum_work_units, maxima.maximum_work_units)
        || !valid(limits.maximum_retained_canonical_bytes, maxima.maximum_retained_canonical_bytes))
        fail(StampError::Code::InvalidConfiguration, "authority verification-stamp limits are invalid");
}

void admitItems(UInt64 count, UInt64 maximum, std::string_view message)
{
    if (count > maximum)
        fail(StampError::Code::LimitExceeded, message);
}

void admitWork(UInt64 work, const AuthorityVerificationStampLimits & limits)
{
    if (work > limits.maximum_work_units)
        fail(StampError::Code::LimitExceeded, "authority verification-stamp work exceeds its limit");
}

UInt64 retainedStampCanonicalBytes(UInt64 required_definition_count)
{
    return checkedAdd(
        stamp_base_canonical_bytes,
        checkedMultiply(
            required_definition_count,
            definition_identity_canonical_bytes,
            "authority verification-stamp retained canonical bytes overflow UInt64"),
        "authority verification-stamp retained canonical bytes overflow UInt64");
}

bool touchesCoveredAuthorityKey(
    const AuthorityVerificationStamp & stamp, std::span<const AuthorityInventoryKey> sorted_unique_touched_authority_keys) noexcept
{
    const auto required_definitions = stamp.getRequiredDefinitions();
    size_t definition_index = 0;
    for (const auto & key : sorted_unique_touched_authority_keys)
    {
        if (key.record_kind == AuthorityInventoryRecordKind::SidecarExpectation)
        {
            if (key.object_uuid == stamp.getVerifiedObject().object.object_uuid)
                return true;
            continue;
        }

        while (definition_index < required_definitions.size()
               && compareUUID(required_definitions[definition_index].type_uuid, key.object_uuid) < 0)
            ++definition_index;
        if (definition_index < required_definitions.size() && required_definitions[definition_index].type_uuid == key.object_uuid)
            return true;
    }
    return false;
}

}

AuthorityVerificationStampError::AuthorityVerificationStampError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

AuthorityVerificationStamp::AuthorityVerificationStamp(
    AuthorityRootIdentity verified_root_,
    AuthorityObjectImageIdentity verified_object_,
    Digest required_definitions_digest_,
    std::vector<DefinitionIdentity> required_definitions_,
    AuthorityVerificationStampStatistics construction_statistics_) noexcept
    : verified_root(std::move(verified_root_))
    , verified_object(std::move(verified_object_))
    , required_definitions_digest(std::move(required_definitions_digest_))
    , required_definitions(std::move(required_definitions_))
    , construction_statistics(std::move(construction_statistics_))
{
}

AuthorityVerificationStamp::Ptr AuthorityVerificationStamp::create(
    const VerifiedAuthorityObjectIntegrity & verification,
    std::span<const DefinitionIdentity> sorted_unique_required_definitions,
    const AuthorityRootIdentity & verified_root,
    const AuthorityVerificationStampLimits & limits)
{
    validateLimits(limits);
    const UInt64 required_definition_count = toUInt64(sorted_unique_required_definitions.size());
    admitItems(
        required_definition_count,
        limits.maximum_required_definitions,
        "authority verification stamp exceeds its required-definition limit");
    const UInt64 work_units = checkedAdd(
        1,
        checkedMultiply(required_definition_count, 4, "authority verification-stamp work overflows UInt64"),
        "authority verification-stamp work overflows UInt64");
    admitWork(work_units, limits);
    const UInt64 retained_canonical_bytes = retainedStampCanonicalBytes(required_definition_count);
    if (retained_canonical_bytes > limits.maximum_retained_canonical_bytes)
        fail(StampError::Code::LimitExceeded, "authority verification stamp exceeds its retained canonical-byte limit");

    if (verified_root.database_uuid == UUIDHelpers::Nil || verified_root.database_catalog_epoch == 0
        || isZeroDigest(verified_root.authority_anchor))
        fail(StampError::Code::InvalidRootIdentity, "verified authority-root identity is invalid");
    if (verification.database_uuid != verified_root.database_uuid
        || verification.database_catalog_epoch != verified_root.database_catalog_epoch
        || verification.authority_anchor != verified_root.authority_anchor)
        fail(StampError::Code::InvalidVerification, "integrity verification belongs to another authority root");
    if (!isSidecarObject(verification.object) || verification.object.database_uuid != verified_root.database_uuid
        || verification.object_schema_revision == 0 || isZeroDigest(verification.sidecar_hash)
        || isZeroDigest(verification.physical_schema_fingerprint))
        fail(StampError::Code::InvalidVerification, "integrity verification object identity is invalid");
    if (verification.required_definition_count == 0 || verification.required_definition_count != required_definition_count)
        fail(StampError::Code::InvalidVerification, "integrity verification dependency count differs from its exact closure");
    if (!hasCanonicalDefinitionClosure(sorted_unique_required_definitions, verified_root.database_uuid))
    {
        for (const auto & identity : sorted_unique_required_definitions)
        {
            if (!isValidDefinitionIdentity(identity, verified_root.database_uuid))
                fail(StampError::Code::InvalidDefinitionIdentity, "verification stamp contains an invalid definition identity");
        }
        fail(
            StampError::Code::NonCanonicalDefinitionClosure,
            "verification-stamp definition closure is not strictly sorted and unique by authority key");
    }
    const Digest required_definitions_digest
        = computeVerifiedRequiredDefinitionsDigest(sorted_unique_required_definitions, limits.maximum_required_definitions);
    if (verification.required_definitions_digest != required_definitions_digest)
        fail(StampError::Code::InvalidVerification, "integrity verification digest differs from its exact dependency closure");

    AuthorityVerificationStampStatistics statistics{
        .required_definition_items = required_definition_count,
        .touched_authority_key_items = 0,
        .work_units = work_units,
        .retained_canonical_bytes = retained_canonical_bytes,
    };
    std::vector<DefinitionIdentity> required_definitions(
        sorted_unique_required_definitions.begin(), sorted_unique_required_definitions.end());
    return Ptr(new AuthorityVerificationStamp(
        verified_root,
        {
            .object = verification.object,
            .object_schema_revision = verification.object_schema_revision,
            .sidecar_hash = verification.sidecar_hash,
            .physical_schema_fingerprint = verification.physical_schema_fingerprint,
        },
        required_definitions_digest,
        std::move(required_definitions),
        statistics));
}

AuthorityVerificationStampReuseDecision decideAuthorityVerificationStampReuse(
    const AuthorityVerificationStamp & stamp,
    const AuthorityRootPublicationProofView & publication_proof,
    const AuthorityObjectImageIdentity & current_object,
    std::span<const DefinitionIdentity> sorted_unique_current_required_definitions,
    const AuthorityVerificationStampLimits & limits)
{
    validateLimits(limits);
    const auto & transition = publication_proof.transition;
    const auto sorted_unique_touched_authority_keys = publication_proof.sorted_unique_touched_authority_keys;
    const auto stamped_definitions = stamp.getRequiredDefinitions();
    const UInt64 required_definition_count = toUInt64(sorted_unique_current_required_definitions.size());
    const UInt64 touched_key_count = toUInt64(sorted_unique_touched_authority_keys.size());
    admitItems(
        toUInt64(stamped_definitions.size()),
        limits.maximum_required_definitions,
        "authority verification stamp exceeds the reuse required-definition limit");
    admitItems(
        required_definition_count, limits.maximum_required_definitions, "current dependency closure exceeds the verification-stamp limit");
    admitItems(
        touched_key_count, limits.maximum_touched_authority_keys, "authority-root touched-key proof exceeds the verification-stamp limit");
    const UInt64 definition_work
        = checkedMultiply(required_definition_count, 4, "authority verification-stamp reuse work overflows UInt64");
    const UInt64 touched_key_work = checkedMultiply(touched_key_count, 2, "authority verification-stamp reuse work overflows UInt64");
    const UInt64 work_units = checkedAdd(
        checkedAdd(1, definition_work, "authority verification-stamp reuse work overflows UInt64"),
        touched_key_work,
        "authority verification-stamp reuse work overflows UInt64");
    admitWork(work_units, limits);
    const UInt64 retained_canonical_bytes = stamp.getConstructionStatistics().retained_canonical_bytes;
    if (retained_canonical_bytes != retainedStampCanonicalBytes(toUInt64(stamped_definitions.size()))
        || retained_canonical_bytes > limits.maximum_retained_canonical_bytes)
        fail(StampError::Code::LimitExceeded, "authority verification stamp exceeds the reuse retained canonical-byte limit");

    AuthorityVerificationStampReuseDecision result{
        .status = AuthorityVerificationStampReuseStatus::RootTransitionUnproven,
        .statistics = {
            .required_definition_items = required_definition_count,
            .touched_authority_key_items = touched_key_count,
            .work_units = work_units,
            .retained_canonical_bytes = retained_canonical_bytes,
        },
    };

    const auto & stamped_root = stamp.getVerifiedRoot();
    if (transition.previous.database_uuid != stamped_root.database_uuid || transition.next.database_uuid != stamped_root.database_uuid)
    {
        result.status = AuthorityVerificationStampReuseStatus::DatabaseChanged;
        return result;
    }
    if (transition.previous != stamped_root || transition.next.database_catalog_epoch <= stamped_root.database_catalog_epoch
        || isZeroDigest(transition.next.authority_anchor) || transition.next.authority_anchor == stamped_root.authority_anchor)
        return result;

    const auto & stamped_object = stamp.getVerifiedObject();
    if (current_object.object.database_uuid != stamped_root.database_uuid)
    {
        result.status = AuthorityVerificationStampReuseStatus::DatabaseChanged;
        return result;
    }
    if (!isSidecarObject(current_object.object) || current_object.object != stamped_object.object)
    {
        result.status = AuthorityVerificationStampReuseStatus::ObjectChanged;
        return result;
    }
    if (current_object.object_schema_revision == 0 || current_object.object_schema_revision != stamped_object.object_schema_revision)
    {
        result.status = AuthorityVerificationStampReuseStatus::ObjectSchemaRevisionChanged;
        return result;
    }
    if (current_object.sidecar_hash != stamped_object.sidecar_hash)
    {
        result.status = AuthorityVerificationStampReuseStatus::SidecarChanged;
        return result;
    }
    if (current_object.physical_schema_fingerprint != stamped_object.physical_schema_fingerprint)
    {
        result.status = AuthorityVerificationStampReuseStatus::PhysicalSchemaChanged;
        return result;
    }
    if (required_definition_count != stamped_definitions.size()
        || !hasCanonicalDefinitionClosure(sorted_unique_current_required_definitions, stamped_root.database_uuid)
        || !std::equal(
            sorted_unique_current_required_definitions.begin(),
            sorted_unique_current_required_definitions.end(),
            stamped_definitions.begin()))
    {
        result.status = AuthorityVerificationStampReuseStatus::DependencyClosureUnproven;
        return result;
    }
    if (computeVerifiedRequiredDefinitionsDigest(sorted_unique_current_required_definitions, limits.maximum_required_definitions)
        != stamp.getRequiredDefinitionsDigest())
    {
        result.status = AuthorityVerificationStampReuseStatus::DependencyClosureUnproven;
        return result;
    }
    if (!hasCanonicalTouchedKeys(sorted_unique_touched_authority_keys))
    {
        result.status = AuthorityVerificationStampReuseStatus::TouchedAuthorityKeysUnproven;
        return result;
    }
    if (touchesCoveredAuthorityKey(stamp, sorted_unique_touched_authority_keys))
    {
        result.status = AuthorityVerificationStampReuseStatus::CoveredAuthorityKeyTouched;
        return result;
    }

    result.status = AuthorityVerificationStampReuseStatus::Reusable;
    return result;
}

}
