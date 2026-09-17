#include <Databases/UDT/AuthorityIntegrityVerifier.h>

#include <Databases/SchemaObjectDependencyGraph.h>
#include <Databases/UDT/AuthorityRoot.h>

#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/ResourceLimits.h>
#include <DataTypes/UDT/SidecarExpectationRecord.h>

#include <algorithm>
#include <array>
#include <limits>
#include <tuple>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

using VerifierError = AuthorityIntegrityVerifierError;

constexpr UInt64 maximum_verifier_work_units = 67'108'864;
constexpr UInt64 maximum_verifier_transient_bytes = 1ULL << 30;
constexpr UInt64 maximum_verified_required_definitions = 65'536;
constexpr std::string_view required_definitions_digest_domain = "ClickHouse UDT verified required definitions V1";

[[noreturn]] void fail(VerifierError::Code code, std::string_view message)
{
    throw VerifierError(code, message);
}

UInt64 checkedAdd(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        fail(VerifierError::Code::LimitExceeded, message);
    return lhs + rhs;
}

UInt64 checkedMultiply(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (lhs != 0 && rhs > std::numeric_limits<UInt64>::max() / lhs)
        fail(VerifierError::Code::LimitExceeded, message);
    return lhs * rhs;
}

UInt64 toUInt64(size_t value) noexcept
{
    static_assert(sizeof(size_t) <= sizeof(UInt64));
    return static_cast<UInt64>(value);
}

void updateUInt64LE(CanonicalHasher & hasher, UInt64 value)
{
    std::array<CanonicalByte, sizeof(UInt64)> bytes{};
    for (size_t index = 0; index < bytes.size(); ++index)
        bytes[index] = static_cast<CanonicalByte>(value >> (8 * index));
    hasher.update(bytes);
}

template <typename IdentityAt>
Digest computeRequiredDefinitionsDigest(size_t count, IdentityAt && identity_at)
{
    CanonicalHasher hasher(required_definitions_digest_domain);
    updateUInt64LE(hasher, toUInt64(count));
    for (size_t index = 0; index < count; ++index)
    {
        const auto & identity = identity_at(index);
        hasher.updateUUID(identity.database_uuid);
        hasher.updateUUID(identity.type_uuid);
        updateUInt64LE(hasher, identity.revision);
    }
    return hasher.finalize();
}

UInt64 varUIntSize(UInt64 value) noexcept
{
    UInt64 result = 1;
    while (value >= 0x80)
    {
        value >>= 7;
        ++result;
    }
    return result;
}

UInt64 framedSize(std::string_view value)
{
    const UInt64 size = toUInt64(value.size());
    return checkedAdd(varUIntSize(size), size, "authority verifier framed byte size overflows UInt64");
}

UInt64 encodedRecordSize(const Record & record)
{
    UInt64 result = 0;
    const auto add = [&](UInt64 value) { result = checkedAdd(result, value, "authority verifier record size overflows UInt64"); };

    add(sizeof(UInt16));
    add(2 * sizeof(CanonicalUUID));
    add(sizeof(UInt64));
    add(framedSize(record.normalized_name));
    add(framedSize(record.normalized_local_name));
    add(varUIntSize(toUInt64(record.parameters.size())));
    for (const auto & parameter : record.parameters)
    {
        add(sizeof(UInt8));
        add(framedSize(parameter.normalized_name));
    }
    add(sizeof(UInt8));
    if (record.decreasing_parameter)
        add(sizeof(UInt16));
    add(4 * sizeof(UInt16));
    add(2 * sizeof(UInt8));
    add(sizeof(Digest));
    add(framedSize(record.canonical_definition_sql));
    add(framedSize(record.canonical_physical_template_sql));
    add(framedSize(record.canonical_template_ir));
    add(varUIntSize(toUInt64(record.dependencies.size())));
    add(checkedMultiply(
        toUInt64(record.dependencies.size()),
        sizeof(CanonicalUUID) + sizeof(UInt64) + sizeof(Digest),
        "authority verifier dependency byte size overflows UInt64"));
    add(3 * sizeof(Digest));
    add(framedSize(record.encoded_checker_certificate));
    add(sizeof(Digest));
    add(3 * sizeof(UInt64));
    add(sizeof(CanonicalUUID));
    add(framedSize(record.owner_display_name));
    add(framedSize(record.comment));
    add(sizeof(Int64));
    add(sizeof(UInt8));
    add(2 * sizeof(UInt16));
    return result;
}

int compareUUID(const UUID & lhs, const UUID & rhs) noexcept
{
    const auto lhs_bytes = uuidToCanonicalBytes(lhs);
    const auto rhs_bytes = uuidToCanonicalBytes(rhs);
    if (lhs_bytes == rhs_bytes)
        return 0;
    return lhs_bytes < rhs_bytes ? -1 : 1;
}

int compareIdentity(const DefinitionIdentity & lhs, const DefinitionIdentity & rhs) noexcept
{
    if (const int database = compareUUID(lhs.database_uuid, rhs.database_uuid))
        return database;
    if (const int type = compareUUID(lhs.type_uuid, rhs.type_uuid))
        return type;
    if (lhs.revision == rhs.revision)
        return 0;
    return lhs.revision < rhs.revision ? -1 : 1;
}

SchemaObjectID definitionObject(const DefinitionIdentity & identity)
{
    return {
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = identity.database_uuid,
        .object_uuid = identity.type_uuid,
    };
}

AuthorityInventoryKey expectationInventoryKey(const SchemaObjectID & object)
{
    return {
        .record_kind = AuthorityInventoryRecordKind::SidecarExpectation,
        .object_uuid = object.object_uuid,
    };
}

AuthorityInventoryKey definitionInventoryKey(const DefinitionIdentity & identity)
{
    return {
        .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
        .object_uuid = identity.type_uuid,
    };
}

UInt64 encodedAuthorityStateSize(const AuthorityState & state)
{
    return checkedAdd(
        sizeof(UInt16) + sizeof(CanonicalUUID) + 2 * sizeof(UInt64) + 3 * sizeof(Digest),
        varUIntSize(state.leaf_count),
        "authority verifier state size overflows UInt64");
}

Digest hashCanonicalSidecar(std::string_view canonical_bytes, const PersistedTypeReferences & references)
{
    const std::string_view hash_domain = references.format_version == persisted_type_references_format_version_v2
        ? persisted_type_references_sidecar_hash_domain_v2
        : persisted_type_references_sidecar_hash_domain;
    CanonicalHasher hasher(hash_domain);
    std::array<CanonicalByte, 10> encoded_size{};
    UInt64 remaining_size = toUInt64(canonical_bytes.size());
    size_t encoded_size_bytes = 0;
    do
    {
        CanonicalByte byte = static_cast<CanonicalByte>(remaining_size & 0x7f);
        remaining_size >>= 7;
        if (remaining_size)
            byte = static_cast<CanonicalByte>(byte | 0x80);
        encoded_size[encoded_size_bytes++] = byte;
    } while (remaining_size);
    hasher.update(std::span(encoded_size).first(encoded_size_bytes));
    hasher.update(canonical_bytes);
    hasher.updateUUID(references.object.database_uuid);
    const std::array<CanonicalByte, 1> object_kind{static_cast<CanonicalByte>(references.object.kind)};
    hasher.update(object_kind);
    hasher.updateUUID(references.object.object_uuid);
    std::array<CanonicalByte, sizeof(UInt64)> revision{};
    for (size_t index = 0; index < revision.size(); ++index)
        revision[index] = static_cast<CanonicalByte>(references.object_schema_revision >> (8 * index));
    hasher.update(revision);
    hasher.update(references.physical_schema_fingerprint);
    return hasher.finalize();
}

void validateLimits(const AuthorityIntegrityVerifierLimits & limits)
{
    constexpr AuthorityStateLimits authority_maxima;
    constexpr PersistedTypeReferencesLimits persisted_maxima;
    constexpr RecordLimits record_maxima;
    const auto valid = [](UInt64 value, UInt64 maximum) { return value != 0 && value <= maximum; };

    if (!valid(limits.authority_state.maximum_leaves, authority_maxima.maximum_leaves)
        || !valid(limits.authority_state.maximum_encoded_bytes, authority_maxima.maximum_encoded_bytes))
        fail(VerifierError::Code::InvalidConfiguration, "authority verifier state limits are invalid");

    const auto & persisted = limits.persisted_references;
    if (!valid(persisted.maximum_sidecar_bytes, persisted_maxima.maximum_sidecar_bytes)
        || !valid(persisted.maximum_descriptors, persisted_maxima.maximum_descriptors)
        || !valid(persisted.maximum_occurrence_paths, persisted_maxima.maximum_occurrence_paths)
        || !valid(persisted.maximum_path_depth, persisted_maxima.maximum_path_depth)
        || !valid(persisted.maximum_canonical_arguments_bytes, persisted_maxima.maximum_canonical_arguments_bytes)
        || !valid(persisted.maximum_canonical_physical_type_bytes, persisted_maxima.maximum_canonical_physical_type_bytes)
        || !valid(persisted.maximum_qualified_name_bytes, persisted_maxima.maximum_qualified_name_bytes)
        || !valid(persisted.maximum_text_bytes, persisted_maxima.maximum_text_bytes))
        fail(VerifierError::Code::InvalidConfiguration, "authority verifier sidecar limits are invalid");

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
        fail(VerifierError::Code::InvalidConfiguration, "authority verifier definition-record limits are invalid");

    const UInt64 maximum_canonical_bytes = getResourceImplementationLimits().get(ResourceLimit::DeterministicCatalogBytesPerDatabase);
    constexpr SchemaObjectDependencyGraphLimits graph_maxima;
    if (!valid(limits.maximum_required_definitions, persisted_maxima.maximum_descriptors)
        || !valid(limits.maximum_outgoing_dependencies, graph_maxima.maximum_edges_per_node)
        || !valid(limits.maximum_canonical_bytes_hashed, maximum_canonical_bytes)
        || !valid(limits.maximum_work_units, maximum_verifier_work_units)
        || !valid(limits.maximum_transient_bytes, maximum_verifier_transient_bytes))
        fail(VerifierError::Code::InvalidConfiguration, "authority verifier aggregate limits are invalid");
}

class VerifierBudget final
{
public:
    VerifierBudget(const AuthorityIntegrityVerifierLimits & limits_, AuthorityIntegrityVerificationStatistics & statistics_)
        : limits(limits_)
        , statistics(statistics_)
    {
    }

    void chargeWork(UInt64 amount)
    {
        statistics.work_units = admitTotal(
            statistics.work_units, amount, limits.maximum_work_units, "authority integrity verification exceeds its work limit");
    }

    void chargeCanonicalBytes(UInt64 amount)
    {
        statistics.canonical_bytes_hashed = admitTotal(
            statistics.canonical_bytes_hashed,
            amount,
            limits.maximum_canonical_bytes_hashed,
            "authority integrity verification exceeds its canonical-byte limit");
    }

    void retain(UInt64 amount)
    {
        retained_bytes = admitTotal(
            retained_bytes, amount, limits.maximum_transient_bytes, "authority integrity verification exceeds its transient-byte limit");
        statistics.peak_transient_bytes = std::max(statistics.peak_transient_bytes, retained_bytes);
    }

    void admitTemporary(UInt64 amount)
    {
        const UInt64 peak = admitTotal(
            retained_bytes, amount, limits.maximum_transient_bytes, "authority integrity verification exceeds its transient-byte limit");
        statistics.peak_transient_bytes = std::max(statistics.peak_transient_bytes, peak);
    }

    UInt64 availableTemporaryBytes() const noexcept { return limits.maximum_transient_bytes - retained_bytes; }

private:
    static UInt64 admitTotal(UInt64 current, UInt64 amount, UInt64 maximum, std::string_view message)
    {
        if (current > maximum || amount > maximum - current)
            fail(VerifierError::Code::LimitExceeded, message);
        return current + amount;
    }

    const AuthorityIntegrityVerifierLimits & limits;
    AuthorityIntegrityVerificationStatistics & statistics;
    UInt64 retained_bytes = 0;
};

struct RequiredDefinition
{
    DefinitionIdentity identity;
    const Record * record = nullptr;
};

const RequiredDefinition *
findRequiredDefinition(std::span<const RequiredDefinition> definitions, const DefinitionIdentity & identity, VerifierBudget & budget)
{
    size_t first = 0;
    size_t count = definitions.size();
    while (count != 0)
    {
        budget.chargeWork(1);
        const size_t step = count / 2;
        const size_t middle = first + step;
        const int comparison = compareIdentity(definitions[middle].identity, identity);
        if (comparison < 0)
        {
            first = middle + 1;
            count -= step + 1;
        }
        else
            count = step;
    }
    if (first == definitions.size() || definitions[first].identity != identity)
        return nullptr;
    return &definitions[first];
}

const RequiredDefinition *
findRequiredDefinition(std::span<const RequiredDefinition> definitions, const UUID & type_uuid, VerifierBudget & budget)
{
    size_t first = 0;
    size_t count = definitions.size();
    while (count != 0)
    {
        budget.chargeWork(1);
        const size_t step = count / 2;
        const size_t middle = first + step;
        const int comparison = compareUUID(definitions[middle].identity.type_uuid, type_uuid);
        if (comparison < 0)
        {
            first = middle + 1;
            count -= step + 1;
        }
        else
            count = step;
    }
    if (first == definitions.size() || definitions[first].identity.type_uuid != type_uuid)
        return nullptr;
    return &definitions[first];
}

bool descriptorMatchesRecord(const PersistedTypeDescriptor & descriptor, const Record & record) noexcept
{
    return descriptor.getDefinitionIdentity() == record.identity && descriptor.getDefinitionHash() == record.definition_hash
        && descriptor.getCheckerABI() == record.checker_abi && descriptor.getCheckerChargeABI() == record.checker_charge_abi
        && descriptor.getPolicyABI() == record.policy_abi && descriptor.getFunctionRegistryABI() == record.function_registry_abi
        && descriptor.getPolicySemanticHash() == record.policy_semantic_hash
        && descriptor.getSemanticCapabilities() == record.semantic_capabilities;
}

}

AuthorityIntegrityVerifierError::AuthorityIntegrityVerifierError(
    Code code_, std::string_view message, AuthorityIntegrityVerificationStatistics statistics_)
    : std::runtime_error(String(message))
    , code(code_)
    , statistics(std::move(statistics_))
{
}

void validateAuthorityIntegrityVerifierLimits(const AuthorityIntegrityVerifierLimits & limits)
{
    validateLimits(limits);
}

Digest
computeVerifiedRequiredDefinitionsDigest(std::span<const DefinitionIdentity> sorted_unique_definitions, UInt64 maximum_required_definitions)
{
    if (maximum_required_definitions == 0 || maximum_required_definitions > maximum_verified_required_definitions)
        fail(VerifierError::Code::InvalidConfiguration, "required-definition digest limit is invalid");
    if (toUInt64(sorted_unique_definitions.size()) > maximum_required_definitions)
        fail(VerifierError::Code::LimitExceeded, "required-definition digest exceeds its item limit");
    return computeRequiredDefinitionsDigest(
        sorted_unique_definitions.size(), [&](size_t index) -> const DefinitionIdentity & { return sorted_unique_definitions[index]; });
}

VerifiedAuthorityObjectIntegrity verifyAuthorityObjectIntegrity(
    const AuthorityRoot & authority,
    const SidecarExpectationRecord & expectation,
    const PersistedTypeReferences & references,
    std::string_view canonical_sidecar_bytes,
    UInt64 current_object_schema_revision,
    const Digest & current_physical_schema_fingerprint,
    const AuthorityIntegrityVerifierLimits & limits)
{
    validateLimits(limits);
    AuthorityIntegrityVerificationStatistics statistics;
    try
    {
        VerifierBudget budget(limits, statistics);

        const auto & state = authority.getAuthorityState();
        const UInt64 state_bytes = encodedAuthorityStateSize(state);
        budget.chargeWork(1);
        budget.chargeCanonicalBytes(state_bytes);
        budget.admitTemporary(state_bytes);
        try
        {
            const String encoded_state = encodeAuthorityState(state, limits.authority_state);
            if (toUInt64(encoded_state.size()) != state_bytes)
                fail(VerifierError::Code::InvalidAuthorityState, "authority state has a noncanonical encoded size");
        }
        catch (const VerifierError &)
        {
            throw;
        }
        catch (const AuthorityStateError & error)
        {
            if (error.code == AuthorityStateError::Code::LimitExceeded)
                fail(VerifierError::Code::LimitExceeded, "authority state exceeds the verifier limit");
            fail(VerifierError::Code::InvalidAuthorityState, "authority state is invalid");
        }

        if (state.database_uuid != authority.getDatabaseUUID() || state.database_uuid == UUIDHelpers::Nil
            || state.persistent_capability_mask != dependent_object_authority_capability_mask)
            fail(VerifierError::Code::InvalidAuthorityState, "authority state cannot verify dependent-object sidecars");

        const auto inventory = authority.pinAuthorityInventory();
        const auto graph = authority.pinSchemaObjectDependencyGraph();
        if (!inventory || !graph)
            fail(VerifierError::Code::InvalidAuthorityState, "authority root has no anchored inventory or schema graph");
        if (inventory->getSummary().leaf_count != state.leaf_count || inventory->getSummary().merkle_radix_root != state.inventory_root)
            fail(VerifierError::Code::InventoryMismatch, "authority inventory differs from its state anchor");
        if (graph->getDatabaseUUID() != state.database_uuid || graph->computeRoot() != state.schema_graph_root)
            fail(VerifierError::Code::GraphMismatch, "schema dependency graph differs from its state anchor");

        if (!expectation.object.isValid() || expectation.object.database_uuid != state.database_uuid || current_object_schema_revision == 0
            || expectation.object_schema_revision != current_object_schema_revision
            || expectation.physical_schema_fingerprint != current_physical_schema_fingerprint)
            fail(
                VerifierError::Code::ExpectationMismatch,
                "current object identity, revision, or physical fingerprint differs from expectation");
        const auto * rooted_expectation = authority.findExpectationRecord(expectation.object);
        if (!rooted_expectation || *rooted_expectation != expectation)
            fail(VerifierError::Code::ExpectationMismatch, "authority root has no exact sidecar expectation record");

        const UInt64 expectation_bytes = expectation.installation_record_hash ? sidecar_expectation_record_extended_encoded_bytes
                                                                              : sidecar_expectation_record_encoded_bytes;
        budget.chargeWork(1);
        budget.chargeCanonicalBytes(expectation_bytes);
        budget.admitTemporary(expectation_bytes);
        Digest expectation_hash{};
        try
        {
            expectation_hash = computeSidecarExpectationRecordHash(expectation);
        }
        catch (const SidecarExpectationRecordError &)
        {
            fail(VerifierError::Code::ExpectationMismatch, "sidecar expectation record is invalid");
        }
        ++statistics.inventory_lookups;
        budget.chargeWork(1);
        const auto * expectation_leaf = inventory->find(expectationInventoryKey(expectation.object));
        if (!expectation_leaf || expectation_leaf->object_revision != expectation.object_schema_revision
            || expectation_leaf->canonical_record_hash != expectation_hash)
            fail(VerifierError::Code::InventoryMismatch, "sidecar expectation inventory leaf is missing or stale");

        if (canonical_sidecar_bytes.empty())
            fail(VerifierError::Code::SidecarMismatch, "persisted type-reference sidecar is empty");
        if (toUInt64(canonical_sidecar_bytes.size()) > limits.persisted_references.maximum_sidecar_bytes)
            fail(VerifierError::Code::LimitExceeded, "persisted type-reference sidecar exceeds the verifier limit");
        if (references.descriptors.size() > limits.persisted_references.maximum_descriptors
            || references.occurrence_paths.size() > limits.persisted_references.maximum_occurrence_paths
            || references.uses.size() > limits.persisted_references.maximum_occurrence_paths)
            fail(VerifierError::Code::LimitExceeded, "persisted type-reference entries exceed the verifier limit");
        budget.chargeWork(checkedAdd(
            checkedAdd(
                toUInt64(references.descriptors.size()), toUInt64(references.occurrence_paths.size()), "sidecar work overflows UInt64"),
            toUInt64(references.uses.size()),
            "sidecar work overflows UInt64"));
        for (const auto & path : references.occurrence_paths)
        {
            if (path.type_child_ordinals.size() > limits.persisted_references.maximum_path_depth)
                fail(VerifierError::Code::LimitExceeded, "persisted type-reference path exceeds the verifier depth limit");
            budget.chargeWork(toUInt64(path.type_child_ordinals.size()));
        }
        budget.admitTemporary(toUInt64(references.descriptors.size()));

        PersistedTypeReferencesLimits sidecar_limits = limits.persisted_references;
        sidecar_limits.maximum_sidecar_bytes = std::min(sidecar_limits.maximum_sidecar_bytes, budget.availableTemporaryBytes());
        if (sidecar_limits.maximum_sidecar_bytes == 0)
            fail(VerifierError::Code::LimitExceeded, "authority verifier has no transient budget for the sidecar");
        try
        {
            const String canonical = encodePersistedTypeReferences(references, sidecar_limits);
            budget.admitTemporary(toUInt64(canonical.size()));
            if (canonical != canonical_sidecar_bytes)
                fail(VerifierError::Code::SidecarMismatch, "persisted type-reference bytes are not the canonical supplied sidecar");
        }
        catch (const VerifierError &)
        {
            throw;
        }
        catch (const PersistedTypeReferencesError & error)
        {
            if (error.code == PersistedTypeReferencesError::Code::LimitExceeded)
                fail(VerifierError::Code::LimitExceeded, "persisted type-reference sidecar exceeds the verifier limit");
            fail(VerifierError::Code::SidecarMismatch, "persisted type-reference sidecar is invalid");
        }
        budget.chargeCanonicalBytes(toUInt64(canonical_sidecar_bytes.size()));
        const Digest sidecar_hash = hashCanonicalSidecar(canonical_sidecar_bytes, references);
        if (references.object != expectation.object || references.object_schema_revision != expectation.object_schema_revision
            || references.physical_schema_fingerprint != expectation.physical_schema_fingerprint
            || references.semantic_extension_version != expectation.semantic_extension_version
            || references.semantic_extension_flags != expectation.semantic_extension_flags || sidecar_hash != expectation.sidecar_hash)
            fail(VerifierError::Code::SidecarMismatch, "persisted type-reference sidecar differs from its exact expectation");

        const UInt64 required_scratch = checkedMultiply(
            toUInt64(references.descriptors.size()), sizeof(RequiredDefinition), "required-definition scratch size overflows UInt64");
        budget.retain(required_scratch);
        std::vector<RequiredDefinition> required_definitions;
        required_definitions.reserve(references.descriptors.size());
        for (const auto & descriptor : references.descriptors)
        {
            budget.chargeWork(1);
            required_definitions.push_back({.identity = descriptor.getDefinitionIdentity()});
        }
        std::sort(
            required_definitions.begin(),
            required_definitions.end(),
            [&](const RequiredDefinition & lhs, const RequiredDefinition & rhs)
            {
                budget.chargeWork(1);
                return compareIdentity(lhs.identity, rhs.identity) < 0;
            });
        size_t unique_count = 0;
        for (size_t index = 0; index < required_definitions.size(); ++index)
        {
            budget.chargeWork(1);
            auto & definition = required_definitions[index];
            if (unique_count != 0 && required_definitions[unique_count - 1].identity.type_uuid == definition.identity.type_uuid)
            {
                if (required_definitions[unique_count - 1].identity != definition.identity)
                    fail(VerifierError::Code::DefinitionMismatch, "one sidecar references multiple revisions of one definition identity");
                continue;
            }
            if (unique_count != 0 && compareIdentity(required_definitions[unique_count - 1].identity, definition.identity) >= 0)
                fail(VerifierError::Code::DefinitionMismatch, "sidecar definition identities are not strictly ordered after normalization");
            if (unique_count != index)
                required_definitions[unique_count] = std::move(definition);
            ++unique_count;
            if (unique_count > limits.maximum_required_definitions)
                fail(VerifierError::Code::LimitExceeded, "sidecar exceeds the verifier required-definition limit");
        }
        required_definitions.resize(unique_count);

        if (!graph->containsNode(expectation.object))
            fail(VerifierError::Code::GraphMismatch, "schema dependency graph has no node for the verified object");
        for (auto & required : required_definitions)
        {
            budget.chargeWork(3);
            required.record = authority.findDefinitionRecord(required.identity);
            if (!required.record || required.record->identity != required.identity)
                fail(VerifierError::Code::DefinitionMismatch, "an exact referenced definition record is absent");

            ++statistics.inventory_lookups;
            const auto * definition_leaf = inventory->find(definitionInventoryKey(required.identity));
            if (!definition_leaf || definition_leaf->object_revision != required.identity.revision)
                fail(VerifierError::Code::InventoryMismatch, "a referenced definition inventory leaf is absent or stale");

            if (required.record->parameters.size() > limits.definition_record.maximum_parameter_count
                || required.record->dependencies.size() > limits.definition_record.maximum_dependency_count)
                fail(VerifierError::Code::LimitExceeded, "a referenced definition record exceeds the verifier item limit");
            budget.chargeWork(checkedAdd(
                checkedAdd(1, toUInt64(required.record->parameters.size()), "definition verification work overflows UInt64"),
                toUInt64(required.record->dependencies.size()),
                "definition verification work overflows UInt64"));
            const UInt64 record_bytes = encodedRecordSize(*required.record);
            if (record_bytes > limits.definition_record.maximum_record_bytes)
                fail(VerifierError::Code::LimitExceeded, "a referenced definition record exceeds the verifier limit");
            budget.chargeCanonicalBytes(record_bytes);
            budget.admitTemporary(record_bytes);
            Digest record_hash{};
            try
            {
                record_hash = computeRecordHash(*required.record, limits.definition_record);
            }
            catch (const RecordError & error)
            {
                if (error.code == RecordError::Code::LimitExceeded)
                    fail(VerifierError::Code::LimitExceeded, "a referenced definition record exceeds the verifier limit");
                fail(VerifierError::Code::DefinitionMismatch, "a referenced definition record is invalid");
            }
            if (definition_leaf->canonical_record_hash != record_hash)
                fail(VerifierError::Code::InventoryMismatch, "a referenced definition record differs from its inventory leaf");

            const auto definition_object = definitionObject(required.identity);
            if (!graph->containsNode(definition_object)
                || !graph->containsEdge({
                    .dependent = expectation.object,
                    .dependency = definition_object,
                    .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
                }))
                fail(VerifierError::Code::GraphMismatch, "schema dependency graph omits a referenced definition edge");
            ++statistics.definition_records_verified;
        }

        for (const auto & descriptor : references.descriptors)
        {
            const auto * required = findRequiredDefinition(required_definitions, descriptor.getDefinitionIdentity(), budget);
            if (!required || !required->record || !descriptorMatchesRecord(descriptor, *required->record))
                fail(VerifierError::Code::DefinitionMismatch, "a persisted descriptor differs from its exact definition record");
            ++statistics.descriptors_verified;
        }

        budget.chargeWork(1);
        const UInt64 dependency_count = graph->getDependencyCount(expectation.object);
        if (dependency_count > limits.maximum_outgoing_dependencies)
            fail(VerifierError::Code::LimitExceeded, "verified object exceeds the outgoing dependency limit");
        const UInt64 dependency_scratch
            = checkedMultiply(dependency_count, sizeof(SchemaObjectDependencyNeighbor), "dependency materialization size overflows UInt64");
        budget.retain(dependency_scratch);
        budget.chargeWork(dependency_count);
        const auto dependencies = graph->getDependencies(expectation.object);
        if (toUInt64(dependencies.size()) != dependency_count)
            fail(VerifierError::Code::GraphMismatch, "schema dependency cardinality changed during immutable verification");
        UInt64 definition_edge_count = 0;
        for (const auto & dependency : dependencies)
        {
            ++statistics.graph_edges_inspected;
            if (dependency.kind != SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition)
                continue;
            if (dependency.object.kind != SchemaObjectKind::TypeDefinition || dependency.object.database_uuid != state.database_uuid
                || !findRequiredDefinition(required_definitions, dependency.object.object_uuid, budget))
                fail(VerifierError::Code::GraphMismatch, "schema dependency graph contains an unreferenced definition edge");
            ++definition_edge_count;
        }
        if (definition_edge_count != required_definitions.size())
            fail(VerifierError::Code::GraphMismatch, "schema dependency graph definition coverage is not exact");

        const UInt64 required_definition_digest_bytes = checkedAdd(
            sizeof(UInt64),
            checkedMultiply(
                toUInt64(required_definitions.size()),
                2 * sizeof(CanonicalUUID) + sizeof(UInt64),
                "required-definition digest byte size overflows UInt64"),
            "required-definition digest byte size overflows UInt64");
        budget.chargeWork(toUInt64(required_definitions.size()));
        budget.chargeCanonicalBytes(required_definition_digest_bytes);
        const Digest required_definitions_digest = computeRequiredDefinitionsDigest(
            required_definitions.size(), [&](size_t index) -> const DefinitionIdentity & { return required_definitions[index].identity; });

        return {
            .database_uuid = state.database_uuid,
            .database_catalog_epoch = state.database_catalog_epoch,
            .authority_anchor = state.anchor_hash,
            .object = expectation.object,
            .object_schema_revision = expectation.object_schema_revision,
            .sidecar_hash = expectation.sidecar_hash,
            .physical_schema_fingerprint = expectation.physical_schema_fingerprint,
            .required_definitions_digest = required_definitions_digest,
            .required_definition_count = toUInt64(required_definitions.size()),
            .statistics = statistics,
        };
    }
    catch (const AuthorityIntegrityVerifierError & error)
    {
        throw AuthorityIntegrityVerifierError(error.code, error.what(), statistics);
    }
}

}
