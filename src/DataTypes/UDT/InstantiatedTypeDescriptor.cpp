#include <DataTypes/UDT/InstantiatedTypeDescriptor.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/PhysicalTypeFingerprint.h>

#include <algorithm>
#include <array>
#include <limits>
#include <utility>

namespace DB::UDT
{
namespace
{

constexpr UInt16 descriptor_identity_version = 2;
constexpr UInt64 implementation_maximum_nodes = 1ULL << 20;
constexpr UInt64 implementation_maximum_edges = 4ULL << 20;
constexpr UInt64 implementation_maximum_path_depth = 64;
constexpr UInt64 implementation_maximum_descriptors = 65'536;
constexpr UInt64 implementation_maximum_occurrences = 65'536;
constexpr UInt64 implementation_maximum_arguments_bytes = 64ULL << 10;
constexpr UInt64 implementation_maximum_physical_type_bytes = 64ULL << 10;
constexpr UInt64 implementation_maximum_qualified_name_bytes = 4ULL << 10;

[[noreturn]] void descriptorError(DescriptorError::Code code, std::string_view message)
{
    throw DescriptorError(code, message);
}

void updateUInt16LE(CanonicalHasher & hasher, UInt16 value)
{
    const std::array<CanonicalByte, 2> bytes{
        static_cast<CanonicalByte>(value),
        static_cast<CanonicalByte>(value >> 8),
    };
    hasher.update(bytes);
}

void updateUInt64LE(CanonicalHasher & hasher, UInt64 value)
{
    std::array<CanonicalByte, sizeof(value)> bytes{};
    for (size_t index = 0; index < sizeof(value); ++index)
        bytes[index] = static_cast<CanonicalByte>(value >> (index * 8));
    hasher.update(bytes);
}

void updateVarUInt(CanonicalHasher & hasher, UInt64 value)
{
    std::array<CanonicalByte, 10> bytes{};
    size_t size = 0;
    while (value >= 0x80)
    {
        bytes[size++] = static_cast<CanonicalByte>(value) | 0x80;
        value >>= 7;
    }
    bytes[size++] = static_cast<CanonicalByte>(value);
    hasher.update(std::span(bytes).first(size));
}

void updateFrame(CanonicalHasher & hasher, std::string_view value)
{
    if (!std::in_range<UInt64>(value.size()))
        descriptorError(DescriptorError::Code::LimitExceeded, "descriptor frame does not fit UInt64");
    updateVarUInt(hasher, static_cast<UInt64>(value.size()));
    hasher.update(value);
}

void updateDigest(CanonicalHasher & hasher, const Digest & value)
{
    hasher.update(value);
}

bool isZeroDigest(const Digest & value) noexcept
{
    return std::all_of(value.begin(), value.end(), [](CanonicalByte byte) { return byte == 0; });
}

bool containsNul(std::string_view value) noexcept
{
    return value.find('\0') != std::string_view::npos;
}

void validateLimits(const TypeDescriptorLimits & limits)
{
    if (limits.maximum_canonical_arguments_bytes == 0 || limits.maximum_canonical_arguments_bytes > implementation_maximum_arguments_bytes
        || limits.maximum_canonical_physical_type_bytes == 0
        || limits.maximum_canonical_physical_type_bytes > implementation_maximum_physical_type_bytes
        || limits.maximum_qualified_name_bytes == 0 || limits.maximum_qualified_name_bytes > implementation_maximum_qualified_name_bytes
        || limits.maximum_nodes == 0 || limits.maximum_nodes > implementation_maximum_nodes
        || limits.maximum_edges > implementation_maximum_edges || limits.maximum_path_depth > implementation_maximum_path_depth
        || limits.maximum_descriptors == 0 || limits.maximum_descriptors > implementation_maximum_descriptors
        || limits.maximum_occurrences == 0 || limits.maximum_occurrences > implementation_maximum_occurrences)
        descriptorError(DescriptorError::Code::LimitExceeded, "descriptor limits exceed the implementation domain");
}

Digest computeInstantiationSemanticHashImpl(const InstantiationSemanticHashInput & input)
{
    /// Hash incrementally: the largest fields are never concatenated into a
    /// duplicate preimage allocation. All variable fields were admitted under
    /// TypeDescriptorLimits before this function is called.
    CanonicalHasher hasher(instantiation_semantic_hash_domain);
    updateUInt16LE(hasher, descriptor_identity_version);
    hasher.updateUUID(input.definition_identity.database_uuid);
    hasher.updateUUID(input.definition_identity.type_uuid);
    updateUInt64LE(hasher, input.definition_identity.revision);
    updateDigest(hasher, input.definition_hash);
    updateFrame(hasher, input.canonical_arguments_encoding);
    updateFrame(hasher, input.canonical_physical_type);
    updateDigest(hasher, input.storage_fingerprint);
    updateUInt16LE(hasher, input.checker_abi);
    updateUInt16LE(hasher, input.checker_charge_abi);
    updateUInt16LE(hasher, input.policy_abi);
    updateUInt16LE(hasher, input.function_registry_abi);
    updateDigest(hasher, input.policy_semantic_hash);
    const std::array<CanonicalByte, 1> capabilities{input.semantic_capabilities};
    hasher.update(capabilities);
    /// The current identity has no instantiated program-kind ABI. The only
    /// canonical value is the empty set; a future identity format must change
    /// before encoding entries here.
    updateVarUInt(hasher, 0);
    return hasher.finalize();
}

bool binaryLess(std::string_view lhs, std::string_view rhs) noexcept
{
    return std::lexicographical_compare(
        lhs.begin(),
        lhs.end(),
        rhs.begin(),
        rhs.end(),
        [](char left, char right) { return static_cast<unsigned char>(left) < static_cast<unsigned char>(right); });
}

bool pathLess(std::span<const UInt32> lhs, std::span<const UInt32> rhs) noexcept
{
    return std::lexicographical_compare(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
}

bool descriptorOccurrenceLess(const InstantiatedTypeDescriptor::Ptr & lhs, const InstantiatedTypeDescriptor::Ptr & rhs) noexcept
{
    if (lhs == rhs)
        return false;
    const auto & left = lhs->getPersistedDescriptor();
    const auto & right = rhs->getPersistedDescriptor();
    if (left.stableLess(right))
        return true;
    if (right.stableLess(left))
        return false;
    /// Diagnostic name is deliberately outside identity, but selecting the
    /// bytewise-min spelling makes interning independent of input order.
    return binaryLess(left.getLastKnownQualifiedName(), right.getLastKnownQualifiedName());
}

bool sameDefinitionIdentity(const Definition & lhs, const Definition & rhs) noexcept
{
    return lhs.getIdentity() == rhs.getIdentity();
}

bool sameDefinitionSemantics(const Definition & lhs, const Definition & rhs) noexcept
{
    /// normalized_name is deliberately diagnostic-only: an immutable revision
    /// survives a rename. Every executable field and the complete checker
    /// certificate must nevertheless agree. In particular, equality of the
    /// definition digest is not treated as permission to conflate two
    /// different bodies if a forged/corrupt catalog ever crosses this boundary.
    return lhs.hasSameCheckedSemantics(rhs);
}

bool definitionLess(const Definition::Ptr & lhs, const Definition::Ptr & rhs)
{
    const auto lhs_database = uuidToCanonicalBytes(lhs->getIdentity().database_uuid);
    const auto rhs_database = uuidToCanonicalBytes(rhs->getIdentity().database_uuid);
    if (lhs_database != rhs_database)
        return lhs_database < rhs_database;
    const auto lhs_type = uuidToCanonicalBytes(lhs->getIdentity().type_uuid);
    const auto rhs_type = uuidToCanonicalBytes(rhs->getIdentity().type_uuid);
    if (lhs_type != rhs_type)
        return lhs_type < rhs_type;
    if (lhs->getIdentity().revision != rhs->getIdentity().revision)
        return lhs->getIdentity().revision < rhs->getIdentity().revision;
    if (lhs->getDefinitionHash() != rhs->getDefinitionHash())
        return lhs->getDefinitionHash() < rhs->getDefinitionHash();
    return binaryLess(lhs->getNormalizedName(), rhs->getNormalizedName());
}

bool hasValidDefinitionIdentity(const Definition::Ptr & definition) noexcept
{
    return definition && definition->getIdentity().database_uuid != UUIDHelpers::Nil
        && definition->getIdentity().type_uuid != UUIDHelpers::Nil && definition->getIdentity().revision != 0
        && !isZeroDigest(definition->getDefinitionHash()) && (definition->getSemanticCapabilities() & ~all_semantic_capabilities) == 0;
}

}

Digest computeInstantiationSemanticHash(const InstantiationSemanticHashInput & input)
{
    return computeInstantiationSemanticHashImpl(input);
}

void validateTypeDescriptorLimits(const TypeDescriptorLimits & limits)
{
    validateLimits(limits);
}

DescriptorError::DescriptorError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

PersistedTypeDescriptor::PersistedTypeDescriptor(
    DefinitionIdentity definition_identity_,
    Digest definition_hash_,
    String canonical_arguments_encoding_,
    String canonical_physical_type_,
    Digest instantiation_semantic_hash_,
    Digest storage_fingerprint_,
    UInt16 checker_abi_,
    UInt16 checker_charge_abi_,
    UInt16 policy_abi_,
    UInt16 function_registry_abi_,
    Digest policy_semantic_hash_,
    SemanticCapabilityMask semantic_capabilities_,
    String last_known_qualified_name_)
    : definition_identity(definition_identity_)
    , definition_hash(definition_hash_)
    , canonical_arguments_encoding(std::move(canonical_arguments_encoding_))
    , canonical_physical_type(std::move(canonical_physical_type_))
    , instantiation_semantic_hash(instantiation_semantic_hash_)
    , storage_fingerprint(storage_fingerprint_)
    , checker_abi(checker_abi_)
    , checker_charge_abi(checker_charge_abi_)
    , policy_abi(policy_abi_)
    , function_registry_abi(function_registry_abi_)
    , policy_semantic_hash(policy_semantic_hash_)
    , semantic_capabilities(semantic_capabilities_)
    , last_known_qualified_name(std::move(last_known_qualified_name_))
{
}

PersistedTypeDescriptor PersistedTypeDescriptor::fromCanonicalPersistenceFields(
    DefinitionIdentity definition_identity_,
    Digest definition_hash_,
    String canonical_arguments_encoding_,
    String canonical_physical_type_,
    Digest instantiation_semantic_hash_,
    Digest storage_fingerprint_,
    UInt16 checker_abi_,
    UInt16 checker_charge_abi_,
    UInt16 policy_abi_,
    UInt16 function_registry_abi_,
    Digest policy_semantic_hash_,
    SemanticCapabilityMask semantic_capabilities_,
    String last_known_qualified_name_,
    const TypeDescriptorLimits & limits)
{
    validateLimits(limits);
    if (definition_identity_.database_uuid == UUIDHelpers::Nil || definition_identity_.type_uuid == UUIDHelpers::Nil
        || definition_identity_.revision == 0)
        descriptorError(DescriptorError::Code::InvalidDefinition, "persisted descriptor identity is invalid");
    /// V1 CanonicalTypeArguments always starts with its UInt16 little-endian
    /// version and contains at least the minimal argument-count VarUInt. The
    /// remaining bytes stay opaque here: validating kinds and TYPE structure
    /// requires the recovered definition's formal parameter declaration.
    if (canonical_arguments_encoding_.size() < 3 || static_cast<UInt8>(canonical_arguments_encoding_[0]) != 1
        || static_cast<UInt8>(canonical_arguments_encoding_[1]) != 0)
        descriptorError(
            DescriptorError::Code::InvalidArguments, "persisted descriptor arguments do not have the canonical V1 envelope");
    if (canonical_arguments_encoding_.size() > limits.maximum_canonical_arguments_bytes)
        descriptorError(DescriptorError::Code::LimitExceeded, "persisted descriptor arguments exceed their byte limit");
    if (canonical_physical_type_.empty() || containsNul(canonical_physical_type_))
        descriptorError(
            DescriptorError::Code::InvalidPhysicalType, "persisted descriptor physical type is empty or contains NUL");
    if (canonical_physical_type_.size() > limits.maximum_canonical_physical_type_bytes)
        descriptorError(DescriptorError::Code::LimitExceeded, "persisted descriptor physical type exceeds its byte limit");
    if (last_known_qualified_name_.empty() || containsNul(last_known_qualified_name_))
        descriptorError(
            DescriptorError::Code::InvalidDefinition, "persisted descriptor diagnostic name is empty or contains NUL");
    if (last_known_qualified_name_.size() > limits.maximum_qualified_name_bytes)
        descriptorError(DescriptorError::Code::LimitExceeded, "persisted descriptor diagnostic name exceeds its byte limit");
    if ((checker_abi_ != 1 && checker_abi_ != 2) || checker_charge_abi_ != 1 || policy_abi_ != 1 || function_registry_abi_ != 1)
        descriptorError(
            DescriptorError::Code::InvalidDefinition, "persisted descriptor contains an unsupported semantic ABI");
    if ((semantic_capabilities_ & ~all_semantic_capabilities) != 0)
        descriptorError(
            DescriptorError::Code::InvalidDefinition, "persisted descriptor contains an unknown semantic capability");

    const Digest expected_instantiation_hash = computeInstantiationSemanticHashImpl({
        .definition_identity = definition_identity_,
        .definition_hash = definition_hash_,
        .canonical_arguments_encoding = canonical_arguments_encoding_,
        .canonical_physical_type = canonical_physical_type_,
        .storage_fingerprint = storage_fingerprint_,
        .checker_abi = checker_abi_,
        .checker_charge_abi = checker_charge_abi_,
        .policy_abi = policy_abi_,
        .function_registry_abi = function_registry_abi_,
        .policy_semantic_hash = policy_semantic_hash_,
        .semantic_capabilities = semantic_capabilities_,
    });
    if (instantiation_semantic_hash_ != expected_instantiation_hash)
        descriptorError(
            DescriptorError::Code::ConflictingIdentity,
            "persisted descriptor instantiation hash does not match its canonical fields");

    return PersistedTypeDescriptor(
        definition_identity_,
        definition_hash_,
        std::move(canonical_arguments_encoding_),
        std::move(canonical_physical_type_),
        instantiation_semantic_hash_,
        storage_fingerprint_,
        checker_abi_,
        checker_charge_abi_,
        policy_abi_,
        function_registry_abi_,
        policy_semantic_hash_,
        semantic_capabilities_,
        std::move(last_known_qualified_name_));
}

bool PersistedTypeDescriptor::hasSameInstantiation(const PersistedTypeDescriptor & rhs) const noexcept
{
    return instantiation_semantic_hash == rhs.instantiation_semantic_hash && definition_identity == rhs.definition_identity
        && definition_hash == rhs.definition_hash && canonical_arguments_encoding == rhs.canonical_arguments_encoding
        && canonical_physical_type == rhs.canonical_physical_type && storage_fingerprint == rhs.storage_fingerprint
        && checker_abi == rhs.checker_abi && checker_charge_abi == rhs.checker_charge_abi && policy_abi == rhs.policy_abi
        && function_registry_abi == rhs.function_registry_abi && policy_semantic_hash == rhs.policy_semantic_hash
        && semantic_capabilities == rhs.semantic_capabilities;
}

bool PersistedTypeDescriptor::stableLess(const PersistedTypeDescriptor & rhs) const noexcept
{
    /// The fixed-size digest is the normal-path ordering key. Exact semantic
    /// fields are visited only on a digest tie, and ensure a SHA-256 collision
    /// can never conflate two instantiated descriptors.
    if (instantiation_semantic_hash != rhs.instantiation_semantic_hash)
        return instantiation_semantic_hash < rhs.instantiation_semantic_hash;
    const auto database = uuidToCanonicalBytes(definition_identity.database_uuid);
    const auto rhs_database = uuidToCanonicalBytes(rhs.definition_identity.database_uuid);
    if (database != rhs_database)
        return database < rhs_database;
    const auto type = uuidToCanonicalBytes(definition_identity.type_uuid);
    const auto rhs_type = uuidToCanonicalBytes(rhs.definition_identity.type_uuid);
    if (type != rhs_type)
        return type < rhs_type;
    if (definition_identity.revision != rhs.definition_identity.revision)
        return definition_identity.revision < rhs.definition_identity.revision;
    if (definition_hash != rhs.definition_hash)
        return definition_hash < rhs.definition_hash;
    if (canonical_arguments_encoding != rhs.canonical_arguments_encoding)
        return binaryLess(canonical_arguments_encoding, rhs.canonical_arguments_encoding);
    if (canonical_physical_type != rhs.canonical_physical_type)
        return binaryLess(canonical_physical_type, rhs.canonical_physical_type);
    if (storage_fingerprint != rhs.storage_fingerprint)
        return storage_fingerprint < rhs.storage_fingerprint;
    if (checker_abi != rhs.checker_abi)
        return checker_abi < rhs.checker_abi;
    if (checker_charge_abi != rhs.checker_charge_abi)
        return checker_charge_abi < rhs.checker_charge_abi;
    if (policy_abi != rhs.policy_abi)
        return policy_abi < rhs.policy_abi;
    if (function_registry_abi != rhs.function_registry_abi)
        return function_registry_abi < rhs.function_registry_abi;
    if (policy_semantic_hash != rhs.policy_semantic_hash)
        return policy_semantic_hash < rhs.policy_semantic_hash;
    return semantic_capabilities < rhs.semantic_capabilities;
}

bool PersistedTypeDescriptor::operator==(const PersistedTypeDescriptor & rhs) const noexcept
{
    return hasSameInstantiation(rhs) && last_known_qualified_name == rhs.last_known_qualified_name;
}

InstantiatedTypeDescriptor::InstantiatedTypeDescriptor(
    Definition::Ptr definition_,
    CanonicalTypeArguments arguments_,
    DataTypePtr physical_type_,
    PersistedTypeDescriptor persisted_)
    : definition(std::move(definition_))
    , arguments(std::move(arguments_))
    , physical_type(std::move(physical_type_))
    , persisted(std::move(persisted_))
{
}

InstantiatedTypeDescriptor::Ptr InstantiatedTypeDescriptor::create(
    Definition::Ptr definition,
    CanonicalTypeArguments arguments,
    DataTypePtr physical_type,
    const TypeDescriptorLimits & limits)
{
    validateLimits(limits);
    if (!hasValidDefinitionIdentity(definition))
        descriptorError(DescriptorError::Code::InvalidDefinition, "instantiated descriptor has an invalid definition");
    if (!physical_type)
        descriptorError(DescriptorError::Code::InvalidPhysicalType, "instantiated descriptor has no physical type");
    if (arguments.values().size() != definition->getParameters().size())
        descriptorError(
            DescriptorError::Code::InvalidArguments, "instantiated descriptor argument count differs from definition");
    for (size_t index = 0; index < arguments.values().size(); ++index)
    {
        if (arguments.values()[index].kind != definition->getParameters()[index].kind)
            descriptorError(
                DescriptorError::Code::InvalidArguments, "instantiated descriptor argument kind differs from definition");
    }
    if (arguments.encoded().size() > limits.maximum_canonical_arguments_bytes)
        descriptorError(DescriptorError::Code::LimitExceeded, "instantiated descriptor arguments exceed their byte limit");

    String canonical_physical_type = physical_type->getName();
    if (canonical_physical_type.empty())
        descriptorError(
            DescriptorError::Code::InvalidPhysicalType, "instantiated descriptor physical type has no canonical name");
    if (canonical_physical_type.size() > limits.maximum_canonical_physical_type_bytes)
        descriptorError(
            DescriptorError::Code::LimitExceeded, "instantiated descriptor physical type exceeds its byte limit");
    if (definition->getNormalizedName().empty())
        descriptorError(
            DescriptorError::Code::InvalidDefinition, "instantiated descriptor definition has no diagnostic name");
    if (definition->getNormalizedName().size() > limits.maximum_qualified_name_bytes)
        descriptorError(
            DescriptorError::Code::LimitExceeded, "instantiated descriptor diagnostic name exceeds its byte limit");

    const Digest storage_fingerprint = physicalTypeFingerprint(physical_type);
    const Digest instantiation_semantic_hash = computeInstantiationSemanticHash({
        .definition_identity = definition->getIdentity(),
        .definition_hash = definition->getDefinitionHash(),
        .canonical_arguments_encoding = arguments.encoded(),
        .canonical_physical_type = canonical_physical_type,
        .storage_fingerprint = storage_fingerprint,
        .checker_abi = definition->getCheckerABI(),
        .checker_charge_abi = definition->getCheckerChargeABI(),
        .policy_abi = definition->getPolicyABI(),
        .function_registry_abi = definition->getFunctionRegistryABI(),
        .policy_semantic_hash = definition->getPolicySemanticHash(),
        .semantic_capabilities = definition->getSemanticCapabilities(),
    });
    PersistedTypeDescriptor persisted = PersistedTypeDescriptor::fromCanonicalPersistenceFields(
        definition->getIdentity(),
        definition->getDefinitionHash(),
        arguments.encoded(),
        std::move(canonical_physical_type),
        instantiation_semantic_hash,
        storage_fingerprint,
        definition->getCheckerABI(),
        definition->getCheckerChargeABI(),
        definition->getPolicyABI(),
        definition->getFunctionRegistryABI(),
        definition->getPolicySemanticHash(),
        definition->getSemanticCapabilities(),
        definition->getNormalizedName(),
        limits);
    return Ptr(new InstantiatedTypeDescriptor(std::move(definition), std::move(arguments), std::move(physical_type), std::move(persisted)));
}

BoundDeclaredTypeTree::BoundDeclaredTypeTree(
    std::vector<BoundDeclaredTypeNode> nodes_,
    std::vector<BoundDeclaredTypeNodeID> children_,
    std::vector<UInt32> descriptor_occurrences_,
    std::vector<InstantiatedTypeDescriptor::Ptr> descriptors_,
    std::vector<Definition::Ptr> definition_handles_)
    : nodes(std::move(nodes_))
    , children(std::move(children_))
    , descriptor_occurrences(std::move(descriptor_occurrences_))
    , descriptors(std::move(descriptors_))
    , definition_handles(std::move(definition_handles_))
{
}

BoundDeclaredTypeTree::Ptr BoundDeclaredTypeTree::build(
    std::vector<BoundDeclaredTypeNodeInput> inputs,
    std::vector<BoundDeclaredTypeOccurrenceInput> occurrences,
    std::vector<Definition::Ptr> transitive_definition_handles,
    const TypeDescriptorLimits & limits)
{
    validateLimits(limits);
    if (inputs.empty())
        descriptorError(DescriptorError::Code::InvalidPath, "bound declared-type tree has no root");
    if (inputs.size() > limits.maximum_nodes)
        descriptorError(DescriptorError::Code::LimitExceeded, "bound declared-type tree exceeds its node limit");
    if (inputs.size() - 1 > limits.maximum_edges)
        descriptorError(DescriptorError::Code::LimitExceeded, "bound declared-type tree exceeds its edge limit");
    if (occurrences.size() > limits.maximum_occurrences)
        descriptorError(DescriptorError::Code::LimitExceeded, "bound declared-type tree exceeds its occurrence limit");
    if (transitive_definition_handles.size() > limits.maximum_descriptors)
        descriptorError(
            DescriptorError::Code::LimitExceeded,
            "bound declared-type tree has too many supplied definition-handle occurrences");

    UInt64 total_path_elements = 0;
    auto chargePath = [&](std::span<const UInt32> path)
    {
        if (path.size() > limits.maximum_path_depth)
            descriptorError(DescriptorError::Code::LimitExceeded, "bound declared-type path exceeds its depth limit");
        if (!std::in_range<UInt64>(path.size()))
            descriptorError(DescriptorError::Code::LimitExceeded, "bound declared-type paths exceed their element limit");
        const UInt64 path_elements = static_cast<UInt64>(path.size());
        if (path_elements > limits.maximum_edges || total_path_elements > limits.maximum_edges - path_elements)
            descriptorError(DescriptorError::Code::LimitExceeded, "bound declared-type paths exceed their element limit");
        total_path_elements += path_elements;
    };
    for (const auto & input : inputs)
    {
        if (!input.physical_type)
            descriptorError(DescriptorError::Code::InvalidPhysicalType, "bound declared-type node has no physical type");
        chargePath(input.type_child_ordinals);
    }
    for (const auto & occurrence : occurrences)
    {
        if (!occurrence.logical_descriptor)
            descriptorError(DescriptorError::Code::InvalidDefinition, "bound declared-type occurrence has no descriptor");
        chargePath(occurrence.type_child_ordinals);
    }

    std::sort(
        inputs.begin(),
        inputs.end(),
        [](const BoundDeclaredTypeNodeInput & lhs, const BoundDeclaredTypeNodeInput & rhs)
        { return pathLess(lhs.type_child_ordinals, rhs.type_child_ordinals); });
    if (!inputs.front().type_child_ordinals.empty())
        descriptorError(DescriptorError::Code::InvalidPath, "bound declared-type tree has no normalized root path");
    for (size_t index = 1; index < inputs.size(); ++index)
    {
        if (inputs[index - 1].type_child_ordinals == inputs[index].type_child_ordinals)
            descriptorError(DescriptorError::Code::InvalidPath, "bound declared-type tree contains a duplicate path");
    }

    std::sort(
        occurrences.begin(),
        occurrences.end(),
        [](const BoundDeclaredTypeOccurrenceInput & lhs, const BoundDeclaredTypeOccurrenceInput & rhs)
        { return lhs.logical_preorder < rhs.logical_preorder; });
    for (size_t index = 1; index < occurrences.size(); ++index)
    {
        if (occurrences[index - 1].logical_preorder == occurrences[index].logical_preorder)
            descriptorError(
                DescriptorError::Code::InvalidPath,
                "bound declared-type occurrences have duplicate logical preorder identities");
    }
    std::sort(
        occurrences.begin(),
        occurrences.end(),
        [](const BoundDeclaredTypeOccurrenceInput & lhs, const BoundDeclaredTypeOccurrenceInput & rhs)
        {
            if (pathLess(lhs.type_child_ordinals, rhs.type_child_ordinals))
                return true;
            if (pathLess(rhs.type_child_ordinals, lhs.type_child_ordinals))
                return false;
            return lhs.logical_preorder < rhs.logical_preorder;
        });

    /// Validate every raw occurrence definition before descriptor interning.
    /// Two corrupt descriptors can have byte-identical persisted headlines;
    /// dropping one first would hide a conflicting immutable checked body.
    if (transitive_definition_handles.size() > std::numeric_limits<size_t>::max() - occurrences.size())
        descriptorError(
            DescriptorError::Code::LimitExceeded, "bound declared-type definition-handle count overflows size_t");
    std::vector<Definition::Ptr> definitions;
    definitions.reserve(occurrences.size() + transitive_definition_handles.size());
    for (const auto & occurrence : occurrences)
    {
        const auto & definition = occurrence.logical_descriptor->getDefinition();
        if (!hasValidDefinitionIdentity(definition))
            descriptorError(
                DescriptorError::Code::InvalidDefinition, "bound declared-type occurrence has an invalid definition handle");
        definitions.push_back(definition);
    }
    for (auto & definition : transitive_definition_handles)
    {
        if (!hasValidDefinitionIdentity(definition))
            descriptorError(
                DescriptorError::Code::InvalidDefinition, "bound declared-type tree has an invalid definition handle");
        definitions.push_back(std::move(definition));
    }
    std::sort(definitions.begin(), definitions.end(), definitionLess);
    size_t unique_definition_count = 0;
    for (size_t index = 0; index < definitions.size(); ++index)
    {
        if (unique_definition_count != 0 && sameDefinitionIdentity(*definitions[index], *definitions[unique_definition_count - 1]))
        {
            if (!sameDefinitionSemantics(*definitions[index], *definitions[unique_definition_count - 1]))
                descriptorError(
                    DescriptorError::Code::ConflictingIdentity,
                    "one immutable definition identity has conflicting checked semantics");
            continue;
        }
        if (unique_definition_count != index)
            definitions[unique_definition_count] = std::move(definitions[index]);
        ++unique_definition_count;
        if (unique_definition_count > limits.maximum_descriptors)
            descriptorError(
                DescriptorError::Code::LimitExceeded, "bound declared-type tree exceeds its definition-handle limit");
    }
    definitions.resize(unique_definition_count);

    std::vector<InstantiatedTypeDescriptor::Ptr> descriptors;
    descriptors.reserve(occurrences.size());
    for (const auto & occurrence : occurrences)
        descriptors.push_back(occurrence.logical_descriptor);
    if (descriptors.empty())
        descriptorError(
            DescriptorError::Code::InvalidDefinition,
            "logical declared-type tree contains no logical descriptor; use a physical-only result");
    std::sort(descriptors.begin(), descriptors.end(), descriptorOccurrenceLess);
    size_t unique_descriptor_count = 0;
    for (size_t index = 0; index < descriptors.size(); ++index)
    {
        if (unique_descriptor_count != 0
            && descriptors[index]->getPersistedDescriptor().hasSameInstantiation(
                descriptors[unique_descriptor_count - 1]->getPersistedDescriptor()))
            continue;
        if (unique_descriptor_count != index)
            descriptors[unique_descriptor_count] = std::move(descriptors[index]);
        ++unique_descriptor_count;
        if (unique_descriptor_count > limits.maximum_descriptors)
            descriptorError(DescriptorError::Code::LimitExceeded, "bound declared-type tree exceeds its descriptor limit");
    }
    descriptors.resize(unique_descriptor_count);

    if (inputs.size() > std::numeric_limits<BoundDeclaredTypeNodeID>::max())
        descriptorError(DescriptorError::Code::LimitExceeded, "bound declared-type node IDs exceed UInt32");
    std::vector<BoundDeclaredTypeNode> nodes(inputs.size());
    std::vector<BoundDeclaredTypeNodeID> path_stack;
    path_stack.reserve(static_cast<size_t>(limits.maximum_path_depth) + 1);
    for (size_t index = 0; index < inputs.size(); ++index)
    {
        auto & output = nodes[index];
        output.physical_type = inputs[index].physical_type;
        if (index == 0)
        {
            path_stack.push_back(0);
        }
        else
        {
            const size_t depth = inputs[index].type_child_ordinals.size();
            if (depth == 0 || path_stack.size() < depth)
                descriptorError(DescriptorError::Code::InvalidPath, "declared-type path has no parent");
            const BoundDeclaredTypeNodeID parent = path_stack[depth - 1];
            const auto & parent_path = inputs[parent].type_child_ordinals;
            if (parent_path.size() != depth - 1
                || !std::equal(parent_path.begin(), parent_path.end(), inputs[index].type_child_ordinals.begin()))
                descriptorError(DescriptorError::Code::InvalidPath, "declared-type path has no parent");
            output.parent = parent;
            output.child_ordinal = inputs[index].type_child_ordinals.back();
            ++nodes[parent].child_count;
            path_stack.resize(depth + 1);
            path_stack[depth] = static_cast<BoundDeclaredTypeNodeID>(index);
        }
    }

    std::vector<UInt32> descriptor_occurrences;
    descriptor_occurrences.reserve(occurrences.size());
    size_t node_index = 0;
    for (const auto & occurrence : occurrences)
    {
        while (node_index < inputs.size() && pathLess(inputs[node_index].type_child_ordinals, occurrence.type_child_ordinals))
            ++node_index;
        if (node_index == inputs.size() || inputs[node_index].type_child_ordinals != occurrence.type_child_ordinals)
            descriptorError(DescriptorError::Code::InvalidPath, "bound declared-type occurrence path has no physical node");
        if (!inputs[node_index].physical_type->equals(*occurrence.logical_descriptor->getPhysicalType()))
            descriptorError(
                DescriptorError::Code::InvalidPhysicalType, "bound logical descriptor does not match its physical subtree");

        const auto descriptor_it = std::lower_bound(
            descriptors.begin(),
            descriptors.end(),
            occurrence.logical_descriptor,
            [](const auto & lhs, const auto & rhs) { return lhs->getPersistedDescriptor().stableLess(rhs->getPersistedDescriptor()); });
        if (descriptor_it == descriptors.end()
            || !(*descriptor_it)->getPersistedDescriptor().hasSameInstantiation(occurrence.logical_descriptor->getPersistedDescriptor()))
            descriptorError(DescriptorError::Code::ConflictingIdentity, "bound logical descriptor was not interned");

        auto & node = nodes[node_index];
        if (node.occurrence_count == 0)
            node.first_occurrence = static_cast<UInt32>(descriptor_occurrences.size());
        descriptor_occurrences.push_back(static_cast<UInt32>(descriptor_it - descriptors.begin()));
        ++node.occurrence_count;
        node.own_semantic_capabilities |= occurrence.logical_descriptor->getPersistedDescriptor().getSemanticCapabilities();
        node.subtree_semantic_capabilities = node.own_semantic_capabilities;
    }

    UInt64 edge_offset = 0;
    for (auto & node : nodes)
    {
        if (edge_offset > std::numeric_limits<UInt32>::max())
            descriptorError(DescriptorError::Code::LimitExceeded, "bound declared-type edge offsets exceed UInt32");
        node.first_child = static_cast<UInt32>(edge_offset);
        edge_offset += node.child_count;
        node.child_count = 0;
    }
    if (edge_offset > limits.maximum_edges || edge_offset > std::numeric_limits<UInt32>::max())
        descriptorError(DescriptorError::Code::LimitExceeded, "bound declared-type edges exceed UInt32");
    std::vector<BoundDeclaredTypeNodeID> children(static_cast<size_t>(edge_offset));
    for (size_t index = 1; index < nodes.size(); ++index)
    {
        const auto parent = nodes[index].parent;
        const UInt32 child_slot = nodes[parent].first_child + nodes[parent].child_count++;
        if (child_slot != nodes[parent].first_child && nodes[children[child_slot - 1]].child_ordinal >= nodes[index].child_ordinal)
            descriptorError(
                DescriptorError::Code::InvalidPath, "bound declared-type child ordinals are not strictly ordered");
        children[child_slot] = static_cast<BoundDeclaredTypeNodeID>(index);
    }
    for (size_t index = nodes.size(); index-- > 1;)
        nodes[nodes[index].parent].subtree_semantic_capabilities |= nodes[index].subtree_semantic_capabilities;

    return Ptr(new BoundDeclaredTypeTree(
        std::move(nodes), std::move(children), std::move(descriptor_occurrences), std::move(descriptors), std::move(definitions)));
}

const BoundDeclaredTypeNode & BoundDeclaredTypeTree::getNode(BoundDeclaredTypeNodeID node) const
{
    if (node >= nodes.size())
        descriptorError(DescriptorError::Code::InvalidPath, "bound declared-type node ID is out of range");
    return nodes[node];
}

std::span<const UInt32> BoundDeclaredTypeTree::getDescriptorIndices(BoundDeclaredTypeNodeID node) const
{
    const auto & bound_node = getNode(node);
    return std::span(descriptor_occurrences).subspan(bound_node.first_occurrence, bound_node.occurrence_count);
}

std::optional<BoundDeclaredTypeNodeID> BoundDeclaredTypeTree::findNode(std::span<const UInt32> type_child_ordinals) const noexcept
{
    BoundDeclaredTypeNodeID current = 0;
    for (const UInt32 ordinal : type_child_ordinals)
    {
        const auto & node = nodes[current];
        const auto first = children.begin() + node.first_child;
        const auto last = first + node.child_count;
        const auto child = std::lower_bound(
            first,
            last,
            ordinal,
            [this](BoundDeclaredTypeNodeID child_id, UInt32 expected_ordinal) { return nodes[child_id].child_ordinal < expected_ordinal; });
        if (child == last || nodes[*child].child_ordinal != ordinal)
            return std::nullopt;
        current = *child;
    }
    return current;
}

BoundDeclaredTypeResult::BoundDeclaredTypeResult(DataTypePtr physical_type_, BoundDeclaredTypeTree::Ptr logical_tree_)
    : physical_type(std::move(physical_type_))
    , logical_tree(std::move(logical_tree_))
{
}

BoundDeclaredTypeResult BoundDeclaredTypeResult::physicalOnly(DataTypePtr physical_type)
{
    if (!physical_type)
        descriptorError(DescriptorError::Code::InvalidPhysicalType, "physical-only declared type has no physical type");
    return BoundDeclaredTypeResult(std::move(physical_type), nullptr);
}

BoundDeclaredTypeResult BoundDeclaredTypeResult::withLogicalTree(BoundDeclaredTypeTree::Ptr logical_tree)
{
    if (!logical_tree)
        descriptorError(DescriptorError::Code::InvalidDefinition, "bound declared type has no logical tree");
    DataTypePtr physical_type = logical_tree->getPhysicalType();
    return BoundDeclaredTypeResult(std::move(physical_type), std::move(logical_tree));
}

}
