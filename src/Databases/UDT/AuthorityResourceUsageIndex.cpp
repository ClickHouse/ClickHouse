#include <Databases/UDT/AuthorityResourceUsageIndex.h>

#include <Databases/DatabaseSchemaWAL.h>

#include <DataTypes/UDT/Catalog.h>
#include <DataTypes/UDT/DependentObjectMetadataInstallationRecord.h>
#include <DataTypes/UDT/ResourceLimits.h>

#include <Core/Defines.h>

#include <Parsers/IAST.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/parseQuery.h>

#include <algorithm>
#include <array>
#include <limits>
#include <utility>
#include <vector>

namespace DB::UDT
{
namespace
{

using Error = AuthorityResourceUsageIndexError;

constexpr std::string_view object_key_domain = "ClickHouse UDT authority quota object key V1";
constexpr std::string_view specialization_key_domain = "ClickHouse UDT authority quota specialization key V1";
constexpr std::string_view template_key_domain = "ClickHouse UDT authority quota template key V1";
constexpr UInt64 maximum_implementation_objects = 1ULL << 24;
constexpr UInt64 maximum_radix_children_capacity = 32;
constexpr size_t digest_nibbles = sizeof(Digest) * 2;

[[noreturn]] void fail(Error::Code code, std::string_view message)
{
    throw Error(code, message);
}

UInt64 checkedAdd(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        fail(Error::Code::LimitExceeded, message);
    return lhs + rhs;
}

UInt64 checkedMultiply(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (lhs && rhs > std::numeric_limits<UInt64>::max() / lhs)
        fail(Error::Code::LimitExceeded, message);
    return lhs * rhs;
}

UInt64 toUInt64(size_t value, std::string_view message)
{
    if (!std::in_range<UInt64>(value))
        fail(Error::Code::LimitExceeded, message);
    return static_cast<UInt64>(value);
}

void validateLimits(const AuthorityResourceUsageIndexLimits & limits)
{
    if (!limits.maximum_objects || limits.maximum_objects > maximum_implementation_objects)
        fail(Error::Code::InvalidConfiguration, "authority resource-index object limit is invalid");
    try
    {
        validatePersistedTypeReferencesLimits(limits.persisted_references);
    }
    catch (const PersistedTypeReferencesError &)
    {
        fail(Error::Code::InvalidConfiguration, "authority resource-index sidecar limits are invalid");
    }
}

void updateUInt64LE(CanonicalHasher & hasher, UInt64 value)
{
    std::array<CanonicalByte, sizeof(value)> bytes{};
    for (size_t index = 0; index < bytes.size(); ++index)
        bytes[index] = static_cast<CanonicalByte>(value >> (8 * index));
    hasher.update(bytes);
}

void updateFrame(CanonicalHasher & hasher, std::string_view value)
{
    UInt64 size = toUInt64(value.size(), "authority resource-index frame does not fit UInt64");
    std::array<CanonicalByte, 10> encoded{};
    size_t encoded_size = 0;
    do
    {
        UInt8 byte = static_cast<UInt8>(size & 0x7f);
        size >>= 7;
        if (size)
            byte = static_cast<UInt8>(byte | 0x80);
        encoded[encoded_size++] = byte;
    } while (size);
    hasher.update(std::span(encoded).first(encoded_size));
    hasher.update(value);
}

Digest makeObjectKey(const SchemaObjectID & object)
{
    CanonicalHasher hasher(object_key_domain);
    const std::array<CanonicalByte, 1> kind{static_cast<CanonicalByte>(object.kind)};
    hasher.update(kind);
    hasher.updateUUID(object.database_uuid);
    hasher.updateUUID(object.object_uuid);
    return hasher.finalize();
}

Digest makeTemplateKey(const DefinitionIdentity & identity, const Digest & definition_hash)
{
    CanonicalHasher hasher(template_key_domain);
    hasher.updateUUID(identity.database_uuid);
    hasher.updateUUID(identity.type_uuid);
    updateUInt64LE(hasher, identity.revision);
    hasher.update(definition_hash);
    return hasher.finalize();
}

Digest makeSpecializationKey(const PersistedTypeDescriptor & descriptor)
{
    const auto & identity = descriptor.getDefinitionIdentity();
    CanonicalHasher hasher(specialization_key_domain);
    hasher.updateUUID(identity.database_uuid);
    hasher.updateUUID(identity.type_uuid);
    updateUInt64LE(hasher, identity.revision);
    hasher.update(descriptor.getDefinitionHash());
    updateFrame(hasher, descriptor.getCanonicalArgumentsEncoding());
    return hasher.finalize();
}

UInt8 keyNibble(const Digest & key, size_t depth) noexcept
{
    const UInt8 byte = key[depth / 2];
    return depth % 2 == 0 ? static_cast<UInt8>(byte >> 4) : static_cast<UInt8>(byte & 0x0f);
}

size_t firstDifferentNibble(const Digest & lhs, const Digest & rhs) noexcept
{
    for (size_t depth = 0; depth < digest_nibbles; ++depth)
        if (keyNibble(lhs, depth) != keyNibble(rhs, depth))
            return depth;
    return digest_nibbles;
}

struct SpecializationReference
{
    Digest specialization_key{};
    Digest template_key{};
};

struct ObjectContribution
{
    SidecarExpectationRecord expectation;
    UInt64 occurrence_paths = 0;
    UInt64 sidecar_bytes = 0;
    UInt64 durable_dependent_object_bytes = 0;
    std::vector<SpecializationReference> specializations;
};

struct SpecializationContribution
{
    DefinitionIdentity definition_identity;
    Digest definition_hash{};
    String canonical_arguments;
    String canonical_physical_type;
    Digest template_key{};
    UInt64 object_refcount = 0;
    UInt64 physical_type_nodes = 0;
};

struct TemplateContribution
{
    DefinitionIdentity definition_identity;
    Digest definition_hash{};
    UInt64 specialization_count = 0;
};

template <typename Leaf>
struct DigestRadixNode
{
    using Ptr = std::shared_ptr<const DigestRadixNode>;

    bool is_leaf = false;
    Digest representative_key{};
    Leaf leaf;
    UInt16 branch_depth = 0;
    std::vector<std::pair<UInt8, Ptr>> children;
    UInt64 metric = 0;
    UInt64 subtree_leaf_count = 0;
    UInt64 subtree_metric_sum = 0;
    UInt64 subtree_metric_maximum = 0;
    UInt64 subtree_secondary_metric_maximum = 0;
    UInt64 subtree_tertiary_metric_sum = 0;
    UInt64 subtree_tertiary_metric_maximum = 0;
    UInt64 subtree_accounted_bytes = 0;
};

template <typename Leaf>
using DigestRadixNodePtr = typename DigestRadixNode<Leaf>::Ptr;

template <typename Leaf>
UInt64 nodeBaseAccountedBytes() noexcept
{
    return sizeof(DigestRadixNode<Leaf>) + 2 * sizeof(void *);
}

template <typename Leaf>
DigestRadixNodePtr<Leaf>
makeLeaf(Digest key, Leaf leaf, UInt64 metric, UInt64 secondary_metric, UInt64 tertiary_metric, UInt64 dynamic_bytes)
{
    auto result = std::make_shared<DigestRadixNode<Leaf>>();
    result->is_leaf = true;
    result->representative_key = key;
    result->leaf = std::move(leaf);
    result->metric = metric;
    result->subtree_leaf_count = 1;
    result->subtree_metric_sum = metric;
    result->subtree_metric_maximum = metric;
    result->subtree_secondary_metric_maximum = secondary_metric;
    result->subtree_tertiary_metric_sum = tertiary_metric;
    result->subtree_tertiary_metric_maximum = tertiary_metric;
    result->subtree_accounted_bytes
        = checkedAdd(nodeBaseAccountedBytes<Leaf>(), dynamic_bytes, "authority resource-index leaf charge overflows UInt64");
    return result;
}

template <typename Leaf>
DigestRadixNodePtr<Leaf> makeBranch(UInt16 depth, std::vector<std::pair<UInt8, DigestRadixNodePtr<Leaf>>> children)
{
    if (children.size() < 2 || depth >= digest_nibbles)
        fail(Error::Code::InvalidInput, "authority resource-index radix branch is invalid");
    std::sort(children.begin(), children.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });
    if (children.capacity() > maximum_radix_children_capacity)
        fail(Error::Code::LimitExceeded, "authority resource-index radix child capacity exceeds its bound");

    auto result = std::make_shared<DigestRadixNode<Leaf>>();
    result->representative_key = children.front().second->representative_key;
    result->branch_depth = depth;
    result->children = std::move(children);
    result->subtree_accounted_bytes = checkedAdd(
        nodeBaseAccountedBytes<Leaf>(),
        checkedMultiply(
            toUInt64(result->children.capacity(), "authority resource-index child capacity does not fit UInt64"),
            sizeof(std::pair<UInt8, DigestRadixNodePtr<Leaf>>),
            "authority resource-index child charge overflows UInt64"),
        "authority resource-index branch charge overflows UInt64");
    for (const auto & [_, child] : result->children)
    {
        result->subtree_leaf_count
            = checkedAdd(result->subtree_leaf_count, child->subtree_leaf_count, "authority resource-index leaf count overflows UInt64");
        result->subtree_metric_sum
            = checkedAdd(result->subtree_metric_sum, child->subtree_metric_sum, "authority resource-index metric sum overflows UInt64");
        result->subtree_metric_maximum = std::max(result->subtree_metric_maximum, child->subtree_metric_maximum);
        result->subtree_secondary_metric_maximum
            = std::max(result->subtree_secondary_metric_maximum, child->subtree_secondary_metric_maximum);
        result->subtree_tertiary_metric_sum = checkedAdd(
            result->subtree_tertiary_metric_sum,
            child->subtree_tertiary_metric_sum,
            "authority resource-index tertiary metric sum overflows UInt64");
        result->subtree_tertiary_metric_maximum = std::max(result->subtree_tertiary_metric_maximum, child->subtree_tertiary_metric_maximum);
        result->subtree_accounted_bytes = checkedAdd(
            result->subtree_accounted_bytes, child->subtree_accounted_bytes, "authority resource-index radix charge overflows UInt64");
    }
    return result;
}

template <typename Leaf>
const DigestRadixNode<Leaf> * findLeaf(const std::shared_ptr<const DigestRadixNode<Leaf>> & root, const Digest & key) noexcept
{
    auto current = root;
    while (current)
    {
        if (current->is_leaf)
            return current->representative_key == key ? current.get() : nullptr;
        const UInt8 nibble = keyNibble(key, current->branch_depth);
        const auto child = std::lower_bound(
            current->children.begin(),
            current->children.end(),
            nibble,
            [](const auto & candidate, UInt8 value) { return candidate.first < value; });
        if (child == current->children.end() || child->first != nibble)
            return nullptr;
        current = child->second;
    }
    return nullptr;
}

template <typename Leaf>
DigestRadixNodePtr<Leaf> insertLeaf(
    const DigestRadixNodePtr<Leaf> & root,
    Digest key,
    Leaf leaf,
    UInt64 metric,
    UInt64 secondary_metric,
    UInt64 tertiary_metric,
    UInt64 dynamic_bytes)
{
    if (!root)
        return makeLeaf(std::move(key), std::move(leaf), metric, secondary_metric, tertiary_metric, dynamic_bytes);
    if (root->is_leaf)
    {
        if (root->representative_key == key)
            return makeLeaf(std::move(key), std::move(leaf), metric, secondary_metric, tertiary_metric, dynamic_bytes);
        const size_t depth = firstDifferentNibble(root->representative_key, key);
        auto addition = makeLeaf(key, std::move(leaf), metric, secondary_metric, tertiary_metric, dynamic_bytes);
        return makeBranch<Leaf>(
            static_cast<UInt16>(depth), {{keyNibble(root->representative_key, depth), root}, {keyNibble(key, depth), std::move(addition)}});
    }

    const size_t divergence = firstDifferentNibble(root->representative_key, key);
    if (divergence < root->branch_depth)
    {
        auto addition = makeLeaf(key, std::move(leaf), metric, secondary_metric, tertiary_metric, dynamic_bytes);
        return makeBranch<Leaf>(
            static_cast<UInt16>(divergence),
            {{keyNibble(root->representative_key, divergence), root}, {keyNibble(key, divergence), std::move(addition)}});
    }

    const UInt8 nibble = keyNibble(key, root->branch_depth);
    auto children = root->children;
    auto child = std::lower_bound(
        children.begin(), children.end(), nibble, [](const auto & candidate, UInt8 value) { return candidate.first < value; });
    if (child == children.end() || child->first != nibble)
        children.insert(child, {nibble, makeLeaf(key, std::move(leaf), metric, secondary_metric, tertiary_metric, dynamic_bytes)});
    else
        child->second
            = insertLeaf(child->second, std::move(key), std::move(leaf), metric, secondary_metric, tertiary_metric, dynamic_bytes);
    return makeBranch<Leaf>(root->branch_depth, std::move(children));
}

template <typename Leaf>
DigestRadixNodePtr<Leaf> removeLeaf(const std::shared_ptr<const DigestRadixNode<Leaf>> & root, const Digest & key)
{
    if (!root)
        fail(Error::Code::MissingObject, "authority resource-index removal misses a leaf");
    if (root->is_leaf)
    {
        if (root->representative_key != key)
            fail(Error::Code::MissingObject, "authority resource-index removal misses a leaf");
        return {};
    }

    const UInt8 nibble = keyNibble(key, root->branch_depth);
    auto children = root->children;
    auto child = std::lower_bound(
        children.begin(), children.end(), nibble, [](const auto & candidate, UInt8 value) { return candidate.first < value; });
    if (child == children.end() || child->first != nibble)
        fail(Error::Code::MissingObject, "authority resource-index removal misses a branch");
    auto replacement = removeLeaf(child->second, key);
    if (replacement)
        child->second = std::move(replacement);
    else
        children.erase(child);
    if (children.size() == 1)
        return children.front().second;
    return makeBranch<Leaf>(root->branch_depth, std::move(children));
}

bool definitionMatchesDescriptor(const Definition & definition, const PersistedTypeDescriptor & descriptor) noexcept
{
    return definition.getIdentity() == descriptor.getDefinitionIdentity()
        && definition.getDefinitionHash() == descriptor.getDefinitionHash() && definition.getCheckerABI() == descriptor.getCheckerABI()
        && definition.getCheckerChargeABI() == descriptor.getCheckerChargeABI() && definition.getPolicyABI() == descriptor.getPolicyABI()
        && definition.getFunctionRegistryABI() == descriptor.getFunctionRegistryABI()
        && definition.getPolicySemanticHash() == descriptor.getPolicySemanticHash()
        && definition.getSemanticCapabilities() == descriptor.getSemanticCapabilities();
}

UInt64 objectDynamicBytes(const ObjectContribution & object)
{
    return checkedMultiply(
        toUInt64(object.specializations.capacity(), "authority object contribution capacity does not fit UInt64"),
        sizeof(SpecializationReference),
        "authority object contribution charge overflows UInt64");
}

UInt64 specializationDynamicBytes(const SpecializationContribution & specialization)
{
    return checkedAdd(
        checkedAdd(
            toUInt64(specialization.canonical_arguments.capacity(), "authority specialization arguments capacity does not fit UInt64"),
            toUInt64(
                specialization.canonical_physical_type.capacity(), "authority specialization physical type capacity does not fit UInt64"),
            "authority specialization string charge overflows UInt64"),
        2,
        "authority specialization arguments charge overflows UInt64");
}

bool specializationMatchesDescriptor(
    const SpecializationContribution & specialization, const PersistedTypeDescriptor & descriptor, const Digest & template_key) noexcept
{
    return specialization.definition_identity == descriptor.getDefinitionIdentity()
        && specialization.definition_hash == descriptor.getDefinitionHash()
        && specialization.canonical_arguments == descriptor.getCanonicalArgumentsEncoding()
        && specialization.canonical_physical_type == descriptor.getCanonicalPhysicalType() && specialization.template_key == template_key;
}

bool sameTemplate(const TemplateContribution & lhs, const TemplateContribution & rhs) noexcept
{
    return lhs.definition_identity == rhs.definition_identity && lhs.definition_hash == rhs.definition_hash;
}

struct DecodedObjectContribution
{
    ObjectContribution object;
    PersistedTypeReferences references;
};

DecodedObjectContribution decodeContribution(
    UUID database_uuid,
    const SidecarExpectationRecord & expectation,
    const AuthorityDependentObjectResourceImage & dependent_object,
    const TypeCatalogRoot & catalog,
    const AuthorityResourceUsageIndexLimits & limits)
{
    if (!expectation.object.isValid() || expectation.object.kind == SchemaObjectKind::TypeDefinition
        || expectation.object.database_uuid != database_uuid)
        fail(Error::Code::DatabaseMismatch, "authority resource-index expectation belongs to another database");
    if (dependent_object.object != expectation.object || dependent_object.canonical_metadata_bytes.empty()
        || dependent_object.canonical_sidecar_bytes.empty()
        || (expectation.installation_record_hash.has_value() != !dependent_object.canonical_installation_record_bytes.empty()))
    {
        fail(Error::Code::InvalidInput, "authority resource-index dependent-object image is incomplete");
    }

    const auto canonical_sidecar_bytes = dependent_object.canonical_sidecar_bytes;
    UInt64 durable_dependent_object_bytes
        = toUInt64(dependent_object.canonical_metadata_bytes.size(), "authority metadata byte count does not fit UInt64");
    durable_dependent_object_bytes = checkedAdd(
        durable_dependent_object_bytes,
        toUInt64(canonical_sidecar_bytes.size(), "authority sidecar byte count does not fit UInt64"),
        "authority durable dependent-object byte count overflows UInt64");
    durable_dependent_object_bytes = checkedAdd(
        durable_dependent_object_bytes,
        toUInt64(
            dependent_object.canonical_installation_record_bytes.size(), "authority installation-record byte count does not fit UInt64"),
        "authority durable dependent-object byte count overflows UInt64");

    if (expectation.installation_record_hash)
    {
        DependentObjectMetadataInstallationRecord installation;
        try
        {
            installation = decodeDependentObjectMetadataInstallationRecord(
                dependent_object.canonical_installation_record_bytes, DependentObjectMetadataInstallationRecordLimits{});
            if (encodeDependentObjectMetadataInstallationRecord(installation, DependentObjectMetadataInstallationRecordLimits{})
                    != dependent_object.canonical_installation_record_bytes
                || computeDependentObjectMetadataInstallationRecordHash(installation, DependentObjectMetadataInstallationRecordLimits{})
                    != *expectation.installation_record_hash)
            {
                fail(Error::Code::ExpectationMismatch, "authority installation record differs from its expectation");
            }
        }
        catch (const DependentObjectMetadataInstallationRecordError & error)
        {
            if (error.code == DependentObjectMetadataInstallationRecordError::Code::LimitExceeded)
                fail(Error::Code::LimitExceeded, "authority installation record exceeds its implementation limit");
            fail(Error::Code::ExpectationMismatch, "authority installation record is invalid");
        }
        if (installation.object != expectation.object || installation.object_schema_revision != expectation.object_schema_revision
            || computeDatabaseSchemaWALStagedArtifactHash(
                   DatabaseSchemaWALStagedArtifactKind::DependentObjectMetadata, dependent_object.canonical_metadata_bytes)
                != installation.metadata_artifact_hash)
        {
            fail(Error::Code::ExpectationMismatch, "authority metadata differs from its installation record");
        }
    }

    PersistedTypeReferences references;
    try
    {
        references = decodePersistedTypeReferences(canonical_sidecar_bytes, limits.persisted_references);
        if (encodePersistedTypeReferences(references, limits.persisted_references) != canonical_sidecar_bytes)
            fail(Error::Code::SidecarMismatch, "authority resource-index sidecar bytes are not canonical");
        if (computePersistedTypeReferencesSidecarHash(references, limits.persisted_references) != expectation.sidecar_hash)
            fail(Error::Code::ExpectationMismatch, "authority resource-index sidecar hash differs from its expectation");
    }
    catch (const PersistedTypeReferencesError & error)
    {
        if (error.code == PersistedTypeReferencesError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, "authority resource-index sidecar exceeds its limit");
        fail(Error::Code::SidecarMismatch, "authority resource-index sidecar is invalid");
    }

    if (references.object != expectation.object || references.object_schema_revision != expectation.object_schema_revision
        || references.physical_schema_fingerprint != expectation.physical_schema_fingerprint
        || references.semantic_extension_version != expectation.semantic_extension_version
        || references.semantic_extension_flags != expectation.semantic_extension_flags)
        fail(Error::Code::ExpectationMismatch, "authority resource-index sidecar identity differs from its expectation");

    ObjectContribution result{
        .expectation = expectation,
        .occurrence_paths = toUInt64(references.occurrence_paths.size(), "authority occurrence count does not fit UInt64"),
        .sidecar_bytes = toUInt64(canonical_sidecar_bytes.size(), "authority sidecar byte count does not fit UInt64"),
        .durable_dependent_object_bytes = durable_dependent_object_bytes,
        .specializations = {},
    };
    result.specializations.reserve(references.descriptors.size());
    for (const auto & descriptor : references.descriptors)
    {
        const auto definition = catalog.findByIdentity(descriptor.getDefinitionIdentity());
        if (!definition || !definitionMatchesDescriptor(*definition, descriptor))
            fail(Error::Code::DefinitionMismatch, "authority resource-index descriptor differs from the exact catalog definition");
        result.specializations.push_back({
            .specialization_key = makeSpecializationKey(descriptor),
            .template_key = makeTemplateKey(descriptor.getDefinitionIdentity(), descriptor.getDefinitionHash()),
        });
    }
    std::sort(
        result.specializations.begin(),
        result.specializations.end(),
        [](const auto & lhs, const auto & rhs) { return lhs.specialization_key < rhs.specialization_key; });
    for (size_t index = 1; index < result.specializations.size(); ++index)
    {
        if (result.specializations[index - 1].specialization_key == result.specializations[index].specialization_key)
            fail(Error::Code::SidecarMismatch, "authority resource-index specialization key collides within one sidecar");
    }
    return {
        .object = std::move(result),
        .references = std::move(references),
    };
}

UInt64 countCanonicalPhysicalTypeNodes(std::string_view canonical_physical_type)
{
    ParserDataType parser;
    String parse_error;
    const char * position = canonical_physical_type.data();
    ASTPtr ast = tryParseQuery(
        parser,
        position,
        canonical_physical_type.data() + canonical_physical_type.size(),
        parse_error,
        false,
        "persisted physical data type",
        false,
        canonical_physical_type.size(),
        64,
        DBMS_DEFAULT_MAX_PARSER_BACKTRACKS,
        true);
    if (!ast)
        fail(Error::Code::SidecarMismatch, "authority specialization physical type is not a complete data type");

    const UInt64 maximum_nodes = getResourceImplementationLimits().get(ResourceLimit::LoweredPhysicalTypeNodes);
    std::vector<const IAST *> pending;
    pending.push_back(ast.get());
    UInt64 nodes = 0;
    while (!pending.empty())
    {
        const IAST * node = pending.back();
        pending.pop_back();
        if (!node)
            fail(Error::Code::SidecarMismatch, "authority specialization physical type contains a null AST child");
        if (nodes == maximum_nodes)
            fail(Error::Code::LimitExceeded, "authority specialization physical type exceeds its node limit");
        ++nodes;
        const UInt64 pending_nodes = toUInt64(pending.size(), "authority physical type pending-node count does not fit UInt64");
        const UInt64 child_nodes = toUInt64(node->children.size(), "authority physical type child count does not fit UInt64");
        if (pending_nodes > maximum_nodes - nodes || child_nodes > maximum_nodes - nodes - pending_nodes)
            fail(Error::Code::LimitExceeded, "authority specialization physical type exceeds its node limit");
        for (const auto & child : node->children)
            pending.push_back(child.get());
    }
    return nodes;
}

SpecializationContribution specializationFromDescriptor(const PersistedTypeDescriptor & descriptor)
{
    return {
        .definition_identity = descriptor.getDefinitionIdentity(),
        .definition_hash = descriptor.getDefinitionHash(),
        .canonical_arguments = descriptor.getCanonicalArgumentsEncoding(),
        .canonical_physical_type = descriptor.getCanonicalPhysicalType(),
        .template_key = makeTemplateKey(descriptor.getDefinitionIdentity(), descriptor.getDefinitionHash()),
        .object_refcount = 1,
        .physical_type_nodes = countCanonicalPhysicalTypeNodes(descriptor.getCanonicalPhysicalType()),
    };
}

TemplateContribution templateFromSpecialization(const SpecializationContribution & specialization)
{
    return {
        .definition_identity = specialization.definition_identity,
        .definition_hash = specialization.definition_hash,
        .specialization_count = 1,
    };
}

}

struct AuthorityResourceUsageIndex::Impl
{
    using ObjectNode = DigestRadixNode<ObjectContribution>;
    using SpecializationNode = DigestRadixNode<SpecializationContribution>;
    using TemplateNode = DigestRadixNode<TemplateContribution>;

    UUID database_uuid = UUIDHelpers::Nil;
    ObjectNode::Ptr objects;
    SpecializationNode::Ptr specializations;
    TemplateNode::Ptr templates;
    AuthorityResourceUsageSummary summary;
    UInt64 accounted_bytes = 0;
};

namespace
{

using Impl = AuthorityResourceUsageIndex::Impl;

std::shared_ptr<const Impl> finalizeImpl(
    UUID database_uuid,
    Impl::ObjectNode::Ptr objects,
    Impl::SpecializationNode::Ptr specializations,
    Impl::TemplateNode::Ptr templates,
    const AuthorityResourceUsageIndexLimits & limits)
{
    const UInt64 object_count = objects ? objects->subtree_leaf_count : 0;
    if (object_count > limits.maximum_objects)
        fail(Error::Code::LimitExceeded, "authority resource-index object count exceeds its limit");
    auto result = std::make_shared<Impl>();
    result->database_uuid = database_uuid;
    result->objects = std::move(objects);
    result->specializations = std::move(specializations);
    result->templates = std::move(templates);
    result->summary = {
        .object_count = object_count,
        .total_occurrence_paths = result->objects ? result->objects->subtree_metric_sum : 0,
        .unique_persisted_specializations = result->specializations ? result->specializations->subtree_leaf_count : 0,
        .maximum_occurrence_paths_per_object = result->objects ? result->objects->subtree_metric_maximum : 0,
        .maximum_persisted_specializations_per_template = result->templates ? result->templates->subtree_metric_maximum : 0,
        .maximum_canonical_argument_bytes = result->specializations ? result->specializations->subtree_secondary_metric_maximum : 0,
        .maximum_lowered_physical_type_nodes = result->specializations ? result->specializations->subtree_tertiary_metric_maximum : 0,
        .maximum_sidecar_bytes_per_object = result->objects ? result->objects->subtree_secondary_metric_maximum : 0,
        .total_durable_dependent_object_bytes = result->objects ? result->objects->subtree_tertiary_metric_sum : 0,
        .maximum_durable_dependent_object_bytes_per_object = result->objects ? result->objects->subtree_tertiary_metric_maximum : 0,
    };
    if (result->summary.total_durable_dependent_object_bytes > resource_implementation_maximum_durable_dependent_object_bytes)
    {
        fail(Error::Code::LimitExceeded, "authority durable dependent-object bytes exceed the immutable implementation limit");
    }
    result->accounted_bytes = sizeof(Impl) + 2 * sizeof(void *);
    if (result->objects)
        result->accounted_bytes = checkedAdd(
            result->accounted_bytes, result->objects->subtree_accounted_bytes, "authority resource-index charge overflows UInt64");
    if (result->specializations)
        result->accounted_bytes = checkedAdd(
            result->accounted_bytes, result->specializations->subtree_accounted_bytes, "authority resource-index charge overflows UInt64");
    if (result->templates)
        result->accounted_bytes = checkedAdd(
            result->accounted_bytes, result->templates->subtree_accounted_bytes, "authority resource-index charge overflows UInt64");
    return result;
}

std::shared_ptr<const Impl> addDecodedObject(
    const std::shared_ptr<const Impl> & base,
    ObjectContribution object,
    const PersistedTypeReferences & references,
    const AuthorityResourceUsageIndexLimits & limits)
{
    const Digest object_key = makeObjectKey(object.expectation.object);
    if (findLeaf(base->objects, object_key))
        fail(Error::Code::DuplicateObject, "authority resource-index object already exists");
    if (object.specializations.size() != references.descriptors.size())
        fail(Error::Code::SidecarMismatch, "authority resource-index descriptor projection is incomplete");

    auto specializations = base->specializations;
    auto templates = base->templates;
    for (const auto & descriptor : references.descriptors)
    {
        const Digest specialization_key = makeSpecializationKey(descriptor);
        const Digest template_key = makeTemplateKey(descriptor.getDefinitionIdentity(), descriptor.getDefinitionHash());
        if (const auto * existing = findLeaf(specializations, specialization_key))
        {
            if (!specializationMatchesDescriptor(existing->leaf, descriptor, template_key))
                fail(Error::Code::SidecarMismatch, "authority resource-index specialization digest collision");
            auto specialization = existing->leaf;
            specialization.object_refcount
                = checkedAdd(specialization.object_refcount, 1, "authority specialization object refcount overflows UInt64");
            specializations = insertLeaf(
                specializations,
                specialization_key,
                specialization,
                specialization.object_refcount,
                toUInt64(specialization.canonical_arguments.size(), "authority specialization arguments do not fit UInt64"),
                specialization.physical_type_nodes,
                specializationDynamicBytes(specialization));
            continue;
        }

        SpecializationContribution specialization = specializationFromDescriptor(descriptor);
        TemplateContribution template_contribution = templateFromSpecialization(specialization);
        if (const auto * existing_template = findLeaf(templates, template_key))
        {
            if (!sameTemplate(existing_template->leaf, template_contribution))
                fail(Error::Code::SidecarMismatch, "authority resource-index template digest collision");
            template_contribution = existing_template->leaf;
            template_contribution.specialization_count
                = checkedAdd(template_contribution.specialization_count, 1, "authority per-template specialization count overflows UInt64");
        }
        templates = insertLeaf(templates, template_key, template_contribution, template_contribution.specialization_count, 0, 0, 0);
        specializations = insertLeaf(
            specializations,
            specialization_key,
            specialization,
            specialization.object_refcount,
            toUInt64(specialization.canonical_arguments.size(), "authority specialization arguments do not fit UInt64"),
            specialization.physical_type_nodes,
            specializationDynamicBytes(specialization));
    }

    const UInt64 occurrence_paths = object.occurrence_paths;
    const UInt64 dynamic_bytes = objectDynamicBytes(object);
    const UInt64 sidecar_bytes = object.sidecar_bytes;
    const UInt64 durable_dependent_object_bytes = object.durable_dependent_object_bytes;
    auto objects = insertLeaf(
        base->objects, object_key, std::move(object), occurrence_paths, sidecar_bytes, durable_dependent_object_bytes, dynamic_bytes);
    return finalizeImpl(base->database_uuid, std::move(objects), std::move(specializations), std::move(templates), limits);
}

std::shared_ptr<const Impl> removeDecodedObject(
    const std::shared_ptr<const Impl> & base, const ObjectContribution & object, const AuthorityResourceUsageIndexLimits & limits)
{
    auto specializations = base->specializations;
    auto templates = base->templates;
    for (const auto & reference : object.specializations)
    {
        const auto * existing = findLeaf(specializations, reference.specialization_key);
        if (!existing || existing->leaf.template_key != reference.template_key || !existing->leaf.object_refcount)
            fail(Error::Code::InvalidInput, "authority resource-index specialization refcount is inconsistent");
        if (existing->leaf.object_refcount > 1)
        {
            auto replacement = existing->leaf;
            --replacement.object_refcount;
            specializations = insertLeaf(
                specializations,
                reference.specialization_key,
                replacement,
                replacement.object_refcount,
                toUInt64(replacement.canonical_arguments.size(), "authority specialization arguments do not fit UInt64"),
                replacement.physical_type_nodes,
                specializationDynamicBytes(replacement));
            continue;
        }

        specializations = removeLeaf(specializations, reference.specialization_key);
        const auto * existing_template = findLeaf(templates, reference.template_key);
        if (!existing_template || !existing_template->leaf.specialization_count)
            fail(Error::Code::InvalidInput, "authority resource-index template count is inconsistent");
        if (existing_template->leaf.specialization_count == 1)
            templates = removeLeaf(templates, reference.template_key);
        else
        {
            auto replacement = existing_template->leaf;
            --replacement.specialization_count;
            templates = insertLeaf(templates, reference.template_key, replacement, replacement.specialization_count, 0, 0, 0);
        }
    }
    auto objects = removeLeaf(base->objects, makeObjectKey(object.expectation.object));
    return finalizeImpl(base->database_uuid, std::move(objects), std::move(specializations), std::move(templates), limits);
}

}

AuthorityResourceUsageIndexError::AuthorityResourceUsageIndexError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

AuthorityResourceUsageIndex::AuthorityResourceUsageIndex(std::shared_ptr<const Impl> impl_)
    : impl(std::move(impl_))
{
}

AuthorityResourceUsageIndex::~AuthorityResourceUsageIndex() = default;

AuthorityResourceUsageIndex::Ptr AuthorityResourceUsageIndex::build(
    UUID database_uuid,
    std::span<const SidecarExpectationRecord> expectations,
    std::span<const AuthorityDependentObjectResourceImage> dependent_objects,
    const TypeCatalogRoot & catalog,
    const AuthorityResourceUsageIndexLimits & limits)
{
    validateLimits(limits);
    if (database_uuid == UUIDHelpers::Nil || catalog.getDatabaseUUID() != database_uuid || expectations.size() != dependent_objects.size())
        fail(Error::Code::InvalidInput, "authority resource-index full-build input is inconsistent");
    if (expectations.size() > limits.maximum_objects)
        fail(Error::Code::LimitExceeded, "authority resource-index object count exceeds its limit");

    std::vector<const SidecarExpectationRecord *> ordered_expectations;
    ordered_expectations.reserve(expectations.size());
    for (const auto & expectation : expectations)
    {
        if (!expectation.object.isValid() || expectation.object.kind == SchemaObjectKind::TypeDefinition
            || expectation.object.database_uuid != database_uuid)
            fail(Error::Code::DatabaseMismatch, "authority resource-index expectation belongs to another database");
        ordered_expectations.push_back(&expectation);
    }

    std::vector<const AuthorityDependentObjectResourceImage *> ordered_dependent_objects;
    ordered_dependent_objects.reserve(dependent_objects.size());
    for (const auto & dependent_object : dependent_objects)
    {
        if (!dependent_object.object.isValid() || dependent_object.object.kind == SchemaObjectKind::TypeDefinition
            || dependent_object.object.database_uuid != database_uuid)
            fail(Error::Code::DatabaseMismatch, "authority resource-index dependent object belongs to another database");
        ordered_dependent_objects.push_back(&dependent_object);
    }

    const auto object_less = [](const auto * lhs, const auto * rhs) { return lhs->object < rhs->object; };
    std::sort(ordered_expectations.begin(), ordered_expectations.end(), object_less);
    std::sort(ordered_dependent_objects.begin(), ordered_dependent_objects.end(), object_less);
    const auto has_duplicate_object = [](const auto & ordered)
    {
        return std::adjacent_find(
                   ordered.begin(), ordered.end(), [](const auto * lhs, const auto * rhs) { return lhs->object == rhs->object; })
            != ordered.end();
    };
    if (has_duplicate_object(ordered_expectations) || has_duplicate_object(ordered_dependent_objects))
        fail(Error::Code::DuplicateObject, "authority resource-index full-build input contains a duplicate object identity");

    auto current = finalizeImpl(database_uuid, {}, {}, {}, limits);
    for (size_t index = 0; index < ordered_expectations.size(); ++index)
    {
        const auto & expectation = *ordered_expectations[index];
        const auto & image = *ordered_dependent_objects[index];
        if (image.object != expectation.object)
            fail(Error::Code::MissingObject, "authority resource-index has no durable image for an expectation");
        auto decoded = decodeContribution(database_uuid, expectation, image, catalog, limits);
        current = addDecodedObject(current, std::move(decoded.object), decoded.references, limits);
    }
    if (current->summary.object_count != expectations.size())
        fail(Error::Code::DuplicateObject, "authority resource-index sidecar images contain duplicate identities");
    return Ptr(new AuthorityResourceUsageIndex(std::move(current)));
}

AuthorityResourceUsageIndex::Ptr AuthorityResourceUsageIndex::addObject(
    const Ptr & base,
    const SidecarExpectationRecord & expectation,
    const AuthorityDependentObjectResourceImage & dependent_object,
    const TypeCatalogRoot & catalog,
    const AuthorityResourceUsageIndexLimits & limits)
{
    validateLimits(limits);
    if (!base || catalog.getDatabaseUUID() != base->getDatabaseUUID())
        fail(Error::Code::InvalidInput, "authority resource-index addition has an invalid base");
    auto decoded = decodeContribution(base->getDatabaseUUID(), expectation, dependent_object, catalog, limits);
    return Ptr(new AuthorityResourceUsageIndex(addDecodedObject(base->impl, std::move(decoded.object), decoded.references, limits)));
}

AuthorityResourceUsageIndex::Ptr AuthorityResourceUsageIndex::replaceObject(
    const Ptr & base,
    const SidecarExpectationRecord & expectation,
    const AuthorityDependentObjectResourceImage & dependent_object,
    const TypeCatalogRoot & catalog,
    const AuthorityResourceUsageIndexLimits & limits)
{
    validateLimits(limits);
    if (!base || catalog.getDatabaseUUID() != base->getDatabaseUUID())
        fail(Error::Code::InvalidInput, "authority resource-index replacement has an invalid base");
    const Digest object_key = makeObjectKey(expectation.object);
    const auto * before = findLeaf(base->impl->objects, object_key);
    if (!before || before->leaf.expectation.object != expectation.object)
        fail(Error::Code::MissingObject, "authority resource-index replacement misses its object");
    auto removed = removeDecodedObject(base->impl, before->leaf, limits);
    auto decoded = decodeContribution(base->getDatabaseUUID(), expectation, dependent_object, catalog, limits);
    return Ptr(new AuthorityResourceUsageIndex(addDecodedObject(removed, std::move(decoded.object), decoded.references, limits)));
}

AuthorityResourceUsageIndex::Ptr AuthorityResourceUsageIndex::removeObjects(
    const Ptr & base, std::span<const SchemaObjectID> objects, const AuthorityResourceUsageIndexLimits & limits)
{
    validateLimits(limits);
    if (!base)
        fail(Error::Code::InvalidInput, "authority resource-index removal has no base");
    auto current = base->impl;
    for (size_t index = 0; index < objects.size(); ++index)
    {
        const auto & object = objects[index];
        if (!object.isValid() || object.database_uuid != base->getDatabaseUUID())
            fail(Error::Code::DatabaseMismatch, "authority resource-index removal belongs to another database");
        if (index && !(objects[index - 1] < object))
            fail(Error::Code::InvalidInput, "authority resource-index removals are not strictly sorted");
        const auto * existing = findLeaf(current->objects, makeObjectKey(object));
        if (!existing || existing->leaf.expectation.object != object)
            fail(Error::Code::MissingObject, "authority resource-index removal misses its object");
        current = removeDecodedObject(current, existing->leaf, limits);
    }
    return Ptr(new AuthorityResourceUsageIndex(std::move(current)));
}

const UUID & AuthorityResourceUsageIndex::getDatabaseUUID() const noexcept
{
    return impl->database_uuid;
}

const AuthorityResourceUsageSummary & AuthorityResourceUsageIndex::getSummary() const noexcept
{
    return impl->summary;
}

UInt64 AuthorityResourceUsageIndex::getAccountedBytes() const noexcept
{
    return impl->accounted_bytes + sizeof(AuthorityResourceUsageIndex) + 2 * sizeof(void *);
}

UInt64 AuthorityResourceUsageIndex::getObjectInsertionAccountedBytesUpperBound(
    UInt64 distinct_specializations, UInt64 canonical_argument_bytes, UInt64 canonical_physical_type_bytes)
{
    const auto branch_charge = [](UInt64 node_bytes)
    {
        return checkedAdd(
            node_bytes,
            checkedMultiply(
                maximum_radix_children_capacity,
                sizeof(std::pair<UInt8, std::shared_ptr<const void>>),
                "authority resource-index insertion branch bound overflows UInt64"),
            "authority resource-index insertion branch bound overflows UInt64");
    };
    UInt64 result = sizeof(Impl) + 2 * sizeof(void *);
    result = checkedAdd(
        result,
        checkedMultiply(
            digest_nibbles,
            branch_charge(nodeBaseAccountedBytes<ObjectContribution>()),
            "authority object resource-index insertion bound overflows UInt64"),
        "authority object resource-index insertion bound overflows UInt64");
    result = checkedAdd(result, nodeBaseAccountedBytes<ObjectContribution>(), "authority object leaf bound overflows UInt64");
    result = checkedAdd(
        result,
        checkedMultiply(
            distinct_specializations, sizeof(SpecializationReference), "authority object specialization-vector bound overflows UInt64"),
        "authority object specialization-vector bound overflows UInt64");

    UInt64 per_specialization = checkedMultiply(
        digest_nibbles,
        checkedAdd(
            branch_charge(nodeBaseAccountedBytes<SpecializationContribution>()),
            branch_charge(nodeBaseAccountedBytes<TemplateContribution>()),
            "authority specialization path-copy bound overflows UInt64"),
        "authority specialization path-copy bound overflows UInt64");
    per_specialization = checkedAdd(
        per_specialization,
        checkedAdd(
            nodeBaseAccountedBytes<SpecializationContribution>(),
            nodeBaseAccountedBytes<TemplateContribution>(),
            "authority specialization leaf bound overflows UInt64"),
        "authority specialization leaf bound overflows UInt64");
    result = checkedAdd(
        result,
        checkedMultiply(distinct_specializations, per_specialization, "authority specialization insertion bound overflows UInt64"),
        "authority specialization insertion bound overflows UInt64");
    result = checkedAdd(
        result,
        checkedAdd(
            checkedAdd(canonical_argument_bytes, canonical_physical_type_bytes, "authority specialization string bound overflows UInt64"),
            checkedMultiply(2, distinct_specializations, "authority specialization string slack overflows UInt64"),
            "authority specialization string bound overflows UInt64"),
        "authority specialization string bound overflows UInt64");
    return result;
}

bool AuthorityResourceUsageIndex::containsExactObject(const SidecarExpectationRecord & expectation) const noexcept
{
    if (expectation.object.database_uuid != impl->database_uuid)
        return false;
    const auto * existing = findLeaf(impl->objects, makeObjectKey(expectation.object));
    return existing && existing->leaf.expectation == expectation;
}

}
