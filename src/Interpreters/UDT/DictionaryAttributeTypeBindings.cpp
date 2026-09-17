#include <Interpreters/UDT/DictionaryAttributeTypeBindings.h>

#include <Interpreters/UDT/StoredObjectTypeBindingProvenance.h>

#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/PhysicalTypeFingerprint.h>

#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>

#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <algorithm>
#include <array>
#include <limits>
#include <set>
#include <unordered_map>
#include <utility>

namespace DB::UDT
{
namespace
{

using Error = DictionaryAttributeTypeBindingError;

[[noreturn]] void fail(Error::Code code, std::string_view message)
{
    throw Error(code, message);
}

UInt64 checkedSize(size_t value, std::string_view message)
{
    if (!std::in_range<UInt64>(value))
        fail(Error::Code::LimitExceeded, message);
    return static_cast<UInt64>(value);
}

void checkedAdd(UInt64 & total, UInt64 value, UInt64 maximum, std::string_view message)
{
    if (total > maximum || value > maximum - total)
        fail(Error::Code::LimitExceeded, message);
    total += value;
}

UInt64 checkedMultiply(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (lhs && rhs > std::numeric_limits<UInt64>::max() / lhs)
        fail(Error::Code::LimitExceeded, message);
    return lhs * rhs;
}

UInt64 varUIntSize(UInt64 value) noexcept
{
    UInt64 size = 1;
    while (value >= 0x80)
    {
        value >>= 7;
        ++size;
    }
    return size;
}

class EncodedSizeCounter final
{
public:
    explicit EncodedSizeCounter(UInt64 maximum_)
        : maximum(maximum_)
    {
    }

    void add(UInt64 value)
    {
        if (value > maximum || size > maximum - value)
            fail(Error::Code::LimitExceeded, "dictionary attribute persisted sidecar exceeds its byte limit");
        size += value;
    }

    void addVarUInt(UInt64 value) { add(varUIntSize(value)); }

    void addFrame(std::string_view value)
    {
        const UInt64 frame_size = checkedSize(value.size(), "dictionary attribute persisted descriptor field exceeds UInt64");
        addVarUInt(frame_size);
        add(frame_size);
    }

    UInt64 getSize() const noexcept { return size; }
    UInt64 getMaximum() const noexcept { return maximum; }

private:
    UInt64 maximum;
    UInt64 size = 0;
};

template <typename Callback>
void addNestedFrame(EncodedSizeCounter & counter, Callback && callback)
{
    EncodedSizeCounter nested(counter.getMaximum());
    callback(nested);
    counter.addVarUInt(nested.getSize());
    counter.add(nested.getSize());
}

void updateVarUInt(CanonicalHasher & hasher, UInt64 value)
{
    std::array<CanonicalByte, 10> encoded{};
    size_t size = 0;
    do
    {
        CanonicalByte byte = static_cast<CanonicalByte>(value & 0x7f);
        value >>= 7;
        if (value)
            byte = static_cast<CanonicalByte>(byte | 0x80);
        encoded[size++] = byte;
    } while (value);
    hasher.update(std::span<const CanonicalByte>(encoded.data(), size));
}

void updateFrame(CanonicalHasher & hasher, std::string_view value)
{
    updateVarUInt(hasher, checkedSize(value.size(), "dictionary attribute name size exceeds UInt64"));
    hasher.update(value);
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

bool pathLess(const PersistedTypeOccurrencePath & lhs, const PersistedTypeOccurrencePath & rhs) noexcept
{
    const UInt8 lhs_section = static_cast<UInt8>(lhs.section);
    const UInt8 rhs_section = static_cast<UInt8>(rhs.section);
    if (lhs_section != rhs_section)
        return lhs_section < rhs_section;
    const UInt8 lhs_site = static_cast<UInt8>(lhs.site);
    const UInt8 rhs_site = static_cast<UInt8>(rhs.site);
    if (lhs_site != rhs_site)
        return lhs_site < rhs_site;
    if (lhs.object_ordinal != rhs.object_ordinal)
        return lhs.object_ordinal < rhs.object_ordinal;
    if (lhs.occurrence_ordinal != rhs.occurrence_ordinal)
        return lhs.occurrence_ordinal < rhs.occurrence_ordinal;
    return std::lexicographical_compare(
        lhs.type_child_ordinals.begin(), lhs.type_child_ordinals.end(), rhs.type_child_ordinals.begin(), rhs.type_child_ordinals.end());
}

std::vector<UInt64> nodePath(const BoundDeclaredTypeTree & tree, BoundDeclaredTypeNodeID node_id)
{
    std::vector<UInt64> reversed;
    auto current = node_id;
    while (current != 0)
    {
        const auto & node = tree.getNode(current);
        if (node.getParent() == invalid_bound_declared_type_node_id)
            fail(Error::Code::ConflictingDescriptor, "non-root bound dictionary-attribute node has no parent");
        reversed.push_back(node.getChildOrdinal());
        current = node.getParent();
    }
    std::reverse(reversed.begin(), reversed.end());
    return reversed;
}

struct PendingOccurrence
{
    PersistedTypeOccurrencePath path;
    InstantiatedTypeDescriptor::Ptr descriptor;
    DataTypePtr physical_type;
    String runtime_owner_key;
};

class DictionaryPhysicalTopologyCache final
{
public:
    DictionaryPhysicalTopologyCache(UInt64 maximum_types_, UInt64 maximum_children_)
        : maximum_types(maximum_types_)
        , maximum_children(maximum_children_)
    {
    }

    const std::vector<DataTypePtr> & getObjectChildren(const DataTypePtr & type)
    {
        if (const auto found = object_children.find(type.get()); found != object_children.end())
            return found->second;
        if (checkedSize(object_children.size(), "dictionary physical topology cache size exceeds UInt64") >= maximum_types)
            fail(Error::Code::LimitExceeded, "dictionary physical topology cache exceeds its type limit");

        const auto & typed_paths = assert_cast<const DataTypeObject &>(*type).getTypedPaths();
        const UInt64 child_count = checkedSize(typed_paths.size(), "dictionary Object child count exceeds UInt64");
        checkedAdd(retained_children, child_count, maximum_children, "dictionary physical topology cache exceeds its retained child limit");
        std::vector<std::pair<std::string_view, DataTypePtr>> sorted;
        sorted.reserve(typed_paths.size());
        for (const auto & [path, child] : typed_paths)
            sorted.emplace_back(path, child);
        std::sort(sorted.begin(), sorted.end(), [](const auto & lhs, const auto & rhs) { return binaryLess(lhs.first, rhs.first); });

        std::vector<DataTypePtr> children;
        children.reserve(sorted.size());
        for (auto & [path, child] : sorted)
        {
            static_cast<void>(path);
            children.push_back(std::move(child));
        }
        return object_children.emplace(type.get(), std::move(children)).first->second;
    }

private:
    const UInt64 maximum_types;
    const UInt64 maximum_children;
    UInt64 retained_children = 0;
    std::unordered_map<const IDataType *, std::vector<DataTypePtr>> object_children;
};

DataTypePtr stableNormalizedPhysicalChild(const DataTypePtr & type, UInt32 ordinal, DictionaryPhysicalTopologyCache & topology_cache)
{
    if (!type)
        fail(Error::Code::PathMismatch, "dictionary attribute occurrence path reached a null physical type");

    if (type->hasCustomName())
    {
        if (isBool(type))
            fail(Error::Code::PathMismatch, "dictionary attribute occurrence path descends through Bool");
        if (const auto * simple = typeid_cast<const DataTypeCustomSimpleAggregateFunction *>(type->getCustomName()))
        {
            const auto & arguments = simple->getArgumentsDataTypes();
            if (ordinal < arguments.size())
                return arguments[ordinal];
            fail(Error::Code::PathMismatch, "dictionary attribute occurrence path exceeds SimpleAggregateFunction children");
        }
        if (const auto * nested = typeid_cast<const DataTypeNestedCustomName *>(type->getCustomName()))
        {
            const auto & elements = nested->getElements();
            if (ordinal < elements.size())
                return elements[ordinal];
            fail(Error::Code::PathMismatch, "dictionary attribute occurrence path exceeds Nested children");
        }
        fail(Error::Code::PathMismatch, "dictionary attribute occurrence path descends through an opaque custom physical type");
    }

    switch (type->getTypeId())
    {
        case TypeIndex::Array:
            if (ordinal == 0)
                return assert_cast<const DataTypeArray &>(*type).getNestedType();
            break;
        case TypeIndex::Tuple: {
            const auto & elements = assert_cast<const DataTypeTuple &>(*type).getElements();
            if (ordinal < elements.size())
                return elements[ordinal];
            break;
        }
        case TypeIndex::QBit:
            if (ordinal == 0)
                return assert_cast<const DataTypeQBit &>(*type).getElementType();
            break;
        case TypeIndex::Nullable:
            if (ordinal == 0)
                return assert_cast<const DataTypeNullable &>(*type).getNestedType();
            break;
        case TypeIndex::Function: {
            const auto & function = assert_cast<const DataTypeFunction &>(*type);
            const auto & arguments = function.getArgumentTypes();
            if (ordinal < arguments.size())
                return arguments[ordinal];
            if (ordinal == arguments.size() && function.getReturnType())
                return function.getReturnType();
            break;
        }
        case TypeIndex::LowCardinality:
            if (ordinal == 0)
                return assert_cast<const DataTypeLowCardinality &>(*type).getDictionaryType();
            break;
        case TypeIndex::Map: {
            const auto & map = assert_cast<const DataTypeMap &>(*type);
            if (ordinal == 0)
                return map.getKeyType();
            if (ordinal == 1)
                return map.getValueType();
            break;
        }
        case TypeIndex::AggregateFunction: {
            const auto & arguments = assert_cast<const DataTypeAggregateFunction &>(*type).getArgumentsDataTypes();
            if (ordinal < arguments.size())
                return arguments[ordinal];
            break;
        }
        case TypeIndex::Object: {
            const auto & children = topology_cache.getObjectChildren(type);
            if (ordinal < children.size())
                return children[ordinal];
            break;
        }
        case TypeIndex::Variant: {
            const auto & variants = assert_cast<const DataTypeVariant &>(*type).getVariants();
            if (ordinal < variants.size())
                return variants[ordinal];
            break;
        }
        default: break;
    }
    fail(Error::Code::PathMismatch, "dictionary attribute occurrence path contains an out-of-range physical child ordinal");
}

DataTypePtr
physicalTypeAtNormalizedPath(const DataTypePtr & root, std::span<const UInt64> path, DictionaryPhysicalTopologyCache & topology_cache)
{
    DataTypePtr current = root;
    for (const UInt64 child_ordinal : path)
    {
        if (!std::in_range<UInt32>(child_ordinal))
            fail(Error::Code::PathMismatch, "dictionary attribute occurrence path child ordinal exceeds UInt32");
        current = stableNormalizedPhysicalChild(current, static_cast<UInt32>(child_ordinal), topology_cache);
    }
    if (!current)
        fail(Error::Code::PathMismatch, "dictionary attribute occurrence path resolved to a null physical type");
    return current;
}

bool descriptorInstantiationLess(const InstantiatedTypeDescriptor::Ptr & lhs, const InstantiatedTypeDescriptor::Ptr & rhs)
{
    return lhs->getPersistedDescriptor().stableLess(rhs->getPersistedDescriptor());
}

bool descriptorCanonicalLess(const InstantiatedTypeDescriptor::Ptr & lhs, const InstantiatedTypeDescriptor::Ptr & rhs)
{
    if (descriptorInstantiationLess(lhs, rhs))
        return true;
    if (descriptorInstantiationLess(rhs, lhs))
        return false;
    return binaryLess(lhs->getPersistedDescriptor().getLastKnownQualifiedName(), rhs->getPersistedDescriptor().getLastKnownQualifiedName());
}

bool sameDescriptorInstantiation(const InstantiatedTypeDescriptor::Ptr & lhs, const InstantiatedTypeDescriptor::Ptr & rhs)
{
    return lhs->getPersistedDescriptor().hasSameInstantiation(rhs->getPersistedDescriptor());
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

void validateDefinitionHandles(std::vector<Definition::Ptr> & definitions, const UUID & database_uuid)
{
    for (const auto & definition : definitions)
    {
        if (!definition)
            fail(Error::Code::ConflictingDescriptor, "dictionary attribute binding contains a null definition handle");
        if (definition->getIdentity().database_uuid != database_uuid)
            fail(Error::Code::CrossDatabaseReference, "dictionary attribute definition belongs to another database");
    }

    std::sort(definitions.begin(), definitions.end(), definitionLess);
    size_t unique_definition_count = 0;
    for (size_t index = 0; index < definitions.size(); ++index)
    {
        if (unique_definition_count)
        {
            const auto & identity = definitions[index]->getIdentity();
            const auto & previous_identity = definitions[unique_definition_count - 1]->getIdentity();
            if (identity.database_uuid == previous_identity.database_uuid && identity.type_uuid == previous_identity.type_uuid
                && identity.revision != previous_identity.revision)
                fail(Error::Code::ConflictingDescriptor, "one dictionary attribute type identity appears at multiple definition revisions");
        }
        if (unique_definition_count && definitions[index]->getIdentity() == definitions[unique_definition_count - 1]->getIdentity())
        {
            if (!definitions[index]->hasSameCheckedSemantics(*definitions[unique_definition_count - 1]))
                fail(Error::Code::ConflictingDescriptor, "one dictionary attribute definition identity has conflicting checked semantics");
            continue;
        }
        if (unique_definition_count != index)
            definitions[unique_definition_count] = std::move(definitions[index]);
        ++unique_definition_count;
    }
    definitions.resize(unique_definition_count);
}

void validateBindingProvenance(
    const SchemaObjectID & dictionary,
    const std::vector<InstantiatedTypeDescriptor::Ptr> & descriptors,
    const std::vector<Definition::Ptr> & definitions,
    const DictionaryAttributeTypeBindingLimits & limits)
{
    try
    {
        validateStoredObjectTypeBindingProvenance(
            dictionary,
            descriptors,
            definitions,
            {
                .maximum_definitions = limits.maximum_distinct_definition_handles,
                .maximum_dependency_edges = limits.maximum_definition_dependencies,
            });
    }
    catch (const StoredObjectTypeBindingProvenanceError & error)
    {
        switch (error.code)
        {
            case StoredObjectTypeBindingProvenanceError::Code::InvalidConfiguration: fail(Error::Code::InvalidConfiguration, error.what());
            case StoredObjectTypeBindingProvenanceError::Code::CrossDatabaseReference:
                fail(Error::Code::CrossDatabaseReference, error.what());
            case StoredObjectTypeBindingProvenanceError::Code::LimitExceeded: fail(Error::Code::LimitExceeded, error.what());
            case StoredObjectTypeBindingProvenanceError::Code::InvalidInput:
            case StoredObjectTypeBindingProvenanceError::Code::DescriptorMismatch:
            case StoredObjectTypeBindingProvenanceError::Code::DefinitionClosureMismatch:
                fail(Error::Code::ConflictingDescriptor, error.what());
        }
    }
}

UInt64 findCanonicalDescriptorID(
    const std::vector<InstantiatedTypeDescriptor::Ptr> & descriptors, const InstantiatedTypeDescriptor::Ptr & descriptor)
{
    const auto found = std::lower_bound(descriptors.begin(), descriptors.end(), descriptor, descriptorInstantiationLess);
    if (found == descriptors.end() || !sameDescriptorInstantiation(*found, descriptor))
        fail(Error::Code::ConflictingDescriptor, "dictionary attribute occurrence descriptor was not interned canonically");
    return checkedSize(static_cast<size_t>(found - descriptors.begin()), "dictionary attribute descriptor ID exceeds UInt64");
}

void validateDescriptorFields(const PersistedTypeDescriptor & descriptor, const PersistedTypeReferencesLimits & limits)
{
    if (descriptor.getCanonicalArgumentsEncoding().size() > limits.maximum_canonical_arguments_bytes
        || descriptor.getCanonicalPhysicalType().size() > limits.maximum_canonical_physical_type_bytes
        || descriptor.getLastKnownQualifiedName().size() > limits.maximum_qualified_name_bytes)
        fail(Error::Code::LimitExceeded, "a dictionary attribute persisted descriptor field exceeds its sidecar limit");
}

UInt64 computeEncodedSidecarSize(
    const SchemaObjectID & dictionary,
    const std::vector<InstantiatedTypeDescriptor::Ptr> & descriptors,
    const std::vector<PendingOccurrence> & occurrences,
    const PersistedTypeReferencesLimits & limits)
{
    /// This mirrors the frozen writer exactly. The codec-size comparison after
    /// materialization turns a future wire change into a closed failure.
    EncodedSizeCounter counter(limits.maximum_sidecar_bytes);
    counter.add(sizeof(UInt16));
    counter.add(sizeof(UInt8) + 2 * sizeof(CanonicalUUID));
    counter.add(sizeof(UInt64));
    counter.add(sizeof(Digest));
    counter.add(sizeof(UInt16));

    counter.addVarUInt(checkedSize(descriptors.size(), "dictionary attribute descriptor count exceeds UInt64"));
    for (const auto & descriptor : descriptors)
    {
        const auto & persisted = descriptor->getPersistedDescriptor();
        if (persisted.getDefinitionIdentity().database_uuid != dictionary.database_uuid)
            fail(Error::Code::CrossDatabaseReference, "dictionary attribute descriptor belongs to another database");
        validateDescriptorFields(persisted, limits);
        addNestedFrame(
            counter,
            [&](EncodedSizeCounter & nested)
            {
                nested.add(sizeof(UInt16));
                nested.add(2 * sizeof(CanonicalUUID));
                nested.add(sizeof(UInt64));
                nested.add(sizeof(Digest));
                nested.addFrame(persisted.getCanonicalArgumentsEncoding());
                nested.addFrame(persisted.getCanonicalPhysicalType());
                nested.add(2 * sizeof(Digest));
                nested.add(4 * sizeof(UInt16));
                nested.add(sizeof(Digest));
                nested.add(sizeof(UInt8));
                nested.addFrame(persisted.getLastKnownQualifiedName());
            });
    }

    counter.addVarUInt(checkedSize(occurrences.size(), "dictionary attribute occurrence count exceeds UInt64"));
    for (const auto & occurrence : occurrences)
    {
        addNestedFrame(
            counter,
            [&](EncodedSizeCounter & nested)
            {
                nested.add(sizeof(UInt8));
                nested.addVarUInt(occurrence.path.object_ordinal);
                nested.addVarUInt(occurrence.path.occurrence_ordinal);
                nested.addVarUInt(
                    checkedSize(occurrence.path.type_child_ordinals.size(), "dictionary attribute occurrence-path depth exceeds UInt64"));
                for (const UInt64 child_ordinal : occurrence.path.type_child_ordinals)
                    nested.addVarUInt(child_ordinal);
            });
    }

    counter.addVarUInt(checkedSize(occurrences.size(), "dictionary attribute occurrence-use count exceeds UInt64"));
    for (size_t path_id = 0; path_id < occurrences.size(); ++path_id)
    {
        const UInt64 descriptor_id = findCanonicalDescriptorID(descriptors, occurrences[path_id].descriptor);
        addNestedFrame(
            counter,
            [&](EncodedSizeCounter & nested)
            {
                nested.addVarUInt(checkedSize(path_id, "dictionary attribute occurrence-path ID exceeds UInt64"));
                nested.addVarUInt(descriptor_id);
            });
    }
    counter.add(2 * sizeof(UInt16));
    return counter.getSize();
}

void validateLimits(const DictionaryAttributeTypeBindingLimits & limits)
{
    constexpr BoundObjectTypeReferencesLimits binding_maxima;
    const auto & persisted_maxima = binding_maxima.persisted;
    if (!limits.persisted.maximum_sidecar_bytes || !limits.persisted.maximum_descriptors || !limits.persisted.maximum_occurrence_paths
        || !limits.persisted.maximum_canonical_arguments_bytes || !limits.persisted.maximum_canonical_physical_type_bytes
        || !limits.persisted.maximum_qualified_name_bytes || !limits.persisted.maximum_text_bytes)
        fail(Error::Code::InvalidConfiguration, "dictionary attribute persisted limits contain a zero required bound");
    if (limits.persisted.maximum_sidecar_bytes > persisted_maxima.maximum_sidecar_bytes
        || limits.persisted.maximum_descriptors > persisted_maxima.maximum_descriptors
        || limits.persisted.maximum_occurrence_paths > persisted_maxima.maximum_occurrence_paths
        || limits.persisted.maximum_path_depth > persisted_maxima.maximum_path_depth
        || limits.persisted.maximum_canonical_arguments_bytes > persisted_maxima.maximum_canonical_arguments_bytes
        || limits.persisted.maximum_canonical_physical_type_bytes > persisted_maxima.maximum_canonical_physical_type_bytes
        || limits.persisted.maximum_qualified_name_bytes > persisted_maxima.maximum_qualified_name_bytes
        || limits.persisted.maximum_text_bytes > persisted_maxima.maximum_text_bytes)
        fail(Error::Code::InvalidConfiguration, "dictionary attribute persisted limits exceed the implementation maxima");

    constexpr DictionaryAttributeTypeBindingLimits implementation_maxima;
    if (!limits.maximum_attributes || !limits.maximum_total_attribute_name_bytes || !limits.maximum_bound_nodes
        || !limits.maximum_descriptor_occurrences || !limits.maximum_definition_handles || !limits.maximum_distinct_definition_handles
        || !limits.maximum_definition_dependencies || !limits.maximum_retained_path_components
        || !limits.maximum_single_runtime_owner_key_bytes || !limits.maximum_retained_runtime_owner_key_bytes)
        fail(Error::Code::InvalidConfiguration, "dictionary attribute binding limits contain a zero required bound");
    if (limits.maximum_attributes > implementation_maxima.maximum_attributes
        || limits.maximum_total_attribute_name_bytes > implementation_maxima.maximum_total_attribute_name_bytes
        || limits.maximum_bound_nodes > implementation_maxima.maximum_bound_nodes
        || limits.maximum_descriptor_occurrences > implementation_maxima.maximum_descriptor_occurrences
        || limits.maximum_definition_handles > implementation_maxima.maximum_definition_handles
        || limits.maximum_distinct_definition_handles > implementation_maxima.maximum_distinct_definition_handles
        || limits.maximum_definition_dependencies > implementation_maxima.maximum_definition_dependencies
        || limits.maximum_retained_path_components > implementation_maxima.maximum_retained_path_components
        || limits.maximum_single_runtime_owner_key_bytes > implementation_maxima.maximum_single_runtime_owner_key_bytes
        || limits.maximum_retained_runtime_owner_key_bytes > implementation_maxima.maximum_retained_runtime_owner_key_bytes
        || limits.maximum_single_runtime_owner_key_bytes > limits.maximum_retained_runtime_owner_key_bytes)
        fail(Error::Code::InvalidConfiguration, "dictionary attribute binding limits exceed the implementation maxima");
    if (limits.maximum_distinct_definition_handles > binding_maxima.specializer.maximum_definition_handles)
        fail(Error::Code::InvalidConfiguration, "dictionary attribute definition-handle limit exceeds the load-time binding maximum");
}

std::vector<DataTypePtr>
validateDictionaryPhysicalAttributes(const NamesAndTypesList & physical_attributes, const DictionaryAttributeTypeBindingLimits & limits)
{
    if (physical_attributes.empty())
        fail(Error::Code::InvalidAttribute, "dictionary physical attribute schema is empty");
    if (checkedSize(physical_attributes.size(), "dictionary physical attribute count exceeds UInt64") > limits.maximum_attributes)
        fail(Error::Code::LimitExceeded, "dictionary physical attribute schema exceeds its attribute limit");

    std::vector<DataTypePtr> attribute_types;
    attribute_types.reserve(physical_attributes.size());
    std::set<std::string_view> attribute_names;
    UInt64 total_attribute_name_bytes = 0;
    for (const auto & attribute : physical_attributes)
    {
        if (attribute.name.empty() || attribute.name.find('\0') != String::npos || !attribute.type)
            fail(Error::Code::InvalidAttribute, "dictionary physical attribute schema contains an invalid attribute");
        checkedAdd(
            total_attribute_name_bytes,
            checkedSize(attribute.name.size(), "dictionary physical attribute name size exceeds UInt64"),
            limits.maximum_total_attribute_name_bytes,
            "dictionary physical attribute names exceed their retained byte limit");
        if (!attribute_names.insert(attribute.name).second)
            fail(Error::Code::InvalidAttribute, "dictionary physical attribute schema contains a duplicate attribute name");
        attribute_types.push_back(attribute.type);
    }
    return attribute_types;
}

Digest computePhysicalSchemaFingerprint(const NamesAndTypesList & attributes)
{
    CanonicalHasher hasher(dictionary_attribute_physical_schema_fingerprint_domain);
    updateVarUInt(hasher, checkedSize(attributes.size(), "dictionary attribute count exceeds UInt64"));
    for (const auto & attribute : attributes)
    {
        updateFrame(hasher, attribute.name);
        hasher.update(physicalTypeFingerprint(attribute.type));
    }
    return hasher.finalize();
}

}

DictionaryAttributeTypeBindingError::DictionaryAttributeTypeBindingError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

Digest computeDictionaryAttributePhysicalSchemaFingerprint(
    const NamesAndTypesList & physical_attributes, const DictionaryAttributeTypeBindingLimits & limits)
{
    validateLimits(limits);
    static_cast<void>(validateDictionaryPhysicalAttributes(physical_attributes, limits));
    return computePhysicalSchemaFingerprint(physical_attributes);
}

PreparedDictionaryAttributeTypeBindings prepareDictionaryAttributeTypeBindings(
    const SchemaObjectID & dictionary,
    UInt64 object_schema_revision,
    std::span<const DictionaryAttributeTypeBindingInput> attributes,
    const DictionaryAttributeTypeBindingLimits & limits)
{
    validateLimits(limits);
    if (!dictionary.isValid() || dictionary.kind != SchemaObjectKind::Dictionary || !object_schema_revision)
        fail(Error::Code::InvalidObject, "dictionary attribute binding object identity or schema revision is invalid");
    if (attributes.empty())
        fail(Error::Code::InvalidAttribute, "dictionary attribute binding has no attributes");
    if (checkedSize(attributes.size(), "dictionary attribute count exceeds UInt64") > limits.maximum_attributes)
        fail(Error::Code::LimitExceeded, "dictionary attribute binding exceeds its attribute limit");

    PreparedDictionaryAttributeTypeBindings result;
    std::set<std::string_view> attribute_names;
    UInt64 total_attribute_name_bytes = 0;
    UInt64 total_bound_nodes = 0;
    UInt64 total_descriptor_occurrences = 0;
    UInt64 total_retained_path_components = 0;
    UInt64 total_runtime_owner_key_bytes = 0;
    DictionaryPhysicalTopologyCache topology_cache(limits.maximum_bound_nodes, limits.maximum_retained_path_components);
    std::vector<InstantiatedTypeDescriptor::Ptr> descriptor_handles;
    std::vector<Definition::Ptr> definition_handles;
    std::vector<PendingOccurrence> occurrences;

    for (size_t attribute_ordinal = 0; attribute_ordinal < attributes.size(); ++attribute_ordinal)
    {
        const auto & attribute = attributes[attribute_ordinal];
        const auto & physical_type = attribute.attribute_type.getPhysicalType();
        if (attribute.attribute_name.empty() || attribute.attribute_name.find('\0') != String::npos || !physical_type)
            fail(Error::Code::InvalidAttribute, "dictionary attribute binding contains an invalid attribute");
        checkedAdd(
            total_attribute_name_bytes,
            checkedSize(attribute.attribute_name.size(), "dictionary attribute name size exceeds UInt64"),
            limits.maximum_total_attribute_name_bytes,
            "dictionary attribute names exceed their retained byte limit");
        if (!attribute_names.insert(attribute.attribute_name).second)
            fail(Error::Code::InvalidAttribute, "dictionary attribute binding contains a duplicate attribute name");
        result.physical_attributes.emplace_back(attribute.attribute_name, physical_type);

        const auto & logical_tree = attribute.attribute_type.getLogicalTree();
        if (!logical_tree)
            continue;
        if (!logical_tree->getPhysicalType()->equals(*physical_type))
            fail(Error::Code::ConflictingDescriptor, "bound dictionary-attribute root differs from its physical result");

        checkedAdd(
            total_bound_nodes,
            logical_tree->getNodeCount(),
            limits.maximum_bound_nodes,
            "dictionary attribute bindings exceed their aggregate node limit");
        checkedAdd(
            total_descriptor_occurrences,
            logical_tree->getOccurrenceCount(),
            limits.maximum_descriptor_occurrences,
            "dictionary attribute bindings exceed their raw descriptor-occurrence limit");
        if (total_descriptor_occurrences > limits.persisted.maximum_occurrence_paths)
            fail(Error::Code::LimitExceeded, "dictionary attribute bindings exceed the persisted occurrence limit");

        const auto & tree_descriptors = logical_tree->getDescriptors();
        const UInt64 retained_descriptor_handles
            = checkedSize(descriptor_handles.size(), "dictionary attribute retained descriptor count exceeds UInt64");
        if (retained_descriptor_handles > limits.maximum_descriptor_occurrences
            || checkedSize(tree_descriptors.size(), "dictionary attribute descriptor count exceeds UInt64")
                > limits.maximum_descriptor_occurrences - retained_descriptor_handles)
            fail(Error::Code::LimitExceeded, "dictionary attribute bindings exceed their raw descriptor work limit");
        descriptor_handles.insert(descriptor_handles.end(), tree_descriptors.begin(), tree_descriptors.end());

        const auto & tree_definitions = logical_tree->getDefinitionHandles();
        const UInt64 retained_definition_handles
            = checkedSize(definition_handles.size(), "dictionary attribute retained definition count exceeds UInt64");
        if (retained_definition_handles > limits.maximum_definition_handles
            || checkedSize(tree_definitions.size(), "dictionary attribute definition count exceeds UInt64")
                > limits.maximum_definition_handles - retained_definition_handles)
            fail(Error::Code::LimitExceeded, "dictionary attribute bindings exceed their raw definition-handle work limit");
        definition_handles.insert(definition_handles.end(), tree_definitions.begin(), tree_definitions.end());

        for (UInt64 node_index = 0; node_index < logical_tree->getNodeCount(); ++node_index)
        {
            if (!std::in_range<BoundDeclaredTypeNodeID>(node_index))
                fail(Error::Code::LimitExceeded, "dictionary attribute bound node index exceeds UInt32");
            const auto node_id = static_cast<BoundDeclaredTypeNodeID>(node_index);
            const auto descriptor_indices = logical_tree->getDescriptorIndices(node_id);
            if (descriptor_indices.empty())
                continue;

            const UInt64 owner_key_bytes
                = checkedSize(attribute.attribute_name.size(), "dictionary attribute runtime owner-key size exceeds UInt64");
            if (owner_key_bytes > limits.maximum_single_runtime_owner_key_bytes)
                fail(Error::Code::LimitExceeded, "dictionary attribute runtime owner key exceeds its single-key byte limit");
            checkedAdd(
                total_runtime_owner_key_bytes,
                checkedMultiply(
                    owner_key_bytes,
                    checkedSize(descriptor_indices.size(), "dictionary attribute descriptor count exceeds UInt64"),
                    "dictionary attribute runtime owner-key bytes overflow UInt64"),
                limits.maximum_retained_runtime_owner_key_bytes,
                "dictionary attribute runtime owner keys exceed their retained byte limit");

            auto type_child_ordinals = nodePath(*logical_tree, node_id);
            if (type_child_ordinals.size() > limits.persisted.maximum_path_depth)
                fail(Error::Code::LimitExceeded, "dictionary attribute occurrence path exceeds its depth limit");
            checkedAdd(
                total_retained_path_components,
                checkedMultiply(
                    checkedSize(type_child_ordinals.size(), "dictionary attribute occurrence-path depth exceeds UInt64"),
                    checkedSize(descriptor_indices.size(), "dictionary attribute descriptor count exceeds UInt64"),
                    "dictionary attribute occurrence-path component count overflows UInt64"),
                limits.maximum_retained_path_components,
                "dictionary attribute occurrence paths exceed their retained component limit");

            DataTypePtr normalized_physical_type;
            try
            {
                normalized_physical_type = physicalTypeAtNormalizedPath(physical_type, type_child_ordinals, topology_cache);
            }
            catch (const Error & error)
            {
                if (error.code == Error::Code::PathMismatch)
                    fail(Error::Code::ConflictingDescriptor, error.what());
                throw;
            }
            if (!normalized_physical_type->equals(*logical_tree->getNode(node_id).getPhysicalType()))
                fail(Error::Code::ConflictingDescriptor, "bound dictionary-attribute node differs from its normalized physical subtree");

            for (size_t occurrence_ordinal = 0; occurrence_ordinal < descriptor_indices.size(); ++occurrence_ordinal)
            {
                const UInt32 descriptor_index = descriptor_indices[occurrence_ordinal];
                if (descriptor_index >= tree_descriptors.size() || !tree_descriptors[descriptor_index])
                    fail(Error::Code::ConflictingDescriptor, "bound dictionary-attribute occurrence references an absent descriptor");
                occurrences.push_back({
                    .path = {
                        .section = PersistedTypePathSection::DictionaryAttribute,
                        .site = PersistedTypeOccurrenceSite::Declaration,
                        .object_ordinal = static_cast<UInt64>(attribute_ordinal),
                        .occurrence_ordinal = static_cast<UInt64>(occurrence_ordinal),
                        .type_child_ordinals = type_child_ordinals,
                    },
                    .descriptor = tree_descriptors[descriptor_index],
                    .physical_type = normalized_physical_type,
                    .runtime_owner_key = attribute.attribute_name,
                });
            }
        }
    }

    result.physical_schema_fingerprint = computePhysicalSchemaFingerprint(result.physical_attributes);
    if (occurrences.empty())
    {
        if (!descriptor_handles.empty() || !definition_handles.empty())
            fail(Error::Code::ConflictingDescriptor, "logical dictionary-attribute descriptors have no occurrences");
        return result;
    }
    if (occurrences.size() != total_descriptor_occurrences)
        fail(Error::Code::ConflictingDescriptor, "bound dictionary-attribute occurrence count is inconsistent");

    validateDefinitionHandles(definition_handles, dictionary.database_uuid);
    if (checkedSize(definition_handles.size(), "dictionary attribute distinct definition count exceeds UInt64")
        > limits.maximum_distinct_definition_handles)
        fail(Error::Code::LimitExceeded, "dictionary attribute binding exceeds its distinct definition-handle limit");

    std::sort(descriptor_handles.begin(), descriptor_handles.end(), descriptorCanonicalLess);
    size_t unique_descriptor_count = 0;
    for (size_t index = 0; index < descriptor_handles.size(); ++index)
    {
        if (!descriptor_handles[index])
            fail(Error::Code::ConflictingDescriptor, "dictionary attribute binding contains a null descriptor handle");
        if (unique_descriptor_count
            && sameDescriptorInstantiation(descriptor_handles[index], descriptor_handles[unique_descriptor_count - 1]))
            continue;
        if (unique_descriptor_count != index)
            descriptor_handles[unique_descriptor_count] = std::move(descriptor_handles[index]);
        ++unique_descriptor_count;
        if (unique_descriptor_count > limits.persisted.maximum_descriptors)
            fail(Error::Code::LimitExceeded, "dictionary attribute binding exceeds its persisted descriptor limit");
    }
    descriptor_handles.resize(unique_descriptor_count);

    validateBindingProvenance(dictionary, descriptor_handles, definition_handles, limits);

    std::sort(
        occurrences.begin(),
        occurrences.end(),
        [](const PendingOccurrence & lhs, const PendingOccurrence & rhs) { return pathLess(lhs.path, rhs.path); });
    for (size_t index = 1; index < occurrences.size(); ++index)
    {
        if (!pathLess(occurrences[index - 1].path, occurrences[index].path))
            fail(Error::Code::ConflictingDescriptor, "dictionary attribute occurrence paths are not unique");
    }

    const UInt64 expected_sidecar_size = computeEncodedSidecarSize(dictionary, descriptor_handles, occurrences, limits.persisted);

    PersistedTypeReferences references;
    references.object = dictionary;
    references.object_schema_revision = object_schema_revision;
    references.physical_schema_fingerprint = result.physical_schema_fingerprint;
    references.descriptors.reserve(descriptor_handles.size());
    for (const auto & descriptor : descriptor_handles)
        references.descriptors.push_back(descriptor->getPersistedDescriptor());

    BoundObjectPhysicalSchema physical_schema{
        .object = dictionary,
        .object_schema_revision = object_schema_revision,
        .physical_schema_fingerprint = result.physical_schema_fingerprint,
        .occurrences = {},
    };
    references.occurrence_paths.reserve(occurrences.size());
    references.uses.reserve(occurrences.size());
    physical_schema.occurrences.reserve(occurrences.size());
    for (size_t path_id = 0; path_id < occurrences.size(); ++path_id)
    {
        auto & occurrence = occurrences[path_id];
        const UInt64 descriptor_id = findCanonicalDescriptorID(descriptor_handles, occurrence.descriptor);
        references.occurrence_paths.push_back(occurrence.path);
        references.uses.push_back({.path_id = static_cast<UInt64>(path_id), .descriptor_id = descriptor_id});
        physical_schema.occurrences.push_back({
            .path = std::move(occurrence.path),
            .physical_type = std::move(occurrence.physical_type),
            .runtime_owner_key = std::move(occurrence.runtime_owner_key),
            .selected_semantic_capabilities = occurrence.descriptor->getPersistedDescriptor().getSemanticCapabilities(),
        });
    }

    std::set<SchemaObjectID> dependency_objects;
    for (const auto & descriptor : references.descriptors)
    {
        const auto & identity = descriptor.getDefinitionIdentity();
        dependency_objects.insert({
            .kind = SchemaObjectKind::TypeDefinition,
            .database_uuid = identity.database_uuid,
            .object_uuid = identity.type_uuid,
        });
    }
    result.dependency_edges.reserve(dependency_objects.size());
    for (const auto & dependency : dependency_objects)
    {
        result.dependency_edges.push_back({
            .dependent = dictionary,
            .dependency = dependency,
            .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
        });
    }

    String encoded_sidecar;
    try
    {
        encoded_sidecar = encodePersistedTypeReferences(references, limits.persisted);
    }
    catch (const PersistedTypeReferencesError & error)
    {
        if (error.code == PersistedTypeReferencesError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, error.what());
        fail(Error::Code::ConflictingDescriptor, error.what());
    }
    if (checkedSize(encoded_sidecar.size(), "dictionary attribute persisted sidecar size exceeds UInt64") != expected_sidecar_size)
        fail(Error::Code::ConflictingDescriptor, "dictionary attribute persisted sidecar size preflight disagrees with its codec");

    result.sidecar_expectation = SidecarExpectationRecord{
        .object = dictionary,
        .object_schema_revision = object_schema_revision,
        .sidecar_hash = computePersistedTypeReferencesSidecarHash(references, limits.persisted),
        .physical_schema_fingerprint = result.physical_schema_fingerprint,
    };
    result.persisted_references = std::move(references);
    result.bound_physical_schema = std::move(physical_schema);
    return result;
}

BoundObjectPhysicalSchema reconstructDictionaryAttributePhysicalSchema(
    const SchemaObjectID & expected_dictionary,
    UInt64 expected_object_schema_revision,
    const NamesAndTypesList & physical_attributes,
    const PersistedTypeReferences & persisted_references,
    const DictionaryAttributeTypeBindingLimits & limits)
{
    validateLimits(limits);
    if (!expected_dictionary.isValid() || expected_dictionary.kind != SchemaObjectKind::Dictionary || !expected_object_schema_revision)
        fail(Error::Code::InvalidObject, "dictionary physical-schema identity or revision is invalid");
    auto attribute_types = validateDictionaryPhysicalAttributes(physical_attributes, limits);
    const UInt64 attribute_count = checkedSize(attribute_types.size(), "dictionary physical attribute count exceeds UInt64");
    std::vector<std::string_view> attribute_names;
    attribute_names.reserve(physical_attributes.size());
    for (const auto & attribute : physical_attributes)
        attribute_names.push_back(attribute.name);

    try
    {
        static_cast<void>(encodePersistedTypeReferences(persisted_references, limits.persisted));
    }
    catch (const PersistedTypeReferencesError & error)
    {
        if (error.code == PersistedTypeReferencesError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, error.what());
        fail(Error::Code::SidecarMismatch, error.what());
    }

    if (persisted_references.object != expected_dictionary
        || persisted_references.object_schema_revision != expected_object_schema_revision)
        fail(Error::Code::SidecarMismatch, "dictionary physical-schema identity or revision differs from its sidecar");

    const Digest physical_schema_fingerprint = computePhysicalSchemaFingerprint(physical_attributes);
    if (physical_schema_fingerprint != persisted_references.physical_schema_fingerprint)
        fail(Error::Code::PhysicalSchemaMismatch, "dictionary physical attribute schema fingerprint differs from its sidecar");

    const UInt64 occurrence_count
        = checkedSize(persisted_references.occurrence_paths.size(), "dictionary physical occurrence count exceeds UInt64");
    if (occurrence_count > limits.maximum_descriptor_occurrences)
        fail(Error::Code::LimitExceeded, "dictionary physical attribute schema exceeds its descriptor-occurrence limit");

    BoundObjectPhysicalSchema result{
        .object = expected_dictionary,
        .object_schema_revision = expected_object_schema_revision,
        .physical_schema_fingerprint = physical_schema_fingerprint,
        .occurrences = {},
    };
    result.occurrences.reserve(persisted_references.occurrence_paths.size());
    std::vector<const PersistedTypeOccurrencePath *> paths_by_endpoint;
    paths_by_endpoint.reserve(persisted_references.occurrence_paths.size());
    DictionaryPhysicalTopologyCache topology_cache(limits.maximum_bound_nodes, limits.maximum_retained_path_components);
    UInt64 total_retained_path_components = 0;
    UInt64 total_runtime_owner_key_bytes = 0;
    for (size_t path_id = 0; path_id < persisted_references.occurrence_paths.size(); ++path_id)
    {
        const auto & path = persisted_references.occurrence_paths[path_id];
        if (path.section != PersistedTypePathSection::DictionaryAttribute || path.site != PersistedTypeOccurrenceSite::Declaration)
            fail(Error::Code::PathMismatch, "dictionary attribute occurrence path is not a declaration endpoint");
        if (path.object_ordinal >= attribute_count)
            fail(Error::Code::PathMismatch, "dictionary occurrence path is not an attribute-declaration endpoint");
        checkedAdd(
            total_retained_path_components,
            checkedSize(path.type_child_ordinals.size(), "dictionary occurrence-path depth exceeds UInt64"),
            limits.maximum_retained_path_components,
            "dictionary occurrence paths exceed their retained component limit");
        const auto owner_key = attribute_names[static_cast<size_t>(path.object_ordinal)];
        const UInt64 owner_key_bytes = checkedSize(owner_key.size(), "dictionary runtime owner-key size exceeds UInt64");
        if (owner_key_bytes > limits.maximum_single_runtime_owner_key_bytes)
            fail(Error::Code::LimitExceeded, "dictionary runtime owner key exceeds its single-key byte limit");
        checkedAdd(
            total_runtime_owner_key_bytes,
            owner_key_bytes,
            limits.maximum_retained_runtime_owner_key_bytes,
            "dictionary runtime owner keys exceed their retained byte limit");

        result.occurrences.push_back({
            .path = path,
            .physical_type = physicalTypeAtNormalizedPath(
                attribute_types[static_cast<size_t>(path.object_ordinal)], path.type_child_ordinals, topology_cache),
            .runtime_owner_key = String(owner_key),
            .selected_semantic_capabilities
            = persisted_references.descriptors[static_cast<size_t>(persisted_references.uses[path_id].descriptor_id)]
                  .getSemanticCapabilities(),
        });
        paths_by_endpoint.push_back(std::addressof(path));
    }

    std::sort(
        paths_by_endpoint.begin(),
        paths_by_endpoint.end(),
        [](const PersistedTypeOccurrencePath * lhs, const PersistedTypeOccurrencePath * rhs)
        {
            if (lhs->object_ordinal != rhs->object_ordinal)
                return lhs->object_ordinal < rhs->object_ordinal;
            if (lhs->type_child_ordinals != rhs->type_child_ordinals)
                return std::lexicographical_compare(
                    lhs->type_child_ordinals.begin(),
                    lhs->type_child_ordinals.end(),
                    rhs->type_child_ordinals.begin(),
                    rhs->type_child_ordinals.end());
            return lhs->occurrence_ordinal < rhs->occurrence_ordinal;
        });
    const PersistedTypeOccurrencePath * previous = nullptr;
    for (const auto * path : paths_by_endpoint)
    {
        const bool same_endpoint
            = previous && previous->object_ordinal == path->object_ordinal && previous->type_child_ordinals == path->type_child_ordinals;
        const UInt64 expected_ordinal = same_endpoint ? previous->occurrence_ordinal + 1 : 0;
        if (path->occurrence_ordinal != expected_ordinal)
            fail(Error::Code::PathMismatch, "dictionary occurrence ordinals are not consecutive at one physical attribute endpoint");
        previous = path;
    }

    return result;
}

}
