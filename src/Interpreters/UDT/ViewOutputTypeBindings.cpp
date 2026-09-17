#include <Interpreters/UDT/ViewOutputTypeBindings.h>

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

inline constexpr std::string_view view_mixed_physical_schema_fingerprint_domain = "ClickHouse UDT view physical endpoints V2";

using Error = ViewOutputTypeBindingError;

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
            fail(Error::Code::LimitExceeded, "view output persisted sidecar exceeds its byte limit");
        size += value;
    }

    void addVarUInt(UInt64 value) { add(varUIntSize(value)); }

    void addFrame(std::string_view value)
    {
        const UInt64 frame_size = checkedSize(value.size(), "view output persisted descriptor field exceeds UInt64");
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
    updateVarUInt(hasher, checkedSize(value.size(), "view output name size exceeds UInt64"));
    hasher.update(value);
}

void updateByte(CanonicalHasher & hasher, UInt8 value)
{
    const CanonicalByte byte = value;
    hasher.update(std::span<const CanonicalByte>(&byte, 1));
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
            fail(Error::Code::ConflictingDescriptor, "non-root bound view-output node has no parent");
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

class ViewPhysicalTopologyCache final
{
public:
    ViewPhysicalTopologyCache(UInt64 maximum_types_, UInt64 maximum_children_)
        : maximum_types(maximum_types_)
        , maximum_children(maximum_children_)
    {
    }

    const std::vector<DataTypePtr> & getObjectChildren(const DataTypePtr & type)
    {
        if (const auto found = object_children.find(type.get()); found != object_children.end())
            return found->second;
        if (checkedSize(object_children.size(), "view physical topology cache size exceeds UInt64") >= maximum_types)
            fail(Error::Code::LimitExceeded, "view physical topology cache exceeds its type limit");

        const auto & typed_paths = assert_cast<const DataTypeObject &>(*type).getTypedPaths();
        const UInt64 child_count = checkedSize(typed_paths.size(), "view Object child count exceeds UInt64");
        checkedAdd(retained_children, child_count, maximum_children, "view physical topology cache exceeds its retained child limit");
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

DataTypePtr stableNormalizedPhysicalChild(const DataTypePtr & type, UInt32 ordinal, ViewPhysicalTopologyCache & topology_cache)
{
    if (!type)
        fail(Error::Code::PathMismatch, "view output occurrence path reached a null physical type");

    if (type->hasCustomName())
    {
        if (isBool(type))
            fail(Error::Code::PathMismatch, "view output occurrence path descends through Bool");
        if (const auto * simple = typeid_cast<const DataTypeCustomSimpleAggregateFunction *>(type->getCustomName()))
        {
            const auto & arguments = simple->getArgumentsDataTypes();
            if (ordinal < arguments.size())
                return arguments[ordinal];
            fail(Error::Code::PathMismatch, "view output occurrence path exceeds SimpleAggregateFunction children");
        }
        if (const auto * nested = typeid_cast<const DataTypeNestedCustomName *>(type->getCustomName()))
        {
            const auto & elements = nested->getElements();
            if (ordinal < elements.size())
                return elements[ordinal];
            fail(Error::Code::PathMismatch, "view output occurrence path exceeds Nested children");
        }
        fail(Error::Code::PathMismatch, "view output occurrence path descends through an opaque custom physical type");
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
    fail(Error::Code::PathMismatch, "view output occurrence path contains an out-of-range physical child ordinal");
}

DataTypePtr physicalTypeAtNormalizedPath(const DataTypePtr & root, std::span<const UInt64> path, ViewPhysicalTopologyCache & topology_cache)
{
    DataTypePtr current = root;
    for (const UInt64 child_ordinal : path)
    {
        if (!std::in_range<UInt32>(child_ordinal))
            fail(Error::Code::PathMismatch, "view output occurrence path child ordinal exceeds UInt32");
        current = stableNormalizedPhysicalChild(current, static_cast<UInt32>(child_ordinal), topology_cache);
    }
    if (!current)
        fail(Error::Code::PathMismatch, "view output occurrence path resolved to a null physical type");
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
            fail(Error::Code::ConflictingDescriptor, "view output binding contains a null definition handle");
        if (definition->getIdentity().database_uuid != database_uuid)
            fail(Error::Code::CrossDatabaseReference, "view output definition belongs to another database");
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
                fail(Error::Code::ConflictingDescriptor, "one view output type identity appears at multiple definition revisions");
        }
        if (unique_definition_count && definitions[index]->getIdentity() == definitions[unique_definition_count - 1]->getIdentity())
        {
            if (!definitions[index]->hasSameCheckedSemantics(*definitions[unique_definition_count - 1]))
                fail(Error::Code::ConflictingDescriptor, "one view output definition identity has conflicting checked semantics");
            continue;
        }
        if (unique_definition_count != index)
            definitions[unique_definition_count] = std::move(definitions[index]);
        ++unique_definition_count;
    }
    definitions.resize(unique_definition_count);
}

void validateBindingProvenance(
    const SchemaObjectID & view,
    const std::vector<InstantiatedTypeDescriptor::Ptr> & descriptors,
    const std::vector<Definition::Ptr> & definitions,
    const ViewOutputTypeBindingLimits & limits)
{
    try
    {
        validateStoredObjectTypeBindingProvenance(
            view,
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
        fail(Error::Code::ConflictingDescriptor, "view output occurrence descriptor was not interned canonically");
    return checkedSize(static_cast<size_t>(found - descriptors.begin()), "view output descriptor ID exceeds UInt64");
}

void validateDescriptorFields(const PersistedTypeDescriptor & descriptor, const PersistedTypeReferencesLimits & limits)
{
    if (descriptor.getCanonicalArgumentsEncoding().size() > limits.maximum_canonical_arguments_bytes
        || descriptor.getCanonicalPhysicalType().size() > limits.maximum_canonical_physical_type_bytes
        || descriptor.getLastKnownQualifiedName().size() > limits.maximum_qualified_name_bytes)
        fail(Error::Code::LimitExceeded, "a view output persisted descriptor field exceeds its sidecar limit");
}

UInt64 computeEncodedSidecarSize(
    const SchemaObjectID & view,
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

    counter.addVarUInt(checkedSize(descriptors.size(), "view output descriptor count exceeds UInt64"));
    for (const auto & descriptor : descriptors)
    {
        const auto & persisted = descriptor->getPersistedDescriptor();
        if (persisted.getDefinitionIdentity().database_uuid != view.database_uuid)
            fail(Error::Code::CrossDatabaseReference, "view output descriptor belongs to another database");
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

    counter.addVarUInt(checkedSize(occurrences.size(), "view output occurrence count exceeds UInt64"));
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
                    checkedSize(occurrence.path.type_child_ordinals.size(), "view output occurrence-path depth exceeds UInt64"));
                for (const UInt64 child_ordinal : occurrence.path.type_child_ordinals)
                    nested.addVarUInt(child_ordinal);
            });
    }

    counter.addVarUInt(checkedSize(occurrences.size(), "view output occurrence-use count exceeds UInt64"));
    for (size_t path_id = 0; path_id < occurrences.size(); ++path_id)
    {
        const UInt64 descriptor_id = findCanonicalDescriptorID(descriptors, occurrences[path_id].descriptor);
        addNestedFrame(
            counter,
            [&](EncodedSizeCounter & nested)
            {
                nested.addVarUInt(checkedSize(path_id, "view output occurrence-path ID exceeds UInt64"));
                nested.addVarUInt(descriptor_id);
            });
    }
    counter.add(2 * sizeof(UInt16));
    return counter.getSize();
}

UInt64 computeEncodedSidecarSize(const PersistedTypeReferences & references, const PersistedTypeReferencesLimits & limits)
{
    EncodedSizeCounter counter(limits.maximum_sidecar_bytes);
    counter.add(sizeof(UInt16));
    counter.add(sizeof(UInt8) + 2 * sizeof(CanonicalUUID));
    counter.add(sizeof(UInt64));
    counter.add(sizeof(Digest));
    counter.add(sizeof(UInt16));

    counter.addVarUInt(checkedSize(references.descriptors.size(), "view descriptor count exceeds UInt64"));
    for (const auto & persisted : references.descriptors)
    {
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

    counter.addVarUInt(checkedSize(references.occurrence_paths.size(), "view occurrence count exceeds UInt64"));
    for (const auto & path : references.occurrence_paths)
    {
        addNestedFrame(
            counter,
            [&](EncodedSizeCounter & nested)
            {
                nested.add(sizeof(UInt8));
                if (references.path_dictionary_version == persisted_type_path_dictionary_version_v2)
                    nested.add(sizeof(UInt8));
                nested.addVarUInt(path.object_ordinal);
                nested.addVarUInt(path.occurrence_ordinal);
                nested.addVarUInt(checkedSize(path.type_child_ordinals.size(), "view occurrence-path depth exceeds UInt64"));
                for (const UInt64 child_ordinal : path.type_child_ordinals)
                    nested.addVarUInt(child_ordinal);
            });
    }

    counter.addVarUInt(checkedSize(references.uses.size(), "view occurrence-use count exceeds UInt64"));
    for (const auto & use : references.uses)
    {
        addNestedFrame(
            counter,
            [&](EncodedSizeCounter & nested)
            {
                nested.addVarUInt(use.path_id);
                nested.addVarUInt(use.descriptor_id);
            });
    }
    counter.add(2 * sizeof(UInt16));
    return counter.getSize();
}

void validateLimits(const ViewOutputTypeBindingLimits & limits)
{
    constexpr BoundObjectTypeReferencesLimits binding_maxima;
    const auto & persisted_maxima = binding_maxima.persisted;
    if (!limits.persisted.maximum_sidecar_bytes || !limits.persisted.maximum_descriptors || !limits.persisted.maximum_occurrence_paths
        || !limits.persisted.maximum_canonical_arguments_bytes || !limits.persisted.maximum_canonical_physical_type_bytes
        || !limits.persisted.maximum_qualified_name_bytes || !limits.persisted.maximum_text_bytes)
        fail(Error::Code::InvalidConfiguration, "view output persisted limits contain a zero required bound");
    if (limits.persisted.maximum_sidecar_bytes > persisted_maxima.maximum_sidecar_bytes
        || limits.persisted.maximum_descriptors > persisted_maxima.maximum_descriptors
        || limits.persisted.maximum_occurrence_paths > persisted_maxima.maximum_occurrence_paths
        || limits.persisted.maximum_path_depth > persisted_maxima.maximum_path_depth
        || limits.persisted.maximum_canonical_arguments_bytes > persisted_maxima.maximum_canonical_arguments_bytes
        || limits.persisted.maximum_canonical_physical_type_bytes > persisted_maxima.maximum_canonical_physical_type_bytes
        || limits.persisted.maximum_qualified_name_bytes > persisted_maxima.maximum_qualified_name_bytes
        || limits.persisted.maximum_text_bytes > persisted_maxima.maximum_text_bytes)
        fail(Error::Code::InvalidConfiguration, "view output persisted limits exceed the implementation maxima");

    constexpr ViewOutputTypeBindingLimits implementation_maxima;
    if (!limits.maximum_outputs || !limits.maximum_total_output_name_bytes || !limits.maximum_bound_nodes
        || !limits.maximum_descriptor_occurrences || !limits.maximum_definition_handles || !limits.maximum_distinct_definition_handles
        || !limits.maximum_definition_dependencies || !limits.maximum_retained_path_components
        || !limits.maximum_single_runtime_owner_key_bytes || !limits.maximum_retained_runtime_owner_key_bytes)
        fail(Error::Code::InvalidConfiguration, "view output binding limits contain a zero required bound");
    if (limits.maximum_outputs > implementation_maxima.maximum_outputs
        || limits.maximum_total_output_name_bytes > implementation_maxima.maximum_total_output_name_bytes
        || limits.maximum_bound_nodes > implementation_maxima.maximum_bound_nodes
        || limits.maximum_descriptor_occurrences > implementation_maxima.maximum_descriptor_occurrences
        || limits.maximum_definition_handles > implementation_maxima.maximum_definition_handles
        || limits.maximum_distinct_definition_handles > implementation_maxima.maximum_distinct_definition_handles
        || limits.maximum_definition_dependencies > implementation_maxima.maximum_definition_dependencies
        || limits.maximum_retained_path_components > implementation_maxima.maximum_retained_path_components
        || limits.maximum_single_runtime_owner_key_bytes > implementation_maxima.maximum_single_runtime_owner_key_bytes
        || limits.maximum_retained_runtime_owner_key_bytes > implementation_maxima.maximum_retained_runtime_owner_key_bytes
        || limits.maximum_single_runtime_owner_key_bytes > limits.maximum_retained_runtime_owner_key_bytes)
        fail(Error::Code::InvalidConfiguration, "view output binding limits exceed the implementation maxima");
    if (limits.maximum_distinct_definition_handles > binding_maxima.specializer.maximum_definition_handles)
        fail(Error::Code::InvalidConfiguration, "view output definition-handle limit exceeds the load-time binding maximum");
}

std::vector<DataTypePtr> validateViewPhysicalOutputs(const NamesAndTypesList & physical_outputs, const ViewOutputTypeBindingLimits & limits)
{
    if (physical_outputs.empty())
        fail(Error::Code::InvalidOutput, "view physical output schema is empty");
    if (checkedSize(physical_outputs.size(), "view physical output count exceeds UInt64") > limits.maximum_outputs)
        fail(Error::Code::LimitExceeded, "view physical output schema exceeds its output limit");

    std::vector<DataTypePtr> output_types;
    output_types.reserve(physical_outputs.size());
    std::set<std::string_view> output_names;
    UInt64 total_output_name_bytes = 0;
    for (const auto & output : physical_outputs)
    {
        if (output.name.empty() || output.name.find('\0') != String::npos || !output.type)
            fail(Error::Code::InvalidOutput, "view physical output schema contains an invalid output");
        checkedAdd(
            total_output_name_bytes,
            checkedSize(output.name.size(), "view physical output name size exceeds UInt64"),
            limits.maximum_total_output_name_bytes,
            "view physical output names exceed their retained byte limit");
        if (!output_names.insert(output.name).second)
            fail(Error::Code::InvalidOutput, "view physical output schema contains a duplicate output name");
        output_types.push_back(output.type);
    }
    return output_types;
}

Digest computePhysicalSchemaFingerprint(const NamesAndTypesList & outputs)
{
    CanonicalHasher hasher(view_output_physical_schema_fingerprint_domain);
    updateVarUInt(hasher, checkedSize(outputs.size(), "view output count exceeds UInt64"));
    for (const auto & output : outputs)
    {
        updateFrame(hasher, output.name);
        hasher.update(physicalTypeFingerprint(output.type));
    }
    return hasher.finalize();
}

bool auxiliaryEndpointLess(const ViewAuxiliaryPhysicalTypeBindingInput * lhs, const ViewAuxiliaryPhysicalTypeBindingInput * rhs) noexcept
{
    const auto lhs_site = static_cast<UInt8>(lhs->site);
    const auto rhs_site = static_cast<UInt8>(rhs->site);
    return lhs_site != rhs_site ? lhs_site < rhs_site : lhs->object_ordinal < rhs->object_ordinal;
}

std::vector<const ViewAuxiliaryPhysicalTypeBindingInput *> validateAuxiliaryPhysicalEndpoints(
    std::span<const ViewAuxiliaryPhysicalTypeBindingInput> endpoints, const ViewOutputTypeBindingLimits & limits)
{
    if (checkedSize(endpoints.size(), "view auxiliary endpoint count exceeds UInt64") > limits.maximum_outputs)
        fail(Error::Code::LimitExceeded, "view auxiliary endpoint table exceeds its endpoint limit");
    std::vector<const ViewAuxiliaryPhysicalTypeBindingInput *> sorted;
    sorted.reserve(endpoints.size());
    UInt64 retained_owner_key_bytes = 0;
    for (const auto & endpoint : endpoints)
    {
        if ((endpoint.site != PersistedTypeOccurrenceSite::StoredExpression && endpoint.site != PersistedTypeOccurrenceSite::SchemaString)
            || endpoint.runtime_owner_key.empty() || endpoint.runtime_owner_key.find('\0') != String::npos || !endpoint.physical_type)
            fail(Error::Code::InvalidOutput, "view auxiliary endpoint is invalid or uses a declaration site");
        const UInt64 owner_key_bytes = checkedSize(endpoint.runtime_owner_key.size(), "view auxiliary owner-key size exceeds UInt64");
        if (owner_key_bytes > limits.maximum_single_runtime_owner_key_bytes)
            fail(Error::Code::LimitExceeded, "view auxiliary runtime owner key exceeds its single-key byte limit");
        checkedAdd(
            retained_owner_key_bytes,
            owner_key_bytes,
            limits.maximum_retained_runtime_owner_key_bytes,
            "view auxiliary runtime owner keys exceed their retained byte limit");
        sorted.push_back(std::addressof(endpoint));
    }
    std::sort(sorted.begin(), sorted.end(), auxiliaryEndpointLess);
    for (size_t index = 1; index < sorted.size(); ++index)
    {
        if (sorted[index - 1]->site == sorted[index]->site && sorted[index - 1]->object_ordinal == sorted[index]->object_ordinal)
            fail(Error::Code::InvalidOutput, "view auxiliary endpoint keys are not unique");
    }
    return sorted;
}

Digest computeMixedPhysicalSchemaFingerprint(
    const NamesAndTypesList & outputs, std::span<const ViewAuxiliaryPhysicalTypeBindingInput * const> sorted_auxiliary_endpoints)
{
    CanonicalHasher hasher(view_mixed_physical_schema_fingerprint_domain);
    updateVarUInt(hasher, checkedSize(outputs.size(), "view output count exceeds UInt64"));
    for (const auto & output : outputs)
    {
        updateFrame(hasher, output.name);
        hasher.update(physicalTypeFingerprint(output.type));
    }
    updateVarUInt(hasher, checkedSize(sorted_auxiliary_endpoints.size(), "view auxiliary endpoint count exceeds UInt64"));
    for (const auto * endpoint : sorted_auxiliary_endpoints)
    {
        updateByte(hasher, static_cast<UInt8>(endpoint->site));
        updateVarUInt(hasher, endpoint->object_ordinal);
        updateFrame(hasher, endpoint->runtime_owner_key);
        hasher.update(physicalTypeFingerprint(endpoint->physical_type));
    }
    return hasher.finalize();
}

TypeDescriptorLimits makeSelectedOutputTreeLimits(const ViewOutputTypeBindingLimits & limits)
{
    TypeDescriptorLimits result;
    result.maximum_canonical_arguments_bytes = limits.persisted.maximum_canonical_arguments_bytes;
    result.maximum_canonical_physical_type_bytes = limits.persisted.maximum_canonical_physical_type_bytes;
    result.maximum_qualified_name_bytes = limits.persisted.maximum_qualified_name_bytes;
    result.maximum_nodes = limits.maximum_bound_nodes;
    result.maximum_edges = std::min(result.maximum_edges, limits.maximum_retained_path_components);
    result.maximum_path_depth = limits.persisted.maximum_path_depth;
    /// BoundDeclaredTypeTree uses one limit for occurrence descriptors and
    /// their transitive checked-definition closure.  A selected slice may
    /// retain fewer persisted descriptors than definitions, so preserve both
    /// public View bounds here (the implementation maxima are validated by
    /// validateLimits before this helper is reached).
    result.maximum_descriptors = std::max(limits.persisted.maximum_descriptors, limits.maximum_distinct_definition_handles);
    result.maximum_occurrences = limits.maximum_descriptor_occurrences;
    return result;
}

bool pathStartsWith(std::span<const UInt64> path, std::span<const UInt64> prefix) noexcept
{
    return path.size() >= prefix.size() && std::equal(prefix.begin(), prefix.end(), path.begin());
}

std::vector<UInt32> narrowSelectedPath(std::span<const UInt64> path)
{
    std::vector<UInt32> result;
    result.reserve(path.size());
    for (const UInt64 ordinal : path)
    {
        if (!std::in_range<UInt32>(ordinal))
            fail(Error::Code::PathMismatch, "selected-output type-child ordinal exceeds UInt32");
        result.push_back(static_cast<UInt32>(ordinal));
    }
    return result;
}

struct SelectedTreeOccurrence
{
    std::vector<UInt32> path;
    InstantiatedTypeDescriptor::Ptr descriptor;
    UInt32 logical_preorder = 0;
};

std::vector<Definition::Ptr> selectDefinitionClosure(
    std::span<const SelectedTreeOccurrence> occurrences,
    std::span<const Definition::Ptr> available,
    const ViewOutputTypeBindingLimits & limits)
{
    if (available.size() > limits.maximum_definition_handles)
        fail(Error::Code::LimitExceeded, "selected-output available definition closure exceeds its raw-handle limit");

    std::unordered_map<UUID, Definition::Ptr> by_type;
    by_type.reserve(available.size());
    for (const auto & definition : available)
    {
        if (!definition)
            fail(Error::Code::ConflictingDescriptor, "selected-output definition closure contains a null handle");
        const auto [found, inserted] = by_type.emplace(definition->getIdentity().type_uuid, definition);
        if (!inserted
            && (found->second->getIdentity() != definition->getIdentity() || !found->second->hasSameCheckedSemantics(*definition)))
            fail(Error::Code::ConflictingDescriptor, "selected-output definition closure contains conflicting revisions");
    }

    std::vector<Definition::Ptr> selected;
    selected.reserve(std::min(available.size(), static_cast<size_t>(limits.maximum_distinct_definition_handles)));
    std::unordered_map<UUID, UInt8> retained;
    retained.reserve(available.size());
    std::vector<Definition::Ptr> pending;
    pending.reserve(occurrences.size());
    for (const auto & occurrence : occurrences)
    {
        if (!occurrence.descriptor || !occurrence.descriptor->getDefinition())
            fail(Error::Code::ConflictingDescriptor, "selected-output occurrence has no checked definition");
        pending.push_back(occurrence.descriptor->getDefinition());
    }

    UInt64 dependency_edges = 0;
    while (!pending.empty())
    {
        auto definition = std::move(pending.back());
        pending.pop_back();
        const auto identity = definition->getIdentity();
        const auto available_definition = by_type.find(identity.type_uuid);
        if (available_definition == by_type.end() || available_definition->second->getIdentity() != identity
            || !available_definition->second->hasSameCheckedSemantics(*definition))
            fail(Error::Code::ConflictingDescriptor, "selected-output definition is absent from its exact retained closure");
        if (!retained.emplace(identity.type_uuid, 1).second)
            continue;
        if (selected.size() >= limits.maximum_distinct_definition_handles)
            fail(Error::Code::LimitExceeded, "selected-output definition closure exceeds its distinct-handle limit");
        selected.push_back(available_definition->second);

        const auto & dependencies = available_definition->second->getDependencies();
        checkedAdd(
            dependency_edges,
            checkedSize(dependencies.size(), "selected-output dependency count exceeds UInt64"),
            limits.maximum_definition_dependencies,
            "selected-output definition closure exceeds its dependency-edge limit");
        for (const auto & dependency : dependencies)
        {
            const auto dependency_definition = by_type.find(dependency.type_uuid);
            if (dependency_definition == by_type.end() || dependency_definition->second->getIdentity().revision != dependency.revision
                || dependency_definition->second->getDefinitionHash() != dependency.target_definition_hash)
                fail(Error::Code::ConflictingDescriptor, "selected-output definition closure omits an exact dependency");
            pending.push_back(dependency_definition->second);
        }
    }
    return selected;
}

BoundDeclaredTypeResult buildSelectedOutputTree(
    const DataTypePtr & physical_root,
    std::vector<SelectedTreeOccurrence> occurrences,
    std::vector<Definition::Ptr> definitions,
    const ViewOutputTypeBindingLimits & limits)
{
    if (!physical_root)
        fail(Error::Code::InvalidOutput, "selected output has no physical type");
    if (occurrences.empty())
        return BoundDeclaredTypeResult::physicalOnly(physical_root);

    std::set<std::vector<UInt32>> node_paths;
    node_paths.emplace();
    for (const auto & occurrence : occurrences)
    {
        if (!occurrence.descriptor || occurrence.path.size() > limits.persisted.maximum_path_depth)
            fail(Error::Code::ConflictingDescriptor, "selected output contains an invalid logical occurrence");
        for (size_t depth = 1; depth <= occurrence.path.size(); ++depth)
            node_paths.emplace(occurrence.path.begin(), occurrence.path.begin() + depth);
    }
    if (node_paths.size() > limits.maximum_bound_nodes)
        fail(Error::Code::LimitExceeded, "selected output bound tree exceeds its node limit");

    ViewPhysicalTopologyCache topology_cache(limits.maximum_bound_nodes, limits.maximum_retained_path_components);
    std::vector<BoundDeclaredTypeNodeInput> nodes;
    nodes.reserve(node_paths.size());
    for (const auto & path : node_paths)
    {
        std::vector<UInt64> wide_path(path.begin(), path.end());
        nodes.push_back({
            .type_child_ordinals = path,
            .physical_type = physicalTypeAtNormalizedPath(physical_root, wide_path, topology_cache),
        });
    }
    std::sort(
        occurrences.begin(),
        occurrences.end(),
        [](const auto & lhs, const auto & rhs)
        { return lhs.path != rhs.path ? lhs.path < rhs.path : lhs.logical_preorder < rhs.logical_preorder; });
    std::vector<BoundDeclaredTypeOccurrenceInput> tree_occurrences;
    tree_occurrences.reserve(occurrences.size());
    for (auto & occurrence : occurrences)
    {
        tree_occurrences.push_back({
            .type_child_ordinals = std::move(occurrence.path),
            .logical_descriptor = std::move(occurrence.descriptor),
            .logical_preorder = occurrence.logical_preorder,
        });
    }

    try
    {
        return BoundDeclaredTypeResult::withLogicalTree(
            BoundDeclaredTypeTree::build(
                std::move(nodes), std::move(tree_occurrences), std::move(definitions), makeSelectedOutputTreeLimits(limits)));
    }
    catch (const DescriptorError & error)
    {
        if (error.code == DescriptorError::Code::LimitExceeded)
            fail(Error::Code::LimitExceeded, error.what());
        fail(Error::Code::ConflictingDescriptor, error.what());
    }
}

BoundDeclaredTypeResult materializeSelectedOutput(const SelectedOutputTypeBinding & selected, const ViewOutputTypeBindingLimits & limits)
{
    if (!selected.isValid())
        fail(Error::Code::InvalidOutput, "analyzer selected-output classification is invalid");
    if (selected.isPhysicalOnly())
        return BoundDeclaredTypeResult::physicalOnly(selected.physical_type);

    std::vector<SelectedTreeOccurrence> occurrences;
    std::vector<Definition::Ptr> definitions;
    UInt32 logical_preorder = 0;
    if (selected.explicit_logical_tree)
    {
        const auto & tree = *selected.explicit_logical_tree;
        const auto selected_prefix = narrowSelectedPath(selected.explicit_type_child_prefix);
        const auto selected_root = tree.findNode(selected_prefix);
        if (!selected_root || !tree.getNode(*selected_root).getPhysicalType()
            || !tree.getNode(*selected_root).getPhysicalType()->equals(*selected.physical_type))
            fail(Error::Code::PathMismatch, "selected explicit proof root differs from its retained physical output");
        for (UInt64 node_index = 0; node_index < tree.getNodeCount(); ++node_index)
        {
            if (!std::in_range<BoundDeclaredTypeNodeID>(node_index))
                fail(Error::Code::LimitExceeded, "selected explicit tree node exceeds UInt32");
            const auto node_id = static_cast<BoundDeclaredTypeNodeID>(node_index);
            const auto path = nodePath(tree, node_id);
            if (!pathStartsWith(path, selected.explicit_type_child_prefix))
                continue;
            const std::vector<UInt64> relative(path.begin() + selected.explicit_type_child_prefix.size(), path.end());
            const auto relative_path = narrowSelectedPath(relative);
            const auto descriptor_indices = tree.getDescriptorIndices(node_id);
            const auto & descriptors = tree.getDescriptors();
            for (const UInt32 descriptor_index : descriptor_indices)
            {
                if (descriptor_index >= descriptors.size() || !descriptors[descriptor_index])
                    fail(Error::Code::ConflictingDescriptor, "selected explicit output lost a descriptor");
                if (logical_preorder == std::numeric_limits<UInt32>::max())
                    fail(Error::Code::LimitExceeded, "selected output logical preorder exceeds UInt32");
                occurrences.push_back({relative_path, descriptors[descriptor_index], logical_preorder++});
            }
        }
        definitions = selectDefinitionClosure(occurrences, tree.getDefinitionHandles(), limits);
    }
    else
    {
        const auto uses = selected.prebound_references->findRuntimeUsesByPrefix(
            selected.prebound_section, selected.prebound_runtime_owner_key, selected.prebound_type_child_prefix);
        if (uses.empty())
            fail(Error::Code::PathMismatch, "selected prebound proof has no occurrence under its exact owner/path prefix");
        const auto descriptors = selected.prebound_references->getDescriptors();
        occurrences.reserve(uses.size());
        for (const auto * use : uses)
        {
            if (!use || use->getDescriptorIndex() >= descriptors.size() || !descriptors[use->getDescriptorIndex()])
                fail(Error::Code::ConflictingDescriptor, "selected prebound output lost a descriptor");
            const auto & path = use->getPath().type_child_ordinals;
            if (!pathStartsWith(path, selected.prebound_type_child_prefix))
                fail(Error::Code::PathMismatch, "selected prebound output escaped its exact prefix");
            const std::vector<UInt64> relative(path.begin() + selected.prebound_type_child_prefix.size(), path.end());
            if (logical_preorder == std::numeric_limits<UInt32>::max())
                fail(Error::Code::LimitExceeded, "selected output logical preorder exceeds UInt32");
            const auto & descriptor = descriptors[use->getDescriptorIndex()];
            occurrences.push_back({narrowSelectedPath(relative), descriptor, logical_preorder++});
        }
        definitions = selectDefinitionClosure(occurrences, selected.prebound_references->getDefinitionHandles(), limits);
    }
    return buildSelectedOutputTree(selected.physical_type, std::move(occurrences), std::move(definitions), limits);
}
}

ViewOutputTypeBindingError::ViewOutputTypeBindingError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

Digest computeViewOutputPhysicalSchemaFingerprint(const NamesAndTypesList & physical_outputs, const ViewOutputTypeBindingLimits & limits)
{
    validateLimits(limits);
    static_cast<void>(validateViewPhysicalOutputs(physical_outputs, limits));
    return computePhysicalSchemaFingerprint(physical_outputs);
}

Digest computeViewMixedPhysicalSchemaFingerprint(
    const NamesAndTypesList & physical_outputs,
    std::span<const ViewAuxiliaryPhysicalTypeBindingInput> auxiliary_endpoints,
    const ViewOutputTypeBindingLimits & limits)
{
    validateLimits(limits);
    static_cast<void>(validateViewPhysicalOutputs(physical_outputs, limits));
    const auto sorted = validateAuxiliaryPhysicalEndpoints(auxiliary_endpoints, limits);
    return computeMixedPhysicalSchemaFingerprint(physical_outputs, sorted);
}

PreparedViewOutputTypeBindings prepareViewOutputTypeBindings(
    const SchemaObjectID & view,
    UInt64 object_schema_revision,
    std::span<const ViewOutputTypeBindingInput> outputs,
    const ViewOutputTypeBindingLimits & limits)
{
    validateLimits(limits);
    if (!view.isValid() || view.kind != SchemaObjectKind::View || !object_schema_revision)
        fail(Error::Code::InvalidObject, "view output binding object identity or schema revision is invalid");
    if (outputs.empty())
        fail(Error::Code::InvalidOutput, "view output binding has no outputs");
    if (checkedSize(outputs.size(), "view output count exceeds UInt64") > limits.maximum_outputs)
        fail(Error::Code::LimitExceeded, "view output binding exceeds its output limit");

    PreparedViewOutputTypeBindings result;
    std::set<std::string_view> output_names;
    UInt64 total_output_name_bytes = 0;
    UInt64 total_bound_nodes = 0;
    UInt64 total_descriptor_occurrences = 0;
    UInt64 total_retained_path_components = 0;
    UInt64 total_runtime_owner_key_bytes = 0;
    ViewPhysicalTopologyCache topology_cache(limits.maximum_bound_nodes, limits.maximum_retained_path_components);
    std::vector<InstantiatedTypeDescriptor::Ptr> descriptor_handles;
    std::vector<Definition::Ptr> definition_handles;
    std::vector<PendingOccurrence> occurrences;

    for (size_t output_ordinal = 0; output_ordinal < outputs.size(); ++output_ordinal)
    {
        const auto & output = outputs[output_ordinal];
        const auto & physical_type = output.output_type.getPhysicalType();
        if (output.output_name.empty() || output.output_name.find('\0') != String::npos || !physical_type)
            fail(Error::Code::InvalidOutput, "view output binding contains an invalid output");
        checkedAdd(
            total_output_name_bytes,
            checkedSize(output.output_name.size(), "view output name size exceeds UInt64"),
            limits.maximum_total_output_name_bytes,
            "view output names exceed their retained byte limit");
        if (!output_names.insert(output.output_name).second)
            fail(Error::Code::InvalidOutput, "view output binding contains a duplicate output name");
        result.physical_outputs.emplace_back(output.output_name, physical_type);

        const auto & logical_tree = output.output_type.getLogicalTree();
        if (!logical_tree)
            continue;
        if (!logical_tree->getPhysicalType()->equals(*physical_type))
            fail(Error::Code::ConflictingDescriptor, "bound view-output root differs from its physical result");

        checkedAdd(
            total_bound_nodes,
            logical_tree->getNodeCount(),
            limits.maximum_bound_nodes,
            "view output bindings exceed their aggregate node limit");
        checkedAdd(
            total_descriptor_occurrences,
            logical_tree->getOccurrenceCount(),
            limits.maximum_descriptor_occurrences,
            "view output bindings exceed their raw descriptor-occurrence limit");
        if (total_descriptor_occurrences > limits.persisted.maximum_occurrence_paths)
            fail(Error::Code::LimitExceeded, "view output bindings exceed the persisted occurrence limit");

        const auto & tree_descriptors = logical_tree->getDescriptors();
        const UInt64 retained_descriptor_handles
            = checkedSize(descriptor_handles.size(), "view output retained descriptor count exceeds UInt64");
        if (retained_descriptor_handles > limits.maximum_descriptor_occurrences
            || checkedSize(tree_descriptors.size(), "view output descriptor count exceeds UInt64")
                > limits.maximum_descriptor_occurrences - retained_descriptor_handles)
            fail(Error::Code::LimitExceeded, "view output bindings exceed their raw descriptor work limit");
        descriptor_handles.insert(descriptor_handles.end(), tree_descriptors.begin(), tree_descriptors.end());

        const auto & tree_definitions = logical_tree->getDefinitionHandles();
        const UInt64 retained_definition_handles
            = checkedSize(definition_handles.size(), "view output retained definition count exceeds UInt64");
        if (retained_definition_handles > limits.maximum_definition_handles
            || checkedSize(tree_definitions.size(), "view output definition count exceeds UInt64")
                > limits.maximum_definition_handles - retained_definition_handles)
            fail(Error::Code::LimitExceeded, "view output bindings exceed their raw definition-handle work limit");
        definition_handles.insert(definition_handles.end(), tree_definitions.begin(), tree_definitions.end());

        for (UInt64 node_index = 0; node_index < logical_tree->getNodeCount(); ++node_index)
        {
            if (!std::in_range<BoundDeclaredTypeNodeID>(node_index))
                fail(Error::Code::LimitExceeded, "view output bound node index exceeds UInt32");
            const auto node_id = static_cast<BoundDeclaredTypeNodeID>(node_index);
            const auto descriptor_indices = logical_tree->getDescriptorIndices(node_id);
            if (descriptor_indices.empty())
                continue;

            const UInt64 owner_key_bytes = checkedSize(output.output_name.size(), "view output runtime owner-key size exceeds UInt64");
            if (owner_key_bytes > limits.maximum_single_runtime_owner_key_bytes)
                fail(Error::Code::LimitExceeded, "view output runtime owner key exceeds its single-key byte limit");
            checkedAdd(
                total_runtime_owner_key_bytes,
                checkedMultiply(
                    owner_key_bytes,
                    checkedSize(descriptor_indices.size(), "view output descriptor count exceeds UInt64"),
                    "view output runtime owner-key bytes overflow UInt64"),
                limits.maximum_retained_runtime_owner_key_bytes,
                "view output runtime owner keys exceed their retained byte limit");

            auto type_child_ordinals = nodePath(*logical_tree, node_id);
            if (type_child_ordinals.size() > limits.persisted.maximum_path_depth)
                fail(Error::Code::LimitExceeded, "view output occurrence path exceeds its depth limit");
            checkedAdd(
                total_retained_path_components,
                checkedMultiply(
                    checkedSize(type_child_ordinals.size(), "view output occurrence-path depth exceeds UInt64"),
                    checkedSize(descriptor_indices.size(), "view output descriptor count exceeds UInt64"),
                    "view output occurrence-path component count overflows UInt64"),
                limits.maximum_retained_path_components,
                "view output occurrence paths exceed their retained component limit");

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
                fail(Error::Code::ConflictingDescriptor, "bound view-output node differs from its normalized physical subtree");

            for (size_t occurrence_ordinal = 0; occurrence_ordinal < descriptor_indices.size(); ++occurrence_ordinal)
            {
                const UInt32 descriptor_index = descriptor_indices[occurrence_ordinal];
                if (descriptor_index >= tree_descriptors.size() || !tree_descriptors[descriptor_index])
                    fail(Error::Code::ConflictingDescriptor, "bound view-output occurrence references an absent descriptor");
                occurrences.push_back({
                    .path = {
                        .section = PersistedTypePathSection::ViewExpression,
                        .site = PersistedTypeOccurrenceSite::Declaration,
                        .object_ordinal = static_cast<UInt64>(output_ordinal),
                        .occurrence_ordinal = static_cast<UInt64>(occurrence_ordinal),
                        .type_child_ordinals = type_child_ordinals,
                    },
                    .descriptor = tree_descriptors[descriptor_index],
                    .physical_type = normalized_physical_type,
                    .runtime_owner_key = output.output_name,
                });
            }
        }
    }

    result.physical_schema_fingerprint = computePhysicalSchemaFingerprint(result.physical_outputs);
    if (occurrences.empty())
    {
        if (!descriptor_handles.empty() || !definition_handles.empty())
            fail(Error::Code::ConflictingDescriptor, "logical view-output descriptors have no occurrences");
        return result;
    }
    if (occurrences.size() != total_descriptor_occurrences)
        fail(Error::Code::ConflictingDescriptor, "bound view-output occurrence count is inconsistent");

    validateDefinitionHandles(definition_handles, view.database_uuid);
    if (checkedSize(definition_handles.size(), "view output distinct definition count exceeds UInt64")
        > limits.maximum_distinct_definition_handles)
        fail(Error::Code::LimitExceeded, "view output binding exceeds its distinct definition-handle limit");

    std::sort(descriptor_handles.begin(), descriptor_handles.end(), descriptorCanonicalLess);
    size_t unique_descriptor_count = 0;
    for (size_t index = 0; index < descriptor_handles.size(); ++index)
    {
        if (!descriptor_handles[index])
            fail(Error::Code::ConflictingDescriptor, "view output binding contains a null descriptor handle");
        if (unique_descriptor_count
            && sameDescriptorInstantiation(descriptor_handles[index], descriptor_handles[unique_descriptor_count - 1]))
            continue;
        if (unique_descriptor_count != index)
            descriptor_handles[unique_descriptor_count] = std::move(descriptor_handles[index]);
        ++unique_descriptor_count;
        if (unique_descriptor_count > limits.persisted.maximum_descriptors)
            fail(Error::Code::LimitExceeded, "view output binding exceeds its persisted descriptor limit");
    }
    descriptor_handles.resize(unique_descriptor_count);

    validateBindingProvenance(view, descriptor_handles, definition_handles, limits);

    std::sort(
        occurrences.begin(),
        occurrences.end(),
        [](const PendingOccurrence & lhs, const PendingOccurrence & rhs) { return pathLess(lhs.path, rhs.path); });
    for (size_t index = 1; index < occurrences.size(); ++index)
    {
        if (!pathLess(occurrences[index - 1].path, occurrences[index].path))
            fail(Error::Code::ConflictingDescriptor, "view output occurrence paths are not unique");
    }

    const UInt64 expected_sidecar_size = computeEncodedSidecarSize(view, descriptor_handles, occurrences, limits.persisted);

    PersistedTypeReferences references;
    references.object = view;
    references.object_schema_revision = object_schema_revision;
    references.physical_schema_fingerprint = result.physical_schema_fingerprint;
    references.descriptors.reserve(descriptor_handles.size());
    for (const auto & descriptor : descriptor_handles)
        references.descriptors.push_back(descriptor->getPersistedDescriptor());

    BoundObjectPhysicalSchema physical_schema{
        .object = view,
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
            .dependent = view,
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
    if (checkedSize(encoded_sidecar.size(), "view output persisted sidecar size exceeds UInt64") != expected_sidecar_size)
        fail(Error::Code::ConflictingDescriptor, "view output persisted sidecar size preflight disagrees with its codec");

    result.sidecar_expectation = SidecarExpectationRecord{
        .object = view,
        .object_schema_revision = object_schema_revision,
        .sidecar_hash = computePersistedTypeReferencesSidecarHash(references, limits.persisted),
        .physical_schema_fingerprint = result.physical_schema_fingerprint,
    };
    result.persisted_references = std::move(references);
    result.bound_physical_schema = std::move(physical_schema);
    return result;
}

PreparedViewOutputTypeBindings prepareViewMixedTypeBindings(
    const SchemaObjectID & view,
    UInt64 object_schema_revision,
    std::span<const ViewOutputTypeBindingInput> outputs,
    std::span<const ViewAuxiliaryTypeBindingInput> auxiliary_endpoints,
    const ViewOutputTypeBindingLimits & limits)
{
    auto declaration_bindings = prepareViewOutputTypeBindings(view, object_schema_revision, outputs, limits);

    std::vector<ViewAuxiliaryPhysicalTypeBindingInput> physical_auxiliary_endpoints;
    physical_auxiliary_endpoints.reserve(auxiliary_endpoints.size());
    bool has_auxiliary_logical_occurrence = false;
    for (const auto & endpoint : auxiliary_endpoints)
    {
        const auto & physical_type = endpoint.endpoint_type.getPhysicalType();
        physical_auxiliary_endpoints.push_back({
            .site = endpoint.site,
            .object_ordinal = endpoint.object_ordinal,
            .runtime_owner_key = endpoint.runtime_owner_key,
            .physical_type = physical_type,
        });
        const auto & logical_tree = endpoint.endpoint_type.getLogicalTree();
        has_auxiliary_logical_occurrence = has_auxiliary_logical_occurrence || (logical_tree && logical_tree->getOccurrenceCount() != 0);
    }
    const auto sorted_physical_auxiliary = validateAuxiliaryPhysicalEndpoints(physical_auxiliary_endpoints, limits);
    if (!has_auxiliary_logical_occurrence)
        return declaration_bindings;

    UInt64 total_bound_nodes = 0;
    UInt64 total_descriptor_occurrences = 0;
    UInt64 total_retained_path_components = 0;
    UInt64 total_runtime_owner_key_bytes = 0;
    std::vector<InstantiatedTypeDescriptor::Ptr> descriptor_handles;
    std::vector<Definition::Ptr> definition_handles;
    const auto retain_tree = [&](const BoundDeclaredTypeTree::Ptr & tree)
    {
        if (!tree)
            return;
        checkedAdd(total_bound_nodes, tree->getNodeCount(), limits.maximum_bound_nodes, "mixed view bindings exceed their node limit");
        checkedAdd(
            total_descriptor_occurrences,
            tree->getOccurrenceCount(),
            limits.maximum_descriptor_occurrences,
            "mixed view bindings exceed their descriptor-occurrence limit");
        const auto & descriptors = tree->getDescriptors();
        if (descriptor_handles.size() > limits.maximum_descriptor_occurrences
            || descriptors.size() > limits.maximum_descriptor_occurrences - descriptor_handles.size())
            fail(Error::Code::LimitExceeded, "mixed view bindings exceed their raw descriptor-handle limit");
        descriptor_handles.insert(descriptor_handles.end(), descriptors.begin(), descriptors.end());
        const auto & definitions = tree->getDefinitionHandles();
        if (definition_handles.size() > limits.maximum_definition_handles
            || definitions.size() > limits.maximum_definition_handles - definition_handles.size())
            fail(Error::Code::LimitExceeded, "mixed view bindings exceed their raw definition-handle limit");
        definition_handles.insert(definition_handles.end(), definitions.begin(), definitions.end());
    };
    for (const auto & output : outputs)
        retain_tree(output.output_type.getLogicalTree());
    for (const auto & endpoint : auxiliary_endpoints)
        retain_tree(endpoint.endpoint_type.getLogicalTree());
    if (total_descriptor_occurrences > limits.persisted.maximum_occurrence_paths)
        fail(Error::Code::LimitExceeded, "mixed view bindings exceed their persisted occurrence limit");

    validateDefinitionHandles(definition_handles, view.database_uuid);
    if (checkedSize(definition_handles.size(), "mixed view definition count exceeds UInt64") > limits.maximum_distinct_definition_handles)
        fail(Error::Code::LimitExceeded, "mixed view bindings exceed their distinct definition-handle limit");
    std::sort(descriptor_handles.begin(), descriptor_handles.end(), descriptorCanonicalLess);
    size_t unique_descriptor_count = 0;
    for (size_t index = 0; index < descriptor_handles.size(); ++index)
    {
        if (!descriptor_handles[index])
            fail(Error::Code::ConflictingDescriptor, "mixed view binding contains a null descriptor handle");
        if (unique_descriptor_count
            && sameDescriptorInstantiation(descriptor_handles[index], descriptor_handles[unique_descriptor_count - 1]))
            continue;
        if (unique_descriptor_count != index)
            descriptor_handles[unique_descriptor_count] = std::move(descriptor_handles[index]);
        if (++unique_descriptor_count > limits.persisted.maximum_descriptors)
            fail(Error::Code::LimitExceeded, "mixed view binding exceeds its persisted descriptor limit");
    }
    descriptor_handles.resize(unique_descriptor_count);
    validateBindingProvenance(view, descriptor_handles, definition_handles, limits);

    const auto find_descriptor = [&](const PersistedTypeDescriptor & persisted)
    {
        const auto found = std::lower_bound(
            descriptor_handles.begin(),
            descriptor_handles.end(),
            persisted,
            [](const InstantiatedTypeDescriptor::Ptr & lhs, const PersistedTypeDescriptor & rhs)
            { return lhs->getPersistedDescriptor().stableLess(rhs); });
        if (found == descriptor_handles.end() || !(*found)->getPersistedDescriptor().hasSameInstantiation(persisted))
            fail(Error::Code::ConflictingDescriptor, "mixed view occurrence descriptor was not retained canonically");
        return *found;
    };

    std::vector<PendingOccurrence> occurrences;
    occurrences.reserve(static_cast<size_t>(total_descriptor_occurrences));
    if (declaration_bindings.persisted_references || declaration_bindings.bound_physical_schema)
    {
        if (!declaration_bindings.persisted_references || !declaration_bindings.bound_physical_schema
            || declaration_bindings.persisted_references->occurrence_paths.size()
                != declaration_bindings.bound_physical_schema->occurrences.size())
            fail(Error::Code::ConflictingDescriptor, "declaration binding package is not indivisible before mixed merge");
        const auto & references = *declaration_bindings.persisted_references;
        const auto & physical_schema = *declaration_bindings.bound_physical_schema;
        for (size_t path_id = 0; path_id < references.occurrence_paths.size(); ++path_id)
        {
            const auto & path = references.occurrence_paths[path_id];
            const auto & use = references.uses[path_id];
            const auto & physical_occurrence = physical_schema.occurrences[path_id];
            if (use.path_id != path_id || use.descriptor_id >= references.descriptors.size() || physical_occurrence.path != path)
                fail(Error::Code::ConflictingDescriptor, "declaration binding changed before mixed merge");
            checkedAdd(
                total_retained_path_components,
                checkedSize(path.type_child_ordinals.size(), "view declaration path depth exceeds UInt64"),
                limits.maximum_retained_path_components,
                "mixed view paths exceed their retained component limit");
            const UInt64 owner_key_bytes
                = checkedSize(physical_occurrence.runtime_owner_key.size(), "view declaration owner-key size exceeds UInt64");
            checkedAdd(
                total_runtime_owner_key_bytes,
                owner_key_bytes,
                limits.maximum_retained_runtime_owner_key_bytes,
                "mixed view runtime owner keys exceed their retained byte limit");
            occurrences.push_back({
                .path = path,
                .descriptor = find_descriptor(references.descriptors[static_cast<size_t>(use.descriptor_id)]),
                .physical_type = physical_occurrence.physical_type,
                .runtime_owner_key = physical_occurrence.runtime_owner_key,
            });
        }
    }

    ViewPhysicalTopologyCache topology_cache(limits.maximum_bound_nodes, limits.maximum_retained_path_components);
    for (const auto & endpoint : auxiliary_endpoints)
    {
        const auto & physical_type = endpoint.endpoint_type.getPhysicalType();
        const auto & logical_tree = endpoint.endpoint_type.getLogicalTree();
        if (!logical_tree)
            continue;
        if (!physical_type || !logical_tree->getPhysicalType()->equals(*physical_type))
            fail(Error::Code::ConflictingDescriptor, "mixed view endpoint differs from its bound physical root");
        const auto & tree_descriptors = logical_tree->getDescriptors();
        for (UInt64 node_index = 0; node_index < logical_tree->getNodeCount(); ++node_index)
        {
            if (!std::in_range<BoundDeclaredTypeNodeID>(node_index))
                fail(Error::Code::LimitExceeded, "mixed view bound node index exceeds UInt32");
            const auto node_id = static_cast<BoundDeclaredTypeNodeID>(node_index);
            const auto descriptor_indices = logical_tree->getDescriptorIndices(node_id);
            if (descriptor_indices.empty())
                continue;
            auto type_child_ordinals = nodePath(*logical_tree, node_id);
            if (type_child_ordinals.size() > limits.persisted.maximum_path_depth)
                fail(Error::Code::LimitExceeded, "mixed view occurrence path exceeds its depth limit");
            checkedAdd(
                total_retained_path_components,
                checkedMultiply(
                    checkedSize(type_child_ordinals.size(), "mixed view occurrence-path depth exceeds UInt64"),
                    checkedSize(descriptor_indices.size(), "mixed view occurrence count exceeds UInt64"),
                    "mixed view occurrence-path components overflow UInt64"),
                limits.maximum_retained_path_components,
                "mixed view paths exceed their retained component limit");
            const UInt64 owner_key_bytes
                = checkedSize(endpoint.runtime_owner_key.size(), "mixed view runtime owner-key size exceeds UInt64");
            checkedAdd(
                total_runtime_owner_key_bytes,
                checkedMultiply(
                    owner_key_bytes,
                    checkedSize(descriptor_indices.size(), "mixed view occurrence count exceeds UInt64"),
                    "mixed view runtime owner-key bytes overflow UInt64"),
                limits.maximum_retained_runtime_owner_key_bytes,
                "mixed view runtime owner keys exceed their retained byte limit");

            const auto normalized_physical_type = physicalTypeAtNormalizedPath(physical_type, type_child_ordinals, topology_cache);
            if (!normalized_physical_type->equals(*logical_tree->getNode(node_id).getPhysicalType()))
                fail(Error::Code::ConflictingDescriptor, "mixed view bound node differs from its physical subtree");
            for (size_t occurrence_ordinal = 0; occurrence_ordinal < descriptor_indices.size(); ++occurrence_ordinal)
            {
                const UInt32 descriptor_index = descriptor_indices[occurrence_ordinal];
                if (descriptor_index >= tree_descriptors.size() || !tree_descriptors[descriptor_index])
                    fail(Error::Code::ConflictingDescriptor, "mixed view occurrence references an absent descriptor");
                occurrences.push_back({
                    .path = {
                        .section = PersistedTypePathSection::ViewExpression,
                        .site = endpoint.site,
                        .object_ordinal = endpoint.object_ordinal,
                        .occurrence_ordinal = static_cast<UInt64>(occurrence_ordinal),
                        .type_child_ordinals = type_child_ordinals,
                    },
                    .descriptor = tree_descriptors[descriptor_index],
                    .physical_type = normalized_physical_type,
                    .runtime_owner_key = endpoint.runtime_owner_key,
                });
            }
        }
    }
    if (occurrences.size() != total_descriptor_occurrences)
        fail(Error::Code::ConflictingDescriptor, "mixed view occurrence count is inconsistent");

    std::sort(occurrences.begin(), occurrences.end(), [](const auto & lhs, const auto & rhs) { return pathLess(lhs.path, rhs.path); });
    for (size_t index = 1; index < occurrences.size(); ++index)
        if (!pathLess(occurrences[index - 1].path, occurrences[index].path))
            fail(Error::Code::ConflictingDescriptor, "mixed view occurrence paths are not unique");

    PreparedViewOutputTypeBindings result;
    result.physical_outputs = std::move(declaration_bindings.physical_outputs);
    result.physical_schema_fingerprint = computeMixedPhysicalSchemaFingerprint(result.physical_outputs, sorted_physical_auxiliary);

    PersistedTypeReferences references;
    references.format_version = persisted_type_references_format_version_v2;
    references.object = view;
    references.object_schema_revision = object_schema_revision;
    references.physical_schema_fingerprint = result.physical_schema_fingerprint;
    references.path_dictionary_version = persisted_type_path_dictionary_version_v2;
    references.descriptors.reserve(descriptor_handles.size());
    for (const auto & descriptor : descriptor_handles)
        references.descriptors.push_back(descriptor->getPersistedDescriptor());

    BoundObjectPhysicalSchema physical_schema{
        .object = view,
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
            .dependent = view,
            .dependency = dependency,
            .kind = SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition,
        });
    }

    const UInt64 expected_sidecar_size = computeEncodedSidecarSize(references, limits.persisted);
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
    if (checkedSize(encoded_sidecar.size(), "mixed view persisted sidecar size exceeds UInt64") != expected_sidecar_size)
        fail(Error::Code::ConflictingDescriptor, "mixed view sidecar size preflight disagrees with its codec");

    result.sidecar_expectation = SidecarExpectationRecord{
        .object = view,
        .object_schema_revision = object_schema_revision,
        .sidecar_hash = computePersistedTypeReferencesSidecarHash(references, limits.persisted),
        .physical_schema_fingerprint = result.physical_schema_fingerprint,
    };
    result.persisted_references = std::move(references);
    result.bound_physical_schema = std::move(physical_schema);
    return result;
}

PreparedViewOutputTypeBindings prepareViewSelectedOutputTypeBindings(
    const SchemaObjectID & view,
    UInt64 object_schema_revision,
    std::span<const SelectedOutputTypeBinding> selected_outputs,
    std::span<const ViewAuxiliaryTypeBindingInput> auxiliary_endpoints,
    const ViewOutputTypeBindingLimits & limits)
{
    validateLimits(limits);
    if (selected_outputs.size() > limits.maximum_outputs)
        fail(Error::Code::LimitExceeded, "selected View outputs exceed their output limit");
    std::vector<ViewOutputTypeBindingInput> outputs;
    outputs.reserve(selected_outputs.size());
    for (const auto & selected : selected_outputs)
    {
        outputs.push_back({
            .output_name = selected.output_name,
            .output_type = materializeSelectedOutput(selected, limits),
        });
    }
    return prepareViewMixedTypeBindings(view, object_schema_revision, outputs, auxiliary_endpoints, limits);
}

PreparedTableColumnTypeBindings prepareTableSelectedOutputTypeBindings(
    const SchemaObjectID & table,
    UInt64 object_schema_revision,
    std::span<const SelectedOutputTypeBinding> selected_outputs,
    const TableColumnTypeBindingLimits & limits)
{
    if (!table.isValid() || table.kind != SchemaObjectKind::Table || !object_schema_revision)
        throw TableColumnTypeBindingError(
            TableColumnTypeBindingError::Code::InvalidObject, "selected table binding object identity or schema revision is invalid");

    /// Validate the table adapter's complete public limit domain and ordered
    /// physical half before retaining any logical proof material.  The
    /// fingerprint is deliberately discarded here; the canonical binder
    /// recomputes it over the exact same selected types below.
    NamesAndTypesList selected_physical_columns;
    for (const auto & selected : selected_outputs)
        selected_physical_columns.emplace_back(selected.output_name, selected.physical_type);
    static_cast<void>(computeTableColumnPhysicalSchemaFingerprint(selected_physical_columns, limits));

    ViewOutputTypeBindingLimits materialization_limits;
    materialization_limits.persisted = limits.persisted;
    materialization_limits.maximum_outputs = limits.maximum_columns;
    materialization_limits.maximum_total_output_name_bytes = limits.maximum_total_column_name_bytes;
    materialization_limits.maximum_bound_nodes = limits.maximum_bound_nodes;
    materialization_limits.maximum_descriptor_occurrences = limits.maximum_descriptor_occurrences;
    materialization_limits.maximum_definition_handles = limits.maximum_definition_handles;
    materialization_limits.maximum_distinct_definition_handles = limits.maximum_distinct_definition_handles;
    materialization_limits.maximum_definition_dependencies
        = std::min(materialization_limits.maximum_definition_dependencies, limits.maximum_retained_path_components);
    materialization_limits.maximum_retained_path_components = limits.maximum_retained_path_components;
    materialization_limits.maximum_single_runtime_owner_key_bytes = limits.maximum_single_runtime_owner_key_bytes;
    materialization_limits.maximum_retained_runtime_owner_key_bytes = limits.maximum_retained_runtime_owner_key_bytes;

    std::vector<TableColumnTypeBindingInput> columns;
    columns.reserve(selected_outputs.size());
    try
    {
        for (const auto & selected : selected_outputs)
        {
            columns.push_back({
                .column_name = selected.output_name,
                .declared_type = materializeSelectedOutput(selected, materialization_limits),
            });
        }
    }
    catch (const ViewOutputTypeBindingError & error)
    {
        using ViewCode = ViewOutputTypeBindingError::Code;
        using TableCode = TableColumnTypeBindingError::Code;
        const auto code = [&]
        {
            switch (error.code)
            {
                case ViewCode::InvalidConfiguration: return TableCode::InvalidConfiguration;
                case ViewCode::InvalidObject: return TableCode::InvalidObject;
                case ViewCode::InvalidOutput: return TableCode::InvalidColumn;
                case ViewCode::CrossDatabaseReference: return TableCode::CrossDatabaseReference;
                case ViewCode::SidecarMismatch: return TableCode::SidecarMismatch;
                case ViewCode::PhysicalSchemaMismatch: return TableCode::PhysicalSchemaMismatch;
                case ViewCode::PathMismatch: return TableCode::PathMismatch;
                case ViewCode::LimitExceeded: return TableCode::LimitExceeded;
                case ViewCode::ConflictingDescriptor: return TableCode::ConflictingDescriptor;
            }
            return TableCode::ConflictingDescriptor;
        }();
        throw TableColumnTypeBindingError(code, error.what());
    }
    return prepareTableColumnTypeBindings(table, object_schema_revision, columns, limits);
}

BoundObjectPhysicalSchema reconstructViewOutputPhysicalSchema(
    const SchemaObjectID & expected_view,
    UInt64 expected_object_schema_revision,
    const NamesAndTypesList & physical_outputs,
    const PersistedTypeReferences & persisted_references,
    const ViewOutputTypeBindingLimits & limits)
{
    validateLimits(limits);
    if (!expected_view.isValid() || expected_view.kind != SchemaObjectKind::View || !expected_object_schema_revision)
        fail(Error::Code::InvalidObject, "view physical-schema identity or revision is invalid");
    auto output_types = validateViewPhysicalOutputs(physical_outputs, limits);
    const UInt64 output_count = checkedSize(output_types.size(), "view physical output count exceeds UInt64");
    std::vector<std::string_view> output_names;
    output_names.reserve(physical_outputs.size());
    for (const auto & output : physical_outputs)
        output_names.push_back(output.name);

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

    if (persisted_references.object != expected_view || persisted_references.object_schema_revision != expected_object_schema_revision)
        fail(Error::Code::SidecarMismatch, "view physical-schema identity or revision differs from its sidecar");

    const Digest physical_schema_fingerprint = computePhysicalSchemaFingerprint(physical_outputs);
    if (physical_schema_fingerprint != persisted_references.physical_schema_fingerprint)
        fail(Error::Code::PhysicalSchemaMismatch, "view physical output schema fingerprint differs from its sidecar");

    const UInt64 occurrence_count
        = checkedSize(persisted_references.occurrence_paths.size(), "view physical occurrence count exceeds UInt64");
    if (occurrence_count > limits.maximum_descriptor_occurrences)
        fail(Error::Code::LimitExceeded, "view physical output schema exceeds its descriptor-occurrence limit");

    BoundObjectPhysicalSchema result{
        .object = expected_view,
        .object_schema_revision = expected_object_schema_revision,
        .physical_schema_fingerprint = physical_schema_fingerprint,
        .occurrences = {},
    };
    result.occurrences.reserve(persisted_references.occurrence_paths.size());
    std::vector<const PersistedTypeOccurrencePath *> paths_by_endpoint;
    paths_by_endpoint.reserve(persisted_references.occurrence_paths.size());
    ViewPhysicalTopologyCache topology_cache(limits.maximum_bound_nodes, limits.maximum_retained_path_components);
    UInt64 total_retained_path_components = 0;
    UInt64 total_runtime_owner_key_bytes = 0;
    for (size_t path_id = 0; path_id < persisted_references.occurrence_paths.size(); ++path_id)
    {
        const auto & path = persisted_references.occurrence_paths[path_id];
        if (path.section != PersistedTypePathSection::ViewExpression || path.site != PersistedTypeOccurrenceSite::Declaration)
            fail(Error::Code::PathMismatch, "view output occurrence path is not a declaration endpoint");
        if (path.object_ordinal >= output_count)
            fail(Error::Code::PathMismatch, "view occurrence path is not an output-declaration endpoint");
        checkedAdd(
            total_retained_path_components,
            checkedSize(path.type_child_ordinals.size(), "view occurrence-path depth exceeds UInt64"),
            limits.maximum_retained_path_components,
            "view occurrence paths exceed their retained component limit");
        const auto owner_key = output_names[static_cast<size_t>(path.object_ordinal)];
        const UInt64 owner_key_bytes = checkedSize(owner_key.size(), "view runtime owner-key size exceeds UInt64");
        if (owner_key_bytes > limits.maximum_single_runtime_owner_key_bytes)
            fail(Error::Code::LimitExceeded, "view runtime owner key exceeds its single-key byte limit");
        checkedAdd(
            total_runtime_owner_key_bytes,
            owner_key_bytes,
            limits.maximum_retained_runtime_owner_key_bytes,
            "view runtime owner keys exceed their retained byte limit");

        result.occurrences.push_back({
            .path = path,
            .physical_type = physicalTypeAtNormalizedPath(
                output_types[static_cast<size_t>(path.object_ordinal)], path.type_child_ordinals, topology_cache),
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
            fail(Error::Code::PathMismatch, "view occurrence ordinals are not consecutive at one physical output endpoint");
        previous = path;
    }

    return result;
}

BoundObjectPhysicalSchema reconstructViewMixedPhysicalSchema(
    const SchemaObjectID & expected_view,
    UInt64 expected_object_schema_revision,
    const NamesAndTypesList & physical_outputs,
    std::span<const ViewAuxiliaryPhysicalTypeBindingInput> auxiliary_endpoints,
    const PersistedTypeReferences & persisted_references,
    const ViewOutputTypeBindingLimits & limits)
{
    validateLimits(limits);
    if (!expected_view.isValid() || expected_view.kind != SchemaObjectKind::View || !expected_object_schema_revision)
        fail(Error::Code::InvalidObject, "mixed view physical-schema identity or revision is invalid");
    auto output_types = validateViewPhysicalOutputs(physical_outputs, limits);
    std::vector<std::string_view> output_names;
    output_names.reserve(physical_outputs.size());
    for (const auto & output : physical_outputs)
        output_names.push_back(output.name);
    const auto sorted_auxiliary = validateAuxiliaryPhysicalEndpoints(auxiliary_endpoints, limits);

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
    if (persisted_references.object != expected_view || persisted_references.object_schema_revision != expected_object_schema_revision)
        fail(Error::Code::SidecarMismatch, "mixed view physical-schema identity or revision differs from its sidecar");

    const bool has_non_declaration_path = std::ranges::any_of(
        persisted_references.occurrence_paths, [](const auto & path) { return path.site != PersistedTypeOccurrenceSite::Declaration; });
    if (!has_non_declaration_path)
    {
        return reconstructViewOutputPhysicalSchema(
            expected_view, expected_object_schema_revision, physical_outputs, persisted_references, limits);
    }
    if (persisted_references.format_version != persisted_type_references_format_version_v2
        || persisted_references.path_dictionary_version != persisted_type_path_dictionary_version_v2)
        fail(Error::Code::SidecarMismatch, "mixed view occurrence sites require the V2 sidecar and path dictionary");

    const Digest physical_schema_fingerprint = computeMixedPhysicalSchemaFingerprint(physical_outputs, sorted_auxiliary);
    if (physical_schema_fingerprint != persisted_references.physical_schema_fingerprint)
        fail(Error::Code::PhysicalSchemaMismatch, "mixed view physical endpoint fingerprint differs from its sidecar");
    if (persisted_references.occurrence_paths.size() > limits.maximum_descriptor_occurrences)
        fail(Error::Code::LimitExceeded, "mixed view physical schema exceeds its descriptor-occurrence limit");

    const auto find_auxiliary
        = [&](PersistedTypeOccurrenceSite site, UInt64 object_ordinal) -> const ViewAuxiliaryPhysicalTypeBindingInput *
    {
        const auto found = std::lower_bound(
            sorted_auxiliary.begin(),
            sorted_auxiliary.end(),
            std::pair{static_cast<UInt8>(site), object_ordinal},
            [](const ViewAuxiliaryPhysicalTypeBindingInput * lhs, const std::pair<UInt8, UInt64> & rhs)
            {
                const auto lhs_site = static_cast<UInt8>(lhs->site);
                return lhs_site != rhs.first ? lhs_site < rhs.first : lhs->object_ordinal < rhs.second;
            });
        if (found == sorted_auxiliary.end() || (*found)->site != site || (*found)->object_ordinal != object_ordinal)
            return nullptr;
        return *found;
    };

    BoundObjectPhysicalSchema result{
        .object = expected_view,
        .object_schema_revision = expected_object_schema_revision,
        .physical_schema_fingerprint = physical_schema_fingerprint,
        .occurrences = {},
    };
    result.occurrences.reserve(persisted_references.occurrence_paths.size());
    ViewPhysicalTopologyCache topology_cache(limits.maximum_bound_nodes, limits.maximum_retained_path_components);
    UInt64 total_retained_path_components = 0;
    UInt64 total_runtime_owner_key_bytes = 0;
    const UInt64 output_count = checkedSize(output_types.size(), "mixed view output count exceeds UInt64");
    for (size_t path_id = 0; path_id < persisted_references.occurrence_paths.size(); ++path_id)
    {
        const auto & path = persisted_references.occurrence_paths[path_id];
        DataTypePtr physical_root;
        std::string_view runtime_owner_key;
        if (path.site == PersistedTypeOccurrenceSite::Declaration)
        {
            if (path.object_ordinal >= output_count)
                fail(Error::Code::PathMismatch, "mixed view declaration path is outside its output schema");
            const auto output_ordinal = static_cast<size_t>(path.object_ordinal);
            physical_root = output_types[output_ordinal];
            runtime_owner_key = output_names[output_ordinal];
        }
        else
        {
            const auto * endpoint = find_auxiliary(path.site, path.object_ordinal);
            if (!endpoint)
                fail(Error::Code::PathMismatch, "mixed view sidecar has no exact auxiliary endpoint owner");
            physical_root = endpoint->physical_type;
            runtime_owner_key = endpoint->runtime_owner_key;
        }

        checkedAdd(
            total_retained_path_components,
            checkedSize(path.type_child_ordinals.size(), "mixed view occurrence-path depth exceeds UInt64"),
            limits.maximum_retained_path_components,
            "mixed view occurrence paths exceed their retained component limit");
        const UInt64 owner_key_bytes = checkedSize(runtime_owner_key.size(), "mixed view runtime owner-key size exceeds UInt64");
        if (owner_key_bytes > limits.maximum_single_runtime_owner_key_bytes)
            fail(Error::Code::LimitExceeded, "mixed view runtime owner key exceeds its single-key byte limit");
        checkedAdd(
            total_runtime_owner_key_bytes,
            owner_key_bytes,
            limits.maximum_retained_runtime_owner_key_bytes,
            "mixed view runtime owner keys exceed their retained byte limit");
        result.occurrences.push_back({
            .path = path,
            .physical_type = physicalTypeAtNormalizedPath(physical_root, path.type_child_ordinals, topology_cache),
            .runtime_owner_key = String(runtime_owner_key),
            .selected_semantic_capabilities
            = persisted_references.descriptors[static_cast<size_t>(persisted_references.uses[path_id].descriptor_id)]
                  .getSemanticCapabilities(),
        });
    }

    std::vector<const PersistedTypeOccurrencePath *> paths_by_endpoint;
    paths_by_endpoint.reserve(persisted_references.occurrence_paths.size());
    for (const auto & path : persisted_references.occurrence_paths)
        paths_by_endpoint.push_back(std::addressof(path));
    std::sort(
        paths_by_endpoint.begin(),
        paths_by_endpoint.end(),
        [](const auto * lhs, const auto * rhs)
        {
            if (lhs->site != rhs->site)
                return static_cast<UInt8>(lhs->site) < static_cast<UInt8>(rhs->site);
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
        const bool same_endpoint = previous && previous->site == path->site && previous->object_ordinal == path->object_ordinal
            && previous->type_child_ordinals == path->type_child_ordinals;
        const UInt64 expected_ordinal = same_endpoint ? previous->occurrence_ordinal + 1 : 0;
        if (path->occurrence_ordinal != expected_ordinal)
            fail(Error::Code::PathMismatch, "mixed view occurrence ordinals are not consecutive at one exact endpoint");
        previous = path;
    }
    return result;
}
}
