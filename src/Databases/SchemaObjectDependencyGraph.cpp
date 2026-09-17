#include <Databases/SchemaObjectDependencyGraph.h>

#include <DataTypes/UDT/ResourceAccounting.h>

#include <algorithm>
#include <array>
#include <iterator>
#include <limits>
#include <map>
#include <utility>


namespace DB::UDT
{

SchemaObjectDependencyGraphError::SchemaObjectDependencyGraphError(Code code_, std::string_view message)
    : std::runtime_error(String{message})
    , code(code_)
{
}

namespace
{

using Error = SchemaObjectDependencyGraphError;
using Edge = SchemaObjectDependencyEdge;
using Limits = SchemaObjectDependencyGraphLimits;
using Neighbor = SchemaObjectDependencyNeighbor;
using Statistics = SchemaObjectDependencyGraphMutationStatistics;

constexpr std::string_view graph_root_domain = "ClickHouse UDT schema object dependency graph root V1";
constexpr std::string_view graph_node_empty_domain = "ClickHouse UDT schema object dependency graph node empty V1";
constexpr std::string_view graph_node_leaf_domain = "ClickHouse UDT schema object dependency graph node leaf V1";
constexpr std::string_view graph_node_branch_domain = "ClickHouse UDT schema object dependency graph node branch V1";
constexpr std::string_view graph_edge_empty_domain = "ClickHouse UDT schema object dependency graph edge empty V1";
constexpr std::string_view graph_edge_leaf_domain = "ClickHouse UDT schema object dependency graph edge leaf V1";
constexpr std::string_view graph_edge_branch_domain = "ClickHouse UDT schema object dependency graph edge branch V1";
constexpr size_t node_key_bytes = sizeof(UInt8) + 2 * sizeof(CanonicalUUID);
constexpr size_t neighbor_key_bytes = node_key_bytes + sizeof(UInt8);
constexpr size_t edge_key_bytes = 2 * node_key_bytes + sizeof(UInt8);
constexpr UInt64 wire_node_size = node_key_bytes;
constexpr UInt64 wire_edge_size = edge_key_bytes;

static_assert(
    schema_object_dependency_graph_maximum_edges
    == resource_implementation_maximum_deterministic_catalog_bytes / DeterministicCatalogChargeRates{}.dependency_edge_bytes);

[[noreturn]] void graphError(Error::Code code, std::string_view message)
{
    throw Error(code, message);
}

UInt64 checkedSize(std::size_t value, std::string_view message)
{
    if (!std::in_range<UInt64>(value))
        graphError(Error::Code::LimitExceeded, message);
    return static_cast<UInt64>(value);
}

UInt64 checkedAdd(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        graphError(Error::Code::LimitExceeded, message);
    return lhs + rhs;
}

UInt64 checkedMultiply(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (lhs && rhs > std::numeric_limits<UInt64>::max() / lhs)
        graphError(Error::Code::LimitExceeded, message);
    return lhs * rhs;
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

void appendUInt16LE(String & output, UInt16 value)
{
    output.push_back(static_cast<char>(value));
    output.push_back(static_cast<char>(value >> 8));
}

void appendUInt64LE(String & output, UInt64 value)
{
    for (size_t index = 0; index < sizeof(value); ++index)
        output.push_back(static_cast<char>(value >> (8 * index)));
}

void appendVarUInt(String & output, UInt64 value)
{
    while (value >= 0x80)
    {
        output.push_back(static_cast<char>(static_cast<UInt8>(value) | 0x80));
        value >>= 7;
    }
    output.push_back(static_cast<char>(value));
}

void appendBytes(String & output, std::span<const CanonicalByte> bytes)
{
    output.append(reinterpret_cast<const char *>(bytes.data()), bytes.size());
}

UInt8 encodeObjectKind(SchemaObjectKind kind)
{
    switch (kind)
    {
        case SchemaObjectKind::Table: return 1;
        case SchemaObjectKind::View: return 2;
        case SchemaObjectKind::Dictionary: return 3;
        case SchemaObjectKind::TypeDefinition: return 4;
        case SchemaObjectKind::SyntheticTestObject: return 254;
    }
    graphError(Error::Code::InvalidValue, "unknown schema-object kind");
}

SchemaObjectKind decodeObjectKind(UInt8 value)
{
    switch (value)
    {
        case 1: return SchemaObjectKind::Table;
        case 2: return SchemaObjectKind::View;
        case 3: return SchemaObjectKind::Dictionary;
        case 4: return SchemaObjectKind::TypeDefinition;
        case 254: return SchemaObjectKind::SyntheticTestObject;
        default: graphError(Error::Code::InvalidValue, "unknown schema-object kind");
    }
}

UInt8 encodeEdgeKind(SchemaObjectDependencyEdgeKind kind)
{
    switch (kind)
    {
        case SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition: return 1;
        case SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition: return 2;
        case SchemaObjectDependencyEdgeKind::ObjectDependsOnObject: return 3;
    }
    graphError(Error::Code::InvalidValue, "unknown schema-object dependency edge kind");
}

SchemaObjectDependencyEdgeKind decodeEdgeKind(UInt8 value)
{
    switch (value)
    {
        case 1: return SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition;
        case 2: return SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition;
        case 3: return SchemaObjectDependencyEdgeKind::ObjectDependsOnObject;
        default: graphError(Error::Code::InvalidValue, "unknown schema-object dependency edge kind");
    }
}

void appendObjectID(String & output, const SchemaObjectID & object)
{
    output.push_back(static_cast<char>(encodeObjectKind(object.kind)));
    appendBytes(output, uuidToCanonicalBytes(object.database_uuid));
    appendBytes(output, uuidToCanonicalBytes(object.object_uuid));
}

void validateLimits(const Limits & limits)
{
    if (!limits.maximum_nodes || !limits.maximum_edges || !limits.maximum_edges_per_node || !limits.maximum_mutation_nodes
        || !limits.maximum_mutation_edges || !limits.maximum_retained_bytes)
        graphError(Error::Code::InvalidConfiguration, "schema-object graph limits must be nonzero");
    if (limits.maximum_nodes > schema_object_dependency_graph_maximum_nodes
        || limits.maximum_edges > schema_object_dependency_graph_maximum_edges)
        graphError(Error::Code::InvalidConfiguration, "schema-object graph count limit exceeds the frozen format maximum");
    if (limits.maximum_edges_per_node > limits.maximum_edges)
        graphError(Error::Code::InvalidConfiguration, "schema-object graph adjacency limit exceeds the edge limit");
    if (limits.maximum_mutation_nodes > 2 * schema_object_dependency_graph_maximum_nodes
        || limits.maximum_mutation_edges > schema_object_dependency_graph_maximum_mutation_edges)
        graphError(Error::Code::InvalidConfiguration, "schema-object graph mutation limit exceeds the implementation maximum");
}

void validateDatabaseUUID(UUID database_uuid)
{
    if (database_uuid == UUIDHelpers::Nil)
        graphError(Error::Code::InvalidValue, "schema-object graph database UUID is nil");
}

void validateObject(const SchemaObjectID & object, UUID database_uuid)
{
    if (!object.isValid())
        graphError(Error::Code::InvalidValue, "schema-object identity is invalid");
    if (object.database_uuid != database_uuid)
        graphError(Error::Code::InvalidValue, "schema-object identity belongs to another database");
}

void validateEdge(const Edge & edge, UUID database_uuid)
{
    validateObject(edge.dependent, database_uuid);
    validateObject(edge.dependency, database_uuid);
    if (!isKnownSchemaObjectDependencyEdgeKind(edge.kind))
        graphError(Error::Code::InvalidValue, "schema-object dependency edge kind is invalid");

    const bool dependent_is_definition = edge.dependent.kind == SchemaObjectKind::TypeDefinition;
    const bool dependency_is_definition = edge.dependency.kind == SchemaObjectKind::TypeDefinition;
    switch (edge.kind)
    {
        case SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition:
            if (!dependent_is_definition || !dependency_is_definition)
                graphError(Error::Code::InvalidValue, "definition dependency edge has a non-definition endpoint");
            return;
        case SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition:
            if (dependent_is_definition || !dependency_is_definition)
                graphError(Error::Code::InvalidValue, "object-to-definition edge has incompatible endpoints");
            return;
        case SchemaObjectDependencyEdgeKind::ObjectDependsOnObject:
            if (dependent_is_definition || dependency_is_definition)
                graphError(Error::Code::InvalidValue, "object dependency edge has a definition endpoint");
            return;
    }
    graphError(Error::Code::InvalidValue, "schema-object dependency edge kind is invalid");
}

std::vector<SchemaObjectID> canonicalizeNodes(std::span<const SchemaObjectID> input, UUID database_uuid, UInt64 maximum)
{
    if (checkedSize(input.size(), "schema-object node count does not fit UInt64") > maximum)
        graphError(Error::Code::LimitExceeded, "schema-object node count exceeds its limit");
    std::vector<SchemaObjectID> result(input.begin(), input.end());
    for (const auto & node : result)
        validateObject(node, database_uuid);
    std::sort(result.begin(), result.end());
    if (std::adjacent_find(result.begin(), result.end()) != result.end())
        graphError(Error::Code::DuplicateNode, "schema-object graph contains a duplicate node");
    return result;
}

std::vector<Edge> canonicalizeEdges(std::span<const Edge> input, UUID database_uuid, UInt64 maximum)
{
    if (checkedSize(input.size(), "schema-object edge count does not fit UInt64") > maximum)
        graphError(Error::Code::LimitExceeded, "schema-object edge count exceeds its limit");
    std::vector<Edge> result(input.begin(), input.end());
    for (const auto & edge : result)
        validateEdge(edge, database_uuid);
    std::sort(result.begin(), result.end());
    if (std::adjacent_find(result.begin(), result.end()) != result.end())
        graphError(Error::Code::DuplicateEdge, "schema-object graph contains a duplicate edge");
    return result;
}

bool reverseEdgeLess(const Edge & lhs, const Edge & rhs)
{
    if (lhs.dependency != rhs.dependency)
        return lhs.dependency < rhs.dependency;
    if (lhs.dependent != rhs.dependent)
        return lhs.dependent < rhs.dependent;
    return static_cast<UInt8>(lhs.kind) < static_cast<UInt8>(rhs.kind);
}

template <typename T>
bool sortedSetsOverlap(std::span<const T> lhs, std::span<const T> rhs)
{
    size_t lhs_index = 0;
    size_t rhs_index = 0;
    while (lhs_index < lhs.size() && rhs_index < rhs.size())
    {
        if (lhs[lhs_index] == rhs[rhs_index])
            return true;
        if (lhs[lhs_index] < rhs[rhs_index])
            ++lhs_index;
        else
            ++rhs_index;
    }
    return false;
}

UInt64 snapshotSize(UInt64 node_count, UInt64 edge_count)
{
    UInt64 result = sizeof(UInt16) + sizeof(CanonicalUUID);
    result = checkedAdd(result, varUIntSize(node_count), "schema-object snapshot byte count overflow");
    result = checkedAdd(
        result,
        checkedMultiply(node_count, wire_node_size, "schema-object snapshot node bytes overflow"),
        "schema-object snapshot byte count overflow");
    result = checkedAdd(result, varUIntSize(edge_count), "schema-object snapshot byte count overflow");
    return checkedAdd(
        result,
        checkedMultiply(edge_count, wire_edge_size, "schema-object snapshot edge bytes overflow"),
        "schema-object snapshot byte count overflow");
}

class Reader final
{
public:
    explicit Reader(std::string_view bytes_)
        : bytes(bytes_)
    {
    }

    UInt8 readByte()
    {
        require(1);
        return static_cast<UInt8>(bytes[position++]);
    }
    UInt16 readUInt16LE()
    {
        require(2);
        const UInt16 result = static_cast<UInt16>(
            static_cast<UInt8>(bytes[position]) | (static_cast<UInt16>(static_cast<UInt8>(bytes[position + 1])) << 8));
        position += 2;
        return result;
    }
    UInt64 readMinimalVarUInt(UInt64 maximum)
    {
        UInt64 result = 0;
        UInt8 shift = 0;
        size_t encoded_size = 0;
        while (true)
        {
            const UInt8 byte = readByte();
            ++encoded_size;
            if (shift == 63 && (byte & 0xfe) != 0)
                graphError(Error::Code::InvalidValue, "schema-object snapshot VarUInt overflows UInt64");
            result |= static_cast<UInt64>(byte & 0x7f) << shift;
            if ((byte & 0x80) == 0)
                break;
            if (shift >= 63)
                graphError(Error::Code::InvalidValue, "schema-object snapshot VarUInt overflows UInt64");
            shift = static_cast<UInt8>(shift + 7);
        }
        if (encoded_size != varUIntSize(result))
            graphError(Error::Code::NonCanonical, "schema-object snapshot VarUInt is not minimally encoded");
        if (result > maximum)
            graphError(Error::Code::LimitExceeded, "schema-object snapshot count exceeds its limit");
        return result;
    }
    template <size_t size>
    std::array<CanonicalByte, size> readArray()
    {
        require(size);
        std::array<CanonicalByte, size> result{};
        std::copy_n(reinterpret_cast<const CanonicalByte *>(bytes.data() + position), size, result.begin());
        position += size;
        return result;
    }
    void requireEnd() const
    {
        if (position != bytes.size())
            graphError(Error::Code::TrailingData, "schema-object snapshot has trailing data");
    }
    void requireElements(size_t count, size_t element_size) const
    {
        if (element_size && count > (bytes.size() - position) / element_size)
            graphError(Error::Code::Truncated, "schema-object snapshot declared count exceeds its remaining bytes");
    }

private:
    void require(size_t count) const
    {
        if (count > bytes.size() - position)
            graphError(Error::Code::Truncated, "schema-object snapshot is truncated");
    }
    std::string_view bytes;
    size_t position = 0;
};

SchemaObjectID readObjectID(Reader & reader, UUID database_uuid)
{
    SchemaObjectID result{
        .kind = decodeObjectKind(reader.readByte()),
        .database_uuid = uuidFromCanonicalBytes(reader.readArray<16>()),
        .object_uuid = uuidFromCanonicalBytes(reader.readArray<16>()),
    };
    validateObject(result, database_uuid);
    return result;
}

struct GraphKey
{
    std::array<CanonicalByte, edge_key_bytes> bytes{};
    UInt16 size = 0;
    bool operator==(const GraphKey &) const = default;
};

GraphKey nodeKey(const SchemaObjectID & object)
{
    GraphKey result;
    result.size = node_key_bytes;
    result.bytes[0] = encodeObjectKind(object.kind);
    const auto database = uuidToCanonicalBytes(object.database_uuid);
    const auto uuid = uuidToCanonicalBytes(object.object_uuid);
    std::copy(database.begin(), database.end(), result.bytes.begin() + 1);
    std::copy(uuid.begin(), uuid.end(), result.bytes.begin() + 17);
    return result;
}

GraphKey edgeKey(const Edge & edge)
{
    GraphKey result;
    result.size = edge_key_bytes;
    const auto dependent = nodeKey(edge.dependent);
    const auto dependency = nodeKey(edge.dependency);
    std::copy_n(dependent.bytes.begin(), node_key_bytes, result.bytes.begin());
    std::copy_n(dependency.bytes.begin(), node_key_bytes, result.bytes.begin() + node_key_bytes);
    result.bytes[2 * node_key_bytes] = encodeEdgeKind(edge.kind);
    return result;
}

GraphKey neighborKey(const Neighbor & neighbor)
{
    GraphKey result = nodeKey(neighbor.object);
    result.size = neighbor_key_bytes;
    result.bytes[node_key_bytes] = encodeEdgeKind(neighbor.kind);
    return result;
}

UInt8 keyNibble(const GraphKey & key, UInt16 depth) noexcept
{
    const UInt8 byte = key.bytes[depth / 2];
    return depth % 2 == 0 ? static_cast<UInt8>(byte >> 4) : static_cast<UInt8>(byte & 0x0f);
}

UInt16 firstDifferentNibble(const GraphKey & lhs, const GraphKey & rhs) noexcept
{
    const UInt16 nibbles = static_cast<UInt16>(lhs.size * 2);
    for (UInt16 depth = 0; depth < nibbles; ++depth)
        if (keyNibble(lhs, depth) != keyNibble(rhs, depth))
            return depth;
    return nibbles;
}

enum class SetKind : UInt8
{
    Node,
    Edge
};

struct SetNode
{
    using Ptr = std::shared_ptr<const SetNode>;
    bool is_leaf = false;
    GraphKey key;
    SchemaObjectID node;
    Edge edge;
    UInt16 branch_depth = 0;
    std::vector<std::pair<UInt8, Ptr>> children;
    Digest digest{};
    UInt64 accounted_bytes = 0;
};

struct SetEntry
{
    GraphKey key;
    SchemaObjectID node;
    Edge edge;
};

std::string_view emptyDomain(SetKind kind)
{
    return kind == SetKind::Node ? graph_node_empty_domain : graph_edge_empty_domain;
}
std::string_view leafDomain(SetKind kind)
{
    return kind == SetKind::Node ? graph_node_leaf_domain : graph_edge_leaf_domain;
}
std::string_view branchDomain(SetKind kind)
{
    return kind == SetKind::Node ? graph_node_branch_domain : graph_edge_branch_domain;
}

Digest emptySetDigest(SetKind kind)
{
    return hashFramedDomainSeparated(emptyDomain(kind), std::string_view{});
}

SetNode::Ptr makeSetLeaf(SetKind kind, const SetEntry & entry, Statistics * statistics = nullptr)
{
    auto result = std::make_shared<SetNode>();
    result->is_leaf = true;
    result->key = entry.key;
    result->node = entry.node;
    result->edge = entry.edge;
    result->digest = hashFramedDomainSeparated(leafDomain(kind), std::span<const CanonicalByte>(entry.key.bytes.data(), entry.key.size));
    result->accounted_bytes = sizeof(SetNode);
    if (statistics)
    {
        ++statistics->set_nodes_created;
        ++statistics->set_nodes_hashed;
    }
    return result;
}

SetNode::Ptr
makeSetBranch(SetKind kind, UInt16 depth, std::vector<std::pair<UInt8, SetNode::Ptr>> children, Statistics * statistics = nullptr)
{
    if (children.size() < 2)
        graphError(Error::Code::NonCanonical, "schema-object graph radix branch is unary");
    std::sort(children.begin(), children.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });
    UInt16 present = 0;
    String payload;
    payload.reserve(2 * sizeof(UInt16) + children.size() * (sizeof(UInt8) + sizeof(Digest)));
    appendUInt16LE(payload, depth);
    for (const auto & [nibble, child] : children)
    {
        if (!child || nibble >= 16 || (present & (UInt16{1} << nibble)))
            graphError(Error::Code::NonCanonical, "schema-object graph radix branch is invalid");
        present = static_cast<UInt16>(present | (UInt16{1} << nibble));
    }
    appendUInt16LE(payload, present);
    for (const auto & [nibble, child] : children)
    {
        payload.push_back(static_cast<char>(nibble));
        appendBytes(payload, child->digest);
    }
    auto result = std::make_shared<SetNode>();
    result->key = children.front().second->key;
    result->branch_depth = depth;
    result->children = std::move(children);
    result->digest = hashFramedDomainSeparated(branchDomain(kind), payload);
    result->accounted_bytes = checkedAdd(
        sizeof(SetNode),
        checkedMultiply(
            result->children.size(), sizeof(decltype(result->children)::value_type), "schema-object graph radix charge overflow"),
        "schema-object graph radix charge overflow");
    for (const auto & [nibble, child] : result->children)
    {
        static_cast<void>(nibble);
        result->accounted_bytes = checkedAdd(result->accounted_bytes, child->accounted_bytes, "schema-object graph radix charge overflow");
    }
    if (statistics)
    {
        ++statistics->set_nodes_created;
        ++statistics->set_nodes_hashed;
    }
    return result;
}

SetNode::Ptr buildSetTree(SetKind kind, std::span<const SetEntry> entries, size_t begin, size_t end, UInt16 depth)
{
    if (begin == end)
        return {};
    if (end - begin == 1)
        return makeSetLeaf(kind, entries[begin]);
    const UInt16 maximum_depth = static_cast<UInt16>(entries[begin].key.size * 2);
    if (depth == maximum_depth)
        graphError(Error::Code::NonCanonical, "schema-object graph radix contains a duplicate key");
    std::vector<std::pair<UInt8, SetNode::Ptr>> children;
    size_t cursor = begin;
    while (cursor < end)
    {
        const UInt8 nibble = keyNibble(entries[cursor].key, depth);
        const size_t child_begin = cursor;
        do
        {
            ++cursor;
        } while (cursor < end && keyNibble(entries[cursor].key, depth) == nibble);
        children.emplace_back(nibble, buildSetTree(kind, entries, child_begin, cursor, static_cast<UInt16>(depth + 1)));
    }
    if (children.size() == 1)
        return children.front().second;
    return makeSetBranch(kind, depth, std::move(children));
}

const SetNode * findSetNode(const SetNode::Ptr & root, const GraphKey & key, Statistics * statistics = nullptr) noexcept
{
    auto current = root;
    while (current)
    {
        if (statistics)
            ++statistics->set_nodes_visited;
        if (current->is_leaf)
            return current->key == key ? current.get() : nullptr;
        const UInt8 nibble = keyNibble(key, current->branch_depth);
        const auto child = std::lower_bound(
            current->children.begin(),
            current->children.end(),
            nibble,
            [](const auto & value, UInt8 sought) { return value.first < sought; });
        if (child == current->children.end() || child->first != nibble)
            return nullptr;
        current = child->second;
    }
    return nullptr;
}

SetNode::Ptr insertSetEntry(SetKind kind, const SetNode::Ptr & root, const SetEntry & entry, Statistics * statistics)
{
    if (root && statistics)
        ++statistics->set_nodes_visited;
    if (!root)
        return makeSetLeaf(kind, entry, statistics);
    if (root->is_leaf)
    {
        if (root->key == entry.key)
            graphError(Error::Code::ExistingNode, "schema-object graph radix insertion replaces a key");
        const UInt16 depth = firstDifferentNibble(root->key, entry.key);
        return makeSetBranch(
            kind,
            depth,
            {{keyNibble(root->key, depth), root}, {keyNibble(entry.key, depth), makeSetLeaf(kind, entry, statistics)}},
            statistics);
    }
    const UInt16 diverging_depth = firstDifferentNibble(root->key, entry.key);
    if (diverging_depth < root->branch_depth)
        return makeSetBranch(
            kind,
            diverging_depth,
            {{keyNibble(root->key, diverging_depth), root}, {keyNibble(entry.key, diverging_depth), makeSetLeaf(kind, entry, statistics)}},
            statistics);
    const UInt8 nibble = keyNibble(entry.key, root->branch_depth);
    auto children = root->children;
    auto child
        = std::lower_bound(children.begin(), children.end(), nibble, [](const auto & value, UInt8 sought) { return value.first < sought; });
    if (child == children.end() || child->first != nibble)
        children.insert(child, {nibble, makeSetLeaf(kind, entry, statistics)});
    else
        child->second = insertSetEntry(kind, child->second, entry, statistics);
    return makeSetBranch(kind, root->branch_depth, std::move(children), statistics);
}

SetNode::Ptr removeSetEntry(SetKind kind, const SetNode::Ptr & root, const GraphKey & key, Statistics * statistics)
{
    if (!root)
        graphError(Error::Code::MissingNode, "schema-object graph radix removal misses a key");
    if (statistics)
        ++statistics->set_nodes_visited;
    if (root->is_leaf)
    {
        if (root->key != key)
            graphError(Error::Code::MissingNode, "schema-object graph radix removal misses a key");
        return {};
    }
    const UInt8 nibble = keyNibble(key, root->branch_depth);
    auto children = root->children;
    auto child
        = std::lower_bound(children.begin(), children.end(), nibble, [](const auto & value, UInt8 sought) { return value.first < sought; });
    if (child == children.end() || child->first != nibble)
        graphError(Error::Code::MissingNode, "schema-object graph radix removal misses a branch");
    auto replacement = removeSetEntry(kind, child->second, key, statistics);
    if (replacement)
        child->second = std::move(replacement);
    else
        children.erase(child);
    if (children.size() == 1)
        return children.front().second;
    return makeSetBranch(kind, root->branch_depth, std::move(children), statistics);
}

void materializeSet(
    const SetNode::Ptr & root, std::vector<SchemaObjectID> & nodes, std::vector<Edge> * edges, Statistics * statistics = nullptr)
{
    if (!root)
        return;
    if (root->is_leaf)
    {
        if (edges)
        {
            edges->push_back(root->edge);
            if (statistics)
                ++statistics->snapshot_edges_materialized;
        }
        else
        {
            nodes.push_back(root->node);
            if (statistics)
                ++statistics->snapshot_nodes_materialized;
        }
        return;
    }
    for (const auto & [nibble, child] : root->children)
    {
        static_cast<void>(nibble);
        materializeSet(child, nodes, edges, statistics);
    }
}

struct NeighborNode
{
    using Ptr = std::shared_ptr<const NeighborNode>;
    bool is_leaf = false;
    GraphKey key;
    Neighbor neighbor;
    UInt16 branch_depth = 0;
    std::vector<std::pair<UInt8, Ptr>> children;
    UInt64 accounted_bytes = 0;
};

NeighborNode::Ptr makeNeighborLeaf(const Neighbor & neighbor, Statistics * statistics = nullptr)
{
    auto result = std::make_shared<NeighborNode>();
    result->is_leaf = true;
    result->key = neighborKey(neighbor);
    result->neighbor = neighbor;
    result->accounted_bytes = sizeof(NeighborNode);
    if (statistics)
        ++statistics->neighbor_nodes_created;
    return result;
}

NeighborNode::Ptr
makeNeighborBranch(UInt16 depth, std::vector<std::pair<UInt8, NeighborNode::Ptr>> children, Statistics * statistics = nullptr)
{
    if (children.size() < 2)
        graphError(Error::Code::NonCanonical, "schema-object neighbor radix branch is unary");
    std::sort(children.begin(), children.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });
    auto result = std::make_shared<NeighborNode>();
    result->key = children.front().second->key;
    result->branch_depth = depth;
    result->children = std::move(children);
    result->accounted_bytes = checkedAdd(
        sizeof(NeighborNode),
        checkedMultiply(
            result->children.size(), sizeof(decltype(result->children)::value_type), "schema-object neighbor radix charge overflow"),
        "schema-object neighbor radix charge overflow");
    for (const auto & [nibble, child] : result->children)
    {
        static_cast<void>(nibble);
        result->accounted_bytes
            = checkedAdd(result->accounted_bytes, child->accounted_bytes, "schema-object neighbor radix charge overflow");
    }
    if (statistics)
        ++statistics->neighbor_nodes_created;
    return result;
}

NeighborNode::Ptr buildNeighborTree(std::span<const Neighbor> neighbors, size_t begin, size_t end, UInt16 depth)
{
    if (begin == end)
        return {};
    if (end - begin == 1)
        return makeNeighborLeaf(neighbors[begin]);
    if (depth == neighbor_key_bytes * 2)
        graphError(Error::Code::NonCanonical, "schema-object neighbor radix contains a duplicate key");
    std::vector<std::pair<UInt8, NeighborNode::Ptr>> children;
    size_t cursor = begin;
    while (cursor < end)
    {
        const UInt8 nibble = keyNibble(neighborKey(neighbors[cursor]), depth);
        const size_t child_begin = cursor;
        do
        {
            ++cursor;
        } while (cursor < end && keyNibble(neighborKey(neighbors[cursor]), depth) == nibble);
        children.emplace_back(nibble, buildNeighborTree(neighbors, child_begin, cursor, static_cast<UInt16>(depth + 1)));
    }
    if (children.size() == 1)
        return children.front().second;
    return makeNeighborBranch(depth, std::move(children));
}

const Neighbor * findNeighbor(const NeighborNode::Ptr & root, const Neighbor & neighbor, Statistics * statistics = nullptr) noexcept
{
    const GraphKey key = neighborKey(neighbor);
    auto current = root;
    while (current)
    {
        if (statistics)
            ++statistics->neighbor_nodes_visited;
        if (current->is_leaf)
            return current->key == key ? &current->neighbor : nullptr;
        const UInt8 nibble = keyNibble(key, current->branch_depth);
        const auto child = std::lower_bound(
            current->children.begin(),
            current->children.end(),
            nibble,
            [](const auto & value, UInt8 sought) { return value.first < sought; });
        if (child == current->children.end() || child->first != nibble)
            return nullptr;
        current = child->second;
    }
    return nullptr;
}

NeighborNode::Ptr insertNeighbor(const NeighborNode::Ptr & root, const Neighbor & neighbor, const GraphKey & key, Statistics * statistics)
{
    if (root && statistics)
        ++statistics->neighbor_nodes_visited;
    if (!root)
        return makeNeighborLeaf(neighbor, statistics);
    if (root->is_leaf)
    {
        if (root->key == key)
            graphError(Error::Code::ExistingEdge, "schema-object neighbor insertion replaces a key");
        const UInt16 depth = firstDifferentNibble(root->key, key);
        return makeNeighborBranch(
            depth, {{keyNibble(root->key, depth), root}, {keyNibble(key, depth), makeNeighborLeaf(neighbor, statistics)}}, statistics);
    }
    const UInt16 diverging_depth = firstDifferentNibble(root->key, key);
    if (diverging_depth < root->branch_depth)
        return makeNeighborBranch(
            diverging_depth,
            {{keyNibble(root->key, diverging_depth), root}, {keyNibble(key, diverging_depth), makeNeighborLeaf(neighbor, statistics)}},
            statistics);
    const UInt8 nibble = keyNibble(key, root->branch_depth);
    auto children = root->children;
    auto child
        = std::lower_bound(children.begin(), children.end(), nibble, [](const auto & value, UInt8 sought) { return value.first < sought; });
    if (child == children.end() || child->first != nibble)
        children.insert(child, {nibble, makeNeighborLeaf(neighbor, statistics)});
    else
        child->second = insertNeighbor(child->second, neighbor, key, statistics);
    return makeNeighborBranch(root->branch_depth, std::move(children), statistics);
}

NeighborNode::Ptr removeNeighbor(const NeighborNode::Ptr & root, const GraphKey & key, Statistics * statistics)
{
    if (!root)
        graphError(Error::Code::MissingEdge, "schema-object neighbor removal misses a key");
    if (statistics)
        ++statistics->neighbor_nodes_visited;
    if (root->is_leaf)
    {
        if (root->key != key)
            graphError(Error::Code::MissingEdge, "schema-object neighbor removal misses a key");
        return {};
    }
    const UInt8 nibble = keyNibble(key, root->branch_depth);
    auto children = root->children;
    auto child
        = std::lower_bound(children.begin(), children.end(), nibble, [](const auto & value, UInt8 sought) { return value.first < sought; });
    if (child == children.end() || child->first != nibble)
        graphError(Error::Code::MissingEdge, "schema-object neighbor removal misses a branch");
    auto replacement = removeNeighbor(child->second, key, statistics);
    if (replacement)
        child->second = std::move(replacement);
    else
        children.erase(child);
    if (children.size() == 1)
        return children.front().second;
    return makeNeighborBranch(root->branch_depth, std::move(children), statistics);
}

void materializeNeighbors(const NeighborNode::Ptr & root, std::vector<Neighbor> & output)
{
    if (!root)
        return;
    if (root->is_leaf)
    {
        output.push_back(root->neighbor);
        return;
    }
    for (const auto & [nibble, child] : root->children)
    {
        static_cast<void>(nibble);
        materializeNeighbors(child, output);
    }
}

struct NeighborSet
{
    using Ptr = std::shared_ptr<const NeighborSet>;

    NeighborSet(NeighborNode::Ptr root_, UInt64 count_)
        : root(std::move(root_))
        , count(count_)
    {
        accounted_bytes = checkedAdd(
            sizeof(NeighborSet),
            checkedMultiply(count, sizeof(Neighbor), "schema-object materialized neighbor charge overflow"),
            "schema-object neighbor-set charge overflow");
        if (root)
            accounted_bytes = checkedAdd(accounted_bytes, root->accounted_bytes, "schema-object neighbor-set charge overflow");
    }

    std::span<const Neighbor> getNeighbors() const
    {
        std::call_once(
            materialize_once,
            [&]
            {
                materialized.reserve(static_cast<size_t>(count));
                materializeNeighbors(root, materialized);
            });
        return materialized;
    }

    NeighborNode::Ptr root;
    UInt64 count = 0;
    UInt64 accounted_bytes = 0;
    mutable std::once_flag materialize_once;
    mutable std::vector<Neighbor> materialized;
};

NeighborSet::Ptr buildNeighborSet(std::span<const Neighbor> neighbors)
{
    return std::make_shared<NeighborSet>(buildNeighborTree(neighbors, 0, neighbors.size(), 0), neighbors.size());
}

struct AdjacencyNode
{
    using Ptr = std::shared_ptr<const AdjacencyNode>;
    bool is_leaf = false;
    GraphKey key;
    SchemaObjectID object;
    NeighborSet::Ptr adjacent;
    UInt16 branch_depth = 0;
    std::vector<std::pair<UInt8, Ptr>> children;
    UInt64 maximum_degree = 0;
    UInt64 accounted_bytes = 0;
};

struct AdjacencyEntry
{
    SchemaObjectID object;
    NeighborSet::Ptr adjacent;
};

AdjacencyNode::Ptr makeAdjacencyLeaf(const AdjacencyEntry & entry, Statistics * statistics = nullptr)
{
    if (!entry.adjacent || !entry.adjacent->count)
        graphError(Error::Code::NonCanonical, "schema-object adjacency leaf is empty");
    auto result = std::make_shared<AdjacencyNode>();
    result->is_leaf = true;
    result->key = nodeKey(entry.object);
    result->object = entry.object;
    result->adjacent = entry.adjacent;
    result->maximum_degree = entry.adjacent->count;
    result->accounted_bytes = checkedAdd(sizeof(AdjacencyNode), entry.adjacent->accounted_bytes, "schema-object adjacency charge overflow");
    if (statistics)
        ++statistics->adjacency_nodes_created;
    return result;
}

AdjacencyNode::Ptr
makeAdjacencyBranch(UInt16 depth, std::vector<std::pair<UInt8, AdjacencyNode::Ptr>> children, Statistics * statistics = nullptr)
{
    if (children.size() < 2)
        graphError(Error::Code::NonCanonical, "schema-object adjacency radix branch is unary");
    std::sort(children.begin(), children.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });
    auto result = std::make_shared<AdjacencyNode>();
    result->key = children.front().second->key;
    result->branch_depth = depth;
    result->children = std::move(children);
    result->accounted_bytes = checkedAdd(
        sizeof(AdjacencyNode),
        checkedMultiply(
            result->children.size(), sizeof(decltype(result->children)::value_type), "schema-object adjacency radix charge overflow"),
        "schema-object adjacency radix charge overflow");
    for (const auto & [nibble, child] : result->children)
    {
        static_cast<void>(nibble);
        result->maximum_degree = std::max(result->maximum_degree, child->maximum_degree);
        result->accounted_bytes
            = checkedAdd(result->accounted_bytes, child->accounted_bytes, "schema-object adjacency radix charge overflow");
    }
    if (statistics)
        ++statistics->adjacency_nodes_created;
    return result;
}

AdjacencyNode::Ptr buildAdjacencyTree(std::span<const AdjacencyEntry> entries, size_t begin, size_t end, UInt16 depth)
{
    if (begin == end)
        return {};
    if (end - begin == 1)
        return makeAdjacencyLeaf(entries[begin]);
    std::vector<std::pair<UInt8, AdjacencyNode::Ptr>> children;
    size_t cursor = begin;
    while (cursor < end)
    {
        const GraphKey key = nodeKey(entries[cursor].object);
        const UInt8 nibble = keyNibble(key, depth);
        const size_t child_begin = cursor;
        do
        {
            ++cursor;
        } while (cursor < end && keyNibble(nodeKey(entries[cursor].object), depth) == nibble);
        children.emplace_back(nibble, buildAdjacencyTree(entries, child_begin, cursor, static_cast<UInt16>(depth + 1)));
    }
    if (children.size() == 1)
        return children.front().second;
    return makeAdjacencyBranch(depth, std::move(children));
}

const NeighborSet *
findAdjacency(const AdjacencyNode::Ptr & root, const SchemaObjectID & object, Statistics * statistics = nullptr) noexcept
{
    const GraphKey key = nodeKey(object);
    auto current = root;
    while (current)
    {
        if (statistics)
            ++statistics->adjacency_nodes_visited;
        if (current->is_leaf)
            return current->key == key ? current->adjacent.get() : nullptr;
        const UInt8 nibble = keyNibble(key, current->branch_depth);
        const auto child = std::lower_bound(
            current->children.begin(),
            current->children.end(),
            nibble,
            [](const auto & value, UInt8 sought) { return value.first < sought; });
        if (child == current->children.end() || child->first != nibble)
            return nullptr;
        current = child->second;
    }
    return nullptr;
}

AdjacencyNode::Ptr upsertAdjacency(const AdjacencyNode::Ptr & root, const AdjacencyEntry & entry, Statistics * statistics)
{
    const GraphKey key = nodeKey(entry.object);
    if (root && statistics)
        ++statistics->adjacency_nodes_visited;
    if (!root)
        return makeAdjacencyLeaf(entry, statistics);
    if (root->is_leaf)
    {
        if (root->key == key)
            return makeAdjacencyLeaf(entry, statistics);
        const UInt16 depth = firstDifferentNibble(root->key, key);
        return makeAdjacencyBranch(
            depth, {{keyNibble(root->key, depth), root}, {keyNibble(key, depth), makeAdjacencyLeaf(entry, statistics)}}, statistics);
    }
    const UInt16 diverging_depth = firstDifferentNibble(root->key, key);
    if (diverging_depth < root->branch_depth)
        return makeAdjacencyBranch(
            diverging_depth,
            {{keyNibble(root->key, diverging_depth), root}, {keyNibble(key, diverging_depth), makeAdjacencyLeaf(entry, statistics)}},
            statistics);
    const UInt8 nibble = keyNibble(key, root->branch_depth);
    auto children = root->children;
    auto child
        = std::lower_bound(children.begin(), children.end(), nibble, [](const auto & value, UInt8 sought) { return value.first < sought; });
    if (child == children.end() || child->first != nibble)
        children.insert(child, {nibble, makeAdjacencyLeaf(entry, statistics)});
    else
        child->second = upsertAdjacency(child->second, entry, statistics);
    return makeAdjacencyBranch(root->branch_depth, std::move(children), statistics);
}

AdjacencyNode::Ptr removeAdjacency(const AdjacencyNode::Ptr & root, const SchemaObjectID & object, Statistics * statistics)
{
    const GraphKey key = nodeKey(object);
    if (!root)
        graphError(Error::Code::MissingEdge, "schema-object adjacency removal misses a key");
    if (statistics)
        ++statistics->adjacency_nodes_visited;
    if (root->is_leaf)
    {
        if (root->key != key)
            graphError(Error::Code::MissingEdge, "schema-object adjacency removal misses a key");
        return {};
    }
    const UInt8 nibble = keyNibble(key, root->branch_depth);
    auto children = root->children;
    auto child
        = std::lower_bound(children.begin(), children.end(), nibble, [](const auto & value, UInt8 sought) { return value.first < sought; });
    if (child == children.end() || child->first != nibble)
        graphError(Error::Code::MissingEdge, "schema-object adjacency removal misses a branch");
    auto replacement = removeAdjacency(child->second, object, statistics);
    if (replacement)
        child->second = std::move(replacement);
    else
        children.erase(child);
    if (children.size() == 1)
        return children.front().second;
    return makeAdjacencyBranch(root->branch_depth, std::move(children), statistics);
}

Digest computeGraphRoot(UUID database_uuid, UInt64 node_count, const Digest & node_root, UInt64 edge_count, const Digest & edge_root)
{
    String payload;
    payload.reserve(sizeof(UInt16) + sizeof(CanonicalUUID) + 2 * sizeof(UInt64) + 2 * sizeof(Digest));
    appendUInt16LE(payload, schema_object_dependency_graph_format_version);
    appendBytes(payload, uuidToCanonicalBytes(database_uuid));
    appendUInt64LE(payload, node_count);
    appendBytes(payload, node_root);
    appendUInt64LE(payload, edge_count);
    appendBytes(payload, edge_root);
    return hashFramedDomainSeparated(graph_root_domain, payload);
}

struct AdjDelta
{
    std::vector<Neighbor> additions;
    std::vector<Neighbor> removals;
};

NeighborSet::Ptr applyNeighborDelta(const NeighborSet * base, AdjDelta & delta, UInt64 maximum, Statistics * statistics)
{
    std::sort(delta.additions.begin(), delta.additions.end());
    std::sort(delta.removals.begin(), delta.removals.end());
    if (std::adjacent_find(delta.additions.begin(), delta.additions.end()) != delta.additions.end()
        || std::adjacent_find(delta.removals.begin(), delta.removals.end()) != delta.removals.end()
        || sortedSetsOverlap<Neighbor>(delta.additions, delta.removals))
        graphError(Error::Code::ConflictingMutation, "schema-object adjacency delta is conflicting");
    const UInt64 base_count = base ? base->count : 0;
    if (delta.removals.size() > base_count)
        graphError(Error::Code::MissingEdge, "schema-object adjacency delta removes too many edges");
    const UInt64 retained_count = base_count - delta.removals.size();
    const UInt64 next_count = checkedAdd(retained_count, delta.additions.size(), "schema-object adjacency degree overflow");
    if (next_count > maximum)
        graphError(Error::Code::LimitExceeded, "schema-object adjacency degree exceeds its limit");

    NeighborNode::Ptr next_root = base ? base->root : NeighborNode::Ptr{};
    for (const auto & removal : delta.removals)
    {
        const GraphKey key = neighborKey(removal);
        if (!findNeighbor(next_root, removal, statistics))
            graphError(Error::Code::MissingEdge, "schema-object adjacency delta removes an absent edge");
        next_root = removeNeighbor(next_root, key, statistics);
    }
    for (const auto & addition : delta.additions)
    {
        const GraphKey key = neighborKey(addition);
        if (findNeighbor(next_root, addition, statistics))
            graphError(Error::Code::ExistingEdge, "schema-object adjacency delta adds an existing edge");
        next_root = insertNeighbor(next_root, addition, key, statistics);
    }
    if (!next_count)
        return {};
    return std::make_shared<NeighborSet>(std::move(next_root), next_count);
}

}

struct SchemaObjectDependencyGraphStorage
{
    SetNode::Ptr node_set;
    SetNode::Ptr edge_set;
    AdjacencyNode::Ptr forward;
    AdjacencyNode::Ptr reverse;
};

namespace
{

UInt64 graphLogicalBytes(const SchemaObjectDependencyGraphStorage & storage, UInt64 nodes)
{
    UInt64 result = checkedAdd(
        sizeof(SchemaObjectDependencyGraph), sizeof(SchemaObjectDependencyGraphStorage), "schema-object graph charge overflow");
    result = checkedAdd(
        result,
        checkedMultiply(nodes, sizeof(SchemaObjectID), "schema-object materialized node charge overflow"),
        "schema-object graph charge overflow");
    if (storage.node_set)
        result = checkedAdd(result, storage.node_set->accounted_bytes, "schema-object graph charge overflow");
    if (storage.edge_set)
        result = checkedAdd(result, storage.edge_set->accounted_bytes, "schema-object graph charge overflow");
    if (storage.forward)
        result = checkedAdd(result, storage.forward->accounted_bytes, "schema-object graph charge overflow");
    if (storage.reverse)
        result = checkedAdd(result, storage.reverse->accounted_bytes, "schema-object graph charge overflow");
    return result;
}

}

SchemaObjectDependencyGraph::SchemaObjectDependencyGraph(
    UUID database_uuid_,
    Limits limits_,
    std::shared_ptr<const SchemaObjectDependencyGraphStorage> storage_,
    UInt64 node_count_,
    UInt64 edge_count_,
    UInt64 forward_group_count_,
    UInt64 reverse_group_count_,
    UInt64 accounted_bytes_,
    Digest merkle_root_,
    std::vector<SchemaObjectID> materialized_nodes_)
    : database_uuid(database_uuid_)
    , limits(std::move(limits_))
    , storage(std::move(storage_))
    , node_count(node_count_)
    , edge_count(edge_count_)
    , forward_group_count(forward_group_count_)
    , reverse_group_count(reverse_group_count_)
    , accounted_bytes(accounted_bytes_)
    , merkle_root(merkle_root_)
    , materialized_nodes(std::move(materialized_nodes_))
{
}

SchemaObjectDependencyGraph::Ptr SchemaObjectDependencyGraph::createEmpty(UUID database_uuid, const Limits & limits)
{
    return buildCanonical(database_uuid, {}, {}, limits);
}

SchemaObjectDependencyGraph::Ptr SchemaObjectDependencyGraph::build(
    UUID database_uuid, std::span<const SchemaObjectID> nodes, std::span<const Edge> edges, const Limits & limits)
{
    validateLimits(limits);
    validateDatabaseUUID(database_uuid);
    return buildCanonical(
        database_uuid,
        canonicalizeNodes(nodes, database_uuid, limits.maximum_nodes),
        canonicalizeEdges(edges, database_uuid, limits.maximum_edges),
        limits);
}

SchemaObjectDependencyGraph::Ptr SchemaObjectDependencyGraph::buildCanonical(
    UUID database_uuid, std::vector<SchemaObjectID> canonical_nodes, std::vector<Edge> canonical_edges, const Limits & limits)
{
    validateLimits(limits);
    validateDatabaseUUID(database_uuid);
    if (canonical_nodes.size() > limits.maximum_nodes || canonical_edges.size() > limits.maximum_edges)
        graphError(Error::Code::LimitExceeded, "schema-object graph count exceeds its limit");
    for (size_t index = 0; index < canonical_nodes.size(); ++index)
    {
        validateObject(canonical_nodes[index], database_uuid);
        if (index && !(canonical_nodes[index - 1] < canonical_nodes[index]))
            graphError(Error::Code::DuplicateNode, "schema-object graph nodes are not uniquely canonical");
    }
    for (size_t index = 0; index < canonical_edges.size(); ++index)
    {
        validateEdge(canonical_edges[index], database_uuid);
        if (index && !(canonical_edges[index - 1] < canonical_edges[index]))
            graphError(Error::Code::DuplicateEdge, "schema-object graph edges are not uniquely canonical");
        if (!std::binary_search(canonical_nodes.begin(), canonical_nodes.end(), canonical_edges[index].dependent)
            || !std::binary_search(canonical_nodes.begin(), canonical_nodes.end(), canonical_edges[index].dependency))
            graphError(Error::Code::MissingNode, "schema-object dependency edge has an absent endpoint");
    }

    std::vector<SetEntry> node_entries;
    node_entries.reserve(canonical_nodes.size());
    for (const auto & node : canonical_nodes)
        node_entries.push_back({.key = nodeKey(node), .node = node, .edge = {}});
    std::vector<SetEntry> edge_entries;
    edge_entries.reserve(canonical_edges.size());
    for (const auto & edge : canonical_edges)
        edge_entries.push_back({.key = edgeKey(edge), .node = {}, .edge = edge});

    std::vector<AdjacencyEntry> forward_entries;
    size_t begin = 0;
    while (begin < canonical_edges.size())
    {
        size_t end = begin + 1;
        while (end < canonical_edges.size() && canonical_edges[end].dependent == canonical_edges[begin].dependent)
            ++end;
        if (end - begin > limits.maximum_edges_per_node)
            graphError(Error::Code::LimitExceeded, "schema-object forward adjacency degree exceeds its limit");
        std::vector<Neighbor> neighbors;
        neighbors.reserve(end - begin);
        for (size_t index = begin; index < end; ++index)
            neighbors.push_back({.object = canonical_edges[index].dependency, .kind = canonical_edges[index].kind});
        forward_entries.push_back({.object = canonical_edges[begin].dependent, .adjacent = buildNeighborSet(neighbors)});
        begin = end;
    }
    auto reverse_edges = canonical_edges;
    std::sort(reverse_edges.begin(), reverse_edges.end(), reverseEdgeLess);
    std::vector<AdjacencyEntry> reverse_entries;
    begin = 0;
    while (begin < reverse_edges.size())
    {
        size_t end = begin + 1;
        while (end < reverse_edges.size() && reverse_edges[end].dependency == reverse_edges[begin].dependency)
            ++end;
        if (end - begin > limits.maximum_edges_per_node)
            graphError(Error::Code::LimitExceeded, "schema-object reverse adjacency degree exceeds its limit");
        std::vector<Neighbor> neighbors;
        neighbors.reserve(end - begin);
        for (size_t index = begin; index < end; ++index)
            neighbors.push_back({.object = reverse_edges[index].dependent, .kind = reverse_edges[index].kind});
        reverse_entries.push_back({.object = reverse_edges[begin].dependency, .adjacent = buildNeighborSet(neighbors)});
        begin = end;
    }

    auto storage = std::make_shared<SchemaObjectDependencyGraphStorage>();
    storage->node_set = buildSetTree(SetKind::Node, node_entries, 0, node_entries.size(), 0);
    storage->edge_set = buildSetTree(SetKind::Edge, edge_entries, 0, edge_entries.size(), 0);
    storage->forward = buildAdjacencyTree(forward_entries, 0, forward_entries.size(), 0);
    storage->reverse = buildAdjacencyTree(reverse_entries, 0, reverse_entries.size(), 0);
    const UInt64 node_count = canonical_nodes.size();
    const UInt64 edge_count = canonical_edges.size();
    const UInt64 accounted = graphLogicalBytes(*storage, node_count);
    if (accounted > limits.maximum_retained_bytes)
        graphError(Error::Code::LimitExceeded, "schema-object graph retained bytes exceed their limit");
    const Digest root = computeGraphRoot(
        database_uuid,
        node_count,
        storage->node_set ? storage->node_set->digest : emptySetDigest(SetKind::Node),
        edge_count,
        storage->edge_set ? storage->edge_set->digest : emptySetDigest(SetKind::Edge));
    return Ptr(new SchemaObjectDependencyGraph(
        database_uuid,
        limits,
        std::move(storage),
        node_count,
        edge_count,
        forward_entries.size(),
        reverse_entries.size(),
        accounted,
        root,
        std::move(canonical_nodes)));
}

std::span<const SchemaObjectID> SchemaObjectDependencyGraph::getNodes() const
{
    std::call_once(
        materialize_nodes_once,
        [&]
        {
            if (materialized_nodes.size() == node_count)
                return;
            materialized_nodes.clear();
            materialized_nodes.reserve(static_cast<size_t>(node_count));
            materializeSet(storage->node_set, materialized_nodes, nullptr);
        });
    return materialized_nodes;
}

UInt64 SchemaObjectDependencyGraph::getDependencyCount(const SchemaObjectID & dependent) const noexcept
{
    const auto * result = findAdjacency(storage->forward, dependent);
    return result ? result->count : 0;
}

UInt64 SchemaObjectDependencyGraph::getDependentCount(const SchemaObjectID & dependency) const noexcept
{
    const auto * result = findAdjacency(storage->reverse, dependency);
    return result ? result->count : 0;
}

std::span<const Neighbor> SchemaObjectDependencyGraph::getDependencies(const SchemaObjectID & dependent) const
{
    const auto * result = findAdjacency(storage->forward, dependent);
    return result ? result->getNeighbors() : std::span<const Neighbor>{};
}

std::span<const Neighbor> SchemaObjectDependencyGraph::getDependents(const SchemaObjectID & dependency) const
{
    const auto * result = findAdjacency(storage->reverse, dependency);
    return result ? result->getNeighbors() : std::span<const Neighbor>{};
}

bool SchemaObjectDependencyGraph::containsNode(const SchemaObjectID & node) const noexcept
{
    try
    {
        return findSetNode(storage->node_set, nodeKey(node)) != nullptr;
    }
    catch (...)
    {
        return false;
    }
}

bool SchemaObjectDependencyGraph::containsEdge(const Edge & edge) const noexcept
{
    try
    {
        return findSetNode(storage->edge_set, edgeKey(edge)) != nullptr;
    }
    catch (...)
    {
        return false;
    }
}

UInt64 SchemaObjectDependencyGraph::getMaximumForwardDegree() const noexcept
{
    return storage->forward ? storage->forward->maximum_degree : 0;
}

UInt64 SchemaObjectDependencyGraph::getMaximumReverseDegree() const noexcept
{
    return storage->reverse ? storage->reverse->maximum_degree : 0;
}

void SchemaObjectDependencyGraph::validateAgainstLimits(const Limits & candidate_limits) const
{
    validateLimits(candidate_limits);
    if (node_count > candidate_limits.maximum_nodes || edge_count > candidate_limits.maximum_edges)
        graphError(Error::Code::LimitExceeded, "schema-object graph count exceeds the candidate limit");
    if (getMaximumForwardDegree() > candidate_limits.maximum_edges_per_node
        || getMaximumReverseDegree() > candidate_limits.maximum_edges_per_node)
        graphError(Error::Code::LimitExceeded, "schema-object graph adjacency degree exceeds the candidate limit");
    if (accounted_bytes > candidate_limits.maximum_retained_bytes)
        graphError(Error::Code::LimitExceeded, "schema-object graph retained bytes exceed the candidate limit");
}

String SchemaObjectDependencyGraph::encodeSnapshot() const
{
    const UInt64 encoded_size = snapshotSize(node_count, edge_count);
    if (!std::in_range<size_t>(encoded_size))
        graphError(Error::Code::LimitExceeded, "schema-object snapshot exceeds the platform size domain");
    std::vector<Edge> edges;
    edges.reserve(static_cast<size_t>(edge_count));
    std::vector<SchemaObjectID> ignored;
    materializeSet(storage->edge_set, ignored, &edges);
    String result;
    result.reserve(static_cast<size_t>(encoded_size));
    appendUInt16LE(result, schema_object_dependency_graph_format_version);
    appendBytes(result, uuidToCanonicalBytes(database_uuid));
    appendVarUInt(result, node_count);
    for (const auto & node : getNodes())
        appendObjectID(result, node);
    appendVarUInt(result, edge_count);
    for (const auto & edge : edges)
    {
        appendObjectID(result, edge.dependent);
        appendObjectID(result, edge.dependency);
        result.push_back(static_cast<char>(encodeEdgeKind(edge.kind)));
    }
    return result;
}

SchemaObjectDependencyGraph::Ptr SchemaObjectDependencyGraph::decodeSnapshot(std::string_view bytes, const Limits & limits)
{
    validateLimits(limits);
    if (checkedSize(bytes.size(), "schema-object snapshot byte count does not fit UInt64")
        > snapshotSize(limits.maximum_nodes, limits.maximum_edges))
        graphError(Error::Code::LimitExceeded, "schema-object snapshot exceeds its maximum canonical size");
    Reader reader(bytes);
    if (reader.readUInt16LE() != schema_object_dependency_graph_format_version)
        graphError(Error::Code::UnsupportedVersion, "unsupported schema-object dependency graph snapshot version");
    const UUID database_uuid = uuidFromCanonicalBytes(reader.readArray<16>());
    validateDatabaseUUID(database_uuid);
    const UInt64 node_count = reader.readMinimalVarUInt(limits.maximum_nodes);
    if (!std::in_range<size_t>(node_count))
        graphError(Error::Code::LimitExceeded, "schema-object node count exceeds the platform size domain");
    reader.requireElements(static_cast<size_t>(node_count), static_cast<size_t>(wire_node_size));
    std::vector<SchemaObjectID> nodes;
    nodes.reserve(static_cast<size_t>(node_count));
    for (UInt64 index = 0; index < node_count; ++index)
    {
        auto node = readObjectID(reader, database_uuid);
        if (!nodes.empty() && !(nodes.back() < node))
            graphError(Error::Code::NonCanonical, "schema-object snapshot nodes are not strictly wire-sorted");
        nodes.push_back(std::move(node));
    }
    const UInt64 edge_count = reader.readMinimalVarUInt(limits.maximum_edges);
    if (!std::in_range<size_t>(edge_count))
        graphError(Error::Code::LimitExceeded, "schema-object edge count exceeds the platform size domain");
    reader.requireElements(static_cast<size_t>(edge_count), static_cast<size_t>(wire_edge_size));
    std::vector<Edge> edges;
    edges.reserve(static_cast<size_t>(edge_count));
    for (UInt64 index = 0; index < edge_count; ++index)
    {
        Edge edge{
            .dependent = readObjectID(reader, database_uuid),
            .dependency = readObjectID(reader, database_uuid),
            .kind = decodeEdgeKind(reader.readByte())};
        validateEdge(edge, database_uuid);
        if (!edges.empty() && !(edges.back() < edge))
            graphError(Error::Code::NonCanonical, "schema-object snapshot edges are not strictly wire-sorted");
        edges.push_back(std::move(edge));
    }
    reader.requireEnd();
    auto result = buildCanonical(database_uuid, std::move(nodes), std::move(edges), limits);
    if (result->encodeSnapshot() != bytes)
        graphError(Error::Code::NonCanonical, "schema-object snapshot does not round-trip canonically");
    return result;
}

SchemaObjectDependencyGraph::Ptr
SchemaObjectDependencyGraph::applyMutation(const Ptr & base, const SchemaObjectDependencyGraphMutation & mutation, Statistics * statistics)
{
    if (statistics)
        *statistics = {};
    if (!base)
        graphError(Error::Code::InvalidConfiguration, "schema-object graph mutation has no base root");
    const auto & limits = base->limits;
    const UInt64 node_delta_count
        = checkedAdd(mutation.node_additions.size(), mutation.node_removals.size(), "schema-object node mutation count overflow");
    const UInt64 edge_delta_count
        = checkedAdd(mutation.edge_additions.size(), mutation.edge_removals.size(), "schema-object edge mutation count overflow");
    if (node_delta_count > limits.maximum_mutation_nodes || edge_delta_count > limits.maximum_mutation_edges)
        graphError(Error::Code::LimitExceeded, "schema-object graph mutation count exceeds its limit");
    if (!node_delta_count && !edge_delta_count)
        return base;

    auto node_additions = canonicalizeNodes(mutation.node_additions, base->database_uuid, limits.maximum_mutation_nodes);
    auto node_removals = canonicalizeNodes(mutation.node_removals, base->database_uuid, limits.maximum_mutation_nodes);
    auto edge_additions = canonicalizeEdges(mutation.edge_additions, base->database_uuid, limits.maximum_mutation_edges);
    auto edge_removals = canonicalizeEdges(mutation.edge_removals, base->database_uuid, limits.maximum_mutation_edges);
    if (sortedSetsOverlap<SchemaObjectID>(node_additions, node_removals) || sortedSetsOverlap<Edge>(edge_additions, edge_removals))
        graphError(Error::Code::ConflictingMutation, "schema-object graph mutation adds and removes the same value");
    for (const auto & node : node_removals)
        if (!base->containsNode(node))
            graphError(Error::Code::MissingNode, "schema-object graph mutation removes an absent node");
    for (const auto & node : node_additions)
        if (base->containsNode(node))
            graphError(Error::Code::ExistingNode, "schema-object graph mutation adds an existing node");
    for (const auto & edge : edge_removals)
        if (!base->containsEdge(edge))
            graphError(Error::Code::MissingEdge, "schema-object graph mutation removes an absent edge");
    for (const auto & edge : edge_additions)
        if (base->containsEdge(edge))
            graphError(Error::Code::ExistingEdge, "schema-object graph mutation adds an existing edge");

    const auto nextContainsNode = [&](const SchemaObjectID & node)
    {
        if (std::binary_search(node_removals.begin(), node_removals.end(), node))
            return false;
        return base->containsNode(node) || std::binary_search(node_additions.begin(), node_additions.end(), node);
    };
    for (const auto & edge : edge_additions)
        if (!nextContainsNode(edge.dependent) || !nextContainsNode(edge.dependency))
            graphError(Error::Code::MissingNode, "schema-object graph edge addition has an absent post-mutation endpoint");

    const UInt64 next_node_count
        = checkedAdd(base->node_count - node_removals.size(), node_additions.size(), "schema-object next node count overflow");
    const UInt64 next_edge_count
        = checkedAdd(base->edge_count - edge_removals.size(), edge_additions.size(), "schema-object next edge count overflow");
    if (next_node_count > limits.maximum_nodes || next_edge_count > limits.maximum_edges)
        graphError(Error::Code::LimitExceeded, "schema-object graph mutation result exceeds its count limit");

    auto next_storage = std::make_shared<SchemaObjectDependencyGraphStorage>(*base->storage);
    for (const auto & edge : edge_removals)
    {
        next_storage->edge_set = removeSetEntry(SetKind::Edge, next_storage->edge_set, edgeKey(edge), statistics);
        if (statistics)
            ++statistics->edge_deltas_applied;
    }
    for (const auto & edge : edge_additions)
    {
        next_storage->edge_set
            = insertSetEntry(SetKind::Edge, next_storage->edge_set, {.key = edgeKey(edge), .node = {}, .edge = edge}, statistics);
        if (statistics)
            ++statistics->edge_deltas_applied;
    }

    std::map<SchemaObjectID, AdjDelta> forward_changes;
    std::map<SchemaObjectID, AdjDelta> reverse_changes;
    for (const auto & edge : edge_removals)
    {
        forward_changes[edge.dependent].removals.push_back({.object = edge.dependency, .kind = edge.kind});
        reverse_changes[edge.dependency].removals.push_back({.object = edge.dependent, .kind = edge.kind});
    }
    for (const auto & edge : edge_additions)
    {
        forward_changes[edge.dependent].additions.push_back({.object = edge.dependency, .kind = edge.kind});
        reverse_changes[edge.dependency].additions.push_back({.object = edge.dependent, .kind = edge.kind});
    }
    UInt64 next_forward_groups = base->forward_group_count;
    UInt64 next_reverse_groups = base->reverse_group_count;
    const auto apply_index = [&](AdjacencyNode::Ptr & index, std::map<SchemaObjectID, AdjDelta> & changes, UInt64 & group_count)
    {
        for (auto & [object, delta] : changes)
        {
            const auto * before = findAdjacency(index, object, statistics);
            auto after = applyNeighborDelta(before, delta, limits.maximum_edges_per_node, statistics);
            if (!before && after)
                ++group_count;
            if (before && !after)
                --group_count;
            if (!after)
                index = removeAdjacency(index, object, statistics);
            else
                index = upsertAdjacency(index, {.object = object, .adjacent = std::move(after)}, statistics);
        }
    };
    apply_index(next_storage->forward, forward_changes, next_forward_groups);
    apply_index(next_storage->reverse, reverse_changes, next_reverse_groups);

    for (const auto & node : node_removals)
    {
        if (findAdjacency(next_storage->forward, node, statistics) || findAdjacency(next_storage->reverse, node, statistics))
            graphError(Error::Code::MissingNode, "schema-object graph node removal leaves an incident edge");
        next_storage->node_set = removeSetEntry(SetKind::Node, next_storage->node_set, nodeKey(node), statistics);
        if (statistics)
            ++statistics->node_deltas_applied;
    }
    for (const auto & node : node_additions)
    {
        next_storage->node_set
            = insertSetEntry(SetKind::Node, next_storage->node_set, {.key = nodeKey(node), .node = node, .edge = {}}, statistics);
        if (statistics)
            ++statistics->node_deltas_applied;
    }

    const UInt64 accounted = graphLogicalBytes(*next_storage, next_node_count);
    if (accounted > limits.maximum_retained_bytes)
        graphError(Error::Code::LimitExceeded, "schema-object graph retained bytes exceed their limit");
    const Digest root = computeGraphRoot(
        base->database_uuid,
        next_node_count,
        next_storage->node_set ? next_storage->node_set->digest : emptySetDigest(SetKind::Node),
        next_edge_count,
        next_storage->edge_set ? next_storage->edge_set->digest : emptySetDigest(SetKind::Edge));
    return Ptr(new SchemaObjectDependencyGraph(
        base->database_uuid,
        limits,
        std::move(next_storage),
        next_node_count,
        next_edge_count,
        next_forward_groups,
        next_reverse_groups,
        accounted,
        root));
}

}
