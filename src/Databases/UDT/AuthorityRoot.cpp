#include <Databases/UDT/AuthorityRoot.h>
#include <Databases/UDT/AuthorityVerificationSchedule.h>

#include <Common/FailPoint.h>

#include <algorithm>
#include <array>
#include <limits>
#include <new>
#include <utility>

namespace DB::FailPoints
{
extern const char udt_authority_root_builder_allocation_failure[];
extern const char udt_authority_root_builder_freeze_failure[];
}

namespace DB::UDT
{

struct DefinitionResourceMaxima
{
    UInt64 canonical_definition_bytes = 0;
    UInt64 template_depth = 0;
    UInt64 logical_template_nodes = 0;
    UInt64 formal_parameters = 0;
    UInt64 direct_dependencies = 0;
    UInt64 transitive_dependencies = 0;
    UInt64 checker_work = 0;
    UInt64 verification_canonical_bytes = 0;
    UInt64 verification_work_units = 0;
    UInt64 verification_transient_bytes = 0;
    UInt64 verification_io_bytes = 0;

    void include(const DefinitionResourceMaxima & other) noexcept
    {
        canonical_definition_bytes = std::max(canonical_definition_bytes, other.canonical_definition_bytes);
        template_depth = std::max(template_depth, other.template_depth);
        logical_template_nodes = std::max(logical_template_nodes, other.logical_template_nodes);
        formal_parameters = std::max(formal_parameters, other.formal_parameters);
        direct_dependencies = std::max(direct_dependencies, other.direct_dependencies);
        transitive_dependencies = std::max(transitive_dependencies, other.transitive_dependencies);
        checker_work = std::max(checker_work, other.checker_work);
        verification_canonical_bytes = std::max(verification_canonical_bytes, other.verification_canonical_bytes);
        verification_work_units = std::max(verification_work_units, other.verification_work_units);
        verification_transient_bytes = std::max(verification_transient_bytes, other.verification_transient_bytes);
        verification_io_bytes = std::max(verification_io_bytes, other.verification_io_bytes);
    }
};

struct AuthorityRecordRadixNode
{
    using Ptr = std::shared_ptr<const AuthorityRecordRadixNode>;
    static constexpr size_t maximum_key_bytes = 2 * sizeof(CanonicalUUID) + sizeof(UInt64) + sizeof(UInt8);

    bool is_leaf = false;
    std::array<CanonicalByte, maximum_key_bytes> key{};
    UInt16 key_size = 0;
    Record definition_record;
    Definition::Ptr checked_definition;
    SidecarExpectationRecord expectation_record;
    UInt64 branch_depth = 0;
    std::vector<std::pair<UInt8, Ptr>> children;
    UInt64 subtree_count = 0;
    UInt64 subtree_logical_charge = 0;
    UInt64 subtree_materialized_logical_charge = 0;
    UInt64 subtree_canonical_bytes = 0;
    UInt64 subtree_maximum_canonical_bytes = 0;
    DefinitionResourceMaxima subtree_definition_resource_maxima;
    Digest digest{};
};

struct AuthorityRecordStore
{
    AuthorityRecordRadixNode::Ptr definitions;
    AuthorityRecordRadixNode::Ptr expectations;
    UInt64 definition_count = 0;
    UInt64 expectation_count = 0;
    UInt64 logical_charge = 0;
    UInt64 canonical_record_bytes = 0;
    UInt64 maximum_expectation_record_bytes = 0;
    DefinitionResourceMaxima definition_resource_maxima;
    Digest type_index_content_digest{};
    mutable std::once_flag materialize_definitions_once;
    mutable std::once_flag materialize_expectations_once;
    mutable std::vector<Record> materialized_definitions;
    mutable std::vector<SidecarExpectationRecord> materialized_expectations;
};

namespace
{

using RootError = AuthorityRootError;

constexpr std::string_view type_index_content_domain = "ClickHouse UDT in-memory type index content V1";
constexpr std::string_view definition_store_empty_domain = "ClickHouse UDT authority definition store empty V1";
constexpr std::string_view definition_store_leaf_domain = "ClickHouse UDT authority definition store leaf V1";
constexpr std::string_view definition_store_branch_domain = "ClickHouse UDT authority definition store branch V1";
constexpr std::string_view expectation_store_empty_domain = "ClickHouse UDT authority expectation store empty V1";
constexpr std::string_view expectation_store_leaf_domain = "ClickHouse UDT authority expectation store leaf V1";
constexpr std::string_view expectation_store_branch_domain = "ClickHouse UDT authority expectation store branch V1";
constexpr std::string_view physicalization_removal_keys_domain = "ClickHouse UDT authority physicalization removal keys V1";
constexpr std::string_view physicalization_graph_delta_domain = "ClickHouse UDT authority physicalization graph delta V1";
constexpr UInt64 maximum_record_radix_children_capacity = 32;

[[noreturn]] void fail(RootError::Code code, std::string_view message)
{
    throw RootError(code, message);
}

UInt64 toUInt64(size_t value, std::string_view message)
{
    if (!std::in_range<UInt64>(value))
        fail(RootError::Code::LimitExceeded, message);
    return static_cast<UInt64>(value);
}

UInt64 checkedAdd(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        fail(RootError::Code::LimitExceeded, message);
    return lhs + rhs;
}

UInt64 checkedMultiply(UInt64 lhs, UInt64 rhs, std::string_view message)
{
    if (lhs != 0 && rhs > std::numeric_limits<UInt64>::max() / lhs)
        fail(RootError::Code::LimitExceeded, message);
    return lhs * rhs;
}

void chargeLogical(UInt64 & total, UInt64 amount, std::string_view message)
{
    total = checkedAdd(total, amount, message);
}

void chargeLogicalString(UInt64 & total, const String & value, std::string_view message)
{
    chargeLogical(total, checkedAdd(toUInt64(value.capacity(), message), 1, message), message);
}

template <typename T>
void chargeLogicalVector(UInt64 & total, const std::vector<T> & value, std::string_view message)
{
    chargeLogical(total, checkedMultiply(toUInt64(value.capacity(), message), sizeof(T), message), message);
}

UInt64 recordNodeBaseLogicalCharge()
{
    return sizeof(AuthorityRecordRadixNode) + 2 * sizeof(void *);
}

void chargeCanonicalBytes(UInt64 & total, UInt64 amount, UInt64 maximum)
{
    if (amount > maximum || total > maximum - amount)
        fail(RootError::Code::LimitExceeded, "authority canonical record bytes exceed their limit");
    total += amount;
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
    if (const int database_result = compareUUID(lhs.database_uuid, rhs.database_uuid))
        return database_result;
    if (const int type_result = compareUUID(lhs.type_uuid, rhs.type_uuid))
        return type_result;
    if (lhs.revision == rhs.revision)
        return 0;
    return lhs.revision < rhs.revision ? -1 : 1;
}

bool definitionIdentityLess(const DefinitionIdentity & lhs, const DefinitionIdentity & rhs) noexcept
{
    return compareDefinitionIdentity(lhs, rhs) < 0;
}

bool expectationRecordLess(const SidecarExpectationRecord & lhs, const SidecarExpectationRecord & rhs) noexcept
{
    if (lhs.object != rhs.object)
        return lhs.object < rhs.object;
    return lhs.object_schema_revision < rhs.object_schema_revision;
}

enum class RecordStoreKind : UInt8
{
    Definition,
    Expectation,
};

using RecordNode = AuthorityRecordRadixNode;
using RecordNodePtr = RecordNode::Ptr;

struct RecordKey
{
    std::array<CanonicalByte, RecordNode::maximum_key_bytes> bytes{};
    UInt16 size = 0;
    bool operator==(const RecordKey &) const = default;
};

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

void appendUInt64BE(RecordKey & output, size_t offset, UInt64 value)
{
    for (size_t index = 0; index < sizeof(value); ++index)
        output.bytes[offset + index] = static_cast<CanonicalByte>(value >> (8 * (sizeof(value) - index - 1)));
}

void appendCanonicalBytes(String & output, std::span<const CanonicalByte> bytes)
{
    output.append(reinterpret_cast<const char *>(bytes.data()), bytes.size());
}

void appendVarUInt(String & output, UInt64 value)
{
    do
    {
        UInt8 byte = static_cast<UInt8>(value & 0x7f);
        value >>= 7;
        if (value)
            byte = static_cast<UInt8>(byte | 0x80);
        output.push_back(static_cast<char>(byte));
    } while (value);
}

void appendFrame(String & output, std::string_view value)
{
    appendVarUInt(output, toUInt64(value.size(), "authority record-store frame size does not fit UInt64"));
    output.append(value);
}

void appendSchemaObject(String & output, const SchemaObjectID & object)
{
    output.push_back(static_cast<char>(object.kind));
    appendCanonicalBytes(output, uuidToCanonicalBytes(object.database_uuid));
    appendCanonicalBytes(output, uuidToCanonicalBytes(object.object_uuid));
}

Digest physicalizationRemovalKeysDigest(std::span<const AuthorityInventoryKey> sorted_keys)
{
    String payload;
    appendUInt64LE(payload, sorted_keys.size());
    const AuthorityInventoryKey * previous = nullptr;
    for (const auto & key : sorted_keys)
    {
        if (key.format_version != authority_inventory_format_version || key.object_uuid == UUIDHelpers::Nil
            || (key.record_kind != AuthorityInventoryRecordKind::TypeDefinition
                && key.record_kind != AuthorityInventoryRecordKind::SidecarExpectation))
            fail(RootError::Code::InvalidRecord, "physicalization provenance contains an invalid authority key");
        if (previous && !authorityInventoryKeyLess(*previous, key))
            fail(RootError::Code::DuplicateRecordIdentity, "physicalization provenance authority keys are not strictly sorted");
        previous = &key;
        appendUInt16LE(payload, key.format_version);
        payload.push_back(static_cast<char>(key.record_kind));
        appendCanonicalBytes(payload, uuidToCanonicalBytes(key.object_uuid));
    }
    return hashFramedDomainSeparated(physicalization_removal_keys_domain, payload);
}

Digest physicalizationGraphDeltaDigest(const SchemaObjectDependencyGraphMutation & delta)
{
    auto node_additions = delta.node_additions;
    auto node_removals = delta.node_removals;
    auto edge_additions = delta.edge_additions;
    auto edge_removals = delta.edge_removals;
    std::sort(node_additions.begin(), node_additions.end());
    std::sort(node_removals.begin(), node_removals.end());
    std::sort(edge_additions.begin(), edge_additions.end());
    std::sort(edge_removals.begin(), edge_removals.end());

    String payload;
    const auto append_nodes = [&](const std::vector<SchemaObjectID> & nodes)
    {
        appendUInt64LE(payload, nodes.size());
        for (const auto & node : nodes)
            appendSchemaObject(payload, node);
    };
    const auto append_edges = [&](const std::vector<SchemaObjectDependencyEdge> & edges)
    {
        appendUInt64LE(payload, edges.size());
        for (const auto & edge : edges)
        {
            appendSchemaObject(payload, edge.dependent);
            appendSchemaObject(payload, edge.dependency);
            payload.push_back(static_cast<char>(edge.kind));
        }
    };
    append_nodes(node_additions);
    append_nodes(node_removals);
    append_edges(edge_additions);
    append_edges(edge_removals);
    return hashFramedDomainSeparated(physicalization_graph_delta_domain, payload);
}

RecordKey definitionRecordKey(const DefinitionIdentity & identity)
{
    RecordKey result;
    result.size = 2 * sizeof(CanonicalUUID) + sizeof(UInt64);
    const auto database = uuidToCanonicalBytes(identity.database_uuid);
    const auto type = uuidToCanonicalBytes(identity.type_uuid);
    std::copy(database.begin(), database.end(), result.bytes.begin());
    std::copy(type.begin(), type.end(), result.bytes.begin() + sizeof(CanonicalUUID));
    appendUInt64BE(result, 2 * sizeof(CanonicalUUID), identity.revision);
    return result;
}

RecordKey expectationRecordKey(const SchemaObjectID & object)
{
    RecordKey result;
    result.size = sizeof(UInt8) + 2 * sizeof(CanonicalUUID);
    result.bytes[0] = static_cast<CanonicalByte>(object.kind);
    const auto database = uuidToCanonicalBytes(object.database_uuid);
    const auto uuid = uuidToCanonicalBytes(object.object_uuid);
    std::copy(database.begin(), database.end(), result.bytes.begin() + 1);
    std::copy(uuid.begin(), uuid.end(), result.bytes.begin() + 1 + sizeof(CanonicalUUID));
    return result;
}

RecordKey nodeKey(const RecordNode & node)
{
    return {.bytes = node.key, .size = node.key_size};
}

UInt8 recordKeyNibble(const RecordKey & key, UInt64 depth) noexcept
{
    const UInt8 byte = key.bytes[depth / 2];
    return depth % 2 == 0 ? static_cast<UInt8>(byte >> 4) : static_cast<UInt8>(byte & 0x0f);
}

std::string_view recordEmptyDomain(RecordStoreKind kind)
{
    return kind == RecordStoreKind::Definition ? definition_store_empty_domain : expectation_store_empty_domain;
}

std::string_view recordLeafDomain(RecordStoreKind kind)
{
    return kind == RecordStoreKind::Definition ? definition_store_leaf_domain : expectation_store_leaf_domain;
}

std::string_view recordBranchDomain(RecordStoreKind kind)
{
    return kind == RecordStoreKind::Definition ? definition_store_branch_domain : expectation_store_branch_domain;
}

Digest emptyRecordDigest(RecordStoreKind kind)
{
    return hashFramedDomainSeparated(recordEmptyDomain(kind), std::string_view{});
}

struct RecordEntry
{
    RecordKey key;
    Record definition_record;
    Definition::Ptr checked_definition;
    SidecarExpectationRecord expectation_record;
    UInt64 logical_charge = 0;
    UInt64 canonical_bytes = 0;
};

DefinitionResourceMaxima definitionResourceMaxima(const Definition & definition)
{
    const auto & certificate = definition.getCertificate();
    return {
        .canonical_definition_bytes = checkedAdd(
            toUInt64(certificate.canonical_template_ir.size(), "canonical template IR bytes do not fit UInt64"),
            toUInt64(certificate.encoded_certificate.size(), "checker certificate bytes do not fit UInt64"),
            "canonical definition bytes overflow UInt64"),
        .template_depth = certificate.maximum_template_depth,
        .logical_template_nodes = certificate.logical_node_count,
        .formal_parameters = toUInt64(definition.getParameters().size(), "formal parameter count does not fit UInt64"),
        .direct_dependencies = toUInt64(definition.getDependencies().size(), "direct dependency count does not fit UInt64"),
        .transitive_dependencies = certificate.transitive_dependency_count,
        .checker_work = certificate.charged_work,
    };
}

UInt64 definitionVerificationTransientBytes(UInt64 encoded_record_bytes)
{
    UInt64 scratch = sizeof(Record);
    scratch = checkedAdd(
        scratch,
        checkedMultiply(encoded_record_bytes, 2, "definition verification string scratch overflows UInt64"),
        "definition verification scratch overflows UInt64");
    scratch = checkedAdd(
        scratch,
        checkedMultiply(
            checkedAdd(encoded_record_bytes, 1, "definition verification parameter count overflows UInt64") / 2,
            sizeof(Parameter),
            "definition verification parameter scratch overflows UInt64"),
        "definition verification scratch overflows UInt64");
    constexpr UInt64 minimum_dependency_bytes = sizeof(CanonicalUUID) + sizeof(UInt64) + sizeof(Digest);
    scratch = checkedAdd(
        scratch,
        checkedMultiply(
            checkedAdd(encoded_record_bytes, minimum_dependency_bytes - 1, "definition verification dependency count overflows UInt64")
                / minimum_dependency_bytes,
            sizeof(DefinitionDependency),
            "definition verification dependency scratch overflows UInt64"),
        "definition verification scratch overflows UInt64");
    return checkedAdd(encoded_record_bytes, scratch, "definition verification retained peak overflows UInt64");
}

Digest hashRecordLeaf(RecordStoreKind kind, const RecordEntry & entry)
{
    String payload;
    payload.append(reinterpret_cast<const char *>(entry.key.bytes.data()), entry.key.size);
    if (kind == RecordStoreKind::Definition)
    {
        appendCanonicalBytes(payload, entry.definition_record.definition_hash);
        appendFrame(payload, entry.definition_record.normalized_name);
        appendFrame(payload, entry.definition_record.normalized_local_name);
    }
    else
    {
        appendCanonicalBytes(payload, computeSidecarExpectationRecordHash(entry.expectation_record));
    }
    return hashFramedDomainSeparated(recordLeafDomain(kind), payload);
}

RecordNodePtr makeRecordLeaf(RecordStoreKind kind, RecordEntry entry)
{
    auto result = std::make_shared<RecordNode>();
    result->is_leaf = true;
    result->key = entry.key.bytes;
    result->key_size = entry.key.size;
    result->definition_record = std::move(entry.definition_record);
    result->checked_definition = std::move(entry.checked_definition);
    result->expectation_record = std::move(entry.expectation_record);
    result->subtree_count = 1;
    result->subtree_logical_charge
        = checkedAdd(recordNodeBaseLogicalCharge(), entry.logical_charge, "authority record-store leaf charge overflows UInt64");
    result->subtree_materialized_logical_charge = checkedAdd(
        kind == RecordStoreKind::Definition ? sizeof(Record) : sizeof(SidecarExpectationRecord),
        entry.logical_charge,
        "authority materialized record charge overflows UInt64");
    result->subtree_canonical_bytes = entry.canonical_bytes;
    result->subtree_maximum_canonical_bytes = entry.canonical_bytes;
    if (kind == RecordStoreKind::Definition)
    {
        if (!result->checked_definition)
            fail(RootError::Code::InvalidDefinition, "authority definition record has no checked definition");
        result->subtree_definition_resource_maxima = definitionResourceMaxima(*result->checked_definition);
        auto & maxima = result->subtree_definition_resource_maxima;
        maxima.verification_canonical_bytes = entry.canonical_bytes;
        maxima.verification_work_units = checkedAdd(
            checkedAdd(
                4,
                toUInt64(result->definition_record.parameters.size(), "definition parameter count does not fit UInt64"),
                "definition verification structural work overflows UInt64"),
            toUInt64(result->definition_record.dependencies.size(), "definition dependency count does not fit UInt64"),
            "definition verification structural work overflows UInt64");
        maxima.verification_transient_bytes = definitionVerificationTransientBytes(entry.canonical_bytes);
        maxima.verification_io_bytes = entry.canonical_bytes;
    }
    RecordEntry hash_entry{
        .key = entry.key,
        .definition_record = result->definition_record,
        .checked_definition = result->checked_definition,
        .expectation_record = result->expectation_record,
        .logical_charge = entry.logical_charge,
        .canonical_bytes = entry.canonical_bytes,
    };
    result->digest = hashRecordLeaf(kind, hash_entry);
    return result;
}

RecordNodePtr makeRecordBranch(RecordStoreKind kind, UInt64 depth, std::vector<std::pair<UInt8, RecordNodePtr>> children)
{
    if (children.size() < 2)
        fail(RootError::Code::InvalidRecord, "authority record-store radix branch is unary");
    std::sort(children.begin(), children.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });
    if (children.capacity() > maximum_record_radix_children_capacity)
        fail(RootError::Code::LimitExceeded, "authority record-store radix child capacity exceeds its implementation bound");
    auto result = std::make_shared<RecordNode>();
    result->key = children.front().second->key;
    result->key_size = children.front().second->key_size;
    result->branch_depth = depth;
    result->children = std::move(children);
    result->subtree_logical_charge = checkedAdd(
        recordNodeBaseLogicalCharge(),
        checkedMultiply(
            toUInt64(result->children.capacity(), "authority record-store child capacity does not fit UInt64"),
            sizeof(std::pair<UInt8, RecordNodePtr>),
            "authority record-store child charge overflows UInt64"),
        "authority record-store branch charge overflows UInt64");
    UInt16 present = 0;
    String payload;
    payload.reserve(2 * sizeof(UInt16) + result->children.size() * (sizeof(UInt8) + sizeof(Digest)));
    appendUInt16LE(payload, static_cast<UInt16>(depth));
    for (const auto & [nibble, child] : result->children)
    {
        if (!child || nibble >= 16 || (present & (UInt16{1} << nibble)))
            fail(RootError::Code::InvalidRecord, "authority record-store radix branch is invalid");
        present = static_cast<UInt16>(present | (UInt16{1} << nibble));
        result->subtree_count = checkedAdd(result->subtree_count, child->subtree_count, "authority record-store count overflows UInt64");
        result->subtree_logical_charge
            = checkedAdd(result->subtree_logical_charge, child->subtree_logical_charge, "authority record-store charge overflows UInt64");
        result->subtree_materialized_logical_charge = checkedAdd(
            result->subtree_materialized_logical_charge,
            child->subtree_materialized_logical_charge,
            "authority materialized record charge overflows UInt64");
        result->subtree_canonical_bytes = checkedAdd(
            result->subtree_canonical_bytes, child->subtree_canonical_bytes, "authority record-store canonical bytes overflow UInt64");
        result->subtree_maximum_canonical_bytes = std::max(result->subtree_maximum_canonical_bytes, child->subtree_maximum_canonical_bytes);
        result->subtree_definition_resource_maxima.include(child->subtree_definition_resource_maxima);
    }
    appendUInt16LE(payload, present);
    for (const auto & [nibble, child] : result->children)
    {
        payload.push_back(static_cast<char>(nibble));
        appendCanonicalBytes(payload, child->digest);
    }
    result->digest = hashFramedDomainSeparated(recordBranchDomain(kind), payload);
    return result;
}

RecordNodePtr buildRecordTree(RecordStoreKind kind, std::span<RecordEntry> entries, size_t begin, size_t end, UInt64 depth)
{
    if (begin == end)
        return {};
    if (end - begin == 1)
        return makeRecordLeaf(kind, std::move(entries[begin]));
    if (depth == static_cast<UInt64>(entries[begin].key.size) * 2)
        fail(RootError::Code::DuplicateRecordIdentity, "authority record-store contains a duplicate key");
    std::vector<std::pair<UInt8, RecordNodePtr>> children;
    size_t cursor = begin;
    while (cursor < end)
    {
        const UInt8 nibble = recordKeyNibble(entries[cursor].key, depth);
        const size_t child_begin = cursor;
        do
        {
            ++cursor;
        } while (cursor < end && recordKeyNibble(entries[cursor].key, depth) == nibble);
        children.emplace_back(nibble, buildRecordTree(kind, entries, child_begin, cursor, depth + 1));
    }
    if (children.size() == 1)
        return children.front().second;
    return makeRecordBranch(kind, depth, std::move(children));
}

const RecordNode * findRecordNode(const RecordNodePtr & root, const RecordKey & key) noexcept
{
    auto current = root;
    while (current)
    {
        if (current->is_leaf)
            return nodeKey(*current) == key ? current.get() : nullptr;
        const UInt8 nibble = recordKeyNibble(key, current->branch_depth);
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

RecordNodePtr removeRecordNode(RecordStoreKind kind, const RecordNodePtr & root, const RecordKey & key)
{
    if (!root)
        fail(RootError::Code::InvalidRecord, "authority record-store removal misses a record");
    if (root->is_leaf)
    {
        if (nodeKey(*root) != key)
            fail(RootError::Code::InvalidRecord, "authority record-store removal misses a record");
        return {};
    }
    const UInt8 nibble = recordKeyNibble(key, root->branch_depth);
    auto children = root->children;
    auto child
        = std::lower_bound(children.begin(), children.end(), nibble, [](const auto & value, UInt8 sought) { return value.first < sought; });
    if (child == children.end() || child->first != nibble)
        fail(RootError::Code::InvalidRecord, "authority record-store removal misses a branch");
    auto replacement = removeRecordNode(kind, child->second, key);
    if (replacement)
        child->second = std::move(replacement);
    else
        children.erase(child);
    if (children.size() == 1)
        return children.front().second;
    return makeRecordBranch(kind, root->branch_depth, std::move(children));
}

RecordNodePtr insertRecordNode(
    RecordStoreKind kind,
    const RecordNodePtr & root,
    const RecordNodePtr & addition,
    UInt64 minimum_depth,
    DependentObjectAdmissionDeltaStatistics * statistics)
{
    if (!root)
        return addition;
    if (statistics)
        ++statistics->expectation_record_nodes_visited;

    const auto root_key = nodeKey(*root);
    const auto addition_key = nodeKey(*addition);
    if (root_key.size != addition_key.size)
        fail(RootError::Code::InvalidRecord, "authority record-store insertion mixes key formats");

    const UInt64 maximum_depth = static_cast<UInt64>(root_key.size) * 2;
    UInt64 differing_depth = minimum_depth;
    while (differing_depth < maximum_depth && recordKeyNibble(root_key, differing_depth) == recordKeyNibble(addition_key, differing_depth))
        ++differing_depth;

    const UInt64 root_split_depth = root->is_leaf ? maximum_depth : root->branch_depth;
    if (differing_depth < root_split_depth)
    {
        if (statistics)
        {
            ++statistics->expectation_record_nodes_created;
            ++statistics->expectation_record_nodes_hashed;
        }
        return makeRecordBranch(
            kind,
            differing_depth,
            {
                {recordKeyNibble(root_key, differing_depth), root},
                {recordKeyNibble(addition_key, differing_depth), addition},
            });
    }
    if (root->is_leaf)
        fail(RootError::Code::DuplicateRecordIdentity, "authority record-store insertion duplicates a record");

    const UInt8 nibble = recordKeyNibble(addition_key, root->branch_depth);
    auto children = root->children;
    auto child
        = std::lower_bound(children.begin(), children.end(), nibble, [](const auto & value, UInt8 sought) { return value.first < sought; });
    if (child == children.end() || child->first != nibble)
        children.insert(child, {nibble, addition});
    else
        child->second = insertRecordNode(kind, child->second, addition, root->branch_depth + 1, statistics);

    if (statistics)
    {
        ++statistics->expectation_record_nodes_created;
        ++statistics->expectation_record_nodes_hashed;
    }
    return makeRecordBranch(kind, root->branch_depth, std::move(children));
}

void materializeDefinitionRecords(const RecordNodePtr & root, std::vector<Record> & output)
{
    if (!root)
        return;
    if (root->is_leaf)
    {
        output.push_back(root->definition_record);
        return;
    }
    for (const auto & [nibble, child] : root->children)
    {
        static_cast<void>(nibble);
        materializeDefinitionRecords(child, output);
    }
}

void materializeExpectationRecords(const RecordNodePtr & root, std::vector<SidecarExpectationRecord> & output)
{
    if (!root)
        return;
    if (root->is_leaf)
    {
        output.push_back(root->expectation_record);
        return;
    }
    for (const auto & [nibble, child] : root->children)
    {
        static_cast<void>(nibble);
        materializeExpectationRecords(child, output);
    }
}

UInt64 computeDefinitionLogicalCharge(const Record & record)
{
    UInt64 result = 0;
    chargeLogicalString(result, record.normalized_name, "authority definition record-name charge overflows UInt64");
    chargeLogicalString(result, record.normalized_local_name, "authority definition local-name charge overflows UInt64");
    chargeLogicalString(result, record.canonical_definition_sql, "authority definition SQL charge overflows UInt64");
    chargeLogicalString(result, record.canonical_physical_template_sql, "authority physical-template charge overflows UInt64");
    chargeLogicalString(result, record.canonical_template_ir, "authority template-IR charge overflows UInt64");
    chargeLogicalString(result, record.encoded_checker_certificate, "authority checker-certificate charge overflows UInt64");
    chargeLogicalString(result, record.owner_display_name, "authority owner-name charge overflows UInt64");
    chargeLogicalString(result, record.comment, "authority comment charge overflows UInt64");
    chargeLogicalVector(result, record.parameters, "authority parameter charge overflows UInt64");
    for (const auto & parameter : record.parameters)
        chargeLogicalString(result, parameter.normalized_name, "authority parameter-name charge overflows UInt64");
    chargeLogicalVector(result, record.dependencies, "authority dependency charge overflows UInt64");
    return result;
}

UInt64 computeRecordStoreLogicalCharge(const AuthorityRecordStore & records)
{
    UInt64 result = sizeof(AuthorityRecordStore) + 2 * sizeof(void *);
    const auto charge_tree = [&](const RecordNodePtr & root)
    {
        if (!root)
            return;
        chargeLogical(result, root->subtree_logical_charge, "authority record-store tree charge overflows UInt64");
        chargeLogical(result, root->subtree_materialized_logical_charge, "authority materialized record-store charge overflows UInt64");
    };
    charge_tree(records.definitions);
    charge_tree(records.expectations);
    return result;
}

Digest finalizeTypeIndexDigest(UInt64 definition_count, const RecordNodePtr & definitions)
{
    String payload;
    appendUInt64LE(payload, definition_count);
    appendCanonicalBytes(payload, definitions ? definitions->digest : emptyRecordDigest(RecordStoreKind::Definition));
    return hashFramedDomainSeparated(type_index_content_domain, payload);
}

std::shared_ptr<const AuthorityRecordStore> buildRecordStore(
    std::span<const Definition::Ptr> definitions,
    std::span<const Record> definition_records,
    std::span<const SidecarExpectationRecord> expectation_records,
    UInt64 canonical_record_bytes)
{
    std::vector<RecordEntry> definition_entries;
    definition_entries.reserve(definition_records.size());
    for (size_t index = 0; index < definition_records.size(); ++index)
    {
        definition_entries.push_back({
            .key = definitionRecordKey(definition_records[index].identity),
            .definition_record = definition_records[index],
            .checked_definition = definitions[index],
            .expectation_record = {},
            .logical_charge = computeDefinitionLogicalCharge(definition_records[index]),
            .canonical_bytes = toUInt64(encodeRecord(definition_records[index]).size(), "definition canonical bytes do not fit UInt64"),
        });
    }
    std::vector<RecordEntry> expectation_entries;
    expectation_entries.reserve(expectation_records.size());
    for (const auto & expectation : expectation_records)
    {
        const UInt64 encoded_bytes
            = toUInt64(encodeSidecarExpectationRecord(expectation).size(), "expectation canonical bytes do not fit UInt64");
        expectation_entries.push_back({
            .key = expectationRecordKey(expectation.object),
            .definition_record = {},
            .checked_definition = {},
            .expectation_record = expectation,
            .logical_charge = 0,
            .canonical_bytes = encoded_bytes,
        });
    }
    auto result = std::make_shared<AuthorityRecordStore>();
    result->definitions = buildRecordTree(RecordStoreKind::Definition, definition_entries, 0, definition_entries.size(), 0);
    result->expectations = buildRecordTree(RecordStoreKind::Expectation, expectation_entries, 0, expectation_entries.size(), 0);
    result->definition_count = definition_records.size();
    result->expectation_count = expectation_records.size();
    result->logical_charge = computeRecordStoreLogicalCharge(*result);
    result->canonical_record_bytes = checkedAdd(
        result->definitions ? result->definitions->subtree_canonical_bytes : 0,
        result->expectations ? result->expectations->subtree_canonical_bytes : 0,
        "authority record-store canonical bytes overflow UInt64");
    result->definition_resource_maxima
        = result->definitions ? result->definitions->subtree_definition_resource_maxima : DefinitionResourceMaxima{};
    result->maximum_expectation_record_bytes = result->expectations ? result->expectations->subtree_maximum_canonical_bytes : 0;
    if (result->canonical_record_bytes != canonical_record_bytes)
        fail(RootError::Code::InvalidRecord, "authority record-store canonical byte accounting differs from validation");
    result->type_index_content_digest = finalizeTypeIndexDigest(result->definition_count, result->definitions);
    return result;
}

std::shared_ptr<const AuthorityRecordStore> applyRecordRemovals(
    const std::shared_ptr<const AuthorityRecordStore> & base,
    std::span<const DefinitionIdentity> definition_removals,
    std::span<const SchemaObjectID> expectation_removals)
{
    auto result = std::make_shared<AuthorityRecordStore>();
    result->definitions = base->definitions;
    result->expectations = base->expectations;
    for (const auto & identity : definition_removals)
    {
        const auto key = definitionRecordKey(identity);
        if (!findRecordNode(result->definitions, key))
            fail(RootError::Code::InvalidRecord, "physicalization removes an absent definition record");
        result->definitions = removeRecordNode(RecordStoreKind::Definition, result->definitions, key);
    }
    for (const auto & object : expectation_removals)
    {
        const auto key = expectationRecordKey(object);
        if (!findRecordNode(result->expectations, key))
            fail(RootError::Code::InvalidRecord, "physicalization removes an absent expectation record");
        result->expectations = removeRecordNode(RecordStoreKind::Expectation, result->expectations, key);
    }
    result->definition_count = base->definition_count - definition_removals.size();
    result->expectation_count = base->expectation_count - expectation_removals.size();
    result->logical_charge = computeRecordStoreLogicalCharge(*result);
    result->canonical_record_bytes = checkedAdd(
        result->definitions ? result->definitions->subtree_canonical_bytes : 0,
        result->expectations ? result->expectations->subtree_canonical_bytes : 0,
        "authority record-store canonical bytes overflow UInt64");
    result->definition_resource_maxima
        = result->definitions ? result->definitions->subtree_definition_resource_maxima : DefinitionResourceMaxima{};
    result->maximum_expectation_record_bytes = result->expectations ? result->expectations->subtree_maximum_canonical_bytes : 0;
    result->type_index_content_digest = finalizeTypeIndexDigest(result->definition_count, result->definitions);
    return result;
}

std::shared_ptr<const AuthorityRecordStore> applyExpectationRecordAddition(
    const std::shared_ptr<const AuthorityRecordStore> & base,
    const SidecarExpectationRecord & expectation,
    DependentObjectAdmissionDeltaStatistics * statistics)
{
    if (findRecordNode(base->expectations, expectationRecordKey(expectation.object)))
        fail(RootError::Code::DuplicateRecordIdentity, "dependent-object admission expectation already exists");

    auto result = std::make_shared<AuthorityRecordStore>();
    result->definitions = base->definitions;
    RecordEntry entry{
        .key = expectationRecordKey(expectation.object),
        .definition_record = {},
        .checked_definition = {},
        .expectation_record = expectation,
        .logical_charge = 0,
        .canonical_bytes = toUInt64(encodeSidecarExpectationRecord(expectation).size(), "expectation canonical bytes do not fit UInt64"),
    };
    auto addition = makeRecordLeaf(RecordStoreKind::Expectation, std::move(entry));
    if (statistics)
    {
        statistics->expectation_record_deltas_applied = 1;
        ++statistics->expectation_record_nodes_created;
        ++statistics->expectation_record_nodes_hashed;
    }
    result->expectations = insertRecordNode(RecordStoreKind::Expectation, base->expectations, addition, 0, statistics);
    result->definition_count = base->definition_count;
    result->expectation_count = checkedAdd(base->expectation_count, 1, "authority expectation count overflows UInt64");
    result->logical_charge = computeRecordStoreLogicalCharge(*result);
    result->canonical_record_bytes = checkedAdd(
        result->definitions ? result->definitions->subtree_canonical_bytes : 0,
        result->expectations ? result->expectations->subtree_canonical_bytes : 0,
        "authority record-store canonical bytes overflow UInt64");
    result->definition_resource_maxima
        = result->definitions ? result->definitions->subtree_definition_resource_maxima : DefinitionResourceMaxima{};
    result->maximum_expectation_record_bytes = result->expectations ? result->expectations->subtree_maximum_canonical_bytes : 0;
    result->type_index_content_digest = base->type_index_content_digest;
    return result;
}

std::shared_ptr<const AuthorityRecordStore>
applyExpectationRecordReplacement(const std::shared_ptr<const AuthorityRecordStore> & base, const SidecarExpectationRecord & expectation)
{
    const auto key = expectationRecordKey(expectation.object);
    if (!findRecordNode(base->expectations, key))
        fail(RootError::Code::InvalidRecord, "dependent-object expectation replacement is absent");

    auto result = std::make_shared<AuthorityRecordStore>();
    result->definitions = base->definitions;
    auto without_old = removeRecordNode(RecordStoreKind::Expectation, base->expectations, key);
    RecordEntry entry{
        .key = key,
        .definition_record = {},
        .checked_definition = {},
        .expectation_record = expectation,
        .logical_charge = 0,
        .canonical_bytes = toUInt64(encodeSidecarExpectationRecord(expectation).size(), "expectation canonical bytes do not fit UInt64"),
    };
    result->expectations = insertRecordNode(
        RecordStoreKind::Expectation, std::move(without_old), makeRecordLeaf(RecordStoreKind::Expectation, std::move(entry)), 0, nullptr);
    result->definition_count = base->definition_count;
    result->expectation_count = base->expectation_count;
    result->logical_charge = computeRecordStoreLogicalCharge(*result);
    result->canonical_record_bytes = checkedAdd(
        result->definitions ? result->definitions->subtree_canonical_bytes : 0,
        result->expectations ? result->expectations->subtree_canonical_bytes : 0,
        "authority record-store canonical bytes overflow UInt64");
    result->definition_resource_maxima
        = result->definitions ? result->definitions->subtree_definition_resource_maxima : DefinitionResourceMaxima{};
    result->maximum_expectation_record_bytes = result->expectations ? result->expectations->subtree_maximum_canonical_bytes : 0;
    result->type_index_content_digest = base->type_index_content_digest;
    return result;
}

SchemaObjectID definitionObjectID(UUID database_uuid, UUID type_uuid)
{
    return SchemaObjectID{
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = database_uuid,
        .object_uuid = type_uuid,
    };
}

void validatePhysicalizationGraphDelta(
    std::span<const DefinitionIdentity> definition_removals,
    std::span<const SchemaObjectID> expectation_removals,
    const SchemaObjectDependencyGraphMutation & delta,
    const SchemaObjectDependencyGraph & after_graph)
{
    if (!delta.node_additions.empty() || !delta.edge_additions.empty())
        fail(RootError::Code::GraphMismatch, "physicalization graph delta contains an addition");

    std::vector<SchemaObjectID> expected_definition_node_removals;
    expected_definition_node_removals.reserve(definition_removals.size());
    for (const auto & identity : definition_removals)
        expected_definition_node_removals.push_back(definitionObjectID(identity.database_uuid, identity.type_uuid));
    std::sort(expected_definition_node_removals.begin(), expected_definition_node_removals.end());
    auto actual_node_removals = delta.node_removals;
    std::sort(actual_node_removals.begin(), actual_node_removals.end());
    if (std::adjacent_find(actual_node_removals.begin(), actual_node_removals.end()) != actual_node_removals.end())
        fail(RootError::Code::DuplicateRecordIdentity, "physicalization graph repeats a node removal");
    for (const auto & definition : expected_definition_node_removals)
    {
        if (!std::binary_search(actual_node_removals.begin(), actual_node_removals.end(), definition))
            fail(RootError::Code::GraphMismatch, "physicalization graph retains a removed definition node");
    }
    for (const auto & node : actual_node_removals)
    {
        if (!std::binary_search(expected_definition_node_removals.begin(), expected_definition_node_removals.end(), node)
            && !std::binary_search(expectation_removals.begin(), expectation_removals.end(), node))
        {
            fail(RootError::Code::GraphMismatch, "physicalization graph removes an unselected node");
        }
    }

    for (const auto & edge : delta.edge_removals)
    {
        if (edge.dependent.kind == SchemaObjectKind::TypeDefinition)
        {
            if (!std::binary_search(expected_definition_node_removals.begin(), expected_definition_node_removals.end(), edge.dependent))
                fail(RootError::Code::GraphMismatch, "physicalization removes an edge of a retained definition");
        }
        else
        {
            const bool selected_definition_edge = edge.kind == SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition
                && std::binary_search(expectation_removals.begin(), expectation_removals.end(), edge.dependent);
            const bool removed_object_edge = edge.kind == SchemaObjectDependencyEdgeKind::ObjectDependsOnObject
                && (std::binary_search(actual_node_removals.begin(), actual_node_removals.end(), edge.dependent)
                    || std::binary_search(actual_node_removals.begin(), actual_node_removals.end(), edge.dependency));
            if (!selected_definition_edge && !removed_object_edge)
                fail(RootError::Code::GraphMismatch, "physicalization removes an edge of an unselected dependent object");
        }
    }

    for (const auto & definition : expected_definition_node_removals)
        if (after_graph.containsNode(definition))
            fail(RootError::Code::GraphMismatch, "physicalization retains a removed definition graph node");
    for (const auto & object : expectation_removals)
    {
        const bool object_was_removed = std::binary_search(actual_node_removals.begin(), actual_node_removals.end(), object);
        if (object_was_removed == after_graph.containsNode(object))
            fail(RootError::Code::GraphMismatch, "physicalization dependent-object node disposition is inconsistent");
        if (object_was_removed)
            continue;
        for (const auto & dependency : after_graph.getDependencies(object))
            if (dependency.kind == SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition)
                fail(RootError::Code::GraphMismatch, "physicalization retains a selected object's definition edge");
    }
}

bool isValidRetainedDependentObjectEdge(
    const AuthorityRoot & base, const SchemaObjectID & dependent, const SchemaObjectDependencyEdge & edge)
{
    if (edge.dependent != dependent || edge.dependency.database_uuid != base.getDatabaseUUID())
        return false;
    if (edge.kind == SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition)
    {
        return edge.dependency.kind == SchemaObjectKind::TypeDefinition
            && base.getSchemaObjectDependencyGraph().containsNode(edge.dependency);
    }
    if (edge.kind != SchemaObjectDependencyEdgeKind::ObjectDependsOnObject)
        return false;

    const bool ordinary_object = edge.dependency.kind == SchemaObjectKind::Table || edge.dependency.kind == SchemaObjectKind::View
        || edge.dependency.kind == SchemaObjectKind::Dictionary;
    return ordinary_object && edge.dependency != dependent && base.getSchemaObjectDependencyGraph().containsNode(edge.dependency)
        && base.findExpectationRecord(edge.dependency);
}

void validateDependentObjectAdmissionGraphDelta(
    const AuthorityRoot & base,
    const SidecarExpectationRecord & expectation,
    const SchemaObjectDependencyGraphMutation & delta,
    const SchemaObjectDependencyGraph & after_graph)
{
    if (delta.node_additions != std::vector<SchemaObjectID>{expectation.object} || !delta.node_removals.empty()
        || !delta.edge_removals.empty() || delta.edge_additions.empty())
        fail(RootError::Code::GraphMismatch, "dependent-object admission graph delta is not one create-only object addition");

    bool has_definition_dependency = false;
    for (const auto & edge : delta.edge_additions)
    {
        if (!isValidRetainedDependentObjectEdge(base, expectation.object, edge))
            fail(RootError::Code::GraphMismatch, "dependent-object admission graph edge does not target retained authority content");
        has_definition_dependency |= edge.kind == SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition;
        if (!after_graph.containsEdge(edge))
            fail(RootError::Code::GraphMismatch, "dependent-object admission graph omitted an added dependency edge");
    }
    if (!has_definition_dependency)
        fail(RootError::Code::GraphMismatch, "dependent-object admission graph omitted its definition dependency");
    if (!after_graph.containsNode(expectation.object))
        fail(RootError::Code::GraphMismatch, "dependent-object admission graph omitted its object node");
}

void validateSchemaGraphContents(
    const AuthorityState & authority_state,
    std::span<const Definition::Ptr> definitions,
    std::span<const SidecarExpectationRecord> expectations,
    const SchemaObjectDependencyGraph & graph)
{
    std::vector<SchemaObjectID> expected_definition_nodes;
    std::vector<SchemaObjectDependencyEdge> expected_definition_edges;
    expected_definition_nodes.reserve(definitions.size());
    for (const auto & definition : definitions)
    {
        const SchemaObjectID dependent = definitionObjectID(authority_state.database_uuid, definition->getIdentity().type_uuid);
        expected_definition_nodes.push_back(dependent);
        for (const auto & dependency : definition->getDependencies())
        {
            expected_definition_edges.push_back({
                .dependent = dependent,
                .dependency = definitionObjectID(authority_state.database_uuid, dependency.type_uuid),
                .kind = SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition,
            });
        }
    }
    std::sort(expected_definition_nodes.begin(), expected_definition_nodes.end());
    std::sort(expected_definition_edges.begin(), expected_definition_edges.end());

    std::vector<SchemaObjectID> actual_definition_nodes;
    std::vector<SchemaObjectDependencyEdge> actual_definition_edges;
    for (const auto & node : graph.getNodes())
    {
        if (node.kind != SchemaObjectKind::TypeDefinition)
            continue;
        actual_definition_nodes.push_back(node);
        for (const auto & dependency : graph.getDependencies(node))
        {
            if (dependency.kind == SchemaObjectDependencyEdgeKind::DefinitionDependsOnDefinition)
            {
                actual_definition_edges.push_back({
                    .dependent = node,
                    .dependency = dependency.object,
                    .kind = dependency.kind,
                });
            }
        }
    }
    if (actual_definition_nodes != expected_definition_nodes || actual_definition_edges != expected_definition_edges)
        fail(RootError::Code::GraphMismatch, "schema graph does not exactly represent checked definition dependencies");

    const bool dependent_object_capable = authority_state.persistent_capability_mask == dependent_object_authority_capability_mask;
    if (!dependent_object_capable)
    {
        if (!expectations.empty())
            fail(RootError::Code::InvalidAuthorityState, "definition-only authority contains a sidecar expectation");
        if (graph.getNodes().size() != actual_definition_nodes.size() || graph.getEdgeCount() != actual_definition_edges.size())
            fail(RootError::Code::GraphMismatch, "definition-only authority graph contains a dependent object");
        return;
    }

    for (const auto & expectation : expectations)
    {
        if (!graph.containsNode(expectation.object))
            fail(RootError::Code::GraphMismatch, "sidecar expectation object is absent from the schema graph");
        const auto dependencies = graph.getDependencies(expectation.object);
        const bool has_definition_dependency = std::any_of(
            dependencies.begin(),
            dependencies.end(),
            [](const auto & dependency) { return dependency.kind == SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition; });
        if (!has_definition_dependency)
            fail(RootError::Code::GraphMismatch, "sidecar expectation object has no definition dependency");
    }

    const auto has_expectation = [&](const SchemaObjectID & object)
    {
        const auto found = std::lower_bound(
            expectations.begin(),
            expectations.end(),
            object,
            [](const SidecarExpectationRecord & expectation, const SchemaObjectID & candidate) { return expectation.object < candidate; });
        return found != expectations.end() && found->object == object;
    };
    for (const auto & node : graph.getNodes())
    {
        if (node.kind == SchemaObjectKind::TypeDefinition)
            continue;
        const auto dependencies = graph.getDependencies(node);
        const bool has_definition_dependency = std::any_of(
            dependencies.begin(),
            dependencies.end(),
            [](const auto & dependency) { return dependency.kind == SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition; });
        if (has_definition_dependency && !has_expectation(node))
            fail(RootError::Code::GraphMismatch, "schema object with a definition dependency has no sidecar expectation");
    }
}

void validateAuthorityState(const AuthorityState & state, const AuthorityStateLimits & limits)
{
    try
    {
        static_cast<void>(encodeAuthorityState(state, limits));
    }
    catch (const AuthorityStateError & error)
    {
        if (error.code == AuthorityStateError::Code::LimitExceeded)
            fail(RootError::Code::LimitExceeded, "authority state exceeds its limit");
        fail(RootError::Code::InvalidAuthorityState, "authority state is not a canonical complete V1 state");
    }
}

String validateDefinitionRecord(const Record & record, const RecordLimits & limits)
{
    try
    {
        return encodeRecord(record, limits);
    }
    catch (const RecordError & error)
    {
        if (error.code == RecordError::Code::LimitExceeded)
            fail(RootError::Code::LimitExceeded, "definition record exceeds its limit");
        fail(RootError::Code::InvalidRecord, "definition record is not canonical V1");
    }
}

String validateExpectationRecord(const SidecarExpectationRecord & record)
{
    try
    {
        return encodeSidecarExpectationRecord(record);
    }
    catch (const SidecarExpectationRecordError &)
    {
        fail(RootError::Code::InvalidRecord, "sidecar expectation record is not canonical V1");
    }
}

AuthorityInventory::Ptr buildInventory(std::vector<AuthorityInventoryLeaf> sorted_leaves, const AuthorityInventoryLimits & limits)
{
    try
    {
        const AuthorityInventorySummary summary = buildAuthorityInventorySummary(sorted_leaves, limits);
        return AuthorityInventory::create(summary, std::move(sorted_leaves), limits);
    }
    catch (const AuthorityInventoryError & error)
    {
        if (error.code == AuthorityInventoryError::Code::LimitExceeded)
            fail(RootError::Code::LimitExceeded, "authority inventory exceeds its limit");
        fail(RootError::Code::InvalidRecord, "authority inventory records are not canonical V1");
    }
}

TypeCatalogRoot::Ptr
buildTypeCatalog(UUID database_uuid, UInt64 generation, std::span<const Definition::Ptr> definitions, const TypeCatalogBuildLimits & limits)
{
    try
    {
        return TypeCatalogBuilder::build(database_uuid, generation, definitions, limits);
    }
    catch (const CatalogError & error)
    {
        if (error.code == CatalogError::Code::LimitExceeded)
            fail(RootError::Code::LimitExceeded, "type index exceeds its limit");
        if (error.code == CatalogError::Code::InvalidConfiguration)
            fail(RootError::Code::InvalidConfiguration, "type-index limits are invalid");
        fail(RootError::Code::InvalidDefinition, "checked definitions cannot form one immutable type index");
    }
}

[[noreturn]] void failResourceUsageIndex(const AuthorityResourceUsageIndexError & error, std::string_view operation)
{
    String diagnostic(operation);
    diagnostic.append(": ");
    diagnostic.append(error.what());
    switch (error.code)
    {
        case AuthorityResourceUsageIndexError::Code::InvalidConfiguration: fail(RootError::Code::InvalidConfiguration, diagnostic);
        case AuthorityResourceUsageIndexError::Code::LimitExceeded: fail(RootError::Code::LimitExceeded, diagnostic);
        case AuthorityResourceUsageIndexError::Code::DatabaseMismatch: fail(RootError::Code::DatabaseMismatch, diagnostic);
        case AuthorityResourceUsageIndexError::Code::DefinitionMismatch: fail(RootError::Code::RecordDefinitionMismatch, diagnostic);
        case AuthorityResourceUsageIndexError::Code::ExpectationMismatch:
        case AuthorityResourceUsageIndexError::Code::SidecarMismatch:
        case AuthorityResourceUsageIndexError::Code::DuplicateObject:
        case AuthorityResourceUsageIndexError::Code::MissingObject:
        case AuthorityResourceUsageIndexError::Code::InvalidInput: fail(RootError::Code::InvalidRecord, diagnostic);
    }
    fail(RootError::Code::InvalidRecord, diagnostic);
}

AuthorityResourceUsageIndex::Ptr buildResourceUsageIndex(
    UUID database_uuid,
    std::span<const SidecarExpectationRecord> expectations,
    std::span<const AuthorityDependentObjectResourceImage> dependent_objects,
    const TypeCatalogRoot & catalog,
    const AuthorityResourceUsageIndexLimits & limits)
{
    try
    {
        return AuthorityResourceUsageIndex::build(database_uuid, expectations, dependent_objects, catalog, limits);
    }
    catch (const AuthorityResourceUsageIndexError & error)
    {
        failResourceUsageIndex(error, "authority resource-usage index cannot be reconstructed from exact sidecars");
    }
}

AuthorityResourceUsageIndex::Ptr removeResourceUsageObjects(
    const AuthorityResourceUsageIndex::Ptr & base,
    std::span<const SchemaObjectID> objects,
    const AuthorityResourceUsageIndexLimits & limits)
{
    try
    {
        return AuthorityResourceUsageIndex::removeObjects(base, objects, limits);
    }
    catch (const AuthorityResourceUsageIndexError & error)
    {
        failResourceUsageIndex(error, "authority resource-usage removal is inconsistent with its exact base");
    }
}

AuthorityResourceUsageIndex::Ptr addResourceUsageObject(
    const AuthorityResourceUsageIndex::Ptr & base,
    const SidecarExpectationRecord & expectation,
    const AuthorityDependentObjectResourceImage & dependent_object,
    const TypeCatalogRoot & catalog,
    const AuthorityResourceUsageIndexLimits & limits)
{
    try
    {
        return AuthorityResourceUsageIndex::addObject(base, expectation, dependent_object, catalog, limits);
    }
    catch (const AuthorityResourceUsageIndexError & error)
    {
        failResourceUsageIndex(error, "authority resource-usage addition is inconsistent with its exact sidecar");
    }
}

AuthorityResourceUsageIndex::Ptr replaceResourceUsageObject(
    const AuthorityResourceUsageIndex::Ptr & base,
    const SidecarExpectationRecord & expectation,
    const AuthorityDependentObjectResourceImage & dependent_object,
    const TypeCatalogRoot & catalog,
    const AuthorityResourceUsageIndexLimits & limits)
{
    try
    {
        return AuthorityResourceUsageIndex::replaceObject(base, expectation, dependent_object, catalog, limits);
    }
    catch (const AuthorityResourceUsageIndexError & error)
    {
        failResourceUsageIndex(error, "authority resource-usage replacement is inconsistent with its exact sidecar");
    }
}

EffectiveResourceLimits makeDefaultDatabaseResourceQuotaLimits()
{
    try
    {
        const std::array<ResourceLimitLayer, 1> layers{makeDatabaseDefaultResourceLimitLayer()};
        return calculateEffectiveResourceLimits(layers);
    }
    catch (const ResourceLimitError &)
    {
        fail(RootError::Code::InvalidConfiguration, "default database resource-quota limits are invalid");
    }
}

[[noreturn]] void failDeterministicCatalogCharge(const DeterministicCatalogChargeResult & result)
{
    String message = "authority deterministic catalog charge is invalid";
    if (result.component)
    {
        message.append(" at component '");
        message.append(deterministicCatalogChargeComponentName(*result.component));
        message.push_back('\'');
    }
    throw RootError(
        result.status == DeterministicCatalogChargeStatus::InvalidMaximum ? RootError::Code::InvalidConfiguration
                                                                          : RootError::Code::LimitExceeded,
        message);
}

ResourceUsage computeDatabaseOwnedResourceUsage(
    const TypeCatalogRoot & catalog,
    const AuthorityInventory & inventory,
    const AuthorityRecordStore & records,
    const SchemaObjectDependencyGraph & graph,
    const AuthorityResourceUsageIndex & resource_usage_index,
    const AuthorityVerificationSchedulePolicy & verification_policy,
    UInt64 verification_maximum_targets_per_batch)
{
    if (catalog.getDefinitionCount() != records.definition_count)
        fail(RootError::Code::InvalidRecord, "authority catalog and record-store definition counts differ");
    if (inventory.getSummary().leaf_count
        != checkedAdd(records.definition_count, records.expectation_count, "authority record count overflows UInt64"))
        fail(RootError::Code::InventoryMismatch, "authority inventory and record-store counts differ");

    const auto & rooted_usage = resource_usage_index.getSummary();
    if (resource_usage_index.getDatabaseUUID() != catalog.getDatabaseUUID() || rooted_usage.object_count != records.expectation_count)
        fail(RootError::Code::InvalidRecord, "authority resource-usage index differs from its expectation store");

    const DeterministicCatalogChargeInput charge_input{
        .canonical_record_bytes = records.canonical_record_bytes,
        .definitions = records.definition_count,
        .specializations = rooted_usage.unique_persisted_specializations,
        .dependency_edges = graph.getEdgeCount(),
        .occurrence_paths = rooted_usage.total_occurrence_paths,
        .inventory_nodes = inventory.getNodeCount(),
        .inventory_leaves = inventory.getSummary().leaf_count,
    };
    const auto charge = calculateDeterministicCatalogCharge(
        charge_input, getResourceImplementationLimits().get(ResourceLimit::DeterministicCatalogBytesPerDatabase));
    if (!charge.isAccepted())
        failDeterministicCatalogCharge(charge);

    ResourceUsage usage;
    const auto & definition_maxima = records.definition_resource_maxima;
    usage.set(ResourceLimit::CanonicalDefinitionBytes, definition_maxima.canonical_definition_bytes);
    usage.set(ResourceLimit::TemplateDepth, definition_maxima.template_depth);
    usage.set(ResourceLimit::LogicalTemplateNodes, definition_maxima.logical_template_nodes);
    usage.set(ResourceLimit::FormalParameters, definition_maxima.formal_parameters);
    usage.set(ResourceLimit::DirectDependencies, definition_maxima.direct_dependencies);
    usage.set(ResourceLimit::TransitiveDependencies, definition_maxima.transitive_dependencies);
    usage.set(ResourceLimit::CheckerExpansionWorkUnits, definition_maxima.checker_work);
    usage.set(ResourceLimit::LoweredPhysicalTypeNodes, rooted_usage.maximum_lowered_physical_type_nodes);
    usage.set(ResourceLimit::CanonicalActualArgumentBytes, rooted_usage.maximum_canonical_argument_bytes);
    usage.set(ResourceLimit::DefinitionsPerDatabase, records.definition_count);
    usage.set(ResourceLimit::PersistedSpecializationsPerTemplate, rooted_usage.maximum_persisted_specializations_per_template);
    usage.set(ResourceLimit::OccurrencePathsPerObject, rooted_usage.maximum_occurrence_paths_per_object);
    usage.set(ResourceLimit::SidecarBytesPerObject, rooted_usage.maximum_sidecar_bytes_per_object);
    usage.set(ResourceLimit::DurableDependentObjectBytesPerDatabase, rooted_usage.total_durable_dependent_object_bytes);
    usage.set(ResourceLimit::DeterministicCatalogBytesPerDatabase, charge.charged_bytes);
    usage.set(ResourceLimit::VerificationTargetsPerDatabase, inventory.getSummary().leaf_count);
    AuthorityVerificationPlanningRequirements planning;
    try
    {
        planning = computeAuthorityVerificationPlanningRequirements(
            inventory.getSummary().leaf_count, verification_policy, verification_maximum_targets_per_batch);
    }
    catch (const AuthorityVerificationScheduleError & error)
    {
        fail(
            error.code == AuthorityVerificationScheduleError::Code::ArithmeticOverflow
                    || error.code == AuthorityVerificationScheduleError::Code::LimitExceeded
                ? RootError::Code::LimitExceeded
                : RootError::Code::InvalidConfiguration,
            "authority root cannot derive its deterministic verification-planning requirements");
    }

    if (inventory.getSummary().leaf_count != 0)
    {
        constexpr AuthorityVerificationScheduleLimits implementation_schedule;

        UInt64 verification_canonical_bytes = definition_maxima.verification_canonical_bytes;
        UInt64 verification_work_units = definition_maxima.verification_work_units;
        UInt64 verification_transient_bytes = definition_maxima.verification_transient_bytes;
        UInt64 verification_io_bytes = definition_maxima.verification_io_bytes;
        if (rooted_usage.object_count != 0)
        {
            /// A dependent-object target is admitted through the complete
            /// admission verifier. Its canonical/work/transient envelope is indivisible;
            /// I/O remains the exact largest rooted durable object image plus
            /// its fixed expectation record.
            verification_canonical_bytes
                = std::max(verification_canonical_bytes, implementation_schedule.maximum_rooted_target_canonical_bytes);
            verification_work_units
                = std::max(verification_work_units, implementation_schedule.maximum_rooted_target_verification_work_units);
            verification_transient_bytes
                = std::max(verification_transient_bytes, implementation_schedule.maximum_rooted_target_transient_bytes);
            verification_io_bytes = std::max(
                verification_io_bytes,
                checkedAdd(
                    records.maximum_expectation_record_bytes,
                    rooted_usage.maximum_durable_dependent_object_bytes_per_object,
                    "authority rooted verification I/O requirement overflows UInt64"));
        }
        usage.set(ResourceLimit::VerificationBucketsPerDatabase, verification_policy.bucket_count);
        usage.set(ResourceLimit::VerificationCanonicalBytesPerBatch, verification_canonical_bytes);
        usage.set(ResourceLimit::VerificationWorkUnitsPerBatch, verification_work_units);
        usage.set(ResourceLimit::VerificationTransientBytesPerBatch, verification_transient_bytes);
        usage.set(ResourceLimit::VerificationIOBytesPerBatch, verification_io_bytes);
        usage.set(ResourceLimit::VerificationPlannerWorkUnitsPerBatch, planning.planner_work_units);
        usage.set(ResourceLimit::VerificationPlannerScratchBytesPerBatch, planning.planner_scratch_bytes);
        usage.set(ResourceLimit::VerificationRetainedBytesPerBatch, planning.retained_canonical_bytes);
    }
    return usage;
}

ResourceDelta makeExactResourceDelta(const ResourceUsage & base, const ResourceUsage & replacement) noexcept
{
    ResourceDelta result;
    for (std::size_t index = 0; index < resource_limit_count; ++index)
    {
        const auto limit = static_cast<ResourceLimit>(index);
        const UInt64 before = base.get(limit);
        const UInt64 after = replacement.get(limit);
        if (after > before)
            result.add(limit, after - before);
        else if (before > after)
            result.remove(limit, before - after);
    }
    return result;
}

[[noreturn]] void failResourceAdmission(const ResourceAdmissionResult & result)
{
    throw RootError(RootError::Code::LimitExceeded, formatResourceAdmissionFailure(result));
}

DatabaseResourceQuotaSnapshot::Ptr makeInitialDatabaseResourceQuota(
    const ResourceUsage & usage, bool require_within_quota, const EffectiveResourceLimits * exact_effective_limits)
{
    auto limits = exact_effective_limits ? *exact_effective_limits : makeDefaultDatabaseResourceQuotaLimits();
    if (require_within_quota)
    {
        const ResourceUsage empty_usage;
        const ResourceDelta delta = makeExactResourceDelta(empty_usage, usage);
        const auto admission = evaluateResourceAdmission(empty_usage, delta, limits);
        if (!admission.isAccepted())
            failResourceAdmission(admission);
    }

    auto result = DatabaseResourceQuotaTransitionBuilder::makeInitial(std::move(limits), usage);
    if (!result || result->getUsage() != usage || (require_within_quota && result->getState() != DatabaseResourceQuotaState::Active))
        fail(RootError::Code::InvalidAuthorityState, "initial database resource-quota image is inconsistent");
    return result;
}

DatabaseResourceQuotaSnapshot::Ptr prepareDatabaseResourceQuotaReplacement(
    DatabaseResourceQuotaSnapshot::Ptr base, const ResourceUsage & exact_base_usage, const ResourceUsage & exact_replacement_usage)
{
    if (!base || base->getUsage() != exact_base_usage)
        fail(RootError::Code::InvalidAuthorityState, "database resource-quota image differs from its authority root");

    /// The exact effective tuple is part of the immutable root payload. An
    /// ordinary content successor cannot silently return to defaults or adopt
    /// a newly configured policy; only the explicit quota-policy transition is
    /// allowed to replace it.
    auto next_limits = base->getLimits();

    auto prepared = DatabaseResourceQuotaTransitionBuilder::prepareReplacement(
        std::move(base), std::move(next_limits), makeExactResourceDelta(exact_base_usage, exact_replacement_usage));
    switch (prepared.getStatus())
    {
        case DatabaseResourceQuotaPreparationStatus::Prepared:
            if (!prepared.getReplacement() || prepared.getReplacement()->getUsage() != exact_replacement_usage)
                fail(RootError::Code::InvalidAuthorityState, "prepared database resource-quota replacement is inconsistent");
            return prepared.getReplacement();
        case DatabaseResourceQuotaPreparationStatus::InvalidBase:
            fail(RootError::Code::InvalidAuthorityState, "database resource-quota replacement lost its base image");
        case DatabaseResourceQuotaPreparationStatus::RevisionOverflow:
            fail(RootError::Code::LimitExceeded, "database resource-quota revision overflows UInt64");
        case DatabaseResourceQuotaPreparationStatus::ResourceRejected: failResourceAdmission(prepared.getResourceAdmission());
    }
    fail(RootError::Code::InvalidAuthorityState, "unknown database resource-quota preparation status");
}

UInt64 computeContentPayloadLogicalCharge(
    const TypeCatalogRoot & catalog,
    const AuthorityInventory & inventory,
    const AuthorityRecordStore & records,
    const SchemaObjectDependencyGraph & graph,
    const AuthorityResourceUsageIndex & resource_usage_index,
    const DatabaseResourceQuotaSnapshot & database_resource_quota)
{
    UInt64 logical_charge = AuthorityRoot::getContentPayloadBaseLogicalCharge();
    chargeLogical(logical_charge, catalog.getAccountedBytes(), "authority content-payload catalog charge overflows UInt64");
    chargeLogical(logical_charge, inventory.getAccountedBytes(), "authority content-payload inventory charge overflows UInt64");
    chargeLogical(logical_charge, graph.getAccountedBytes(), "authority content-payload graph charge overflows UInt64");
    chargeLogical(logical_charge, records.logical_charge, "authority content-payload record-store charge overflows UInt64");
    chargeLogical(
        logical_charge, resource_usage_index.getAccountedBytes(), "authority content-payload resource-index charge overflows UInt64");
    chargeLogical(
        logical_charge,
        database_resource_quota.getLogicalCharge(),
        "authority content-payload database resource-quota charge overflows UInt64");
    return logical_charge;
}

}

AuthorityRootError::AuthorityRootError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

AuthorityRoot::AuthorityRoot(AuthorityState authority_state_, ContentPayloadPtr content_payload_)
    : authority_state(std::move(authority_state_))
    , content_payload(std::move(content_payload_))
{
}

AuthorityRoot::~AuthorityRoot() = default;

Definition::Ptr AuthorityRoot::findByIdentity(const DefinitionIdentity & identity) const
{
    return content_payload->type_catalog->findByIdentity(identity);
}

Definition::Ptr AuthorityRoot::findByName(std::string_view normalized_local_name) const
{
    return content_payload->type_catalog->findByName(normalized_local_name);
}

std::span<const Record> AuthorityRoot::getDefinitionRecords() const
{
    auto & records = *content_payload->records;
    std::call_once(
        records.materialize_definitions_once,
        [&]
        {
            if (records.materialized_definitions.size() == records.definition_count)
                return;
            records.materialized_definitions.clear();
            records.materialized_definitions.reserve(static_cast<size_t>(records.definition_count));
            materializeDefinitionRecords(records.definitions, records.materialized_definitions);
        });
    return records.materialized_definitions;
}

std::span<const SidecarExpectationRecord> AuthorityRoot::getExpectationRecords() const
{
    auto & records = *content_payload->records;
    std::call_once(
        records.materialize_expectations_once,
        [&]
        {
            if (records.materialized_expectations.size() == records.expectation_count)
                return;
            records.materialized_expectations.clear();
            records.materialized_expectations.reserve(static_cast<size_t>(records.expectation_count));
            materializeExpectationRecords(records.expectations, records.materialized_expectations);
        });
    return records.materialized_expectations;
}

const Record * AuthorityRoot::findDefinitionRecord(const DefinitionIdentity & identity) const noexcept
{
    const auto * node = findRecordNode(content_payload->records->definitions, definitionRecordKey(identity));
    return node ? &node->definition_record : nullptr;
}

const Record * AuthorityRoot::findDefinitionRecord(UUID type_uuid) const noexcept
{
    const auto * leaf = content_payload->inventory->find({
        .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
        .object_uuid = type_uuid,
    });
    if (!leaf)
        return nullptr;
    return findDefinitionRecord({
        .database_uuid = getDatabaseUUID(),
        .type_uuid = type_uuid,
        .revision = leaf->object_revision,
    });
}

const SidecarExpectationRecord * AuthorityRoot::findExpectationRecord(const SchemaObjectID & object) const noexcept
{
    const auto * node = findRecordNode(content_payload->records->expectations, expectationRecordKey(object));
    return node ? &node->expectation_record : nullptr;
}

const SidecarExpectationRecord * AuthorityRoot::findExpectationRecord(UUID object_uuid) const noexcept
{
    if (object_uuid == UUIDHelpers::Nil)
        return nullptr;
    constexpr std::array kinds{
        SchemaObjectKind::Table,
        SchemaObjectKind::View,
        SchemaObjectKind::Dictionary,
        SchemaObjectKind::SyntheticTestObject,
    };
    const SidecarExpectationRecord * result = nullptr;
    for (const auto kind : kinds)
    {
        const auto * candidate = findExpectationRecord({
            .kind = kind,
            .database_uuid = getDatabaseUUID(),
            .object_uuid = object_uuid,
        });
        if (!candidate)
            continue;
        if (result)
            return nullptr;
        result = candidate;
    }
    return result;
}

UInt64 AuthorityRoot::getDefinitionRecordCount() const noexcept
{
    return content_payload->records->definition_count;
}

UInt64 AuthorityRoot::getExpectationRecordCount() const noexcept
{
    return content_payload->records->expectation_count;
}

bool AuthorityRoot::sharesDefinitionContentWith(const AuthorityRoot & other) const noexcept
{
    return content_payload->type_catalog == other.content_payload->type_catalog
        && content_payload->records->definitions == other.content_payload->records->definitions
        && content_payload->records->definition_count == other.content_payload->records->definition_count
        && content_payload->type_index_generation == other.content_payload->type_index_generation
        && content_payload->type_index_content_digest == other.content_payload->type_index_content_digest;
}

bool AuthorityRoot::provesPhysicalizationDeltaFrom(
    const AuthorityRoot & base,
    std::span<const AuthorityInventoryKey> sorted_removal_keys,
    const SchemaObjectDependencyGraphMutation & graph_delta) const
{
    const auto & provenance = content_payload->physicalization_provenance;
    return provenance && provenance->base_authority_anchor == base.getAuthorityState().anchor_hash
        && provenance->authority_removal_keys_digest == physicalizationRemovalKeysDigest(sorted_removal_keys)
        && provenance->graph_delta_digest == physicalizationGraphDeltaDigest(graph_delta);
}

AuthorityRoot::Ptr AuthorityRoot::cloneWithAuthorityState(AuthorityState next_authority_state, const AuthorityStateLimits & limits) const
{
    validateAuthorityState(next_authority_state, limits);

    AuthorityState expected;
    try
    {
        expected = activateDependentObjectAuthority(authority_state, limits);
    }
    catch (const AuthorityStateError & error)
    {
        if (error.code == AuthorityStateError::Code::LimitExceeded)
            fail(RootError::Code::LimitExceeded, "authority activation exceeds its limit");
        fail(RootError::Code::InvalidAuthorityState, "authority root is not eligible for exact dependent-object-capable activation");
    }
    if (next_authority_state != expected)
        fail(RootError::Code::InvalidAuthorityState, "authority activation changes content anchors or is not the exact next V1 state");

    return Ptr(new AuthorityRoot(std::move(next_authority_state), content_payload));
}

AuthorityRoot::Ptr AuthorityRoot::cloneForExactRepair(const AuthorityStateLimits & limits) const
{
    if (authority_state.database_catalog_epoch == std::numeric_limits<UInt64>::max())
        fail(RootError::Code::InvalidAuthorityState, "authority exact-repair epoch domain is exhausted");

    AuthorityState next;
    try
    {
        next = makeAuthorityState(
            authority_state.database_uuid,
            authority_state.database_catalog_epoch + 1,
            authority_state.persistent_capability_mask,
            authority_state.leaf_count,
            authority_state.inventory_root,
            authority_state.schema_graph_root,
            limits);
    }
    catch (const AuthorityStateError & error)
    {
        if (error.code == AuthorityStateError::Code::LimitExceeded)
            fail(RootError::Code::LimitExceeded, "authority exact-repair successor exceeds its limit");
        fail(RootError::Code::InvalidAuthorityState, "authority exact-repair successor state is invalid");
    }
    return Ptr(new AuthorityRoot(std::move(next), content_payload));
}

AuthorityRoot::Ptr AuthorityRoot::cloneWithDatabaseResourceLimits(
    UInt64 next_database_catalog_epoch, EffectiveResourceLimits next_limits, const AuthorityStateLimits & limits) const
{
    if (authority_state.database_catalog_epoch == std::numeric_limits<UInt64>::max()
        || next_database_catalog_epoch != authority_state.database_catalog_epoch + 1)
        fail(RootError::Code::InvalidAuthorityState, "database resource-quota policy epoch is not the exact successor");

    AuthorityState next_state;
    try
    {
        next_state = makeAuthorityState(
            authority_state.database_uuid,
            next_database_catalog_epoch,
            authority_state.persistent_capability_mask,
            authority_state.leaf_count,
            authority_state.inventory_root,
            authority_state.schema_graph_root,
            limits);
    }
    catch (const AuthorityStateError & error)
    {
        if (error.code == AuthorityStateError::Code::LimitExceeded)
            fail(RootError::Code::LimitExceeded, "database resource-quota policy successor exceeds its limit");
        fail(RootError::Code::InvalidAuthorityState, "database resource-quota policy successor state is invalid");
    }

    auto prepared = DatabaseResourceQuotaTransitionBuilder::prepareReplacement(
        content_payload->database_resource_quota, std::move(next_limits), ResourceDelta{});
    DatabaseResourceQuotaSnapshot::Ptr replacement_quota;
    switch (prepared.getStatus())
    {
        case DatabaseResourceQuotaPreparationStatus::Prepared: replacement_quota = prepared.getReplacement(); break;
        case DatabaseResourceQuotaPreparationStatus::InvalidBase:
            fail(RootError::Code::InvalidAuthorityState, "database resource-quota policy replacement lost its base image");
        case DatabaseResourceQuotaPreparationStatus::RevisionOverflow:
            fail(RootError::Code::LimitExceeded, "database resource-quota policy revision overflows UInt64");
        case DatabaseResourceQuotaPreparationStatus::ResourceRejected: failResourceAdmission(prepared.getResourceAdmission());
    }
    if (!replacement_quota || replacement_quota->getUsage() != content_payload->database_resource_quota->getUsage())
        fail(RootError::Code::InvalidAuthorityState, "database resource-quota policy replacement changed exact usage");

    const UInt64 logical_charge = computeContentPayloadLogicalCharge(
        *content_payload->type_catalog,
        *content_payload->inventory,
        *content_payload->records,
        *content_payload->schema_graph,
        *content_payload->resource_usage_index,
        *replacement_quota);
    auto replacement_payload = ContentPayloadPtr(new ContentPayload{
        .type_index_generation = content_payload->type_index_generation,
        .type_index_content_digest = content_payload->type_index_content_digest,
        .type_catalog = content_payload->type_catalog,
        .inventory = content_payload->inventory,
        .schema_graph = content_payload->schema_graph,
        .records = content_payload->records,
        .resource_usage_index = content_payload->resource_usage_index,
        .database_resource_quota = std::move(replacement_quota),
        .verification_policy = content_payload->verification_policy,
        .verification_maximum_targets_per_batch = content_payload->verification_maximum_targets_per_batch,
        .logical_charge = logical_charge,
        .physicalization_provenance = std::nullopt,
    });
    return Ptr(new AuthorityRoot(std::move(next_state), std::move(replacement_payload)));
}

AuthorityRoot::Ptr AuthorityRoot::cloneWithVerificationPlanningDomainForStartup(
    const AuthorityVerificationSchedulePolicy & policy, UInt64 maximum_targets_per_batch) const
{
    const ResourceUsage current_usage = computeDatabaseOwnedResourceUsage(
        *content_payload->type_catalog,
        *content_payload->inventory,
        *content_payload->records,
        *content_payload->schema_graph,
        *content_payload->resource_usage_index,
        content_payload->verification_policy,
        content_payload->verification_maximum_targets_per_batch);
    if (current_usage != content_payload->database_resource_quota->getUsage())
        fail(RootError::Code::InvalidAuthorityState, "authority startup verification policy lost its exact quota base");

    const ResourceUsage replacement_usage = computeDatabaseOwnedResourceUsage(
        *content_payload->type_catalog,
        *content_payload->inventory,
        *content_payload->records,
        *content_payload->schema_graph,
        *content_payload->resource_usage_index,
        policy,
        maximum_targets_per_batch);
    auto replacement_quota = DatabaseResourceQuotaTransitionBuilder::makeInitial(
        content_payload->database_resource_quota->getLimits(), replacement_usage, content_payload->database_resource_quota->getRevision());
    if (!replacement_quota || replacement_quota->getUsage() != replacement_usage
        || replacement_quota->getLimits() != content_payload->database_resource_quota->getLimits())
    {
        fail(RootError::Code::InvalidAuthorityState, "authority startup verification policy produced an inconsistent quota image");
    }

    const UInt64 logical_charge = computeContentPayloadLogicalCharge(
        *content_payload->type_catalog,
        *content_payload->inventory,
        *content_payload->records,
        *content_payload->schema_graph,
        *content_payload->resource_usage_index,
        *replacement_quota);
    auto replacement_payload = ContentPayloadPtr(new ContentPayload{
        .type_index_generation = content_payload->type_index_generation,
        .type_index_content_digest = content_payload->type_index_content_digest,
        .type_catalog = content_payload->type_catalog,
        .inventory = content_payload->inventory,
        .schema_graph = content_payload->schema_graph,
        .records = content_payload->records,
        .resource_usage_index = content_payload->resource_usage_index,
        .database_resource_quota = std::move(replacement_quota),
        .verification_policy = policy,
        .verification_maximum_targets_per_batch = maximum_targets_per_batch,
        .logical_charge = logical_charge,
        .physicalization_provenance = content_payload->physicalization_provenance,
    });
    return Ptr(new AuthorityRoot(authority_state, std::move(replacement_payload)));
}

AuthorityRoot::Ptr AuthorityRootBuilder::build(
    AuthorityState authority_state,
    UInt64 type_index_generation,
    std::span<const Definition::Ptr> checked_definitions,
    std::span<const Record> definition_records,
    std::span<const SidecarExpectationRecord> expectation_records,
    SchemaObjectDependencyGraph::Ptr schema_graph,
    const AuthorityRootBuildLimits & limits,
    std::span<const AuthorityDependentObjectResourceImage> dependent_objects)
{
    return buildImpl(
        nullptr,
        false,
        nullptr,
        std::move(authority_state),
        type_index_generation,
        checked_definitions,
        definition_records,
        expectation_records,
        std::move(schema_graph),
        limits,
        dependent_objects);
}

AuthorityRoot::Ptr AuthorityRootBuilder::build(
    AuthorityState authority_state,
    UInt64 type_index_generation,
    std::span<const Definition::Ptr> checked_definitions,
    std::span<const Record> definition_records,
    std::span<const SidecarExpectationRecord> expectation_records,
    SchemaObjectDependencyGraph::Ptr schema_graph,
    const EffectiveResourceLimits & effective_database_limits,
    const AuthorityRootBuildLimits & limits,
    std::span<const AuthorityDependentObjectResourceImage> dependent_objects)
{
    return buildImpl(
        nullptr,
        false,
        &effective_database_limits,
        std::move(authority_state),
        type_index_generation,
        checked_definitions,
        definition_records,
        expectation_records,
        std::move(schema_graph),
        limits,
        dependent_objects);
}

AuthorityRoot::Ptr AuthorityRootBuilder::buildInitialAdmission(
    AuthorityState authority_state,
    UInt64 type_index_generation,
    std::span<const Definition::Ptr> checked_definitions,
    std::span<const Record> definition_records,
    std::span<const SidecarExpectationRecord> expectation_records,
    SchemaObjectDependencyGraph::Ptr schema_graph,
    const AuthorityRootBuildLimits & limits,
    std::span<const AuthorityDependentObjectResourceImage> dependent_objects)
{
    return buildImpl(
        nullptr,
        true,
        nullptr,
        std::move(authority_state),
        type_index_generation,
        checked_definitions,
        definition_records,
        expectation_records,
        std::move(schema_graph),
        limits,
        dependent_objects);
}

AuthorityRoot::Ptr AuthorityRootBuilder::buildInitialAdmission(
    AuthorityState authority_state,
    UInt64 type_index_generation,
    std::span<const Definition::Ptr> checked_definitions,
    std::span<const Record> definition_records,
    std::span<const SidecarExpectationRecord> expectation_records,
    SchemaObjectDependencyGraph::Ptr schema_graph,
    const EffectiveResourceLimits & effective_database_limits,
    const AuthorityRootBuildLimits & limits,
    std::span<const AuthorityDependentObjectResourceImage> dependent_objects)
{
    return buildImpl(
        nullptr,
        true,
        &effective_database_limits,
        std::move(authority_state),
        type_index_generation,
        checked_definitions,
        definition_records,
        expectation_records,
        std::move(schema_graph),
        limits,
        dependent_objects);
}

AuthorityRoot::Ptr AuthorityRootBuilder::buildReplacement(
    const AuthorityRoot & base,
    AuthorityState authority_state,
    UInt64 type_index_generation,
    std::span<const Definition::Ptr> checked_definitions,
    std::span<const Record> definition_records,
    std::span<const SidecarExpectationRecord> expectation_records,
    SchemaObjectDependencyGraph::Ptr schema_graph,
    const AuthorityRootBuildLimits & limits,
    std::span<const AuthorityDependentObjectResourceImage> dependent_objects)
{
    if (authority_state.database_uuid != base.getDatabaseUUID())
        fail(RootError::Code::DatabaseMismatch, "authority replacement belongs to another database");
    if (base.getDatabaseCatalogEpoch() == std::numeric_limits<UInt64>::max()
        || authority_state.database_catalog_epoch != base.getDatabaseCatalogEpoch() + 1)
        fail(RootError::Code::InvalidAuthorityState, "authority replacement catalog epoch is not the exact successor");
    if (authority_state.persistent_capability_mask != base.getPersistentCapabilityMask())
        fail(RootError::Code::InvalidAuthorityState, "authority replacement changes the persistent capability mask");

    return buildImpl(
        &base,
        false,
        nullptr,
        std::move(authority_state),
        type_index_generation,
        checked_definitions,
        definition_records,
        expectation_records,
        std::move(schema_graph),
        limits,
        dependent_objects);
}

AuthorityRoot::Ptr AuthorityRootBuilder::buildImpl(
    const AuthorityRoot * base,
    bool require_initial_usage_within_quota,
    const EffectiveResourceLimits * initial_effective_database_limits,
    AuthorityState authority_state,
    UInt64 type_index_generation,
    std::span<const Definition::Ptr> checked_definitions,
    std::span<const Record> definition_records,
    std::span<const SidecarExpectationRecord> expectation_records,
    SchemaObjectDependencyGraph::Ptr schema_graph,
    const AuthorityRootBuildLimits & limits,
    std::span<const AuthorityDependentObjectResourceImage> dependent_objects)
{
    fiu_do_on(DB::FailPoints::udt_authority_root_builder_allocation_failure, { throw std::bad_alloc(); });

    if (base && (require_initial_usage_within_quota || initial_effective_database_limits))
        fail(RootError::Code::InvalidConfiguration, "an authority successor cannot use initial resource-quota inputs");
    validateAuthorityState(authority_state, limits.authority_state);
    if (!schema_graph)
        fail(RootError::Code::InvalidConfiguration, "schema-object dependency graph is null");

    const UInt64 definition_count = toUInt64(definition_records.size(), "definition record count does not fit UInt64");
    const UInt64 expectation_count = toUInt64(expectation_records.size(), "expectation record count does not fit UInt64");
    if (definition_count > limits.maximum_definition_records || expectation_count > limits.maximum_expectation_records)
        fail(RootError::Code::LimitExceeded, "authority record count exceeds its limit");
    if (definition_count > limits.type_catalog.maximum_definitions)
        fail(RootError::Code::LimitExceeded, "definition count exceeds the type-index limit");
    if (expectation_count > std::numeric_limits<UInt64>::max() - definition_count)
        fail(RootError::Code::LimitExceeded, "authority record count overflows UInt64");
    const UInt64 record_count = definition_count + expectation_count;
    if (record_count > limits.inventory.maximum_leaves)
        fail(RootError::Code::LimitExceeded, "authority record count exceeds the inventory limit");
    if (record_count != authority_state.leaf_count)
        fail(RootError::Code::InventoryMismatch, "authority record count differs from authority state");
    if (checked_definitions.size() != definition_records.size())
        fail(RootError::Code::RecordDefinitionMismatch, "checked definition and durable record counts differ");

    if (schema_graph->getDatabaseUUID() != authority_state.database_uuid)
        fail(RootError::Code::DatabaseMismatch, "schema-object dependency graph belongs to another database");
    if (schema_graph->computeRoot() != authority_state.schema_graph_root)
        fail(RootError::Code::GraphMismatch, "schema-object dependency graph root differs from authority state");

    std::vector<Definition::Ptr> canonical_definitions(checked_definitions.begin(), checked_definitions.end());
    for (const auto & definition : canonical_definitions)
    {
        if (!definition)
            fail(RootError::Code::InvalidDefinition, "checked definition is null");
        const auto & identity = definition->getIdentity();
        if (identity.database_uuid == UUIDHelpers::Nil || identity.type_uuid == UUIDHelpers::Nil || identity.revision == 0)
            fail(RootError::Code::InvalidDefinition, "checked definition identity is invalid");
        if (identity.database_uuid != authority_state.database_uuid)
            fail(RootError::Code::DatabaseMismatch, "checked definition belongs to another database");
    }
    std::sort(
        canonical_definitions.begin(),
        canonical_definitions.end(),
        [](const auto & lhs, const auto & rhs) { return definitionIdentityLess(lhs->getIdentity(), rhs->getIdentity()); });
    for (size_t index = 1; index < canonical_definitions.size(); ++index)
    {
        if (canonical_definitions[index - 1]->getIdentity() == canonical_definitions[index]->getIdentity())
            fail(RootError::Code::InvalidDefinition, "checked definitions contain a duplicate identity");
    }
    if (!canonical_definitions.empty() && type_index_generation == 0)
        fail(RootError::Code::InvalidConfiguration, "a nonempty type index cannot use the never-enabled generation");

    std::vector<Record> canonical_definition_records(definition_records.begin(), definition_records.end());
    std::sort(
        canonical_definition_records.begin(),
        canonical_definition_records.end(),
        [](const auto & lhs, const auto & rhs) { return definitionIdentityLess(lhs.identity, rhs.identity); });

    UInt64 canonical_record_bytes = 0;
    std::vector<AuthorityInventoryLeaf> inventory_leaves;
    inventory_leaves.reserve(static_cast<size_t>(record_count));
    for (size_t index = 0; index < canonical_definition_records.size(); ++index)
    {
        const auto & record = canonical_definition_records[index];
        if (record.identity.database_uuid == UUIDHelpers::Nil || record.identity.type_uuid == UUIDHelpers::Nil
            || record.identity.revision == 0)
            fail(RootError::Code::InvalidRecord, "definition record identity is invalid");
        if (record.identity.database_uuid != authority_state.database_uuid)
            fail(RootError::Code::DatabaseMismatch, "definition record belongs to another database");
        if (index != 0 && canonical_definition_records[index - 1].identity.type_uuid == record.identity.type_uuid)
            fail(RootError::Code::DuplicateRecordIdentity, "definition records contain a duplicate type identity");

        const String encoded = validateDefinitionRecord(record, limits.definition_record);
        chargeCanonicalBytes(
            canonical_record_bytes,
            toUInt64(encoded.size(), "definition record size does not fit UInt64"),
            limits.maximum_canonical_record_bytes);
        if (record.identity != canonical_definitions[index]->getIdentity()
            || !recordMatchesCheckedDefinition(record, *canonical_definitions[index]))
            fail(RootError::Code::RecordDefinitionMismatch, "durable record does not exactly match its checked definition");

        inventory_leaves.push_back({
            .key = {
                .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
                .object_uuid = record.identity.type_uuid,
            },
            .object_revision = record.identity.revision,
            .canonical_record_hash = computeRecordHash(record, limits.definition_record),
        });
    }

    std::vector<SidecarExpectationRecord> canonical_expectation_records(expectation_records.begin(), expectation_records.end());
    std::sort(canonical_expectation_records.begin(), canonical_expectation_records.end(), expectationRecordLess);
    for (size_t index = 0; index < canonical_expectation_records.size(); ++index)
    {
        const auto & record = canonical_expectation_records[index];
        if (!record.object.isValid() || record.object.kind == SchemaObjectKind::TypeDefinition || record.object_schema_revision == 0)
            fail(RootError::Code::InvalidRecord, "sidecar expectation identity is invalid");
        if (record.object.database_uuid != authority_state.database_uuid)
            fail(RootError::Code::DatabaseMismatch, "sidecar expectation belongs to another database");
        if (index != 0 && canonical_expectation_records[index - 1].object == record.object)
            fail(RootError::Code::DuplicateRecordIdentity, "sidecar expectations contain a duplicate object identity");

        const String encoded = validateExpectationRecord(record);
        chargeCanonicalBytes(
            canonical_record_bytes,
            toUInt64(encoded.size(), "expectation record size does not fit UInt64"),
            limits.maximum_canonical_record_bytes);
        inventory_leaves.push_back({
            .key = {
                .record_kind = AuthorityInventoryRecordKind::SidecarExpectation,
                .object_uuid = record.object.object_uuid,
            },
            .object_revision = record.object_schema_revision,
            .canonical_record_hash = computeSidecarExpectationRecordHash(record),
        });
    }

    std::sort(inventory_leaves.begin(), inventory_leaves.end(), authorityInventoryLeafLess);
    for (size_t index = 1; index < inventory_leaves.size(); ++index)
    {
        if (inventory_leaves[index - 1].key == inventory_leaves[index].key)
            fail(RootError::Code::DuplicateRecordIdentity, "authority records contain a duplicate inventory identity");
    }

    auto inventory = buildInventory(std::move(inventory_leaves), limits.inventory);
    if (inventory->getSummary().leaf_count != authority_state.leaf_count
        || inventory->getSummary().merkle_radix_root != authority_state.inventory_root)
        fail(RootError::Code::InventoryMismatch, "authority inventory differs from authority state");

    validateSchemaGraphContents(authority_state, canonical_definitions, canonical_expectation_records, *schema_graph);

    auto records
        = buildRecordStore(canonical_definitions, canonical_definition_records, canonical_expectation_records, canonical_record_bytes);
    const Digest type_index_content_digest = records->type_index_content_digest;
    std::shared_ptr<const TypeCatalogRoot> type_catalog
        = buildTypeCatalog(authority_state.database_uuid, type_index_generation, canonical_definitions, limits.type_catalog);
    AuthorityResourceUsageIndex::Ptr resource_usage_index;
    if (base && dependent_objects.empty())
    {
        const auto base_expectations = base->getExpectationRecords();
        if (base_expectations.size() != canonical_expectation_records.size()
            || !std::equal(base_expectations.begin(), base_expectations.end(), canonical_expectation_records.begin()))
            fail(RootError::Code::InvalidRecord, "authority replacement changed expectations without exact sidecar images");
        resource_usage_index = base->content_payload->resource_usage_index;
    }
    else
    {
        resource_usage_index = buildResourceUsageIndex(
            authority_state.database_uuid, canonical_expectation_records, dependent_objects, *type_catalog, limits.resource_usage_index);
    }
    const auto & verification_policy = base ? base->content_payload->verification_policy : limits.verification_policy;
    const UInt64 verification_maximum_targets_per_batch
        = base ? base->content_payload->verification_maximum_targets_per_batch : limits.verification_maximum_targets_per_batch;
    const ResourceUsage replacement_usage = computeDatabaseOwnedResourceUsage(
        *type_catalog,
        *inventory,
        *records,
        *schema_graph,
        *resource_usage_index,
        verification_policy,
        verification_maximum_targets_per_batch);
    DatabaseResourceQuotaSnapshot::Ptr database_resource_quota;
    if (base)
    {
        const ResourceUsage base_usage = computeDatabaseOwnedResourceUsage(
            *base->content_payload->type_catalog,
            *base->content_payload->inventory,
            *base->content_payload->records,
            *base->content_payload->schema_graph,
            *base->content_payload->resource_usage_index,
            verification_policy,
            verification_maximum_targets_per_batch);
        database_resource_quota
            = prepareDatabaseResourceQuotaReplacement(base->content_payload->database_resource_quota, base_usage, replacement_usage);
    }
    else
    {
        database_resource_quota
            = makeInitialDatabaseResourceQuota(replacement_usage, require_initial_usage_within_quota, initial_effective_database_limits);
    }
    const UInt64 logical_charge = computeContentPayloadLogicalCharge(
        *type_catalog, *inventory, *records, *schema_graph, *resource_usage_index, *database_resource_quota);
    fiu_do_on(DB::FailPoints::udt_authority_root_builder_freeze_failure, {
        fail(RootError::Code::InvalidConfiguration, "fault injected before immutable authority-root freeze");
    });
    auto content_payload = AuthorityRoot::ContentPayloadPtr(new AuthorityRoot::ContentPayload{
        .type_index_generation = type_index_generation,
        .type_index_content_digest = type_index_content_digest,
        .type_catalog = std::move(type_catalog),
        .inventory = std::move(inventory),
        .schema_graph = std::move(schema_graph),
        .records = std::move(records),
        .resource_usage_index = std::move(resource_usage_index),
        .database_resource_quota = std::move(database_resource_quota),
        .verification_policy = verification_policy,
        .verification_maximum_targets_per_batch = verification_maximum_targets_per_batch,
        .logical_charge = logical_charge,
        .physicalization_provenance = std::nullopt,
    });
    return AuthorityRoot::Ptr(new AuthorityRoot(std::move(authority_state), std::move(content_payload)));
}

AuthorityRoot::Ptr AuthorityRootBuilder::buildPhysicalizationDelta(
    const AuthorityRoot & base,
    UInt64 next_database_catalog_epoch,
    std::span<const DefinitionIdentity> definition_removals,
    std::span<const SchemaObjectID> expectation_removals,
    const SchemaObjectDependencyGraphMutation & graph_delta,
    const AuthorityRootBuildLimits & limits,
    AuthorityInventoryMutationStatistics * inventory_statistics,
    SchemaObjectDependencyGraphMutationStatistics * graph_statistics)
{
    if (base.getPersistentCapabilityMask() != dependent_object_authority_capability_mask)
        fail(RootError::Code::InvalidAuthorityState, "physicalization delta requires a dependent-object-capable authority root");
    if (base.getDatabaseCatalogEpoch() == std::numeric_limits<UInt64>::max()
        || next_database_catalog_epoch != base.getDatabaseCatalogEpoch() + 1)
        fail(RootError::Code::InvalidAuthorityState, "physicalization delta catalog epoch is not the exact successor");
    if (definition_removals.size() > base.content_payload->records->definition_count
        || expectation_removals.size() > base.content_payload->records->expectation_count)
        fail(RootError::Code::InvalidRecord, "physicalization delta removal count exceeds the authority record count");

    for (size_t index = 0; index < definition_removals.size(); ++index)
    {
        const auto & identity = definition_removals[index];
        if (identity.database_uuid != base.getDatabaseUUID() || identity.type_uuid == UUIDHelpers::Nil || identity.revision == 0)
            fail(RootError::Code::InvalidRecord, "physicalization definition removal identity is invalid");
        if (index && !definitionIdentityLess(definition_removals[index - 1], identity))
            fail(RootError::Code::DuplicateRecordIdentity, "physicalization definition removals are not strictly sorted");
    }
    for (size_t index = 0; index < expectation_removals.size(); ++index)
    {
        const auto & object = expectation_removals[index];
        if (!object.isValid() || object.database_uuid != base.getDatabaseUUID())
            fail(RootError::Code::InvalidRecord, "physicalization expectation removal identity is invalid");
        if (index && !(expectation_removals[index - 1] < object))
            fail(RootError::Code::DuplicateRecordIdentity, "physicalization expectation removals are not strictly sorted");
    }

    std::vector<AuthorityInventoryLeafDelta> inventory_deltas;
    inventory_deltas.reserve(definition_removals.size() + expectation_removals.size());
    for (const auto & identity : definition_removals)
    {
        const auto * record = base.findDefinitionRecord(identity);
        if (!record)
            fail(RootError::Code::InvalidRecord, "physicalization removes an absent definition record");
        AuthorityInventoryLeaf leaf{
            .key = {
                .record_kind = AuthorityInventoryRecordKind::TypeDefinition,
                .object_uuid = identity.type_uuid,
            },
            .object_revision = identity.revision,
            .canonical_record_hash = computeRecordHash(*record, limits.definition_record),
        };
        inventory_deltas.push_back({.key = leaf.key, .before = std::move(leaf), .after = std::nullopt});
    }
    for (const auto & object : expectation_removals)
    {
        const auto * record = base.findExpectationRecord(object);
        if (!record)
            fail(RootError::Code::InvalidRecord, "physicalization removes an absent expectation record");
        AuthorityInventoryLeaf leaf{
            .key = {
                .record_kind = AuthorityInventoryRecordKind::SidecarExpectation,
                .object_uuid = object.object_uuid,
            },
            .object_revision = record->object_schema_revision,
            .canonical_record_hash = computeSidecarExpectationRecordHash(*record),
        };
        inventory_deltas.push_back({.key = leaf.key, .before = std::move(leaf), .after = std::nullopt});
    }
    std::sort(
        inventory_deltas.begin(),
        inventory_deltas.end(),
        [](const AuthorityInventoryLeafDelta & lhs, const AuthorityInventoryLeafDelta & rhs)
        { return authorityInventoryKeyLess(lhs.key, rhs.key); });
    for (size_t index = 1; index < inventory_deltas.size(); ++index)
        if (inventory_deltas[index - 1].key == inventory_deltas[index].key)
            fail(RootError::Code::DuplicateRecordIdentity, "physicalization removals collide in the authority inventory");

    AuthorityInventory::Ptr next_inventory;
    try
    {
        next_inventory
            = AuthorityInventory::applyMutation(base.content_payload->inventory, inventory_deltas, limits.inventory, inventory_statistics);
    }
    catch (const AuthorityInventoryError & error)
    {
        if (error.code == AuthorityInventoryError::Code::LimitExceeded)
            fail(RootError::Code::LimitExceeded, "physicalization inventory delta exceeds its limit");
        fail(RootError::Code::InventoryMismatch, "physicalization inventory delta is inconsistent");
    }

    SchemaObjectDependencyGraph::Ptr next_graph;
    try
    {
        next_graph = SchemaObjectDependencyGraph::applyMutation(base.content_payload->schema_graph, graph_delta, graph_statistics);
    }
    catch (const SchemaObjectDependencyGraphError & error)
    {
        if (error.code == SchemaObjectDependencyGraphError::Code::LimitExceeded)
            fail(RootError::Code::LimitExceeded, "physicalization graph delta exceeds its limit");
        fail(RootError::Code::GraphMismatch, "physicalization graph delta is inconsistent");
    }
    validatePhysicalizationGraphDelta(definition_removals, expectation_removals, graph_delta, *next_graph);

    auto next_records = applyRecordRemovals(base.content_payload->records, definition_removals, expectation_removals);
    if (next_records->definition_count > limits.maximum_definition_records
        || next_records->definition_count > limits.type_catalog.maximum_definitions
        || next_records->expectation_count > limits.maximum_expectation_records
        || next_records->canonical_record_bytes > limits.maximum_canonical_record_bytes)
        fail(RootError::Code::LimitExceeded, "physicalization record-store delta exceeds its limit");
    const UInt64 next_record_count = checkedAdd(
        next_records->definition_count, next_records->expectation_count, "physicalization authority record count overflows UInt64");
    if (next_inventory->getSummary().leaf_count != next_record_count)
        fail(RootError::Code::InventoryMismatch, "physicalization record store and inventory counts differ");

    UInt64 next_generation = base.getTypeIndexGeneration();
    std::shared_ptr<const TypeCatalogRoot> next_catalog = base.content_payload->type_catalog;
    if (!definition_removals.empty())
    {
        if (next_generation == std::numeric_limits<UInt64>::max())
            fail(RootError::Code::LimitExceeded, "physicalization type-index generation cannot advance");
        ++next_generation;
        try
        {
            next_catalog = TypeCatalogBuilder::removeDefinitions(
                *base.content_payload->type_catalog, next_generation, definition_removals, limits.type_catalog);
        }
        catch (const CatalogError & error)
        {
            if (error.code == CatalogError::Code::LimitExceeded)
                fail(RootError::Code::LimitExceeded, "physicalization catalog delta exceeds its limit");
            if (error.code == CatalogError::Code::InvalidConfiguration)
                fail(RootError::Code::InvalidConfiguration, "physicalization catalog limits are inconsistent");
            fail(RootError::Code::InvalidDefinition, "physicalization catalog delta is inconsistent");
        }
    }

    AuthorityState next_state;
    try
    {
        next_state = makeAuthorityState(
            base.getDatabaseUUID(),
            next_database_catalog_epoch,
            dependent_object_authority_capability_mask,
            next_inventory->getSummary().leaf_count,
            next_inventory->getSummary().merkle_radix_root,
            next_graph->computeRoot(),
            limits.authority_state);
    }
    catch (const AuthorityStateError & error)
    {
        if (error.code == AuthorityStateError::Code::LimitExceeded)
            fail(RootError::Code::LimitExceeded, "physicalization authority state exceeds its limit");
        fail(RootError::Code::InvalidAuthorityState, "physicalization authority state is invalid");
    }

    auto next_resource_usage_index
        = removeResourceUsageObjects(base.content_payload->resource_usage_index, expectation_removals, limits.resource_usage_index);
    const ResourceUsage base_usage = computeDatabaseOwnedResourceUsage(
        *base.content_payload->type_catalog,
        *base.content_payload->inventory,
        *base.content_payload->records,
        *base.content_payload->schema_graph,
        *base.content_payload->resource_usage_index,
        base.content_payload->verification_policy,
        base.content_payload->verification_maximum_targets_per_batch);
    const ResourceUsage replacement_usage = computeDatabaseOwnedResourceUsage(
        *next_catalog,
        *next_inventory,
        *next_records,
        *next_graph,
        *next_resource_usage_index,
        base.content_payload->verification_policy,
        base.content_payload->verification_maximum_targets_per_batch);
    auto database_resource_quota
        = prepareDatabaseResourceQuotaReplacement(base.content_payload->database_resource_quota, base_usage, replacement_usage);
    const UInt64 logical_charge = computeContentPayloadLogicalCharge(
        *next_catalog, *next_inventory, *next_records, *next_graph, *next_resource_usage_index, *database_resource_quota);
    auto content_payload = AuthorityRoot::ContentPayloadPtr(
        new AuthorityRoot::ContentPayload{
            .type_index_generation = next_generation,
            .type_index_content_digest = next_records->type_index_content_digest,
            .type_catalog = std::move(next_catalog),
            .inventory = std::move(next_inventory),
            .schema_graph = std::move(next_graph),
            .records = std::move(next_records),
            .resource_usage_index = std::move(next_resource_usage_index),
            .database_resource_quota = std::move(database_resource_quota),
            .verification_policy = base.content_payload->verification_policy,
            .verification_maximum_targets_per_batch = base.content_payload->verification_maximum_targets_per_batch,
            .logical_charge = logical_charge,
            .physicalization_provenance = AuthorityRoot::ContentPayload::PhysicalizationProvenance{
                .base_authority_anchor = base.getAuthorityState().anchor_hash,
                .authority_removal_keys_digest = [&]
                {
                    std::vector<AuthorityInventoryKey> keys;
                    keys.reserve(inventory_deltas.size());
                    for (const auto & delta : inventory_deltas)
                        keys.push_back(delta.key);
                    return physicalizationRemovalKeysDigest(keys);
                }(),
                .graph_delta_digest = physicalizationGraphDeltaDigest(graph_delta),
            },
        });
    return AuthorityRoot::Ptr(new AuthorityRoot(std::move(next_state), std::move(content_payload)));
}

AuthorityRoot::Ptr AuthorityRootBuilder::buildDependentObjectAdmissionDelta(
    const AuthorityRoot & base,
    UInt64 next_database_catalog_epoch,
    const SidecarExpectationRecord & expectation_addition,
    const SchemaObjectDependencyGraphMutation & graph_delta,
    const AuthorityDependentObjectResourceImage & dependent_object,
    const AuthorityRootBuildLimits & limits,
    DependentObjectAdmissionDeltaStatistics * statistics)
{
    if (statistics)
        *statistics = {};
    if (base.getPersistentCapabilityMask() != dependent_object_authority_capability_mask)
        fail(RootError::Code::InvalidAuthorityState, "dependent-object admission delta requires a dependent-object-capable authority root");
    if (base.getDatabaseCatalogEpoch() == std::numeric_limits<UInt64>::max()
        || next_database_catalog_epoch != base.getDatabaseCatalogEpoch() + 1)
        fail(RootError::Code::InvalidAuthorityState, "dependent-object admission delta catalog epoch is not the exact successor");
    if (!expectation_addition.object.isValid() || expectation_addition.object.kind == SchemaObjectKind::TypeDefinition
        || expectation_addition.object.database_uuid != base.getDatabaseUUID() || expectation_addition.object_schema_revision == 0)
        fail(RootError::Code::InvalidRecord, "dependent-object admission expectation identity is invalid");

    const String encoded_expectation = validateExpectationRecord(expectation_addition);
    UInt64 next_canonical_record_bytes = base.content_payload->records->canonical_record_bytes;
    chargeCanonicalBytes(
        next_canonical_record_bytes,
        toUInt64(encoded_expectation.size(), "dependent-object admission expectation size does not fit UInt64"),
        limits.maximum_canonical_record_bytes);
    if (base.content_payload->records->definition_count > limits.maximum_definition_records
        || base.content_payload->records->definition_count > limits.type_catalog.maximum_definitions
        || base.content_payload->records->expectation_count >= limits.maximum_expectation_records)
        fail(RootError::Code::LimitExceeded, "dependent-object admission record-store delta exceeds its limit");

    const AuthorityInventoryLeaf inventory_leaf{
        .key = {
            .record_kind = AuthorityInventoryRecordKind::SidecarExpectation,
            .object_uuid = expectation_addition.object.object_uuid,
        },
        .object_revision = expectation_addition.object_schema_revision,
        .canonical_record_hash = computeSidecarExpectationRecordHash(expectation_addition),
    };
    if (base.content_payload->inventory->find(inventory_leaf.key) || base.findExpectationRecord(expectation_addition.object)
        || base.content_payload->schema_graph->containsNode(expectation_addition.object))
        fail(RootError::Code::DuplicateRecordIdentity, "dependent-object admission object already exists");

    const AuthorityInventoryLeafDelta inventory_delta{
        .key = inventory_leaf.key,
        .before = std::nullopt,
        .after = inventory_leaf,
    };
    AuthorityInventory::Ptr next_inventory;
    try
    {
        next_inventory = AuthorityInventory::applyMutation(
            base.content_payload->inventory,
            std::span<const AuthorityInventoryLeafDelta>(&inventory_delta, 1),
            limits.inventory,
            statistics ? &statistics->inventory : nullptr);
    }
    catch (const AuthorityInventoryError & error)
    {
        if (error.code == AuthorityInventoryError::Code::LimitExceeded)
            fail(RootError::Code::LimitExceeded, "dependent-object admission inventory delta exceeds its limit");
        fail(RootError::Code::InventoryMismatch, "dependent-object admission inventory delta is inconsistent");
    }

    SchemaObjectDependencyGraph::Ptr next_graph;
    try
    {
        next_graph = SchemaObjectDependencyGraph::applyMutation(
            base.content_payload->schema_graph, graph_delta, statistics ? &statistics->graph : nullptr);
    }
    catch (const SchemaObjectDependencyGraphError & error)
    {
        if (error.code == SchemaObjectDependencyGraphError::Code::LimitExceeded)
            fail(RootError::Code::LimitExceeded, "dependent-object admission graph delta exceeds its limit");
        fail(RootError::Code::GraphMismatch, "dependent-object admission graph delta is inconsistent");
    }
    validateDependentObjectAdmissionGraphDelta(base, expectation_addition, graph_delta, *next_graph);

    auto next_records = applyExpectationRecordAddition(base.content_payload->records, expectation_addition, statistics);
    if (next_records->canonical_record_bytes != next_canonical_record_bytes)
        fail(RootError::Code::InvalidRecord, "dependent-object admission record byte accounting differs from validation");
    const UInt64 next_record_count = checkedAdd(
        next_records->definition_count,
        next_records->expectation_count,
        "dependent-object admission authority record count overflows UInt64");
    if (next_inventory->getSummary().leaf_count != next_record_count)
        fail(RootError::Code::InventoryMismatch, "dependent-object admission record store and inventory counts differ");

    AuthorityState next_state;
    try
    {
        next_state = makeAuthorityState(
            base.getDatabaseUUID(),
            next_database_catalog_epoch,
            dependent_object_authority_capability_mask,
            next_inventory->getSummary().leaf_count,
            next_inventory->getSummary().merkle_radix_root,
            next_graph->computeRoot(),
            limits.authority_state);
    }
    catch (const AuthorityStateError & error)
    {
        if (error.code == AuthorityStateError::Code::LimitExceeded)
            fail(RootError::Code::LimitExceeded, "dependent-object admission authority state exceeds its limit");
        fail(RootError::Code::InvalidAuthorityState, "dependent-object admission authority state is invalid");
    }

    auto next_resource_usage_index = addResourceUsageObject(
        base.content_payload->resource_usage_index,
        expectation_addition,
        dependent_object,
        *base.content_payload->type_catalog,
        limits.resource_usage_index);
    const ResourceUsage base_usage = computeDatabaseOwnedResourceUsage(
        *base.content_payload->type_catalog,
        *base.content_payload->inventory,
        *base.content_payload->records,
        *base.content_payload->schema_graph,
        *base.content_payload->resource_usage_index,
        base.content_payload->verification_policy,
        base.content_payload->verification_maximum_targets_per_batch);
    const ResourceUsage replacement_usage = computeDatabaseOwnedResourceUsage(
        *base.content_payload->type_catalog,
        *next_inventory,
        *next_records,
        *next_graph,
        *next_resource_usage_index,
        base.content_payload->verification_policy,
        base.content_payload->verification_maximum_targets_per_batch);
    auto database_resource_quota
        = prepareDatabaseResourceQuotaReplacement(base.content_payload->database_resource_quota, base_usage, replacement_usage);
    const UInt64 logical_charge = computeContentPayloadLogicalCharge(
        *base.content_payload->type_catalog,
        *next_inventory,
        *next_records,
        *next_graph,
        *next_resource_usage_index,
        *database_resource_quota);
    auto content_payload = AuthorityRoot::ContentPayloadPtr(new AuthorityRoot::ContentPayload{
        .type_index_generation = base.content_payload->type_index_generation,
        .type_index_content_digest = base.content_payload->type_index_content_digest,
        .type_catalog = base.content_payload->type_catalog,
        .inventory = std::move(next_inventory),
        .schema_graph = std::move(next_graph),
        .records = std::move(next_records),
        .resource_usage_index = std::move(next_resource_usage_index),
        .database_resource_quota = std::move(database_resource_quota),
        .verification_policy = base.content_payload->verification_policy,
        .verification_maximum_targets_per_batch = base.content_payload->verification_maximum_targets_per_batch,
        .logical_charge = logical_charge,
        .physicalization_provenance = std::nullopt,
    });
    return AuthorityRoot::Ptr(new AuthorityRoot(std::move(next_state), std::move(content_payload)));
}

UInt64 AuthorityRootBuilder::getExpectationRecordInsertionLogicalChargeUpperBound()
{
    constexpr UInt64 expectation_key_nibbles = 2 * (sizeof(UInt8) + 2 * sizeof(CanonicalUUID));
    const UInt64 maximum_branch_charge = checkedAdd(
        recordNodeBaseLogicalCharge(),
        checkedMultiply(
            maximum_record_radix_children_capacity,
            sizeof(std::pair<UInt8, RecordNodePtr>),
            "authority expectation insertion bound overflows UInt64"),
        "authority expectation insertion bound overflows UInt64");
    UInt64 result
        = checkedMultiply(expectation_key_nibbles, maximum_branch_charge, "authority expectation insertion bound overflows UInt64");
    result = checkedAdd(result, recordNodeBaseLogicalCharge(), "authority expectation insertion bound overflows UInt64");
    result = checkedAdd(result, sizeof(SidecarExpectationRecord), "authority expectation insertion bound overflows UInt64");
    return result;
}

AuthorityRoot::Ptr AuthorityRootBuilder::buildDependentObjectExpectationReplacementDelta(
    const AuthorityRoot & base,
    UInt64 next_database_catalog_epoch,
    const SidecarExpectationRecord & expectation_replacement,
    const SchemaObjectDependencyGraphMutation & graph_delta,
    const AuthorityDependentObjectResourceImage & dependent_object,
    const AuthorityRootBuildLimits & limits)
{
    if (base.getPersistentCapabilityMask() != dependent_object_authority_capability_mask)
        fail(
            RootError::Code::InvalidAuthorityState,
            "dependent-object expectation replacement requires a dependent-object-capable authority root");
    if (base.getDatabaseCatalogEpoch() == std::numeric_limits<UInt64>::max()
        || next_database_catalog_epoch != base.getDatabaseCatalogEpoch() + 1)
        fail(RootError::Code::InvalidAuthorityState, "dependent-object expectation replacement catalog epoch is not the exact successor");
    if (!expectation_replacement.object.isValid() || expectation_replacement.object.kind == SchemaObjectKind::TypeDefinition
        || expectation_replacement.object.database_uuid != base.getDatabaseUUID() || !expectation_replacement.object_schema_revision)
        fail(RootError::Code::InvalidRecord, "dependent-object expectation replacement identity is invalid");

    const auto * before = base.findExpectationRecord(expectation_replacement.object);
    if (!before)
        fail(RootError::Code::InvalidRecord, "dependent-object expectation replacement is absent");
    if (before->object_schema_revision == std::numeric_limits<UInt64>::max()
        || expectation_replacement.object_schema_revision != before->object_schema_revision + 1
        || expectation_replacement.sidecar_hash == before->sidecar_hash || !before->installation_record_hash
        || !expectation_replacement.installation_record_hash
        || expectation_replacement.installation_record_hash == before->installation_record_hash)
    {
        fail(RootError::Code::InvalidRecord, "dependent-object expectation replacement is not an exact metadata-only successor");
    }
    if (!base.content_payload->schema_graph->containsNode(expectation_replacement.object))
        fail(RootError::Code::GraphMismatch, "dependent-object expectation replacement graph node is absent");
    if (!graph_delta.node_additions.empty() || !graph_delta.node_removals.empty())
        fail(RootError::Code::GraphMismatch, "dependent-object expectation replacement cannot add or remove graph nodes");
    const auto validate_edge = [&](const SchemaObjectDependencyEdge & edge)
    {
        if (!isValidRetainedDependentObjectEdge(base, expectation_replacement.object, edge))
        {
            fail(RootError::Code::GraphMismatch, "dependent-object expectation replacement graph edge is outside its object");
        }
    };
    for (const auto & edge : graph_delta.edge_additions)
        validate_edge(edge);
    for (const auto & edge : graph_delta.edge_removals)
        validate_edge(edge);

    const String encoded_expectation = validateExpectationRecord(expectation_replacement);
    const UInt64 before_bytes = toUInt64(encodeSidecarExpectationRecord(*before).size(), "expectation canonical bytes do not fit UInt64");
    const UInt64 after_bytes = toUInt64(encoded_expectation.size(), "expectation canonical bytes do not fit UInt64");
    UInt64 next_canonical_record_bytes = base.content_payload->records->canonical_record_bytes;
    if (before_bytes > next_canonical_record_bytes)
        fail(RootError::Code::InvalidRecord, "dependent-object expectation byte accounting underflows");
    next_canonical_record_bytes -= before_bytes;
    chargeCanonicalBytes(next_canonical_record_bytes, after_bytes, limits.maximum_canonical_record_bytes);

    const AuthorityInventoryKey key{
        .record_kind = AuthorityInventoryRecordKind::SidecarExpectation,
        .object_uuid = expectation_replacement.object.object_uuid,
    };
    const auto * before_leaf = base.content_payload->inventory->find(key);
    if (!before_leaf || before_leaf->object_revision != before->object_schema_revision
        || before_leaf->canonical_record_hash != computeSidecarExpectationRecordHash(*before))
        fail(RootError::Code::InventoryMismatch, "dependent-object expectation replacement base inventory is inconsistent");
    const AuthorityInventoryLeaf after_leaf{
        .key = key,
        .object_revision = expectation_replacement.object_schema_revision,
        .canonical_record_hash = computeSidecarExpectationRecordHash(expectation_replacement),
    };
    const AuthorityInventoryLeafDelta inventory_delta{.key = key, .before = *before_leaf, .after = after_leaf};
    AuthorityInventory::Ptr next_inventory;
    try
    {
        next_inventory = AuthorityInventory::applyMutation(
            base.content_payload->inventory, std::span<const AuthorityInventoryLeafDelta>(&inventory_delta, 1), limits.inventory);
    }
    catch (const AuthorityInventoryError & error)
    {
        if (error.code == AuthorityInventoryError::Code::LimitExceeded)
            fail(RootError::Code::LimitExceeded, "dependent-object expectation replacement inventory exceeds its limit");
        fail(RootError::Code::InventoryMismatch, "dependent-object expectation replacement inventory is inconsistent");
    }

    auto next_records = applyExpectationRecordReplacement(base.content_payload->records, expectation_replacement);
    if (next_records->canonical_record_bytes != next_canonical_record_bytes)
        fail(RootError::Code::InvalidRecord, "dependent-object expectation replacement byte accounting differs from validation");
    if (next_inventory->getSummary().leaf_count
        != checkedAdd(next_records->definition_count, next_records->expectation_count, "authority record count overflows UInt64"))
        fail(RootError::Code::InventoryMismatch, "dependent-object expectation replacement record and inventory counts differ");

    SchemaObjectDependencyGraph::Ptr next_graph;
    try
    {
        next_graph = SchemaObjectDependencyGraph::applyMutation(base.content_payload->schema_graph, graph_delta);
    }
    catch (const SchemaObjectDependencyGraphError & error)
    {
        if (error.code == SchemaObjectDependencyGraphError::Code::LimitExceeded)
            fail(RootError::Code::LimitExceeded, "dependent-object expectation replacement graph exceeds its limit");
        fail(RootError::Code::GraphMismatch, "dependent-object expectation replacement graph delta is inconsistent");
    }
    if (!next_graph->containsNode(expectation_replacement.object))
        fail(RootError::Code::GraphMismatch, "dependent-object expectation replacement lost its graph node");
    const auto resulting_dependencies = next_graph->getDependencies(expectation_replacement.object);
    if (std::none_of(
            resulting_dependencies.begin(),
            resulting_dependencies.end(),
            [](const auto & dependency) { return dependency.kind == SchemaObjectDependencyEdgeKind::ObjectDependsOnDefinition; }))
    {
        fail(RootError::Code::GraphMismatch, "dependent-object expectation replacement lost its definition dependency");
    }

    AuthorityState next_state;
    try
    {
        next_state = makeAuthorityState(
            base.getDatabaseUUID(),
            next_database_catalog_epoch,
            dependent_object_authority_capability_mask,
            next_inventory->getSummary().leaf_count,
            next_inventory->getSummary().merkle_radix_root,
            next_graph->computeRoot(),
            limits.authority_state);
    }
    catch (const AuthorityStateError & error)
    {
        if (error.code == AuthorityStateError::Code::LimitExceeded)
            fail(RootError::Code::LimitExceeded, "dependent-object expectation replacement authority state exceeds its limit");
        fail(RootError::Code::InvalidAuthorityState, "dependent-object expectation replacement authority state is invalid");
    }

    auto next_resource_usage_index = replaceResourceUsageObject(
        base.content_payload->resource_usage_index,
        expectation_replacement,
        dependent_object,
        *base.content_payload->type_catalog,
        limits.resource_usage_index);
    const ResourceUsage base_usage = computeDatabaseOwnedResourceUsage(
        *base.content_payload->type_catalog,
        *base.content_payload->inventory,
        *base.content_payload->records,
        *base.content_payload->schema_graph,
        *base.content_payload->resource_usage_index,
        base.content_payload->verification_policy,
        base.content_payload->verification_maximum_targets_per_batch);
    const ResourceUsage replacement_usage = computeDatabaseOwnedResourceUsage(
        *base.content_payload->type_catalog,
        *next_inventory,
        *next_records,
        *next_graph,
        *next_resource_usage_index,
        base.content_payload->verification_policy,
        base.content_payload->verification_maximum_targets_per_batch);
    auto database_resource_quota
        = prepareDatabaseResourceQuotaReplacement(base.content_payload->database_resource_quota, base_usage, replacement_usage);
    const UInt64 logical_charge = computeContentPayloadLogicalCharge(
        *base.content_payload->type_catalog,
        *next_inventory,
        *next_records,
        *next_graph,
        *next_resource_usage_index,
        *database_resource_quota);
    auto content_payload = AuthorityRoot::ContentPayloadPtr(new AuthorityRoot::ContentPayload{
        .type_index_generation = base.content_payload->type_index_generation,
        .type_index_content_digest = base.content_payload->type_index_content_digest,
        .type_catalog = base.content_payload->type_catalog,
        .inventory = std::move(next_inventory),
        .schema_graph = std::move(next_graph),
        .records = std::move(next_records),
        .resource_usage_index = std::move(next_resource_usage_index),
        .database_resource_quota = std::move(database_resource_quota),
        .verification_policy = base.content_payload->verification_policy,
        .verification_maximum_targets_per_batch = base.content_payload->verification_maximum_targets_per_batch,
        .logical_charge = logical_charge,
        .physicalization_provenance = std::nullopt,
    });
    return AuthorityRoot::Ptr(new AuthorityRoot(std::move(next_state), std::move(content_payload)));
}

}
