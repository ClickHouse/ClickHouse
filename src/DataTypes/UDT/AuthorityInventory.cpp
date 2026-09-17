#include <DataTypes/UDT/AuthorityInventory.h>

#include <algorithm>
#include <array>
#include <limits>
#include <utility>

namespace DB::UDT
{

struct AuthorityInventoryNode
{
    using Ptr = std::shared_ptr<const AuthorityInventoryNode>;
    using KeyBytes = std::array<CanonicalByte, sizeof(UInt16) + sizeof(UInt8) + sizeof(CanonicalUUID)>;

    bool is_leaf = false;
    KeyBytes representative_key{};
    AuthorityInventoryLeaf leaf;
    UInt16 branch_depth = 0;
    std::vector<std::pair<UInt8, Ptr>> children;
    Digest digest{};
    UInt64 subtree_node_count = 0;
    UInt64 subtree_leaf_count = 0;
    UInt64 subtree_accounted_bytes = 0;
};

namespace
{

constexpr std::string_view inventory_empty_domain = "ClickHouse UDT authority inventory empty V1";
constexpr std::string_view inventory_leaf_domain = "ClickHouse UDT authority inventory leaf V1";
constexpr std::string_view inventory_branch_domain = "ClickHouse UDT authority inventory branch V1";
constexpr size_t inventory_key_bytes = sizeof(UInt16) + sizeof(UInt8) + sizeof(CanonicalUUID);
constexpr size_t inventory_leaf_bytes = inventory_key_bytes + sizeof(UInt64) + sizeof(Digest);
constexpr size_t inventory_key_nibbles = inventory_key_bytes * 2;
constexpr UInt64 maximum_radix_children_capacity = 32;

[[noreturn]] void fail(AuthorityInventoryError::Code code, std::string_view message)
{
    throw AuthorityInventoryError(code, message);
}

bool isKnownRecordKind(AuthorityInventoryRecordKind kind) noexcept
{
    return kind == AuthorityInventoryRecordKind::TypeDefinition || kind == AuthorityInventoryRecordKind::SidecarExpectation;
}

UInt8 encodeRecordKind(AuthorityInventoryRecordKind kind)
{
    switch (kind)
    {
        case AuthorityInventoryRecordKind::TypeDefinition: return 1;
        case AuthorityInventoryRecordKind::SidecarExpectation: return 2;
    }
    fail(AuthorityInventoryError::Code::InvalidValue, "unknown authority-inventory record kind");
}

AuthorityInventoryRecordKind decodeRecordKind(UInt8 value)
{
    switch (value)
    {
        case 1: return AuthorityInventoryRecordKind::TypeDefinition;
        case 2: return AuthorityInventoryRecordKind::SidecarExpectation;
        default: fail(AuthorityInventoryError::Code::InvalidValue, "unknown authority-inventory record kind");
    }
}

void validateLimits(const AuthorityInventoryLimits & limits)
{
    constexpr UInt64 maximum_implementation_leaves = 1ULL << 24;
    constexpr UInt64 maximum_implementation_record_bytes = 1ULL << 20;
    if (limits.maximum_leaves == 0 || limits.maximum_leaf_bytes == 0)
        fail(AuthorityInventoryError::Code::InvalidValue, "every authority-inventory limit must be nonzero");
    if (limits.maximum_leaves > maximum_implementation_leaves || limits.maximum_leaf_bytes > maximum_implementation_record_bytes)
        fail(AuthorityInventoryError::Code::InvalidValue, "an authority-inventory limit exceeds the implementation maximum");
}

void validateKey(const AuthorityInventoryKey & key)
{
    if (key.format_version != authority_inventory_format_version)
        fail(AuthorityInventoryError::Code::UnsupportedVersion, "unsupported authority-inventory key version");
    if (!isKnownRecordKind(key.record_kind))
        fail(AuthorityInventoryError::Code::InvalidValue, "unknown authority-inventory record kind");
    if (key.object_uuid == UUIDHelpers::Nil)
        fail(AuthorityInventoryError::Code::InvalidValue, "authority-inventory object UUID is nil");
}

void validateLeaf(const AuthorityInventoryLeaf & leaf)
{
    validateKey(leaf.key);
    if (leaf.object_revision == 0)
        fail(AuthorityInventoryError::Code::InvalidValue, "authority-inventory object revision is zero");
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

class Reader final
{
public:
    explicit Reader(std::string_view bytes_)
        : bytes(bytes_)
    {
    }

    UInt8 readByte()
    {
        require(sizeof(UInt8));
        return static_cast<UInt8>(bytes[position++]);
    }

    UInt16 readUInt16LE()
    {
        require(sizeof(UInt16));
        const UInt16 result = static_cast<UInt16>(
            static_cast<UInt8>(bytes[position]) | (static_cast<UInt16>(static_cast<UInt8>(bytes[position + 1])) << 8));
        position += sizeof(UInt16);
        return result;
    }

    UInt64 readUInt64LE()
    {
        require(sizeof(UInt64));
        UInt64 result = 0;
        for (size_t index = 0; index < sizeof(UInt64); ++index)
            result |= static_cast<UInt64>(static_cast<UInt8>(bytes[position + index])) << (8 * index);
        position += sizeof(UInt64);
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
            fail(AuthorityInventoryError::Code::TrailingData, "authority-inventory record has trailing data");
    }

private:
    void require(size_t count) const
    {
        if (count > bytes.size() - position)
            fail(AuthorityInventoryError::Code::Truncated, "authority-inventory record is truncated");
    }

    std::string_view bytes;
    size_t position = 0;
};

using InventoryKeyBytes = std::array<CanonicalByte, inventory_key_bytes>;
using InventoryNode = AuthorityInventoryNode;
using InventoryNodePtr = InventoryNode::Ptr;

UInt64 checkedAccountingAdd(UInt64 lhs, UInt64 rhs)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        fail(AuthorityInventoryError::Code::LimitExceeded, "authority-inventory accounted bytes overflow UInt64");
    return lhs + rhs;
}

UInt64 checkedAccountingMultiply(UInt64 lhs, UInt64 rhs)
{
    if (lhs != 0 && rhs > std::numeric_limits<UInt64>::max() / lhs)
        fail(AuthorityInventoryError::Code::LimitExceeded, "authority-inventory accounted bytes overflow UInt64");
    return lhs * rhs;
}

UInt64 checkedNodeCountAdd(UInt64 lhs, UInt64 rhs)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        fail(AuthorityInventoryError::Code::LimitExceeded, "authority-inventory node count overflows UInt64");
    return lhs + rhs;
}

UInt64 inventoryNodeBaseAccountedBytes()
{
    return sizeof(InventoryNode) + 2 * sizeof(void *);
}

InventoryKeyBytes encodeKey(const AuthorityInventoryKey & key)
{
    validateKey(key);
    InventoryKeyBytes result{};
    result[0] = static_cast<CanonicalByte>(key.format_version);
    result[1] = static_cast<CanonicalByte>(key.format_version >> 8);
    result[2] = encodeRecordKind(key.record_kind);
    const auto uuid = uuidToCanonicalBytes(key.object_uuid);
    std::copy(uuid.begin(), uuid.end(), result.begin() + 3);
    return result;
}

void appendBytes(String & output, std::span<const CanonicalByte> bytes)
{
    output.append(reinterpret_cast<const char *>(bytes.data()), bytes.size());
}

UInt8 keyNibble(const AuthorityInventoryLeaf & leaf, size_t depth)
{
    const auto key = encodeKey(leaf.key);
    const UInt8 byte = key[depth / 2];
    return depth % 2 == 0 ? static_cast<UInt8>(byte >> 4) : static_cast<UInt8>(byte & 0x0f);
}

Digest hashLeaf(const AuthorityInventoryLeaf & leaf, UInt64 * hashed_node_count)
{
    if (hashed_node_count)
        ++*hashed_node_count;
    const auto key = encodeKey(leaf.key);
    String payload;
    payload.reserve(inventory_key_bytes + sizeof(UInt64) + sizeof(Digest));
    appendBytes(payload, key);
    appendUInt64LE(payload, leaf.object_revision);
    appendBytes(payload, leaf.canonical_record_hash);
    return hashFramedDomainSeparated(inventory_leaf_domain, payload);
}

Digest hashBranch(UInt16 depth, std::span<const std::pair<UInt8, InventoryNodePtr>> children, UInt64 * hashed_node_count)
{
    UInt16 present = 0;
    String branch;
    branch.reserve(2 * sizeof(UInt16) + children.size() * (sizeof(UInt8) + sizeof(Digest)));
    appendUInt16LE(branch, depth);
    for (const auto & [nibble, child] : children)
    {
        if (!child || nibble >= 16 || (present & (UInt16{1} << nibble)))
            fail(AuthorityInventoryError::Code::NonCanonical, "authority-inventory radix branch is invalid");
        present = static_cast<UInt16>(present | (UInt16{1} << nibble));
    }
    appendUInt16LE(branch, present);
    for (const auto & [nibble, child] : children)
    {
        branch.push_back(static_cast<char>(nibble));
        appendBytes(branch, child->digest);
    }
    if (hashed_node_count)
        ++*hashed_node_count;
    return hashFramedDomainSeparated(inventory_branch_domain, branch);
}

InventoryNodePtr makeLeafNode(const AuthorityInventoryLeaf & leaf, AuthorityInventoryMutationStatistics * statistics = nullptr)
{
    auto node = std::make_shared<InventoryNode>();
    node->is_leaf = true;
    node->representative_key = encodeKey(leaf.key);
    node->leaf = leaf;
    node->digest = hashLeaf(leaf, statistics ? &statistics->nodes_hashed : nullptr);
    node->subtree_node_count = 1;
    node->subtree_leaf_count = 1;
    node->subtree_accounted_bytes = inventoryNodeBaseAccountedBytes();
    if (statistics)
        ++statistics->nodes_created;
    return node;
}

InventoryNodePtr makeBranchNode(
    UInt16 depth, std::vector<std::pair<UInt8, InventoryNodePtr>> children, AuthorityInventoryMutationStatistics * statistics = nullptr)
{
    if (children.size() < 2 || depth >= inventory_key_nibbles)
        fail(AuthorityInventoryError::Code::NonCanonical, "authority-inventory radix branch shape is invalid");
    std::sort(children.begin(), children.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });
    if (children.capacity() > maximum_radix_children_capacity)
        fail(AuthorityInventoryError::Code::LimitExceeded, "authority-inventory radix child capacity exceeds its implementation bound");
    auto node = std::make_shared<InventoryNode>();
    node->representative_key = children.front().second->representative_key;
    node->branch_depth = depth;
    node->children = std::move(children);
    node->digest = hashBranch(depth, node->children, statistics ? &statistics->nodes_hashed : nullptr);
    node->subtree_node_count = 1;
    node->subtree_leaf_count = 0;
    node->subtree_accounted_bytes = checkedAccountingAdd(
        inventoryNodeBaseAccountedBytes(),
        checkedAccountingMultiply(static_cast<UInt64>(node->children.capacity()), sizeof(std::pair<UInt8, InventoryNodePtr>)));
    for (const auto & [_, child] : node->children)
    {
        node->subtree_node_count = checkedNodeCountAdd(node->subtree_node_count, child->subtree_node_count);
        node->subtree_leaf_count = checkedNodeCountAdd(node->subtree_leaf_count, child->subtree_leaf_count);
        node->subtree_accounted_bytes = checkedAccountingAdd(node->subtree_accounted_bytes, child->subtree_accounted_bytes);
    }
    if (statistics)
        ++statistics->nodes_created;
    return node;
}

UInt8 keyNibble(const InventoryKeyBytes & key, size_t depth) noexcept
{
    const UInt8 byte = key[depth / 2];
    return depth % 2 == 0 ? static_cast<UInt8>(byte >> 4) : static_cast<UInt8>(byte & 0x0f);
}

size_t firstDifferentNibble(const InventoryKeyBytes & lhs, const InventoryKeyBytes & rhs) noexcept
{
    for (size_t depth = 0; depth < inventory_key_nibbles; ++depth)
        if (keyNibble(lhs, depth) != keyNibble(rhs, depth))
            return depth;
    return inventory_key_nibbles;
}

InventoryNodePtr buildPersistentTree(std::span<const AuthorityInventoryLeaf> leaves, size_t begin, size_t end, size_t depth)
{
    if (begin == end)
        return {};
    if (end - begin == 1)
        return makeLeafNode(leaves[begin]);
    if (depth == inventory_key_nibbles)
        fail(AuthorityInventoryError::Code::NonCanonical, "duplicate authority-inventory key");

    std::vector<std::pair<UInt8, InventoryNodePtr>> children;
    size_t cursor = begin;
    while (cursor < end)
    {
        const UInt8 nibble = keyNibble(encodeKey(leaves[cursor].key), depth);
        const size_t child_begin = cursor;
        do
        {
            ++cursor;
        } while (cursor < end && keyNibble(encodeKey(leaves[cursor].key), depth) == nibble);
        children.emplace_back(nibble, buildPersistentTree(leaves, child_begin, cursor, depth + 1));
    }
    if (children.size() == 1)
        return children.front().second;
    return makeBranchNode(static_cast<UInt16>(depth), std::move(children));
}

const AuthorityInventoryLeaf * findPersistentLeaf(const InventoryNodePtr & root, const InventoryKeyBytes & key) noexcept
{
    auto node = root;
    while (node)
    {
        if (node->is_leaf)
            return node->representative_key == key ? &node->leaf : nullptr;
        const UInt8 nibble = keyNibble(key, node->branch_depth);
        const auto child = std::lower_bound(
            node->children.begin(),
            node->children.end(),
            nibble,
            [](const auto & candidate, UInt8 value) { return candidate.first < value; });
        if (child == node->children.end() || child->first != nibble)
            return nullptr;
        node = child->second;
    }
    return nullptr;
}

InventoryNodePtr insertPersistentLeaf(
    const InventoryNodePtr & root,
    const AuthorityInventoryLeaf & leaf,
    const InventoryKeyBytes & key,
    AuthorityInventoryMutationStatistics * statistics)
{
    if (statistics && root)
        ++statistics->nodes_visited;
    if (!root)
        return makeLeafNode(leaf, statistics);
    if (root->is_leaf)
    {
        if (root->representative_key == key)
            return makeLeafNode(leaf, statistics);
        const size_t depth = firstDifferentNibble(root->representative_key, key);
        auto replacement = makeLeafNode(leaf, statistics);
        return makeBranchNode(
            static_cast<UInt16>(depth),
            {{keyNibble(root->representative_key, depth), root}, {keyNibble(key, depth), std::move(replacement)}},
            statistics);
    }

    const size_t diverging_depth = firstDifferentNibble(root->representative_key, key);
    if (diverging_depth < root->branch_depth)
    {
        auto replacement = makeLeafNode(leaf, statistics);
        return makeBranchNode(
            static_cast<UInt16>(diverging_depth),
            {{keyNibble(root->representative_key, diverging_depth), root}, {keyNibble(key, diverging_depth), std::move(replacement)}},
            statistics);
    }

    const UInt8 nibble = keyNibble(key, root->branch_depth);
    auto children = root->children;
    auto child = std::lower_bound(
        children.begin(), children.end(), nibble, [](const auto & candidate, UInt8 value) { return candidate.first < value; });
    if (child == children.end() || child->first != nibble)
    {
        children.insert(child, {nibble, makeLeafNode(leaf, statistics)});
    }
    else
    {
        child->second = insertPersistentLeaf(child->second, leaf, key, statistics);
    }
    return makeBranchNode(root->branch_depth, std::move(children), statistics);
}

InventoryNodePtr
removePersistentLeaf(const InventoryNodePtr & root, const InventoryKeyBytes & key, AuthorityInventoryMutationStatistics * statistics)
{
    if (!root)
        fail(AuthorityInventoryError::Code::NonCanonical, "authority-inventory radix removal misses its leaf");
    if (statistics)
        ++statistics->nodes_visited;
    if (root->is_leaf)
    {
        if (root->representative_key != key)
            fail(AuthorityInventoryError::Code::NonCanonical, "authority-inventory radix removal misses its leaf");
        return {};
    }

    const UInt8 nibble = keyNibble(key, root->branch_depth);
    auto children = root->children;
    auto child = std::lower_bound(
        children.begin(), children.end(), nibble, [](const auto & candidate, UInt8 value) { return candidate.first < value; });
    if (child == children.end() || child->first != nibble)
        fail(AuthorityInventoryError::Code::NonCanonical, "authority-inventory radix removal misses its branch");
    auto replacement = removePersistentLeaf(child->second, key, statistics);
    if (replacement)
        child->second = std::move(replacement);
    else
        children.erase(child);
    if (children.size() == 1)
        return children.front().second;
    return makeBranchNode(root->branch_depth, std::move(children), statistics);
}

void materializePersistentLeaves(
    const InventoryNodePtr & root,
    std::vector<AuthorityInventoryLeaf> & output,
    AuthorityInventoryMutationStatistics * statistics = nullptr)
{
    if (!root)
        return;
    if (root->is_leaf)
    {
        output.push_back(root->leaf);
        if (statistics)
            ++statistics->leaves_materialized;
        return;
    }
    for (const auto & [nibble, child] : root->children)
    {
        static_cast<void>(nibble);
        materializePersistentLeaves(child, output, statistics);
    }
}

Digest hashRange(std::span<const AuthorityInventoryLeaf> leaves, size_t begin, size_t end, size_t depth, UInt64 * hashed_node_count)
{
    if (end - begin == 1)
        return hashLeaf(leaves[begin], hashed_node_count);
    if (depth == inventory_key_nibbles)
        fail(AuthorityInventoryError::Code::NonCanonical, "duplicate authority-inventory key");

    std::array<std::pair<size_t, size_t>, 16> ranges{};
    UInt16 present = 0;
    size_t child_count = 0;
    UInt8 only_child = 0;
    size_t cursor = begin;
    while (cursor < end)
    {
        const UInt8 nibble = keyNibble(leaves[cursor], depth);
        const size_t child_begin = cursor;
        do
        {
            ++cursor;
        } while (cursor < end && keyNibble(leaves[cursor], depth) == nibble);
        ranges[nibble] = {child_begin, cursor};
        present = static_cast<UInt16>(present | (UInt16{1} << nibble));
        ++child_count;
        only_child = nibble;
    }

    if (child_count == 1)
    {
        const auto [child_begin, child_end] = ranges[only_child];
        return hashRange(leaves, child_begin, child_end, depth + 1, hashed_node_count);
    }

    String branch;
    branch.reserve(2 * sizeof(UInt16) + child_count * (sizeof(UInt8) + sizeof(Digest)));
    appendUInt16LE(branch, static_cast<UInt16>(depth));
    appendUInt16LE(branch, present);
    for (UInt8 nibble = 0; nibble < 16; ++nibble)
    {
        if ((present & (UInt16{1} << nibble)) == 0)
            continue;
        branch.push_back(static_cast<char>(nibble));
        const auto [child_begin, child_end] = ranges[nibble];
        const Digest child_hash = hashRange(leaves, child_begin, child_end, depth + 1, hashed_node_count);
        appendBytes(branch, child_hash);
    }
    if (hashed_node_count)
        ++*hashed_node_count;
    return hashFramedDomainSeparated(inventory_branch_domain, branch);
}

void validateSortedLeaves(std::span<const AuthorityInventoryLeaf> leaves, const AuthorityInventoryLimits & limits)
{
    validateLimits(limits);
    if (leaves.size() > limits.maximum_leaves)
        fail(AuthorityInventoryError::Code::LimitExceeded, "authority-inventory leaf count exceeds its limit");
    if (!leaves.empty() && inventory_leaf_bytes > limits.maximum_leaf_bytes)
        fail(AuthorityInventoryError::Code::LimitExceeded, "authority-inventory leaf exceeds its byte limit");
    for (const auto & leaf : leaves)
        validateLeaf(leaf);
    for (size_t index = 1; index < leaves.size(); ++index)
    {
        if (!authorityInventoryLeafLess(leaves[index - 1], leaves[index]))
            fail(AuthorityInventoryError::Code::NonCanonical, "authority-inventory leaves are not strictly key-sorted");
    }
}

}

AuthorityInventoryError::AuthorityInventoryError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

bool authorityInventoryKeyLess(const AuthorityInventoryKey & lhs, const AuthorityInventoryKey & rhs) noexcept
{
    if (lhs.format_version != rhs.format_version)
        return lhs.format_version < rhs.format_version;
    const UInt8 lhs_kind = static_cast<UInt8>(lhs.record_kind);
    const UInt8 rhs_kind = static_cast<UInt8>(rhs.record_kind);
    if (lhs_kind != rhs_kind)
        return lhs_kind < rhs_kind;
    return uuidToCanonicalBytes(lhs.object_uuid) < uuidToCanonicalBytes(rhs.object_uuid);
}

bool authorityInventoryLeafLess(const AuthorityInventoryLeaf & lhs, const AuthorityInventoryLeaf & rhs) noexcept
{
    return authorityInventoryKeyLess(lhs.key, rhs.key);
}

String encodeAuthorityInventoryLeaf(const AuthorityInventoryLeaf & leaf, const AuthorityInventoryLimits & limits)
{
    validateLimits(limits);
    validateLeaf(leaf);
    if (inventory_leaf_bytes > limits.maximum_leaf_bytes)
        fail(AuthorityInventoryError::Code::LimitExceeded, "authority-inventory leaf exceeds its byte limit");

    String result;
    result.reserve(inventory_leaf_bytes);
    appendBytes(result, encodeKey(leaf.key));
    appendUInt64LE(result, leaf.object_revision);
    appendBytes(result, leaf.canonical_record_hash);
    return result;
}

AuthorityInventoryLeaf decodeAuthorityInventoryLeaf(std::string_view bytes, const AuthorityInventoryLimits & limits)
{
    validateLimits(limits);
    if (bytes.size() > limits.maximum_leaf_bytes)
        fail(AuthorityInventoryError::Code::LimitExceeded, "authority-inventory leaf exceeds its byte limit");

    Reader reader(bytes);
    AuthorityInventoryLeaf result;
    result.key.format_version = reader.readUInt16LE();
    if (result.key.format_version != authority_inventory_format_version)
        fail(AuthorityInventoryError::Code::UnsupportedVersion, "unsupported authority-inventory leaf version");
    result.key.record_kind = decodeRecordKind(reader.readByte());
    result.key.object_uuid = uuidFromCanonicalBytes(reader.readArray<16>());
    result.object_revision = reader.readUInt64LE();
    result.canonical_record_hash = reader.readArray<32>();
    reader.requireEnd();
    validateLeaf(result);
    return result;
}

Digest computeAuthorityInventoryRoot(
    std::span<const AuthorityInventoryLeaf> sorted_leaves, const AuthorityInventoryLimits & limits, UInt64 * hashed_node_count)
{
    validateSortedLeaves(sorted_leaves, limits);
    if (hashed_node_count)
        *hashed_node_count = 0;
    if (sorted_leaves.empty())
    {
        if (hashed_node_count)
            *hashed_node_count = 1;
        return hashFramedDomainSeparated(inventory_empty_domain, std::string_view{});
    }
    return hashRange(sorted_leaves, 0, sorted_leaves.size(), 0, hashed_node_count);
}

AuthorityInventorySummary
buildAuthorityInventorySummary(std::span<const AuthorityInventoryLeaf> sorted_leaves, const AuthorityInventoryLimits & limits)
{
    return {
        .leaf_count = sorted_leaves.size(),
        .merkle_radix_root = computeAuthorityInventoryRoot(sorted_leaves, limits),
    };
}

AuthorityInventory::Ptr AuthorityInventory::create(
    AuthorityInventorySummary summary, std::vector<AuthorityInventoryLeaf> sorted_leaves, const AuthorityInventoryLimits & limits)
{
    validateSortedLeaves(sorted_leaves, limits);
    if (summary.leaf_count != sorted_leaves.size())
        fail(AuthorityInventoryError::Code::InvalidValue, "authority-inventory summary leaf count is inconsistent");
    auto root = buildPersistentTree(sorted_leaves, 0, sorted_leaves.size(), 0);
    const Digest actual_root = root ? root->digest : hashFramedDomainSeparated(inventory_empty_domain, std::string_view{});
    if (summary.merkle_radix_root != actual_root || summary.merkle_radix_root != computeAuthorityInventoryRoot(sorted_leaves, limits))
        fail(AuthorityInventoryError::Code::InvalidValue, "authority-inventory summary root is inconsistent");
    return Ptr(new AuthorityInventory(std::move(summary), std::move(root), std::move(sorted_leaves)));
}

AuthorityInventory::AuthorityInventory(
    AuthorityInventorySummary summary_, NodePtr root_, std::vector<AuthorityInventoryLeaf> materialized_leaves_)
    : summary(std::move(summary_))
    , root(std::move(root_))
    , accounted_bytes(checkedAccountingAdd(
          checkedAccountingAdd(sizeof(AuthorityInventory) + 2 * sizeof(void *), root ? root->subtree_accounted_bytes : 0),
          checkedAccountingMultiply(
              std::max(summary.leaf_count, static_cast<UInt64>(materialized_leaves_.capacity())), sizeof(AuthorityInventoryLeaf))))
    , materialized_leaves(std::move(materialized_leaves_))
{
}

AuthorityInventory::~AuthorityInventory() = default;

UInt64 AuthorityInventory::getNodeCount() const noexcept
{
    return root ? root->subtree_node_count : 1;
}

UInt64 AuthorityInventory::getSingleLeafInsertionAccountedBytesUpperBound()
{
    const UInt64 maximum_branch_charge = checkedAccountingAdd(
        inventoryNodeBaseAccountedBytes(),
        checkedAccountingMultiply(maximum_radix_children_capacity, sizeof(std::pair<UInt8, InventoryNodePtr>)));
    UInt64 result = checkedAccountingMultiply(inventory_key_nibbles, maximum_branch_charge);
    result = checkedAccountingAdd(result, inventoryNodeBaseAccountedBytes());
    result = checkedAccountingAdd(result, sizeof(AuthorityInventoryLeaf));
    return result;
}

AuthorityInventory::Ptr AuthorityInventory::applyMutation(
    const Ptr & base,
    std::span<const AuthorityInventoryLeafDelta> sorted_deltas,
    const AuthorityInventoryLimits & limits,
    AuthorityInventoryMutationStatistics * statistics)
{
    if (statistics)
        *statistics = {};
    validateLimits(limits);
    if (!base)
        fail(AuthorityInventoryError::Code::InvalidValue, "authority-inventory mutation has no base root");
    if (base->summary.leaf_count > limits.maximum_leaves)
        fail(AuthorityInventoryError::Code::LimitExceeded, "authority-inventory base leaf count exceeds its limit");
    if (base->summary.leaf_count && inventory_leaf_bytes > limits.maximum_leaf_bytes)
        fail(AuthorityInventoryError::Code::LimitExceeded, "authority-inventory leaf exceeds its byte limit");

    UInt64 additions = 0;
    UInt64 removals = 0;
    const AuthorityInventoryKey * previous_key = nullptr;
    for (const auto & delta : sorted_deltas)
    {
        validateKey(delta.key);
        if (previous_key && !authorityInventoryKeyLess(*previous_key, delta.key))
            fail(AuthorityInventoryError::Code::NonCanonical, "authority-inventory deltas are not strictly key-sorted");
        previous_key = &delta.key;
        if (!delta.before && !delta.after)
            fail(AuthorityInventoryError::Code::InvalidValue, "authority-inventory delta has no image");
        if (delta.before)
        {
            validateLeaf(*delta.before);
            if (delta.before->key != delta.key)
                fail(AuthorityInventoryError::Code::InvalidValue, "authority-inventory before-image key differs from its delta");
        }
        if (delta.after)
        {
            validateLeaf(*delta.after);
            if (delta.after->key != delta.key)
                fail(AuthorityInventoryError::Code::InvalidValue, "authority-inventory after-image key differs from its delta");
        }

        const auto encoded_key = encodeKey(delta.key);
        const auto * actual = findPersistentLeaf(base->root, encoded_key);
        if (delta.before)
        {
            if (!actual || *actual != *delta.before)
                fail(AuthorityInventoryError::Code::NonCanonical, "authority-inventory before-image differs from the base leaf");
        }
        else if (actual)
        {
            fail(AuthorityInventoryError::Code::NonCanonical, "authority-inventory addition replaces an existing leaf");
        }

        if (!actual)
            ++additions;
        else if (!delta.after)
            ++removals;
    }

    if (removals > base->summary.leaf_count)
        fail(AuthorityInventoryError::Code::NonCanonical, "authority-inventory removal count exceeds its base leaf count");
    const UInt64 retained_count = base->summary.leaf_count - removals;
    if (additions > limits.maximum_leaves - retained_count)
        fail(AuthorityInventoryError::Code::LimitExceeded, "authority-inventory mutation exceeds its final leaf limit");
    const UInt64 next_count = retained_count + additions;
    if (next_count && inventory_leaf_bytes > limits.maximum_leaf_bytes)
        fail(AuthorityInventoryError::Code::LimitExceeded, "authority-inventory leaf exceeds its byte limit");
    if (sorted_deltas.empty())
        return base;

    InventoryNodePtr next_root = base->root;
    for (const auto & delta : sorted_deltas)
    {
        const auto encoded_key = encodeKey(delta.key);
        if (delta.after)
            next_root = insertPersistentLeaf(next_root, *delta.after, encoded_key, statistics);
        else
            next_root = removePersistentLeaf(next_root, encoded_key, statistics);
        if (statistics)
            ++statistics->deltas_applied;
    }

    AuthorityInventorySummary next_summary{
        .leaf_count = next_count,
        .merkle_radix_root = next_root ? next_root->digest : hashFramedDomainSeparated(inventory_empty_domain, std::string_view{}),
    };
    return Ptr(new AuthorityInventory(std::move(next_summary), std::move(next_root), {}));
}

std::span<const AuthorityInventoryLeaf> AuthorityInventory::getLeaves() const
{
    std::call_once(
        materialize_once,
        [&]
        {
            if (materialized_leaves.size() == summary.leaf_count)
                return;
            materialized_leaves.clear();
            materialized_leaves.reserve(static_cast<size_t>(summary.leaf_count));
            materializePersistentLeaves(root, materialized_leaves);
        });
    return materialized_leaves;
}

const AuthorityInventoryLeaf * AuthorityInventory::getLeafByCanonicalIndex(UInt64 index) const noexcept
{
    if (!root || root->subtree_leaf_count != summary.leaf_count || index >= summary.leaf_count)
        return nullptr;
    auto node = root;
    while (node && !node->is_leaf)
    {
        UInt64 child_leaf_count = 0;
        UInt8 previous_nibble = 0;
        bool have_previous = false;
        for (const auto & [nibble, child] : node->children)
        {
            /// makeBranchNode() sorts and canonical hashing rejects duplicate
            /// nibbles. Recheck here so an invalid in-memory node cannot make
            /// indexed order differ from materializePersistentLeaves().
            if (!child || child->subtree_leaf_count == 0 || (have_previous && previous_nibble >= nibble))
                return nullptr;
            previous_nibble = nibble;
            have_previous = true;
            if (child->subtree_leaf_count > std::numeric_limits<UInt64>::max() - child_leaf_count)
                return nullptr;
            child_leaf_count += child->subtree_leaf_count;
        }
        if (child_leaf_count != node->subtree_leaf_count)
            return nullptr;

        bool descended = false;
        for (const auto & [_, child] : node->children)
        {
            if (index < child->subtree_leaf_count)
            {
                node = child;
                descended = true;
                break;
            }
            index -= child->subtree_leaf_count;
        }
        if (!descended)
            return nullptr;
    }
    return node && node->is_leaf && node->subtree_leaf_count == 1 && index == 0 ? &node->leaf : nullptr;
}

const AuthorityInventoryLeaf * AuthorityInventory::find(const AuthorityInventoryKey & key) const noexcept
{
    try
    {
        return findPersistentLeaf(root, encodeKey(key));
    }
    catch (...)
    {
        return nullptr;
    }
}

bool AuthorityInventory::sharesRootNodeWith(const AuthorityInventory & other) const noexcept
{
    return root == other.root;
}

}
