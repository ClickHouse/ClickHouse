#pragma once

#include <DataTypes/UDT/CanonicalHash.h>

#include <Core/Types.h>
#include <Core/UUID.h>

#include <compare>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB::UDT
{

inline constexpr UInt16 authority_inventory_format_version = 1;

/// Values are local model tags. The codec below maps every value explicitly;
/// callers must not persist the compiler enum ordinal through another codec.
enum class AuthorityInventoryRecordKind : UInt8
{
    TypeDefinition = 1,
    SidecarExpectation = 2,
};

struct AuthorityInventoryKey
{
    UInt16 format_version = authority_inventory_format_version;
    AuthorityInventoryRecordKind record_kind = AuthorityInventoryRecordKind::TypeDefinition;
    UUID object_uuid = UUIDHelpers::Nil;

    bool operator==(const AuthorityInventoryKey &) const = default;
};

struct AuthorityInventoryLeaf
{
    AuthorityInventoryKey key;
    UInt64 object_revision = 0;
    Digest canonical_record_hash{};

    bool operator==(const AuthorityInventoryLeaf &) const = default;
};

struct AuthorityInventorySummary
{
    UInt64 leaf_count = 0;
    Digest merkle_radix_root{};

    bool operator==(const AuthorityInventorySummary &) const = default;
};

struct AuthorityInventoryLimits
{
    UInt64 maximum_leaves = 200'000;
    UInt64 maximum_leaf_bytes = 128;
};

/// Exact immutable-leaf edit. Deltas must be strictly key-sorted; each
/// before-image is matched against the pinned base and each after-image must
/// use `key`. At least one image must be present.
struct AuthorityInventoryLeafDelta
{
    AuthorityInventoryKey key;
    std::optional<AuthorityInventoryLeaf> before;
    std::optional<AuthorityInventoryLeaf> after;
};

/// Work performed by one persistent-radix mutation. The key depth is frozen
/// by V1, so these counters are proportional only to touched leaves.
struct AuthorityInventoryMutationStatistics
{
    UInt64 deltas_applied = 0;
    UInt64 nodes_visited = 0;
    UInt64 nodes_created = 0;
    UInt64 nodes_hashed = 0;
    UInt64 leaves_materialized = 0;

    bool operator==(const AuthorityInventoryMutationStatistics &) const = default;
};

struct AuthorityInventoryNode;

class AuthorityInventoryError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        Truncated,
        UnsupportedVersion,
        InvalidValue,
        LimitExceeded,
        NonCanonical,
        TrailingData,
    };

    AuthorityInventoryError(Code code_, std::string_view message);

    const Code code;
};

/// Stable byte ordering by `(format version, record kind, RFC UUID bytes)`.
bool authorityInventoryKeyLess(const AuthorityInventoryKey & lhs, const AuthorityInventoryKey & rhs) noexcept;
bool authorityInventoryLeafLess(const AuthorityInventoryLeaf & lhs, const AuthorityInventoryLeaf & rhs) noexcept;

/// Canonical V1 codecs. Decoders reject non-minimal integer framing, unknown
/// tags and trailing data before returning a value.
String encodeAuthorityInventoryLeaf(const AuthorityInventoryLeaf & leaf, const AuthorityInventoryLimits & limits = {});
AuthorityInventoryLeaf decodeAuthorityInventoryLeaf(std::string_view bytes, const AuthorityInventoryLimits & limits = {});

/// Input must be strictly key-sorted. The canonical Patricia shape compresses
/// unary prefixes, so a nonempty N-leaf root hashes at most 2N-1 nodes.
Digest computeAuthorityInventoryRoot(
    std::span<const AuthorityInventoryLeaf> sorted_leaves,
    const AuthorityInventoryLimits & limits = {},
    UInt64 * hashed_node_count = nullptr);

AuthorityInventorySummary
buildAuthorityInventorySummary(std::span<const AuthorityInventoryLeaf> sorted_leaves, const AuthorityInventoryLimits & limits = {});

/// Immutable authority value. Construction validates the supplied
/// summary against the exact sorted leaf set. Later storage adapters may
/// publish this value as one member of their database-owned authority root.
class AuthorityInventory final
{
public:
    using Ptr = std::shared_ptr<const AuthorityInventory>;

    ~AuthorityInventory();

    static Ptr create(
        AuthorityInventorySummary summary, std::vector<AuthorityInventoryLeaf> sorted_leaves, const AuthorityInventoryLimits & limits = {});

    /// Applies a fixed-depth path-copy mutation to the pinned canonical radix
    /// tree. Untouched subtrees are shared and their hashes are reused.
    static Ptr applyMutation(
        const Ptr & base,
        std::span<const AuthorityInventoryLeafDelta> sorted_deltas,
        const AuthorityInventoryLimits & limits = {},
        AuthorityInventoryMutationStatistics * statistics = nullptr);

    const AuthorityInventorySummary & getSummary() const noexcept { return summary; }
    /// Exact number of canonical Patricia nodes retained by this immutable
    /// root. The canonical empty root contributes its one constant hash node.
    UInt64 getNodeCount() const noexcept;
    /// Conservative immutable ownership charge, including the persistent
    /// radix tree and the lazily materialized full leaf vector. Shared
    /// subtrees are charged to each root that can retain them, matching the
    /// authority retirement accounting contract.
    UInt64 getAccountedBytes() const noexcept { return accounted_bytes; }
    /// Maximum incremental logical charge of adding one leaf through
    /// `applyMutation`. The bound includes every copied radix branch, the new
    /// leaf and the additional materialized-leaf slot.
    static UInt64 getSingleLeafInsertionAccountedBytesUpperBound();
    /// Snapshot/checkpoint-only materialization. Point mutation and lookup do
    /// not call this method. The canonical vector is built at most once.
    std::span<const AuthorityInventoryLeaf> getLeaves() const;

    /// Bounded canonical-order lookup used by cooperative scanners. Unlike
    /// getLeaves(), this never materializes the complete persistent radix
    /// tree: the cost is bounded by the fixed V1 key depth and branch fanout.
    const AuthorityInventoryLeaf * getLeafByCanonicalIndex(UInt64 index) const noexcept;

    const AuthorityInventoryLeaf * find(const AuthorityInventoryKey & key) const noexcept;

    /// Test/diagnostic predicate for proving path-copy reuse.
    bool sharesRootNodeWith(const AuthorityInventory & other) const noexcept;

private:
    using NodePtr = std::shared_ptr<const AuthorityInventoryNode>;

    AuthorityInventory(AuthorityInventorySummary summary_, NodePtr root_, std::vector<AuthorityInventoryLeaf> materialized_leaves_);

    AuthorityInventorySummary summary;
    NodePtr root;
    UInt64 accounted_bytes = 0;
    mutable std::once_flag materialize_once;
    mutable std::vector<AuthorityInventoryLeaf> materialized_leaves;
};

}
