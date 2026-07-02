#pragma once

#include <Coordination/KeeperNodesStorage.h>
#include <Coordination/CompactChildrenSet.h>
#include <Coordination/SnapshotableHashTable.h>
#include <Common/StringHashForHeterogeneousLookup.h>

#include <map>
#include <optional>
#include <unordered_map>
#include <unordered_set>

namespace DB
{

/// KeeperMemNode should have as minimal size as possible to reduce memory footprint
/// of stored nodes
/// New fields should be added to the struct only if it's really necessary
struct KeeperMemNode
{
    KeeperNodeStats stats;
    std::unique_ptr<char[]> data{nullptr};
    mutable uint64_t cached_digest = 0;

    KeeperMemNode() = default;

    KeeperMemNode & operator=(const KeeperMemNode & other);
    KeeperMemNode(const KeeperMemNode & other);

    KeeperMemNode & operator=(KeeperMemNode && other) noexcept;
    KeeperMemNode(KeeperMemNode && other) noexcept;

    bool empty() const;

    /// Object memory size
    uint64_t sizeInBytes() const;

    void setData(std::string_view new_data);

    std::string_view getData() const noexcept { return {data.get(), stats.data_size}; }

    void addChild(std::string_view child_path);

    void removeChild(std::string_view child_path);

    template <typename Self>
    auto & getChildren(this Self & self)
    {
        return self.children;
    }

    // Invalidate the calculated digest so it's recalculated again on the next
    // getDigest call
    void invalidateDigestCache() const;

    // get the calculated digest of the node
    uint64_t getDigest(std::string_view path) const;

    // copy only necessary information for preprocessing and digest calculation
    // (e.g. we don't need to copy list of children)
    void shallowCopy(const KeeperMemNode & other);

    // copy data from node that is left only for snapshot write
    // e.g. we don't need list of children when writing snapshots so we can
    // move it to the new copy of node
    KeeperMemNode copyFromSnapshotNode();
private:
    CompactChildrenSet children{};
};

#if !defined(ADDRESS_SANITIZER) && !defined(MEMORY_SANITIZER)
    static_assert(sizeof(CompactChildrenSet) == 16);
    static_assert(sizeof(KeeperMemNode) == 104);
    static_assert(
        sizeof(ListNode<KeeperMemNode>) <= 128,
        "std::list node containing ListNode<Node> is > 144 bytes (sizeof(ListNode<Node>) + 16 bytes for pointers) which will increase "
        "memory consumption");
    static_assert(std::is_nothrow_move_assignable_v<CompactChildrenSet>);
    static_assert(std::is_nothrow_move_constructible_v<CompactChildrenSet>);
#endif

struct KeeperMemNodesStorage final : public KeeperNodesStorage
{
    /// ========== KeeperNodesStorage virtual methods ==========

    bool getCommittedNodeSimple(std::string_view path, KeeperNodeStats * out_stats, std::string * out_data) override;
    bool getUncommittedNodeSimple(std::string_view path, KeeperNodeStats * out_stats, std::string * out_data) override;

    std::vector<std::string> listCommittedChildrenNames(std::string_view path) const override;

    bool addCommittedNodeIfNotExists(std::string_view path, const KeeperNodeStats & stats, std::string_view data, bool update_parent_num_children, uint64_t * out_digest) override;
    void updateCommittedNode(std::string_view path, std::optional<const KeeperNodeStats *> new_stats, std::optional<std::string_view> new_data, uint64_t * out_digest) override;
    void removeCommittedNode(std::string_view path) override;

    void loadNodesFromSnapshot(KeeperSnapshotReader & reader, KeeperStorage * storage, uint64_t * out_digest) override;

    std::unique_ptr<KeeperNodeStreamForSnapshot> beginWritingSnapshot() override;
    void finishWritingSnapshot(std::unique_ptr<KeeperNodeStreamForSnapshot> stream) override;

    void getNodeStorageStats(KeeperStorageStats & out) override;

    void recalculateStats() override;

    void commitDelta(Delta & delta, uint64_t * digest) override;
    void cleanupUncommittedState(int64_t commit_zxid) override;
    void rollbackUncommittedDelta(const Delta & delta) override;
    void cleanupAfterRollback(std::vector<uint64_t> rollbacked_zxids) override;
    void updateNodesDigest(uint64_t & current_digest, uint64_t zxid) const override;

    /// ========== Duck-typed interface used by KeeperStorageImpl ==========

    using Node = KeeperMemNode;
    struct NodeHolder;
    struct UncommittedNodeRef;

    using KeeperNodesStorage::KeeperNodesStorage;

    NodeHolder getCommittedNode(std::string_view path);
    UncommittedNodeRef getUncommittedNode(std::string_view path);

    std::vector<std::string> listCommittedChildrenNames(std::string_view path, const Node * node) const;

    /// Traverses subtree in pre-order (parent before children). Stops early and returns false
    /// if callback returns false or if more than `limit` nodes are found.
    /// Returns true if the whole subtree was visited (reported to check_node).
    /// If the root node doesn't exist, returns true.
    bool visitUncommittedRecursive(std::string_view root_path, size_t limit, std::function<bool(std::string_view /*path*/, UncommittedNodeRef &&)> check_node);

    void prepareUpdateNodeStat(
        std::string_view path, UncommittedNodeRef && node, const KeeperNodeStats & new_stats,
        KeeperStagingTransaction & staging);
    void prepareUpdateNodeDataAndStat(
        std::string_view path, UncommittedNodeRef && node, const KeeperNodeStats & new_stats,
        std::string_view new_data, KeeperStagingTransaction & staging);
    void prepareCreateNodeWithoutUpdatingParent(
        std::string_view path, UncommittedNodeRef && node, const Coordination::Stat & stat,
        ACLId acl_id, std::string_view data, std::optional<int64_t> ttl,
        KeeperStagingTransaction & staging);
    void prepareRemoveNodeWithoutUpdatingParent(
        std::string_view path, UncommittedNodeRef && node, KeeperStagingTransaction & staging);

    /// ========== Implementations ==========

    struct NodeHolder
    {
        const Node * node = nullptr;

        const Node * get() const { return node; }
    };

    using Container = SnapshotableHashTable<KeeperMemNode>;

    struct UncommittedNode
    {
        std::shared_ptr<Node> node{nullptr};
        /// Tracks which zxids have been applied to this uncommitted node.
        /// Typically 1-3 entries; vector is faster than unordered_set at this size.
        /// May contain duplicates when a Multi operation applies multiple deltas
        /// with the same zxid to the same node — this is harmless because erasure
        /// uses std::erase (removes all matches) and only emptiness is checked.
        /// 0 means the node was read by the currently prepared transaction but not necessarily mutated.
        std::vector<uint64_t> applied_zxids{};
    };

    /// zxid_to_nodes stores iterators of nodes map
    /// so we should be careful when removing nodes from it
    /// Note: we rely on unordered_map iterators staying valid even after rehash. The standard
    ///       doesn't guarantee it, but apparently the libc++ we're using has this property.
    using UncommittedNodesMap = std::unordered_map<
        std::string,
        UncommittedNode,
        StringHashForHeterogeneousLookup,
        StringHashForHeterogeneousLookup::transparent_key_equal>;

    using UncommittedNodesIterator = UncommittedNodesMap::iterator;
    struct UncommittedNodesIteratorHash
    {
        auto operator()(UncommittedNodesIterator it) const
        {
            return std::hash<std::string_view>{}(it->first);
        }
    };

    struct UncommittedNodeRef
    {
        std::optional<UncommittedNodesIterator> it{};

        const Node * get() const { return it ? (*it)->second.node.get() : nullptr; }

        /// Mutable access to the node. Should only be used from `prepare*` functions, together
        /// with producing deltas matching the changes made to the Node.
        Node * getMut() const
        {
            chassert(it.has_value());
            chassert((*it)->second.node != nullptr);
            return (*it)->second.node.get();
        }
    };

    struct NodeStreamForSnapshot final : public KeeperNodeStreamForSnapshot
    {
        size_t next_node_idx = 0;
        Container::const_iterator it;

        bool next(std::string_view & out_path, std::string_view & out_data, KeeperNodeStats & out_stats) override;
    };

    UncommittedNodesMap uncommitted_nodes;

    /// Mapping of uncommitted transaction to all it's modified nodes for a faster cleanup.
    /// zxid_to_nodes[0] contains nodes that were duplicated from committed container to
    /// UncommittedState::nodes, but weren't necessarily updated.
    using ZxidToNodes = std::map<int64_t, std::unordered_set<UncommittedNodesIterator, UncommittedNodesIteratorHash>>;
    mutable ZxidToNodes uncommitted_zxid_to_nodes;

    /// Main hashtable with nodes. Contain all information about data.
    /// All other structures expect session_and_timeout can be restored from
    /// container.
    Container container;

    /// Must be called before mutating any node in other prepare* functions.
    void prepareWriteCommon(std::string_view path, UncommittedNodeRef & node, KeeperStagingTransaction & staging);

    // Create node in the storage
    // Returns false if it failed to create the node, true otherwise
    // We don't care about the exact failure because we should've caught it during preprocessing
    bool
    createNode(
        const std::string & path,
        String data,
        const Coordination::Stat & stat,
        ACLId acl_id,
        std::optional<int64_t> ttl,
        uint64_t * digest) TSA_NO_THREAD_SAFETY_ANALYSIS;

    // Remove node in the storage
    // Returns false if it failed to remove the node, true otherwise
    // We don't care about the exact failure because we should've caught it during preprocessing
    bool removeNode(const std::string & path, int32_t version, uint64_t * digest)TSA_NO_THREAD_SAFETY_ANALYSIS;
};

}
