#pragma once

#include <Coordination/CompactChildrenSet.h>

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
    UInt64 getDigest(std::string_view path) const;

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

class KeeperStorageImpl : public KeeperStorage
{
public:
    using Container = SnapshotableHashTable<KeeperMemNode>;
    using Node = KeeperMemNode;

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

    struct NodeStreamForSnapshot : public KeeperNodeStreamForSnapshot
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

    KeeperStorageImpl(int64_t tick_time_ms, const String & superdigest_, const KeeperContextPtr & keeper_context_)
        : KeeperStorage(tick_time_ms, superdigest_, keeper_context_) {}

    KeeperStorageStats getStorageStats() const override;

    bool getCommittedNodeSlow(std::string_view path, KeeperNodeStats * out_stats = nullptr, std::string * out_data = nullptr) override;

    bool addSystemNodeIfNotExists(std::string_view path, const KeeperNodeStats & stats, std::string_view data, bool update_parent_num_children, uint64_t * out_digest) override;

    std::unique_ptr<KeeperNodeStreamForSnapshot> beginWritingSnapshot() override;
    void finishWritingSnapshot(std::unique_ptr<KeeperNodeStreamForSnapshot> stream) override;

    KeeperResponsesForSessions processLocalRequests(
        const KeeperRequestsForSessions & requests,
        bool check_acl = true) override;
    KeeperDigest preprocessRequest(
        const Coordination::ZooKeeperRequestPtr & request,
        int64_t session_id,
        int64_t time,
        int64_t new_last_zxid,
        bool check_acl = true,
        std::optional<KeeperDigest> digest = std::nullopt,
        int64_t log_idx = 0) override;
    KeeperResponsesForSessions processRequest(
        const Coordination::ZooKeeperRequestPtr & request,
        int64_t session_id,
        std::optional<int64_t> new_last_zxid) override;

    void recalculateStats() override;

    UncommittedNodeRef getUncommittedNode(std::string_view path);

    /// Traverses subtree in pre-order (parent before children). Stops early and returns false
    /// if callback returns false or if more than `limit` nodes are found.
    /// Returns true if the whole subtree was visited (reported to check_node).
    /// If the root node doesn't exist, returns true.
    bool visitUncommittedRecursive(std::string_view root_path, size_t limit, std::function<bool(std::string_view /*path*/, UncommittedNodeRef &&)> check_node);

    /// Functions that mutate UncommittedState, add corresponding deltas to staging_.deltas, and
    /// update staging_.digest.
    /// These are public because they're called from the free `preprocess` request handlers.
    /// ('&&' to prevent the caller from using the value afterwards in case the implementation wants
    ///  to mutate it or move it out, but to not copy the bytes because UncommittedNodeRef is pretty
    ///  big in LSM tree storage.)
    void prepareUpdateNodeStat(std::string_view path, UncommittedNodeRef && node, const KeeperNodeStats & new_stats, KeeperStagingTransaction & staging_);
    void prepareUpdateNodeData(std::string_view path, UncommittedNodeRef && node, const KeeperNodeStats & new_stats, std::string_view new_data, KeeperStagingTransaction & staging_);
    void prepareCreateNode(
        std::string_view parent_path, UncommittedNodeRef && parent,
        const KeeperNodeStats & new_parent_stats,
        std::string_view path, UncommittedNodeRef && node, const Coordination::Stat & stat,
        ACLId acl_id, std::string_view data, KeeperStagingTransaction & staging_);
    void prepareRemoveNode(
        std::string_view parent_path, UncommittedNodeRef && parent,
        const KeeperNodeStats & new_parent_stats,
        std::string_view path, UncommittedNodeRef && node, KeeperStagingTransaction & staging_);
    /// (`nodes_to_remove` must be a node + the set of all its descendants, not arbitrary set of
    ///  nodes. Because we don't update children stats on parents of removed nodes, expecting those
    ///  parents to also be in the set to be removed, except for the outermost `parent`.)
    void prepareRemoveRecursive(
        std::string_view parent_path, UncommittedNodeRef && parent,
        const KeeperNodeStats & new_parent_stats,
        std::deque<std::pair<std::string, UncommittedNodeRef>> nodes_to_remove, KeeperStagingTransaction & staging_);
    void prepareRemoveEphemeralNodes(const std::unordered_set<std::string> & paths, int64_t session_id, KeeperStagingTransaction & staging_);

    /// Helpers used by other `prepare*` implementations.
    void prepareWriteCommon(std::string_view path, UncommittedNodeRef & node, KeeperStagingTransaction & staging_);
    void prepareRemoveNodeWithoutUpdatingParent(std::string_view path, UncommittedNodeRef && node, KeeperStagingTransaction & staging_);

protected:
    void commitDelta(const Delta & delta, uint64_t * digest) override;
    void cleanupUncommittedState(int64_t commit_zxid) override;
    void rollbackUncommittedDelta(const Delta & delta) override;
    /// Call after a corresponding group of rollbackUncommittedDelta calls.
    void cleanupAfterRollback(std::vector<uint64_t> rollbacked_zxids) override;
    uint64_t updateNodesDigest(uint64_t current_digest, uint64_t zxid) const override;

    void loadNodesFromSnapshot(KeeperSnapshotReader & reader, uint64_t * out_digest) override;

private:
    // Create node in the storage
    // Returns false if it failed to create the node, true otherwise
    // We don't care about the exact failure because we should've caught it during preprocessing
    bool
    createNode(const std::string & path, String data, const Coordination::Stat & stat, ACLId acl_id, uint64_t * digest);

    // Remove node in the storage
    // Returns false if it failed to remove the node, true otherwise
    // We don't care about the exact failure because we should've caught it during preprocessing
    bool removeNode(const std::string & path, int32_t version, uint64_t * digest);
};

}
