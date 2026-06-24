#pragma once

#include <Coordination/KeeperNodesStorage.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/Storage/StorageState.h>
#include <Coordination/Storage/Node.h>

namespace DB
{

struct KeeperLSMTNodesStorage final : public KeeperNodesStorage
{
    using NodeRef = Coordination::Storage::NodeRef;
    using FullNode = Coordination::Storage::FullNode;
    using StorageState = Coordination::Storage::StorageState;
    using NodeAction = Coordination::Storage::NodeAction;

    /// ========== KeeperNodesStorage virtual methods ==========

    bool getCommittedNodeSimple(std::string_view path, KeeperNodeStats * out_stats = nullptr, std::string * out_data = nullptr) override;
    bool getUncommittedNodeSimple(std::string_view path, KeeperNodeStats * out_stats = nullptr, std::string * out_data = nullptr) override;

    std::vector<std::string> listCommittedChildrenNames(std::string_view path) const override;

    bool addCommittedNodeIfNotExists(std::string_view path, const KeeperNodeStats & stats, std::string_view data, bool update_parent_num_children, uint64_t * out_digest) override;
    void updateCommittedNode(std::string_view path, std::optional<const KeeperNodeStats *> new_stats, std::optional<std::string_view> new_data, uint64_t * out_digest) override;

    void loadNodesFromSnapshot(KeeperSnapshotReader & reader, KeeperStorage * storage, uint64_t * out_digest) override;

    std::unique_ptr<KeeperNodeStreamForSnapshot> beginWritingSnapshot() override;
    void finishWritingSnapshot(std::unique_ptr<KeeperNodeStreamForSnapshot> stream) override;

    void getNodeStorageStats(KeeperStorageStats & out) override;

    void commitDelta(Delta & delta, uint64_t * digest) override;
    void cleanupUncommittedState(int64_t commit_zxid) override;
    void rollbackUncommittedDelta(const Delta & delta) override;

    void shutdown() override;

    /// ========== Duck-typed interface used by KeeperStorageImpl ==========

    using Node = FullNode;
    struct NodeHolder;
    struct UncommittedNodeRef;

    KeeperLSMTNodesStorage(KeeperContextPtr keeper_context_, SharedMutex * storage_mutex_);

    NodeHolder getCommittedNode(std::string_view path);
    UncommittedNodeRef getUncommittedNode(std::string_view path);

    std::vector<std::string> listCommittedChildrenNames(std::string_view path, const Node * node) const;

    void visitCommittedChildren(
        std::string_view path, const Node * node,
        std::function<bool(std::string_view /*name*/, const Node *)> check_node) const;

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
        ACLId acl_id, std::string_view data, KeeperStagingTransaction & staging);
    void prepareRemoveNodeWithoutUpdatingParent(
        std::string_view path, UncommittedNodeRef && node, KeeperStagingTransaction & staging);

    /// ========== Implementations ==========

    struct NodeHolder
    {
        NodeRef ref;
        /// NodePath is invalid.
        FullNode node;

        const Node * get() const { return node.action == NodeAction::Remove ? nullptr : &node; }
    };

    struct UncommittedNodeRef
    {
        NodeRef ref;
        /// Has digest. Has correct NodePath length and depth but likely invalidated NodePath::ptr.
        FullNode node;

        const Node * get() const { return node.action == NodeAction::Remove ? nullptr : &node; }
    };

    struct NodeStreamForSnapshot;

    StorageState state;

    /// Call with node.node mutated, but node.ref and node.node.digest still having the old values.
    void prepareImpl(std::string_view path, UncommittedNodeRef && node, KeeperStagingTransaction & staging, ACLId old_acl_id, int64_t ephemeral_owner);
};

}
