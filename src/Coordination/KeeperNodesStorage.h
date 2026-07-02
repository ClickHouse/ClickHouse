#pragma once

#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperDelta.h>
#include <Common/SharedMutex.h>

#include <base/defines.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace DB
{

class KeeperStorage;
struct KeeperSnapshotReader;

/// Iterator over nodes in KeeperStorage, frozen at the moment in time when
/// KeeperNodeStreamForSnapshot was created. Creation locks storage_mutex but is relatively fast,
/// then the long-running iteration can proceed after the mutex is unlocked.
struct KeeperNodeStreamForSnapshot
{
    /// Total number of nodes that `next` will report. Storage must provide it in advance.
    size_t node_count = 0;

    virtual bool next(std::string_view & out_path, std::string_view & out_data, KeeperNodeStats & out_stats) = 0;

    virtual ~KeeperNodeStreamForSnapshot() { chassert(node_count == 0); }
};

struct KeeperStorageStats
{
    uint64_t nodes_count = 0;
    uint64_t approximate_data_size = 0;

    uint64_t total_watches_count = 0;
    uint64_t watched_paths_count = 0;
    uint64_t sessions_with_watches_count = 0;
    uint64_t session_with_ephemeral_nodes_count = 0;
    uint64_t total_emphemeral_nodes_count = 0;
    int64_t last_committed_zxid = 0;
};

[[noreturn]] void onStorageInconsistency(std::string_view message);

/// Interface for a thing storing the bulk of keeper server's data: nodes.
/// The dispatch between two implementations uses virtual for some things and template for others.
/// (TODO: Try getting rid of templates by making NodeHolder and UncommittedNodeRef virtual and
///        passing them by unique_ptr. See if that's noticeably slower.)
///
/// There are two implementations:
///  * Legacy in-memory KeeperMemNodesStorage. Kept for now because it's relatively simple and known to work well.
///  * (TODO: The following doesn't exist yet, and the performance claims are not verified.)
///    LSM tree KeeperLSMTStorage. It may write data to files/S3 or may be used in memory-only mode.
///    It's slightly faster than the legacy storage (when all data fits in memory).
///    Its memory usage is lower than the legacy storage, even in memory-only mode.
/// (Supporting two implementations makes the code pretty awkward and big. Hopefully we'll
///  eventually delete one of them, along with this interface and all the inheritance+template
///  nonsense in KeeperStorage[Impl].)
struct KeeperNodesStorage
{
    using Delta = KeeperDelta;

    KeeperContextPtr keeper_context;
    SharedMutex * storage_mutex = nullptr;

    KeeperNodesStorage(KeeperContextPtr keeper_context_, SharedMutex * storage_mutex_) : keeper_context(std::move(keeper_context_)), storage_mutex(storage_mutex_) {}

    virtual ~KeeperNodesStorage() = default;

    virtual void shutdown() {}

    /// Assigns just the fields relevant to node storage. Other fields are set by KeeperStorage.
    /// Caller must hold storage_mutex (shared_lock is sufficient).
    virtual void getNodeStorageStats(KeeperStorageStats & out) = 0;

    /// (A little slower than getCommittedNode/getUncommittedNode, so most request processing should
    ///  be a template and use getCommittedNode instead.)
    /// (No default arguments: clang-tidy's google-default-arguments prohibits them on virtual methods.)
    virtual bool getCommittedNodeSimple(std::string_view path, KeeperNodeStats * out_stats, std::string * out_data) = 0;
    virtual bool getUncommittedNodeSimple(std::string_view path, KeeperNodeStats * out_stats, std::string * out_data) = 0;

    /// List children names without data.
    /// (Implementation should not rely on getNumChildren() stat because it may be inaccurate for system nodes.)
    virtual std::vector<std::string> listCommittedChildrenNames(std::string_view path) const = 0;

    /// Directly create or mutate a committed node. Used to set up system nodes and by tests.
    virtual bool addCommittedNodeIfNotExists(std::string_view path, const KeeperNodeStats & stats, std::string_view data, bool update_parent_num_children, uint64_t * out_digest) = 0;
    virtual void updateCommittedNode(std::string_view path, std::optional<const KeeperNodeStats *> new_stats, std::optional<std::string_view> new_data, uint64_t * out_digest) = 0;
    virtual void removeCommittedNode(std::string_view path) = 0;

    /// Caller must hold storage mutex.
    /// At most one stream can exist at any given time.
    /// Stream must be destroyed using finishWritingSnapshot (with storage mutex held), otherwise
    /// destructor fails assert.
    virtual std::unique_ptr<KeeperNodeStreamForSnapshot> beginWritingSnapshot() = 0;
    /// Should set stream->node_count = 0 to tell destructor to be chill.
    virtual void finishWritingSnapshot(std::unique_ptr<KeeperNodeStreamForSnapshot> stream) = 0;

    /// Does only createStreams-finishStreams on `reader`, the caller does everything before and after that.
    virtual void loadNodesFromSnapshot(KeeperSnapshotReader & reader, KeeperStorage * storage, uint64_t * out_digest) = 0;

    virtual void commitDelta(Delta & delta, uint64_t * digest) = 0;
    virtual void cleanupUncommittedState(int64_t commit_zxid) = 0;
    virtual void rollbackUncommittedDelta(const Delta & delta) = 0;
    virtual void cleanupAfterRollback(std::vector<uint64_t> /*rollbacked_zxids*/) {}
    /// Call to calculate digest after preparing all uncommitted changes for a given zxid.
    virtual void updateNodesDigest(uint64_t & /*current_digest*/, uint64_t /*zxid*/) const {}

    virtual void recalculateStats() {}

    /// In addition to the above, the KeeperStorageImpl template argument must have the following
    /// types and methods.
    /// (We could use C++ concepts to enforce it, but the concept definition would be harder to read
    ///  than this comment.)
    ///
    ///   struct Node {
    ///       KeeperNodeStats stats;
    ///
    ///       std::string_view getData() const noexcept;
    ///       uint64_t getDigest(std::string_view path) const;
    ///   };
    ///
    ///   struct NodeHolder {
    ///       /// Pointer valid only while this NodeHolder is alive.
    ///       const Node * get() const;
    ///   };
    ///
    ///   struct UncommittedNodeRef {
    ///       /// nullptr if node doesn't exist.
    ///       /// Pointer valid only while this UncommittedNodeRef is alive.
    ///       const Node * get() const;
    ///   };
    ///
    ///   NodeHolder getCommittedNode(std::string_view path);
    ///   UncommittedNodeRef getUncommittedNode(std::string_view path);
    ///
    ///   /// Traverses subtree in pre-order (parent before children). Stops early and returns false
    ///   /// if callback returns false or if more than `limit` nodes are found.
    ///   /// Returns true if the whole subtree was visited (reported to check_node).
    ///   /// If the root node doesn't exist, returns true.
    ///   bool visitUncommittedRecursive(
    ///       std::string_view root_path, size_t limit, std::function<bool(std::string_view /*path*/,
    ///       UncommittedNodeRef &&)> check_node);
    ///
    ///   /// Functions that mutate UncommittedState, add corresponding deltas to staging.deltas,
    ///   /// and update staging.digest.
    ///   void prepareUpdateNodeStat(
    ///       std::string_view path, UncommittedNodeRef && node, const KeeperNodeStats & new_stats,
    ///       KeeperStagingTransaction & staging);
    ///   void prepareUpdateNodeDataAndStat(
    ///       std::string_view path, UncommittedNodeRef && node, const KeeperNodeStats & new_stats,
    ///       std::string_view new_data, KeeperStagingTransaction & staging);
    ///   void prepareCreateNodeWithoutUpdatingParent(
    ///       std::string_view path, UncommittedNodeRef && node, const Coordination::Stat & stat,
    ///       ACLId acl_id, std::string_view data, std::optional<int64_t> ttl,
    ///       KeeperStagingTransaction & staging);
    ///   void prepareRemoveNodeWithoutUpdatingParent(
    ///       std::string_view path, UncommittedNodeRef && node, KeeperStagingTransaction & staging);
};

}
