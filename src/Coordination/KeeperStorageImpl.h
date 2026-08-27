#pragma once

#include <Coordination/CompactChildrenSet.h>

namespace DB
{

template <typename NodesStorage>
class KeeperStorageImpl : public KeeperStorage
{
public:
    using Node = typename NodesStorage::Node;
    using UncommittedNodeRef = typename NodesStorage::UncommittedNodeRef;

    NodesStorage nodes;

    KeeperStorageImpl(int64_t tick_time_ms, const String & superdigest_, const KeeperContextPtr & keeper_context_);
    ~KeeperStorageImpl() override;

    KeeperResponsesForSessions processLocalRequests(
        const KeeperRequestsForSessions & requests,
        bool check_acl) override;
    std::optional<KeeperDigest> preprocessBatch(const KeeperRequestBatch & batch, bool check_acl) override;
    KeeperResponsesForSessions processRequest(
        const Coordination::ZooKeeperRequestPtr & request,
        int64_t session_id,
        std::optional<int64_t> new_last_zxid) override;

    /// Preprocess one (transaction-creating) request of a batch. first_in_batch means this is
    /// the first transaction-creating request of its batch, so a new UncommittedBatchInfo is
    /// created; otherwise the last one is extended.
    KeeperDigest preprocessOneRequest(
        const Coordination::ZooKeeperRequestPtr & request,
        int64_t session_id,
        int64_t time,
        int64_t new_last_zxid,
        bool check_acl,
        int64_t log_idx,
        bool first_in_batch);

    /// Helper that uses getUncommittedNode, prepareRemoveNodeWithoutUpdatingParent, and
    /// prepareUpdateNodeStat to remove the given set of ephemeral nodes and update their parents'
    /// stats accordingly. Requires that no nodes in the set are ancestors of other nodes (true for
    /// ephemeral nodes because they can't have children).
    void prepareRemoveEphemeralNodes(const std::unordered_set<std::string> & paths, int64_t session_id);
};

}
