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

    /// Returns false if the request was rejected and rolled back.
    bool preprocessOneRequest(
        const Coordination::ZooKeeperRequestPtr & request,
        int64_t session_id,
        int64_t time,
        bool check_acl);

    /// Helper that uses getUncommittedNode, prepareRemoveNodeWithoutUpdatingParent, and
    /// prepareUpdateNodeStat to remove the given set of ephemeral nodes and update their parents'
    /// stats accordingly. Requires that no nodes in the set are ancestors of other nodes (true for
    /// ephemeral nodes because they can't have children).
    void prepareRemoveEphemeralNodes(const std::unordered_set<std::string> & paths, int64_t session_id);
};

}
