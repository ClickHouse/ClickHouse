#include <Coordination/KeeperLSMTNodesStorage.h>

#include <Coordination/Storage/NodeStream.h>
#include <Coordination/KeeperSnapshotManager.h>

#include <shared_mutex>

using namespace Coordination::Storage;

namespace DB
{

KeeperLSMTNodesStorage::KeeperLSMTNodesStorage(KeeperContextPtr keeper_context_, SharedMutex * storage_mutex_)
    : KeeperNodesStorage(keeper_context_, storage_mutex_), state(keeper_context_, storage_mutex_)
{
    state.startup();
}

void KeeperLSMTNodesStorage::shutdown()
{
    state.shutdown();
}

KeeperLSMTNodesStorage::NodeHolder KeeperLSMTNodesStorage::getCommittedNode(std::string_view path)
{
    /// TODO: Change all paths in KeeperNodesStorage interface from string_view to
    /// NodePath/NodePathWithHash, ideally precalculating hashes during request parsing or batching.
    NodePathWithHash node_path = NodePath(path).withCalculatedHash();
    NodeHolder node;
    node.ref = state.getCommittedNode(node_path);
    if (node.ref)
        node.ref.readWithKnownPath(node.node, node_path);
    return node;
}

KeeperLSMTNodesStorage::UncommittedNodeRef KeeperLSMTNodesStorage::getUncommittedNode(std::string_view path)
{
    NodePathWithHash node_path = NodePath(path).withCalculatedHash();
    UncommittedNodeRef node;
    node.ref = state.getUncommittedNode(node_path);
    if (node.ref)
    {
        node.ref.readWithKnownPath(node.node, node_path);
        node.node.getOrCalculateDigest(); // precalculate digest for prepare*'s convenience
    }
    else
    {
        node.node.path = node_path.path;
        node.node.path_hash = node_path.hash;
    }
    return node;
}

bool KeeperLSMTNodesStorage::getCommittedNodeSimple(std::string_view path, KeeperNodeStats * out_stats, std::string * out_data)
{
    NodeRef ref = state.getCommittedNode(NodePath(path).withCalculatedHash());
    if (!ref)
        return false;
    if (!out_stats && !out_data)
        return true;

    FullNode node;
    ref.readWithoutPath(node);
    if (out_stats)
        *out_stats = node.stats;
    if (out_data)
        *out_data = std::string{node.getData()};
    return true;
}

bool KeeperLSMTNodesStorage::getUncommittedNodeSimple(std::string_view path, KeeperNodeStats * out_stats, std::string * out_data)
{
    NodeRef ref = state.getUncommittedNode(NodePath(path).withCalculatedHash());
    if (!ref)
        return false;
    if (!out_stats && !out_data)
        return true;

    FullNode node;
    ref.readWithoutPath(node);
    if (out_stats)
        *out_stats = node.stats;
    if (out_data)
        *out_data = std::string{node.getData()};
    return true;
}

std::vector<std::string> KeeperLSMTNodesStorage::listCommittedChildrenNames(std::string_view path) const
{
    NodePathWithHash node_path = NodePath(path).withCalculatedHash();
    Coordination::Storage::ChildrenSet2 set;
    Arena arena;
    state.listCommittedChildrenNames(node_path, set, arena);

    std::vector<std::string> res;
    for (const auto & entry : set.set)
    {
        if (entry.action != NodeAction::Remove)
            res.emplace_back(entry.str());
    }
    return res;
}

bool KeeperLSMTNodesStorage::addCommittedNodeIfNotExists(std::string_view path, const KeeperNodeStats & stats, std::string_view data, bool update_parent_num_children, uint64_t * out_digest)
{
    NodePathWithHash node_path = NodePath(path).withCalculatedHash();
    if (state.getCommittedNode(node_path))
        return false;

    FullNode node;
    node.action = NodeAction::Create;
    node.stats = stats;
    node.setData(data);
    node.path = node_path.path;
    node.path_hash = node_path.hash;

    if (out_digest)
        *out_digest += node.getOrCalculateDigest();

    state.appendCommittedNode(node);

    if (update_parent_num_children && node_path.path.depth > 0)
    {
        NodePathWithHash parent_path = node_path.path.parentPath().withCalculatedHash();
        NodeRef parent_ref = state.getCommittedNode(parent_path);
        chassert(parent_ref);
        FullNode parent_node;
        parent_ref.readWithKnownPath(parent_node, parent_path);
        if (out_digest)
            *out_digest -= parent_node.getOrCalculateDigest();
        parent_node.action = NodeAction::Update;
        parent_node.stats.increaseNumChildren();
        parent_node.digest = 0;
        if (out_digest)
            *out_digest += parent_node.getOrCalculateDigest();
        state.appendCommittedNode(parent_node);
    }
    return true;
}

void KeeperLSMTNodesStorage::updateCommittedNode(std::string_view path, std::optional<const KeeperNodeStats *> new_stats, std::optional<std::string_view> new_data, uint64_t * out_digest)
{
    NodePathWithHash node_path = NodePath(path).withCalculatedHash();
    NodeRef ref = state.getCommittedNode(node_path);
    chassert(ref);
    FullNode node;
    ref.readWithKnownPath(node, node_path);
    if (out_digest)
        *out_digest -= node.getOrCalculateDigest();
    node.action = NodeAction::Update;
    if (new_stats)
        node.stats = **new_stats;
    if (new_data)
        node.setData(*new_data);
    node.digest = 0;
    if (out_digest)
        *out_digest += node.getOrCalculateDigest();
    state.appendCommittedNode(node);
}

void KeeperLSMTNodesStorage::removeCommittedNode(std::string_view path)
{
    NodePathWithHash node_path = NodePath(path).withCalculatedHash();
    chassert(state.getCommittedNode(node_path));

    FullNode node;
    node.path = node_path.path;
    node.path_hash = node_path.hash;
    node.action = NodeAction::Remove;
    state.appendCommittedNode(node);
}

void KeeperLSMTNodesStorage::loadNodesFromSnapshot(KeeperSnapshotReader & reader, KeeperStorage * storage, uint64_t * out_digest)
{
    /// TODO: Autodetect long sorted runs, turn them directly into SortedRun-s.
    auto streams = reader.createStreams(1);
    chassert(streams.size() == 1);
    size_t path_size = 0;
    std::string path_buf;
    std::string data_buf;
    while (streams[0]->readNodePathSize(path_size))
    {
        state.throttleWrite();

        path_buf.resize(path_size);
        size_t data_size = 0;
        streams[0]->readNodePathAndDataSize(path_buf.data(), path_size, data_size);
        FullNode node;
        node.action = NodeAction::Create;
        node.path = NodePath(path_buf);
        data_buf.resize(data_size);
        streams[0]->readNodeDataAndStats(node.path.str(), data_buf.data(), data_size, node.stats);
        node.setData(data_buf);

        if (storage)
            storage->nodeLoadedFromSnapshot(node.path.str(), node.stats);

        if (out_digest)
            *out_digest += node.getOrCalculateDigest();

        state.appendCommittedNode(node);
    }

    reader.finishStreams(std::move(streams));
}

struct KeeperLSMTNodesStorage::NodeStreamForSnapshot final : public KeeperNodeStreamForSnapshot
{
    SnapshotWriterNodeStream stream;

    NodeStreamForSnapshot(const StorageState & state_) : stream(state_)
    {
        node_count = stream.getNodeCount();
    }

    bool next(std::string_view & out_path, std::string_view & out_data, KeeperNodeStats & out_stats) override
    {
        stream.next();
        if (stream.at_end)
            return false;
        out_path = stream.node.path.str();
        out_data = stream.node.getData();
        out_stats = stream.node.stats;
        return true;
    }
};

std::unique_ptr<KeeperNodeStreamForSnapshot> KeeperLSMTNodesStorage::beginWritingSnapshot()
{
    return std::make_unique<NodeStreamForSnapshot>(state);
}

void KeeperLSMTNodesStorage::finishWritingSnapshot(std::unique_ptr<KeeperNodeStreamForSnapshot> stream)
{
    stream->node_count = 0;
}

void KeeperLSMTNodesStorage::getNodeStorageStats(KeeperStorageStats & out)
{
    state.getNodeCountAndDataSize(out.nodes_count, out.approximate_data_size);
}

void KeeperLSMTNodesStorage::fillAsynchronousMetrics(AsynchronousMetricValues & new_values)
{
    std::shared_lock lock(*storage_mutex);
    state.fillAsynchronousMetrics(new_values);
}

void KeeperLSMTNodesStorage::commitDelta(Delta & delta, uint64_t * digest)
{
    auto & op = std::get<LSMTDelta>(delta.operation);
    op.new_node.path.ptr = delta.path.data();
    if (digest)
        *digest += op.new_node.getOrCalculateDigest() - op.old_digest;
    state.appendCommittedNode(op.new_node);
}

void KeeperLSMTNodesStorage::cleanupUncommittedState(int64_t commit_zxid)
{
    state.cleanupUncommittedState(commit_zxid);
}

void KeeperLSMTNodesStorage::rollbackUncommittedDelta(const Delta & delta)
{
    auto & op = std::get<LSMTDelta>(delta.operation);

    /// Instead of un-appending the entry, append another entry that has the opposite effect.
    /// (Alternatively we could add support for truncating a memtable, but that seems more complex and error-prone.)

    FullNode node;
    std::string path_buf;
    if (op.new_node.action == NodeAction::Create)
    {
        node.path = op.new_node.path;
        node.path.ptr = delta.path.data();
    }
    else
    {
        op.old_node_ref.read(node, path_buf);
    }

    switch (op.new_node.action)
    {
        case NodeAction::Create: node.action = NodeAction::Remove; break;
        case NodeAction::Update: node.action = NodeAction::Update; break;
        case NodeAction::Remove: node.action = NodeAction::Create; break;
    }

    chassert(node.getOrCalculateDigest() == op.old_digest);

    state.appendUncommittedNode(node, delta.zxid);
}

bool KeeperLSMTNodesStorage::visitUncommittedRecursive(std::string_view root_path, size_t limit, std::function<bool(std::string_view /*path*/, UncommittedNodeRef &&)> check_node)
{
    DB::Arena arena; // for paths
    std::deque<NodePathWithHash> queue;

    {
        UncommittedNodeRef root_node = getUncommittedNode(root_path);
        if (!root_node.get())
            return true;

        NodePathWithHash root_node_path = root_node.node.getPathWithHash();
        if (!check_node(root_path, std::move(root_node)))
            return false;
        queue.push_back(root_node_path);
    }

    size_t nodes_visited = 1;
    bool ok = true;

    while (!queue.empty() && ok)
    {
        NodePathWithHash node_path = queue.front();
        queue.pop_front();

        Coordination::Storage::ChildrenSet2 set;
        state.listUncommittedChildrenNames(node_path, set, arena);

        for (const auto & entry : set.set)
        {
            if (entry.action == NodeAction::Remove)
                continue;

            NodePathWithHash child_path = node_path.path.childPath(entry.str(), arena).withCalculatedHash();
            UncommittedNodeRef child;
            child.ref = state.getUncommittedNode(child_path);
            child.ref.readWithKnownPath(child.node, child_path);

            ++nodes_visited;
            if (nodes_visited > limit || !check_node(child.node.path.str(), std::move(child)))
            {
                ok = false;
                break;
            }
            queue.push_back(child_path);
        }
    }

    return ok;
}

void KeeperLSMTNodesStorage::prepareImpl(std::string_view path, UncommittedNodeRef && node, KeeperStagingTransaction & staging, ACLId old_acl_id, int64_t ephemeral_owner, bool has_ttl, bool is_container)
{
    state.throttleWrite();

    chassert((!node.ref) == (node.node.action == NodeAction::Create));
    chassert((!node.ref) == (node.node.digest == 0));
    /// Digest must've been calculated by getUncommittedNode, then left unchanged by our caller.
    uint64_t old_digest = node.node.digest;
    node.node.path.ptr = path.data();
    node.node.digest = 0;
    staging.digest.value += node.node.getOrCalculateDigest() - old_digest;
    NodeRef new_ref = state.appendUncommittedNode(node.node, staging.zxid);
    staging.deltas.emplace_back(std::string{path}, staging.zxid, LSMTDelta {new_ref, node.node, node.ref, old_digest, old_acl_id, ephemeral_owner, has_ttl, is_container});
}

void KeeperLSMTNodesStorage::prepareUpdateNodeStat(
    std::string_view path, UncommittedNodeRef && node, const KeeperNodeStats & new_stats,
    KeeperStagingTransaction & staging)
{
    ACLId old_acl_id = node.node.stats.acl_id;
    node.node.action = NodeAction::Update;
    chassert(node.node.stats.getEphemeralOwner() == new_stats.getEphemeralOwner());
    node.node.stats = new_stats;
    prepareImpl(path, std::move(node), staging, old_acl_id, new_stats.getEphemeralOwner(), new_stats.isTTL(), new_stats.isContainer());
}

void KeeperLSMTNodesStorage::prepareUpdateNodeDataAndStat(
    std::string_view path, UncommittedNodeRef && node, const KeeperNodeStats & new_stats,
    std::string_view new_data, KeeperStagingTransaction & staging)
{
    ACLId old_acl_id = node.node.stats.acl_id;
    node.node.action = NodeAction::Update;
    chassert(node.node.stats.getEphemeralOwner() == new_stats.getEphemeralOwner());
    node.node.stats = new_stats;
    node.node.setData(new_data);
    prepareImpl(path, std::move(node), staging, old_acl_id, new_stats.getEphemeralOwner(), new_stats.isTTL(), new_stats.isContainer());
}

void KeeperLSMTNodesStorage::prepareCreateNodeWithoutUpdatingParent(
    std::string_view path, UncommittedNodeRef && node, const KeeperNodeStats & stats,
    std::string_view data, KeeperStagingTransaction & staging)
{
    ACLId old_acl_id = node.node.stats.acl_id;
    node.node.action = NodeAction::Create;
    node.node.stats = stats;
    node.node.setData(data);
    prepareImpl(path, std::move(node), staging, old_acl_id, stats.getEphemeralOwner(), stats.isTTL(), stats.isContainer());
}

void KeeperLSMTNodesStorage::prepareRemoveNodeWithoutUpdatingParent(
    std::string_view path, UncommittedNodeRef && node, KeeperStagingTransaction & staging)
{
    ACLId old_acl_id = node.node.stats.acl_id;
    int64_t ephemeral_owner = node.node.stats.getEphemeralOwner();
    bool has_ttl = node.node.stats.isTTL();
    bool is_container = node.node.stats.isContainer();
    node.node.action = NodeAction::Remove;
    node.node.stats = {};
    prepareImpl(path, std::move(node), staging, old_acl_id, ephemeral_owner, has_ttl, is_container);
}

}
