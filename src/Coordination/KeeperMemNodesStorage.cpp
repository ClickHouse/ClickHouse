#include <Coordination/KeeperMemNodesStorage.h>

#include <Coordination/KeeperStorage.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/KeeperSnapshotManager.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/logger_useful.h>

#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

KeeperMemNode & KeeperMemNode::operator=(const KeeperMemNode & other)
{
    if (this == &other)
        return *this;

    stats = other.stats;
    cached_digest = other.cached_digest;

    if (stats.data_size != 0)
    {
        data = std::unique_ptr<char[]>(new char[stats.data_size]);
        memcpy(data.get(), other.data.get(), stats.data_size);
    }

    children = other.children;
    return *this;
}

KeeperMemNode::KeeperMemNode(const KeeperMemNode & other)
{
    *this = other;
}

KeeperMemNode & KeeperMemNode::operator=(KeeperMemNode && other) noexcept
{
    if (this == &other)
        return *this;

    stats = other.stats;
    cached_digest = other.cached_digest;

    data = std::move(other.data);

    other.stats.data_size = 0;

    static_assert(std::is_nothrow_move_assignable_v<CompactChildrenSet>);
    children = std::move(other.children);

    return *this;
}

KeeperMemNode::KeeperMemNode(KeeperMemNode && other) noexcept
{
    *this = std::move(other);
}

bool KeeperMemNode::empty() const
{
    return stats.data_size == 0 && stats.mzxid == 0;
}

uint64_t KeeperMemNode::sizeInBytes() const
{
    return sizeof(KeeperMemNode) + children.heapSizeInBytes() + stats.data_size;
}

void KeeperMemNode::setData(std::string_view new_data)
{
    stats.data_size = static_cast<uint32_t>(new_data.size());
    if (stats.data_size != 0)
    {
        data = std::unique_ptr<char[]>(new char[stats.data_size]);
        memcpy(data.get(), new_data.data(), stats.data_size);
    }
}

void KeeperMemNode::addChild(std::string_view child_path)
{
    children.insert(child_path);
}

void KeeperMemNode::removeChild(std::string_view child_path)
{
    children.erase(child_path);
}

void KeeperMemNode::invalidateDigestCache() const
{
    cached_digest = 0;
}

uint64_t KeeperMemNode::getDigest(const std::string_view path) const
{
    if (cached_digest == 0)
        cached_digest = stats.calculateDigest(path, getData());

    return cached_digest;
};

void KeeperMemNode::shallowCopy(const KeeperMemNode & other)
{
    stats = other.stats;
    if (stats.data_size != 0)
    {
        data = std::unique_ptr<char[]>(new char[stats.data_size]);
        memcpy(data.get(), other.data.get(), stats.data_size);
    }

    cached_digest = other.cached_digest;
}

KeeperMemNode KeeperMemNode::copyFromSnapshotNode()
{
    KeeperMemNode node_copy;
    node_copy.shallowCopy(*this);
    node_copy.children = std::move(children);
    children.clear();
    return node_copy;
}

KeeperMemNodesStorage::NodeHolder KeeperMemNodesStorage::getCommittedNode(std::string_view path)
{
    auto node_it = container.find(path);
    if (node_it == container.end())
        return {};
    return {&node_it->value};
}

KeeperMemNodesStorage::UncommittedNodeRef KeeperMemNodesStorage::getUncommittedNode(std::string_view path)
{
    if (auto node_it = uncommitted_nodes.find(path); node_it != uncommitted_nodes.end())
        return {node_it};

    std::shared_ptr<Node> node;
    {
        std::shared_lock lock(*storage_mutex);
        if (auto node_it = container.find(path); node_it != container.end())
        {
            const auto & committed_node = node_it->value;
            node = std::make_shared<Node>();
            node->shallowCopy(committed_node);
        }
    }

    if (!node)
        return {};

    auto [node_it, _] = uncommitted_nodes.emplace(std::string{path}, UncommittedNode{.node = node});
    uncommitted_zxid_to_nodes[0].insert(node_it);
    return {node_it};
}

bool KeeperMemNodesStorage::getCommittedNodeSimple(std::string_view path, KeeperNodeStats * out_stats, std::string * out_data)
{
    auto node_it = container.find(path);
    if (node_it == container.end())
        return false;

    if (out_stats)
        *out_stats = node_it->value.stats;
    if (out_data)
        *out_data = std::string{node_it->value.getData()};
    return true;
}

bool KeeperMemNodesStorage::getUncommittedNodeSimple(std::string_view path, KeeperNodeStats * out_stats, std::string * out_data)
{
    if (auto node_it = uncommitted_nodes.find(path); node_it != uncommitted_nodes.end())
    {
        if (!node_it->second.node)
            return false;
        if (out_stats)
            *out_stats = node_it->second.node->stats;
        if (out_data)
            *out_data = std::string{node_it->second.node->getData()};
        return true;
    }

    std::shared_lock lock(*storage_mutex);
    return getCommittedNodeSimple(path, out_stats, out_data);
}

std::vector<std::string> KeeperMemNodesStorage::listCommittedChildrenNames(std::string_view path) const
{
    auto node_it = container.find(path);
    if (node_it == container.end())
        return {};
    return listCommittedChildrenNames(path, &node_it->value);
}

std::vector<std::string> KeeperMemNodesStorage::listCommittedChildrenNames(std::string_view /*path*/, const Node * node) const
{
    const CompactChildrenSet & set = node->getChildren();
    std::vector<std::string> res;
    res.reserve(set.size());
    res.insert(res.end(), set.begin(), set.end());
    return res;
}

bool KeeperMemNodesStorage::addCommittedNodeIfNotExists(std::string_view path, const KeeperNodeStats & stats, std::string_view data, bool update_parent_num_children, uint64_t * out_digest)
{
    auto it = container.find(path);
    if (it != container.end())
        return false;

    Node node;
    node.stats = stats;
    node.setData(data);
    if (out_digest)
        *out_digest += node.getDigest(path);
    auto [map_key, _] = container.insert(std::string{path}, std::move(node));
    /// Take child path from key owned by map.
    auto child_name = Coordination::getBaseNodeName(map_key->getKey());

    if (path != "/")
    {
        auto parent_path = Coordination::parentNodePath(path);
        container.updateValue(
            parent_path,
            [&](Node & parent_node)
            {
                parent_node.addChild(child_name);

                if (update_parent_num_children)
                {
                    if (out_digest)
                        *out_digest -= parent_node.getDigest(parent_path);

                    parent_node.stats.increaseNumChildren();
                    parent_node.invalidateDigestCache();

                    if (out_digest)
                        *out_digest += parent_node.getDigest(parent_path);
                }
            }
        );
    }

    return true;
}

void KeeperMemNodesStorage::updateCommittedNode(std::string_view path, std::optional<const KeeperNodeStats *> new_stats, std::optional<std::string_view> new_data, uint64_t * out_digest)
{
    container.updateValue(
        path,
        [&](Node & node)
        {
            if (out_digest)
                *out_digest -= node.getDigest(path);

            if (new_stats)
                node.stats = **new_stats;
            if (new_data)
                node.setData(*new_data);
            node.invalidateDigestCache();

            if (out_digest)
                *out_digest += node.getDigest(path);
        });
}

void KeeperMemNodesStorage::removeCommittedNode(std::string_view path)
{
    container.erase(path);
    if (path != "/")
    {
        auto parent_path = Coordination::parentNodePath(path);
        container.updateValue(
            parent_path,
            [&](Node & parent_node)
            {
                parent_node.removeChild(Coordination::getBaseNodeName(path));
            }
        );
    }
}

void KeeperMemNodesStorage::loadNodesFromSnapshot(KeeperSnapshotReader & reader, KeeperStorage * storage, uint64_t * out_digest)
{
    container.reserve(reader.node_count);
    auto streams = reader.createStreams(1);
    chassert(streams.size() == 1);
    size_t path_size = 0;
    while (streams[0]->readNodePathSize(path_size))
    {
        auto path_data = container.allocateKey(path_size);
        size_t data_size = 0;
        streams[0]->readNodePathAndDataSize(path_data.get(), path_size, data_size);
        std::string_view path{path_data.get(), path_size};

        Node node;
        node.stats.data_size = static_cast<uint32_t>(data_size);
        if (data_size != 0)
            node.data = std::unique_ptr<char[]>(new char[node.stats.data_size]);
        streams[0]->readNodeDataAndStats(path, node.data.get(), data_size, node.stats);

        auto ephemeral_owner = node.stats.getEphemeralOwner();
        if (!node.stats.isEphemeral() && node.stats.getNumChildren() > 0)
            node.getChildren().reserve(node.stats.getNumChildren());

        if (storage)
        {
            if (ephemeral_owner != 0)
            {
                storage->committed_ephemerals[node.stats.getEphemeralOwner()].insert(std::string{path});
                ++storage->committed_ephemeral_nodes;
            }
            if (node.stats.isTTL())
                storage->ttl_paths.insert(std::string{path});
        }

        if (out_digest)
            *out_digest += node.getDigest(path);

        container.insertOrReplace(std::move(path_data), path_size, std::move(node));
    }

    reader.finishStreams(std::move(streams));

    LOG_TRACE(getLogger("KeeperMemNodeStorage"), "Building structure for children nodes");

    /// Populate children sets.
    for (const auto & itr : container)
    {
        if (itr.key != "/")
        {
            auto parent_path = Coordination::parentNodePath(itr.key);
            container.updateValue(
                parent_path, [path = itr.key](Node & value) { value.addChild(Coordination::getBaseNodeName(path)); });
        }
    }

    for (const auto & itr : container)
    {
        if (itr.key != "/")
        {
            if (itr.value.stats.getNumChildren() != static_cast<int32_t>(itr.value.getChildren().size()))
            {
#ifdef NDEBUG
                /// TODO (alesapin) remove this, it should be always CORRUPTED_DATA.
                LOG_ERROR(
                    getLogger("KeeperStorage"),
                    "Children counter in stat.numChildren {}"
                    " is different from actual children size {} for node {}",
                    itr.value.stats.getNumChildren(),
                    itr.value.getChildren().size(),
                    itr.key);
#else
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Children counter in stat.numChildren {}"
                    " is different from actual children size {} for node {}",
                    itr.value.stats.getNumChildren(),
                    itr.value.getChildren().size(),
                    itr.key);
#endif
            }
        }
    }
}

std::unique_ptr<KeeperNodeStreamForSnapshot> KeeperMemNodesStorage::beginWritingSnapshot()
{
    auto res = std::make_unique<NodeStreamForSnapshot>();
    auto [size, ver] = container.snapshotSizeWithVersion();
    container.enableSnapshotMode(ver);
    res->node_count = size;
    res->it = container.begin();
    return res;
}

void KeeperMemNodesStorage::finishWritingSnapshot(std::unique_ptr<KeeperNodeStreamForSnapshot> stream)
{
    stream->node_count = 0;
    container.disableSnapshotMode();
    container.clearOutdatedNodes();
}

bool KeeperMemNodesStorage::NodeStreamForSnapshot::next(std::string_view & out_path, std::string_view & out_data, KeeperNodeStats & out_stats)
{
    if (next_node_idx >= node_count)
        return false;

    out_path = it->key;
    out_data = it->value.getData();
    out_stats = it->value.stats;

    ++next_node_idx;
    if (next_node_idx < node_count) // don't move the iterator past the end of immutable range
        ++it;

    return true;
}

void KeeperMemNodesStorage::getNodeStorageStats(KeeperStorageStats & out)
{
    out.nodes_count = container.size();
    out.approximate_data_size = container.getApproximateDataSize();
}

void KeeperMemNodesStorage::recalculateStats()
{
    container.recalculateDataSize();
}

void KeeperMemNodesStorage::commitDelta(Delta & delta, uint64_t * digest)
{
    std::visit(
        [&, &path = delta.path]<typename DeltaType>(const DeltaType & operation) -> Coordination::Error
        {
            if constexpr (std::same_as<DeltaType, CreateNodeDelta>)
            {
                if (!createNode(path, operation.data, operation.stat, operation.acl_id, operation.ttl, digest))
                    onStorageInconsistency("Failed to create a node");

                return Coordination::Error::ZOK;
            }
            else if constexpr (std::same_as<DeltaType, UpdateNodeStatDelta> || std::same_as<DeltaType, UpdateNodeDataDelta>)
            {
                auto node_it = container.find(path);
                if (node_it == container.end())
                    onStorageInconsistency("Node to be updated is missing");

                if constexpr (std::same_as<DeltaType, UpdateNodeStatDelta>)
                {
                    chassert(operation.old_stats.version == node_it->value.stats.version);
                }
                else
                {
                    chassert(operation.old_data == node_it->value.getData());
                }

                if (digest)
                    *digest -= node_it->value.getDigest(path);

                auto updated_node = container.updateValue(path, [&](auto & node)
                {
                    if constexpr (std::same_as<DeltaType, UpdateNodeStatDelta>)
                        node.stats = operation.new_stats;
                    else
                        node.setData(operation.new_data);

                    node.invalidateDigestCache();
                });

                if (digest)
                    *digest += updated_node->value.getDigest(path);

                return Coordination::Error::ZOK;
            }
            else if constexpr (std::same_as<DeltaType, RemoveNodeDelta>)
            {
                if (!removeNode(path, operation.stat.version, digest))
                    onStorageInconsistency("Failed to remove node");

                return Coordination::Error::ZOK;
            }
            else
            {
                // shouldn't be called in any process functions
                onStorageInconsistency("Invalid delta operation");
            }
        },
        delta.operation);
}

void KeeperMemNodesStorage::cleanupUncommittedState(int64_t commit_zxid)
{
    for (auto it = uncommitted_zxid_to_nodes.begin(); it != uncommitted_zxid_to_nodes.end(); it = uncommitted_zxid_to_nodes.erase(it))
    {
        const auto & [transaction_zxid, transaction_nodes] = *it;

        if (transaction_zxid > commit_zxid)
            break;

        for (const auto node_it : transaction_nodes)
        {
            std::erase(node_it->second.applied_zxids, transaction_zxid);
            if (node_it->second.applied_zxids.empty())
                uncommitted_nodes.erase(node_it);
        }
    }
}

void KeeperMemNodesStorage::rollbackUncommittedDelta(const Delta & delta)
{
    auto & [node, applied_zxids] = uncommitted_nodes.at(delta.path);

    std::visit(
        [&]<typename DeltaType>(const DeltaType & operation)
        {
            if constexpr (std::same_as<DeltaType, CreateNodeDelta>)
            {
                chassert(node);
                node = nullptr;
            }
            else if constexpr (std::same_as<DeltaType, RemoveNodeDelta>)
            {
                chassert(!node);
                node = std::make_shared<Node>();
                node->stats = operation.stat;
                node->setData(operation.data);
            }
            else if constexpr (std::same_as<DeltaType, UpdateNodeStatDelta>)
            {
                chassert(node);
                node->invalidateDigestCache();
                node->stats = operation.old_stats;
            }
            else if constexpr (std::same_as<DeltaType, UpdateNodeDataDelta>)
            {
                chassert(node);
                node->invalidateDigestCache();
                node->setData(operation.old_data);
            }
            else
            {
                onStorageInconsistency("Unexpected delta type in rollbackUncommittedDelta");
            }
        },
        delta.operation);
}

void KeeperMemNodesStorage::cleanupAfterRollback(std::vector<uint64_t> rollbacked_zxids)
{
    /// once we have rollbacked all operations, we can cleanup nodes that were
    /// created just for these transactions
    const auto cleanup_uncommitted_nodes = [&](const auto for_zxid)
    {
        auto it = uncommitted_zxid_to_nodes.find(for_zxid);

        if (it == uncommitted_zxid_to_nodes.end())
            return;

        const auto & [transaction_zxid, transaction_nodes] = *it;

        for (const auto node_it : transaction_nodes)
        {
            std::erase(node_it->second.applied_zxids, transaction_zxid);
            if (node_it->second.applied_zxids.empty())
                uncommitted_nodes.erase(node_it);
        }

        uncommitted_zxid_to_nodes.erase(it);
    };

    /// first cleanup nodes that were not modified by those transactions
    cleanup_uncommitted_nodes(0);
    std::ranges::for_each(rollbacked_zxids, cleanup_uncommitted_nodes);
}

void KeeperMemNodesStorage::updateNodesDigest(uint64_t & current_digest, uint64_t for_zxid) const
{
    if (!keeper_context->digestEnabled())
        return;

    auto nodes_it = uncommitted_zxid_to_nodes.find(for_zxid);
    if (nodes_it == uncommitted_zxid_to_nodes.end())
        return;

    for (const auto node_it : nodes_it->second)
    {
        const auto & [path, uncommitted_node] = *node_it;
        if (uncommitted_node.node)
        {
            uncommitted_node.node->invalidateDigestCache();
            current_digest += uncommitted_node.node->getDigest(path);
        }
    }
}

bool KeeperMemNodesStorage::visitUncommittedRecursive(std::string_view root_path, size_t limit, std::function<bool(std::string_view /*path*/, UncommittedNodeRef &&)> check_node)
{
    struct PathCmp
    {
        auto operator()(const std::string_view a,
                        const std::string_view b) const
        {
            size_t level_a = std::count(a.begin(), a.end(), '/');
            size_t level_b = std::count(b.begin(), b.end(), '/');
            return level_a < level_b || (level_a == level_b && a < b);
        }

        using is_transparent = void; // required to make find() work with different type than key_type
    };

    struct QueueEntry
    {
        std::string path;
        UncommittedNodeRef uncommitted_ref{}; // empty if not in uncommitted_nodes
    };
    std::deque<QueueEntry> queue;

    {
        UncommittedNodeRef root_node = getUncommittedNode(root_path);
        if (!root_node.get())
            return true;

        queue.push_back(QueueEntry{std::string{root_path}, std::move(root_node)});
    }

    /// Collect uncommitted children of root node in a specialized structure so we avoid iterating
    /// all uncommitted nodes for each child node.
    std::map<std::string_view, UncommittedNodesIterator, PathCmp> uncommitted_children;
    for (auto it = uncommitted_nodes.begin(); it != uncommitted_nodes.end(); ++it)
    {
        if (Coordination::matchPath(it->first, root_path) == Coordination::PathMatchResult::IS_CHILD)
            uncommitted_children[it->first] = it;
    }

    size_t nodes_visited = 0;
    auto limit_reached = [&]
    {
        return nodes_visited + queue.size() > limit;
    };

    while (!queue.empty())
    {
        std::string path = std::move(queue.front().path);
        chassert(!path.empty());
        UncommittedNodeRef uncommitted_ref = std::move(queue.front().uncommitted_ref);
        queue.pop_front();
        ++nodes_visited;

        std::unordered_set<std::string_view, StringHashForHeterogeneousLookup, StringHashForHeterogeneousLookup::transparent_key_equal> processed_uncommitted_children;

        /// Add uncommitted children to queue.
        for (auto nodes_it = uncommitted_children.upper_bound(path + "/");
             nodes_it != uncommitted_children.end() && Coordination::parentNodePath(nodes_it->first) == path;
             ++nodes_it)
        {
            const auto & [node_path, uncommitted_node_it] = *nodes_it;

            processed_uncommitted_children.insert(node_path);

            if (uncommitted_node_it->second.node == nullptr)
                /// Node was deleted in uncommitted state. Don't visit it, but it's important that
                /// we added it to processed_uncommitted_children; otherwise it could be incorrectly
                /// visited by the committed children iteration below.
                continue;

            queue.push_back(QueueEntry{std::string{node_path}, UncommittedNodeRef{uncommitted_node_it}});
            if (limit_reached())
                return false;
        }

        /// Add committed children to queue, except the ones already processed as uncommitted above.
        /// Also add to uncommitted_nodes and assign uncommitted_ref if needed.
        {
            std::shared_lock lock(*storage_mutex);

            /// Committed node lookup is needed for two separate reasons:
            ///  * To get committed children list.
            ///  * To add it to uncommitted_nodes, if not present yet.
            if (auto node_it = container.find(path); node_it != container.end())
            {
                const auto & committed_node = node_it->value;

                if (!uncommitted_ref.get())
                {
                    auto node = std::make_shared<Node>();
                    node->shallowCopy(committed_node);
                    auto [uncommitted_node_it, added] = uncommitted_nodes.emplace(path, UncommittedNode{.node = node});
                    chassert(added);
                    uncommitted_zxid_to_nodes[0].insert(uncommitted_node_it);
                    uncommitted_ref = {uncommitted_node_it};
                }

                std::filesystem::path current_path_fs(path);
                const auto & children = node_it->value.getChildren();

                for (const auto & child_name : children)
                {
                    auto child_path = (current_path_fs / child_name).generic_string();

                    if (processed_uncommitted_children.contains(child_path))
                        continue;

                    queue.push_back(QueueEntry{child_path});
                    if (limit_reached())
                        return false;
                }
            }
        }

        /// Finally report the node to the caller.
        chassert(uncommitted_ref.get() != nullptr);
        if (!check_node(path, std::move(uncommitted_ref)))
            return false;
    }

    return true;
}

void KeeperMemNodesStorage::prepareWriteCommon(std::string_view path, UncommittedNodeRef & node, KeeperStagingTransaction & staging)
{
    chassert(node.it.has_value());

    auto node_it = *node.it;
    auto & zxid_nodes = uncommitted_zxid_to_nodes[staging.zxid];
    const bool node_was_not_yet_in_zxid = zxid_nodes.insert(node_it).second;

    /// if it's the first time we see that node in the transaction
    /// we need to subtract it's digest from the point before
    /// we started the transaction
    /// at the end of transaction, we add new node digests in updateNodesDigest
    if (node_was_not_yet_in_zxid && staging.digest.version != KeeperDigestVersion::NO_DIGEST &&
        node_it->second.node)
        staging.digest.value -= node_it->second.node->getDigest(path);

    node_it->second.applied_zxids.push_back(staging.zxid);
}

void KeeperMemNodesStorage::prepareUpdateNodeStat(std::string_view path, UncommittedNodeRef && node, const KeeperNodeStats & new_stats, KeeperStagingTransaction & staging)
{
    prepareWriteCommon(path, node, staging);

    Node * node_ptr = node.getMut();
    UpdateNodeStatDelta delta(node_ptr->stats);
    delta.new_stats = new_stats;
    staging.deltas.emplace_back(std::string{path}, staging.zxid, std::move(delta));

    node_ptr->invalidateDigestCache();
    node_ptr->stats = new_stats;
}

void KeeperMemNodesStorage::prepareUpdateNodeDataAndStat(std::string_view path, UncommittedNodeRef && node, const KeeperNodeStats & new_stats, std::string_view new_data, KeeperStagingTransaction & staging)
{
    prepareWriteCommon(path, node, staging);

    Node * node_ptr = node.getMut();

    /// The data delta must be ordered before the stat delta: at commit time the stat delta
    /// overwrites `stats.data_size` (to the new size), after which `getData` would read the old
    /// buffer with the new size. Committing the data delta first keeps the node consistent.
    staging.deltas.emplace_back(
        std::string{path}, staging.zxid,
        UpdateNodeDataDelta{.old_data = std::string{node_ptr->getData()}, .new_data = std::string{new_data}});

    node_ptr->invalidateDigestCache();
    node_ptr->setData(new_data);

    prepareUpdateNodeStat(path, std::move(node), new_stats, staging);
}

void KeeperMemNodesStorage::prepareCreateNodeWithoutUpdatingParent(
    std::string_view path, UncommittedNodeRef && node, const Coordination::Stat & stat,
    ACLId acl_id, std::string_view data, std::optional<int64_t> ttl,
    KeeperStagingTransaction & staging)
{
    if (!node.it.has_value())
        node.it = uncommitted_nodes.emplace(std::string{path}, UncommittedNode{}).first;
    prepareWriteCommon(path, node, staging);

    staging.deltas.emplace_back(
        std::string{path},
        staging.zxid,
        CreateNodeDelta{stat, acl_id, std::string{data}, ttl});

    auto node_it = *node.it;
    chassert(!node_it->second.node);
    node_it->second.node = std::make_shared<Node>();
    Node * node_ptr = node_it->second.node.get();
    node_ptr->stats.copyStats(stat);
    node_ptr->stats.acl_id = acl_id;
    if (ttl)
        node_ptr->stats.makeTTL(*ttl);
    node_ptr->setData(data);
}

void KeeperMemNodesStorage::prepareRemoveNodeWithoutUpdatingParent(
    std::string_view path, UncommittedNodeRef && node, KeeperStagingTransaction & staging)
{
    prepareWriteCommon(path, node, staging);
    const Node * node_ptr = node.get();
    staging.deltas.emplace_back(
        std::string{path}, staging.zxid,
        RemoveNodeDelta{node_ptr->stats, std::string{node_ptr->getData()}});

    (*node.it)->second.node = nullptr;
}

bool KeeperMemNodesStorage::createNode(
    const std::string & path, String data, const Coordination::Stat & stat, ACLId acl_id, std::optional<int64_t> ttl, uint64_t * digest) TSA_NO_THREAD_SAFETY_ANALYSIS
{
    auto parent_path = Coordination::parentNodePath(path);
    auto node_it = container.find(parent_path);

    if (node_it == container.end())
        return false;

    if (node_it->value.stats.isEphemeral())
        return false;

    if (container.contains(path))
        return false;

    Node created_node;

    created_node.stats.copyStats(stat);
    created_node.stats.acl_id = acl_id;
    if (ttl)
        created_node.stats.makeTTL(*ttl);
    created_node.setData(data);

    auto [map_key, _] = container.insert(path, std::move(created_node));
    /// Take child path from key owned by map.
    auto child_path = Coordination::getBaseNodeName(map_key->getKey());
    container.updateValue(
            parent_path,
            [child_path](KeeperMemNode & parent)
            {
                parent.addChild(child_path);
                chassert(parent.stats.getNumChildren() == static_cast<int32_t>(parent.getChildren().size()));
            }
    );

    if (digest)
        *digest += map_key->getMapped()->value.getDigest(map_key->getKey());

    return true;
};

bool KeeperMemNodesStorage::removeNode(const std::string & path, int32_t version, uint64_t * digest) TSA_NO_THREAD_SAFETY_ANALYSIS
{
    auto node_it = container.find(path);
    if (node_it == container.end())
        return false;

    chassert(version == node_it->value.stats.version);

    if (digest)
        *digest -= node_it->value.getDigest(path);

    container.updateValue(
        Coordination::parentNodePath(path),
        [child_basename = Coordination::getBaseNodeName(node_it->key)](KeeperMemNode & parent)
        {
            parent.removeChild(child_basename);
        }
    );

    container.erase(path);

    return true;
}

}
