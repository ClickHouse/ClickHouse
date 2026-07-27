#include <Coordination/KeeperStorage.h>
#include <Coordination/KeeperStorageImpl.h>
#include <Coordination/KeeperMemNodesStorage.h>
#include <Coordination/KeeperStorage_fwd.h>

#include <algorithm>
#include <limits>
#include <shared_mutex>

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/StringHashForHeterogeneousLookup.h>
#include <Common/StringUtils.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>

#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperConstants.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/KeeperDispatcher.h>
#include <Coordination/KeeperReconfiguration.h>
#include <Coordination/KeeperSnapshotManager.h>

#include <base/defines.h>

namespace ProfileEvents
{
    extern const Event KeeperCreateRequest;
    extern const Event KeeperRemoveRequest;
    extern const Event KeeperSetRequest;
    extern const Event KeeperCheckRequest;
    extern const Event KeeperMultiRequest;
    extern const Event KeeperMultiReadRequest;
    extern const Event KeeperGetRequest;
    extern const Event KeeperListRequest;
    extern const Event KeeperListRecursiveRequest;
    extern const Event KeeperExistsRequest;
    extern const Event KeeperPreprocessElapsedMicroseconds;
    extern const Event KeeperProcessElapsedMicroseconds;
    extern const Event KeeperSetWatchesRequest;
    extern const Event KeeperCheckWatchRequest;
    extern const Event KeeperRemoveWatchRequest;
    extern const Event KeeperAddWatchRequest;
}

namespace DB
{

namespace CoordinationSetting
{
    extern const CoordinationSettingsUInt64 log_slow_cpu_threshold_ms;
    extern const CoordinationSettingsBool check_node_acl_on_remove;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
    extern const char keeper_leader_sets_invalid_digest[];
}

namespace
{

bool fixupACL(
    const std::vector<Coordination::ACL> & request_acls,
    int64_t session_id,
    const KeeperStorage::UncommittedState & uncommitted_state,
    bool block_acl,
    std::vector<Coordination::ACL> & result_acls)
{
    if (block_acl || request_acls.empty())
        return true;

    bool valid_found = false;
    for (const auto & request_acl : request_acls)
    {
        if (request_acl.scheme == "auth")
        {
            uncommitted_state.forEachAuthInSession(
                session_id,
                [&](const KeeperStorage::AuthID & auth_id)
                {
                    valid_found = true;
                    Coordination::ACL new_acl = request_acl;

                    new_acl.scheme = auth_id.scheme;
                    new_acl.id = auth_id.id;

                    result_acls.push_back(new_acl);
                });
        }
        else if (request_acl.scheme == "world" && request_acl.id == "anyone")
        {
            /// Save world:anyone ACLs to support specific permissions
            if (request_acl.permissions != Coordination::ACL::All)
                result_acls.push_back(request_acl);
            valid_found = true;
        }
        else if (request_acl.scheme == "digest")
        {
            Coordination::ACL new_acl = request_acl;

            /// Bad auth
            if (std::count(new_acl.id.begin(), new_acl.id.end(), ':') != 1)
                return false;

            valid_found = true;
            result_acls.push_back(new_acl);
        }
    }
    return valid_found;
}

template <typename F>
auto callOnConcreteRequestType(Coordination::ZooKeeperRequest & zk_request, F function)
{
    switch (zk_request.getOpNum())
    {
        case Coordination::OpNum::Heartbeat:
            return function(static_cast<Coordination::ZooKeeperHeartbeatRequest &>(zk_request));
        case Coordination::OpNum::Sync:
            return function(static_cast<Coordination::ZooKeeperSyncRequest &>(zk_request));
        case Coordination::OpNum::Get:
            return function(static_cast<Coordination::ZooKeeperGetRequest &>(zk_request));
        case Coordination::OpNum::Create:
        case Coordination::OpNum::Create2:
        case Coordination::OpNum::CreateContainer:
        case Coordination::OpNum::CreateIfNotExists:
        case Coordination::OpNum::CreateTTL:
            return function(static_cast<Coordination::ZooKeeperCreateRequest &>(zk_request));
        case Coordination::OpNum::Remove:
            return function(static_cast<Coordination::ZooKeeperRemoveRequest &>(zk_request));
        case Coordination::OpNum::TryRemove:
            return function(static_cast<Coordination::ZooKeeperRemoveRequest &>(zk_request));
        case Coordination::OpNum::RemoveRecursive:
            return function(static_cast<Coordination::ZooKeeperRemoveRecursiveRequest &>(zk_request));
        case Coordination::OpNum::ListRecursive:
            return function(static_cast<Coordination::ZooKeeperListRecursiveRequest &>(zk_request));
        case Coordination::OpNum::Exists:
            return function(static_cast<Coordination::ZooKeeperExistsRequest &>(zk_request));
        case Coordination::OpNum::Set:
            return function(static_cast<Coordination::ZooKeeperSetRequest &>(zk_request));
        case Coordination::OpNum::List:
        case Coordination::OpNum::FilteredList:
        case Coordination::OpNum::FilteredListWithStatsAndData:
        case Coordination::OpNum::SimpleList:
            return function(static_cast<Coordination::ZooKeeperListRequest &>(zk_request));
        case Coordination::OpNum::Check:
        case Coordination::OpNum::CheckNotExists:
        case Coordination::OpNum::CheckStat:
            return function(static_cast<Coordination::ZooKeeperCheckRequest &>(zk_request));
        case Coordination::OpNum::Multi:
        case Coordination::OpNum::MultiRead:
            return function(static_cast<Coordination::ZooKeeperMultiRequest &>(zk_request));
        case Coordination::OpNum::Auth:
            return function(static_cast<Coordination::ZooKeeperAuthRequest &>(zk_request));
        case Coordination::OpNum::Close:
            return function(static_cast<Coordination::ZooKeeperCloseRequest &>(zk_request));
        case Coordination::OpNum::SetACL:
            return function(static_cast<Coordination::ZooKeeperSetACLRequest &>(zk_request));
        case Coordination::OpNum::GetACL:
            return function(static_cast<Coordination::ZooKeeperGetACLRequest &>(zk_request));
        case Coordination::OpNum::AddWatch:
            return function(static_cast<Coordination::ZooKeeperAddWatchRequest &>(zk_request));
        case Coordination::OpNum::SetWatch:
            return function(static_cast<Coordination::ZooKeeperSetWatchesRequest &>(zk_request));
        case Coordination::OpNum::SetWatch2:
            return function(static_cast<Coordination::ZooKeeperSetWatches2Request &>(zk_request));
        case Coordination::OpNum::CheckWatch:
            return function(static_cast<Coordination::ZooKeeperCheckWatchRequest &>(zk_request));
        case Coordination::OpNum::RemoveWatch:
            return function(static_cast<Coordination::ZooKeeperRemoveWatchRequest &>(zk_request));
        default:
            throw Exception{DB::ErrorCodes::LOGICAL_ERROR, "Unexpected request type: {}", zk_request.getOpNum()};
    }
}

bool takeNodeStatsFromUpdateDelta(std::string_view path, const KeeperStorage::DeltaRange & deltas, Coordination::Stat & out_stat)
{
    for (auto it = deltas.end(); it != deltas.begin();)
    {
        --it;
        if (it->path != path)
            continue;
        if (const auto * update_delta = std::get_if<UpdateNodeStatDelta>(&it->operation))
        {
            update_delta->new_stats.setResponseStat(out_stat);
            return true;
        }
    }
    return false;
}

}

/// ========== Request implementations ==========

/// Default implementations ///

/// For most read requests, processLocal can be called for multiple consecutive read requests in
/// parallel. So it must be thread-safe and shouldn't depend on the order of requests.
/// In particular, it shouldn't read or write watches.
/// Requests that need to access watches are excluded from parallel execution in processLocalRequests.
/// processLocal is not allowed to lock storage_mutex as the caller is already holding it
/// (trying to lock it recursively may deadlock if another thread is trying to lock it exclusively).
template <std::derived_from<Coordination::ZooKeeperRequest> T>
Coordination::ZooKeeperResponsePtr
processLocal(const T & zk_request, KeeperStorage & /*storage*/, int64_t /*session_id*/, bool /*check_acl*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Local processing not supported for request with type {}", zk_request.getOpNum());
}

/// Applies changes to UncommittedState and staging.{deltas,digest}.
/// If returns non-ZOK, the caller rolls back all added deltas and adds an ErrorDelta instead.
template <std::derived_from<Coordination::ZooKeeperRequest> T>
Coordination::Error preprocess(
    const T & /*zk_request*/,
    KeeperStorage & /*storage*/,
    int64_t /*session_id*/,
    bool /*check_acl*/,
    int64_t /*time*/,
    const KeeperContext & /*keeper_context*/)
{
    return {};
}

template <std::derived_from<Coordination::ZooKeeperRequest> T>
std::pair<KeeperResponsesForSessions, Int64>
processWatches(const T & /*zk_request*/, KeeperStorage::DeltaRange /*deltas*/, KeeperStorage & /*storage*/, int64_t /*session_id*/)
{
    return {};
}

/// Default implementations ///

/// HEARTBEAT Request ///
static Coordination::ZooKeeperResponsePtr
process(const Coordination::ZooKeeperHeartbeatRequest & zk_request, KeeperStorage & storage, KeeperStorage::DeltaRange deltas, int64_t /*session_id*/)
{
    Coordination::ZooKeeperResponsePtr response_ptr = zk_request.makeResponse();
    response_ptr->error = storage.commit(deltas);
    return response_ptr;
}
/// HEARTBEAT Request ///

/// SYNC Request ///
static Coordination::ZooKeeperResponsePtr
process(const Coordination::ZooKeeperSyncRequest & zk_request, KeeperStorage & /* storage */, KeeperStorage::DeltaRange /* deltas */, int64_t /*session_id*/)
{
    auto response = std::make_shared<Coordination::ZooKeeperSyncResponse>();
    response->path = zk_request.path;
    return response;
}
/// SYNC Request ///

/// CREATE Request ///
static std::pair<KeeperResponsesForSessions, Int64> processWatches(
    const Coordination::ZooKeeperCreateRequest & zk_request,
    KeeperStorage::DeltaRange /*deltas*/,
    KeeperStorage & storage,
    int64_t /*session_id*/)
{
    return storage.processWatchesImpl(zk_request.getPath(), Coordination::Event::CREATED);
}

template <typename Storage>
static Coordination::Error preprocess(
    const Coordination::ZooKeeperCreateRequest & zk_request,
    Storage & storage,
    int64_t session_id,
    bool check_acl,
    int64_t time,
    const KeeperContext & keeper_context)
{
    auto parent_path = Coordination::parentNodePath(zk_request.path);
    auto parent_node_ref = storage.nodes.getUncommittedNode(parent_path);
    const auto * parent_node = parent_node_ref.get();
    if (parent_node == nullptr)
        return Coordination::Error::ZNONODE;

    if (check_acl && !storage.checkACL(parent_node->stats.acl_id, Coordination::ACL::Create, session_id, /*committed=*/ false))
        return Coordination::Error::ZNOAUTH;

    if (parent_node->stats.isEphemeral())
        return Coordination::Error::ZNOCHILDRENFOREPHEMERALS;

    if (parent_node->stats.isTTL())
        return Coordination::Error::ZBADARGUMENTS;

    std::string path_created = zk_request.path;
    if (zk_request.is_sequential)
    {
        if (zk_request.not_exists)
            return Coordination::Error::ZBADARGUMENTS;

        auto seq_num = parent_node->stats.getSeqNum();

        std::stringstream seq_num_str; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        seq_num_str.exceptions(std::ios::failbit);
        seq_num_str << std::setw(10) << std::setfill('0') << seq_num;

        path_created += seq_num_str.str();
    }

    if (Coordination::matchPath(path_created, keeper_system_path) != Coordination::PathMatchResult::NOT_MATCH)
    {
        LOG_ERROR(getLogger("KeeperStorage"), "Trying to create a node inside the internal Keeper path ({}) which is not allowed. Path: {}", keeper_system_path, path_created);
        return Coordination::Error::ZBADARGUMENTS;
    }

    auto child_node_ref = storage.nodes.getUncommittedNode(path_created);
    if (child_node_ref.get() != nullptr)
    {
        if (zk_request.getOpNum() == Coordination::OpNum::CreateIfNotExists)
            return Coordination::Error::ZOK;

        return Coordination::Error::ZNODEEXISTS;
    }

    if (Coordination::getBaseNodeName(path_created).empty())
        return Coordination::Error::ZBADARGUMENTS;

    if (zk_request.include_ttl)
    {
        if (zk_request.is_ephemeral)
            return Coordination::Error::ZBADARGUMENTS;
        /// Reject non-positive or unbounded TTL — matches ZK's MAX_TTL bound and
        /// prevents `time + ttl` overflow when computing destroy_time.
        if (zk_request.ttl <= 0 || zk_request.ttl > MAX_KEEPER_TTL_MS)
            return Coordination::Error::ZBADARGUMENTS;
    }

    Coordination::ACLs node_acls;
    if (!fixupACL(zk_request.acls, session_id, storage.uncommitted_state, keeper_context.shouldBlockACL(), node_acls))
        return Coordination::Error::ZINVALIDACL;

    if (zk_request.is_ephemeral)
        storage.uncommitted_state.ephemerals[session_id].emplace(path_created);

    int32_t parent_cversion = zk_request.parent_cversion;

    KeeperNodeStats new_parent_stats = parent_node->stats;
    new_parent_stats.increaseSeqNum();

    if (parent_cversion == -1)
        ++new_parent_stats.cversion;
    else if (parent_cversion > new_parent_stats.cversion)
        new_parent_stats.cversion = parent_cversion;

    new_parent_stats.pzxid = std::max(storage.staging.zxid, new_parent_stats.pzxid);

    new_parent_stats.increaseNumChildren();

    KeeperNodeStats stats;
    stats.czxid = storage.staging.zxid;
    stats.mzxid = storage.staging.zxid;
    stats.pzxid = storage.staging.zxid;
    stats.setCTime(time);
    stats.mtime = time;
    stats.acl_id = storage.acl_map.convertACLs(node_acls);
    stats.data_size = static_cast<int32_t>(zk_request.data.size());
    if (zk_request.is_ephemeral)
        stats.makeEphemeral(session_id);
    else if (zk_request.include_ttl)
        stats.makeTTL(zk_request.ttl);
    else if (zk_request.is_container)
        stats.makeContainer();

    storage.nodes.prepareUpdateNodeStat(
        parent_path, std::move(parent_node_ref), new_parent_stats, storage.staging);
    storage.nodes.prepareCreateNodeWithoutUpdatingParent(
        path_created, std::move(child_node_ref), stats, zk_request.data, storage.staging);

    return Coordination::Error::ZOK;
}

static Coordination::ZooKeeperResponsePtr process(const Coordination::ZooKeeperCreateRequest & zk_request, KeeperStorage & storage, KeeperStorage::DeltaRange deltas, int64_t /*session_id*/)
{
    ProfileEvents::increment(ProfileEvents::KeeperCreateRequest);

    auto response = std::static_pointer_cast<Coordination::ZooKeeperCreateResponse>(zk_request.makeResponse());

    if (deltas.empty())
    {
        chassert(zk_request.not_exists);
        response->path_created = zk_request.getPath();
        response->error = Coordination::Error::ZOK;
        return response;
    }

    std::string created_path;
    auto create_delta_it = std::find_if(
        deltas.begin(),
        deltas.end(),
        [](const auto & delta)
        { return std::holds_alternative<CreateNodeDelta>(delta.operation); });

    if (create_delta_it != deltas.end())
    {
        created_path = create_delta_it->path;
        if (response->getOpNum() == Coordination::OpNum::Create2 ||
            response->getOpNum() == Coordination::OpNum::CreateTTL ||
            response->getOpNum() == Coordination::OpNum::CreateContainer)
            std::get<CreateNodeDelta>(create_delta_it->operation).stat.setResponseStat(static_cast<Coordination::ZooKeeperCreate2Response &>(*response).zstat);
    }
    if (const auto result = storage.commit(std::move(deltas)); result != Coordination::Error::ZOK)
    {
        response->error = result;
        return response;
    }

    response->path_created = std::move(created_path);
    response->error = Coordination::Error::ZOK;
    return response;
}
/// CREATE Request ///

/// GET Request ///
template <typename Storage>
static Coordination::ZooKeeperResponsePtr
processLocal(const Coordination::ZooKeeperGetRequest & zk_request, Storage & storage, int64_t session_id, bool check_acl)
{
    ProfileEvents::increment(ProfileEvents::KeeperGetRequest);
    auto response = std::make_shared<Coordination::ZooKeeperGetResponse>();

    if (zk_request.path == Coordination::keeper_config_path)
    {
        response->data = serializeClusterConfig(
            storage.keeper_context->getDispatcher()->getStateMachine().getClusterConfig());
        response->error = Coordination::Error::ZOK;
        return response;
    }

    auto node_holder = storage.nodes.getCommittedNode(zk_request.path);
    const auto * node = node_holder.get();
    if (node == nullptr)
    {
        response->error = Coordination::Error::ZNONODE;
    }
    else
    {
        if (check_acl && !storage.checkACL(node->stats.acl_id, Coordination::ACL::Read, session_id, /*committed=*/ true))
        {
            response->error = Coordination::Error::ZNOAUTH;
            return response;
        }

        node->stats.setResponseStat(response->stat);
        auto data = node->getData();
        response->data = std::string(data);
        response->error = Coordination::Error::ZOK;
    }

    return response;
}

template <typename Storage>
static Coordination::ZooKeeperResponsePtr process(const Coordination::ZooKeeperGetRequest & zk_request, Storage & storage, KeeperStorage::DeltaRange deltas, int64_t session_id)
{
    chassert(deltas.empty());
    return processLocal(zk_request, storage, session_id, /*check_acl=*/ true);
}
/// GET Request ///

/// REMOVE Request ///
static std::pair<KeeperResponsesForSessions, Int64> processWatches(
    const Coordination::ZooKeeperRemoveRequest & zk_request,
    KeeperStorage::DeltaRange deltas,
    KeeperStorage & storage,
    int64_t /*session_id*/)
{
    for (const auto & delta : deltas)
    {
        if (std::holds_alternative<RemoveNodeDelta>(delta.operation))
            return storage.processWatchesImpl(zk_request.getPath(), Coordination::Event::DELETED);
    }
    return {};
}

template <typename Storage>
static Coordination::Error preprocess(
    const Coordination::ZooKeeperRemoveRequest & zk_request,
    Storage & storage,
    int64_t session_id,
    bool check_acl,
    int64_t time,
    const KeeperContext & /*keeper_context*/)
{
    if (Coordination::matchPath(zk_request.path, keeper_system_path) != Coordination::PathMatchResult::NOT_MATCH)
    {
        LOG_ERROR(getLogger("KeeperStorage"), "Trying to delete an internal Keeper path ({}) which is not allowed", zk_request.path);
        return Coordination::Error::ZBADARGUMENTS;
    }

    /// The internal TTL/container garbage collector sessions have no ACLs and are server-issued;
    /// they must be allowed to remove a node regardless of user-level Delete ACLs.
    const bool is_ttl_gc_remove = zk_request.try_remove && session_id == keeper_internal_ttl_garbage_collector_session_id;
    const bool is_container_gc_remove = zk_request.try_remove && session_id == keeper_internal_container_garbage_collector_session_id;

    auto parent_path = Coordination::parentNodePath(zk_request.path);
    auto parent_node_ref = storage.nodes.getUncommittedNode(parent_path);
    const auto * parent_node = parent_node_ref.get();

    if (!parent_node)
        return zk_request.try_remove ? Coordination::Error::ZOK : Coordination::Error::ZNONODE;

    const bool is_internal_gc_remove = is_ttl_gc_remove || is_container_gc_remove;
    if (check_acl && !is_internal_gc_remove &&
        !storage.checkACL(parent_node->stats.acl_id, Coordination::ACL::Delete, session_id, /*committed=*/ false))
        return Coordination::Error::ZNOAUTH;

    KeeperNodeStats new_parent_stats = parent_node->stats;

    if (zk_request.restored_from_zookeeper_log && new_parent_stats.pzxid < storage.staging.zxid)
        new_parent_stats.pzxid = storage.staging.zxid;

    auto node_ref = storage.nodes.getUncommittedNode(zk_request.path);
    const auto * node = node_ref.get();

    if (!node)
    {
        if (zk_request.try_remove)
            return Coordination::Error::ZOK;

        if (zk_request.restored_from_zookeeper_log)
        {
            storage.nodes.prepareUpdateNodeStat(parent_path, std::move(parent_node_ref), new_parent_stats, storage.staging);
            return Coordination::Error::ZOK;
        }

        return Coordination::Error::ZNONODE;
    }

    if (check_acl && !is_internal_gc_remove &&
        storage.keeper_context->getCoordinationSettings()[CoordinationSetting::check_node_acl_on_remove] &&
        !storage.checkACL(node->stats.acl_id, Coordination::ACL::Delete, session_id, /*committed=*/ false))
        return Coordination::Error::ZNOAUTH;

    if (zk_request.version != -1 && zk_request.version != node->stats.version)
    {
        if (zk_request.try_remove)
            return {};

        return Coordination::Error::ZBADVERSION;
    }

    if (is_ttl_gc_remove)
    {
        /// Re-check TTL expiration, in case user requests bumped modification time or re-created the
        /// node after garbage collector decided to delete it.
        if (!node->stats.isTTL() || time < node->stats.destroyTime())
            return {};
    }
    /// Similarly re-check container deletions. This has ABA problem: the container could be removed
    /// and re-created since the GC request was issued. In this case we may delete a newly created
    /// node too early, before container_gc_max_never_used_interval_ms elapsed. This is ok and
    /// not worth fixing, and ZooKeeper behaves the same way.
    if (is_container_gc_remove)
    {
        if (!node->stats.isContainer() || node->stats.getNumChildren() != 0)
            return {};
    }

    if (node->stats.getNumChildren() != 0)
    {
        if (zk_request.try_remove)
            return {};

        return Coordination::Error::ZNOTEMPTY;
    }

    ++new_parent_stats.cversion;
    new_parent_stats.decreaseNumChildren();

    if (node->stats.isEphemeral())
    {
        /// try deleting the ephemeral node from the uncommitted state
        unregisterEphemeralPath(storage.uncommitted_state.ephemerals, node->stats.getEphemeralOwner(), zk_request.path, /*throw_if_missing=*/false);
    }

    storage.nodes.prepareUpdateNodeStat(parent_path, std::move(parent_node_ref), new_parent_stats, storage.staging);
    storage.nodes.prepareRemoveNodeWithoutUpdatingParent(zk_request.path, std::move(node_ref), storage.staging);

    return Coordination::Error::ZOK;
}

static Coordination::ZooKeeperResponsePtr
process(const Coordination::ZooKeeperRemoveRequest & zk_request, KeeperStorage & storage, KeeperStorage::DeltaRange deltas, int64_t /*session_id*/)
{
    ProfileEvents::increment(ProfileEvents::KeeperRemoveRequest);

    auto response = zk_request.makeResponse();

    if (deltas.empty())
    {
        chassert(zk_request.try_remove);
        response->error = Coordination::Error::ZOK;
        return response;
    }

    response->error = storage.commit(std::move(deltas));
    return response;
}

/// REMOVE Request ///

/// REMOVERECURSIVE Request ///

static std::pair<KeeperResponsesForSessions, Int64> processWatches(
    const Coordination::ZooKeeperRemoveRecursiveRequest & /*zk_request*/,
    KeeperStorage::DeltaRange deltas,
    KeeperStorage & storage,
    int64_t /*session_id*/)
{
    KeeperResponsesForSessions responses;
    Int64 total_removed_watches = 0;
    for (const auto & delta : deltas)
    {
        if (std::holds_alternative<RemoveNodeDelta>(delta.operation))
        {
            auto [new_responses, removed_watches] = storage.processWatchesImpl(delta.path, Coordination::Event::DELETED);
            responses.insert(responses.end(), std::make_move_iterator(new_responses.begin()), std::make_move_iterator(new_responses.end()));
            total_removed_watches += removed_watches;
        }
    }

    return {responses, total_removed_watches};
}

template <typename Storage>
static Coordination::Error preprocess(
    const Coordination::ZooKeeperRemoveRecursiveRequest & zk_request,
    Storage & storage,
    int64_t session_id,
    bool check_acl,
    int64_t /* time */,
    const KeeperContext & /*keeper_context*/)
{
    if (Coordination::matchPath(zk_request.path, keeper_system_path) != Coordination::PathMatchResult::NOT_MATCH)
    {
        LOG_ERROR(getLogger("KeeperStorage"), "Trying to delete (recursive) an internal Keeper path ({}) which is not allowed", zk_request.path);
        return Coordination::Error::ZBADARGUMENTS;
    }

    if (zk_request.path == "/")
        /// Refuse to removeRecursive the root node.
        /// Alternatively, we could allow it but skip removing system nodes and the "/" itself.
        return Coordination::Error::ZBADARGUMENTS;

    auto parent_path = Coordination::parentNodePath(zk_request.path);
    auto parent_node_ref = storage.nodes.getUncommittedNode(parent_path);
    const auto * parent_node = parent_node_ref.get();

    if (!parent_node)
        return Coordination::Error::ZOK;

    if (check_acl && !storage.checkACL(parent_node->stats.acl_id, Coordination::ACL::Delete, session_id, /*committed=*/ false))
        return Coordination::Error::ZNOAUTH;

    KeeperNodeStats new_parent_stats = parent_node->stats;

    Coordination::Error error = Coordination::Error::ZNOTEMPTY;
    std::deque<std::pair<std::string, typename Storage::UncommittedNodeRef>> nodes_to_remove;
    bool visited_all = storage.nodes.visitUncommittedRecursive(zk_request.path, zk_request.remove_nodes_limit,
            [&](std::string_view path, typename Storage::UncommittedNodeRef && uncommitted_ref)
            {
                if (check_acl && !storage.checkACL(uncommitted_ref.get()->stats.acl_id, Coordination::ACL::Delete, session_id, /*committed=*/false))
                {
                    error = Coordination::Error::ZNOAUTH;
                    return false;
                }
                nodes_to_remove.emplace_back(std::string{path}, std::move(uncommitted_ref));
                return true;
            });

    if (!visited_all)
        return error;

    if (nodes_to_remove.empty())
        /// The node doesn't exist. Don't update parent.
        return Coordination::Error::ZOK;

    ++new_parent_stats.cversion;
    new_parent_stats.decreaseNumChildren();

    for (const auto & [path, uncommitted_ref] : nodes_to_remove)
    {
        if (uncommitted_ref.get()->stats.isEphemeral())
        {
            unregisterEphemeralPath(
                storage.uncommitted_state.ephemerals, uncommitted_ref.get()->stats.getEphemeralOwner(), path, /*throw_if_missing=*/false);
        }
    }

    storage.nodes.prepareUpdateNodeStat(parent_path, std::move(parent_node_ref), new_parent_stats, storage.staging);

    /// Remove children before parents (the traversal above is pre-order).
    for (auto it = nodes_to_remove.rbegin(); it != nodes_to_remove.rend(); ++it)
        storage.nodes.prepareRemoveNodeWithoutUpdatingParent(it->first, std::move(it->second), storage.staging);

    return Coordination::Error::ZOK;
}

static Coordination::ZooKeeperResponsePtr
process(const Coordination::ZooKeeperRemoveRecursiveRequest & zk_request, KeeperStorage & storage, KeeperStorage::DeltaRange deltas, int64_t /*session_id*/)
{
    ProfileEvents::increment(ProfileEvents::KeeperRemoveRequest);

    Coordination::ZooKeeperResponsePtr response_ptr = zk_request.makeResponse();
    response_ptr->error = storage.commit(std::move(deltas));
    return response_ptr;
}

/// REMOVERECURSIVE Request ///

/// LISTRECURSIVE Request ///
template <typename Storage>
static Coordination::ZooKeeperResponsePtr
processLocal(const Coordination::ZooKeeperListRecursiveRequest & zk_request, Storage & storage, int64_t session_id, bool check_acl)
{
    ProfileEvents::increment(ProfileEvents::KeeperListRecursiveRequest);
    auto response = std::make_shared<Coordination::ZooKeeperListRecursiveResponse>();

    auto root_node_holder = storage.nodes.getCommittedNode(zk_request.path);
    const auto * root_node = root_node_holder.get();
    if (root_node == nullptr)
    {
        response->error = Coordination::Error::ZNONODE;
        return response;
    }

    if (check_acl && !storage.checkACL(root_node->stats.acl_id, Coordination::ACL::Read, session_id, /*committed=*/ true))
    {
        response->error = Coordination::Error::ZNOAUTH;
        return response;
    }

    response->children.push_back(std::string{zk_request.path});
    bool reached_limit = false;
    for (size_t frontier = 0; frontier < response->children.size(); ++frontier)
    {
        const std::string & current_path = response->children[frontier];
        std::filesystem::path current_path_fs(current_path);

        std::vector<std::string> children_names = storage.nodes.listCommittedChildrenNames(current_path);
        for (const std::string & child_name : children_names)
        {
            std::string child_path = (current_path_fs / child_name).generic_string();

            if (check_acl)
            {
                const auto child_node_holder = storage.nodes.getCommittedNode(child_path);
                const auto * child_node = child_node_holder.get();
                chassert(child_node != nullptr);
                if (!storage.checkACL(child_node->stats.acl_id, Coordination::ACL::Read, session_id, /*committed=*/ true))
                    continue;
            }

            if (response->children.size() - 1 >= zk_request.children_nodes_limit)
            {
                reached_limit = true;
                break;
            }
            response->children.emplace_back(child_path);
        }

        if (reached_limit)
            break;
    }

    /// Remove the root path
    response->children.erase(response->children.begin());

    response->error = Coordination::Error::ZOK;
    return response;
}

template <typename Storage>
static Coordination::ZooKeeperResponsePtr process(const Coordination::ZooKeeperListRecursiveRequest & zk_request, Storage & storage, KeeperStorage::DeltaRange deltas, int64_t session_id)
{
    chassert(deltas.empty());
    return processLocal(zk_request, storage, session_id, /*check_acl=*/ true);
}
/// LISTRECURSIVE Request ///

/// EXISTS Request ///
template <typename Storage>
static Coordination::ZooKeeperResponsePtr
processLocal(const Coordination::ZooKeeperExistsRequest & zk_request, Storage & storage, int64_t /*session_id*/, bool /*check_acl*/)
{
    ProfileEvents::increment(ProfileEvents::KeeperExistsRequest);
    auto response = std::make_shared<Coordination::ZooKeeperExistsResponse>();

    auto node_holder = storage.nodes.getCommittedNode(zk_request.path);
    const auto * node = node_holder.get();
    if (node == nullptr)
    {
        response->error = Coordination::Error::ZNONODE;
    }
    else
    {
        node->stats.setResponseStat(response->stat);
        response->error = Coordination::Error::ZOK;
    }

    return response;
}

template <typename Storage>
static Coordination::ZooKeeperResponsePtr process(const Coordination::ZooKeeperExistsRequest & zk_request, Storage & storage, KeeperStorage::DeltaRange deltas, int64_t session_id)
{
    chassert(deltas.empty());
    return processLocal(zk_request, storage, session_id, /*check_acl=*/ true);
}
/// EXISTS Request ///

/// SET Request ///
static std::pair<KeeperResponsesForSessions, Int64> processWatches(
    const Coordination::ZooKeeperSetRequest & zk_request,
    KeeperStorage::DeltaRange /*deltas*/,
    KeeperStorage & storage,
    int64_t /*session_id*/)
{
    return storage.processWatchesImpl(zk_request.getPath(), Coordination::Event::CHANGED);
}

template <typename Storage>
static Coordination::Error preprocess(
    const Coordination::ZooKeeperSetRequest & zk_request,
    Storage & storage,
    int64_t session_id,
    bool check_acl,
    int64_t time,
    const KeeperContext & /*keeper_context*/)
{
    if (Coordination::matchPath(zk_request.path, keeper_system_path) != Coordination::PathMatchResult::NOT_MATCH)
    {
        LOG_ERROR(getLogger("KeeperStorage"), "Trying to update an internal Keeper path ({}) which is not allowed", zk_request.path);
        return Coordination::Error::ZBADARGUMENTS;
    }

    auto node_ref = storage.nodes.getUncommittedNode(zk_request.path);
    const auto * node = node_ref.get();

    if (!node)
        return Coordination::Error::ZNONODE;

    if (check_acl && !storage.checkACL(node->stats.acl_id, Coordination::ACL::Write, session_id, /*committed=*/ false))
        return Coordination::Error::ZNOAUTH;

    if (zk_request.version != -1 && zk_request.version != node->stats.version)
        return Coordination::Error::ZBADVERSION;

    KeeperNodeStats new_stats = node->stats;
    new_stats.version++;
    new_stats.mzxid = storage.staging.zxid;
    new_stats.mtime = time;
    new_stats.data_size = static_cast<uint32_t>(zk_request.data.size());
    storage.nodes.prepareUpdateNodeDataAndStat(zk_request.path, std::move(node_ref), new_stats, zk_request.data, storage.staging);

    if (zk_request.path != "/")
    {
        auto parent_path = Coordination::parentNodePath(zk_request.path);
        auto parent_node_ref = storage.nodes.getUncommittedNode(parent_path);
        const auto * parent_node = parent_node_ref.get();
        KeeperNodeStats new_parent_stats = parent_node->stats;
        ++new_parent_stats.cversion;
        storage.nodes.prepareUpdateNodeStat(parent_path, std::move(parent_node_ref), new_parent_stats, storage.staging);
    }

    return Coordination::Error::ZOK;
}

static Coordination::ZooKeeperResponsePtr process(const Coordination::ZooKeeperSetRequest & zk_request, KeeperStorage & storage, KeeperStorage::DeltaRange deltas, int64_t /*session_id*/)
{
    ProfileEvents::increment(ProfileEvents::KeeperSetRequest);

    auto response = std::make_shared<Coordination::ZooKeeperSetResponse>();

    bool found_delta = takeNodeStatsFromUpdateDelta(zk_request.path, deltas, response->stat);
    if (const auto result = storage.commit(std::move(deltas)); result != Coordination::Error::ZOK)
    {
        response->error = result;
        return response;
    }
    if (!found_delta)
        onStorageInconsistency("Unexpected deltas for Set request");

    response->error = Coordination::Error::ZOK;
    return response;
}
/// SET Request ///

/// CHECK WATCHES Request ///
static Coordination::ZooKeeperResponsePtr
processLocal(const Coordination::ZooKeeperCheckWatchRequest & zk_request, KeeperStorage & storage, int64_t /*session_id*/, bool /*check_acl*/)
{
    ProfileEvents::increment(ProfileEvents::KeeperCheckWatchRequest);
    auto response = zk_request.makeResponse();

    if (!storage.containsWatch(zk_request.getPath(), zk_request.type))
        response->error = Coordination::Error::ZNOWATCHER;
    return response;
}

static Coordination::ZooKeeperResponsePtr process(const Coordination::ZooKeeperCheckWatchRequest & zk_request, KeeperStorage & storage, KeeperStorage::DeltaRange deltas, int64_t session_id)
{
    chassert(deltas.empty());
    return processLocal(zk_request, storage, session_id, /*check_acl=*/ true);
}
/// CHECK WATCHES Request ///

/// REMOVE WATCHES Request ///
static Coordination::ZooKeeperResponsePtr
processLocal(const Coordination::ZooKeeperRemoveWatchRequest & zk_request, KeeperStorage & storage, int64_t session_id, bool /*check_acl*/)
{
    ProfileEvents::increment(ProfileEvents::KeeperRemoveWatchRequest);
    auto response = zk_request.makeResponse();

    if (!storage.removePersistentWatch(zk_request.path, zk_request.type, session_id))
        response->error = Coordination::Error::ZNOWATCHER;

    return response;
}

static Coordination::ZooKeeperResponsePtr process(const Coordination::ZooKeeperRemoveWatchRequest & zk_request, KeeperStorage & storage, KeeperStorage::DeltaRange deltas, int64_t session_id)
{
    chassert(deltas.empty());
    return processLocal(zk_request, storage, session_id, /*check_acl=*/ true);
}
/// REMOVE WATCHES Request ///

/// ADD WATCHES Request ///
static Coordination::ZooKeeperResponsePtr
processLocal(const Coordination::ZooKeeperAddWatchRequest & zk_request, KeeperStorage & storage, int64_t session_id, bool /*check_acl*/)
{
    ProfileEvents::increment(ProfileEvents::KeeperAddWatchRequest);
    auto response = zk_request.makeResponse();

    storage.addPersistentWatch(zk_request.path, zk_request.mode, session_id);
    return response;
}

static Coordination::ZooKeeperResponsePtr process(const Coordination::ZooKeeperAddWatchRequest & zk_request, KeeperStorage & storage, KeeperStorage::DeltaRange deltas, int64_t session_id)
{
    chassert(deltas.empty());
    return processLocal(zk_request, storage, session_id, /*check_acl=*/ true);
}
/// ADD WATCHES Request ///

/// SET WATCHES Request ///
static std::pair<KeeperResponsesForSessions, Int64> processWatches(
    const Coordination::ZooKeeperSetWatchesRequest & zk_request,
    KeeperStorage::DeltaRange /*deltas*/,
    KeeperStorage & storage,
    int64_t session_id)
{
    auto watch_responses = storage.setWatches(
        zk_request.zxid, zk_request.data_watches, zk_request.child_watches, zk_request.exist_watches, {}, {}, session_id);
    return {std::move(watch_responses), 0};
}

static Coordination::ZooKeeperResponsePtr
processLocal(const Coordination::ZooKeeperSetWatchesRequest & zk_request, KeeperStorage & /*storage*/, int64_t /*session_id*/, bool /*check_acl*/)
{
    ProfileEvents::increment(ProfileEvents::KeeperSetWatchesRequest);
    auto response_ptr = zk_request.makeResponse();
    response_ptr->error = Coordination::Error::ZOK;
    return response_ptr;
}

static Coordination::ZooKeeperResponsePtr process(const Coordination::ZooKeeperSetWatchesRequest & zk_request, KeeperStorage & storage, KeeperStorage::DeltaRange deltas, int64_t session_id)
{
    chassert(deltas.empty());
    return processLocal(zk_request, storage, session_id, /*check_acl=*/ true);
}
/// SET WATCHES Request ///

/// SET WATCHES 2 Request ///
static std::pair<KeeperResponsesForSessions, Int64> processWatches(
    const Coordination::ZooKeeperSetWatches2Request & zk_request,
    KeeperStorage::DeltaRange /*deltas*/,
    KeeperStorage & storage,
    int64_t session_id)
{
    auto watch_responses = storage.setWatches(
        zk_request.zxid,
        zk_request.data_watches,
        zk_request.child_watches,
        zk_request.exist_watches,
        zk_request.persistent_watches,
        zk_request.persistent_recursive_watches,
        session_id);
    return {std::move(watch_responses), 0};
}

static Coordination::ZooKeeperResponsePtr
processLocal(const Coordination::ZooKeeperSetWatches2Request & zk_request, KeeperStorage & /*storage*/, int64_t /*session_id*/, bool /*check_acl*/)
{
    ProfileEvents::increment(ProfileEvents::KeeperSetWatchesRequest);
    auto response_ptr = zk_request.makeResponse();
    response_ptr->error = Coordination::Error::ZOK;
    return response_ptr;
}

static Coordination::ZooKeeperResponsePtr process(const Coordination::ZooKeeperSetWatches2Request & zk_request, KeeperStorage & storage, KeeperStorage::DeltaRange deltas, int64_t session_id)
{
    chassert(deltas.empty());
    return processLocal(zk_request, storage, session_id, /*check_acl=*/ true);
}
/// SET WATCHES 2 Request ///

/// LIST Request ///
template <typename Storage>
static Coordination::ZooKeeperResponsePtr
processLocal(const Coordination::ZooKeeperListRequest & zk_request, Storage & storage, int64_t session_id, bool check_acl)
{
    ProfileEvents::increment(ProfileEvents::KeeperListRequest);
    auto response = std::static_pointer_cast<Coordination::ZooKeeperListResponse>(zk_request.makeResponse());

    auto node_holder = storage.nodes.getCommittedNode(zk_request.path);
    const auto * node = node_holder.get();
    if (node == nullptr)
    {
        response->error = Coordination::Error::ZNONODE;
        return response;
    }

    auto auth_error = [&]() -> Coordination::ZooKeeperResponsePtr
    {
        response->names.clear();
        response->stats.clear();
        response->data.clear();
        response->error = Coordination::Error::ZNOAUTH;
        return std::move(response);
    };

    if (check_acl && !storage.checkACL(node->stats.acl_id, Coordination::ACL::Read, session_id, /*committed=*/ true))
        return auth_error();

    bool is_system_node = zk_request.path.starts_with(keeper_system_path);

    const auto list_request_type = zk_request.list_request_type.value_or(Coordination::ListRequestType::ALL);
    const bool with_stat = zk_request.with_stat.value_or(false);
    const bool with_data = zk_request.with_data.value_or(false);

    std::vector<std::string> children_names;

    /// (Save NodeStorage the trouble of looking for children if we know there are none.)
    if (is_system_node || node->stats.getNumChildren() != 0)
    {
        if constexpr (std::is_same_v<Storage, KeeperMemoryStorage>)
            /// Pass additional `nodes` argument, as an optimization.
            children_names = storage.nodes.listCommittedChildrenNames(zk_request.path, node);
        else
            children_names = storage.nodes.listCommittedChildrenNames(zk_request.path);
    }

    if (!with_stat && !with_data && Coordination::ListRequestType::ALL == list_request_type)
    {
        /// Only need children names, no stats or data.
        /// We don't check children ACLs here because existence of znodes is not secret in zookeeper.
        response->names = std::move(children_names);
    }
    else
    {
        std::string child_path = zk_request.path;
        if (!child_path.ends_with("/"))
            child_path += "/";
        size_t child_path_prefix = child_path.size();
        for (const std::string & child_name : children_names)
        {
            /// (Reuse a string for a little bit of speed.)
            child_path.resize(child_path_prefix);
            child_path += child_name;

            const auto child_node_holder = storage.nodes.getCommittedNode(child_path);
            const auto * child_node = child_node_holder.get();
            chassert(child_node != nullptr);

            /// Check child's ACLs because we're going to report its data or stats - this
            /// information should not be accessible without Read permission.
            /// (Even if only list_request_type filtering is enabled, we'll expose the
            ///  is_ephemeral flag, which is also secret.)
            if (check_acl && !storage.checkACL(child_node->stats.acl_id, Coordination::ACL::Read, session_id, /*committed=*/ true))
                return auth_error();

            using enum Coordination::ListRequestType;
            if (Coordination::ListRequestType::ALL != list_request_type)
            {
                bool is_ephemeral = child_node->stats.isEphemeral();
                if ((is_ephemeral && list_request_type == PERSISTENT_ONLY) ||
                    (!is_ephemeral && list_request_type == EPHEMERAL_ONLY))
                    continue;
            }

            response->names.emplace_back(child_name);

            if (with_stat)
            {
                Coordination::Stat child_stat;
                child_node->stats.setResponseStat(child_stat);
                response->stats.emplace_back(child_stat);
            }
            if (with_data)
                response->data.emplace_back(child_node->getData());
        }
    }

#ifdef DEBUG_OR_SANITIZER_BUILD
    if (!is_system_node &&
        Coordination::ListRequestType::ALL == list_request_type &&
        static_cast<size_t>(node->stats.getNumChildren()) != response->names.size())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Difference between numChildren ({}) and actual children size ({}) for '{}'",
            node->stats.getNumChildren(),
            response->names.size(),
            zk_request.path);
    }
#endif

    node->stats.setResponseStat(response->stat);
    response->error = Coordination::Error::ZOK;
    return response;
}

template <typename Storage>
static Coordination::ZooKeeperResponsePtr process(const Coordination::ZooKeeperListRequest & zk_request, Storage & storage, KeeperStorage::DeltaRange deltas, int64_t session_id)
{
    chassert(deltas.empty());
    return processLocal(zk_request, storage, session_id, /*check_acl=*/ true);
}
/// LIST Request ///

/// CHECK Request ///
namespace
{

bool checkNodeStat(const KeeperNodeStats & verifiable, const Coordination::Stat & validator)
{
    if (validator.czxid != -1 && validator.czxid != verifiable.czxid)
        return false;
    else if (validator.mzxid != -1 && validator.mzxid != verifiable.mzxid)
        return false;
    else if (validator.ctime != -1 && validator.ctime != verifiable.getCTime())
        return false;
    else if (validator.mtime != -1 && validator.mtime != verifiable.mtime)
        return false;
    else if (validator.version != -1 && validator.version != verifiable.version)
        return false;
    else if (validator.cversion != -1 && validator.cversion != verifiable.cversion)
        return false;
    else if (validator.aversion != -1 && validator.aversion != verifiable.aversion)
        return false;
    else if (validator.ephemeralOwner != -1 && validator.ephemeralOwner != verifiable.getEphemeralOwner())
        return false;
    else if (validator.dataLength != -1 && validator.dataLength != static_cast<int32_t>(verifiable.data_size))
        return false;
    else if (validator.numChildren != -1 && validator.numChildren != verifiable.getNumChildren())
        return false;
    else if (validator.pzxid != -1 && validator.pzxid != verifiable.pzxid)
        return false;

    return true;
}

/// Unlike other read requests, Check request must check all conditions in `preprocess`, because
/// it can be used inside Multi request. If we defer checking until `process`, failed Check wouldn't
/// fail the whole Multi.
template <typename Storage>
Coordination::Error preprocess(
    const Coordination::ZooKeeperCheckRequest & zk_request,
    Storage & storage,
    int64_t session_id,
    bool check_acl,
    int64_t /*time*/,
    const KeeperContext & /*keeper_context*/)
{
    if (Coordination::matchPath(zk_request.path, keeper_system_path) != Coordination::PathMatchResult::NOT_MATCH)
    {
        LOG_ERROR(getLogger("KeeperStorage"), "Trying to Check a node inside the internal Keeper path ({}) which is not allowed (because Check is allowed inside Multi requests, which must be deterministic; but internal nodes may depend on Keeper version and configuration). Path: {}", keeper_system_path, zk_request.path);
        return Coordination::Error::ZBADARGUMENTS;
    }

    auto node_ref = storage.nodes.getUncommittedNode(zk_request.path);
    const auto * node = node_ref.get();

    /// Make sure to not leak `version` or `stats` information without Read permission.
    /// Revealing the existence of znode is ok; zookeeper already exposes it without permissions,
    /// e.g. through Get or Exists, which return ZNOAUTH if node exists but ZNONODE if it doesn't.
    if (check_acl && node && !storage.checkACL(node->stats.acl_id, Coordination::ACL::Read, session_id, /*committed=*/ false))
        return Coordination::Error::ZNOAUTH;

    if (zk_request.getOpNum() == Coordination::OpNum::CheckNotExists)
    {
        if (node && (zk_request.version == -1 || zk_request.version == node->stats.version))
            return Coordination::Error::ZNODEEXISTS;
    }
    else
    {
        if (!node)
            return Coordination::Error::ZNONODE;

        if (zk_request.version != -1 && zk_request.version != node->stats.version)
            return Coordination::Error::ZBADVERSION;

        if (zk_request.getOpNum() == Coordination::OpNum::CheckStat && !checkNodeStat(node->stats, zk_request.stat_to_check.value()))
            return Coordination::Error::ZBADVERSION;
    }

    return {};
}

}

template <bool local, typename Storage>
Coordination::ZooKeeperResponsePtr processCheckRequestImpl(const Coordination::ZooKeeperCheckRequest & zk_request, Storage & storage, int64_t session_id, bool check_acl)
{
    auto response = std::static_pointer_cast<Coordination::ZooKeeperCheckResponse>(zk_request.makeResponse());

    const auto on_error = [&]([[maybe_unused]] const auto error_code)
    {
        if constexpr (local)
            response->error = error_code;
        else
            onStorageInconsistency("Node to check is unexpectedly missing");
    };

    auto node_holder = storage.nodes.getCommittedNode(zk_request.path);
    const auto * node = node_holder.get();

    if constexpr (local)
    {
        if (check_acl && node != nullptr &&
            !storage.checkACL(node->stats.acl_id, Coordination::ACL::Read, session_id, /*committed=*/ true))
        {
            response->error = Coordination::Error::ZNOAUTH;
            return response;
        }
    }

    if (zk_request.getOpNum() == Coordination::OpNum::CheckNotExists)
    {
        if (node != nullptr && (zk_request.version == -1 || zk_request.version == node->stats.version))
            on_error(Coordination::Error::ZNODEEXISTS);
        else
            response->error = Coordination::Error::ZOK;
    }
    else
    {
        if (node == nullptr)
            on_error(Coordination::Error::ZNONODE);
        else if (zk_request.version != -1 && zk_request.version != node->stats.version)
            on_error(Coordination::Error::ZBADVERSION);
        else if (zk_request.getOpNum() == Coordination::OpNum::CheckStat && !checkNodeStat(node->stats, zk_request.stat_to_check.value()))
            on_error(Coordination::Error::ZBADVERSION);
        else
            response->error = Coordination::Error::ZOK;
    }

    return response;
}

template <typename Storage>
static Coordination::ZooKeeperResponsePtr process(const Coordination::ZooKeeperCheckRequest & zk_request, Storage & storage, KeeperStorage::DeltaRange deltas, int64_t session_id)
{
    ProfileEvents::increment(ProfileEvents::KeeperCheckRequest);

    if (const auto result = storage.commit(std::move(deltas)); result != Coordination::Error::ZOK)
    {
        auto response = std::static_pointer_cast<Coordination::ZooKeeperCheckResponse>(zk_request.makeResponse());
        response->error = result;
        return response;
    }
    return processCheckRequestImpl<false>(zk_request, storage, session_id, /*check_acl=*/ true);
}

template <typename Storage>
static Coordination::ZooKeeperResponsePtr
processLocal(const Coordination::ZooKeeperCheckRequest & zk_request, Storage & storage, int64_t session_id, bool check_acl)
{
    ProfileEvents::increment(ProfileEvents::KeeperCheckRequest);
    return processCheckRequestImpl<true>(zk_request, storage, session_id, check_acl);
}
/// CHECK Request ///


/// AUTH Request ///
template <typename Storage>
static Coordination::Error preprocess(
    const Coordination::ZooKeeperAuthRequest & zk_request,
    Storage & storage,
    int64_t session_id,
    bool /*check_acl*/,
    int64_t /*time*/,
    const KeeperContext & /*keeper_context*/)
{
    if (zk_request.scheme != "digest" || std::count(zk_request.data.begin(), zk_request.data.end(), ':') != 1)
        return Coordination::Error::ZAUTHFAILED;

    auto new_auth = std::make_shared<KeeperStorage::AuthID>();
    auto auth_digest = KeeperStorage::generateDigest(zk_request.data);
    if (auth_digest == storage.superdigest)
    {
        new_auth->scheme = "super";
    }
    else
    {
        new_auth->scheme = zk_request.scheme;
        new_auth->id = std::move(auth_digest);
        if (storage.uncommitted_state.hasACL(session_id, false, [&](const auto & auth_id) { return *new_auth == auth_id; }))
            return Coordination::Error::ZOK;
    }

    storage.prepareAddAuth(new_auth, session_id);

    return Coordination::Error::ZOK;
}

static Coordination::ZooKeeperResponsePtr
process(const Coordination::ZooKeeperAuthRequest & /*zk_request*/, KeeperStorage & storage, KeeperStorage::DeltaRange deltas, int64_t /*session_id*/)
{
    auto response = std::make_shared<Coordination::ZooKeeperAuthResponse>();

    if (const auto result = storage.commit(std::move(deltas)); result != Coordination::Error::ZOK)
        response->error = result;

    return response;
}
/// AUTH Request ///

/// CLOSE Request ///
static Coordination::ZooKeeperResponsePtr
process(const Coordination::ZooKeeperCloseRequest & /* zk_request */, KeeperStorage &, KeeperStorage::DeltaRange /* deltas */, int64_t /*session_id*/)
{
    throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Called process on close request");
}
/// CLOSE Request ///

/// SETACL Request ///
template <typename Storage>
static Coordination::Error preprocess(
    const Coordination::ZooKeeperSetACLRequest & zk_request,
    Storage & storage,
    int64_t session_id,
    bool check_acl,
    int64_t /*time*/,
    const KeeperContext & keeper_context)
{
    if (Coordination::matchPath(zk_request.path, keeper_system_path) != Coordination::PathMatchResult::NOT_MATCH)
    {
        LOG_ERROR(getLogger("KeeperStorage"), "Trying to update (ACL) an internal Keeper path ({}) which is not allowed", zk_request.path);
        return Coordination::Error::ZBADARGUMENTS;
    }

    auto & uncommitted_state = storage.uncommitted_state;
    auto node_ref = storage.nodes.getUncommittedNode(zk_request.path);
    const auto * node = node_ref.get();
    if (!node)
        return Coordination::Error::ZNONODE;

    if (check_acl && !storage.checkACL(node->stats.acl_id, Coordination::ACL::Admin, session_id, /*committed=*/ false))
        return Coordination::Error::ZNOAUTH;

    if (zk_request.version != -1 && zk_request.version != node->stats.aversion)
        return Coordination::Error::ZBADVERSION;

    Coordination::ACLs node_acls;
    if (!fixupACL(zk_request.acls, session_id, uncommitted_state, keeper_context.shouldBlockACL(), node_acls))
        return Coordination::Error::ZINVALIDACL;

    KeeperNodeStats new_stats = node->stats;
    ++new_stats.aversion;
    new_stats.acl_id = storage.acl_map.convertACLs(node_acls);
    if (new_stats.acl_id == node->stats.acl_id)
        /// Undo the usage increment done by convertACLs. Delta that doesn't change acl_id doesn't
        /// call removeUsage on commit or rollback.
        storage.acl_map.removeUsage(new_stats.acl_id);
    storage.nodes.prepareUpdateNodeStat(zk_request.path, std::move(node_ref), new_stats, storage.staging);

    return Coordination::Error::ZOK;
}

static Coordination::ZooKeeperResponsePtr
process(const Coordination::ZooKeeperSetACLRequest & zk_request, KeeperStorage & storage, KeeperStorage::DeltaRange deltas, int64_t /*session_id*/)
{
    auto response = std::make_shared<Coordination::ZooKeeperSetACLResponse>();

    bool found_delta = takeNodeStatsFromUpdateDelta(zk_request.path, deltas, response->stat);
    if (const auto result = storage.commit(std::move(deltas)); result != Coordination::Error::ZOK)
    {
        response->error = result;
        return response;
    }
    if (!found_delta)
        onStorageInconsistency("Unexpected deltas for SetACL request");
    response->error = Coordination::Error::ZOK;

    return response;
}
/// SETACL Request ///

/// GETACL Request ///
template <typename Storage>
static Coordination::ZooKeeperResponsePtr processLocal(const Coordination::ZooKeeperGetACLRequest & zk_request, Storage & storage, int64_t session_id, bool check_acl)
{
    auto response = std::make_shared<Coordination::ZooKeeperGetACLResponse>();

    auto node_holder = storage.nodes.getCommittedNode(zk_request.path);
    const auto * node = node_holder.get();
    if (node == nullptr)
    {
        response->error = Coordination::Error::ZNONODE;
    }
    else
    {
        if (check_acl && !storage.checkACL(node->stats.acl_id, Coordination::ACL::Admin | Coordination::ACL::Read, session_id, /*committed=*/ true))
        {
            response->error = Coordination::Error::ZNOAUTH;
            return response;
        }

        node->stats.setResponseStat(response->stat);
        response->acl = storage.acl_map.convertNumber(node->stats.acl_id);
    }

    return response;
}

template <typename Storage>
static Coordination::ZooKeeperResponsePtr process(const Coordination::ZooKeeperGetACLRequest & zk_request, Storage & storage, KeeperStorage::DeltaRange deltas, int64_t session_id)
{
    chassert(deltas.empty());
    return processLocal(zk_request, storage, session_id, /*check_acl=*/ true);
}
/// GETACL Request ///

/// MULTI Request ///
template <typename Storage>
static Coordination::Error preprocess(
    const Coordination::ZooKeeperMultiRequest & zk_request,
    Storage & storage,
    int64_t session_id,
    bool check_acl,
    int64_t time,
    const KeeperContext & keeper_context)
{
    const auto & subrequests = zk_request.requests;

    for (size_t i = 0; i < subrequests.size(); ++i)
    {
        Coordination::Error error = callOnConcreteRequestType(
            *subrequests[i],
            [&](const auto & subrequest)
            { return preprocess(subrequest, storage, session_id, check_acl, time, keeper_context); });

        if (error != Coordination::Error::ZOK && zk_request.getOpNum() == Coordination::OpNum::Multi)
        {
            /// Failed multi-write. Caller will roll back any changes we've made.
            /// (Caller has special case to preserve FailedMultiDelta but roll back everything before it.)
            storage.staging.deltas.emplace_back(storage.staging.zxid, FailedMultiDelta{ .failed_pos = i, .failed_pos_error = error });
            return error;
        }

        storage.staging.deltas.emplace_back(storage.staging.zxid, SubDeltaEnd{});
    }

    return Coordination::Error::ZOK;
}

static KeeperStorage::DeltaRange extractSubdeltas(KeeperStorage::DeltaRange & deltas)
{
    auto it = deltas.begin();

    for (; it != deltas.end(); ++it)
    {
        if (std::holds_alternative<SubDeltaEnd>(it->operation))
            break;
    }

    KeeperStorage::DeltaRange result{deltas.begin(), it};
    ++it;
    deltas = KeeperStorage::DeltaRange{it, deltas.end()};
    return result;
}

template <typename Storage>
static Coordination::ZooKeeperResponsePtr
process(const Coordination::ZooKeeperMultiRequest & zk_request, Storage & storage, KeeperStorage::DeltaRange deltas, int64_t session_id)
{
    ProfileEvents::increment(ProfileEvents::KeeperMultiRequest);

    std::shared_ptr<Coordination::ZooKeeperMultiResponse> response;
    if (zk_request.getOpNum() == Coordination::OpNum::Multi)
        response = std::make_shared<Coordination::ZooKeeperMultiWriteResponse>();
    else
        response = std::make_shared<Coordination::ZooKeeperMultiReadResponse>();

    response->responses.reserve(zk_request.requests.size());

    const auto & subrequests = zk_request.requests;

    // the deltas will have at least SubDeltaEnd or FailedMultiDelta
    chassert(!deltas.empty());
    if (const auto * failed_multi = std::get_if<FailedMultiDelta>(&deltas.front().operation))
    {
        const size_t subrequests_count = subrequests.size();

        for (size_t i = 0; i < subrequests_count; ++i)
            response->responses.push_back(std::make_shared<Coordination::ZooKeeperErrorResponse>());

        if (failed_multi->failed_pos < subrequests_count)
        {
            response->responses[failed_multi->failed_pos]->error = failed_multi->failed_pos_error;
            for (size_t i = failed_multi->failed_pos + 1; i < subrequests_count; ++i)
                response->responses[i]->error = Coordination::Error::ZRUNTIMEINCONSISTENCY;
        }

        response->error = failed_multi->global_error;
        return response;
    }

    for (const auto & multi_subrequest : subrequests)
    {
        auto subdeltas = extractSubdeltas(deltas);
        response->responses.push_back(callOnConcreteRequestType(
            *multi_subrequest, [&](const auto & subrequest) { return process(subrequest, storage, std::move(subdeltas), session_id); }));
    }

    response->error = Coordination::Error::ZOK;
    return response;
}

template <typename Storage>
static Coordination::ZooKeeperResponsePtr processLocal(const Coordination::ZooKeeperMultiRequest & zk_request, Storage & storage, int64_t session_id, bool check_acl)
{
    ProfileEvents::increment(ProfileEvents::KeeperMultiReadRequest);
    auto response = std::make_shared<Coordination::ZooKeeperMultiReadResponse>();
    response->responses.reserve(zk_request.requests.size());

    for (const auto & multi_subrequest : zk_request.requests)
    {
        response->responses.push_back(callOnConcreteRequestType(
            *multi_subrequest, [&](const auto & subrequest) { return processLocal(subrequest, storage, session_id, check_acl); }));
    }

    response->error = Coordination::Error::ZOK;
    return response;
}

static std::pair<KeeperResponsesForSessions, Int64> processWatches(
    const Coordination::ZooKeeperMultiRequest & zk_request,
    KeeperStorage::DeltaRange deltas,
    KeeperStorage & storage,
    int64_t session_id)
{
    KeeperResponsesForSessions result;

    if (deltas.empty() || std::get_if<FailedMultiDelta>(&deltas.front().operation))
        return {result, 0};

    const auto & subrequests = zk_request.requests;
    Int64 total_removed_watches = 0;
    for (const auto & generic_request : subrequests)
    {
        auto subdeltas = extractSubdeltas(deltas);
        auto responses = callOnConcreteRequestType(
            *generic_request,
            [&](const auto & subrequest)
            {
                auto [rsp, removed_watches] = processWatches(subrequest, subdeltas, storage, session_id);
                total_removed_watches += removed_watches;
                return rsp;
            });
        result.insert(result.end(), responses.begin(), responses.end());
    }
    return {result, total_removed_watches};
}
/// MULTI Request ///

/// ========== End of request implementations ==========

template <typename NS>
KeeperDigest KeeperStorageImpl<NS>::preprocessRequest(
    const Coordination::ZooKeeperRequestPtr & zk_request,
    int64_t session_id,
    int64_t time,
    int64_t new_last_zxid,
    bool check_acl,
    std::optional<KeeperDigest> digest,
    int64_t log_idx)
{
    Stopwatch watch;
    SCOPE_EXIT({
        watch.stop();

        UInt64 elapsed_us = watch.elapsedMicroseconds();
        UInt64 elapsed_ms = elapsed_us / 1000;

        if (elapsed_ms > keeper_context->getCoordinationSettings()[CoordinationSetting::log_slow_cpu_threshold_ms])
        {
            LOG_INFO(
                getLogger("KeeperStorage"),
                "Preprocessing a request took too long ({}ms).\nRequest info: {}",
                elapsed_ms,
                zk_request->toString(/*short_format=*/true));
        }

        ProfileEvents::increment(ProfileEvents::KeeperPreprocessElapsedMicroseconds, elapsed_us);
    });

    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "KeeperStorage system nodes are not initialized");

    if (!staging.deltas.empty() || staging.zxid != -1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "State left over from previous transaction");

    TransactionInfo * transaction = nullptr;
    {
        std::lock_guard lock(transaction_mutex);
        int64_t last_zxid = getNextZXIDLocked() - 1;
        auto current_digest = getNodesDigest(false, /*lock_transaction_mutex=*/false);

        if (uncommitted_transactions.empty())
        {
            // if we have no uncommitted transactions it means the last zxid is possibly loaded from snapshot
            if (last_zxid != old_snapshot_zxid && new_last_zxid <= last_zxid)
                throw Exception(
                                ErrorCodes::LOGICAL_ERROR,
                                "Got new ZXID ({}) smaller or equal to current ZXID ({}). It's a bug",
                                new_last_zxid, last_zxid);
        }
        else
        {
            /// On leader, preprocessRequest is called for each log entry twice:
            ///  1. In PreAppendLogLeader callback, before the entry is written to changelog.
            ///     (At this point we're allowed to reject the entry. We could do that for failed
            ///      requests to avoid the cost of sending them through raft.
            ///      But currently we don't, all non-read requests go through raft.)
            ///  2. In pre_commit, after the entry is written to changelog, and there's no way back.
            /// Here we detect the second call to avoid preprocessing request twice.
            if (last_zxid == new_last_zxid && digest && checkDigest(*digest, current_digest))
            {
                auto & last_transaction = uncommitted_transactions.back();
                // we found the preprocessed request with the same ZXID, we can get log_idx and skip preprocessing it
                chassert(last_transaction.zxid == new_last_zxid && log_idx != 0);
                /// initially leader preprocessed without knowing the log idx
                /// on the second call we have that information and can set the log idx for the correct transaction
                last_transaction.log_idx = log_idx;
                return current_digest;
            }

            if (new_last_zxid <= last_zxid)
                throw Exception(
                                ErrorCodes::LOGICAL_ERROR,
                                "Got new ZXID ({}) smaller or equal to current ZXID ({}). It's a bug",
                                new_last_zxid, last_zxid);
        }

        staging.zxid = new_last_zxid;
        staging.digest = current_digest;
        KeeperDigestVersion version = keeper_context->digestEnabled() ? KEEPER_CURRENT_DIGEST_VERSION : KeeperDigestVersion::NO_DIGEST;
        chassert(staging.digest.version == version);

        transaction = &uncommitted_transactions.emplace_back(TransactionInfo{.zxid = new_last_zxid, .nodes_digest = current_digest, .log_idx = log_idx});
    }

    bool request_finalized = false;
    const auto finalize = [&](bool rolled_back)
    {
        if (request_finalized)
            return;
        uncommitted_state.addDeltas(std::move(staging.deltas));
        staging.deltas.clear();

        if (zk_request->getOpNum() == Coordination::OpNum::Create
            || zk_request->getOpNum() == Coordination::OpNum::CreateContainer)
        {
            fiu_do_on(FailPoints::keeper_leader_sets_invalid_digest, staging.digest.value = 42);
        }

        if (!rolled_back && staging.digest.version != KeeperDigestVersion::NO_DIGEST)
        {
            nodes.updateNodesDigest(staging.digest.value, new_last_zxid);
            // if the version of digest we got from the leader is the same as the one this instances has, we can simply copy the value
            // and just check the digest on the commit
            // a mistake can happen while applying the changes to the uncommitted_state so for now let's just recalculate the digest here also
            //
            // It's ok to assign this without locking transaction_mutex, even though `transaction`
            // is inside `uncommitted_transactions`. Because it just so happens that no code path
            // reads nodes_digest from uncommitted_transactions without holding nuraft's main lock_,
            // which we're also holding. (Except for commit callback, which reads
            // `uncommitted_transactions.front()`, but only after the transaction is committed,
            // which can't happen before we return from here.)
            transaction->nodes_digest = staging.digest;
        }

        uncommitted_state.cleanup(getZXID());
        staging.zxid = -1;
        request_finalized = true;
    };

    SCOPE_EXIT({
        if (!request_finalized)
        {
            LOG_FATAL(getLogger("KeeperStorage"), "Finalize not called before returning");
            std::abort();
        }
    });

    if (zk_request->getOpNum() == Coordination::OpNum::Close) /// Close request is special
    {
        std::unordered_set<std::string> ephemeral_paths;
        auto it = uncommitted_state.ephemerals.find(session_id);
        if (it != uncommitted_state.ephemerals.end())
        {
            ephemeral_paths = std::move(it->second);
            uncommitted_state.ephemerals.erase(it);
        }
        {
            std::lock_guard ephemeral_lock(ephemeral_mutex);
            it = committed_ephemerals.find(session_id);
            if (it != committed_ephemerals.end())
                ephemeral_paths.insert(it->second.begin(), it->second.end());
        }

        prepareRemoveEphemeralNodes(ephemeral_paths, session_id);

        staging.deltas.emplace_back(staging.zxid, CloseSessionDelta{session_id});
        uncommitted_state.closed_sessions_to_zxids[session_id].insert(staging.zxid);

        finalize(/*rolled_back=*/ false);
        return transaction->nodes_digest;
    }

    Coordination::Error error = Coordination::Error::ZOK;
    const auto preprocess_request = [&]<std::derived_from<Coordination::ZooKeeperRequest> T>(const T & concrete_zk_request)
    {
        error = preprocess(concrete_zk_request, *this, session_id, check_acl, time, *keeper_context);
    };

    callOnConcreteRequestType(*zk_request, preprocess_request);
    if (error == Coordination::Error::ZOK)
    {
#ifndef NDEBUG
        /// In debug build, sometimes do a little consistency test: roll back the transaction,
        /// prepare it again, and assert that the result is the same.
        /// This shouldn't have any externally observable effects.
        /// Good for finding bugs in rollback.
        if (staging.digest.version != KeeperDigestVersion::NO_DIGEST && thread_local_rng() % 2 == 0)
        {
            nodes.updateNodesDigest(staging.digest.value, new_last_zxid);
            const UInt64 first_digest = staging.digest.value;
            const size_t first_delta_count = staging.deltas.size();

            uncommitted_state.rollback(std::move(staging.deltas));
            staging.deltas.clear();
            staging.digest = transaction->nodes_digest;

            callOnConcreteRequestType(*zk_request, preprocess_request);
            chassert(error == Coordination::Error::ZOK, "Re-preprocessing after a spurious rollback unexpectedly failed");

            UInt64 second_digest = staging.digest.value;
            nodes.updateNodesDigest(second_digest, new_last_zxid);
            chassert(
                first_digest == second_digest && first_delta_count == staging.deltas.size(),
                "Re-preprocessing after rollback produced a different result: preprocessing is non-deterministic or rollback is incomplete");
        }
#endif

        finalize(/*rolled_back=*/ false);
    }
    else
    {
        /// Note: We currently only have the capability to roll back *all* deltas for a zxid.
        /// Can't roll back an arbitrary suffix of deltas because there's no digest update logic
        /// in delta rollback. We either calculate fully updated digest (through a combination of
        /// prepareWriteCommon and updateNodesDigest) or revert to original digest.

        /// Hack: Multi request needs a FailedMultiDelta instead of ErrorDelta. We pass it from
        /// `preprocess` to here through staging.deltas.
        std::optional<Delta> custom_error_delta;
        if (!staging.deltas.empty() && std::get_if<FailedMultiDelta>(&staging.deltas.back().operation))
        {
            custom_error_delta = std::move(staging.deltas.back());
            staging.deltas.pop_back();
        }

        uncommitted_state.rollback(std::move(staging.deltas));
        staging.deltas.clear();
        if (custom_error_delta.has_value())
            staging.deltas.push_back(std::move(*custom_error_delta));
        else
            staging.deltas.emplace_back(staging.zxid, error);
        finalize(/*rolled_back=*/ true);
    }
    return transaction->nodes_digest;
}

template <typename NS>
KeeperResponsesForSessions KeeperStorageImpl<NS>::processRequest(
    const Coordination::ZooKeeperRequestPtr & zk_request,
    int64_t session_id,
    std::optional<int64_t> new_last_zxid)
{
    Stopwatch watch;
    SCOPE_EXIT({
        watch.stop();

        UInt64 elapsed_us = watch.elapsedMicroseconds();
        UInt64 elapsed_ms = elapsed_us / 1000;

        if (elapsed_ms > keeper_context->getCoordinationSettings()[CoordinationSetting::log_slow_cpu_threshold_ms])
        {
            LOG_INFO(
                getLogger("KeeperStorage"),
                "Processing a request took too long ({}ms).\nRequest info: {}",
                elapsed_ms,
                zk_request->toString(/*short_format=*/true));
        }

        ProfileEvents::increment(ProfileEvents::KeeperProcessElapsedMicroseconds, elapsed_us);
    });

    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "KeeperStorage system nodes are not initialized");

    int64_t commit_zxid = 0;
    uint64_t transaction_digest = 0;
    {
        std::lock_guard lock(transaction_mutex);
        if (new_last_zxid)
        {
            if (uncommitted_transactions.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to commit a ZXID ({}) which was not preprocessed", *new_last_zxid);

            auto & front_transaction = uncommitted_transactions.front();
            if (front_transaction.zxid != *new_last_zxid)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Trying to commit a ZXID {} while the next ZXID to commit is {}",
                    *new_last_zxid,
                    front_transaction.zxid);

            commit_zxid = *new_last_zxid;
            transaction_digest = front_transaction.nodes_digest.value;
        }
        else
        {
            commit_zxid = zxid;
        }
    }

    std::list<Delta> deltas;
    {
        std::lock_guard lock(uncommitted_state.deltas_mutex);
        auto it = uncommitted_state.deltas.begin();
        for (; it != uncommitted_state.deltas.end() && it->zxid == commit_zxid; ++it)
            ;

        chassert(it == uncommitted_state.deltas.end() || it->zxid > commit_zxid);
        deltas.splice(deltas.end(), uncommitted_state.deltas, uncommitted_state.deltas.begin(), it);
    }

    KeeperStorage::DeltaRange deltas_range{deltas.begin(), deltas.end()};

    KeeperResponsesForSessions results;

    if (session_id >= 0)
        session_expiry_queue.addNewSessionOrUpdate(session_id, session_and_timeout[session_id]);

    if (zk_request->getOpNum() == Coordination::OpNum::Close) /// Close request is special
    {
        for (const auto & delta : deltas)
        {
            if (std::holds_alternative<RemoveNodeDelta>(delta.operation))
            {
                auto [responses, cnt_removed_watches] = processWatchesImpl(delta.path, Coordination::Event::DELETED);
                total_watches_count -= cnt_removed_watches;
                results.insert(results.end(), responses.begin(), responses.end());
            }
        }

        {
            std::lock_guard lock(storage_mutex);
            commit(deltas_range);
            if (!keeper_context->digestEnabledOnCommit())
                nodes_digest = transaction_digest;
        }
        {
            std::lock_guard lock(auth_mutex);
            auto auth_it = committed_session_and_auth.find(session_id);
            if (auth_it != committed_session_and_auth.end())
                committed_session_and_auth.erase(auth_it);
        }

        clearDeadWatches(session_id);

        /// Finish connection
        auto response = std::make_shared<Coordination::ZooKeeperCloseResponse>();
        response->xid = zk_request->xid;
        response->zxid = commit_zxid;
        session_expiry_queue.remove(session_id);
        session_and_timeout.erase(session_id);
        results.push_back(KeeperResponseForSession{session_id, response});
    }
    else if (zk_request->getOpNum() == Coordination::OpNum::Heartbeat) /// Heartbeat request is also special
    {
        Coordination::ZooKeeperResponsePtr response = nullptr;
        {
            std::lock_guard lock(storage_mutex);
            response = process(dynamic_cast<const Coordination::ZooKeeperHeartbeatRequest &>(*zk_request), *this, deltas_range, session_id);
        }
        response->xid = zk_request->xid;
        response->zxid = commit_zxid;

        results.push_back(KeeperResponseForSession{session_id, response});
    }
    else /// normal requests processing
    {
        const auto process_request = [&]<std::derived_from<Coordination::ZooKeeperRequest> T>(T & concrete_zk_request)
        {
            Coordination::ZooKeeperResponsePtr response;

            {
                std::lock_guard lock(storage_mutex);
                response = process(concrete_zk_request, *this, deltas_range, session_id);
                if (!keeper_context->digestEnabledOnCommit())
                    nodes_digest = transaction_digest;
            }

            /// Watches for this request are added to the watches lists
            updateWatches(zk_request, response.get(), session_id);

            /// If this request was processed successfully we need to check watches
            if (response->error == Coordination::Error::ZOK)
            {
                auto [watch_responses, total_removed_watches] = processWatches(concrete_zk_request, deltas_range, *this, session_id);
                total_watches_count -= total_removed_watches;
                results.insert(results.end(), watch_responses.begin(), watch_responses.end());
            }

            response->xid = zk_request->xid;
            response->zxid = commit_zxid;

            results.push_back(KeeperResponseForSession{session_id, response});
        };

        callOnConcreteRequestType(*zk_request, process_request);
    }

    {
        std::lock_guard lock(transaction_mutex);

        if (new_last_zxid)
            uncommitted_transactions.pop_front();

        if (commit_zxid < zxid)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to commit smaller ZXID, commit ZXID: {}, current ZXID {}", commit_zxid, zxid);

        zxid = commit_zxid;
    }

    return results;
}

template <typename NS>
KeeperResponsesForSessions KeeperStorageImpl<NS>::processLocalRequests(
    const KeeperRequestsForSessions & requests,
    bool check_acl)
{
    const UInt64 start_time_us = ZooKeeperOpentelemetrySpans::now();
    Stopwatch watch;
    SCOPE_EXIT({
        watch.stop();

        UInt64 elapsed_us = watch.elapsedMicroseconds();
        UInt64 elapsed_ms = elapsed_us / 1000;

        if (elapsed_ms > keeper_context->getCoordinationSettings()[CoordinationSetting::log_slow_cpu_threshold_ms])
        {
            LOG_INFO(
                getLogger("KeeperStorage"),
                "Processing a batch of local read requests took too long ({}ms for {} requests).\nFirst request info: {}",
                elapsed_ms,
                requests.size(),
                requests.at(0).request->toString(/*short_format=*/true));
        }

        ProfileEvents::increment(ProfileEvents::KeeperProcessElapsedMicroseconds, elapsed_us);
    });

    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "KeeperStorage system nodes are not initialized");

    int64_t current_zxid = getZXID();

    std::list<Delta> empty_deltas;
    KeeperStorage::DeltaRange deltas_range{empty_deltas.begin(), empty_deltas.end()};

    /// Which processLocal overloads can be run in parallel. Excludes things like AddWatch and SetWatches.
    auto is_request_thread_safe = [](Coordination::OpNum op) -> bool
    {
        switch (op)
        {
            case Coordination::OpNum::Exists:
            case Coordination::OpNum::Get:
            case Coordination::OpNum::GetACL:
            case Coordination::OpNum::SimpleList:
            case Coordination::OpNum::List:
            case Coordination::OpNum::Check:
            case Coordination::OpNum::MultiRead:
            case Coordination::OpNum::FilteredList:
            case Coordination::OpNum::CheckNotExists:
            case Coordination::OpNum::CheckStat:
            case Coordination::OpNum::FilteredListWithStatsAndData:
                return true;
            /// (Note: CheckWatch is not on this list because it needs to be ordered correctly with
            ///  other RemoveWatch/AddWatch requests in the same batch.)
            default:
                return false;
        }
    };

    /// Read requests usually have exactly one response each (unlike write requests, which can
    /// trigger watch notifications). So we preallocate the responses array here and write to it
    /// lock-free from worker threads.
    /// Exception: SetWatches/SetWatches2 can produce more responses; we insert them separately below.
    KeeperResponsesForSessions results(requests.size());

    /// Unbundle MultiRead requests so that their subrequests can be processed in parallel.
    /// Useful only if there's a huge MultiRead.
    struct Task
    {
        size_t request_idx;
        int64_t session_id;
        const Coordination::ZooKeeperRequestPtr * request;
        void * response;
        bool is_base_response_type;
        bool thread_safe;

        /// Workaround for historical nonsense: KeeperResponseForSession.response is ZooKeeperResponsePtr,
        /// but MultiResponse.responses[i] is ResponsePtr. They're compatible but slightly different types.
        void setResponse(Coordination::ZooKeeperResponsePtr r) const
        {
            if (is_base_response_type)
                *static_cast<Coordination::ResponsePtr *>(response) = std::move(r);
            else
                *static_cast<Coordination::ZooKeeperResponsePtr *>(response) = std::move(r);
        }
        const Coordination::Response * getResponse() const
        {
            if (is_base_response_type)
                return static_cast<Coordination::ResponsePtr *>(response)->get();
            else
                return static_cast<Coordination::ZooKeeperResponsePtr *>(response)->get();
        }
    };
    std::vector<Task> tasks;
    tasks.reserve(requests.size());

    int64_t prev_session_id = -1;
    for (size_t request_idx = 0; request_idx < requests.size(); ++request_idx)
    {
        const auto & request_for_session = requests[request_idx];
        int64_t session_id = request_for_session.session_id;
        const Coordination::ZooKeeperRequestPtr & zk_request = request_for_session.request;
        results[request_idx] = KeeperResponseForSession{session_id, nullptr, zk_request};

        /// Bump session expiry times along the way.
        if (session_id != prev_session_id)
        {
            session_expiry_queue.addNewSessionOrUpdate(session_id, session_and_timeout[session_id]);
            prev_session_id = session_id;
        }

        auto op = zk_request->getOpNum();
        if (op == Coordination::OpNum::MultiRead)
        {
            ProfileEvents::increment(ProfileEvents::KeeperMultiReadRequest);
            const auto & multi = static_cast<const Coordination::ZooKeeperMultiRequest &>(*zk_request);
            auto response = std::make_shared<Coordination::ZooKeeperMultiReadResponse>();
            response->responses.resize(multi.requests.size());
            response->xid = zk_request->xid;
            response->zxid = current_zxid;
            for (size_t i = 0; i < multi.requests.size(); ++i)
            {
                const Coordination::ZooKeeperRequestPtr & subrequest = multi.requests[i];
                auto * resp = &response->responses[i];
                static_assert(std::is_same_v<decltype(resp), Coordination::ResponsePtr *>);
                tasks.push_back(Task {
                    .request_idx = request_idx, .session_id = session_id, .request = &subrequest,
                    .response = static_cast<void*>(resp), .is_base_response_type = true,
                    .thread_safe = is_request_thread_safe(subrequest->getOpNum())});
            }
            results[request_idx].response = std::move(response);
        }
        else
        {
            auto * resp = &results[request_idx].response;
            static_assert(std::is_same_v<decltype(resp), Coordination::ZooKeeperResponsePtr *>);
            tasks.push_back(Task {
                .request_idx = request_idx, .session_id = session_id, .request = &zk_request,
                .response = static_cast<void*>(resp), .is_base_response_type = false,
                .thread_safe = is_request_thread_safe(op)});
        }
    }

    /// Phase 1: Execute reads.
    /// This is the parallelizable part — each request reads committed storage state
    /// under shared_lock(storage_mutex) and writes to its own slot in `results`.
    const auto run_task = [&](size_t task_idx)
    {
        Task & task = tasks[task_idx];
        const Coordination::ZooKeeperRequestPtr & zk_request = *task.request;
        chassert(zk_request->isReadRequest());
        Coordination::ZooKeeperResponsePtr response;

        const auto maybe_log_opentelemetry_span = [&](OpenTelemetry::SpanStatus status, const std::string & error_message)
        {
            zk_request->spans.maybeInitialize(
                KeeperSpan::ReadProcess,
                zk_request->tracing_context.get(),
                start_time_us);

            zk_request->spans.maybeFinalize(
                KeeperSpan::ReadProcess,
                [&]
                {
                    return std::vector<OpenTelemetry::SpanAttribute>{
                        {"keeper.operation", Coordination::opNumToString(zk_request->getOpNum())},
                        {"keeper.xid", std::to_string(zk_request->xid)},
                    };
                },
                status,
                error_message);
        };

        const auto process_request = [&]<std::derived_from<Coordination::ZooKeeperRequest> T>(T & concrete_zk_request)
        {
            response = processLocal(concrete_zk_request, *this, task.session_id, check_acl);
        };

        try
        {
            callOnConcreteRequestType(*zk_request, process_request);
        }
        catch (...)
        {
            maybe_log_opentelemetry_span(OpenTelemetry::SpanStatus::ERROR, getCurrentExceptionMessage(true));
            throw;
        }

        maybe_log_opentelemetry_span(OpenTelemetry::SpanStatus::OK, "");

        response->xid = zk_request->xid;
        response->zxid = current_zxid;
        task.setResponse(std::move(response));
    };

    {
        std::shared_lock lock(storage_mutex);
        read_thread_pool.execute(tasks.size(), keeper_context->getCoordinationSettings(),
            [&](size_t begin, size_t end)
            {
                for (size_t task_idx = begin; task_idx < end; ++task_idx)
                {
                    if (tasks[task_idx].thread_safe)
                        run_task(task_idx);
                }
            });
    }

    /// Phase 2 (sequential): session expiry, watch registration, processWatches.
    /// These mutate shared state (watches, sessions_and_watchers, total_watches_count)
    /// and must run single-threaded.
    std::vector<std::pair</*request_idx*/ size_t, KeeperResponsesForSessions>> additional_responses;
    for (size_t task_idx = 0; task_idx < tasks.size(); ++task_idx)
    {
        Task & task = tasks[task_idx];
        const Coordination::ZooKeeperRequestPtr & zk_request = *task.request;

        /// If we didn't process the request above, process it now.
        if (!task.thread_safe)
        {
            std::shared_lock lock(storage_mutex);
            run_task(task_idx);
        }

        updateWatches(zk_request, task.getResponse(), task.session_id);

        /// SetWatches/SetWatches2 have specialized processWatches that call storage.setWatches.
        /// Other read requests return {} from the default processWatches template.
        const auto op = zk_request->getOpNum();
        if (op == Coordination::OpNum::SetWatch || op == Coordination::OpNum::SetWatch2)
        {
            const auto run_process_watches = [&]<std::derived_from<Coordination::ZooKeeperRequest> T>(T & concrete_zk_request)
            {
                auto [watch_responses, removed_count] = processWatches(concrete_zk_request, deltas_range, *this, task.session_id);
                total_watches_count -= removed_count;
                if (!watch_responses.empty())
                    additional_responses.emplace_back(task.request_idx, std::move(watch_responses));
            };
            callOnConcreteRequestType(*zk_request, run_process_watches);
        }
    }

    if (!additional_responses.empty())
    {
        /// Rare case where responses are not 1:1 matched to requests, because of SetWatches/SetWatches2.
        /// We have to insert some responses into the middle of the list.
        KeeperResponsesForSessions merged;
        size_t j = 0;
        for (size_t request_idx = 0; request_idx < requests.size(); ++request_idx)
        {
            merged.push_back(std::move(results[request_idx]));
            while (j < additional_responses.size() && additional_responses[j].first == request_idx)
            {
                auto & to_insert = additional_responses[j].second;
                merged.insert(merged.end(), std::move_iterator(to_insert.begin()), std::move_iterator(to_insert.end()));
                ++j;
            }
        }
        chassert(j == additional_responses.size());
        results = std::move(merged);
    }

    return results;
}

template <typename NS>
void KeeperStorageImpl<NS>::prepareRemoveEphemeralNodes(const std::unordered_set<std::string> & paths, int64_t session_id)
{
    struct ParentUpdate
    {
        UncommittedNodeRef node;
        KeeperNodeStats new_stats;
    };

    std::unordered_map<
        std::string,
        ParentUpdate,
        StringHashForHeterogeneousLookup,
        StringHashForHeterogeneousLookup::transparent_key_equal>
        parent_updates;

    for (const auto & ephemeral_path : paths)
    {
        auto node = nodes.getUncommittedNode(ephemeral_path);
        const auto * node_ptr = node.get();

        /// maybe the node is deleted or recreated with different session_id in the uncommitted state
        if (!node_ptr || node_ptr->stats.getEphemeralOwner() != session_id)
            continue;

        auto parent_node_path = Coordination::parentNodePath(ephemeral_path);

        auto parent_update_it = parent_updates.find(parent_node_path);
        if (parent_update_it == parent_updates.end())
        {
            auto parent_node = nodes.getUncommittedNode(parent_node_path);
            std::tie(parent_update_it, std::ignore) = parent_updates.emplace(parent_node_path, ParentUpdate{parent_node, parent_node.get()->stats});
        }

        auto & parent_update = parent_update_it->second;
        ++parent_update.new_stats.cversion;
        parent_update.new_stats.decreaseNumChildren();

        nodes.prepareRemoveNodeWithoutUpdatingParent(ephemeral_path, std::move(node), staging);
    }

    /// Note: we rely on the fact that ephemeral nodes can't have children. Otherwise we'd need to
    ///       check to make sure we're not updating a node we already removed, and that we're not
    ///       removing a node that still has children.
    for (auto & [path, update] : parent_updates)
        nodes.prepareUpdateNodeStat(path, std::move(update.node), update.new_stats, staging);
}

template <typename NS>
KeeperStorageImpl<NS>::KeeperStorageImpl(int64_t tick_time_ms, const String & superdigest_, const KeeperContextPtr & keeper_context_)
    : KeeperStorage(tick_time_ms, superdigest_, keeper_context_), nodes(keeper_context_, &storage_mutex)
{
    nodes_storage = &nodes;
}

template <typename NS>
KeeperStorageImpl<NS>::~KeeperStorageImpl()
{
    nodes_storage = nullptr; // make sure base dtor doesn't access destroyed `nodes`
}

template class KeeperStorageImpl<KeeperMemNodesStorage>;

}
