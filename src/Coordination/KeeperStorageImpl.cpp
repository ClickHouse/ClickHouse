#include <Coordination/KeeperStorage.h>
#include <Coordination/KeeperStorageImpl.h>

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
        case Coordination::OpNum::CreateIfNotExists:
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

void handleSystemNodeModification(const KeeperContext & keeper_context, std::string_view error_msg)
{
    if (keeper_context.getServerState() == KeeperContext::Phase::INIT && !keeper_context.ignoreSystemPathOnStartup())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "{}. Ignoring it can lead to data loss. "
            "If you still want to ignore it, you can set 'keeper_server.ignore_system_path_on_startup' to true.",
            error_msg);

    LOG_ERROR(getLogger("KeeperStorage"), fmt::runtime(error_msg));
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

/// Applies changes to UncommittedState and staging_{deltas,digest}.
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
    auto parent_node_ref = storage.getUncommittedNode(parent_path);
    const auto * parent_node = parent_node_ref.get();
    if (parent_node == nullptr)
        return Coordination::Error::ZNONODE;

    if (check_acl && !storage.checkACL(parent_node->stats.acl_id, Coordination::ACL::Create, session_id, /*committed=*/ false))
        return Coordination::Error::ZNOAUTH;

    if (parent_node->stats.isEphemeral())
        return Coordination::Error::ZNOCHILDRENFOREPHEMERALS;

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
        auto error_msg = fmt::format("Trying to create a node inside the internal Keeper path ({}) which is not allowed. Path: {}", keeper_system_path, path_created);

        handleSystemNodeModification(keeper_context, error_msg);
        return Coordination::Error::ZBADARGUMENTS;
    }

    auto child_node_ref = storage.getUncommittedNode(path_created);
    if (child_node_ref.get() != nullptr)
    {
        if (zk_request.getOpNum() == Coordination::OpNum::CreateIfNotExists)
            return Coordination::Error::ZOK;

        return Coordination::Error::ZNODEEXISTS;
    }

    if (Coordination::getBaseNodeName(path_created).empty())
        return Coordination::Error::ZBADARGUMENTS;

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

    new_parent_stats.pzxid = std::max(storage.staging_zxid, new_parent_stats.pzxid);

    new_parent_stats.increaseNumChildren();

    Coordination::Stat stat;
    stat.czxid = storage.staging_zxid;
    stat.mzxid = storage.staging_zxid;
    stat.pzxid = storage.staging_zxid;
    stat.ctime = time;
    stat.mtime = time;
    stat.numChildren = 0;
    stat.version = 0;
    stat.aversion = 0;
    stat.cversion = 0;
    stat.ephemeralOwner = zk_request.is_ephemeral ? session_id : 0;

    ACLId acl_id = storage.acl_map.convertACLs(node_acls);

    storage.prepareCreateNode(
        parent_path, std::move(parent_node_ref), new_parent_stats,
        path_created, std::move(child_node_ref), stat, acl_id, zk_request.data);

    return Coordination::Error::ZOK;
}

static Coordination::ZooKeeperResponsePtr process(const Coordination::ZooKeeperCreateRequest & zk_request, KeeperStorage & storage, KeeperStorage::DeltaRange deltas, int64_t /*session_id*/)
{
    ProfileEvents::increment(ProfileEvents::KeeperCreateRequest);

    std::shared_ptr<Coordination::ZooKeeperCreateResponse> response;

    if (zk_request.include_stats)
    {
        auto create2response = std::make_shared<Coordination::ZooKeeperCreate2Response>();
        response = create2response;
    }
    else if (zk_request.not_exists)
        response = std::make_shared<Coordination::ZooKeeperCreateIfNotExistsResponse>();
    else
        response = std::make_shared<Coordination::ZooKeeperCreateResponse>();

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
        { return std::holds_alternative<KeeperStorage::CreateNodeDelta>(delta.operation); });

    if (create_delta_it != deltas.end())
    {
        created_path = create_delta_it->path;
        if (response->getOpNum() == Coordination::OpNum::Create2)
            static_cast<Coordination::ZooKeeperCreate2Response &>(*response).zstat = std::get<KeeperStorage::CreateNodeDelta>(create_delta_it->operation).stat;
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

    auto & container = storage.container;
    auto node_it = container.find(zk_request.path);
    if (node_it == container.end())
    {
        response->error = Coordination::Error::ZNONODE;
    }
    else
    {
        if (check_acl && !storage.checkACL(node_it->value.stats.acl_id, Coordination::ACL::Read, session_id, /*committed=*/ true))
        {
            response->error = Coordination::Error::ZNOAUTH;
            return response;
        }

        node_it->value.stats.setResponseStat(response->stat);
        auto data = node_it->value.getData();
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
    KeeperStorage::DeltaRange /*deltas*/,
    KeeperStorage & storage,
    int64_t /*session_id*/)
{
    return storage.processWatchesImpl(zk_request.getPath(), Coordination::Event::DELETED);
}

template <typename Storage>
static Coordination::Error preprocess(
    const Coordination::ZooKeeperRemoveRequest & zk_request,
    Storage & storage,
    int64_t session_id,
    bool check_acl,
    int64_t /* time */,
    const KeeperContext & keeper_context)
{
    if (Coordination::matchPath(zk_request.path, keeper_system_path) != Coordination::PathMatchResult::NOT_MATCH)
    {
        auto error_msg = fmt::format("Trying to delete an internal Keeper path ({}) which is not allowed", zk_request.path);

        handleSystemNodeModification(keeper_context, error_msg);
        return Coordination::Error::ZBADARGUMENTS;
    }

    auto parent_path = Coordination::parentNodePath(zk_request.path);
    auto parent_node_ref = storage.getUncommittedNode(parent_path);
    const auto * parent_node = parent_node_ref.get();

    if (!parent_node)
        return zk_request.try_remove ? Coordination::Error::ZOK : Coordination::Error::ZNONODE;

    if (check_acl && !storage.checkACL(parent_node->stats.acl_id, Coordination::ACL::Delete, session_id, /*committed=*/ false))
        return Coordination::Error::ZNOAUTH;

    KeeperNodeStats new_parent_stats = parent_node->stats;

    if (zk_request.restored_from_zookeeper_log && new_parent_stats.pzxid < storage.staging_zxid)
        new_parent_stats.pzxid = storage.staging_zxid;

    auto node_ref = storage.getUncommittedNode(zk_request.path);
    const auto * node = node_ref.get();

    if (!node)
    {
        if (zk_request.try_remove)
            return Coordination::Error::ZOK;

        if (zk_request.restored_from_zookeeper_log)
        {
            storage.prepareUpdateNodeStat(parent_path, std::move(parent_node_ref), new_parent_stats);
            return Coordination::Error::ZOK;
        }

        return Coordination::Error::ZNONODE;
    }

    if (check_acl &&
        storage.keeper_context->getCoordinationSettings()[CoordinationSetting::check_node_acl_on_remove] &&
        !storage.checkACL(node->stats.acl_id, Coordination::ACL::Delete, session_id, /*committed=*/ false))
        return Coordination::Error::ZNOAUTH;

    if (zk_request.version != -1 && zk_request.version != node->stats.version)
    {
        if (zk_request.try_remove)
            return {};

        return Coordination::Error::ZBADVERSION;
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

    storage.prepareRemoveNode(
        parent_path, std::move(parent_node_ref), new_parent_stats, zk_request.path, std::move(node_ref));

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
        if (std::holds_alternative<KeeperStorage::RemoveNodeDelta>(delta.operation))
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
    const KeeperContext & keeper_context)
{
    if (Coordination::matchPath(zk_request.path, keeper_system_path) != Coordination::PathMatchResult::NOT_MATCH)
    {
        auto error_msg = fmt::format("Trying to delete an internal Keeper path ({}) which is not allowed", zk_request.path);

        handleSystemNodeModification(keeper_context, error_msg);
        return Coordination::Error::ZBADARGUMENTS;
    }

    auto parent_path = Coordination::parentNodePath(zk_request.path);
    auto parent_node_ref = storage.getUncommittedNode(parent_path);
    const auto * parent_node = parent_node_ref.get();

    if (!parent_node)
        return Coordination::Error::ZOK;

    if (check_acl && !storage.checkACL(parent_node->stats.acl_id, Coordination::ACL::Delete, session_id, /*committed=*/ false))
        return Coordination::Error::ZNOAUTH;

    KeeperNodeStats new_parent_stats = parent_node->stats;

    Coordination::Error error = Coordination::Error::ZNOTEMPTY;
    std::deque<std::pair<std::string, KeeperStorageImpl::UncommittedNodeRef>> nodes_to_remove;
    bool visited_all = storage.visitUncommittedRecursive(zk_request.path, zk_request.remove_nodes_limit,
            [&](std::string_view path, KeeperStorageImpl::UncommittedNodeRef && uncommitted_ref)
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

    storage.prepareRemoveRecursive(parent_path, std::move(parent_node_ref), new_parent_stats, std::move(nodes_to_remove));

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

    auto & container = storage.container;

    auto node_it = container.find(zk_request.path);
    if (node_it == container.end())
    {
        response->error = Coordination::Error::ZNONODE;
        return response;
    }

    if (check_acl && !storage.checkACL(node_it->value.stats.acl_id, Coordination::ACL::Read, session_id, /*committed=*/ true))
    {
        response->error = Coordination::Error::ZNOAUTH;
        return response;
    }

    response->children.push_back(std::string{zk_request.path});
    for (size_t frontier = 0; frontier < response->children.size(); ++frontier)
    {
        const std::string & current_path = response->children[frontier];
        std::filesystem::path current_path_fs(current_path);

        auto it = container.find(current_path);
        if (it == container.end())
            continue;
        for (const auto & child_name : it->value.getChildren())
        {
            auto child_path = (current_path_fs / child_name).generic_string();
            auto child_it = container.find(child_path);
            chassert(child_it != container.end());
            if (!check_acl || storage.checkACL(child_it->value.stats.acl_id, Coordination::ACL::Read, session_id, /*committed=*/ true))
            {
                if (response->children.size() - 1 >= zk_request.children_nodes_limit)
                {
                    response->error = Coordination::Error::ZOK;
                    response->children.erase(response->children.begin());
                    return response;
                }
                response->children.push_back(std::move(child_path));
            }
        }
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

    auto & container = storage.container;
    auto node_it = container.find(zk_request.path);
    if (node_it == container.end())
    {
        response->error = Coordination::Error::ZNONODE;
    }
    else
    {
        node_it->value.stats.setResponseStat(response->stat);
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
    const KeeperContext & keeper_context)
{
    if (zk_request.path == "/" || Coordination::matchPath(zk_request.path, keeper_system_path) != Coordination::PathMatchResult::NOT_MATCH)
    {
        auto error_msg = fmt::format("Trying to update an internal Keeper path ({}) which is not allowed", zk_request.path);

        handleSystemNodeModification(keeper_context, error_msg);
        return Coordination::Error::ZBADARGUMENTS;
    }

    auto node_ref = storage.getUncommittedNode(zk_request.path);
    const auto * node = node_ref.get();

    if (!node)
        return Coordination::Error::ZNONODE;

    if (check_acl && !storage.checkACL(node->stats.acl_id, Coordination::ACL::Write, session_id, /*committed=*/ false))
        return Coordination::Error::ZNOAUTH;

    if (zk_request.version != -1 && zk_request.version != node->stats.version)
        return Coordination::Error::ZBADVERSION;

    KeeperNodeStats new_stats = node->stats;
    new_stats.version++;
    new_stats.mzxid = storage.staging_zxid;
    new_stats.mtime = time;
    new_stats.data_size = static_cast<uint32_t>(zk_request.data.size());
    storage.prepareUpdateNodeData(zk_request.path, std::move(node_ref), new_stats, zk_request.data);

    auto parent_path = Coordination::parentNodePath(zk_request.path);
    auto parent_node_ref = storage.getUncommittedNode(parent_path);
    const auto * parent_node = parent_node_ref.get();
    KeeperNodeStats new_parent_stats = parent_node->stats;
    ++new_parent_stats.cversion;
    storage.prepareUpdateNodeStat(parent_path, std::move(parent_node_ref), new_parent_stats);

    return Coordination::Error::ZOK;
}

static Coordination::ZooKeeperResponsePtr process(const Coordination::ZooKeeperSetRequest & /*zk_request*/, KeeperStorage & storage, KeeperStorage::DeltaRange deltas, int64_t /*session_id*/)
{
    ProfileEvents::increment(ProfileEvents::KeeperSetRequest);

    auto response = std::make_shared<Coordination::ZooKeeperSetResponse>();

    bool found_delta = false;
    for (auto it = deltas.end(); it != deltas.begin();)
    {
        --it;
        if (const auto * update_delta = std::get_if<KeeperStorage::UpdateNodeStatDelta>(&it->operation))
        {
            found_delta = true;
            update_delta->new_stats.setResponseStat(response->stat);
            break;
        }
    }
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
    std::shared_ptr<Coordination::ZooKeeperListResponse> response;
    if (zk_request.getOpNum() == Coordination::OpNum::FilteredListWithStatsAndData)
        response = std::make_shared<Coordination::ZooKeeperFilteredListWithStatsAndDataResponse>();
    else if (zk_request.getOpNum() == Coordination::OpNum::SimpleList)
        response = std::make_shared<Coordination::ZooKeeperSimpleListResponse>();
    else
        response = std::make_shared<Coordination::ZooKeeperListResponse>();

    auto & container = storage.container;

    auto node_it = container.find(zk_request.path);
    if (node_it == container.end())
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

    if (check_acl && !storage.checkACL(node_it->value.stats.acl_id, Coordination::ACL::Read, session_id, /*committed=*/ true))
        return auth_error();

    auto list_request_type = Coordination::ListRequestType::ALL;
    bool with_stat = false;
    bool with_data = false;

    if (const auto * filtered_list = dynamic_cast<const Coordination::ZooKeeperFilteredListRequest *>(&zk_request))
    {
        list_request_type = filtered_list->list_request_type;

        // Check if it's the extended version with stats/data support
        if (const auto * with_stats = dynamic_cast<const Coordination::ZooKeeperFilteredListWithStatsAndDataRequest *>(filtered_list))
        {
            with_stat = with_stats->with_stat;
            with_data = with_stats->with_data;
        }
    }

    bool is_system_node = zk_request.path.starts_with(keeper_system_path);

    if (!is_system_node && node_it->value.stats.getNumChildren() == 0)
    {
        /// (Save the trouble of looking for children if we know there are none.)
    }
    else if (!with_stat && !with_data && Coordination::ListRequestType::ALL == list_request_type)
    {
        /// Only need children names, no stats or data.
        /// We don't check children ACLs here because existence of znodes is not secret in zookeeper.
        const auto & children = node_it->value.getChildren();
        response->names.reserve(children.size());
        for (const auto & child : children)
            response->names.emplace_back(child);
    }
    else
    {
        const auto & children = node_it->value.getChildren();
        response->names.reserve(children.size());
        if (with_stat)
            response->stats.reserve(children.size());
        if (with_data)
            response->data.reserve(children.size());

        bool auth_failed = false;
        for (const auto & child : children)
        {
            using enum Coordination::ListRequestType;

            auto child_path = (std::filesystem::path(zk_request.path) / child).generic_string();
            auto child_it = container.find(child_path);
            if (child_it == container.end())
                onStorageInconsistency("Failed to find a child");
            const auto & child_node = child_it->value;

            if (Coordination::ListRequestType::ALL != list_request_type)
            {
                bool is_ephemeral = child_node.stats.isEphemeral();
                if ((is_ephemeral && list_request_type == PERSISTENT_ONLY) ||
                    (!is_ephemeral && list_request_type == EPHEMERAL_ONLY))
                    continue;
            }

            /// Check child's ACLs because we're going to report its data or stats - this
            /// information should not be accessible without Read permission.
            /// (Even if only list_request_type filtering is enabled, we'll expose the
            ///  is_ephemeral flag, which is also secret.)
            if (check_acl && !storage.checkACL(child_node.stats.acl_id, Coordination::ACL::Read, session_id, /*committed=*/ true))
            {
                auth_failed = true;
                break;
            }

            response->names.emplace_back(child);

            if (with_stat)
            {
                Coordination::Stat child_stat;
                child_node.stats.setResponseStat(child_stat);
                response->stats.emplace_back(child_stat);
            }
            if (with_data)
                response->data.emplace_back(child_node.getData());
        }
        if (auth_failed)
            return auth_error();
    }

#ifdef DEBUG_OR_SANITIZER_BUILD
    if (!is_system_node &&
        Coordination::ListRequestType::ALL == list_request_type &&
        static_cast<size_t>(node_it->value.stats.getNumChildren()) != response->names.size())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Difference between numChildren ({}) and actual children size ({}) for '{}'",
            node_it->value.stats.getNumChildren(),
            response->names.size(),
            zk_request.path);
    }
#endif

    node_it->value.stats.setResponseStat(response->stat);
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

template <typename Node>
bool checkNodeStat(const Node & node, const Coordination::Stat & validator)
{
    const auto & verifiable = node.stats;
    if (validator.czxid != -1 && validator.czxid != verifiable.czxid)
        return false;
    else if (validator.mzxid != -1 && validator.mzxid != verifiable.mzxid)
        return false;
    else if (validator.ctime != -1 && validator.ctime != verifiable.ctime)
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
    else if (validator.numChildren != -1 && validator.numChildren != node.stats.getNumChildren())
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

    auto node_ref = storage.getUncommittedNode(zk_request.path);
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

        if (zk_request.getOpNum() == Coordination::OpNum::CheckStat && !checkNodeStat(*node, zk_request.stat_to_check.value()))
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

    auto & container = storage.container;
    auto node_it = container.find(zk_request.path);

    if constexpr (local)
    {
        if (check_acl && node_it != container.end() &&
            !storage.checkACL(node_it->value.stats.acl_id, Coordination::ACL::Read, session_id, /*committed=*/ true))
        {
            response->error = Coordination::Error::ZNOAUTH;
            return response;
        }
    }

    if (zk_request.getOpNum() == Coordination::OpNum::CheckNotExists)
    {
        if (node_it != container.end() && (zk_request.version == -1 || zk_request.version == node_it->value.stats.version))
            on_error(Coordination::Error::ZNODEEXISTS);
        else
            response->error = Coordination::Error::ZOK;
    }
    else
    {
        if (node_it == container.end())
            on_error(Coordination::Error::ZNONODE);
        else if (zk_request.version != -1 && zk_request.version != node_it->value.stats.version)
            on_error(Coordination::Error::ZBADVERSION);
        else if (zk_request.getOpNum() == Coordination::OpNum::CheckStat && !checkNodeStat(node_it->value, zk_request.stat_to_check.value()))
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
        auto error_msg = fmt::format("Trying to update an internal Keeper path ({}) which is not allowed", zk_request.path);

        handleSystemNodeModification(keeper_context, error_msg);
        return Coordination::Error::ZBADARGUMENTS;
    }

    auto & uncommitted_state = storage.uncommitted_state;
    auto node_ref = storage.getUncommittedNode(zk_request.path);
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
    storage.prepareUpdateNodeStat(zk_request.path, std::move(node_ref), new_stats);

    return Coordination::Error::ZOK;
}

static Coordination::ZooKeeperResponsePtr
process(const Coordination::ZooKeeperSetACLRequest & /*zk_request*/, KeeperStorage & storage, KeeperStorage::DeltaRange deltas, int64_t /*session_id*/)
{
    auto response = std::make_shared<Coordination::ZooKeeperSetACLResponse>();

    bool found_delta = false;
    for (auto it = deltas.end(); it != deltas.begin();)
    {
        --it;
        if (const auto * update_delta = std::get_if<KeeperStorage::UpdateNodeStatDelta>(&it->operation))
        {
            found_delta = true;
            update_delta->new_stats.setResponseStat(response->stat);
            break;
        }
    }
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

    auto & container = storage.container;
    auto node_it = container.find(zk_request.path);
    if (node_it == container.end())
    {
        response->error = Coordination::Error::ZNONODE;
    }
    else
    {
        if (check_acl && !storage.checkACL(node_it->value.stats.acl_id, Coordination::ACL::Admin | Coordination::ACL::Read, session_id, /*committed=*/ true))
        {
            response->error = Coordination::Error::ZNOAUTH;
            return response;
        }

        node_it->value.stats.setResponseStat(response->stat);
        response->acl = storage.acl_map.convertNumber(node_it->value.stats.acl_id);
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
            storage.staging_deltas.emplace_back(storage.staging_zxid, KeeperStorage::FailedMultiDelta{ .failed_pos = i, .failed_pos_error = error });
            return error;
        }

        storage.staging_deltas.emplace_back(storage.staging_zxid, KeeperStorage::SubDeltaEnd{});
    }

    return Coordination::Error::ZOK;
}

static KeeperStorage::DeltaRange extractSubdeltas(KeeperStorage::DeltaRange & deltas)
{
    auto it = deltas.begin();

    for (; it != deltas.end(); ++it)
    {
        if (std::holds_alternative<KeeperStorage::SubDeltaEnd>(it->operation))
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
    if (const auto * failed_multi = std::get_if<KeeperStorage::FailedMultiDelta>(&deltas.front().operation))
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

    if (deltas.empty() || std::get_if<KeeperStorage::FailedMultiDelta>(&deltas.front().operation))
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

KeeperMemNode & KeeperMemNode::operator=(const KeeperMemNode & other)
{
    if (this == &other)
        return *this;

    stats = other.stats;

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

UInt64 KeeperMemNode::getDigest(const std::string_view path) const
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

KeeperStorageStats KeeperStorageImpl::getStorageStats() const
{
    std::shared_lock lock(storage_mutex);
    return KeeperStorageStats
    {
        .nodes_count = container.size(),
        .approximate_data_size = container.getApproximateDataSize(),
        .total_watches_count = getTotalWatchesCount(),
        .watched_paths_count = watches.size() + list_watches.size() + persistent_watches.size() + persistent_list_watches.size()
        + persistent_recursive_watches.size(),
        .sessions_with_watches_count = sessions_and_watchers.size(),
        .session_with_ephemeral_nodes_count = committed_ephemerals.size(),
        .total_emphemeral_nodes_count = committed_ephemeral_nodes,
        .last_committed_zxid = getZXID(),
    };
}

void KeeperStorageImpl::commitDelta(const Delta & delta, uint64_t * digest)
{
    std::visit(
        [&, &path = delta.path]<typename DeltaType>(const DeltaType & operation) -> Coordination::Error
        {
            if constexpr (std::same_as<DeltaType, CreateNodeDelta>)
            {
                if (!createNode(path, operation.data, operation.stat, operation.acl_id, digest))
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

void KeeperStorageImpl::cleanupUncommittedState(int64_t commit_zxid)
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

void KeeperStorageImpl::rollbackUncommittedDelta(const Delta & delta)
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

void KeeperStorageImpl::cleanupAfterRollback(std::vector<uint64_t> rollbacked_zxids)
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

uint64_t KeeperStorageImpl::updateNodesDigest(uint64_t current_digest, uint64_t for_zxid) const
{
    if (!keeper_context->digestEnabled())
        return current_digest;

    auto nodes_it = uncommitted_zxid_to_nodes.find(for_zxid);
    if (nodes_it == uncommitted_zxid_to_nodes.end())
        return current_digest;

    for (const auto node_it : nodes_it->second)
    {
        const auto & [path, uncommitted_node] = *node_it;
        if (uncommitted_node.node)
        {
            uncommitted_node.node->invalidateDigestCache();
            current_digest += uncommitted_node.node->getDigest(path);
        }
    }

    return current_digest;
}

bool KeeperStorageImpl::createNode(
    const std::string & path, String data, const Coordination::Stat & stat, ACLId acl_id, uint64_t * digest)
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

bool KeeperStorageImpl::removeNode(const std::string & path, int32_t version, uint64_t * digest)
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

bool KeeperStorageImpl::getCommittedNodeSlow(std::string_view path, KeeperNodeStats * out_stats, std::string * out_data)
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

std::unique_ptr<KeeperNodeStreamForSnapshot> KeeperStorageImpl::beginWritingSnapshot()
{
    auto res = std::make_unique<NodeStreamForSnapshot>();
    auto [size, ver] = container.snapshotSizeWithVersion();
    container.enableSnapshotMode(ver);
    res->node_count = size;
    res->it = container.begin();
    return res;
}

void KeeperStorageImpl::finishWritingSnapshot(std::unique_ptr<KeeperNodeStreamForSnapshot> stream)
{
    stream->node_count = 0;
    container.disableSnapshotMode();
    container.clearOutdatedNodes();
}

bool KeeperStorageImpl::NodeStreamForSnapshot::next(std::string_view & out_path, std::string_view & out_data, KeeperNodeStats & out_stats)
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

bool KeeperStorageImpl::addSystemNodeIfNotExists(std::string_view path, const KeeperNodeStats & stats, std::string_view data, bool update_parent_num_children, uint64_t * out_digest)
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
    auto child_path = Coordination::getBaseNodeName(map_key->getKey());

    if (path != "/")
    {
        auto parent_path = Coordination::parentNodePath(path);
        container.updateValue(
            parent_path,
            [&](Node & parent_node)
            {
                parent_node.addChild(child_path);

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

void KeeperStorageImpl::loadNodesFromSnapshot(KeeperSnapshotReader & reader, uint64_t * out_digest)
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
        streams[0]->readNodeDataAndStats(node.data.get(), data_size, node.stats);

        switch (streams[0]->checkIfSystemNode(path, node.stats))
        {
            case KeeperSnapshotReader::Stream::WhatToDoWithNode::ProcessNormally: break;
            case KeeperSnapshotReader::Stream::WhatToDoWithNode::Skip: continue;
            case KeeperSnapshotReader::Stream::WhatToDoWithNode::Clear: node = Node{}; break;
        }

        auto ephemeral_owner = node.stats.getEphemeralOwner();
        if (!node.stats.isEphemeral() && node.stats.getNumChildren() > 0)
            node.getChildren().reserve(node.stats.getNumChildren());

        if (ephemeral_owner != 0)
        {
            committed_ephemerals[node.stats.getEphemeralOwner()].insert(std::string{path});
            ++committed_ephemeral_nodes;
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

KeeperDigest KeeperStorageImpl::preprocessRequest(
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

    if (!staging_deltas.empty() || staging_zxid != -1)
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

        staging_zxid = new_last_zxid;
        staging_digest = current_digest;
        KeeperDigestVersion version = keeper_context->digestEnabled() ? KEEPER_CURRENT_DIGEST_VERSION : KeeperDigestVersion::NO_DIGEST;
        chassert(staging_digest.version == version);

        transaction = &uncommitted_transactions.emplace_back(TransactionInfo{.zxid = new_last_zxid, .nodes_digest = current_digest, .log_idx = log_idx});
    }

    bool request_finalized = false;
    const auto finalize = [&](bool rolled_back)
    {
        if (request_finalized)
            return;
        uncommitted_state.addDeltas(std::move(staging_deltas));
        staging_deltas.clear();

        if (zk_request->getOpNum() == Coordination::OpNum::Create)
        {
            fiu_do_on(FailPoints::keeper_leader_sets_invalid_digest, staging_digest.value = 42);
        }

        if (!rolled_back && staging_digest.version != KeeperDigestVersion::NO_DIGEST)
        {
            staging_digest.value = updateNodesDigest(staging_digest.value, new_last_zxid);
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
            transaction->nodes_digest = staging_digest;
        }

        uncommitted_state.cleanup(getZXID());
        staging_zxid = -1;
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

        staging_deltas.emplace_back(staging_zxid, CloseSessionDelta{session_id});
        uncommitted_state.closed_sessions_to_zxids[session_id].insert(staging_zxid);

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
        if (staging_digest.version != KeeperDigestVersion::NO_DIGEST && thread_local_rng() % 2 == 0)
        {
            const UInt64 first_digest = updateNodesDigest(staging_digest.value, new_last_zxid);
            const size_t first_delta_count = staging_deltas.size();

            uncommitted_state.rollback(std::move(staging_deltas));
            staging_deltas.clear();
            staging_digest = transaction->nodes_digest;

            callOnConcreteRequestType(*zk_request, preprocess_request);
            chassert(error == Coordination::Error::ZOK, "Re-preprocessing after a spurious rollback unexpectedly failed");

            const UInt64 second_digest = updateNodesDigest(staging_digest.value, new_last_zxid);
            chassert(
                first_digest == second_digest && first_delta_count == staging_deltas.size(),
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
        /// `preprocess` to here through staging_deltas.
        std::optional<Delta> custom_error_delta;
        if (!staging_deltas.empty() && std::get_if<FailedMultiDelta>(&staging_deltas.back().operation))
        {
            custom_error_delta = std::move(staging_deltas.back());
            staging_deltas.pop_back();
        }

        uncommitted_state.rollback(std::move(staging_deltas));
        staging_deltas.clear();
        if (custom_error_delta.has_value())
            staging_deltas.push_back(std::move(*custom_error_delta));
        else
            staging_deltas.emplace_back(staging_zxid, error);
        finalize(/*rolled_back=*/ true);
    }
    return transaction->nodes_digest;
}

KeeperResponsesForSessions KeeperStorageImpl::processRequest(
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

    /// ZooKeeper update sessions expirity for each request, not only for heartbeats
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

KeeperResponsesForSessions KeeperStorageImpl::processLocalRequests(
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

void KeeperStorageImpl::recalculateStats()
{
    container.recalculateDataSize();
}

KeeperStorageImpl::UncommittedNodeRef KeeperStorageImpl::getUncommittedNode(std::string_view path)
{
    if (auto node_it = uncommitted_nodes.find(path); node_it != uncommitted_nodes.end())
        return {node_it};

    std::shared_ptr<Node> node;
    {
        std::shared_lock lock(storage_mutex);
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

bool KeeperStorageImpl::visitUncommittedRecursive(std::string_view root_path, size_t limit, std::function<bool(std::string_view /*path*/, UncommittedNodeRef &&)> check_node)
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
            std::shared_lock lock(storage_mutex);

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

/// Must be called before mutating any node in other prepare* functions.
void KeeperStorageImpl::prepareWriteCommon(std::string_view path, UncommittedNodeRef & node)
{
    chassert(node.it.has_value());

    auto node_it = *node.it;
    auto & zxid_nodes = uncommitted_zxid_to_nodes[staging_zxid];
    const bool node_was_not_yet_in_zxid = zxid_nodes.insert(node_it).second;

    /// if it's the first time we see that node in the transaction
    /// we need to subtract it's digest from the point before
    /// we started the transaction
    /// at the end of transaction, we add new node digests in updateNodesDigest
    if (node_was_not_yet_in_zxid && staging_digest.version != KeeperDigestVersion::NO_DIGEST &&
        node_it->second.node)
        staging_digest.value -= node_it->second.node->getDigest(path);

    node_it->second.applied_zxids.push_back(staging_zxid);
}

void KeeperStorageImpl::prepareUpdateNodeStat(std::string_view path, UncommittedNodeRef && node, const KeeperNodeStats & new_stats)
{
    prepareWriteCommon(path, node);

    Node * node_ptr = node.getMut();
    UpdateNodeStatDelta delta(node_ptr->stats);
    delta.new_stats = new_stats;
    staging_deltas.emplace_back(std::string{path}, staging_zxid, std::move(delta));

    node_ptr->invalidateDigestCache();
    node_ptr->stats = new_stats;
}

void KeeperStorageImpl::prepareUpdateNodeData(std::string_view path, UncommittedNodeRef && node, const KeeperNodeStats & new_stats, std::string_view new_data)
{
    prepareWriteCommon(path, node);

    Node * node_ptr = node.getMut();

    /// The data delta must be ordered before the stat delta: at commit time the stat delta
    /// overwrites `stats.data_size` (to the new size), after which `getData` would read the old
    /// buffer with the new size. Committing the data delta first keeps the node consistent.
    staging_deltas.emplace_back(
        std::string{path}, staging_zxid,
        UpdateNodeDataDelta{.old_data = std::string{node_ptr->getData()}, .new_data = std::string{new_data}});

    node_ptr->invalidateDigestCache();
    node_ptr->setData(new_data);

    prepareUpdateNodeStat(path, std::move(node), new_stats);
}

void KeeperStorageImpl::prepareCreateNode(
    std::string_view parent_path, UncommittedNodeRef && parent,
    const KeeperNodeStats & new_parent_stats,
    std::string_view path, UncommittedNodeRef && node, const Coordination::Stat & stat,
    ACLId acl_id, std::string_view data)
{
    /// (prepareCreateNode combines parent node update and new node creation. Currently these
    ///  operations are independent here, and we could equally well remove the parent update from
    ///  here and have the caller call prepare for the parent separately. But other implementations
    ///  would want a different type of update for parent, e.g. "update stats and add child" delta.
    ///  Even currently it would be more efficient to add such delta type; it would reduce the
    ///  number of node lookups when committing Create request from 3 to 2, as currently it looks up
    ///  the parent twice: to update stats and to add child. Same story for Remove.)

    prepareUpdateNodeStat(parent_path, std::move(parent), new_parent_stats);

    if (!node.it.has_value())
        node.it = uncommitted_nodes.emplace(std::string{path}, UncommittedNode{}).first;
    prepareWriteCommon(path, node);

    staging_deltas.emplace_back(
        std::string{path},
        staging_zxid,
        CreateNodeDelta{stat, acl_id, std::string{data}});

    auto node_it = *node.it;
    chassert(!node_it->second.node);
    node_it->second.node = std::make_shared<Node>();
    Node * node_ptr = node_it->second.node.get();
    node_ptr->stats.copyStats(stat);
    node_ptr->stats.acl_id = acl_id;
    node_ptr->setData(data);
}

void KeeperStorageImpl::prepareRemoveNodeWithoutUpdatingParent(
    std::string_view path, UncommittedNodeRef && node)
{
    prepareWriteCommon(path, node);
    const Node * node_ptr = node.get();
    staging_deltas.emplace_back(
        std::string{path}, staging_zxid,
        RemoveNodeDelta{node_ptr->stats, std::string{node_ptr->getData()}});

    (*node.it)->second.node = nullptr;
}

void KeeperStorageImpl::prepareRemoveNode(
    std::string_view parent_path, UncommittedNodeRef && parent,
    const KeeperNodeStats & new_parent_stats,
    std::string_view path, UncommittedNodeRef && node)
{
    prepareUpdateNodeStat(parent_path, std::move(parent), new_parent_stats);
    prepareRemoveNodeWithoutUpdatingParent(path, std::move(node));
}

void KeeperStorageImpl::prepareRemoveRecursive(
    std::string_view parent_path, UncommittedNodeRef && parent,
    const KeeperNodeStats & new_parent_stats,
    std::deque<std::pair<std::string, UncommittedNodeRef>> nodes_to_remove)
{
    prepareUpdateNodeStat(parent_path, std::move(parent), new_parent_stats);

    for (auto & [path, node] : nodes_to_remove)
        prepareRemoveNodeWithoutUpdatingParent(path, std::move(node));
}

void KeeperStorageImpl::prepareRemoveEphemeralNodes(const std::unordered_set<std::string> & paths, int64_t session_id)
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
        auto node = getUncommittedNode(ephemeral_path);
        const auto * node_ptr = node.get();

        /// maybe the node is deleted or recreated with different session_id in the uncommitted state
        if (!node_ptr || node_ptr->stats.getEphemeralOwner() != session_id)
            continue;

        auto parent_node_path = Coordination::parentNodePath(ephemeral_path);

        auto parent_update_it = parent_updates.find(parent_node_path);
        if (parent_update_it == parent_updates.end())
        {
            auto parent_node = getUncommittedNode(parent_node_path);
            std::tie(parent_update_it, std::ignore) = parent_updates.emplace(parent_node_path, ParentUpdate{parent_node, parent_node.get()->stats});
        }

        auto & parent_update = parent_update_it->second;
        ++parent_update.new_stats.cversion;
        parent_update.new_stats.decreaseNumChildren();

        prepareRemoveNodeWithoutUpdatingParent(ephemeral_path, std::move(node));
    }

    /// Note: we rely on the fact that ephemeral nodes can't have children. Otherwise we'd need to
    ///       check to make sure we're not updating a node we already removed, and that we're not
    ///       removing a node that still has children.
    for (auto & [path, update] : parent_updates)
        prepareUpdateNodeStat(path, std::move(update.node), update.new_stats);
}

void KeeperStorageImpl::prepareAddAuth(std::shared_ptr<KeeperStorage::AuthID> new_auth, int64_t session_id)
{
    auto & uncommitted_auth = uncommitted_state.session_and_auth[session_id];
    uncommitted_auth.push_back(std::pair{staging_zxid, new_auth});
    staging_deltas.emplace_back(staging_zxid, AddAuthDelta{session_id, std::move(new_auth)});
}

}
