// NOLINTBEGIN(clang-analyzer-optin.core.EnumCastOutOfRange)

#include <iterator>
#include <variant>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <boost/algorithm/string.hpp>
#include <Poco/SHA1Engine.h>

#include <Common/Base64.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/SipHash.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/StringUtils.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <base/hex.h>
#include <base/scope_guard.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/ProfileEvents.h>

#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperConstants.h>
#include <Coordination/KeeperDispatcher.h>
#include <Coordination/KeeperReconfiguration.h>
#include <Coordination/KeeperStorage.h>

#include <functional>
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
    extern const Event KeeperExistsRequest;
    extern const Event KeeperPreprocessElapsedMicroseconds;
    extern const Event KeeperProcessElapsedMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{
String getSHA1(const String & userdata)
{
    Poco::SHA1Engine engine;
    engine.update(userdata);
    const auto & digest_id = engine.digest();
    return String{digest_id.begin(), digest_id.end()};
}

template<typename UncommittedState>
bool fixupACL(
    const std::vector<Coordination::ACL> & request_acls,
    int64_t session_id,
    const UncommittedState & uncommitted_state,
    std::vector<Coordination::ACL> & result_acls)
{
    if (request_acls.empty())
        return true;

    bool valid_found = false;
    for (const auto & request_acl : request_acls)
    {
        if (request_acl.scheme == "auth")
        {
            uncommitted_state.forEachAuthInSession(
                session_id,
                [&](const KeeperStorageBase::AuthID & auth_id)
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
            /// We don't need to save default ACLs
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

KeeperStorageBase::ResponsesForSessions processWatchesImpl(
    const String & path, KeeperStorageBase::Watches & watches, KeeperStorageBase::Watches & list_watches, Coordination::Event event_type)
{
    KeeperStorageBase::ResponsesForSessions result;
    auto watch_it = watches.find(path);
    if (watch_it != watches.end())
    {
        std::shared_ptr<Coordination::ZooKeeperWatchResponse> watch_response = std::make_shared<Coordination::ZooKeeperWatchResponse>();
        watch_response->path = path;
        watch_response->xid = Coordination::WATCH_XID;
        watch_response->zxid = -1;
        watch_response->type = event_type;
        watch_response->state = Coordination::State::CONNECTED;
        for (auto watcher_session : watch_it->second)
            result.push_back(KeeperStorageBase::ResponseForSession{watcher_session, watch_response});

        watches.erase(watch_it);
    }

    auto parent_path = parentNodePath(path);

    Strings paths_to_check_for_list_watches;
    if (event_type == Coordination::Event::CREATED)
    {
        paths_to_check_for_list_watches.push_back(parent_path.toString()); /// Trigger list watches for parent
    }
    else if (event_type == Coordination::Event::DELETED)
    {
        paths_to_check_for_list_watches.push_back(path); /// Trigger both list watches for this path
        paths_to_check_for_list_watches.push_back(parent_path.toString()); /// And for parent path
    }
    /// CHANGED event never trigger list wathes

    for (const auto & path_to_check : paths_to_check_for_list_watches)
    {
        watch_it = list_watches.find(path_to_check);
        if (watch_it != list_watches.end())
        {
            std::shared_ptr<Coordination::ZooKeeperWatchResponse> watch_list_response
                = std::make_shared<Coordination::ZooKeeperWatchResponse>();
            watch_list_response->path = path_to_check;
            watch_list_response->xid = Coordination::WATCH_XID;
            watch_list_response->zxid = -1;
            if (path_to_check == parent_path)
                watch_list_response->type = Coordination::Event::CHILD;
            else
                watch_list_response->type = Coordination::Event::DELETED;

            watch_list_response->state = Coordination::State::CONNECTED;
            for (auto watcher_session : watch_it->second)
                result.push_back(KeeperStorageBase::ResponseForSession{watcher_session, watch_list_response});

            list_watches.erase(watch_it);
        }
    }
    return result;
}

// When this function is updated, update CURRENT_DIGEST_VERSION!!
template <typename Node>
uint64_t calculateDigest(std::string_view path, const Node & node)
{
    SipHash hash;

    hash.update(path);

    auto data = node.getData();
    if (!data.empty())
    {
        chassert(data.data() != nullptr);
        hash.update(data);
    }

    hash.update(node.czxid);
    hash.update(node.mzxid);
    hash.update(node.ctime());
    hash.update(node.mtime);
    hash.update(node.version);
    hash.update(node.cversion);
    hash.update(node.aversion);
    hash.update(node.ephemeralOwner());
    hash.update(node.numChildren());
    hash.update(node.pzxid);

    auto digest = hash.get64();

    /// 0 means no cached digest
    if (digest == 0)
        return 1;

    return digest;
}

}

void KeeperRocksNodeInfo::copyStats(const Coordination::Stat & stat)
{
    czxid = stat.czxid;
    mzxid = stat.mzxid;
    pzxid = stat.pzxid;

    mtime = stat.mtime;
    setCtime(stat.ctime);

    version = stat.version;
    cversion = stat.cversion;
    aversion = stat.aversion;

    if (stat.ephemeralOwner == 0)
    {
        is_ephemeral_and_ctime.is_ephemeral = false;
        setNumChildren(stat.numChildren);
    }
    else
    {
        setEphemeralOwner(stat.ephemeralOwner);
    }
}

void KeeperRocksNode::invalidateDigestCache() const
{
    if (serialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "We modify node after serialized it");
    digest = 0;
}

UInt64 KeeperRocksNode::getDigest(std::string_view path) const
{
    if (!digest)
        digest = calculateDigest(path, *this);
    return digest;
}

String KeeperRocksNode::getEncodedString()
{
    if (serialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "We modify node after serialized it");
    serialized = true;

    WriteBufferFromOwnString buffer;
    const KeeperRocksNodeInfo & node_info = *this;
    writePODBinary(node_info, buffer);
    writeBinary(getData(), buffer);
    return buffer.str();
}

void KeeperRocksNode::decodeFromString(const String &buffer_str)
{
    ReadBufferFromOwnString buffer(buffer_str);
    KeeperRocksNodeInfo & node_info = *this;
    readPODBinary(node_info, buffer);
    readVarUInt(data_size, buffer);
    if (data_size)
    {
        data = std::unique_ptr<char[]>(new char[data_size]);
        buffer.readStrict(data.get(), data_size);
    }
}

KeeperMemNode & KeeperMemNode::operator=(const KeeperMemNode & other)
{
    if (this == &other)
        return *this;

    czxid = other.czxid;
    mzxid = other.mzxid;
    pzxid = other.pzxid;
    acl_id = other.acl_id;
    mtime = other.mtime;
    is_ephemeral_and_ctime = other.is_ephemeral_and_ctime;
    ephemeral_or_children_data = other.ephemeral_or_children_data;
    data_size = other.data_size;
    version = other.version;
    cversion = other.cversion;
    aversion = other.aversion;

    if (data_size != 0)
    {
        data = std::unique_ptr<char[]>(new char[data_size]);
        memcpy(data.get(), other.data.get(), data_size);
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

    czxid = other.czxid;
    mzxid = other.mzxid;
    pzxid = other.pzxid;
    acl_id = other.acl_id;
    mtime = other.mtime;
    is_ephemeral_and_ctime = other.is_ephemeral_and_ctime;
    ephemeral_or_children_data = other.ephemeral_or_children_data;
    version = other.version;
    cversion = other.cversion;
    aversion = other.aversion;

    data_size = other.data_size;
    data = std::move(other.data);

    other.data_size = 0;

    static_assert(std::is_nothrow_move_assignable_v<ChildrenSet>);
    children = std::move(other.children);

    return *this;
}

KeeperMemNode::KeeperMemNode(KeeperMemNode && other) noexcept
{
    *this = std::move(other);
}

bool KeeperMemNode::empty() const
{
    return data_size == 0 && mzxid == 0;
}

void KeeperMemNode::copyStats(const Coordination::Stat & stat)
{
    czxid = stat.czxid;
    mzxid = stat.mzxid;
    pzxid = stat.pzxid;

    mtime = stat.mtime;
    setCtime(stat.ctime);

    version = stat.version;
    cversion = stat.cversion;
    aversion = stat.aversion;

    if (stat.ephemeralOwner == 0)
    {
        is_ephemeral_and_ctime.is_ephemeral = false;
        setNumChildren(stat.numChildren);
    }
    else
    {
        setEphemeralOwner(stat.ephemeralOwner);
    }
}

void KeeperMemNode::setResponseStat(Coordination::Stat & response_stat) const
{
    response_stat.czxid = czxid;
    response_stat.mzxid = mzxid;
    response_stat.ctime = ctime();
    response_stat.mtime = mtime;
    response_stat.version = version;
    response_stat.cversion = cversion;
    response_stat.aversion = aversion;
    response_stat.ephemeralOwner = ephemeralOwner();
    response_stat.dataLength = static_cast<int32_t>(data_size);
    response_stat.numChildren = numChildren();
    response_stat.pzxid = pzxid;

}

uint64_t KeeperMemNode::sizeInBytes() const
{
    return sizeof(KeeperMemNode) + children.size() * sizeof(StringRef) + data_size;
}

void KeeperMemNode::setData(const String & new_data)
{
    data_size = static_cast<uint32_t>(new_data.size());
    if (data_size != 0)
    {
        data = std::unique_ptr<char[]>(new char[new_data.size()]);
        memcpy(data.get(), new_data.data(), data_size);
    }
}

void KeeperMemNode::addChild(StringRef child_path)
{
    children.insert(child_path);
}

void KeeperMemNode::removeChild(StringRef child_path)
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
        cached_digest = calculateDigest(path, *this);

    return cached_digest;
};

void KeeperMemNode::shallowCopy(const KeeperMemNode & other)
{
    czxid = other.czxid;
    mzxid = other.mzxid;
    pzxid = other.pzxid;
    acl_id = other.acl_id; /// 0 -- no ACL by default

    mtime = other.mtime;

    is_ephemeral_and_ctime = other.is_ephemeral_and_ctime;

    ephemeral_or_children_data = other.ephemeral_or_children_data;

    data_size = other.data_size;
    if (data_size != 0)
    {
        data = std::unique_ptr<char[]>(new char[data_size]);
        memcpy(data.get(), other.data.get(), data_size);
    }

    version = other.version;
    cversion = other.cversion;
    aversion = other.aversion;

    cached_digest = other.cached_digest;
}


template<typename Container>
KeeperStorage<Container>::KeeperStorage(
    int64_t tick_time_ms, const String & superdigest_, const KeeperContextPtr & keeper_context_, const bool initialize_system_nodes)
    : session_expiry_queue(tick_time_ms), keeper_context(keeper_context_), superdigest(superdigest_)
{
    if constexpr (use_rocksdb)
        container.initialize(keeper_context);
    Node root_node;
    container.insert("/", root_node);
    if constexpr (!use_rocksdb)
        addDigest(root_node, "/");

    if (initialize_system_nodes)
        initializeSystemNodes();
}

template<typename Container>
void KeeperStorage<Container>::initializeSystemNodes()
{
    if (initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "KeeperStorage system nodes initialized twice");

    // insert root system path if it isn't already inserted
    if (container.find(keeper_system_path) == container.end())
    {
        Node system_node;
        container.insert(keeper_system_path, system_node);
        // store digest for the empty node because we won't update
        // its stats
        if constexpr (!use_rocksdb)
            addDigest(system_node, keeper_system_path);

        // update root and the digest based on it
        auto current_root_it = container.find("/");
        chassert(current_root_it != container.end());
        if constexpr (!use_rocksdb)
            removeDigest(current_root_it->value, "/");
        auto updated_root_it = container.updateValue(
            "/",
            [](KeeperStorage::Node & node)
            {
                node.increaseNumChildren();
                if constexpr (!use_rocksdb)
                    node.addChild(getBaseNodeName(keeper_system_path));
            }
        );
        if constexpr (!use_rocksdb)
            addDigest(updated_root_it->value, "/");
    }

    // insert child system nodes
    for (const auto & [path, data] : keeper_context->getSystemNodesWithData())
    {
        chassert(path.starts_with(keeper_system_path));
        Node child_system_node;
        child_system_node.setData(data);
        if constexpr (use_rocksdb)
            container.insert(std::string{path}, child_system_node);
        else
        {
            auto [map_key, _] = container.insert(std::string{path}, child_system_node);
            /// Take child path from key owned by map.
            auto child_path = getBaseNodeName(map_key->getKey());
            container.updateValue(
                parentNodePath(StringRef(path)),
                [child_path](auto & parent)
                {
                    // don't update stats so digest is okay
                    parent.addChild(child_path);
                }
            );
        }
    }

    initialized = true;
}

template <class... Ts>
struct Overloaded : Ts...
{
    using Ts::operator()...;
};

// explicit deduction guide
// https://en.cppreference.com/w/cpp/language/class_template_argument_deduction
template <class... Ts>
Overloaded(Ts...) -> Overloaded<Ts...>;

template<typename Container>
std::shared_ptr<typename Container::Node> KeeperStorage<Container>::UncommittedState::tryGetNodeFromStorage(StringRef path) const
{
    if (auto node_it = storage.container.find(path); node_it != storage.container.end())
    {
        const auto & committed_node = node_it->value;
        auto node = std::make_shared<KeeperStorage<Container>::Node>();
        node->shallowCopy(committed_node);
        return node;
    }

    return nullptr;
}

template<typename Container>
void KeeperStorage<Container>::UncommittedState::applyDelta(const Delta & delta)
{
    chassert(!delta.path.empty());
    if (!nodes.contains(delta.path))
    {
        if (auto storage_node = tryGetNodeFromStorage(delta.path))
            nodes.emplace(delta.path, UncommittedNode{.node = std::move(storage_node)});
        else
            nodes.emplace(delta.path, UncommittedNode{.node = nullptr});
    }

    std::visit(
        [&]<typename DeltaType>(const DeltaType & operation)
        {
            auto & [node, acls, last_applied_zxid] = nodes.at(delta.path);

            if constexpr (std::same_as<DeltaType, CreateNodeDelta>)
            {
                chassert(!node);
                node = std::make_shared<Node>();
                node->copyStats(operation.stat);
                node->setData(operation.data);
                acls = operation.acls;
                last_applied_zxid = delta.zxid;
            }
            else if constexpr (std::same_as<DeltaType, RemoveNodeDelta>)
            {
                chassert(node);
                node = nullptr;
                last_applied_zxid = delta.zxid;
            }
            else if constexpr (std::same_as<DeltaType, UpdateNodeDelta>)
            {
                chassert(node);
                node->invalidateDigestCache();
                operation.update_fn(*node);
                last_applied_zxid = delta.zxid;
            }
            else if constexpr (std::same_as<DeltaType, SetACLDelta>)
            {
                acls = operation.acls;
                last_applied_zxid = delta.zxid;
            }
        },
        delta.operation);
}

template<typename Container>
bool KeeperStorage<Container>::UncommittedState::hasACL(int64_t session_id, bool is_local, std::function<bool(const AuthID &)> predicate) const
{
    const auto check_auth = [&](const auto & auth_ids)
    {
        for (const auto & auth : auth_ids)
        {
            using TAuth = std::remove_reference_t<decltype(auth)>;

            const AuthID * auth_ptr = nullptr;
            if constexpr (std::is_pointer_v<TAuth>)
                auth_ptr = auth;
            else
                auth_ptr = &auth;

            if (predicate(*auth_ptr))
                return true;
        }
        return false;
    };

    if (is_local)
        return check_auth(storage.session_and_auth[session_id]);

    /// we want to close the session and with that we will remove all the auth related to the session
    if (closed_sessions.contains(session_id))
        return false;

    if (check_auth(storage.session_and_auth[session_id]))
        return true;

    // check if there are uncommitted
    const auto auth_it = session_and_auth.find(session_id);
    if (auth_it == session_and_auth.end())
        return false;

    return check_auth(auth_it->second);
}

template<typename Container>
void KeeperStorage<Container>::UncommittedState::addDelta(Delta new_delta)
{
    const auto & added_delta = deltas.emplace_back(std::move(new_delta));

    if (!added_delta.path.empty())
    {
        deltas_for_path[added_delta.path].push_back(&added_delta);
        applyDelta(added_delta);
    }
    else if (const auto * auth_delta = std::get_if<AddAuthDelta>(&added_delta.operation))
    {
        auto & uncommitted_auth = session_and_auth[auth_delta->session_id];
        uncommitted_auth.emplace_back(&auth_delta->auth_id);
    }
    else if (const auto * close_session_delta = std::get_if<CloseSessionDelta>(&added_delta.operation))
    {
        closed_sessions.insert(close_session_delta->session_id);
    }
}

template<typename Container>
void KeeperStorage<Container>::UncommittedState::addDeltas(std::vector<Delta> new_deltas)
{
    for (auto & delta : new_deltas)
        addDelta(std::move(delta));
}

template<typename Container>
void KeeperStorage<Container>::UncommittedState::commit(int64_t commit_zxid)
{
    chassert(deltas.empty() || deltas.front().zxid >= commit_zxid);

    // collect nodes that have no further modification in the current transaction
    std::unordered_set<std::string> modified_nodes;

    while (!deltas.empty() && deltas.front().zxid == commit_zxid)
    {
        if (std::holds_alternative<SubDeltaEnd>(deltas.front().operation))
        {
            deltas.pop_front();
            break;
        }

        auto & front_delta = deltas.front();

        if (!front_delta.path.empty())
        {
            auto & path_deltas = deltas_for_path.at(front_delta.path);
            chassert(path_deltas.front() == &front_delta);
            path_deltas.pop_front();
            if (path_deltas.empty())
            {
                deltas_for_path.erase(front_delta.path);

                // no more deltas for path -> no modification
                modified_nodes.insert(std::move(front_delta.path));
            }
            else if (path_deltas.front()->zxid > commit_zxid)
            {
                // next delta has a zxid from a different transaction -> no modification in this transaction
                modified_nodes.insert(std::move(front_delta.path));
            }
        }
        else if (auto * add_auth = std::get_if<AddAuthDelta>(&front_delta.operation))
        {
            auto & uncommitted_auth = session_and_auth[add_auth->session_id];
            chassert(!uncommitted_auth.empty() && uncommitted_auth.front() == &add_auth->auth_id);
            uncommitted_auth.pop_front();
            if (uncommitted_auth.empty())
                session_and_auth.erase(add_auth->session_id);
        }
        else if (auto * close_session = std::get_if<CloseSessionDelta>(&front_delta.operation))
        {
            closed_sessions.erase(close_session->session_id);
        }

        deltas.pop_front();
    }

    // delete all cached nodes that were not modified after the commit_zxid
    // we only need to check the nodes that were modified in this transaction
    for (const auto & node : modified_nodes)
    {
        if (nodes[node].zxid == commit_zxid)
            nodes.erase(node);
    }
}

template<typename Container>
void KeeperStorage<Container>::UncommittedState::rollback(int64_t rollback_zxid)
{
    // we can only rollback the last zxid (if there is any)
    // if there is a delta with a larger zxid, we have invalid state
    if (!deltas.empty() && deltas.back().zxid > rollback_zxid)
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Invalid state of deltas found while trying to rollback request. Last ZXID ({}) is larger than the requested ZXID ({})",
            deltas.back().zxid,
            rollback_zxid);

    auto delta_it = deltas.rbegin();

    // we need to undo ephemeral mapping modifications
    // CreateNodeDelta added ephemeral for session id -> we need to remove it
    // RemoveNodeDelta removed ephemeral for session id -> we need to add it back
    for (; delta_it != deltas.rend(); ++delta_it)
    {
        if (delta_it->zxid < rollback_zxid)
            break;

        chassert(delta_it->zxid == rollback_zxid);
        if (!delta_it->path.empty())
        {
            std::visit(
                [&]<typename DeltaType>(const DeltaType & operation)
                {
                    if constexpr (std::same_as<DeltaType, CreateNodeDelta>)
                    {
                        if (operation.stat.ephemeralOwner != 0)
                            storage.unregisterEphemeralPath(operation.stat.ephemeralOwner, delta_it->path);
                    }
                    else if constexpr (std::same_as<DeltaType, RemoveNodeDelta>)
                    {
                        if (operation.ephemeral_owner != 0)
                            storage.ephemerals[operation.ephemeral_owner].emplace(delta_it->path);
                    }
                },
                delta_it->operation);

            auto & path_deltas = deltas_for_path.at(delta_it->path);
            if (path_deltas.back() == &*delta_it)
            {
                path_deltas.pop_back();
                if (path_deltas.empty())
                    deltas_for_path.erase(delta_it->path);
            }
        }
        else if (auto * add_auth = std::get_if<AddAuthDelta>(&delta_it->operation))
        {
            auto & uncommitted_auth = session_and_auth[add_auth->session_id];
            if (uncommitted_auth.back() == &add_auth->auth_id)
            {
                uncommitted_auth.pop_back();
                if (uncommitted_auth.empty())
                    session_and_auth.erase(add_auth->session_id);
            }
        }
        else if (auto * close_session = std::get_if<CloseSessionDelta>(&delta_it->operation))
        {
           closed_sessions.erase(close_session->session_id);
        }
    }

    if (delta_it == deltas.rend())
        deltas.clear();
    else
        deltas.erase(delta_it.base(), deltas.end());

    absl::flat_hash_set<std::string> deleted_nodes;
    std::erase_if(
        nodes,
        [&, rollback_zxid](const auto & node)
        {
            if (node.second.zxid == rollback_zxid)
            {
                deleted_nodes.emplace(std::move(node.first));
                return true;
            }
            return false;
        });

    // recalculate all the uncommitted deleted nodes
    for (const auto & deleted_node : deleted_nodes)
    {
        auto path_delta_it = deltas_for_path.find(deleted_node);
        if (path_delta_it != deltas_for_path.end())
        {
            for (const auto & delta : path_delta_it->second)
            {
                applyDelta(*delta);
            }
        }
    }
}

template<typename Container>
std::shared_ptr<typename Container::Node> KeeperStorage<Container>::UncommittedState::getNode(StringRef path) const
{
    if (auto node_it = nodes.find(path.toView()); node_it != nodes.end())
        return node_it->second.node;

    return tryGetNodeFromStorage(path);
}

template<typename Container>
const typename Container::Node * KeeperStorage<Container>::UncommittedState::getActualNodeView(StringRef path, const Node & storage_node) const
{
    if (auto node_it = nodes.find(path.toView()); node_it != nodes.end())
        return node_it->second.node.get();

    return &storage_node;
}

template<typename Container>
Coordination::ACLs KeeperStorage<Container>::UncommittedState::getACLs(StringRef path) const
{
    if (auto node_it = nodes.find(path.toView()); node_it != nodes.end())
        return node_it->second.acls;

    auto node_it = storage.container.find(path);
    if (node_it == storage.container.end())
        return {};

    return storage.acl_map.convertNumber(node_it->value.acl_id);
}

template<typename Container>
void KeeperStorage<Container>::UncommittedState::forEachAuthInSession(int64_t session_id, std::function<void(const AuthID &)> func) const
{
    const auto call_for_each_auth = [&func](const auto & auth_ids)
    {
        for (const auto & auth : auth_ids)
        {
            using TAuth = std::remove_reference_t<decltype(auth)>;

            const AuthID * auth_ptr = nullptr;
            if constexpr (std::is_pointer_v<TAuth>)
                auth_ptr = auth;
            else
                auth_ptr = &auth;

            func(*auth_ptr);
        }
    };

    // for committed
    if (storage.session_and_auth.contains(session_id))
        call_for_each_auth(storage.session_and_auth.at(session_id));
    // for uncommitted
    if (session_and_auth.contains(session_id))
        call_for_each_auth(session_and_auth.at(session_id));
}

namespace
{

[[noreturn]] void onStorageInconsistency()
{
    LOG_ERROR(
        getLogger("KeeperStorage"),
        "Inconsistency found between uncommitted and committed data. Keeper will terminate to avoid undefined behaviour.");
    std::terminate();
}

}

template<typename Container>
void KeeperStorage<Container>::applyUncommittedState(KeeperStorage & other, int64_t last_log_idx)
{
    std::unordered_set<int64_t> zxids_to_apply;
    for (const auto & transaction : uncommitted_transactions)
    {
        if (transaction.log_idx == 0)
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Transaction has log idx equal to 0");

        if (transaction.log_idx <= last_log_idx)
            continue;

        other.uncommitted_transactions.push_back(transaction);
        zxids_to_apply.insert(transaction.zxid);
    }

    auto it = uncommitted_state.deltas.begin();

    for (; it != uncommitted_state.deltas.end(); ++it)
    {
        if (!zxids_to_apply.contains(it->zxid))
            continue;

        other.uncommitted_state.addDelta(*it);
    }
}

template<typename Container>
Coordination::Error KeeperStorage<Container>::commit(int64_t commit_zxid)
{
    // Deltas are added with increasing ZXIDs
    // If there are no deltas for the commit_zxid (e.g. read requests), we instantly return
    // on first delta
    for (auto & delta : uncommitted_state.deltas)
    {
        if (delta.zxid > commit_zxid)
            break;

        bool finish_subdelta = false;
        auto result = std::visit(
            [&, &path = delta.path]<typename DeltaType>(DeltaType & operation) -> Coordination::Error
            {
                if constexpr (std::same_as<DeltaType, CreateNodeDelta>)
                {
                    if (!createNode(
                            path,
                            std::move(operation.data),
                            operation.stat,
                            std::move(operation.acls)))
                        onStorageInconsistency();

                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, UpdateNodeDelta>)
                {
                    auto node_it = container.find(path);
                    if (node_it == container.end())
                        onStorageInconsistency();

                    if (operation.version != -1 && operation.version != node_it->value.version)
                        onStorageInconsistency();

                    if constexpr (!use_rocksdb)
                        removeDigest(node_it->value, path);
                    auto updated_node = container.updateValue(path, operation.update_fn);
                    if constexpr (!use_rocksdb)
                        addDigest(updated_node->value, path);

                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, RemoveNodeDelta>)
                {
                    if (!removeNode(path, operation.version))
                        onStorageInconsistency();

                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, SetACLDelta>)
                {
                    auto node_it = container.find(path);
                    if (node_it == container.end())
                        onStorageInconsistency();

                    if (operation.version != -1 && operation.version != node_it->value.aversion)
                        onStorageInconsistency();

                    acl_map.removeUsage(node_it->value.acl_id);

                    uint64_t acl_id = acl_map.convertACLs(operation.acls);
                    acl_map.addUsage(acl_id);

                    container.updateValue(path, [acl_id](Node & node) { node.acl_id = acl_id; });

                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, ErrorDelta>)
                    return operation.error;
                else if constexpr (std::same_as<DeltaType, SubDeltaEnd>)
                {
                    finish_subdelta = true;
                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, AddAuthDelta>)
                {
                    session_and_auth[operation.session_id].emplace_back(std::move(operation.auth_id));
                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, KeeperStorage::CloseSessionDelta>)
                {
                    return Coordination::Error::ZOK;
                }
                else
                {
                    // shouldn't be called in any process functions
                    onStorageInconsistency();
                }
            },
            delta.operation);

        if (result != Coordination::Error::ZOK)
            return result;

        if (finish_subdelta)
            return Coordination::Error::ZOK;
    }

    return Coordination::Error::ZOK;
}

template<typename Container>
bool KeeperStorage<Container>::createNode(
    const std::string & path,
    String data,
    const Coordination::Stat & stat,
    Coordination::ACLs node_acls)
{
    auto parent_path = parentNodePath(path);
    auto node_it = container.find(parent_path);

    if (node_it == container.end())
        return false;

    if (node_it->value.isEphemeral())
        return false;

    if (container.contains(path))
        return false;

    Node created_node;

    uint64_t acl_id = acl_map.convertACLs(node_acls);
    acl_map.addUsage(acl_id);

    created_node.acl_id = acl_id;
    created_node.copyStats(stat);
    created_node.setData(data);
    if constexpr (use_rocksdb)
    {
        container.insert(path, created_node);
    }
    else
    {
        auto [map_key, _] = container.insert(path, created_node);
        /// Take child path from key owned by map.
        auto child_path = getBaseNodeName(map_key->getKey());
        container.updateValue(
                parent_path,
                [child_path](KeeperMemNode & parent)
                {
                    parent.addChild(child_path);
                    chassert(parent.numChildren() == static_cast<int32_t>(parent.getChildren().size()));
                }
        );

        addDigest(map_key->getMapped()->value, map_key->getKey().toView());
    }
    return true;
};

template<typename Container>
bool KeeperStorage<Container>::removeNode(const std::string & path, int32_t version)
{
    auto node_it = container.find(path);
    if (node_it == container.end())
        return false;

    if (version != -1 && version != node_it->value.version)
        return false;

    if (node_it->value.numChildren())
        return false;

    KeeperStorage::Node prev_node;
    prev_node.shallowCopy(node_it->value);
    acl_map.removeUsage(node_it->value.acl_id);

    if constexpr (use_rocksdb)
        container.erase(path);
    else
    {
        container.updateValue(
            parentNodePath(path),
            [child_basename = getBaseNodeName(node_it->key)](KeeperMemNode & parent)
            {
                parent.removeChild(child_basename);
                chassert(parent.numChildren() == static_cast<int32_t>(parent.getChildren().size()));
            }
        );

        container.erase(path);

        removeDigest(prev_node, path);
    }
    return true;
}

template<typename Storage>
struct KeeperStorageRequestProcessor
{
    Coordination::ZooKeeperRequestPtr zk_request;

    explicit KeeperStorageRequestProcessor(const Coordination::ZooKeeperRequestPtr & zk_request_) : zk_request(zk_request_) { }

    virtual Coordination::ZooKeeperResponsePtr process(Storage & storage, int64_t zxid) const = 0;

    virtual std::vector<typename Storage::Delta>
    preprocess(Storage & /*storage*/, int64_t /*zxid*/, int64_t /*session_id*/, int64_t /*time*/, uint64_t & /*digest*/, const KeeperContext & /*keeper_context*/) const
    {
        return {};
    }

    // process the request using locally committed data
    virtual Coordination::ZooKeeperResponsePtr
    processLocal(Storage & /*storage*/, int64_t /*zxid*/) const
    {
        throw Exception{DB::ErrorCodes::LOGICAL_ERROR, "Cannot process the request locally"};
    }

    virtual KeeperStorageBase::ResponsesForSessions
    processWatches(const Storage & /*storage*/, int64_t /*zxid*/, KeeperStorageBase::Watches & /*watches*/, KeeperStorageBase::Watches & /*list_watches*/) const
    {
        return {};
    }

    virtual bool checkAuth(Storage & /*storage*/, int64_t /*session_id*/, bool /*is_local*/) const { return true; }

    virtual ~KeeperStorageRequestProcessor() = default;
};

template<typename Storage>
struct KeeperStorageHeartbeatRequestProcessor final : public KeeperStorageRequestProcessor<Storage>
{
    using KeeperStorageRequestProcessor<Storage>::KeeperStorageRequestProcessor;

    Coordination::ZooKeeperResponsePtr
    process(Storage & storage, int64_t zxid) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = this->zk_request->makeResponse();
        response_ptr->error = storage.commit(zxid);
        return response_ptr;
    }
};

template<typename Storage>
struct KeeperStorageSyncRequestProcessor final : public KeeperStorageRequestProcessor<Storage>
{
    using KeeperStorageRequestProcessor<Storage>::KeeperStorageRequestProcessor;

    Coordination::ZooKeeperResponsePtr
    process(Storage & /* storage */, int64_t /* zxid */) const override
    {
        auto response = this->zk_request->makeResponse();
        dynamic_cast<Coordination::ZooKeeperSyncResponse &>(*response).path
            = dynamic_cast<Coordination::ZooKeeperSyncRequest &>(*this->zk_request).path;
        return response;
    }
};

namespace
{

template<typename Storage>
Coordination::ACLs getNodeACLs(Storage & storage, StringRef path, bool is_local)
{
    if (is_local)
    {
        auto node_it = storage.container.find(path);
        if (node_it == storage.container.end())
            return {};

        return storage.acl_map.convertNumber(node_it->value.acl_id);
    }

    return storage.uncommitted_state.getACLs(path);
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

template<typename Container>
bool KeeperStorage<Container>::checkACL(StringRef path, int32_t permission, int64_t session_id, bool is_local)
{
    const auto node_acls = getNodeACLs(*this, path, is_local);
    if (node_acls.empty())
        return true;

    if (uncommitted_state.hasACL(session_id, is_local, [](const auto & auth_id) { return auth_id.scheme == "super"; }))
        return true;

    for (const auto & node_acl : node_acls)
    {
        if (node_acl.permissions & permission)
        {
            if (node_acl.scheme == "world" && node_acl.id == "anyone")
                return true;

            if (uncommitted_state.hasACL(
                    session_id,
                    is_local,
                    [&](const auto & auth_id) { return auth_id.scheme == node_acl.scheme && auth_id.id == node_acl.id; }))
                return true;
        }
    }

    return false;
}

template<typename Container>
void KeeperStorage<Container>::unregisterEphemeralPath(int64_t session_id, const std::string & path)
{
    auto ephemerals_it = ephemerals.find(session_id);
    if (ephemerals_it == ephemerals.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Session {} is missing ephemeral path", session_id);

    ephemerals_it->second.erase(path);
    if (ephemerals_it->second.empty())
        ephemerals.erase(ephemerals_it);
}

template<typename Storage>
struct KeeperStorageCreateRequestProcessor final : public KeeperStorageRequestProcessor<Storage>
{
    using KeeperStorageRequestProcessor<Storage>::KeeperStorageRequestProcessor;

    KeeperStorageBase::ResponsesForSessions
    processWatches(const Storage & /*storage*/, int64_t /*zxid*/, KeeperStorageBase::Watches & watches, KeeperStorageBase::Watches & list_watches) const override
    {
        return processWatchesImpl(this->zk_request->getPath(), watches, list_watches, Coordination::Event::CREATED);
    }

    bool checkAuth(Storage & storage, int64_t session_id, bool is_local) const override
    {
        auto path = this->zk_request->getPath();
        return storage.checkACL(parentNodePath(path), Coordination::ACL::Create, session_id, is_local);
    }

    std::vector<typename Storage::Delta>
    preprocess(Storage & storage, int64_t zxid, int64_t session_id, int64_t time, uint64_t & digest, const KeeperContext & keeper_context) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperCreateRequest);
        Coordination::ZooKeeperCreateRequest & request = dynamic_cast<Coordination::ZooKeeperCreateRequest &>(*this->zk_request);

        std::vector<typename Storage::Delta> new_deltas;

        auto parent_path = parentNodePath(request.path);
        auto parent_node = storage.uncommitted_state.getNode(parent_path);
        if (parent_node == nullptr)
            return {typename Storage::Delta{zxid, Coordination::Error::ZNONODE}};

        else if (parent_node->isEphemeral())
            return {typename Storage::Delta{zxid, Coordination::Error::ZNOCHILDRENFOREPHEMERALS}};

        std::string path_created = request.path;
        if (request.is_sequential)
        {
            if (request.not_exists)
                return {typename Storage::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};

            auto seq_num = parent_node->seqNum();

            std::stringstream seq_num_str; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            seq_num_str.exceptions(std::ios::failbit);
            seq_num_str << std::setw(10) << std::setfill('0') << seq_num;

            path_created += seq_num_str.str();
        }

        if (Coordination::matchPath(path_created, keeper_system_path) != Coordination::PathMatchResult::NOT_MATCH)
        {
            auto error_msg = fmt::format("Trying to create a node inside the internal Keeper path ({}) which is not allowed. Path: {}", keeper_system_path, path_created);

            handleSystemNodeModification(keeper_context, error_msg);
            return {typename Storage::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};
        }

        if (storage.uncommitted_state.getNode(path_created))
        {
            if (this->zk_request->getOpNum() == Coordination::OpNum::CreateIfNotExists)
                return new_deltas;

            return {typename Storage::Delta{zxid, Coordination::Error::ZNODEEXISTS}};
        }

        if (getBaseNodeName(path_created).size == 0)
            return {typename Storage::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};

        Coordination::ACLs node_acls;
        if (!fixupACL(request.acls, session_id, storage.uncommitted_state, node_acls))
            return {typename Storage::Delta{zxid, Coordination::Error::ZINVALIDACL}};

        if (request.is_ephemeral)
            storage.ephemerals[session_id].emplace(path_created);

        int32_t parent_cversion = request.parent_cversion;

        auto parent_update = [parent_cversion, zxid](Storage::Node & node)
        {
            /// Increment sequential number even if node is not sequential
            node.increaseSeqNum();
            if (parent_cversion == -1)
                ++node.cversion;
            else if (parent_cversion > node.cversion)
                node.cversion = parent_cversion;

            node.pzxid = std::max(zxid, node.pzxid);
            node.increaseNumChildren();
        };

        new_deltas.emplace_back(std::string{parent_path}, zxid, typename Storage::UpdateNodeDelta{std::move(parent_update)});

        Coordination::Stat stat;
        stat.czxid = zxid;
        stat.mzxid = zxid;
        stat.pzxid = zxid;
        stat.ctime = time;
        stat.mtime = time;
        stat.numChildren = 0;
        stat.version = 0;
        stat.aversion = 0;
        stat.cversion = 0;
        stat.ephemeralOwner = request.is_ephemeral ? session_id : 0;

        new_deltas.emplace_back(
            std::move(path_created),
            zxid,
            typename Storage::CreateNodeDelta{stat, std::move(node_acls), request.data});

        digest = storage.calculateNodesDigest(digest, new_deltas);
        return new_deltas;
    }

    Coordination::ZooKeeperResponsePtr process(Storage & storage, int64_t zxid) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = this->zk_request->makeResponse();
        Coordination::ZooKeeperCreateResponse & response = dynamic_cast<Coordination::ZooKeeperCreateResponse &>(*response_ptr);

        if (storage.uncommitted_state.deltas.begin()->zxid != zxid)
        {
            response.path_created = this->zk_request->getPath();
            response.error = Coordination::Error::ZOK;
            return response_ptr;
        }

        if (const auto result = storage.commit(zxid); result != Coordination::Error::ZOK)
        {
            response.error = result;
            return response_ptr;
        }

        const auto & deltas = storage.uncommitted_state.deltas;
        auto create_delta_it = std::find_if(
            deltas.begin(),
            deltas.end(),
            [zxid](const auto & delta)
            { return delta.zxid == zxid && std::holds_alternative<typename Storage::CreateNodeDelta>(delta.operation); });

        response.path_created = create_delta_it->path;
        response.error = Coordination::Error::ZOK;
        return response_ptr;
    }
};

template<typename Storage>
struct KeeperStorageGetRequestProcessor final : public KeeperStorageRequestProcessor<Storage>
{
    bool checkAuth(Storage & storage, int64_t session_id, bool is_local) const override
    {
        return storage.checkACL(this->zk_request->getPath(), Coordination::ACL::Read, session_id, is_local);
    }

    using KeeperStorageRequestProcessor<Storage>::KeeperStorageRequestProcessor;

    std::vector<typename Storage::Delta>
    preprocess(Storage & storage, int64_t zxid, int64_t /*session_id*/, int64_t /*time*/, uint64_t & /*digest*/, const KeeperContext & /*keeper_context*/) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperGetRequest);
        Coordination::ZooKeeperGetRequest & request = dynamic_cast<Coordination::ZooKeeperGetRequest &>(*this->zk_request);

        if (request.path == Coordination::keeper_api_feature_flags_path
            || request.path == Coordination::keeper_config_path
            || request.path == Coordination::keeper_availability_zone_path)
            return {};

        if (!storage.uncommitted_state.getNode(request.path))
            return {typename Storage::Delta{zxid, Coordination::Error::ZNONODE}};

        return {};
    }

    template <bool local>
    Coordination::ZooKeeperResponsePtr processImpl(Storage & storage, int64_t zxid) const
    {
        Coordination::ZooKeeperResponsePtr response_ptr = this->zk_request->makeResponse();
        Coordination::ZooKeeperGetResponse & response = dynamic_cast<Coordination::ZooKeeperGetResponse &>(*response_ptr);
        Coordination::ZooKeeperGetRequest & request = dynamic_cast<Coordination::ZooKeeperGetRequest &>(*this->zk_request);

        if constexpr (!local)
        {
            if (const auto result = storage.commit(zxid); result != Coordination::Error::ZOK)
            {
                response.error = result;
                return response_ptr;
            }
        }

        if (request.path == Coordination::keeper_config_path)
        {
            response.data = serializeClusterConfig(
                storage.keeper_context->getDispatcher()->getStateMachine().getClusterConfig());
            response.error = Coordination::Error::ZOK;
            return response_ptr;
        }

        auto & container = storage.container;
        auto node_it = container.find(request.path);
        if (node_it == container.end())
        {
            if constexpr (local)
                response.error = Coordination::Error::ZNONODE;
            else
                onStorageInconsistency();
        }
        else
        {
            node_it->value.setResponseStat(response.stat);
            auto data = node_it->value.getData();
            response.data = std::string(data);
            response.error = Coordination::Error::ZOK;
        }

        return response_ptr;
    }


    Coordination::ZooKeeperResponsePtr process(Storage & storage, int64_t zxid) const override
    {
        return processImpl<false>(storage, zxid);
    }

    Coordination::ZooKeeperResponsePtr processLocal(Storage & storage, int64_t zxid) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperGetRequest);
        return processImpl<true>(storage, zxid);
    }
};

namespace
{

template <typename Storage>
void addUpdateParentPzxidDelta(Storage & storage, std::vector<typename Storage::Delta> & deltas, int64_t zxid, StringRef path)
{
    auto parent_path = parentNodePath(path);
    if (!storage.uncommitted_state.getNode(parent_path))
        return;

    deltas.emplace_back(
        std::string{parent_path},
        zxid,
        typename Storage::UpdateNodeDelta
        {
            [zxid](Storage::Node & parent)
            {
                parent.pzxid = std::max(parent.pzxid, zxid);
            }
        }
    );
}

}

template<typename Storage>
struct KeeperStorageRemoveRequestProcessor final : public KeeperStorageRequestProcessor<Storage>
{
    using KeeperStorageRequestProcessor<Storage>::KeeperStorageRequestProcessor;

    bool checkAuth(Storage & storage, int64_t session_id, bool is_local) const override
    {
        return storage.checkACL(parentNodePath(this->zk_request->getPath()), Coordination::ACL::Delete, session_id, is_local);
    }

    std::vector<typename Storage::Delta>
    preprocess(Storage & storage, int64_t zxid, int64_t /*session_id*/, int64_t /*time*/, uint64_t & digest, const KeeperContext & keeper_context) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperRemoveRequest);
        Coordination::ZooKeeperRemoveRequest & request = dynamic_cast<Coordination::ZooKeeperRemoveRequest &>(*this->zk_request);

        std::vector<typename Storage::Delta> new_deltas;

        if (Coordination::matchPath(request.path, keeper_system_path) != Coordination::PathMatchResult::NOT_MATCH)
        {
            auto error_msg = fmt::format("Trying to delete an internal Keeper path ({}) which is not allowed", request.path);

            handleSystemNodeModification(keeper_context, error_msg);
            return {typename Storage::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};
        }

        auto node = storage.uncommitted_state.getNode(request.path);

        if (!node)
        {
            if (request.restored_from_zookeeper_log)
                addUpdateParentPzxidDelta(storage, new_deltas, zxid, request.path);
            return {typename Storage::Delta{zxid, Coordination::Error::ZNONODE}};
        }
        else if (request.version != -1 && request.version != node->version)
            return {typename Storage::Delta{zxid, Coordination::Error::ZBADVERSION}};
        else if (node->numChildren() != 0)
            return {typename Storage::Delta{zxid, Coordination::Error::ZNOTEMPTY}};

        if (request.restored_from_zookeeper_log)
            addUpdateParentPzxidDelta(storage, new_deltas, zxid, request.path);

        new_deltas.emplace_back(
            std::string{parentNodePath(request.path)},
            zxid,
            typename Storage::UpdateNodeDelta{[](typename Storage::Node & parent)
                                           {
                                               ++parent.cversion;
                                               parent.decreaseNumChildren();
                                           }});

        new_deltas.emplace_back(request.path, zxid, typename Storage::RemoveNodeDelta{request.version, node->ephemeralOwner()});

        if (node->isEphemeral())
            storage.unregisterEphemeralPath(node->ephemeralOwner(), request.path);

        digest = storage.calculateNodesDigest(digest, new_deltas);

        return new_deltas;
    }

    Coordination::ZooKeeperResponsePtr process(Storage & storage, int64_t zxid) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = this->zk_request->makeResponse();
        Coordination::ZooKeeperRemoveResponse & response = dynamic_cast<Coordination::ZooKeeperRemoveResponse &>(*response_ptr);

        response.error = storage.commit(zxid);
        return response_ptr;
    }

    KeeperStorageBase::ResponsesForSessions
    processWatches(const Storage & /*storage*/, int64_t /*zxid*/, KeeperStorageBase::Watches & watches, KeeperStorageBase::Watches & list_watches) const override
    {
        return processWatchesImpl(this->zk_request->getPath(), watches, list_watches, Coordination::Event::DELETED);
    }
};

template<typename Storage>
struct KeeperStorageRemoveRecursiveRequestProcessor final : public KeeperStorageRequestProcessor<Storage>
{
    using KeeperStorageRequestProcessor<Storage>::KeeperStorageRequestProcessor;

    bool checkAuth(Storage & storage, int64_t session_id, bool is_local) const override
    {
        return storage.checkACL(parentNodePath(this->zk_request->getPath()), Coordination::ACL::Delete, session_id, is_local);
    }

    std::vector<typename Storage::Delta>
    preprocess(Storage & storage, int64_t zxid, int64_t session_id, int64_t /*time*/, uint64_t & digest, const KeeperContext & keeper_context) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperRemoveRequest);
        Coordination::ZooKeeperRemoveRecursiveRequest & request = dynamic_cast<Coordination::ZooKeeperRemoveRecursiveRequest &>(*this->zk_request);

        std::vector<typename Storage::Delta> new_deltas;

        if (Coordination::matchPath(request.path, keeper_system_path) != Coordination::PathMatchResult::NOT_MATCH)
        {
            auto error_msg = fmt::format("Trying to delete an internal Keeper path ({}) which is not allowed", request.path);

            handleSystemNodeModification(keeper_context, error_msg);
            return {typename Storage::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};
        }

        auto node = storage.uncommitted_state.getNode(request.path);

        if (!node)
        {
            if (request.restored_from_zookeeper_log)
                addUpdateParentPzxidDelta(storage, new_deltas, zxid, request.path);

            return {typename Storage::Delta{zxid, Coordination::Error::ZNONODE}};
        }

        ToDeleteTreeCollector collector(storage, zxid, session_id, request.remove_nodes_limit);
        auto collect_status = collector.collect(request.path, *node);

        if (collect_status == ToDeleteTreeCollector::CollectStatus::NoAuth)
            return {typename Storage::Delta{zxid, Coordination::Error::ZNOAUTH}};

        if (collect_status == ToDeleteTreeCollector::CollectStatus::LimitExceeded)
            return {typename Storage::Delta{zxid, Coordination::Error::ZNOTEMPTY}};

        if (request.restored_from_zookeeper_log)
            addUpdateParentPzxidDelta(storage, new_deltas, zxid, request.path);

        auto delete_deltas = collector.extractDeltas();

        for (const auto & delta : delete_deltas)
        {
            const auto * remove_delta = std::get_if<typename Storage::RemoveNodeDelta>(&delta.operation);
            if (remove_delta && remove_delta->ephemeral_owner)
                storage.unregisterEphemeralPath(remove_delta->ephemeral_owner, delta.path);
        }

        new_deltas.insert(new_deltas.end(), std::make_move_iterator(delete_deltas.begin()), std::make_move_iterator(delete_deltas.end()));

        digest = storage.calculateNodesDigest(digest, new_deltas);

        return new_deltas;
    }

    Coordination::ZooKeeperResponsePtr process(Storage & storage, int64_t zxid) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = this->zk_request->makeResponse();
        Coordination::ZooKeeperRemoveRecursiveResponse & response = dynamic_cast<Coordination::ZooKeeperRemoveRecursiveResponse &>(*response_ptr);

        response.error = storage.commit(zxid);
        return response_ptr;
    }

    KeeperStorageBase::ResponsesForSessions
    processWatches(const Storage & storage, int64_t zxid, KeeperStorageBase::Watches & watches, KeeperStorageBase::Watches & list_watches) const override
    {
        /// need to iterate over zxid deltas and update watches for deleted tree.
        const auto & deltas = storage.uncommitted_state.deltas;

        KeeperStorageBase::ResponsesForSessions responses;
        for (auto it = deltas.rbegin(); it != deltas.rend() && it->zxid == zxid; ++it)
        {
            const auto * remove_delta = std::get_if<typename Storage::RemoveNodeDelta>(&it->operation);
            if (remove_delta)
            {
                auto new_responses = processWatchesImpl(it->path, watches, list_watches, Coordination::Event::DELETED);
                responses.insert(responses.end(), std::make_move_iterator(new_responses.begin()), std::make_move_iterator(new_responses.end()));
            }
        }

        return responses;
    }

private:
    using SNode = typename Storage::Node;

    class ToDeleteTreeCollector
    {
        Storage & storage;
        int64_t zxid;
        int64_t session_id;
        uint32_t limit;

        uint32_t max_level = 0;
        uint32_t nodes_observed = 1;  /// root node
        std::unordered_map<uint32_t, std::vector<typename Storage::Delta>> by_level_deltas;

        struct Step
        {
            String path;
            std::variant<SNode, const SNode *> node;
            uint32_t level;
        };

        enum class CollectStatus
        {
            Ok,
            NoAuth,
            LimitExceeded,
        };

        friend struct KeeperStorageRemoveRecursiveRequestProcessor;

    public:
        ToDeleteTreeCollector(Storage & storage_, int64_t zxid_, int64_t session_id_, uint32_t limit_)
            : storage(storage_)
            , zxid(zxid_)
            , session_id(session_id_)
            , limit(limit_)
        {
        }

        CollectStatus collect(StringRef root_path, const SNode & root_node)
        {
            std::deque<Step> steps;

            if (checkLimits(&root_node))
                return CollectStatus::LimitExceeded;

            steps.push_back(Step{root_path.toString(), &root_node, 0});

            while (!steps.empty())
            {
                Step step = std::move(steps.front());
                steps.pop_front();

                StringRef path = step.path;
                uint32_t level = step.level;
                const SNode * node_ptr = nullptr;

                if (auto * rdb = std::get_if<SNode>(&step.node))
                    node_ptr = rdb;
                else
                    node_ptr = std::get<const SNode *>(step.node);

                chassert(!path.empty());
                chassert(node_ptr != nullptr);

                const auto & node = *node_ptr;
                auto actual_node_ptr = storage.uncommitted_state.getActualNodeView(path, node);
                chassert(actual_node_ptr != nullptr); /// explicitly check that node is not deleted

                if (actual_node_ptr->numChildren() > 0 && !storage.checkACL(path, Coordination::ACL::Delete, session_id, /*is_local=*/false))
                    return CollectStatus::NoAuth;

                if (auto status = visitRocksDBNode(steps, path, level); status != CollectStatus::Ok)
                    return status;

                if (auto status = visitMemNode(steps, path, level); status != CollectStatus::Ok)
                    return status;

                if (auto status = visitRootAndUncommitted(steps, path, node, level); status != CollectStatus::Ok)
                    return status;
            }

            return CollectStatus::Ok;
        }

        std::vector<typename Storage::Delta> extractDeltas()
        {
            std::vector<typename Storage::Delta> deltas;

            for (ssize_t level = max_level; level >= 0; --level)
            {
                auto & level_deltas = by_level_deltas[static_cast<uint32_t>(level)];
                deltas.insert(deltas.end(), std::make_move_iterator(level_deltas.begin()), std::make_move_iterator(level_deltas.end()));
            }

            return std::move(deltas);
        }

    private:
        CollectStatus visitRocksDBNode(std::deque<Step> & steps, StringRef root_path, uint32_t level)
        {
            if constexpr (Storage::use_rocksdb)
            {
                std::filesystem::path root_fs_path(root_path.toString());
                auto children = storage.container.getChildren(root_path.toString());

                for (auto && [child_name, child_node] : children)
                {
                    auto child_path = (root_fs_path / child_name).generic_string();
                    const auto actual_child_node_ptr = storage.uncommitted_state.getActualNodeView(child_path, child_node);

                    if (actual_child_node_ptr == nullptr) /// node was deleted in previous step of multi transaction
                        continue;

                    if (checkLimits(actual_child_node_ptr))
                        return CollectStatus::LimitExceeded;

                    steps.push_back(Step{std::move(child_path), std::move(child_node), level + 1});
                }
            }

            return CollectStatus::Ok;
        }

        CollectStatus visitMemNode(std::deque<Step> & steps, StringRef root_path, uint32_t level)
        {
            if constexpr (!Storage::use_rocksdb)
            {
                auto node_it = storage.container.find(root_path);
                if (node_it == storage.container.end())
                    return CollectStatus::Ok;

                std::filesystem::path root_fs_path(root_path.toString());
                const auto & children = node_it->value.getChildren();

                for (const auto & child_name : children)
                {
                    auto child_path = (root_fs_path / child_name.toView()).generic_string();

                    auto child_it = storage.container.find(child_path);
                    chassert(child_it != storage.container.end());
                    const auto & child_node = child_it->value;

                    const auto actual_child_node_ptr = storage.uncommitted_state.getActualNodeView(child_path, child_node);

                    if (actual_child_node_ptr == nullptr) /// node was deleted in previous step of multi transaction
                        continue;

                    if (checkLimits(actual_child_node_ptr))
                        return CollectStatus::LimitExceeded;

                    steps.push_back(Step{std::move(child_path), &child_node, level + 1});
                }
            }

            return CollectStatus::Ok;
        }

        CollectStatus visitRootAndUncommitted(std::deque<Step> & steps, StringRef root_path, const SNode & root_node, uint32_t level)
        {
            const auto & nodes = storage.uncommitted_state.nodes;

            /// nodes are sorted by paths with level locality
            auto it = nodes.upper_bound(root_path.toString() + "/");

            for (; it != nodes.end() && parentNodePath(it->first) == root_path; ++it)
            {
                const auto actual_child_node_ptr = it->second.node.get();

                if (actual_child_node_ptr == nullptr) /// node was deleted in previous step of multi transaction
                    continue;

                if (checkLimits(actual_child_node_ptr))
                    return CollectStatus::LimitExceeded;

                const String & child_path = it->first;
                const SNode & child_node = *it->second.node;

                steps.push_back(Step{child_path, &child_node, level + 1});
            }

            addDelta(root_path, root_node, level);

            return CollectStatus::Ok;
        }

        void addDelta(StringRef root_path, const SNode & root_node, uint32_t level)
        {
            max_level = std::max(max_level, level);

            by_level_deltas[level].emplace_back(
                parentNodePath(root_path).toString(),
                zxid,
                typename Storage::UpdateNodeDelta{
                    [](SNode & parent)
                    {
                        ++parent.cversion;
                        parent.decreaseNumChildren();
                    }
                });

            by_level_deltas[level].emplace_back(root_path.toString(), zxid, typename Storage::RemoveNodeDelta{root_node.version, root_node.ephemeralOwner()});
        }

        bool checkLimits(const SNode * node)
        {
            chassert(node != nullptr);
            nodes_observed += node->numChildren();
            return nodes_observed > limit;
        }
    };
};

template<typename Storage>
struct KeeperStorageExistsRequestProcessor final : public KeeperStorageRequestProcessor<Storage>
{
    using KeeperStorageRequestProcessor<Storage>::KeeperStorageRequestProcessor;

    std::vector<typename Storage::Delta>
    preprocess(Storage & storage, int64_t zxid, int64_t /*session_id*/, int64_t /*time*/, uint64_t & /*digest*/, const KeeperContext & /*keeper_context*/) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperExistsRequest);
        Coordination::ZooKeeperExistsRequest & request = dynamic_cast<Coordination::ZooKeeperExistsRequest &>(*this->zk_request);

        if (!storage.uncommitted_state.getNode(request.path))
            return {typename Storage::Delta{zxid, Coordination::Error::ZNONODE}};

        return {};
    }

    template <bool local>
    Coordination::ZooKeeperResponsePtr processImpl(Storage & storage, int64_t zxid) const
    {
        Coordination::ZooKeeperResponsePtr response_ptr = this->zk_request->makeResponse();
        Coordination::ZooKeeperExistsResponse & response = dynamic_cast<Coordination::ZooKeeperExistsResponse &>(*response_ptr);
        Coordination::ZooKeeperExistsRequest & request = dynamic_cast<Coordination::ZooKeeperExistsRequest &>(*this->zk_request);

        if constexpr (!local)
        {
            if (const auto result = storage.commit(zxid); result != Coordination::Error::ZOK)
            {
                response.error = result;
                return response_ptr;
            }
        }

        auto & container = storage.container;
        auto node_it = container.find(request.path);
        if (node_it == container.end())
        {
            if constexpr (local)
                response.error = Coordination::Error::ZNONODE;
            else
                onStorageInconsistency();
        }
        else
        {
            node_it->value.setResponseStat(response.stat);
            response.error = Coordination::Error::ZOK;
        }

        return response_ptr;
    }

    Coordination::ZooKeeperResponsePtr process(Storage & storage, int64_t zxid) const override
    {
        return processImpl<false>(storage, zxid);
    }

    Coordination::ZooKeeperResponsePtr processLocal(Storage & storage, int64_t zxid) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperExistsRequest);
        return processImpl<true>(storage, zxid);
    }
};

template<typename Storage>
struct KeeperStorageSetRequestProcessor final : public KeeperStorageRequestProcessor<Storage>
{
    bool checkAuth(Storage & storage, int64_t session_id, bool is_local) const override
    {
        return storage.checkACL(this->zk_request->getPath(), Coordination::ACL::Write, session_id, is_local);
    }

    using KeeperStorageRequestProcessor<Storage>::KeeperStorageRequestProcessor;

    std::vector<typename Storage::Delta>
    preprocess(Storage & storage, int64_t zxid, int64_t /*session_id*/, int64_t time, uint64_t & digest, const KeeperContext & keeper_context) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperSetRequest);
        Coordination::ZooKeeperSetRequest & request = dynamic_cast<Coordination::ZooKeeperSetRequest &>(*this->zk_request);

        std::vector<typename Storage::Delta> new_deltas;

        if (Coordination::matchPath(request.path, keeper_system_path) != Coordination::PathMatchResult::NOT_MATCH)
        {
            auto error_msg = fmt::format("Trying to update an internal Keeper path ({}) which is not allowed", request.path);

            handleSystemNodeModification(keeper_context, error_msg);
            return {typename Storage::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};
        }

        if (!storage.uncommitted_state.getNode(request.path))
            return {typename Storage::Delta{zxid, Coordination::Error::ZNONODE}};

        auto node = storage.uncommitted_state.getNode(request.path);

        if (request.version != -1 && request.version != node->version)
            return {typename Storage::Delta{zxid, Coordination::Error::ZBADVERSION}};

        new_deltas.emplace_back(
            request.path,
            zxid,
            typename Storage::UpdateNodeDelta{
                [zxid, data = request.data, time](typename Storage::Node & value)
                {
                    value.version++;
                    value.mzxid = zxid;
                    value.mtime = time;
                    value.setData(data);
                },
                request.version});

        new_deltas.emplace_back(
                parentNodePath(request.path).toString(),
                zxid,
                typename Storage::UpdateNodeDelta
                {
                    [](Storage::Node & parent)
                    {
                        parent.cversion++;
                    }
                }
        );

        digest = storage.calculateNodesDigest(digest, new_deltas);
        return new_deltas;
    }

    Coordination::ZooKeeperResponsePtr process(Storage & storage, int64_t zxid) const override
    {
        auto & container = storage.container;

        Coordination::ZooKeeperResponsePtr response_ptr = this->zk_request->makeResponse();
        Coordination::ZooKeeperSetResponse & response = dynamic_cast<Coordination::ZooKeeperSetResponse &>(*response_ptr);
        Coordination::ZooKeeperSetRequest & request = dynamic_cast<Coordination::ZooKeeperSetRequest &>(*this->zk_request);

        if (const auto result = storage.commit(zxid); result != Coordination::Error::ZOK)
        {
            response.error = result;
            return response_ptr;
        }

        auto node_it = container.find(request.path);
        if (node_it == container.end())
            onStorageInconsistency();

        node_it->value.setResponseStat(response.stat);
        response.error = Coordination::Error::ZOK;

        return response_ptr;
    }

    KeeperStorageBase::ResponsesForSessions
    processWatches(const Storage & /*storage*/, int64_t /*zxid*/, typename Storage::Watches & watches, typename Storage::Watches & list_watches) const override
    {
        return processWatchesImpl(this->zk_request->getPath(), watches, list_watches, Coordination::Event::CHANGED);
    }
};

template<typename Storage>
struct KeeperStorageListRequestProcessor final : public KeeperStorageRequestProcessor<Storage>
{
    bool checkAuth(Storage & storage, int64_t session_id, bool is_local) const override
    {
        return storage.checkACL(this->zk_request->getPath(), Coordination::ACL::Read, session_id, is_local);
    }

    using KeeperStorageRequestProcessor<Storage>::KeeperStorageRequestProcessor;

    std::vector<typename Storage::Delta>
    preprocess(Storage & storage, int64_t zxid, int64_t /*session_id*/, int64_t /*time*/, uint64_t & /*digest*/, const KeeperContext & /*keeper_context*/) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperListRequest);
        Coordination::ZooKeeperListRequest & request = dynamic_cast<Coordination::ZooKeeperListRequest &>(*this->zk_request);

        if (!storage.uncommitted_state.getNode(request.path))
            return {typename Storage::Delta{zxid, Coordination::Error::ZNONODE}};

        return {};
    }

    template <bool local>
    Coordination::ZooKeeperResponsePtr processImpl(Storage & storage, int64_t zxid) const
    {
        Coordination::ZooKeeperResponsePtr response_ptr = this->zk_request->makeResponse();
        Coordination::ZooKeeperListResponse & response = dynamic_cast<Coordination::ZooKeeperListResponse &>(*response_ptr);
        Coordination::ZooKeeperListRequest & request = dynamic_cast<Coordination::ZooKeeperListRequest &>(*this->zk_request);

        if constexpr (!local)
        {
            if (const auto result = storage.commit(zxid); result != Coordination::Error::ZOK)
            {
                response.error = result;
                return response_ptr;
            }
        }

        auto & container = storage.container;

        auto node_it = container.find(request.path);
        if (node_it == container.end())
        {
            if constexpr (local)
                response.error = Coordination::Error::ZNONODE;
            else
                onStorageInconsistency();
        }
        else
        {
            auto path_prefix = request.path;
            if (path_prefix.empty())
                throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Path cannot be empty");

            const auto & get_children = [&]()
            {
                if constexpr (Storage::use_rocksdb)
                    return container.getChildren(request.path);
                else
                    return node_it->value.getChildren();
            };
            const auto & children = get_children();
            response.names.reserve(children.size());

            const auto add_child = [&](const auto & child)
            {
                using enum Coordination::ListRequestType;

                auto list_request_type = ALL;
                if (auto * filtered_list = dynamic_cast<Coordination::ZooKeeperFilteredListRequest *>(&request))
                {
                    list_request_type = filtered_list->list_request_type;
                }

                if (list_request_type == ALL)
                    return true;

                bool is_ephemeral;
                if constexpr (!Storage::use_rocksdb)
                {
                    auto child_path = (std::filesystem::path(request.path) / child.toView()).generic_string();
                    auto child_it = container.find(child_path);
                    if (child_it == container.end())
                        onStorageInconsistency();
                    is_ephemeral = child_it->value.isEphemeral();
                }
                else
                {
                    is_ephemeral = child.second.isEphemeral();
                }

                return (is_ephemeral && list_request_type == EPHEMERAL_ONLY) || (!is_ephemeral && list_request_type == PERSISTENT_ONLY);
            };

            for (const auto & child : children)
            {
                if (add_child(child))
                {
                    if constexpr (Storage::use_rocksdb)
                        response.names.push_back(child.first);
                    else
                        response.names.push_back(child.toString());
                }
            }

            node_it->value.setResponseStat(response.stat);
            response.error = Coordination::Error::ZOK;
        }

        return response_ptr;
    }

    Coordination::ZooKeeperResponsePtr process(Storage & storage, int64_t zxid) const override
    {
        return processImpl<false>(storage, zxid);
    }

    Coordination::ZooKeeperResponsePtr processLocal(Storage & storage, int64_t zxid) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperListRequest);
        return processImpl<true>(storage, zxid);
    }
};

template<typename Storage>
struct KeeperStorageCheckRequestProcessor final : public KeeperStorageRequestProcessor<Storage>
{
    explicit KeeperStorageCheckRequestProcessor(const Coordination::ZooKeeperRequestPtr & zk_request_)
        : KeeperStorageRequestProcessor<Storage>(zk_request_)
    {
        check_not_exists = this->zk_request->getOpNum() == Coordination::OpNum::CheckNotExists;
    }

    bool checkAuth(Storage & storage, int64_t session_id, bool is_local) const override
    {
        auto path = this->zk_request->getPath();
        return storage.checkACL(check_not_exists ? parentNodePath(path) : path, Coordination::ACL::Read, session_id, is_local);
    }

    std::vector<typename Storage::Delta>
    preprocess(Storage & storage, int64_t zxid, int64_t /*session_id*/, int64_t /*time*/, uint64_t & /*digest*/, const KeeperContext & /*keeper_context*/) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperCheckRequest);

        Coordination::ZooKeeperCheckRequest & request = dynamic_cast<Coordination::ZooKeeperCheckRequest &>(*this->zk_request);

        auto node = storage.uncommitted_state.getNode(request.path);
        if (check_not_exists)
        {
            if (node && (request.version == -1 || request.version == node->version))
                return {typename Storage::Delta{zxid, Coordination::Error::ZNODEEXISTS}};
        }
        else
        {
            if (!node)
                return {typename Storage::Delta{zxid, Coordination::Error::ZNONODE}};

            if (request.version != -1 && request.version != node->version)
                return {typename Storage::Delta{zxid, Coordination::Error::ZBADVERSION}};
        }

        return {};
    }

    template <bool local>
    Coordination::ZooKeeperResponsePtr processImpl(Storage & storage, int64_t zxid) const
    {
        Coordination::ZooKeeperResponsePtr response_ptr = this->zk_request->makeResponse();
        Coordination::ZooKeeperCheckResponse & response = dynamic_cast<Coordination::ZooKeeperCheckResponse &>(*response_ptr);
        Coordination::ZooKeeperCheckRequest & request = dynamic_cast<Coordination::ZooKeeperCheckRequest &>(*this->zk_request);

        if constexpr (!local)
        {
            if (const auto result = storage.commit(zxid); result != Coordination::Error::ZOK)
            {
                response.error = result;
                return response_ptr;
            }
        }

        const auto on_error = [&]([[maybe_unused]] const auto error_code)
        {
            if constexpr (local)
                response.error = error_code;
            else
                onStorageInconsistency();
        };

        auto & container = storage.container;
        auto node_it = container.find(request.path);

        if (check_not_exists)
        {
            if (node_it != container.end() && (request.version == -1 || request.version == node_it->value.version))
                on_error(Coordination::Error::ZNODEEXISTS);
            else
                response.error = Coordination::Error::ZOK;
        }
        else
        {
            if (node_it == container.end())
                on_error(Coordination::Error::ZNONODE);
            else if (request.version != -1 && request.version != node_it->value.version)
                on_error(Coordination::Error::ZBADVERSION);
            else
                response.error = Coordination::Error::ZOK;
        }

        return response_ptr;
    }

    Coordination::ZooKeeperResponsePtr process(Storage & storage, int64_t zxid) const override
    {
        return processImpl<false>(storage, zxid);
    }

    Coordination::ZooKeeperResponsePtr processLocal(Storage & storage, int64_t zxid) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperCheckRequest);
        return processImpl<true>(storage, zxid);
    }

private:
    bool check_not_exists;
};


template<typename Storage>
struct KeeperStorageSetACLRequestProcessor final : public KeeperStorageRequestProcessor<Storage>
{
    bool checkAuth(Storage & storage, int64_t session_id, bool is_local) const override
    {
        return storage.checkACL(this->zk_request->getPath(), Coordination::ACL::Admin, session_id, is_local);
    }

    using KeeperStorageRequestProcessor<Storage>::KeeperStorageRequestProcessor;

    std::vector<typename Storage::Delta>
    preprocess(Storage & storage, int64_t zxid, int64_t session_id, int64_t /*time*/, uint64_t & digest, const KeeperContext & keeper_context) const override
    {
        Coordination::ZooKeeperSetACLRequest & request = dynamic_cast<Coordination::ZooKeeperSetACLRequest &>(*this->zk_request);

        if (Coordination::matchPath(request.path, keeper_system_path) != Coordination::PathMatchResult::NOT_MATCH)
        {
            auto error_msg = fmt::format("Trying to update an internal Keeper path ({}) which is not allowed", request.path);

            handleSystemNodeModification(keeper_context, error_msg);
            return {typename Storage::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};
        }

        auto & uncommitted_state = storage.uncommitted_state;
        if (!uncommitted_state.getNode(request.path))
            return {typename Storage::Delta{zxid, Coordination::Error::ZNONODE}};

        auto node = uncommitted_state.getNode(request.path);

        if (request.version != -1 && request.version != node->aversion)
            return {typename Storage::Delta{zxid, Coordination::Error::ZBADVERSION}};


        Coordination::ACLs node_acls;
        if (!fixupACL(request.acls, session_id, uncommitted_state, node_acls))
            return {typename Storage::Delta{zxid, Coordination::Error::ZINVALIDACL}};

        std::vector<typename Storage::Delta> new_deltas
        {
            {
                request.path,
                zxid,
                typename Storage::SetACLDelta{std::move(node_acls), request.version}
            },
            {
                request.path,
                zxid,
                typename Storage::UpdateNodeDelta
                {
                    [](typename Storage::Node & n) { ++n.aversion; }
                }
            }
        };

        digest = storage.calculateNodesDigest(digest, new_deltas);

        return new_deltas;
    }

    Coordination::ZooKeeperResponsePtr process(Storage & storage, int64_t zxid) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = this->zk_request->makeResponse();
        Coordination::ZooKeeperSetACLResponse & response = dynamic_cast<Coordination::ZooKeeperSetACLResponse &>(*response_ptr);
        Coordination::ZooKeeperSetACLRequest & request = dynamic_cast<Coordination::ZooKeeperSetACLRequest &>(*this->zk_request);

        if (const auto result = storage.commit(zxid); result != Coordination::Error::ZOK)
        {
            response.error = result;
            return response_ptr;
        }

        auto node_it = storage.container.find(request.path);
        if (node_it == storage.container.end())
            onStorageInconsistency();
        node_it->value.setResponseStat(response.stat);
        response.error = Coordination::Error::ZOK;

        return response_ptr;
    }
};

template<typename Storage>
struct KeeperStorageGetACLRequestProcessor final : public KeeperStorageRequestProcessor<Storage>
{
    bool checkAuth(Storage & storage, int64_t session_id, bool is_local) const override
    {
        return storage.checkACL(this->zk_request->getPath(), Coordination::ACL::Admin | Coordination::ACL::Read, session_id, is_local);
    }

    using KeeperStorageRequestProcessor<Storage>::KeeperStorageRequestProcessor;

    std::vector<typename Storage::Delta>
    preprocess(Storage & storage, int64_t zxid, int64_t /*session_id*/, int64_t /*time*/, uint64_t & /*digest*/, const KeeperContext & /*keeper_context*/) const override
    {
        Coordination::ZooKeeperGetACLRequest & request = dynamic_cast<Coordination::ZooKeeperGetACLRequest &>(*this->zk_request);

        if (!storage.uncommitted_state.getNode(request.path))
            return {typename Storage::Delta{zxid, Coordination::Error::ZNONODE}};

        return {};
    }

    template <bool local>
    Coordination::ZooKeeperResponsePtr processImpl(Storage & storage, int64_t zxid) const
    {
        Coordination::ZooKeeperResponsePtr response_ptr = this->zk_request->makeResponse();
        Coordination::ZooKeeperGetACLResponse & response = dynamic_cast<Coordination::ZooKeeperGetACLResponse &>(*response_ptr);
        Coordination::ZooKeeperGetACLRequest & request = dynamic_cast<Coordination::ZooKeeperGetACLRequest &>(*this->zk_request);

        if constexpr (!local)
        {
            if (const auto result = storage.commit(zxid); result != Coordination::Error::ZOK)
            {
                response.error = result;
                return response_ptr;
            }
        }

        auto & container = storage.container;
        auto node_it = container.find(request.path);
        if (node_it == container.end())
        {
            if constexpr (local)
                response.error = Coordination::Error::ZNONODE;
            else
                onStorageInconsistency();
        }
        else
        {
            node_it->value.setResponseStat(response.stat);
            response.acl = storage.acl_map.convertNumber(node_it->value.acl_id);
        }

        return response_ptr;
    }

    Coordination::ZooKeeperResponsePtr process(Storage & storage, int64_t zxid) const override
    {
        return processImpl<false>(storage, zxid);
    }

    Coordination::ZooKeeperResponsePtr processLocal(Storage & storage, int64_t zxid) const override
    {
        return processImpl<true>(storage, zxid);
    }
};

template<typename Storage>
struct KeeperStorageMultiRequestProcessor final : public KeeperStorageRequestProcessor<Storage>
{
    using OperationType = Coordination::ZooKeeperMultiRequest::OperationType;
    std::optional<OperationType> operation_type;

    bool checkAuth(Storage & storage, int64_t session_id, bool is_local) const override
    {
        for (const auto & concrete_request : concrete_requests)
            if (!concrete_request->checkAuth(storage, session_id, is_local))
                return false;
        return true;
    }

    std::vector<std::shared_ptr<KeeperStorageRequestProcessor<Storage>>> concrete_requests;
    explicit KeeperStorageMultiRequestProcessor(const Coordination::ZooKeeperRequestPtr & zk_request_)
        : KeeperStorageRequestProcessor<Storage>(zk_request_)
    {
        Coordination::ZooKeeperMultiRequest & request = dynamic_cast<Coordination::ZooKeeperMultiRequest &>(*this->zk_request);
        concrete_requests.reserve(request.requests.size());

        const auto check_operation_type = [&](OperationType type)
        {
            if (operation_type.has_value() && *operation_type != type)
                throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Illegal mixing of read and write operations in multi request");
            operation_type = type;
        };

        for (const auto & sub_request : request.requests)
        {
            auto sub_zk_request = std::dynamic_pointer_cast<Coordination::ZooKeeperRequest>(sub_request);
            switch (sub_zk_request->getOpNum())
            {
                case Coordination::OpNum::Create:
                case Coordination::OpNum::CreateIfNotExists:
                    check_operation_type(OperationType::Write);
                    concrete_requests.push_back(std::make_shared<KeeperStorageCreateRequestProcessor<Storage>>(sub_zk_request));
                    break;
                case Coordination::OpNum::Remove:
                    check_operation_type(OperationType::Write);
                    concrete_requests.push_back(std::make_shared<KeeperStorageRemoveRequestProcessor<Storage>>(sub_zk_request));
                    break;
                case Coordination::OpNum::RemoveRecursive:
                    check_operation_type(OperationType::Write);
                    concrete_requests.push_back(std::make_shared<KeeperStorageRemoveRecursiveRequestProcessor<Storage>>(sub_zk_request));
                    break;
                case Coordination::OpNum::Set:
                    check_operation_type(OperationType::Write);
                    concrete_requests.push_back(std::make_shared<KeeperStorageSetRequestProcessor<Storage>>(sub_zk_request));
                    break;
                case Coordination::OpNum::Check:
                case Coordination::OpNum::CheckNotExists:
                    check_operation_type(OperationType::Write);
                    concrete_requests.push_back(std::make_shared<KeeperStorageCheckRequestProcessor<Storage>>(sub_zk_request));
                    break;
                case Coordination::OpNum::Get:
                    check_operation_type(OperationType::Read);
                    concrete_requests.push_back(std::make_shared<KeeperStorageGetRequestProcessor<Storage>>(sub_zk_request));
                    break;
                case Coordination::OpNum::Exists:
                    check_operation_type(OperationType::Read);
                    concrete_requests.push_back(std::make_shared<KeeperStorageExistsRequestProcessor<Storage>>(sub_zk_request));
                    break;
                case Coordination::OpNum::List:
                case Coordination::OpNum::FilteredList:
                case Coordination::OpNum::SimpleList:
                    check_operation_type(OperationType::Read);
                    concrete_requests.push_back(std::make_shared<KeeperStorageListRequestProcessor<Storage>>(sub_zk_request));
                    break;
                default:
                    throw DB::Exception(
                                        ErrorCodes::BAD_ARGUMENTS,
                                        "Illegal command as part of multi ZooKeeper request {}",
                                        sub_zk_request->getOpNum());
            }
        }

        chassert(request.requests.empty() || operation_type.has_value());
    }

    std::vector<typename Storage::Delta>
    preprocess(Storage & storage, int64_t zxid, int64_t session_id, int64_t time, uint64_t & digest, const KeeperContext & keeper_context) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperMultiRequest);
        std::vector<Coordination::Error> response_errors;
        response_errors.reserve(concrete_requests.size());
        uint64_t current_digest = digest;
        for (size_t i = 0; i < concrete_requests.size(); ++i)
        {
            auto new_deltas = concrete_requests[i]->preprocess(storage, zxid, session_id, time, current_digest, keeper_context);

            if (!new_deltas.empty())
            {
                if (auto * error = std::get_if<typename Storage::ErrorDelta>(&new_deltas.back().operation);
                    error && *operation_type == OperationType::Write)
                {
                    storage.uncommitted_state.rollback(zxid);
                    response_errors.push_back(error->error);

                    for (size_t j = i + 1; j < concrete_requests.size(); ++j)
                    {
                        response_errors.push_back(Coordination::Error::ZRUNTIMEINCONSISTENCY);
                    }

                    return {typename Storage::Delta{zxid, typename Storage::FailedMultiDelta{std::move(response_errors)}}};
                }
            }
            new_deltas.emplace_back(zxid, typename Storage::SubDeltaEnd{});
            response_errors.push_back(Coordination::Error::ZOK);

            // manually add deltas so that the result of previous request in the transaction is used in the next request
            storage.uncommitted_state.addDeltas(std::move(new_deltas));
        }

        digest = current_digest;

        return {};
    }

    Coordination::ZooKeeperResponsePtr process(Storage & storage, int64_t zxid) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = this->zk_request->makeResponse();
        Coordination::ZooKeeperMultiResponse & response = dynamic_cast<Coordination::ZooKeeperMultiResponse &>(*response_ptr);

        auto & deltas = storage.uncommitted_state.deltas;
        // the deltas will have at least SubDeltaEnd or FailedMultiDelta
        chassert(!deltas.empty());
        if (auto * failed_multi = std::get_if<typename Storage::FailedMultiDelta>(&deltas.front().operation))
        {
            for (size_t i = 0; i < concrete_requests.size(); ++i)
            {
                response.responses[i] = std::make_shared<Coordination::ZooKeeperErrorResponse>();
                response.responses[i]->error = failed_multi->error_codes[i];
            }

            storage.uncommitted_state.commit(zxid);
            return response_ptr;
        }

        for (size_t i = 0; i < concrete_requests.size(); ++i)
        {
            response.responses[i] = concrete_requests[i]->process(storage, zxid);
            storage.uncommitted_state.commit(zxid);
        }

        response.error = Coordination::Error::ZOK;
        return response_ptr;
    }

    Coordination::ZooKeeperResponsePtr processLocal(Storage & storage, int64_t zxid) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperMultiReadRequest);
        Coordination::ZooKeeperResponsePtr response_ptr = this->zk_request->makeResponse();
        Coordination::ZooKeeperMultiResponse & response = dynamic_cast<Coordination::ZooKeeperMultiResponse &>(*response_ptr);

        for (size_t i = 0; i < concrete_requests.size(); ++i)
        {
            response.responses[i] = concrete_requests[i]->processLocal(storage, zxid);
        }

        response.error = Coordination::Error::ZOK;
        return response_ptr;
    }

    KeeperStorageBase::ResponsesForSessions
    processWatches(const Storage & storage, int64_t zxid, typename Storage::Watches & watches, typename Storage::Watches & list_watches) const override
    {
        typename Storage::ResponsesForSessions result;
        for (const auto & generic_request : concrete_requests)
        {
            auto responses = generic_request->processWatches(storage, zxid, watches, list_watches);
            result.insert(result.end(), responses.begin(), responses.end());
        }
        return result;
    }
};

template<typename Storage>
struct KeeperStorageCloseRequestProcessor final : public KeeperStorageRequestProcessor<Storage>
{
    using KeeperStorageRequestProcessor<Storage>::KeeperStorageRequestProcessor;
    Coordination::ZooKeeperResponsePtr process(Storage &, int64_t) const override
    {
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Called process on close request");
    }
};

template<typename Storage>
struct KeeperStorageAuthRequestProcessor final : public KeeperStorageRequestProcessor<Storage>
{
    using KeeperStorageRequestProcessor<Storage>::KeeperStorageRequestProcessor;

    std::vector<typename Storage::Delta>
    preprocess(Storage & storage, int64_t zxid, int64_t session_id, int64_t /*time*/, uint64_t & /*digest*/, const KeeperContext & /*keeper_context*/) const override
    {
        Coordination::ZooKeeperAuthRequest & auth_request = dynamic_cast<Coordination::ZooKeeperAuthRequest &>(*this->zk_request);
        Coordination::ZooKeeperResponsePtr response_ptr = this->zk_request->makeResponse();

        if (auth_request.scheme != "digest" || std::count(auth_request.data.begin(), auth_request.data.end(), ':') != 1)
            return {typename Storage::Delta{zxid, Coordination::Error::ZAUTHFAILED}};

        std::vector<typename Storage::Delta> new_deltas;
        auto auth_digest = Storage::generateDigest(auth_request.data);
        if (auth_digest == storage.superdigest)
        {
            typename Storage::AuthID auth{"super", ""};
            new_deltas.emplace_back(zxid, typename Storage::AddAuthDelta{session_id, std::move(auth)});
        }
        else
        {
            typename Storage::AuthID new_auth{auth_request.scheme, auth_digest};
            if (!storage.uncommitted_state.hasACL(session_id, false, [&](const auto & auth_id) { return new_auth == auth_id; }))
                new_deltas.emplace_back(zxid, typename Storage::AddAuthDelta{session_id, std::move(new_auth)});
        }

        return new_deltas;
    }

    Coordination::ZooKeeperResponsePtr process(Storage & storage, int64_t zxid) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = this->zk_request->makeResponse();
        Coordination::ZooKeeperAuthResponse & auth_response = dynamic_cast<Coordination::ZooKeeperAuthResponse &>(*response_ptr);

        if (const auto result = storage.commit(zxid); result != Coordination::Error::ZOK)
            auth_response.error = result;

        return response_ptr;
    }
};

template<typename Container>
void KeeperStorage<Container>::finalize()
{
    if (finalized)
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "KeeperStorage already finalized");

    finalized = true;

    ephemerals.clear();

    watches.clear();
    list_watches.clear();
    sessions_and_watchers.clear();
    session_expiry_queue.clear();
}

template<typename Container>
bool KeeperStorage<Container>::isFinalized() const
{
    return finalized;
}

template<typename Storage>
class KeeperStorageRequestProcessorsFactory final : private boost::noncopyable
{
public:
    using Creator = std::function<std::shared_ptr<KeeperStorageRequestProcessor<Storage>>(const Coordination::ZooKeeperRequestPtr &)>;
    using OpNumToRequest = std::unordered_map<Coordination::OpNum, Creator>;

    static KeeperStorageRequestProcessorsFactory<Storage> & instance()
    {
        static KeeperStorageRequestProcessorsFactory<Storage> factory;
        return factory;
    }

    std::shared_ptr<KeeperStorageRequestProcessor<Storage>> get(const Coordination::ZooKeeperRequestPtr & zk_request) const
    {
        auto request_it = op_num_to_request.find(zk_request->getOpNum());
        if (request_it == op_num_to_request.end())
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Unknown operation type {}", zk_request->getOpNum());

        return request_it->second(zk_request);
    }

    void registerRequest(Coordination::OpNum op_num, Creator creator)
    {
        if (!op_num_to_request.try_emplace(op_num, creator).second)
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Request with op num {} already registered", op_num);
    }

private:
    OpNumToRequest op_num_to_request;
    KeeperStorageRequestProcessorsFactory();
};

template <Coordination::OpNum num, typename RequestT, typename Factory>
void registerKeeperRequestProcessor(Factory & factory)
{
    factory.registerRequest(
        num, [](const Coordination::ZooKeeperRequestPtr & zk_request) { return std::make_shared<RequestT>(zk_request); });
}


template<typename Storage>
KeeperStorageRequestProcessorsFactory<Storage>::KeeperStorageRequestProcessorsFactory()
{
    registerKeeperRequestProcessor<Coordination::OpNum::Heartbeat, KeeperStorageHeartbeatRequestProcessor<Storage>>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::Sync, KeeperStorageSyncRequestProcessor<Storage>>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::Auth, KeeperStorageAuthRequestProcessor<Storage>>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::Close, KeeperStorageCloseRequestProcessor<Storage>>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::Create, KeeperStorageCreateRequestProcessor<Storage>>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::Remove, KeeperStorageRemoveRequestProcessor<Storage>>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::Exists, KeeperStorageExistsRequestProcessor<Storage>>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::Get, KeeperStorageGetRequestProcessor<Storage>>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::Set, KeeperStorageSetRequestProcessor<Storage>>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::List, KeeperStorageListRequestProcessor<Storage>>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::SimpleList, KeeperStorageListRequestProcessor<Storage>>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::FilteredList, KeeperStorageListRequestProcessor<Storage>>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::Check, KeeperStorageCheckRequestProcessor<Storage>>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::Multi, KeeperStorageMultiRequestProcessor<Storage>>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::MultiRead, KeeperStorageMultiRequestProcessor<Storage>>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::CreateIfNotExists, KeeperStorageCreateRequestProcessor<Storage>>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::SetACL, KeeperStorageSetACLRequestProcessor<Storage>>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::GetACL, KeeperStorageGetACLRequestProcessor<Storage>>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::CheckNotExists, KeeperStorageCheckRequestProcessor<Storage>>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::RemoveRecursive, KeeperStorageRemoveRecursiveRequestProcessor<Storage>>(*this);
}


template<typename Container>
UInt64 KeeperStorage<Container>::calculateNodesDigest(UInt64 current_digest, const std::vector<Delta> & new_deltas) const
{
    if (!keeper_context->digestEnabled())
        return current_digest;

    std::unordered_map<std::string_view, std::shared_ptr<Node>> updated_nodes;

    for (const auto & delta : new_deltas)
    {
        std::visit(
            Overloaded{
                [&](const CreateNodeDelta & create_delta)
                {
                    auto node = std::make_shared<Node>();
                    node->copyStats(create_delta.stat);
                    node->setData(create_delta.data);
                    updated_nodes.emplace(delta.path, node);
                },
                [&](const RemoveNodeDelta & /* remove_delta */)
                {
                    if (!updated_nodes.contains(delta.path))
                    {
                        auto old_digest = uncommitted_state.getNode(delta.path)->getDigest(delta.path);
                        current_digest -= old_digest;
                    }

                    updated_nodes.insert_or_assign(delta.path, nullptr);
                },
                [&](const UpdateNodeDelta & update_delta)
                {
                    std::shared_ptr<Node> node{nullptr};

                    auto updated_node_it = updated_nodes.find(delta.path);
                    if (updated_node_it == updated_nodes.end())
                    {
                        node = std::make_shared<Node>();
                        node->shallowCopy(*uncommitted_state.getNode(delta.path));
                        current_digest -= node->getDigest(delta.path);
                        updated_nodes.emplace(delta.path, node);
                    }
                    else
                        node = updated_node_it->second;

                    update_delta.update_fn(*node);
                },
                [](auto && /* delta */) {}},
            delta.operation);
    }

    for (const auto & [path, updated_node] : updated_nodes)
    {
        if (updated_node)
        {
            updated_node->invalidateDigestCache();
            current_digest += updated_node->getDigest(path);
        }
    }

    return current_digest;
}

template<typename Container>
void KeeperStorage<Container>::preprocessRequest(
    const Coordination::ZooKeeperRequestPtr & zk_request,
    int64_t session_id,
    int64_t time,
    int64_t new_last_zxid,
    bool check_acl,
    std::optional<Digest> digest,
    int64_t log_idx)
{
    Stopwatch watch;
    SCOPE_EXIT({
        auto elapsed = watch.elapsedMicroseconds();
        if (auto elapsed_ms = elapsed / 1000; elapsed_ms > keeper_context->getCoordinationSettings()->log_slow_cpu_threshold_ms)
        {
            LOG_INFO(
                getLogger("KeeperStorage"),
                "Preprocessing a request took too long ({}ms).\nRequest info: {}",
                elapsed_ms,
                zk_request->toString(/*short_format=*/true));
        }
        ProfileEvents::increment(ProfileEvents::KeeperPreprocessElapsedMicroseconds, elapsed);
    });

    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "KeeperStorage system nodes are not initialized");

    int64_t last_zxid = getNextZXID() - 1;

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
        if (last_zxid == new_last_zxid && digest && checkDigest(*digest, getNodesDigest(false)))
        {
            auto & last_transaction = uncommitted_transactions.back();
            // we found the preprocessed request with the same ZXID, we can get log_idx and skip preprocessing it
            chassert(last_transaction.zxid == new_last_zxid && log_idx != 0);
            /// initially leader preprocessed without knowing the log idx
            /// on the second call we have that information and can set the log idx for the correct transaction
            last_transaction.log_idx = log_idx;
            return;
        }

        if (new_last_zxid <= last_zxid)
            throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "Got new ZXID ({}) smaller or equal to current ZXID ({}). It's a bug",
                            new_last_zxid, last_zxid);
    }

    std::vector<Delta> new_deltas;
    TransactionInfo transaction{.zxid = new_last_zxid, .nodes_digest = {}, .log_idx = log_idx};
    uint64_t new_digest = getNodesDigest(false).value;
    SCOPE_EXIT({
        if (keeper_context->digestEnabled())
            // if the version of digest we got from the leader is the same as the one this instances has, we can simply copy the value
            // and just check the digest on the commit
            // a mistake can happen while applying the changes to the uncommitted_state so for now let's just recalculate the digest here also
            transaction.nodes_digest = Digest{CURRENT_DIGEST_VERSION, new_digest};
        else
            transaction.nodes_digest = Digest{DigestVersion::NO_DIGEST};

        uncommitted_transactions.emplace_back(transaction);
        uncommitted_state.addDeltas(std::move(new_deltas));
    });

    auto request_processor = KeeperStorageRequestProcessorsFactory<KeeperStorage<Container>>::instance().get(zk_request);

    if (zk_request->getOpNum() == Coordination::OpNum::Close) /// Close request is special
    {
        auto session_ephemerals = ephemerals.find(session_id);
        if (session_ephemerals != ephemerals.end())
        {
            for (const auto & ephemeral_path : session_ephemerals->second)
            {
                new_deltas.emplace_back
                (
                    parentNodePath(ephemeral_path).toString(),
                    new_last_zxid,
                    UpdateNodeDelta
                    {
                        [ephemeral_path](Node & parent)
                        {
                            ++parent.cversion;
                            parent.decreaseNumChildren();
                        }
                    }
                );

                new_deltas.emplace_back(ephemeral_path, transaction.zxid, RemoveNodeDelta{.ephemeral_owner = session_id});
            }

            ephemerals.erase(session_ephemerals);
        }

        new_deltas.emplace_back(transaction.zxid, CloseSessionDelta{session_id});
        new_digest = calculateNodesDigest(new_digest, new_deltas);
        return;
    }

    if (check_acl && !request_processor->checkAuth(*this, session_id, false))
    {
        uncommitted_state.deltas.emplace_back(new_last_zxid, Coordination::Error::ZNOAUTH);
        return;
    }

    new_deltas = request_processor->preprocess(*this, transaction.zxid, session_id, time, new_digest, *keeper_context);
}

template<typename Container>
KeeperStorage<Container>::ResponsesForSessions KeeperStorage<Container>::processRequest(
    const Coordination::ZooKeeperRequestPtr & zk_request,
    int64_t session_id,
    std::optional<int64_t> new_last_zxid,
    bool check_acl,
    bool is_local)
{
    Stopwatch watch;
    SCOPE_EXIT({
        auto elapsed = watch.elapsedMicroseconds();
        if (auto elapsed_ms = elapsed / 1000; elapsed_ms > keeper_context->getCoordinationSettings()->log_slow_cpu_threshold_ms)
        {
            LOG_INFO(
                getLogger("KeeperStorage"),
                "Processing a request took too long ({}ms).\nRequest info: {}",
                elapsed_ms,
                zk_request->toString(/*short_format=*/true));
        }
        ProfileEvents::increment(ProfileEvents::KeeperProcessElapsedMicroseconds, elapsed);
    });

    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "KeeperStorage system nodes are not initialized");

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
                uncommitted_transactions.front().zxid);

        zxid = *new_last_zxid;
        uncommitted_transactions.pop_front();
    }

    ResponsesForSessions results;

    /// ZooKeeper update sessions expirity for each request, not only for heartbeats
    session_expiry_queue.addNewSessionOrUpdate(session_id, session_and_timeout[session_id]);

    if (zk_request->getOpNum() == Coordination::OpNum::Close) /// Close request is special
    {
        commit(zxid);

        for (const auto & delta : uncommitted_state.deltas)
        {
            if (delta.zxid > zxid)
                break;

            if (std::holds_alternative<RemoveNodeDelta>(delta.operation))
            {
                auto responses = processWatchesImpl(delta.path, watches, list_watches, Coordination::Event::DELETED);
                results.insert(results.end(), responses.begin(), responses.end());
            }
        }

        clearDeadWatches(session_id);
        auto auth_it = session_and_auth.find(session_id);
        if (auth_it != session_and_auth.end())
            session_and_auth.erase(auth_it);

        /// Finish connection
        auto response = std::make_shared<Coordination::ZooKeeperCloseResponse>();
        response->xid = zk_request->xid;
        response->zxid = getZXID();
        session_expiry_queue.remove(session_id);
        session_and_timeout.erase(session_id);
        results.push_back(ResponseForSession{session_id, response});
    }
    else if (zk_request->getOpNum() == Coordination::OpNum::Heartbeat) /// Heartbeat request is also special
    {
        auto storage_request = KeeperStorageRequestProcessorsFactory<KeeperStorage<Container>>::instance().get(zk_request);
        auto response = storage_request->process(*this, zxid);
        response->xid = zk_request->xid;
        response->zxid = getZXID();

        results.push_back(ResponseForSession{session_id, response});
    }
    else /// normal requests proccession
    {
        auto request_processor = KeeperStorageRequestProcessorsFactory<KeeperStorage<Container>>::instance().get(zk_request);
        Coordination::ZooKeeperResponsePtr response;

        if (is_local)
        {
            chassert(zk_request->isReadRequest());
            if (check_acl && !request_processor->checkAuth(*this, session_id, true))
            {
                response = zk_request->makeResponse();
                /// Original ZooKeeper always throws no auth, even when user provided some credentials
                response->error = Coordination::Error::ZNOAUTH;
            }
            else
            {
                response = request_processor->processLocal(*this, zxid);
            }
        }
        else
        {
            response = request_processor->process(*this, zxid);
        }

        /// Watches for this requests are added to the watches lists
        if (zk_request->has_watch)
        {
            if (response->error == Coordination::Error::ZOK)
            {
                static constexpr std::array list_requests{
                    Coordination::OpNum::List, Coordination::OpNum::SimpleList, Coordination::OpNum::FilteredList};

                auto & watches_type = std::find(list_requests.begin(), list_requests.end(), zk_request->getOpNum()) != list_requests.end()
                    ? list_watches
                    : watches;

                auto add_watch_result = watches_type[zk_request->getPath()].emplace(session_id);
                if (add_watch_result.second)
                    sessions_and_watchers[session_id].emplace(zk_request->getPath());
            }
            else if (response->error == Coordination::Error::ZNONODE && zk_request->getOpNum() == Coordination::OpNum::Exists)
            {
                auto add_watch_result = watches[zk_request->getPath()].emplace(session_id);
                if (add_watch_result.second)
                    sessions_and_watchers[session_id].emplace(zk_request->getPath());
            }
        }

        /// If this requests processed successfully we need to check watches
        if (response->error == Coordination::Error::ZOK)
        {
            auto watch_responses = request_processor->processWatches(*this, zxid, watches, list_watches);
            results.insert(results.end(), watch_responses.begin(), watch_responses.end());
        }

        response->xid = zk_request->xid;
        response->zxid = getZXID();

        results.push_back(ResponseForSession{session_id, response});
    }

    uncommitted_state.commit(zxid);
    return results;
}

template<typename Container>
void KeeperStorage<Container>::rollbackRequest(int64_t rollback_zxid, bool allow_missing)
{
    if (allow_missing && (uncommitted_transactions.empty() || uncommitted_transactions.back().zxid < rollback_zxid))
        return;

    if (uncommitted_transactions.empty() || uncommitted_transactions.back().zxid != rollback_zxid)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Trying to rollback invalid ZXID ({}). It should be the last preprocessed.", rollback_zxid);
    }

    // if an exception occurs during rollback, the best option is to terminate because we can end up in an inconsistent state
    // we block memory tracking so we can avoid terminating if we're rollbacking because of memory limit
    LockMemoryExceptionInThread blocker{VariableContext::Global};
    try
    {
        uncommitted_transactions.pop_back();
        uncommitted_state.rollback(rollback_zxid);
    }
    catch (...)
    {
        LOG_FATAL(getLogger("KeeperStorage"), "Failed to rollback log. Terminating to avoid inconsistencies");
        std::terminate();
    }
}

template<typename Container>
KeeperStorageBase::Digest KeeperStorage<Container>::getNodesDigest(bool committed) const
{
    if (!keeper_context->digestEnabled())
        return {.version = DigestVersion::NO_DIGEST};

    if (committed || uncommitted_transactions.empty())
        return {CURRENT_DIGEST_VERSION, nodes_digest};

    return uncommitted_transactions.back().nodes_digest;
}

template<typename Container>
void KeeperStorage<Container>::removeDigest(const Node & node, const std::string_view path)
{
    if (keeper_context->digestEnabled())
        nodes_digest -= node.getDigest(path);
}

template<typename Container>
void KeeperStorage<Container>::addDigest(const Node & node, const std::string_view path)
{
    if (keeper_context->digestEnabled())
    {
        node.invalidateDigestCache();
        nodes_digest += node.getDigest(path);
    }
}

template<typename Container>
void KeeperStorage<Container>::clearDeadWatches(int64_t session_id)
{
    /// Clear all watches for this session
    auto watches_it = sessions_and_watchers.find(session_id);
    if (watches_it != sessions_and_watchers.end())
    {
        for (const auto & watch_path : watches_it->second)
        {
            /// Maybe it's a normal watch
            auto watch = watches.find(watch_path);
            if (watch != watches.end())
            {
                auto & watches_for_path = watch->second;
                watches_for_path.erase(session_id);
                if (watches_for_path.empty())
                    watches.erase(watch);
            }

            /// Maybe it's a list watch
            auto list_watch = list_watches.find(watch_path);
            if (list_watch != list_watches.end())
            {
                auto & list_watches_for_path = list_watch->second;
                list_watches_for_path.erase(session_id);
                if (list_watches_for_path.empty())
                    list_watches.erase(list_watch);
            }
        }

        sessions_and_watchers.erase(watches_it);
    }
}

template<typename Container>
void KeeperStorage<Container>::dumpWatches(WriteBufferFromOwnString & buf) const
{
    for (const auto & [session_id, watches_paths] : sessions_and_watchers)
    {
        buf << "0x" << getHexUIntLowercase(session_id) << "\n";
        for (const String & path : watches_paths)
            buf << "\t" << path << "\n";
    }
}

template<typename Container>
void KeeperStorage<Container>::dumpWatchesByPath(WriteBufferFromOwnString & buf) const
{
    auto write_int_container = [&buf](const auto & session_ids)
    {
        for (int64_t session_id : session_ids)
        {
            buf << "\t0x" << getHexUIntLowercase(session_id) << "\n";
        }
    };

    for (const auto & [watch_path, sessions] : watches)
    {
        buf << watch_path << "\n";
        write_int_container(sessions);
    }

    for (const auto & [watch_path, sessions] : list_watches)
    {
        buf << watch_path << "\n";
        write_int_container(sessions);
    }
}

template<typename Container>
void KeeperStorage<Container>::dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const
{
    auto write_str_set = [&buf](const std::unordered_set<String> & ephemeral_paths)
    {
        for (const String & path : ephemeral_paths)
        {
            buf << "\t" << path << "\n";
        }
    };

    buf << "Sessions dump (" << session_and_timeout.size() << "):\n";

    for (const auto & [session_id, _] : session_and_timeout)
    {
        buf << "0x" << getHexUIntLowercase(session_id) << "\n";
    }

    buf << "Sessions with Ephemerals (" << getSessionWithEphemeralNodesCount() << "):\n";
    for (const auto & [session_id, ephemeral_paths] : ephemerals)
    {
        buf << "0x" << getHexUIntLowercase(session_id) << "\n";
        write_str_set(ephemeral_paths);
    }
}

template<typename Container>
uint64_t KeeperStorage<Container>::getTotalWatchesCount() const
{
    uint64_t ret = 0;
    for (const auto & [session, paths] : sessions_and_watchers)
        ret += paths.size();

    return ret;
}

template<typename Container>
uint64_t KeeperStorage<Container>::getSessionsWithWatchesCount() const
{
    return sessions_and_watchers.size();
}

template<typename Container>
uint64_t KeeperStorage<Container>::getTotalEphemeralNodesCount() const
{
    uint64_t ret = 0;
    for (const auto & [session_id, nodes] : ephemerals)
        ret += nodes.size();

    return ret;
}

template<typename Container>
void KeeperStorage<Container>::recalculateStats()
{
    container.recalculateDataSize();
}

bool KeeperStorageBase::checkDigest(const Digest & first, const Digest & second)
{
    if (first.version != second.version)
        return true;

    if (first.version == DigestVersion::NO_DIGEST)
        return true;

    return first.value == second.value;
}

template<typename Container>
String KeeperStorage<Container>::generateDigest(const String & userdata)
{
    std::vector<String> user_password;
    boost::split(user_password, userdata, [](char character) { return character == ':'; });
    return user_password[0] + ":" + base64Encode(getSHA1(userdata));
}

template class KeeperStorage<SnapshotableHashTable<KeeperMemNode>>;
#if USE_ROCKSDB
template class KeeperStorage<RocksDBContainer<KeeperRocksNode>>;
#endif

}

// NOLINTEND(clang-analyzer-optin.core.EnumCastOutOfRange)
