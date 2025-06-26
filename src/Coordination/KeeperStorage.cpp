/// NOLINTBEGIN(clang-analyzer-optin.core.EnumCastOutOfRange)

#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <boost/algorithm/string.hpp>
#include <Poco/SHA1Engine.h>

#include <Common/Base64.h>
#include <Common/FailPoint.h>
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
#include <Common/StringHashForHeterogeneousLookup.h>

#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperConstants.h>
#include <Coordination/KeeperDispatcher.h>
#include <Coordination/KeeperReconfiguration.h>
#include <Coordination/KeeperStorage.h>

#include <shared_mutex>
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

namespace CoordinationSetting
{
    extern const CoordinationSettingsUInt64 log_slow_cpu_threshold_ms;
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

void unregisterEphemeralPath(KeeperStorageBase::Ephemerals & ephemerals, int64_t session_id, const std::string & path, bool throw_if_missing)
{
    auto ephemerals_it = ephemerals.find(session_id);
    if (ephemerals_it == ephemerals.end())
    {
        if (throw_if_missing)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Session {} is expected to have ephemeral paths but no path is registered", session_id);

        return;
    }

    if (auto erased = ephemerals_it->second.erase(path); !erased && throw_if_missing)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Session {} is missing ephemeral path {}", session_id, path);

    if (ephemerals_it->second.empty())
        ephemerals.erase(ephemerals_it);
}

KeeperResponsesForSessions processWatchesImpl(
    const String & path,
    KeeperStorageBase::Watches & watches,
    KeeperStorageBase::Watches & list_watches,
    KeeperStorageBase::SessionAndWatcher & sessions_and_watchers,
    Coordination::Event event_type)
{
    KeeperResponsesForSessions result;
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
        {
            [[maybe_unused]] auto erased = sessions_and_watchers[watcher_session].erase(
                KeeperStorageBase::WatchInfo{.path = path, .is_list_watch = false});
            chassert(erased);
            result.push_back(KeeperResponseForSession{watcher_session, watch_response});
        }

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
    /// CHANGED event never trigger list watches

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
            {
                [[maybe_unused]] auto erased = sessions_and_watchers[watcher_session].erase(
                    KeeperStorageBase::WatchInfo{.path = path_to_check, .is_list_watch = true});
                chassert(erased);
                result.push_back(KeeperResponseForSession{watcher_session, watch_list_response});
            }

            list_watches.erase(watch_it);
        }
    }
    return result;
}

// When this function is updated, update KEEPER_CURRENT_DIGEST_VERSION!!
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

    hash.update(node.stats.czxid);
    hash.update(node.stats.mzxid);
    hash.update(node.stats.ctime());
    hash.update(node.stats.mtime);
    hash.update(node.stats.version);
    hash.update(node.stats.cversion);
    hash.update(node.stats.aversion);
    hash.update(node.stats.ephemeralOwner());
    hash.update(node.stats.numChildren());
    hash.update(node.stats.pzxid);

    auto digest = hash.get64();

    /// 0 means no cached digest
    if (digest == 0)
        return 1;

    return digest;
}

}

void NodeStats::copyStats(const Coordination::Stat & stat)
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
        setNumChildren(stat.numChildren);
    else
        setEphemeralOwner(stat.ephemeralOwner);
}

void KeeperRocksNodeInfo::copyStats(const Coordination::Stat & stat)
{
    stats.copyStats(stat);
}

void KeeperRocksNode::invalidateDigestCache() const
{
    if (serialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "We modify node after serialized it");
    cached_digest = 0;
}

UInt64 KeeperRocksNode::getDigest(std::string_view path) const
{
    if (!cached_digest)
        cached_digest = calculateDigest(path, *this);
    return cached_digest;
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
    readVarUInt(stats.data_size, buffer);
    if (stats.data_size)
    {
        data = std::unique_ptr<char[]>(new char[stats.data_size]);
        buffer.readStrict(data.get(), stats.data_size);
    }
}

void KeeperRocksNode::setResponseStat(Coordination::Stat & response_stat) const
{
    response_stat.czxid = stats.czxid;
    response_stat.mzxid = stats.mzxid;
    response_stat.ctime = stats.ctime();
    response_stat.mtime = stats.mtime;
    response_stat.version = stats.version;
    response_stat.cversion = stats.cversion;
    response_stat.aversion = stats.aversion;
    response_stat.ephemeralOwner = stats.ephemeralOwner();
    response_stat.dataLength = static_cast<int32_t>(stats.data_size);
    response_stat.numChildren = stats.numChildren();
    response_stat.pzxid = stats.pzxid;
}

KeeperMemNode & KeeperMemNode::operator=(const KeeperMemNode & other)
{
    if (this == &other)
        return *this;

    stats = other.stats;
    acl_id = other.acl_id;

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
    acl_id = other.acl_id;

    data = std::move(other.data);

    other.stats.data_size = 0;

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
    return stats.data_size == 0 && stats.mzxid == 0;
}

void KeeperMemNode::copyStats(const Coordination::Stat & stat)
{
    stats.copyStats(stat);
}

void KeeperMemNode::setResponseStat(Coordination::Stat & response_stat) const
{
    response_stat.czxid = stats.czxid;
    response_stat.mzxid = stats.mzxid;
    response_stat.ctime = stats.ctime();
    response_stat.mtime = stats.mtime;
    response_stat.version = stats.version;
    response_stat.cversion = stats.cversion;
    response_stat.aversion = stats.aversion;
    response_stat.ephemeralOwner = stats.ephemeralOwner();
    response_stat.dataLength = static_cast<int32_t>(stats.data_size);
    response_stat.numChildren = stats.numChildren();
    response_stat.pzxid = stats.pzxid;
}

uint64_t KeeperMemNode::sizeInBytes() const
{
    return sizeof(KeeperMemNode) + children.size() * sizeof(StringRef) + stats.data_size;
}

void KeeperMemNode::setData(const String & new_data)
{
    stats.data_size = static_cast<uint32_t>(new_data.size());
    if (stats.data_size != 0)
    {
        data = std::unique_ptr<char[]>(new char[stats.data_size]);
        memcpy(data.get(), new_data.data(), stats.data_size);
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
    stats = other.stats;
    acl_id = other.acl_id;
    if (stats.data_size != 0)
    {
        data = std::unique_ptr<char[]>(new char[stats.data_size]);
        memcpy(data.get(), other.data.get(), stats.data_size);
    }

    cached_digest = other.cached_digest;
}

struct CreateNodeDelta
{
    Coordination::Stat stat;
    Coordination::ACLs acls;
    String data;
};

struct RemoveNodeDelta
{
    int32_t version{-1};
    NodeStats stat;
    Coordination::ACLs acls;
    String data;
};

struct UpdateNodeStatDelta
{
    template <is_any_of<KeeperMemNode, KeeperRocksNode> Node>
    explicit UpdateNodeStatDelta(const Node & node)
        : old_stats(node.stats)
        , new_stats(node.stats)
    {}

    NodeStats old_stats;
    NodeStats new_stats;
    int32_t version{-1};
};

struct UpdateNodeDataDelta
{

    std::string old_data;
    std::string new_data;
    int32_t version{-1};
};

struct SetACLDelta
{
    Coordination::ACLs old_acls;
    Coordination::ACLs new_acls;
    int32_t version{-1};
};

struct ErrorDelta
{
    Coordination::Error error;
};

struct FailedMultiDelta
{
    std::vector<Coordination::Error> error_codes;
    Coordination::Error global_error{Coordination::Error::ZOK};
};

// Denotes end of a subrequest in multi request
struct SubDeltaEnd
{
};

struct AddAuthDelta
{
    int64_t session_id;
    std::shared_ptr<KeeperStorageBase::AuthID> auth_id;
};

struct CloseSessionDelta
{
    int64_t session_id;
};

using Operation = std::variant<
    CreateNodeDelta,
    RemoveNodeDelta,
    UpdateNodeStatDelta,
    UpdateNodeDataDelta,
    SetACLDelta,
    AddAuthDelta,
    ErrorDelta,
    SubDeltaEnd,
    FailedMultiDelta,
    CloseSessionDelta>;

struct KeeperStorageBase::Delta
{
    Delta(String path_, int64_t zxid_, Operation operation_) : path(std::move(path_)), zxid(zxid_), operation(std::move(operation_)) { }

    Delta(int64_t zxid_, Coordination::Error error) : Delta("", zxid_, ErrorDelta{error}) { }

    Delta(int64_t zxid_, Operation subdelta) : Delta("", zxid_, subdelta) { }

    String path;
    int64_t zxid;
    Operation operation;
};

KeeperStorageBase::DeltaIterator KeeperStorageBase::DeltaRange::begin() const
{
    return begin_it;
}

KeeperStorageBase::DeltaIterator KeeperStorageBase::DeltaRange::end() const
{
    return end_it;
}

bool KeeperStorageBase::DeltaRange::empty() const
{
    return begin_it == end_it;
}

const KeeperStorageBase::Delta & KeeperStorageBase::DeltaRange::front() const
{
    return *begin_it;
}

KeeperStorageBase::KeeperStorageBase(int64_t tick_time_ms, const KeeperContextPtr & keeper_context_, const String & superdigest_)
    : keeper_context(keeper_context_), superdigest(superdigest_), session_expiry_queue(tick_time_ms)
{}

template <typename Container>
KeeperStorage<Container>::KeeperStorage(
    int64_t tick_time_ms, const String & superdigest_, const KeeperContextPtr & keeper_context_, const bool initialize_system_nodes)
    : KeeperStorageBase(tick_time_ms, keeper_context_, superdigest_)
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
                node.stats.increaseNumChildren();
                if constexpr (!use_rocksdb)
                {
                    node.addChild(getBaseNodeName(keeper_system_path));
                    node.invalidateDigestCache();
                }
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

    updateStats();
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
Overloaded(Ts...) -> Overloaded<Ts...>;  /// NOLINT(misc-use-internal-linkage)

template<typename Container>
std::shared_ptr<typename Container::Node> KeeperStorage<Container>::UncommittedState::tryGetNodeFromStorage(StringRef path, bool should_lock_storage) const
{
    std::shared_lock lock(storage.storage_mutex, std::defer_lock);
    if (should_lock_storage)
        lock.lock();
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
void KeeperStorage<Container>::UncommittedState::UncommittedNode::materializeACL(const ACLMap & current_acl_map)
{
    if (!acls.has_value())
        acls.emplace(current_acl_map.convertNumber(node->acl_id));
}

template<typename Container>
void KeeperStorage<Container>::UncommittedState::applyDelta(const Delta & delta, uint64_t * digest)
{
    chassert(!delta.path.empty());
    UncommittedNode * uncommitted_node = nullptr;

    auto node_it = nodes.end();
    if (auto it = nodes.find(delta.path); it != nodes.end())
    {
        uncommitted_node = &it->second;
        node_it = it;
    }
    else
    {
        if (auto storage_node = tryGetNodeFromStorage(delta.path))
        {
            auto [emplaced_it, _] = nodes.emplace(delta.path, UncommittedNode{.node = std::move(storage_node)});
            node_it = emplaced_it;
            zxid_to_nodes[0].insert(emplaced_it);
            uncommitted_node = &emplaced_it->second;
        }
        else
        {
            auto [emplaced_it, _] = nodes.emplace(delta.path, UncommittedNode{.node = nullptr});
            node_it = emplaced_it;
            zxid_to_nodes[0].insert(emplaced_it);
            uncommitted_node = &emplaced_it->second;
        }
    }

    /// if it's the first time we see that node in the transaction
    /// we need to subtract it's digest from the point before
    /// we started the transaction
    /// at the end of transaction, we add new node digests in updateNodesDigest
    std::visit(
        [&]<typename DeltaType>(const DeltaType & operation)
        {
            auto & [node, acls, applied_zxids] = *uncommitted_node;

            if constexpr (std::same_as<DeltaType, CreateNodeDelta>)
            {
                chassert(!node);
                node = std::make_shared<Node>();
                node->copyStats(operation.stat);
                node->setData(operation.data);
                acls = operation.acls;
            }
            else if constexpr (std::same_as<DeltaType, RemoveNodeDelta>)
            {
                if (digest && !zxid_to_nodes[delta.zxid].contains(node_it))
                    *digest -= node->getDigest(delta.path);

                chassert(node);
                node = nullptr;
            }
            else if constexpr (std::same_as<DeltaType, UpdateNodeStatDelta>)
            {
                if (digest && !zxid_to_nodes[delta.zxid].contains(node_it))
                    *digest -= node->getDigest(delta.path);

                chassert(node);
                node->invalidateDigestCache();
                node->stats = operation.new_stats;
            }
            else if constexpr (std::same_as<DeltaType, UpdateNodeDataDelta>)
            {
                if (digest && !zxid_to_nodes[delta.zxid].contains(node_it))
                    *digest -= node->getDigest(delta.path);

                chassert(node);
                node->invalidateDigestCache();
                node->setData(operation.new_data);
            }
            else if constexpr (std::same_as<DeltaType, SetACLDelta>)
            {
                acls = operation.new_acls;
            }

            applied_zxids.insert(delta.zxid);
            zxid_to_nodes[delta.zxid].insert(node_it);
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
            using TAuth = std::remove_cvref_t<decltype(auth)>;

            const AuthID * auth_ptr = nullptr;
            if constexpr (std::same_as<TAuth, AuthID>)
                auth_ptr = &auth;
            else
                auth_ptr = auth.second.get();

            if (predicate(*auth_ptr))
                return true;
        }
        return false;
    };

    if (is_local)
    {
        std::shared_lock lock(storage.auth_mutex);
        return check_auth(storage.committed_session_and_auth[session_id]);
    }

    /// we want to close the session and with that we will remove all the auth related to the session
    if (closed_sessions.contains(session_id))
        return false;

    std::shared_lock lock(storage.auth_mutex);
    if (check_auth(storage.committed_session_and_auth[session_id]))
        return true;

    // check if there are uncommitted
    const auto auth_it = session_and_auth.find(session_id);
    if (auth_it == session_and_auth.end())
        return false;

    if (check_auth(auth_it->second))
        return true;

    return check_auth(storage.committed_session_and_auth[session_id]);
}

template<typename Container>
void KeeperStorage<Container>::UncommittedState::rollbackDelta(const Delta & delta)
{
    chassert(!delta.path.empty());

    std::visit(
        [&]<typename DeltaType>(const DeltaType & operation)
        {
            auto & [node, acls, applied_zxids] = nodes.at(delta.path);

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
                acls = operation.acls;
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
            else if constexpr (std::same_as<DeltaType, SetACLDelta>)
            {
                acls = operation.old_acls;
            }

            applied_zxids.erase(delta.zxid);
            zxid_to_nodes.erase(delta.zxid);
        },
        delta.operation);
}

template <typename Container>
UInt64 KeeperStorage<Container>::UncommittedState::updateNodesDigest(UInt64 current_digest, UInt64 zxid) const
{
    if (!storage.keeper_context->digestEnabled())
        return current_digest;

    auto nodes_it = zxid_to_nodes.find(zxid);
    if (nodes_it == zxid_to_nodes.end())
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

template<typename Container>
void KeeperStorage<Container>::UncommittedState::applyDeltas(const std::list<Delta> & new_deltas, uint64_t * digest)
{
    for (const auto & delta : new_deltas)
    {
        if (!delta.path.empty())
        {
            applyDelta(delta, digest);
        }
        else if (const auto * auth_delta = std::get_if<AddAuthDelta>(&delta.operation))
        {
            auto & uncommitted_auth = session_and_auth[auth_delta->session_id];
            uncommitted_auth.push_back(std::pair{delta.zxid, auth_delta->auth_id});
        }
        else if (const auto * close_session_delta = std::get_if<CloseSessionDelta>(&delta.operation))
        {
            closed_sessions.insert(close_session_delta->session_id);
        }
    }
}

template<typename Container>
KeeperStorage<Container>::UncommittedState::~UncommittedState() = default;

template<typename Container>
void KeeperStorage<Container>::UncommittedState::addDeltas(std::list<Delta> new_deltas)
{
    std::lock_guard lock(deltas_mutex);
    deltas.splice(deltas.end(), std::move(new_deltas));
}

template<typename Container>
void KeeperStorage<Container>::UncommittedState::cleanup(int64_t commit_zxid)
{
    for (auto it = zxid_to_nodes.begin(); it != zxid_to_nodes.end(); it = zxid_to_nodes.erase(it))
    {
        const auto & [transaction_zxid, transaction_nodes] = *it;

        if (transaction_zxid > commit_zxid)
            break;

        for (const auto node_it : transaction_nodes)
        {
            node_it->second.applied_zxids.erase(transaction_zxid);
            if (node_it->second.applied_zxids.empty())
                nodes.erase(node_it);
        }
    }

    for (auto it = session_and_auth.begin(); it != session_and_auth.end();)
    {
        auto & auths = it->second;
        std::erase_if(auths, [commit_zxid](auto auth_pair) { return auth_pair.first <= commit_zxid; });
        if (auths.empty())
            it = session_and_auth.erase(it);
        else
            ++it;
    }
}

template<typename Container>
void KeeperStorage<Container>::UncommittedState::rollback(int64_t rollback_zxid)
{
    // we can only rollback the last zxid (if there is any)
    std::list<Delta> rollback_deltas;
    {
        std::lock_guard lock(deltas_mutex);
        if (!deltas.empty() && deltas.back().zxid > rollback_zxid)
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Invalid state of deltas found while trying to rollback request. Last ZXID ({}) is larger than the requested ZXID ({})",
                deltas.back().zxid,
                rollback_zxid);

        auto delta_it = deltas.rbegin();
        for (; delta_it != deltas.rend(); ++delta_it)
        {
            if (delta_it->zxid != rollback_zxid)
                break;
        }

        if (delta_it == deltas.rend())
            rollback_deltas = std::move(deltas);
        else
            rollback_deltas.splice(rollback_deltas.end(), deltas, delta_it.base(), deltas.end());
    }

    rollback(std::move(rollback_deltas));
}

template<typename Container>
void KeeperStorage<Container>::UncommittedState::rollback(std::list<Delta> rollback_deltas)
{
    // we need to undo ephemeral mapping modifications
    // CreateNodeDelta added ephemeral for session id -> we need to remove it
    // RemoveNodeDelta removed ephemeral for session id -> we need to add it back
    for (auto delta_it = rollback_deltas.rbegin(); delta_it != rollback_deltas.rend(); ++delta_it)
    {
        const auto & delta = *delta_it;
        if (!delta.path.empty())
        {
            std::visit(
                [&]<typename DeltaType>(const DeltaType & operation)
                {
                    if constexpr (std::same_as<DeltaType, CreateNodeDelta>)
                    {
                        if (operation.stat.ephemeralOwner != 0)
                            unregisterEphemeralPath(storage.uncommitted_state.ephemerals, operation.stat.ephemeralOwner, delta.path, /*throw_if_missing=*/false);
                    }
                    else if constexpr (std::same_as<DeltaType, RemoveNodeDelta>)
                    {
                        if (operation.stat.ephemeralOwner() != 0)
                            storage.uncommitted_state.ephemerals[operation.stat.ephemeralOwner()].emplace(delta.path);
                    }
                },
                delta.operation);

            rollbackDelta(delta);
        }
        else if (const auto * add_auth = std::get_if<AddAuthDelta>(&delta.operation))
        {
            auto & uncommitted_auth = session_and_auth[add_auth->session_id];
            if (uncommitted_auth.back().second == add_auth->auth_id)
            {
                uncommitted_auth.pop_back();
                if (uncommitted_auth.empty())
                    session_and_auth.erase(add_auth->session_id);
            }
        }
        else if (const auto * close_session = std::get_if<CloseSessionDelta>(&delta.operation))
        {
           closed_sessions.erase(close_session->session_id);
        }
    }
}

template<typename Container>
std::shared_ptr<typename Container::Node> KeeperStorage<Container>::UncommittedState::getNode(StringRef path, bool should_lock_storage) const
{
    if (auto node_it = nodes.find(path.toView()); node_it != nodes.end())
        return node_it->second.node;

    std::shared_ptr<KeeperStorage::Node> node = tryGetNodeFromStorage(path, should_lock_storage);

    if (node)
    {
        auto [node_it, _] = nodes.emplace(std::string{path}, UncommittedNode{.node = node});
        zxid_to_nodes[0].insert(node_it);
    }

    return node;
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
    {
        node_it->second.materializeACL(storage.acl_map);
        return *node_it->second.acls;
    }

    std::shared_ptr<KeeperStorage::Node> node = tryGetNodeFromStorage(path);

    if (node)
    {
        auto [it, inserted] = nodes.emplace(std::string{path}, UncommittedNode{.node = node});
        zxid_to_nodes[0].insert(it);
        it->second.acls = storage.acl_map.convertNumber(node->acl_id);
        return *it->second.acls;
    }

    return {};
}

template<typename Container>
void KeeperStorage<Container>::UncommittedState::forEachAuthInSession(int64_t session_id, std::function<void(const AuthID &)> func) const
{
    const auto call_for_each_auth = [&func](const auto & auth_ids)
    {
        for (const auto & auth : auth_ids)
        {
            using TAuth = std::remove_cvref_t<decltype(auth)>;

            const AuthID * auth_ptr = nullptr;
            if constexpr (std::same_as<TAuth, AuthID>)
                auth_ptr = &auth;
            else
                auth_ptr = auth.second.get();

            if (!auth_ptr->scheme.empty())
                func(*auth_ptr);
        }
    };

    /// both committed and uncommitted need to be under the lock to avoid fetching the same AuthID from both committed and uncommitted state
    std::shared_lock lock(storage.auth_mutex);
    // for committed
    if (auto auth_it = storage.committed_session_and_auth.find(session_id); auth_it != storage.committed_session_and_auth.end())
        call_for_each_auth(auth_it->second);

    // for uncommitted
    if (auto auth_it = session_and_auth.find(session_id); auth_it != session_and_auth.end())
        call_for_each_auth(auth_it->second);
}

namespace
{

[[noreturn]] void onStorageInconsistency(std::string_view message)
{
    LOG_ERROR(
        getLogger("KeeperStorage"),
        "Inconsistency found between uncommitted and committed data ({}). Keeper will terminate to avoid undefined behaviour.", message);
    std::terminate();
}

}

/// Get current committed zxid
int64_t KeeperStorageBase::getZXID() const
{
    std::lock_guard lock(transaction_mutex);
    return zxid;
}

int64_t KeeperStorageBase::getNextZXIDLocked() const
{
    if (uncommitted_transactions.empty())
        return zxid + 1;

    return uncommitted_transactions.back().zxid + 1;
}

int64_t KeeperStorageBase::getNextZXID() const
{
    std::lock_guard lock(transaction_mutex);
    return getNextZXIDLocked();
}

template<typename Container>
void KeeperStorage<Container>::applyUncommittedState(KeeperStorage & other, int64_t last_log_idx)  TSA_NO_THREAD_SAFETY_ANALYSIS
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

        other.uncommitted_state.applyDelta(*it, /*digest=*/nullptr);
        other.uncommitted_state.deltas.push_back(*it);
    }
}

template<typename Container>
Coordination::Error KeeperStorage<Container>::commit(KeeperStorageBase::DeltaRange deltas)
{
    auto digest_on_commit = keeper_context->digestEnabled() && keeper_context->digestEnabledOnCommit();
    for (const auto & delta : deltas)
    {
        auto result = std::visit(
            [&, &path = delta.path]<typename DeltaType>(const DeltaType & operation) -> Coordination::Error
            {
                if constexpr (std::same_as<DeltaType, CreateNodeDelta>)
                {
                    if (!createNode(path, operation.data, operation.stat, operation.acls, digest_on_commit))
                        onStorageInconsistency("Failed to create a node");

                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, UpdateNodeStatDelta> || std::same_as<DeltaType, UpdateNodeDataDelta>)
                {
                    auto node_it = container.find(path);
                    if (node_it == container.end())
                        onStorageInconsistency("Node to be updated is missing");

                    if (operation.version != -1 && operation.version != node_it->value.stats.version)
                        onStorageInconsistency("Node to be updated has invalid version");

                    if constexpr (!use_rocksdb)
                    {
                        if (digest_on_commit)
                            removeDigest(node_it->value, path);
                    }

                    auto updated_node = container.updateValue(path, [&](auto & node)
                    {
                        if constexpr (std::same_as<DeltaType, UpdateNodeStatDelta>)
                            node.stats = operation.new_stats;
                        else
                            node.setData(std::move(operation.new_data));

                        if constexpr (!use_rocksdb)
                            node.invalidateDigestCache();
                    });

                    if constexpr (!use_rocksdb)
                    {
                        if (digest_on_commit)
                            addDigest(updated_node->value, path);
                    }

                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, RemoveNodeDelta>)
                {
                    if (!removeNode(path, operation.version, digest_on_commit))
                        onStorageInconsistency("Failed to remove node");

                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, SetACLDelta>)
                {
                    auto node_it = container.find(path);
                    if (node_it == container.end())
                        onStorageInconsistency("Failed to set ACL because node is missing");

                    if (operation.version != -1 && operation.version != node_it->value.stats.aversion)
                        onStorageInconsistency("Failed to set ACL because version of the node is invalid");

                    acl_map.removeUsage(node_it->value.acl_id);

                    uint64_t acl_id = acl_map.convertACLs(operation.new_acls);
                    acl_map.addUsage(acl_id);

                    container.updateValue(path, [acl_id](Node & node) { node.acl_id = acl_id; });

                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, ErrorDelta>)
                    return operation.error;
                else if constexpr (std::same_as<DeltaType, SubDeltaEnd>)
                {
                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, AddAuthDelta>)
                {
                    std::lock_guard auth_lock{auth_mutex};
                    committed_session_and_auth[operation.session_id].emplace_back(std::move(*operation.auth_id));
                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, CloseSessionDelta>)
                {
                    return Coordination::Error::ZOK;
                }
                else
                {
                    // shouldn't be called in any process functions
                    onStorageInconsistency("Invalid delta operation");
                }
            },
            delta.operation);

        if (result != Coordination::Error::ZOK)
            return result;
    }

    return Coordination::Error::ZOK;
}

template <typename Container>
bool KeeperStorage<Container>::createNode(
    const std::string & path, String data, const Coordination::Stat & stat, Coordination::ACLs node_acls, bool update_digest)
{
    auto parent_path = parentNodePath(path);
    auto node_it = container.find(parent_path);

    if (node_it == container.end())
        return false;

    if (node_it->value.stats.isEphemeral())
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
                    chassert(parent.stats.numChildren() == static_cast<int32_t>(parent.getChildren().size()));
                }
        );

        if (update_digest)
            addDigest(map_key->getMapped()->value, map_key->getKey().toView());
    }

    if (stat.ephemeralOwner != 0)
    {
        ++committed_ephemeral_nodes;
        std::lock_guard lock(ephemeral_mutex);
        committed_ephemerals[stat.ephemeralOwner].emplace(path);
    }

    return true;
};

template<typename Container>
bool KeeperStorage<Container>::removeNode(const std::string & path, int32_t version, bool update_digest)
{
    auto node_it = container.find(path);
    if (node_it == container.end())
        return false;

    if (version != -1 && version != node_it->value.stats.version)
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
            }
        );

        container.erase(path);

        if (update_digest)
            removeDigest(prev_node, path);
    }

    if (prev_node.stats.ephemeralOwner() != 0)
    {
        chassert(committed_ephemeral_nodes != 0);
        --committed_ephemeral_nodes;
        std::lock_guard lock(ephemeral_mutex);
        unregisterEphemeralPath(committed_ephemerals, prev_node.stats.ephemeralOwner(), path, /*throw_if_missing=*/true);
    }

    return true;
}

template <typename F>
auto callOnConcreteRequestType(const Coordination::ZooKeeperRequest & zk_request, F function)
{
    switch (zk_request.getOpNum())
    {
        case Coordination::OpNum::Heartbeat:
            return function(static_cast<const Coordination::ZooKeeperHeartbeatRequest &>(zk_request));
        case Coordination::OpNum::Sync:
            return function(static_cast<const Coordination::ZooKeeperSyncRequest &>(zk_request));
        case Coordination::OpNum::Get:
            return function(static_cast<const Coordination::ZooKeeperGetRequest &>(zk_request));
        case Coordination::OpNum::Create:
        case Coordination::OpNum::CreateIfNotExists:
            return function(static_cast<const Coordination::ZooKeeperCreateRequest &>(zk_request));
        case Coordination::OpNum::Remove:
            return function(static_cast<const Coordination::ZooKeeperRemoveRequest &>(zk_request));
        case Coordination::OpNum::RemoveRecursive:
            return function(static_cast<const Coordination::ZooKeeperRemoveRecursiveRequest &>(zk_request));
        case Coordination::OpNum::Exists:
            return function(static_cast<const Coordination::ZooKeeperExistsRequest &>(zk_request));
        case Coordination::OpNum::Set:
            return function(static_cast<const Coordination::ZooKeeperSetRequest &>(zk_request));
        case Coordination::OpNum::List:
        case Coordination::OpNum::FilteredList:
        case Coordination::OpNum::SimpleList:
            return function(static_cast<const Coordination::ZooKeeperListRequest &>(zk_request));
        case Coordination::OpNum::Check:
        case Coordination::OpNum::CheckNotExists:
            return function(static_cast<const Coordination::ZooKeeperCheckRequest &>(zk_request));
        case Coordination::OpNum::Multi:
        case Coordination::OpNum::MultiRead:
            return function(static_cast<const Coordination::ZooKeeperMultiRequest &>(zk_request));
        case Coordination::OpNum::Auth:
            return function(static_cast<const Coordination::ZooKeeperAuthRequest &>(zk_request));
        case Coordination::OpNum::Close:
            return function(static_cast<const Coordination::ZooKeeperCloseRequest &>(zk_request));
        case Coordination::OpNum::SetACL:
            return function(static_cast<const Coordination::ZooKeeperSetACLRequest &>(zk_request));
        case Coordination::OpNum::GetACL:
            return function(static_cast<const Coordination::ZooKeeperGetACLRequest &>(zk_request));
        default:
            throw Exception{DB::ErrorCodes::LOGICAL_ERROR, "Unexpected request type: {}", zk_request.getOpNum()};
    }
}

namespace
{

template<typename Storage>
Coordination::ACLs getNodeACLs(Storage & storage, StringRef path, bool is_local)
{
    if (is_local)
    {
        std::shared_lock lock(storage.storage_mutex);
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

/// Default implementations ///
template <std::derived_from<Coordination::ZooKeeperRequest> T, typename Storage>
Coordination::ZooKeeperResponsePtr
processLocal(const T & zk_request, Storage & /*storage*/, KeeperStorageBase::DeltaRange /*deltas*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Local processing not supported for request with type {}", zk_request.getOpNum());
}

template <std::derived_from<Coordination::ZooKeeperRequest> T, typename Storage>
std::list<KeeperStorageBase::Delta> preprocess(
    const T & /*zk_request*/,
    Storage & /*storage*/,
    int64_t /*zxid*/,
    int64_t /*session_id*/,
    int64_t /*time*/,
    uint64_t * /*digest*/,
    const KeeperContext & /*keeper_context*/)
{
    return {};
}

template <std::derived_from<Coordination::ZooKeeperRequest> T>
KeeperResponsesForSessions processWatches(
    const T & /*zk_request*/,
    KeeperStorageBase::DeltaRange /*deltas*/,
    KeeperStorageBase::Watches & /*watches*/,
    KeeperStorageBase::Watches & /*list_watches*/,
    KeeperStorageBase::SessionAndWatcher & /*sessions_and_watchers*/)
{
    return {};
}

template <std::derived_from<Coordination::ZooKeeperRequest> T, typename Storage>
bool checkAuth(const T & /*zk_request*/, Storage & /*storage*/, int64_t /*session_id*/, bool /*is_local*/)
{
    return true;
}
/// Default implementations ///

/// HEARTBEAT Request ///
template <typename Storage>
Coordination::ZooKeeperResponsePtr process(
    const Coordination::ZooKeeperHeartbeatRequest & zk_request,
    Storage & storage,
    KeeperStorageBase::DeltaRange deltas)
{
    Coordination::ZooKeeperResponsePtr response_ptr = zk_request.makeResponse();
    response_ptr->error = storage.commit(deltas);
    return response_ptr;
}
/// HEARTBEAT Request ///

/// SYNC Request ///
template <typename Storage>
Coordination::ZooKeeperResponsePtr
process(const Coordination::ZooKeeperSyncRequest & zk_request, Storage & /* storage */, KeeperStorageBase::DeltaRange /* deltas */)
{
    auto response = std::make_shared<Coordination::ZooKeeperSyncResponse>();
    response->path = zk_request.path;
    return response;
}
/// SYNC Request ///

/// CREATE Request ///
template <typename Storage>
bool checkAuth(const Coordination::ZooKeeperCreateRequest & zk_request, Storage & storage, int64_t session_id, bool is_local)
{
    auto path = zk_request.getPath();
    return storage.checkACL(parentNodePath(path), Coordination::ACL::Create, session_id, is_local);
}

KeeperResponsesForSessions processWatches(
    const Coordination::ZooKeeperCreateRequest & zk_request,
    KeeperStorageBase::DeltaRange /*deltas*/,
    KeeperStorageBase::Watches & watches,
    KeeperStorageBase::Watches & list_watches,
    KeeperStorageBase::SessionAndWatcher & sessions_and_watchers)
{
    return processWatchesImpl(zk_request.getPath(), watches, list_watches, sessions_and_watchers, Coordination::Event::CREATED);
}

template <typename Storage>
std::list<KeeperStorageBase::Delta> preprocess(
    const Coordination::ZooKeeperCreateRequest & zk_request,
    Storage & storage,
    int64_t zxid,
    int64_t session_id,
    int64_t time,
    uint64_t * /*digest*/,
    const KeeperContext & keeper_context)
{
    ProfileEvents::increment(ProfileEvents::KeeperCreateRequest);

    std::list<KeeperStorageBase::Delta> new_deltas;

    auto parent_path = parentNodePath(zk_request.path);
    auto parent_node = storage.uncommitted_state.getNode(parent_path);
    if (parent_node == nullptr)
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZNONODE}};

    if (parent_node->stats.isEphemeral())
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZNOCHILDRENFOREPHEMERALS}};

    std::string path_created = zk_request.path;
    if (zk_request.is_sequential)
    {
        if (zk_request.not_exists)
            return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};

        auto seq_num = parent_node->stats.seqNum();

        std::stringstream seq_num_str; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        seq_num_str.exceptions(std::ios::failbit);
        seq_num_str << std::setw(10) << std::setfill('0') << seq_num;

        path_created += seq_num_str.str();
    }

    if (Coordination::matchPath(path_created, keeper_system_path) != Coordination::PathMatchResult::NOT_MATCH)
    {
        auto error_msg = fmt::format("Trying to create a node inside the internal Keeper path ({}) which is not allowed. Path: {}", keeper_system_path, path_created);

        handleSystemNodeModification(keeper_context, error_msg);
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};
    }

    if (storage.uncommitted_state.getNode(path_created))
    {
        if (zk_request.getOpNum() == Coordination::OpNum::CreateIfNotExists)
            return new_deltas;

        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZNODEEXISTS}};
    }

    if (getBaseNodeName(path_created).size == 0)
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};

    Coordination::ACLs node_acls;
    if (!fixupACL(zk_request.acls, session_id, storage.uncommitted_state, keeper_context.shouldBlockACL(), node_acls))
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZINVALIDACL}};

    if (zk_request.is_ephemeral)
        storage.uncommitted_state.ephemerals[session_id].emplace(path_created);

    int32_t parent_cversion = zk_request.parent_cversion;

    UpdateNodeStatDelta update_parent_delta(*parent_node);
    update_parent_delta.new_stats.increaseSeqNum();

    if (parent_cversion == -1)
        ++update_parent_delta.new_stats.cversion;
    else if (parent_cversion > update_parent_delta.old_stats.cversion)
        update_parent_delta.new_stats.cversion = parent_cversion;

    if (zxid > update_parent_delta.old_stats.pzxid)
        update_parent_delta.new_stats.pzxid = zxid;

    update_parent_delta.new_stats.increaseNumChildren();

    new_deltas.emplace_back(std::string{parent_path}, zxid, std::move(update_parent_delta));

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
    stat.ephemeralOwner = zk_request.is_ephemeral ? session_id : 0;

    new_deltas.emplace_back(
        std::move(path_created),
        zxid,
        CreateNodeDelta{stat, std::move(node_acls), zk_request.data});

    return new_deltas;
}

template <typename Storage>
Coordination::ZooKeeperResponsePtr process(const Coordination::ZooKeeperCreateRequest & zk_request, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
    std::shared_ptr<Coordination::ZooKeeperCreateResponse> response = zk_request.not_exists
        ? std::make_shared<Coordination::ZooKeeperCreateIfNotExistsResponse>()
        : std::make_shared<Coordination::ZooKeeperCreateResponse>();

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
        created_path = create_delta_it->path;

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
bool checkAuth(const Coordination::ZooKeeperGetRequest & zk_request, Storage & storage, int64_t session_id, bool is_local)
{
    return storage.checkACL(zk_request.getPath(), Coordination::ACL::Read, session_id, is_local);
}

template <typename Storage>
std::list<KeeperStorageBase::Delta> preprocess(
    const Coordination::ZooKeeperGetRequest & zk_request,
    Storage & storage,
    int64_t zxid,
    int64_t /*session_id*/,
    int64_t /*time*/,
    uint64_t * /*digest*/,
    const KeeperContext & /*keeper_context*/)
{
    ProfileEvents::increment(ProfileEvents::KeeperGetRequest);

    if (zk_request.path == Coordination::keeper_api_feature_flags_path
        || zk_request.path == Coordination::keeper_config_path
        || zk_request.path == Coordination::keeper_availability_zone_path)
        return {};

    if (!storage.uncommitted_state.getNode(zk_request.path))
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZNONODE}};

    return {};
}

template <bool local, typename Storage>
Coordination::ZooKeeperResponsePtr
processImpl(const Coordination::ZooKeeperGetRequest & zk_request, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
    auto response = std::make_shared<Coordination::ZooKeeperGetResponse>();

    if constexpr (!local)
    {
        if (const auto result = storage.commit(std::move(deltas)); result != Coordination::Error::ZOK)
        {
            response->error = result;
            return response;
        }
    }

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
        if constexpr (local)
            response->error = Coordination::Error::ZNONODE;
        else
            onStorageInconsistency("Failed to get node because it's missing");
    }
    else
    {
        node_it->value.setResponseStat(response->stat);
        auto data = node_it->value.getData();
        response->data = std::string(data);
        response->error = Coordination::Error::ZOK;
    }

    return response;
}

template <typename Storage>
Coordination::ZooKeeperResponsePtr process(const Coordination::ZooKeeperGetRequest & zk_request, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
    return processImpl<false>(zk_request, storage, std::move(deltas));
}

template <typename Storage>
Coordination::ZooKeeperResponsePtr
processLocal(const Coordination::ZooKeeperGetRequest & zk_request, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
    ProfileEvents::increment(ProfileEvents::KeeperGetRequest);
    return processImpl<true>(zk_request, storage, std::move(deltas));
}
/// GET Request ///

/// REMOVE Request ///
template <typename Storage>
bool checkAuth(const Coordination::ZooKeeperRemoveRequest & zk_request, Storage & storage, int64_t session_id, bool is_local)
{
    return storage.checkACL(parentNodePath(zk_request.getPath()), Coordination::ACL::Delete, session_id, is_local);
}

KeeperResponsesForSessions processWatches(
    const Coordination::ZooKeeperRemoveRequest & zk_request,
    KeeperStorageBase::DeltaRange /*deltas*/,
    KeeperStorageBase::Watches & watches,
    KeeperStorageBase::Watches & list_watches,
    KeeperStorageBase::SessionAndWatcher & sessions_and_watchers)
{
    return processWatchesImpl(zk_request.getPath(), watches, list_watches, sessions_and_watchers, Coordination::Event::DELETED);
}

template <typename Storage>
std::list<KeeperStorageBase::Delta> preprocess(
    const Coordination::ZooKeeperRemoveRequest & zk_request,
    Storage & storage,
    int64_t zxid,
    int64_t /* session_id */,
    int64_t /* time */,
    uint64_t * /* digest */,
    const KeeperContext & keeper_context)
{
    ProfileEvents::increment(ProfileEvents::KeeperRemoveRequest);

    std::list<KeeperStorageBase::Delta> new_deltas;

    if (Coordination::matchPath(zk_request.path, keeper_system_path) != Coordination::PathMatchResult::NOT_MATCH)
    {
        auto error_msg = fmt::format("Trying to delete an internal Keeper path ({}) which is not allowed", zk_request.path);

        handleSystemNodeModification(keeper_context, error_msg);
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};
    }

    auto parent_path = parentNodePath(zk_request.path);
    auto parent_node = storage.uncommitted_state.getNode(parent_path);

    std::optional<UpdateNodeStatDelta> update_parent_delta;
    if (parent_node)
        update_parent_delta.emplace(*parent_node);

    const auto add_parent_update_delta = [&]
    {
        if (!update_parent_delta)
            return;

        new_deltas.emplace_back(
            std::string{parent_path},
            zxid,
            std::move(*update_parent_delta)
        );
    };

    const auto update_parent_pzxid = [&]()
    {
        if (!update_parent_delta)
            return;

        if (update_parent_delta->old_stats.pzxid < zxid)
            update_parent_delta->new_stats.pzxid = zxid;
    };

    auto node = storage.uncommitted_state.getNode(zk_request.path);

    if (!node)
    {
        if (zk_request.restored_from_zookeeper_log)
        {
            update_parent_pzxid();
            add_parent_update_delta();
        }
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZNONODE}};
    }
    if (zk_request.version != -1 && zk_request.version != node->stats.version)
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZBADVERSION}};
    if (node->stats.numChildren() != 0)
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZNOTEMPTY}};

    if (zk_request.restored_from_zookeeper_log)
        update_parent_pzxid();

    chassert(update_parent_delta);
    ++update_parent_delta->new_stats.cversion;
    update_parent_delta->new_stats.decreaseNumChildren();
    add_parent_update_delta();

    new_deltas.emplace_back(
        zk_request.path,
        zxid,
        RemoveNodeDelta{zk_request.version, node->stats, storage.uncommitted_state.getACLs(zk_request.path), std::string{node->getData()}});

    if (node->stats.isEphemeral())
    {
        /// try deleting the ephemeral node from the uncommitted state
        unregisterEphemeralPath(storage.uncommitted_state.ephemerals, node->stats.ephemeralOwner(), zk_request.path, /*throw_if_missing=*/false);
    }

    return new_deltas;
}

template <typename Storage>
Coordination::ZooKeeperResponsePtr
process(const Coordination::ZooKeeperRemoveRequest & /*zk_request*/, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
    auto response = std::make_shared<Coordination::ZooKeeperRemoveResponse>();

    response->error = storage.commit(std::move(deltas));
    return response;
}

/// REMOVE Request ///

/// REMOVERECURSIVE Request ///

namespace
{

template <typename Storage>
class ToDeleteTreeCollector
{
    Storage & storage;
    int64_t zxid;
    int64_t session_id;
    uint32_t limit;

    uint32_t nodes_observed = 1;  /// root node

    std::list<KeeperStorageBase::Delta> deltas;
    using UncommittedChildren
        = std::unordered_set<std::string_view, StringHashForHeterogeneousLookup, StringHashForHeterogeneousLookup::transparent_key_equal>;

public:
    enum class CollectStatus
    {
        Ok,
        NoAuth,
        LimitExceeded,
    };

    friend struct KeeperStorageRemoveRecursiveRequestProcessor;

    ToDeleteTreeCollector(Storage & storage_, int64_t zxid_, int64_t session_id_, uint32_t limit_)
        : storage(storage_)
        , zxid(zxid_)
        , session_id(session_id_)
        , limit(limit_)
    {
    }

    CollectStatus collect(StringRef root_path, const Storage::Node & root_node)
    {
        if (checkLimits(root_node))
            return CollectStatus::LimitExceeded;

        addDelta(root_path, root_node.stats, storage.uncommitted_state.getACLs(root_path), std::string{root_node.getData()});

        for (auto current_delta_it = deltas.rbegin(); current_delta_it != deltas.rend(); ++current_delta_it)
        {
            const auto & current_path = current_delta_it->path;
            chassert(!current_path.empty());

            if (!storage.checkACL(current_path, Coordination::ACL::Delete, session_id, /*is_local=*/false))
                return CollectStatus::NoAuth;

            UncommittedChildren uncommitted_children;
            if (auto status = visitUncommitted(current_path, uncommitted_children); status != CollectStatus::Ok)
                return status;

            if constexpr (Storage::use_rocksdb)
            {
                if (auto status = visitRocksDBNode(current_path, uncommitted_children); status != CollectStatus::Ok)
                    return status;
            }
            else
            {
                if (auto status = visitMemNode(current_path, uncommitted_children); status != CollectStatus::Ok)
                    return status;
            }
        }

        return CollectStatus::Ok;
    }

    std::list<KeeperStorageBase::Delta> extractDeltas()
    {
        return std::move(deltas);
    }

private:
    CollectStatus visitRocksDBNode(StringRef current_path, const UncommittedChildren & uncommitted_children) requires Storage::use_rocksdb
    {
        std::filesystem::path current_path_fs(current_path.toString());

        std::vector<std::pair<std::string, typename Storage::Node>> children;
        {
            std::lock_guard lock(storage.storage_mutex);
            children = storage.container.getChildren(current_path.toString(), /*read_meta*/ true, /*read_data=*/true);
        }

        for (auto && [child_name, child_node] : children)
        {
            auto child_path = (current_path_fs / child_name).generic_string();

            if (uncommitted_children.contains(child_path))
                continue;

            if (checkLimits(child_node))
                return CollectStatus::LimitExceeded;

            addDelta(child_path, child_node.stats, storage.acl_map.convertNumber(child_node.acl_id), std::string{child_node.getData()});
        }

        return CollectStatus::Ok;
    }

    CollectStatus visitMemNode(StringRef current_path, const UncommittedChildren & uncommitted_children) requires (!Storage::use_rocksdb)
    {
        std::lock_guard lock(storage.storage_mutex);
        auto node_it = storage.container.find(current_path);
        if (node_it == storage.container.end())
            return CollectStatus::Ok;

        std::filesystem::path current_path_fs(current_path.toString());
        const auto & children = node_it->value.getChildren();

        for (const auto & child_name : children)
        {
            auto child_path = (current_path_fs / child_name.toView()).generic_string();

            if (uncommitted_children.contains(child_path))
                continue;

            auto child_it = storage.container.find(child_path);
            chassert(child_it != storage.container.end());
            const auto & child_node = child_it->value;

            if (checkLimits(child_node))
                return CollectStatus::LimitExceeded;

            addDelta(child_path, child_node.stats, storage.acl_map.convertNumber(child_node.acl_id), std::string{child_node.getData()});
        }

        return CollectStatus::Ok;
    }

    CollectStatus visitUncommitted(const std::string & path, UncommittedChildren & uncommitted_children)
    {
        auto & nodes = storage.uncommitted_state.nodes;

        for (auto & [node_path, uncommitted_node] : nodes)
        {
            if (parentNodePath(node_path) != path)
                continue;

            uncommitted_children.insert(node_path);

            const auto node_ptr = uncommitted_node.node;

            if (node_ptr == nullptr) /// node was deleted in previous step of multi transaction
                continue;

            if (checkLimits(*node_ptr))
                return CollectStatus::LimitExceeded;

            uncommitted_node.materializeACL(storage.acl_map);
            addDelta(node_path, node_ptr->stats, *uncommitted_node.acls, std::string{node_ptr->getData()});
        }

        return CollectStatus::Ok;
    }

    void addDelta(StringRef path, const NodeStats & stats, Coordination::ACLs acls, std::string data)
    {
        deltas.emplace_front(std::string{path}, zxid, RemoveNodeDelta{/*version=*/-1, stats, std::move(acls), std::move(data)});
    }

    bool checkLimits(const Storage::Node & node)
    {
        nodes_observed += node.stats.numChildren();
        return nodes_observed > limit;
    }
};
}

template <typename Storage>
bool checkAuth(const Coordination::ZooKeeperRemoveRecursiveRequest & zk_request, Storage & storage, int64_t session_id, bool is_local)
{
    return storage.checkACL(parentNodePath(zk_request.getPath()), Coordination::ACL::Delete, session_id, is_local);
}

KeeperResponsesForSessions processWatches(
    const Coordination::ZooKeeperRemoveRecursiveRequest & /*zk_request*/,
    KeeperStorageBase::DeltaRange deltas,
    KeeperStorageBase::Watches & watches,
    KeeperStorageBase::Watches & list_watches,
    KeeperStorageBase::SessionAndWatcher & sessions_and_watchers)
{
    KeeperResponsesForSessions responses;
    for (const auto & delta : deltas)
    {
        const auto * remove_delta = std::get_if<RemoveNodeDelta>(&delta.operation);
        if (remove_delta)
        {
            auto new_responses = processWatchesImpl(delta.path, watches, list_watches, sessions_and_watchers, Coordination::Event::DELETED);
            responses.insert(responses.end(), std::make_move_iterator(new_responses.begin()), std::make_move_iterator(new_responses.end()));
        }
    }

    return responses;
}

template <typename Storage>
std::list<KeeperStorageBase::Delta> preprocess(
    const Coordination::ZooKeeperRemoveRecursiveRequest & zk_request,
    Storage & storage,
    int64_t zxid,
    int64_t session_id,
    int64_t /* time */,
    uint64_t * /* digest */,
    const KeeperContext & keeper_context)
{
    ProfileEvents::increment(ProfileEvents::KeeperRemoveRequest);

    std::list<KeeperStorageBase::Delta> new_deltas;

    if (Coordination::matchPath(zk_request.path, keeper_system_path) != Coordination::PathMatchResult::NOT_MATCH)
    {
        auto error_msg = fmt::format("Trying to delete an internal Keeper path ({}) which is not allowed", zk_request.path);

        handleSystemNodeModification(keeper_context, error_msg);
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};
    }

    auto node = storage.uncommitted_state.getNode(zk_request.path);

    auto parent_path = parentNodePath(zk_request.path);
    auto parent_node = storage.uncommitted_state.getNode(parent_path);

    std::optional<UpdateNodeStatDelta> update_parent_delta;
    if (parent_node)
        update_parent_delta.emplace(*parent_node);

    const auto add_parent_update_delta = [&]
    {
        if (!update_parent_delta)
            return;

        new_deltas.emplace_back(
            std::string{parent_path},
            zxid,
            std::move(*update_parent_delta)
        );
    };

    const auto update_parent_pzxid = [&]()
    {
        if (!update_parent_delta)
            return;

        if (update_parent_delta->old_stats.pzxid < zxid)
            update_parent_delta->new_stats.pzxid = zxid;
    };

    if (!node)
    {
        if (zk_request.restored_from_zookeeper_log)
        {
            update_parent_pzxid();
            add_parent_update_delta();
        }

        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZNONODE}};
    }

    ToDeleteTreeCollector<Storage> collector(storage, zxid, session_id, zk_request.remove_nodes_limit);
    auto collect_status = collector.collect(zk_request.path, *node);

    if (collect_status == ToDeleteTreeCollector<Storage>::CollectStatus::NoAuth)
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZNOAUTH}};

    if (collect_status == ToDeleteTreeCollector<Storage>::CollectStatus::LimitExceeded)
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZNOTEMPTY}};

    if (zk_request.restored_from_zookeeper_log)
    {
        update_parent_pzxid();
    }

    chassert(update_parent_delta);
    ++update_parent_delta->new_stats.cversion;
    update_parent_delta->new_stats.decreaseNumChildren();
    add_parent_update_delta();

    auto delete_deltas = collector.extractDeltas();

    for (const auto & delta : delete_deltas)
    {
        const auto * remove_delta = std::get_if<RemoveNodeDelta>(&delta.operation);
        if (remove_delta && remove_delta->stat.ephemeralOwner())
        {
            unregisterEphemeralPath(
                storage.uncommitted_state.ephemerals, remove_delta->stat.ephemeralOwner(), delta.path, /*throw_if_missing=*/false);
        }
    }

    new_deltas.splice(new_deltas.end(), std::move(delete_deltas));

    return new_deltas;
}

template <typename Storage>
Coordination::ZooKeeperResponsePtr
process(const Coordination::ZooKeeperRemoveRecursiveRequest & zk_request, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
    Coordination::ZooKeeperResponsePtr response_ptr = zk_request.makeResponse();
    response_ptr->error = storage.commit(std::move(deltas));
    return response_ptr;
}

/// REMOVERECURSIVE Request ///

/// EXISTS Request ///
template <typename Storage>
std::list<KeeperStorageBase::Delta> preprocess(
    const Coordination::ZooKeeperExistsRequest & zk_request,
    Storage & storage,
    int64_t zxid,
    int64_t /*session_id*/,
    int64_t /*time*/,
    uint64_t * /*digest*/,
    const KeeperContext & /*keeper_context*/)
{
    ProfileEvents::increment(ProfileEvents::KeeperExistsRequest);

    if (!storage.uncommitted_state.getNode(zk_request.path))
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZNONODE}};

    return {};
}

template <bool local, typename Storage>
Coordination::ZooKeeperResponsePtr
processImpl(const Coordination::ZooKeeperExistsRequest & zk_request, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
    auto response = std::make_shared<Coordination::ZooKeeperExistsResponse>();

    if constexpr (!local)
    {
        if (const auto result = storage.commit(std::move(deltas)); result != Coordination::Error::ZOK)
        {
            response->error = result;
            return response;
        }
    }

    auto & container = storage.container;
    auto node_it = container.find(zk_request.path);
    if (node_it == container.end())
    {
        if constexpr (local)
            response->error = Coordination::Error::ZNONODE;
        else
            onStorageInconsistency("Node unexpectedly missing while checking if it exists");
    }
    else
    {
        node_it->value.setResponseStat(response->stat);
        response->error = Coordination::Error::ZOK;
    }

    return response;
}

template <typename Storage>
Coordination::ZooKeeperResponsePtr process(const Coordination::ZooKeeperExistsRequest & zk_request, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
    return processImpl<false>(zk_request, storage, std::move(deltas));
}

template <typename Storage>
Coordination::ZooKeeperResponsePtr
processLocal(const Coordination::ZooKeeperExistsRequest & zk_request, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
    ProfileEvents::increment(ProfileEvents::KeeperExistsRequest);
    return processImpl<true>(zk_request, storage, std::move(deltas));
}
/// EXISTS Request ///

/// SET Request ///
template <typename Storage>
bool checkAuth(const Coordination::ZooKeeperSetRequest & zk_request, Storage & storage, int64_t session_id, bool is_local)
{
    return storage.checkACL(zk_request.getPath(), Coordination::ACL::Write, session_id, is_local);
}

KeeperResponsesForSessions processWatches(
    const Coordination::ZooKeeperSetRequest & zk_request,
    KeeperStorageBase::DeltaRange /*deltas*/,
    KeeperStorageBase::Watches & watches,
    KeeperStorageBase::Watches & list_watches,
    KeeperStorageBase::SessionAndWatcher & sessions_and_watchers)
{
    return processWatchesImpl(zk_request.getPath(), watches, list_watches, sessions_and_watchers, Coordination::Event::CHANGED);
}

template <typename Storage>
std::list<KeeperStorageBase::Delta> preprocess(
    const Coordination::ZooKeeperSetRequest & zk_request,
    Storage & storage,
    int64_t zxid,
    int64_t /* session_id */,
    int64_t time,
    uint64_t * /* digest */,
    const KeeperContext & keeper_context)
{
    ProfileEvents::increment(ProfileEvents::KeeperSetRequest);

    std::list<KeeperStorageBase::Delta> new_deltas;

    if (Coordination::matchPath(zk_request.path, keeper_system_path) != Coordination::PathMatchResult::NOT_MATCH)
    {
        auto error_msg = fmt::format("Trying to update an internal Keeper path ({}) which is not allowed", zk_request.path);

        handleSystemNodeModification(keeper_context, error_msg);
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};
    }

    if (!storage.uncommitted_state.getNode(zk_request.path))
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZNONODE}};

    auto node = storage.uncommitted_state.getNode(zk_request.path);

    if (zk_request.version != -1 && zk_request.version != node->stats.version)
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZBADVERSION}};

    new_deltas.emplace_back(
        zk_request.path,
        zxid,
        UpdateNodeDataDelta{.old_data = std::string{node->getData()}, .new_data = zk_request.data, .version = zk_request.version});

    UpdateNodeStatDelta node_delta(*node);
    node_delta.version = zk_request.version;
    auto & new_stats = node_delta.new_stats;
    new_stats.version++;
    new_stats.mzxid = zxid;
    new_stats.mtime = time;
    new_stats.data_size = static_cast<uint32_t>(zk_request.data.size());
    new_deltas.emplace_back(zk_request.path, zxid, std::move(node_delta));

    auto parent_path = parentNodePath(zk_request.path);
    auto parent_node = storage.uncommitted_state.getNode(parent_path);
    UpdateNodeStatDelta parent_delta(*parent_node);
    ++parent_delta.new_stats.cversion;
    new_deltas.emplace_back(std::string{parent_path}, zxid, std::move(parent_delta));

    return new_deltas;
}

template <typename Storage>
Coordination::ZooKeeperResponsePtr process(const Coordination::ZooKeeperSetRequest & zk_request, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
    auto & container = storage.container;

    auto response = std::make_shared<Coordination::ZooKeeperSetResponse>();

    if (const auto result = storage.commit(std::move(deltas)); result != Coordination::Error::ZOK)
    {
        response->error = result;
        return response;
    }

    auto node_it = container.find(zk_request.path);
    if (node_it == container.end())
        onStorageInconsistency("Node to be updated is missing");

    node_it->value.setResponseStat(response->stat);
    response->error = Coordination::Error::ZOK;

    return response;
}
/// SET Request ///

/// LIST Request ///
template <typename Storage>
bool checkAuth(const Coordination::ZooKeeperListRequest & zk_request, Storage & storage, int64_t session_id, bool is_local)
{
    return storage.checkACL(zk_request.getPath(), Coordination::ACL::Read, session_id, is_local);
}

template <typename Storage>
std::list<KeeperStorageBase::Delta> preprocess(
    const Coordination::ZooKeeperListRequest & zk_request,
    Storage & storage,
    int64_t zxid,
    int64_t /*session_id*/,
    int64_t /*time*/,
    uint64_t * /*digest*/,
    const KeeperContext & /*keeper_context*/)
{
    ProfileEvents::increment(ProfileEvents::KeeperListRequest);

    if (!storage.uncommitted_state.getNode(zk_request.path))
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZNONODE}};

    return {};
}

template <bool local, typename Storage>
Coordination::ZooKeeperResponsePtr processImpl(const Coordination::ZooKeeperListRequest & zk_request, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
    std::shared_ptr<Coordination::ZooKeeperListResponse> response = zk_request.getOpNum() == Coordination::OpNum::SimpleList
        ? std::make_shared<Coordination::ZooKeeperSimpleListResponse>()
        : std::make_shared<Coordination::ZooKeeperListResponse>();

    if constexpr (!local)
    {
        if (const auto result = storage.commit(std::move(deltas)); result != Coordination::Error::ZOK)
        {
            response->error = result;
            return response;
        }
    }

    auto & container = storage.container;

    auto node_it = container.find(zk_request.path);
    if (node_it == container.end())
    {
        if constexpr (local)
            response->error = Coordination::Error::ZNONODE;
        else
            onStorageInconsistency("Failed to list children of node because it's missing");
    }
    else
    {
        auto path_prefix = zk_request.path;
        if (path_prefix.empty())
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Path cannot be empty");

        auto list_request_type = Coordination::ListRequestType::ALL;
        if (const auto * filtered_list = dynamic_cast<const Coordination::ZooKeeperFilteredListRequest *>(&zk_request))
        {
            list_request_type = filtered_list->list_request_type;
        }

        const auto get_children = [&]()
        {
            /// if list_request_type will read all the children, we don't have to read any meta, just list all the paths.
            if constexpr (Storage::use_rocksdb)
                return std::optional{container.getChildren(zk_request.path, list_request_type != Coordination::ListRequestType::ALL)};
            else
                return &node_it->value.getChildren();
        };

        const auto children = get_children();
        response->names.reserve(children->size());

#ifdef DEBUG_OR_SANITIZER_BUILD
        if (!zk_request.path.starts_with(keeper_system_path) && static_cast<size_t>(node_it->value.stats.numChildren()) != children->size())
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Difference between numChildren ({}) and actual children size ({}) for '{}'",
                node_it->value.stats.numChildren(),
                children->size(),
                zk_request.path);
        }
#endif

        const auto add_child = [&](const auto & child)
        {
            using enum Coordination::ListRequestType;

            bool is_ephemeral;
            if constexpr (!Storage::use_rocksdb)
            {
                auto child_path = (std::filesystem::path(zk_request.path) / child.toView()).generic_string();
                auto child_it = container.find(child_path);
                if (child_it == container.end())
                    onStorageInconsistency("Failed to find a child");
                is_ephemeral = child_it->value.stats.isEphemeral();
            }
            else
            {
                is_ephemeral = child.second.stats.isEphemeral();
            }

            return (is_ephemeral && list_request_type == EPHEMERAL_ONLY) || (!is_ephemeral && list_request_type == PERSISTENT_ONLY);
        };

        for (const auto & child : *children)
        {
            if (Coordination::ListRequestType::ALL == list_request_type || add_child(child))
            {
                if constexpr (Storage::use_rocksdb)
                    response->names.push_back(child.first);
                else
                    response->names.push_back(child.toString());
            }
        }

        node_it->value.setResponseStat(response->stat);
        response->error = Coordination::Error::ZOK;
    }

    return response;
}

template <typename Storage>
Coordination::ZooKeeperResponsePtr process(const Coordination::ZooKeeperListRequest & zk_request, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
    return processImpl<false>(zk_request, storage, std::move(deltas));
}

template <typename Storage>
Coordination::ZooKeeperResponsePtr
processLocal(const Coordination::ZooKeeperListRequest & zk_request, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
    ProfileEvents::increment(ProfileEvents::KeeperListRequest);
    return processImpl<true>(zk_request, storage, std::move(deltas));
}
/// LIST Request ///

/// CHECK Request ///
template <typename Storage>
bool checkAuth(const Coordination::ZooKeeperCheckRequest & zk_request, Storage & storage, int64_t session_id, bool is_local)
{
    auto path = zk_request.getPath();
    return storage.checkACL(
        zk_request.getOpNum() == Coordination::OpNum::CheckNotExists ? parentNodePath(path) : path,
        Coordination::ACL::Read,
        session_id,
        is_local);
}

template <typename Storage>
std::list<KeeperStorageBase::Delta> preprocess(
    const Coordination::ZooKeeperCheckRequest & zk_request,
    Storage & storage,
    int64_t zxid,
    int64_t /*session_id*/,
    int64_t /*time*/,
    uint64_t * /*digest*/,
    const KeeperContext & /*keeper_context*/)
{
    ProfileEvents::increment(ProfileEvents::KeeperCheckRequest);

    auto node = storage.uncommitted_state.getNode(zk_request.path);
    if (zk_request.getOpNum() == Coordination::OpNum::CheckNotExists)
    {
        if (node && (zk_request.version == -1 || zk_request.version == node->stats.version))
            return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZNODEEXISTS}};
    }
    else
    {
        if (!node)
            return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZNONODE}};

        if (zk_request.version != -1 && zk_request.version != node->stats.version)
            return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZBADVERSION}};
    }

    return {};
}

template <bool local, typename Storage>
Coordination::ZooKeeperResponsePtr processImpl(const Coordination::ZooKeeperCheckRequest & zk_request, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
    std::shared_ptr<Coordination::ZooKeeperCheckResponse> response = zk_request.not_exists
        ? std::make_shared<Coordination::ZooKeeperCheckNotExistsResponse>()
        : std::make_shared<Coordination::ZooKeeperCheckResponse>();

    if constexpr (!local)
    {
        if (const auto result = storage.commit(std::move(deltas)); result != Coordination::Error::ZOK)
        {
            response->error = result;
            return response;
        }
    }

    const auto on_error = [&]([[maybe_unused]] const auto error_code)
    {
        if constexpr (local)
            response->error = error_code;
        else
            onStorageInconsistency("Node to check is unexpectedly missing");
    };

    auto & container = storage.container;
    auto node_it = container.find(zk_request.path);

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
        else
            response->error = Coordination::Error::ZOK;
    }

    return response;
}

template <typename Storage>
Coordination::ZooKeeperResponsePtr process(const Coordination::ZooKeeperCheckRequest & zk_request, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
    return processImpl<false>(zk_request, storage, std::move(deltas));
}

template <typename Storage>
Coordination::ZooKeeperResponsePtr
processLocal(const Coordination::ZooKeeperCheckRequest & zk_request, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
    ProfileEvents::increment(ProfileEvents::KeeperCheckRequest);
    return processImpl<true>(zk_request, storage, std::move(deltas));
}
/// CHECK Request ///

/// MULTI Request ///
using OperationType = Coordination::ZooKeeperMultiRequest::OperationType;
template <typename Storage>
bool checkAuth(const Coordination::ZooKeeperMultiRequest & zk_request, Storage & storage, int64_t session_id, bool is_local)
{
    for (const auto & concrete_request : zk_request.requests)
    {
        if (!callOnConcreteRequestType(
                *concrete_request, [&](const auto & subrequest) { return checkAuth(subrequest, storage, session_id, is_local); }))
            return false;
    }
    return true;
}

template <typename Storage>
std::list<KeeperStorageBase::Delta> preprocess(
    const Coordination::ZooKeeperMultiRequest & zk_request,
    Storage & storage,
    int64_t zxid,
    int64_t session_id,
    int64_t time,
    uint64_t * digest,
    const KeeperContext & keeper_context)
{
    ProfileEvents::increment(ProfileEvents::KeeperMultiRequest);
    std::vector<Coordination::Error> response_errors;
    const auto & subrequests = zk_request.requests;
    response_errors.reserve(subrequests.size());

    /// we cannot use `digest` directly in case we need to rollback Multi request
    uint64_t current_digest = 0;
    uint64_t * current_digest_ptr = nullptr;
    if (digest)
    {
        current_digest = *digest;
        current_digest_ptr = &current_digest;
    }

    std::list<KeeperStorageBase::Delta> new_deltas;
    for (size_t i = 0; i < subrequests.size(); ++i)
    {
        auto new_subdeltas = callOnConcreteRequestType(
            *subrequests[i],
            [&](const auto & subrequest)
            { return preprocess(subrequest, storage, zxid, session_id, time, /*digest=*/nullptr, keeper_context); });

        if (!new_subdeltas.empty())
        {
            if (auto * error = std::get_if<ErrorDelta>(&new_subdeltas.back().operation);
                error && zk_request.getOpNum() == Coordination::OpNum::Multi)
            {
                storage.uncommitted_state.rollback(std::move(new_deltas));
                response_errors.push_back(error->error);

                for (size_t j = i + 1; j < subrequests.size(); ++j)
                    response_errors.push_back(Coordination::Error::ZRUNTIMEINCONSISTENCY);

                return {KeeperStorageBase::Delta{zxid, FailedMultiDelta{std::move(response_errors)}}};
            }
        }

        new_subdeltas.emplace_back(zxid, SubDeltaEnd{});
        response_errors.push_back(Coordination::Error::ZOK);

        // manually add deltas so that the result of previous request in the transaction is used in the next request
        storage.uncommitted_state.applyDeltas(new_subdeltas, current_digest_ptr);
        new_deltas.splice(new_deltas.end(), std::move(new_subdeltas));
    }

    if (digest)
        *digest = current_digest;

    storage.uncommitted_state.addDeltas(std::move(new_deltas));
    return {};
}

KeeperStorageBase::DeltaRange extractSubdeltas(KeeperStorageBase::DeltaRange & deltas)
{
    std::list<KeeperStorageBase::Delta> subdeltas;
    auto it = deltas.begin();

    for (; it != deltas.end(); ++it)
    {
        if (std::holds_alternative<SubDeltaEnd>(it->operation))
        {
            ++it;
            break;
        }
    }

    KeeperStorageBase::DeltaRange result{.begin_it = deltas.begin(), .end_it = it};
    deltas.begin_it = it;
    return result;
}

template <typename Storage>
Coordination::ZooKeeperResponsePtr
process(const Coordination::ZooKeeperMultiRequest & zk_request, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
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
        for (size_t i = 0; i < subrequests.size(); ++i)
        {
            response->responses.push_back(std::make_shared<Coordination::ZooKeeperErrorResponse>());
            response->responses[i]->error = failed_multi->error_codes[i];
        }

        response->error = failed_multi->global_error;
        return response;
    }

    for (const auto & multi_subrequest : subrequests)
    {
        auto subdeltas = extractSubdeltas(deltas);
        response->responses.push_back(callOnConcreteRequestType(
            *multi_subrequest, [&](const auto & subrequest) { return process(subrequest, storage, std::move(subdeltas)); }));
    }

    response->error = Coordination::Error::ZOK;
    return response;
}

template <typename Storage>
Coordination::ZooKeeperResponsePtr processLocal(const Coordination::ZooKeeperMultiRequest & zk_request, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
    ProfileEvents::increment(ProfileEvents::KeeperMultiReadRequest);
    auto response = std::make_shared<Coordination::ZooKeeperMultiReadResponse>();
    response->responses.reserve(zk_request.requests.size());

    for (const auto & multi_subrequest : zk_request.requests)
    {
        auto subdeltas = extractSubdeltas(deltas);
        response->responses.push_back(callOnConcreteRequestType(
            *multi_subrequest, [&](const auto & subrequest) { return processLocal(subrequest, storage, std::move(subdeltas)); }));
    }

    response->error = Coordination::Error::ZOK;
    return response;
}

KeeperResponsesForSessions processWatches(
    const Coordination::ZooKeeperMultiRequest & zk_request,
    KeeperStorageBase::DeltaRange deltas,
    KeeperStorageBase::Watches & watches,
    KeeperStorageBase::Watches & list_watches,
    KeeperStorageBase::SessionAndWatcher & sessions_and_watchers)
{
    KeeperResponsesForSessions result;

    if (deltas.empty() || std::get_if<FailedMultiDelta>(&deltas.front().operation))
        return result;

    const auto & subrequests = zk_request.requests;
    for (const auto & generic_request : subrequests)
    {
        auto subdeltas = extractSubdeltas(deltas);
        auto responses = callOnConcreteRequestType(
            *generic_request, [&](const auto & subrequest) { return processWatches(subrequest, subdeltas, watches, list_watches, sessions_and_watchers); });
        result.insert(result.end(), responses.begin(), responses.end());
    }
    return result;
}
/// MULTI Request ///

/// AUTH Request ///
template <typename Storage>
std::list<KeeperStorageBase::Delta> preprocess(
    const Coordination::ZooKeeperAuthRequest & zk_request,
    Storage & storage,
    int64_t zxid,
    int64_t session_id,
    int64_t /*time*/,
    uint64_t * /*digest*/,
    const KeeperContext & /*keeper_context*/)
{
    if (zk_request.scheme != "digest" || std::count(zk_request.data.begin(), zk_request.data.end(), ':') != 1)
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZAUTHFAILED}};

    std::list<KeeperStorageBase::Delta> new_deltas;
    auto auth_digest = Storage::generateDigest(zk_request.data);
    if (auth_digest == storage.superdigest)
    {
        auto auth = std::make_shared<KeeperStorageBase::AuthID>();
        auth->scheme = "super";
        new_deltas.emplace_back(zxid, AddAuthDelta{session_id, std::move(auth)});
    }
    else
    {
        auto new_auth = std::make_shared<KeeperStorageBase::AuthID>();
        new_auth->scheme = zk_request.scheme;
        new_auth->id = std::move(auth_digest);
        if (!storage.uncommitted_state.hasACL(session_id, false, [&](const auto & auth_id) { return *new_auth == auth_id; }))
            new_deltas.emplace_back(zxid, AddAuthDelta{session_id, std::move(new_auth)});
    }

    return new_deltas;
}

template <typename Storage>
Coordination::ZooKeeperResponsePtr
process(const Coordination::ZooKeeperAuthRequest & /*zk_request*/, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
    auto response = std::make_shared<Coordination::ZooKeeperAuthResponse>();

    if (const auto result = storage.commit(std::move(deltas)); result != Coordination::Error::ZOK)
        response->error = result;

    return response;
}
/// AUTH Request ///

/// CLOSE Request ///
template <typename Storage>
Coordination::ZooKeeperResponsePtr
process(const Coordination::ZooKeeperCloseRequest & /* zk_request */, Storage &, KeeperStorageBase::DeltaRange /* deltas */)
{
    throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Called process on close request");
}
/// CLOSE Request ///

/// SETACL Request ///
template <typename Storage>
bool checkAuth(const Coordination::ZooKeeperSetACLRequest & zk_request, Storage & storage, int64_t session_id, bool is_local)
{
    return storage.checkACL(zk_request.getPath(), Coordination::ACL::Admin, session_id, is_local);
}

template <typename Storage>
std::list<KeeperStorageBase::Delta> preprocess(
    const Coordination::ZooKeeperSetACLRequest & zk_request,
    Storage & storage,
    int64_t zxid,
    int64_t session_id,
    int64_t /*time*/,
    uint64_t * /*digest*/,
    const KeeperContext & keeper_context)
{
    if (Coordination::matchPath(zk_request.path, keeper_system_path) != Coordination::PathMatchResult::NOT_MATCH)
    {
        auto error_msg = fmt::format("Trying to update an internal Keeper path ({}) which is not allowed", zk_request.path);

        handleSystemNodeModification(keeper_context, error_msg);
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};
    }

    auto & uncommitted_state = storage.uncommitted_state;
    if (!uncommitted_state.getNode(zk_request.path))
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZNONODE}};

    auto node = uncommitted_state.getNode(zk_request.path);

    if (zk_request.version != -1 && zk_request.version != node->stats.aversion)
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZBADVERSION}};


    Coordination::ACLs node_acls;
    if (!fixupACL(zk_request.acls, session_id, uncommitted_state, keeper_context.shouldBlockACL(), node_acls))
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZINVALIDACL}};

    UpdateNodeStatDelta update_stat_delta(*node);
    ++update_stat_delta.new_stats.aversion;
    std::list<KeeperStorageBase::Delta> new_deltas{
        {zk_request.path, zxid, SetACLDelta{uncommitted_state.getACLs(zk_request.path), std::move(node_acls), zk_request.version}},
        {zk_request.path, zxid, std::move(update_stat_delta)}};

    return new_deltas;
}

template <typename Storage>
Coordination::ZooKeeperResponsePtr
process(const Coordination::ZooKeeperSetACLRequest & zk_request, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
    auto response = std::make_shared<Coordination::ZooKeeperSetACLResponse>();

    if (const auto result = storage.commit(std::move(deltas)); result != Coordination::Error::ZOK)
    {
        response->error = result;
        return response;
    }

    auto node_it = storage.container.find(zk_request.path);
    if (node_it == storage.container.end())
        onStorageInconsistency("Failed to set ACL because node is missing");
    node_it->value.setResponseStat(response->stat);
    response->error = Coordination::Error::ZOK;

    return response;
}
/// SETACL Request ///

/// GETACL Request ///
template <typename Storage>
bool checkAuth(const Coordination::ZooKeeperGetACLRequest & zk_request, Storage & storage, int64_t session_id, bool is_local)
{
    return storage.checkACL(zk_request.getPath(), Coordination::ACL::Admin | Coordination::ACL::Read, session_id, is_local);
}

template <typename Storage>
std::list<KeeperStorageBase::Delta> preprocess(
    const Coordination::ZooKeeperGetACLRequest & zk_request,
    Storage & storage,
    int64_t zxid,
    int64_t /*session_id*/,
    int64_t /*time*/,
    uint64_t * /*digest*/,
    const KeeperContext & /*keeper_context*/)
{
    if (!storage.uncommitted_state.getNode(zk_request.path))
        return {KeeperStorageBase::Delta{zxid, Coordination::Error::ZNONODE}};

    return {};
}

template <bool local, typename Storage>
Coordination::ZooKeeperResponsePtr processImpl(const Coordination::ZooKeeperGetACLRequest & zk_request, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
    auto response = std::make_shared<Coordination::ZooKeeperGetACLResponse>();

    if constexpr (!local)
    {
        if (const auto result = storage.commit(std::move(deltas)); result != Coordination::Error::ZOK)
        {
            response->error = result;
            return response;
        }
    }

    auto & container = storage.container;
    auto node_it = container.find(zk_request.path);
    if (node_it == container.end())
    {
        if constexpr (local)
            response->error = Coordination::Error::ZNONODE;
        else
            onStorageInconsistency("Failed to get ACL because node is missing");
    }
    else
    {
        node_it->value.setResponseStat(response->stat);
        response->acl = storage.acl_map.convertNumber(node_it->value.acl_id);
    }

    return response;
}

template <typename Storage>
Coordination::ZooKeeperResponsePtr process(const Coordination::ZooKeeperGetACLRequest & zk_request, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
    return processImpl<false>(zk_request, storage, std::move(deltas));
}

template <typename Storage>
Coordination::ZooKeeperResponsePtr processLocal(const Coordination::ZooKeeperGetACLRequest & zk_request, Storage & storage, KeeperStorageBase::DeltaRange deltas)
{
    return processImpl<true>(zk_request, storage, std::move(deltas));
}
/// GETACL Request ///

void KeeperStorageBase::finalize()
{
    if (finalized)
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "KeeperStorage already finalized");

    finalized = true;

    committed_ephemerals.clear();

    watches.clear();
    list_watches.clear();
    sessions_and_watchers.clear();

    session_expiry_queue.clear();
}

bool KeeperStorageBase::isFinalized() const
{
    return finalized;
}

template<typename Container>
void KeeperStorage<Container>::preprocessRequest(
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
        auto elapsed = watch.elapsedMicroseconds();
        if (auto elapsed_ms = elapsed / 1000; elapsed_ms > keeper_context->getCoordinationSettings()[CoordinationSetting::log_slow_cpu_threshold_ms])
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

    TransactionInfo * transaction;
    uint64_t new_digest = 0;

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
            if (last_zxid == new_last_zxid && digest && checkDigest(*digest, current_digest))
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

        new_digest = current_digest.value;
        transaction = &uncommitted_transactions.emplace_back(TransactionInfo{.zxid = new_last_zxid, .nodes_digest = {}, .log_idx = log_idx});
    }

    std::list<Delta> new_deltas;
    SCOPE_EXIT({
        uncommitted_state.applyDeltas(new_deltas, keeper_context->digestEnabled() ? &new_digest : nullptr);
        uncommitted_state.addDeltas(std::move(new_deltas));

        if (zk_request->getOpNum() == Coordination::OpNum::Create)
        {
            fiu_do_on(FailPoints::keeper_leader_sets_invalid_digest, new_digest = 42);
        }

        if (keeper_context->digestEnabled())
        {
            new_digest = uncommitted_state.updateNodesDigest(new_digest, new_last_zxid);
            // if the version of digest we got from the leader is the same as the one this instances has, we can simply copy the value
            // and just check the digest on the commit
            // a mistake can happen while applying the changes to the uncommitted_state so for now let's just recalculate the digest here also
            transaction->nodes_digest = KeeperDigest{KEEPER_CURRENT_DIGEST_VERSION, new_digest};
        }
        else
        {
            transaction->nodes_digest = KeeperDigest{KeeperDigestVersion::NO_DIGEST};
        }

        uncommitted_state.cleanup(getZXID());
    });

    if (zk_request->getOpNum() == Coordination::OpNum::Close) /// Close request is special
    {
        std::unordered_map<
            std::string,
            UpdateNodeStatDelta,
            StringHashForHeterogeneousLookup,
            StringHashForHeterogeneousLookup::transparent_key_equal>
            parent_updates;

        const auto process_ephemerals_for_session
            = [&](const auto & current_ephemerals, auto & processed_ephemeral_nodes, bool check_processed_nodes)
        {
            auto session_ephemerals = current_ephemerals.find(session_id);
            if (session_ephemerals != current_ephemerals.end())
            {
                for (const auto & ephemeral_path : session_ephemerals->second)
                {
                    if (check_processed_nodes)
                    {
                        if (processed_ephemeral_nodes.contains(ephemeral_path))
                            continue;
                    }
                    else
                    {
                        processed_ephemeral_nodes.insert(ephemeral_path);
                    }

                    auto node = uncommitted_state.getNode(ephemeral_path, /*should_lock_storage=*/false);

                    /// maybe the node is deleted or recreated with different session_id in the uncommitted state
                    if (!node || node->stats.ephemeralOwner() != session_id)
                        continue;

                    auto parent_node_path = parentNodePath(ephemeral_path).toView();

                    auto parent_update_it = parent_updates.find(parent_node_path);
                    if (parent_update_it == parent_updates.end())
                    {
                        auto parent_node = uncommitted_state.getNode(StringRef{parent_node_path}, /*should_lock_storage=*/false);
                        std::tie(parent_update_it, std::ignore) = parent_updates.emplace(parent_node_path, *parent_node);
                    }

                    auto & parent_update_delta = parent_update_it->second;
                    ++parent_update_delta.new_stats.cversion;
                    parent_update_delta.new_stats.decreaseNumChildren();

                    new_deltas.emplace_back(
                        ephemeral_path,
                        transaction->zxid,
                        RemoveNodeDelta{.stat = node->stats, .acls = uncommitted_state.getACLs(ephemeral_path), .data = std::string{node->getData()}});
                }
            }
        };

        {
            /// storage lock should always be taken before ephemeral lock
            std::shared_lock storage_lock(storage_mutex);

            std::unordered_set<std::string_view> processed_ephemeral_nodes;
            process_ephemerals_for_session(uncommitted_state.ephemerals, processed_ephemeral_nodes, /*check_processed_nodes=*/false);

            std::lock_guard ephemeral_lock(ephemeral_mutex);
            process_ephemerals_for_session(committed_ephemerals, processed_ephemeral_nodes, /*check_processed_nodes=*/true);
        }

        uncommitted_state.ephemerals.erase(session_id);

        for (auto & [parent_path, parent_update_delta] : parent_updates)
        {
            new_deltas.emplace_back(parent_path, new_last_zxid, std::move(parent_update_delta));
        }

        new_deltas.emplace_back(transaction->zxid, CloseSessionDelta{session_id});
        return;
    }

    const auto preprocess_request = [&]<std::derived_from<Coordination::ZooKeeperRequest> T>(const T & concrete_zk_request)
    {
        if (check_acl && !checkAuth(concrete_zk_request, *this, session_id, false))
        {
            /// Multi requests handle failures using FailedMultiDelta
            if (zk_request->getOpNum() == Coordination::OpNum::Multi || zk_request->getOpNum() == Coordination::OpNum::MultiRead)
            {
                const auto & multi_request = dynamic_cast<const Coordination::ZooKeeperMultiRequest &>(*zk_request);
                std::vector<Coordination::Error> response_errors;
                response_errors.resize(multi_request.requests.size(), Coordination::Error::ZOK);
                new_deltas.emplace_back(
                    new_last_zxid, FailedMultiDelta{std::move(response_errors), Coordination::Error::ZNOAUTH});
            }
            else
            {
                new_deltas.emplace_back(new_last_zxid, Coordination::Error::ZNOAUTH);
            }
            return;
        }

        uint64_t * digest_ptr = keeper_context->digestEnabled() ? &new_digest : nullptr;
        new_deltas = preprocess(concrete_zk_request, *this, transaction->zxid, session_id, time, digest_ptr, *keeper_context);
    };

    callOnConcreteRequestType(*zk_request, preprocess_request);
}

template<typename Container>
KeeperResponsesForSessions KeeperStorage<Container>::processRequest(
    const Coordination::ZooKeeperRequestPtr & zk_request,
    int64_t session_id,
    std::optional<int64_t> new_last_zxid,
    bool check_acl,
    bool is_local)
{
    Stopwatch watch;
    SCOPE_EXIT({
        auto elapsed = watch.elapsedMicroseconds();
        if (auto elapsed_ms = elapsed / 1000; elapsed_ms > keeper_context->getCoordinationSettings()[CoordinationSetting::log_slow_cpu_threshold_ms])
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
    if (!is_local)
    {
        std::lock_guard lock(uncommitted_state.deltas_mutex);
        auto it = uncommitted_state.deltas.begin();
        for (; it != uncommitted_state.deltas.end() && it->zxid == commit_zxid; ++it)
            ;

        chassert(it == uncommitted_state.deltas.end() || it->zxid > commit_zxid);
        deltas.splice(deltas.end(), uncommitted_state.deltas, uncommitted_state.deltas.begin(), it);
    }

    KeeperStorageBase::DeltaRange deltas_range{.begin_it = deltas.begin(), .end_it = deltas.end()};

    KeeperResponsesForSessions results;

    /// ZooKeeper update sessions expirity for each request, not only for heartbeats
    session_expiry_queue.addNewSessionOrUpdate(session_id, session_and_timeout[session_id]);

    if (zk_request->getOpNum() == Coordination::OpNum::Close) /// Close request is special
    {
        for (const auto & delta : deltas)
        {
            if (std::holds_alternative<RemoveNodeDelta>(delta.operation))
            {
                auto responses = processWatchesImpl(delta.path, watches, list_watches, sessions_and_watchers, Coordination::Event::DELETED);
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
            response = process(dynamic_cast<const Coordination::ZooKeeperHeartbeatRequest &>(*zk_request), *this, deltas_range);
        }
        response->xid = zk_request->xid;
        response->zxid = commit_zxid;

        results.push_back(KeeperResponseForSession{session_id, response});
    }
    else /// normal requests proccession
    {
        const auto process_request = [&]<std::derived_from<Coordination::ZooKeeperRequest> T>(const T & concrete_zk_request)
        {
            Coordination::ZooKeeperResponsePtr response;

            if (is_local)
            {
                chassert(zk_request->isReadRequest());
                if (check_acl && !checkAuth(concrete_zk_request, *this, session_id, true))
                {
                    response = zk_request->makeResponse();
                    /// Original ZooKeeper always throws no auth, even when user provided some credentials
                    response->error = Coordination::Error::ZNOAUTH;
                }
                else
                {
                    std::shared_lock lock(storage_mutex);
                    response = processLocal(concrete_zk_request, *this, deltas_range);
                }
            }
            else
            {
                std::lock_guard lock(storage_mutex);
                response = process(concrete_zk_request, *this, deltas_range);
                if (!keeper_context->digestEnabledOnCommit())
                    nodes_digest = transaction_digest;
            }

            /// Watches for this requests are added to the watches lists
            if (zk_request->has_watch)
            {
                if (response->error == Coordination::Error::ZOK)
                {
                    static constexpr std::array list_requests{
                        Coordination::OpNum::List, Coordination::OpNum::SimpleList, Coordination::OpNum::FilteredList};

                    auto is_list_watch = std::ranges::contains(list_requests, zk_request->getOpNum());

                    auto & watches_type = is_list_watch ? list_watches : watches;

                    auto [watch_it, path_inserted] = watches_type.try_emplace(zk_request->getPath());
                    auto [path_it, session_inserted] = watch_it->second.emplace(session_id);
                    if (session_inserted)
                    {
                        ++total_watches_count;
                        sessions_and_watchers[session_id].emplace(WatchInfo{.path = watch_it->first, .is_list_watch = is_list_watch});
                    }
                }
                else if (response->error == Coordination::Error::ZNONODE && zk_request->getOpNum() == Coordination::OpNum::Exists)
                {
                    auto [watch_it, path_inserted] = watches.try_emplace(zk_request->getPath());
                    auto session_insert_info = watch_it->second.emplace(session_id);
                    if (session_insert_info.second)
                    {
                        ++total_watches_count;
                        sessions_and_watchers[session_id].emplace(WatchInfo{.path = watch_it->first, .is_list_watch = false});
                    }
                }
            }

            /// If this requests processed successfully we need to check watches
            if (response->error == Coordination::Error::ZOK)
            {
                auto watch_responses = processWatches(concrete_zk_request, deltas_range, watches, list_watches, sessions_and_watchers);
                total_watches_count -= watch_responses.size();
                results.insert(results.end(), watch_responses.begin(), watch_responses.end());
            }

            response->xid = zk_request->xid;
            response->zxid = commit_zxid;

            results.push_back(KeeperResponseForSession{session_id, response});
        };

        callOnConcreteRequestType(*zk_request, process_request);
    }

    updateStats();

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

template<typename Container>
void KeeperStorage<Container>::rollbackRequest(int64_t rollback_zxid, bool allow_missing) TSA_NO_THREAD_SAFETY_ANALYSIS
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

KeeperDigest KeeperStorageBase::getNodesDigest(bool committed, bool lock_transaction_mutex) const TSA_NO_THREAD_SAFETY_ANALYSIS
{
    if (!keeper_context->digestEnabled())
        return {.version = KeeperDigestVersion::NO_DIGEST};

    if (committed)
    {
        std::shared_lock storage_lock(storage_mutex);
        return {KEEPER_CURRENT_DIGEST_VERSION, nodes_digest};
    }

    std::unique_lock transaction_lock(transaction_mutex, std::defer_lock);
    if (lock_transaction_mutex)
        transaction_lock.lock();

    if (uncommitted_transactions.empty())
    {
        if (lock_transaction_mutex)
            transaction_lock.unlock();
        std::shared_lock storage_lock(storage_mutex);
        return {KEEPER_CURRENT_DIGEST_VERSION, nodes_digest};
    }

    return uncommitted_transactions.back().nodes_digest;
}

template<typename Container>
void KeeperStorage<Container>::removeDigest(const Node & node, const std::string_view path)
{
    nodes_digest -= node.getDigest(path);
}

template<typename Container>
void KeeperStorage<Container>::addDigest(const Node & node, const std::string_view path)
{
    nodes_digest += node.getDigest(path);
}

/// Allocate new session id with the specified timeouts
int64_t KeeperStorageBase::getSessionID(int64_t session_timeout_ms)
{
    auto result = session_id_counter++;
    session_and_timeout.emplace(result, session_timeout_ms);
    session_expiry_queue.addNewSessionOrUpdate(result, session_timeout_ms);
    return result;
}

/// Add session id. Used when restoring KeeperStorage from snapshot.
void KeeperStorageBase::addSessionID(int64_t session_id, int64_t session_timeout_ms)
{
    session_and_timeout.emplace(session_id, session_timeout_ms);
    session_expiry_queue.addNewSessionOrUpdate(session_id, session_timeout_ms);
}

std::vector<int64_t> KeeperStorageBase::getDeadSessions() const
{
    return session_expiry_queue.getExpiredSessions();
}

SessionAndTimeout KeeperStorageBase::getActiveSessions() const
{
    return session_and_timeout;
}

/// Turn on snapshot mode, so data inside Container is not deleted, but replaced with new version.
template<typename Container>
void KeeperStorage<Container>::enableSnapshotMode(size_t up_to_version)
{
    container.enableSnapshotMode(up_to_version);
}

/// Turn off snapshot mode.
template<typename Container>
void KeeperStorage<Container>::disableSnapshotMode()
{
    container.disableSnapshotMode();
}

template<typename Container>
KeeperStorage<Container>::Container::const_iterator KeeperStorage<Container>::getSnapshotIteratorBegin() const
{
    return container.begin();
}

/// Clear outdated data from internal container.
template<typename Container>
void KeeperStorage<Container>::clearGarbageAfterSnapshot()
{
    container.clearOutdatedNodes();
    stats.approximate_data_size.store(getApproximateDataSize(), std::memory_order_relaxed);
}

/// Introspection functions mostly used in 4-letter commands
template<typename Container>
uint64_t KeeperStorage<Container>::getNodesCount() const
{
    return container.size();
}

template<typename Container>
uint64_t KeeperStorage<Container>::getApproximateDataSize() const
{
    return container.getApproximateDataSize();
}

template<typename Container>
uint64_t KeeperStorage<Container>::getArenaDataSize() const
{
    return container.keyArenaSize();
}

uint64_t KeeperStorageBase::getWatchedPathsCount() const
{
    return watches.size() + list_watches.size();
}

void KeeperStorageBase::clearDeadWatches(int64_t session_id)
{
    /// Clear all watches for this session
    auto watches_it = sessions_and_watchers.find(session_id);
    if (watches_it == sessions_and_watchers.end())
        return;

    for (const auto [watch_path, is_list_watch] : watches_it->second)
    {
        if (is_list_watch)
        {
            auto list_watch = list_watches.find(watch_path);
            chassert(list_watch != list_watches.end());
            auto & list_watches_for_path = list_watch->second;
            list_watches_for_path.erase(session_id);
            if (list_watches_for_path.empty())
                list_watches.erase(list_watch);
        }
        else
        {
            auto watch = watches.find(watch_path);
            chassert(watch != watches.end());
            auto & watches_for_path = watch->second;
            watches_for_path.erase(session_id);
            if (watches_for_path.empty())
                watches.erase(watch);
        }
    }

    total_watches_count -= watches_it->second.size();
    sessions_and_watchers.erase(watches_it);
}

void KeeperStorageBase::dumpWatches(WriteBufferFromOwnString & buf) const
{
    for (const auto & [session_id, watches_paths] : sessions_and_watchers)
    {
        buf << "0x" << getHexUIntLowercase(session_id) << "\n";
        for (const auto [path, is_list_watch] : watches_paths)
            buf << "\t" << path << "\n";
    }
}

void KeeperStorageBase::dumpWatchesByPath(WriteBufferFromOwnString & buf) const
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

void KeeperStorageBase::dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const
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
    for (const auto & [session_id, ephemeral_paths] : committed_ephemerals)
    {
        buf << "0x" << getHexUIntLowercase(session_id) << "\n";
        write_str_set(ephemeral_paths);
    }
}

template<typename Container>
void KeeperStorage<Container>::updateStats()
{
    stats.nodes_count.store(getNodesCount(), std::memory_order_relaxed);
    stats.approximate_data_size.store(getApproximateDataSize(), std::memory_order_relaxed);
    stats.total_watches_count.store(getTotalWatchesCount(), std::memory_order_relaxed);
    stats.watched_paths_count.store(getWatchedPathsCount(), std::memory_order_relaxed);
    stats.sessions_with_watches_count.store(getSessionsWithWatchesCount(), std::memory_order_relaxed);
    stats.session_with_ephemeral_nodes_count.store(getSessionWithEphemeralNodesCount(), std::memory_order_relaxed);
    stats.total_emphemeral_nodes_count.store(getTotalEphemeralNodesCount(), std::memory_order_relaxed);
    stats.last_zxid.store(getZXID(), std::memory_order_relaxed);
}

const KeeperStorageStats & KeeperStorageBase::getStorageStats() const
{
    return stats;
}

uint64_t KeeperStorageBase::getTotalWatchesCount() const
{
    return total_watches_count;
}

uint64_t KeeperStorageBase::getSessionWithEphemeralNodesCount() const
{
    return committed_ephemerals.size();
}

uint64_t KeeperStorageBase::getSessionsWithWatchesCount() const
{
    return sessions_and_watchers.size();
}

uint64_t KeeperStorageBase::getTotalEphemeralNodesCount() const
{
    return committed_ephemeral_nodes;
}

template<typename Container>
void KeeperStorage<Container>::recalculateStats()
{
    container.recalculateDataSize();
    stats.approximate_data_size.store(getApproximateDataSize(), std::memory_order_relaxed);
}

bool KeeperStorageBase::checkDigest(const KeeperDigest & first, const KeeperDigest & second)
{
    if (first.version != second.version)
        return true;

    if (first.version == KeeperDigestVersion::NO_DIGEST)
        return true;

    return first.value == second.value;
}

UInt64 KeeperStorageBase::WatchInfoHash::operator()(WatchInfo info) const
{
    SipHash hash;
    hash.update(info.path);
    hash.update(info.is_list_watch);
    return hash.get64();
}

String KeeperStorageBase::generateDigest(const String & userdata)
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
