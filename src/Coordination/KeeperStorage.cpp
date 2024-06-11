// NOLINTBEGIN(clang-analyzer-optin.core.EnumCastOutOfRange)

#include <iterator>
#include <variant>
#include <IO/Operators.h>
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

#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperConstants.h>
#include <Coordination/KeeperReconfiguration.h>
#include <Coordination/KeeperStorage.h>
#include <Coordination/KeeperDispatcher.h>

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

bool fixupACL(
    const std::vector<Coordination::ACL> & request_acls,
    int64_t session_id,
    const KeeperStorage::UncommittedState & uncommitted_state,
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

KeeperStorage::ResponsesForSessions processWatchesImpl(
    const String & path, KeeperStorage::Watches & watches, KeeperStorage::Watches & list_watches, Coordination::Event event_type)
{
    KeeperStorage::ResponsesForSessions result;
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
            result.push_back(KeeperStorage::ResponseForSession{watcher_session, watch_response});

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
                result.push_back(KeeperStorage::ResponseForSession{watcher_session, watch_list_response});

            list_watches.erase(watch_it);
        }
    }
    return result;
}

// When this function is updated, update CURRENT_DIGEST_VERSION!!
uint64_t calculateDigest(std::string_view path, const KeeperStorage::Node & node)
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

KeeperStorage::Node & KeeperStorage::Node::operator=(const Node & other)
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

KeeperStorage::Node::Node(const Node & other)
{
    *this = other;
}

KeeperStorage::Node & KeeperStorage::Node::operator=(Node && other) noexcept
{
    if (this == &other)
        return *this;

    stats = other.stats;

    data = std::move(other.data);

    other.stats.data_size = 0;

    static_assert(std::is_nothrow_move_assignable_v<ChildrenSet>);
    children = std::move(other.children);

    return *this;
}

KeeperStorage::Node::Node(Node && other) noexcept
{
    *this = std::move(other);
}

bool KeeperStorage::Node::empty() const
{
    return stats.data_size == 0 && stats.mzxid == 0;
}

void KeeperStorage::Node::copyStats(const Coordination::Stat & stat)
{
    stats.czxid = stat.czxid;
    stats.mzxid = stat.mzxid;
    stats.pzxid = stat.pzxid;

    stats.mtime = stat.mtime;
    stats.setCtime(stat.ctime);

    stats.version = stat.version;
    stats.cversion = stat.cversion;
    stats.aversion = stat.aversion;

    if (stat.ephemeralOwner == 0)
        stats.setNumChildren(stat.numChildren);
    else
        stats.setEphemeralOwner(stat.ephemeralOwner);
}

void KeeperStorage::Node::setResponseStat(Coordination::Stat & response_stat) const
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

uint64_t KeeperStorage::Node::sizeInBytes() const
{
    return sizeof(Node) + children.size() * sizeof(StringRef) + stats.data_size;
}

void KeeperStorage::Node::setData(const String & new_data)
{
    stats.data_size = static_cast<uint32_t>(new_data.size());
    if (stats.data_size != 0)
    {
        data = std::unique_ptr<char[]>(new char[stats.data_size]);
        memcpy(data.get(), new_data.data(), stats.data_size);
    }
}

void KeeperStorage::Node::addChild(StringRef child_path)
{
    children.insert(child_path);
}

void KeeperStorage::Node::removeChild(StringRef child_path)
{
    children.erase(child_path);
}

void KeeperStorage::Node::invalidateDigestCache() const
{
    cached_digest = 0;
}

UInt64 KeeperStorage::Node::getDigest(const std::string_view path) const
{
    if (cached_digest == 0)
        cached_digest = calculateDigest(path, *this);

    return cached_digest;
};

void KeeperStorage::Node::shallowCopy(const KeeperStorage::Node & other)
{
    stats = other.stats;
    if (stats.data_size != 0)
    {
        data = std::unique_ptr<char[]>(new char[stats.data_size]);
        memcpy(data.get(), other.data.get(), stats.data_size);
    }

    cached_digest = other.cached_digest;
}

KeeperStorage::KeeperStorage(
    int64_t tick_time_ms, const String & superdigest_, const KeeperContextPtr & keeper_context_, const bool initialize_system_nodes)
    : session_expiry_queue(tick_time_ms), keeper_context(keeper_context_), superdigest(superdigest_)
{
    Node root_node;
    container.insert("/", root_node);
    addDigest(root_node, "/");

    if (initialize_system_nodes)
        initializeSystemNodes();
}

void KeeperStorage::initializeSystemNodes()
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
        addDigest(system_node, keeper_system_path);

        // update root and the digest based on it
        auto current_root_it = container.find("/");
        chassert(current_root_it != container.end());
        removeDigest(current_root_it->value, "/");
        auto updated_root_it = container.updateValue(
            "/",
            [](KeeperStorage::Node & node)
            {
                node.stats.increaseNumChildren();
                node.addChild(getBaseNodeName(keeper_system_path));
            }
        );
        addDigest(updated_root_it->value, "/");
    }

    // insert child system nodes
    for (const auto & [path, data] : keeper_context->getSystemNodesWithData())
    {
        chassert(path.starts_with(keeper_system_path));
        Node child_system_node;
        child_system_node.setData(data);
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

std::shared_ptr<KeeperStorage::Node> KeeperStorage::UncommittedState::tryGetNodeFromStorage(StringRef path) const
{
    std::shared_lock lock(storage.storage_mutex);
    if (auto node_it = storage.container.find(path); node_it != storage.container.end())
    {
        const auto & committed_node = node_it->value;
        auto node = std::make_shared<KeeperStorage::Node>();
        node->shallowCopy(committed_node);
        return node;
    }

    return nullptr;
}

void KeeperStorage::UncommittedState::applyDelta(const Delta & delta)
{
    chassert(!delta.path.empty());
    UncommittedNode * uncommitted_node = nullptr;

    if (auto it = nodes.find(delta.path); it != nodes.end())
    {
        uncommitted_node = &it->second;
    }
    else
    {
        if (auto storage_node = tryGetNodeFromStorage(delta.path))
        {
            auto [emplaced_it, _] = nodes.emplace(delta.path, UncommittedNode{.node = std::move(storage_node)});
            uncommitted_node = &emplaced_it->second;
        }
        else
        {
            auto [emplaced_it, _] = nodes.emplace(delta.path, UncommittedNode{.node = nullptr});
            uncommitted_node = &emplaced_it->second;
        }
    }

    std::visit(
        [&]<typename DeltaType>(const DeltaType & operation)
        {
            auto & [node, acls, applied_zxids] = *uncommitted_node;

            int64_t last_applied_zxid = 0;
            if (!applied_zxids.empty())
                last_applied_zxid = applied_zxids.back();

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
                chassert(node);
                node = nullptr;
            }
            else if constexpr (std::same_as<DeltaType, UpdateNodeStatDelta>)
            {
                chassert(node);
                node->invalidateDigestCache();
                node->stats = operation.new_stats;
            }
            else if constexpr (std::same_as<DeltaType, UpdateNodeDataDelta>)
            {
                assert(node);
                node->invalidateDigestCache();
                node->setData(operation.new_data);
            }
            else if constexpr (std::same_as<DeltaType, SetACLDelta>)
            {
                acls = operation.new_acls;
            }

            if (last_applied_zxid != delta.zxid)
                last_applied_zxid = applied_zxids.emplace_back(delta.zxid);
        },
        delta.operation);
}

bool KeeperStorage::UncommittedState::hasACL(int64_t session_id, bool is_local, std::function<bool(const AuthID &)> predicate) const
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
        std::shared_lock lock(storage.storage_mutex);
        return check_auth(storage.session_and_auth[session_id]);
    }

    // check if there are uncommitted
    const auto auth_it = session_and_auth.find(session_id);
    if (auth_it == session_and_auth.end())
        return false;

    if (check_auth(auth_it->second))
        return true;

    std::lock_guard lock(storage.storage_mutex);
    return check_auth(storage.session_and_auth[session_id]);
}

void KeeperStorage::UncommittedState::rollbackDelta(const Delta & delta)
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

            if (applied_zxids.back() == delta.zxid)
                applied_zxids.pop_back();
        },
        delta.operation);
}

void KeeperStorage::UncommittedState::applyDeltas(const std::list<Delta> & new_deltas)
{
    for (const auto & delta : new_deltas)
    {
        if (!delta.path.empty())
        {
            applyDelta(delta);
        }
        else if (const auto * auth_delta = std::get_if<AddAuthDelta>(&delta.operation))
        {
            auto & uncommitted_auth = session_and_auth[auth_delta->session_id];
            uncommitted_auth.push_back(std::pair{delta.zxid, auth_delta->auth_id});
        }
    }
}

void KeeperStorage::UncommittedState::addDeltas(std::list<Delta> new_deltas)
{
    std::lock_guard lock(deltas_mutex);
    deltas.splice(deltas.end(), std::move(new_deltas));
}

void KeeperStorage::UncommittedState::cleanup(int64_t commit_zxid)
{
    for (auto it = nodes.begin(); it != nodes.end();)
    {
        auto & applied_zxids = it->second.applied_zxids;
        std::erase_if(applied_zxids, [commit_zxid](auto current_zxid) { return current_zxid <= commit_zxid; });
        if (applied_zxids.empty())
            it = nodes.erase(it);
        else
            ++it;
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

void KeeperStorage::UncommittedState::rollback(int64_t rollback_zxid)
{
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

void KeeperStorage::UncommittedState::rollback(std::list<Delta> rollback_deltas)
{
    // we need to undo ephemeral mapping modifications
    // CreateNodeDelta added ephemeral for session id -> we need to remove it
    // RemoveNodeDelta removed ephemeral for session id -> we need to add it back
    for (auto & delta : rollback_deltas)
    {
        if (!delta.path.empty())
        {
            std::visit(
                [&]<typename DeltaType>(const DeltaType & operation)
                {
                    if constexpr (std::same_as<DeltaType, CreateNodeDelta>)
                    {
                        if (operation.stat.ephemeralOwner != 0)
                            storage.unregisterEphemeralPath(operation.stat.ephemeralOwner, delta.path);
                    }
                    else if constexpr (std::same_as<DeltaType, RemoveNodeDelta>)
                    {
                        if (operation.stat.ephemeralOwner() != 0)
                        {
                            std::lock_guard lock(storage.ephemerals_mutex);
                            storage.ephemerals[operation.stat.ephemeralOwner()].emplace(delta.path);
                        }
                    }
                },
                delta.operation);

            rollbackDelta(delta);
        }
        else if (auto * add_auth = std::get_if<AddAuthDelta>(&delta.operation))
        {
            auto & uncommitted_auth = session_and_auth[add_auth->session_id];
            if (uncommitted_auth.back().second == add_auth->auth_id)
            {
                uncommitted_auth.pop_back();
                if (uncommitted_auth.empty())
                    session_and_auth.erase(add_auth->session_id);
            }
        }
    }
}

std::shared_ptr<KeeperStorage::Node> KeeperStorage::UncommittedState::getNode(StringRef path) const
{
    if (auto node_it = nodes.find(path.toView()); node_it != nodes.end())
        return node_it->second.node;

    std::shared_ptr<KeeperStorage::Node> node = tryGetNodeFromStorage(path);

    if (node)
        nodes.emplace(std::string{path}, UncommittedNode{node});

    return node;
}

Coordination::ACLs KeeperStorage::UncommittedState::getACLs(StringRef path) const
{
    if (auto node_it = nodes.find(path.toView()); node_it != nodes.end())
    {
        if (!node_it->second.acls.has_value())
            node_it->second.acls.emplace(storage.acl_map.convertNumber(node_it->second.node->stats.acl_id));

        return *node_it->second.acls;
    }

    std::shared_ptr<KeeperStorage::Node> node = tryGetNodeFromStorage(path);

    if (node)
    {
        auto [it, inserted] = nodes.emplace(std::string{path}, UncommittedNode{node});
        it->second.acls = storage.acl_map.convertNumber(node->stats.acl_id);
        return *it->second.acls;
    }

    return {};
}

void KeeperStorage::UncommittedState::forEachAuthInSession(int64_t session_id, std::function<void(const AuthID &)> func) const
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

            func(*auth_ptr);
        }
    };

    {
        std::lock_guard lock(storage.storage_mutex);
        // for committed
        if (auto auth_it = storage.session_and_auth.find(session_id); auth_it != storage.session_and_auth.end())
            call_for_each_auth(auth_it->second);
    }

    // for uncommitted
    if (auto auth_it = session_and_auth.find(session_id); auth_it != session_and_auth.end())
        call_for_each_auth(auth_it->second);
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

/// Get current committed zxid
int64_t KeeperStorage::getZXID() const
{
    std::lock_guard lock(transaction_mutex);
    return zxid;
}

int64_t KeeperStorage::getNextZXIDLocked() const
{
    if (uncommitted_transactions.empty())
        return zxid + 1;

    return uncommitted_transactions.back().zxid + 1;
}

int64_t KeeperStorage::getNextZXID() const
{
    std::lock_guard lock(transaction_mutex);
    return getNextZXIDLocked();
}

void KeeperStorage::applyUncommittedState(KeeperStorage & other, int64_t last_log_idx) TSA_NO_THREAD_SAFETY_ANALYSIS
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

        other.uncommitted_state.applyDelta(*it);
        other.uncommitted_state.deltas.push_back(*it);
    }
}

Coordination::Error KeeperStorage::commit(std::list<Delta> deltas)
{
    // Deltas are added with increasing ZXIDs
    // If there are no deltas for the commit_zxid (e.g. read requests), we instantly return
    // on first delta
    for (auto & delta : deltas)
    {
        auto result = std::visit(
            [&, &path = delta.path]<typename DeltaType>(DeltaType & operation) -> Coordination::Error
            {
                if constexpr (std::same_as<DeltaType, KeeperStorage::CreateNodeDelta>)
                {
                    if (!createNode(
                            path,
                            std::move(operation.data),
                            operation.stat,
                            std::move(operation.acls)))
                        onStorageInconsistency();

                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, KeeperStorage::UpdateNodeStatDelta> || std::same_as<DeltaType, KeeperStorage::UpdateNodeDataDelta>)
                {
                    auto node_it = container.find(path);
                    if (node_it == container.end())
                        onStorageInconsistency();

                    if (operation.version != -1 && operation.version != node_it->value.stats.version)
                        onStorageInconsistency();

                    removeDigest(node_it->value, path);
                    auto updated_node = container.updateValue(path, [&](auto & node) {
                        if constexpr (std::same_as<DeltaType, KeeperStorage::UpdateNodeStatDelta>)
                            node.stats = operation.new_stats;
                        else
                            node.setData(std::move(operation.new_data));
                    });
                    addDigest(updated_node->value, path);

                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, KeeperStorage::RemoveNodeDelta>)
                {
                    if (!removeNode(path, operation.version))
                        onStorageInconsistency();

                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, KeeperStorage::SetACLDelta>)
                {
                    auto node_it = container.find(path);
                    if (node_it == container.end())
                        onStorageInconsistency();

                    if (operation.version != -1 && operation.version != node_it->value.stats.aversion)
                        onStorageInconsistency();

                    acl_map.removeUsage(node_it->value.stats.acl_id);

                    uint64_t acl_id = acl_map.convertACLs(operation.new_acls);
                    acl_map.addUsage(acl_id);

                    container.updateValue(path, [acl_id](KeeperStorage::Node & node) { node.stats.acl_id = acl_id; });

                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, KeeperStorage::ErrorDelta>)
                    return operation.error;
                else if constexpr (std::same_as<DeltaType, KeeperStorage::SubDeltaEnd>)
                {
                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, KeeperStorage::AddAuthDelta>)
                {
                    session_and_auth[operation.session_id].emplace_back(std::move(*operation.auth_id));
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
    }

    return Coordination::Error::ZOK;
}

bool KeeperStorage::createNode(
    const std::string & path,
    String data,
    const Coordination::Stat & stat,
    Coordination::ACLs node_acls)
{
    auto parent_path = parentNodePath(path);
    auto node_it = container.find(parent_path);

    if (node_it == container.end())
        return false;

    if (node_it->value.stats.isEphemeral())
        return false;

    if (container.contains(path))
        return false;

    KeeperStorage::Node created_node;

    uint64_t acl_id = acl_map.convertACLs(node_acls);
    acl_map.addUsage(acl_id);

    created_node.stats.acl_id = acl_id;
    created_node.copyStats(stat);
    created_node.setData(data);
    auto [map_key, _] = container.insert(path, created_node);
    /// Take child path from key owned by map.
    auto child_path = getBaseNodeName(map_key->getKey());
    container.updateValue(
            parent_path,
            [child_path](KeeperStorage::Node & parent)
            {
                parent.addChild(child_path);
                chassert(parent.stats.numChildren() == static_cast<int32_t>(parent.getChildren().size()));
            }
    );

    addDigest(map_key->getMapped()->value, map_key->getKey().toView());
    return true;
};

bool KeeperStorage::removeNode(const std::string & path, int32_t version)
{
    auto node_it = container.find(path);
    if (node_it == container.end())
        return false;

    if (version != -1 && version != node_it->value.stats.version)
        return false;

    if (node_it->value.stats.numChildren())
        return false;

    KeeperStorage::Node prev_node;
    prev_node.shallowCopy(node_it->value);
    acl_map.removeUsage(node_it->value.stats.acl_id);

    container.updateValue(
        parentNodePath(path),
        [child_basename = getBaseNodeName(node_it->key)](KeeperStorage::Node & parent)
        {
            parent.removeChild(child_basename);
            chassert(parent.stats.numChildren() == static_cast<int32_t>(parent.getChildren().size()));
        }
    );

    container.erase(path);

    removeDigest(prev_node, path);
    return true;
}

struct KeeperStorageRequestProcessor
{
    Coordination::ZooKeeperRequestPtr zk_request;

    explicit KeeperStorageRequestProcessor(const Coordination::ZooKeeperRequestPtr & zk_request_) : zk_request(zk_request_) { }
    virtual Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const = 0;
    virtual std::list<KeeperStorage::Delta>
    preprocess(KeeperStorage & /*storage*/, int64_t /*zxid*/, int64_t /*session_id*/, int64_t /*time*/, uint64_t & /*digest*/, const KeeperContext & /*keeper_context*/) const
    {
        return {};
    }

    // process the request using locally committed data
    virtual Coordination::ZooKeeperResponsePtr
    processLocal(KeeperStorage & /*storage*/, std::list<KeeperStorage::Delta> /* deltas */) const
    {
        throw Exception{DB::ErrorCodes::LOGICAL_ERROR, "Cannot process the request locally"};
    }

    virtual KeeperStorage::ResponsesForSessions
    processWatches(KeeperStorage::Watches & /*watches*/, KeeperStorage::Watches & /*list_watches*/) const
    {
        return {};
    }
    virtual bool checkAuth(KeeperStorage & /*storage*/, int64_t /*session_id*/, bool /*is_local*/) const { return true; }

    virtual ~KeeperStorageRequestProcessor() = default;
};

struct KeeperStorageHeartbeatRequestProcessor final : public KeeperStorageRequestProcessor
{
    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;
    Coordination::ZooKeeperResponsePtr
    process(KeeperStorage & /* storage */, std::list<KeeperStorage::Delta> /* deltas */) const override
    {
        return zk_request->makeResponse();
    }
};

struct KeeperStorageSyncRequestProcessor final : public KeeperStorageRequestProcessor
{
    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;
    Coordination::ZooKeeperResponsePtr
    process(KeeperStorage & /* storage */, std::list<KeeperStorage::Delta> /* deltas */) const override
    {
        auto response = zk_request->makeResponse();
        dynamic_cast<Coordination::ZooKeeperSyncResponse &>(*response).path
            = dynamic_cast<Coordination::ZooKeeperSyncRequest &>(*zk_request).path;
        return response;
    }
};

namespace
{

Coordination::ACLs getNodeACLs(KeeperStorage & storage, StringRef path, bool is_local)
{
    if (is_local)
    {
        std::shared_lock lock(storage.storage_mutex);
        auto node_it = storage.container.find(path);
        if (node_it == storage.container.end())
            return {};

        return storage.acl_map.convertNumber(node_it->value.stats.acl_id);
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

KeeperStorage::UpdateNodeStatDelta::UpdateNodeStatDelta(const KeeperStorage::Node & node)
    : old_stats(node.stats)
    , new_stats(node.stats)
{}

bool KeeperStorage::checkACL(StringRef path, int32_t permission, int64_t session_id, bool is_local)
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

void KeeperStorage::unregisterEphemeralPath(int64_t session_id, const std::string & path)
{
    std::lock_guard ephemerals_lock(ephemerals_mutex);
    auto ephemerals_it = ephemerals.find(session_id);
    if (ephemerals_it == ephemerals.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Session {} is missing ephemeral path", session_id);

    ephemerals_it->second.erase(path);
    if (ephemerals_it->second.empty())
        ephemerals.erase(ephemerals_it);
}

struct KeeperStorageCreateRequestProcessor final : public KeeperStorageRequestProcessor
{
    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;

    KeeperStorage::ResponsesForSessions
    processWatches(KeeperStorage::Watches & watches, KeeperStorage::Watches & list_watches) const override
    {
        return processWatchesImpl(zk_request->getPath(), watches, list_watches, Coordination::Event::CREATED);
    }

    bool checkAuth(KeeperStorage & storage, int64_t session_id, bool is_local) const override
    {
        auto path = zk_request->getPath();
        return storage.checkACL(parentNodePath(path), Coordination::ACL::Create, session_id, is_local);
    }

    std::list<KeeperStorage::Delta>
    preprocess(KeeperStorage & storage, int64_t zxid, int64_t session_id, int64_t time, uint64_t & digest, const KeeperContext & keeper_context) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperCreateRequest);
        Coordination::ZooKeeperCreateRequest & request = dynamic_cast<Coordination::ZooKeeperCreateRequest &>(*zk_request);

        std::list<KeeperStorage::Delta> new_deltas;

        auto parent_path = parentNodePath(request.path);
        auto parent_node = storage.uncommitted_state.getNode(parent_path);
        if (parent_node == nullptr)
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNONODE}};

        else if (parent_node->stats.isEphemeral())
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNOCHILDRENFOREPHEMERALS}};

        std::string path_created = request.path;
        if (request.is_sequential)
        {
            if (request.not_exists)
                return {KeeperStorage::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};

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
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};
        }

        if (storage.uncommitted_state.getNode(path_created))
        {
            if (zk_request->getOpNum() == Coordination::OpNum::CreateIfNotExists)
                return new_deltas;

            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNODEEXISTS}};
        }

        if (getBaseNodeName(path_created).size == 0)
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};

        Coordination::ACLs node_acls;
        if (!fixupACL(request.acls, session_id, storage.uncommitted_state, node_acls))
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZINVALIDACL}};

        if (request.is_ephemeral)
        {
            std::lock_guard lock(storage.ephemerals_mutex);
            storage.ephemerals[session_id].emplace(path_created);
        }

        int32_t parent_cversion = request.parent_cversion;

        KeeperStorage::UpdateNodeStatDelta update_parent_delta(*parent_node);
        update_parent_delta.new_stats.increaseNumChildren();

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
        stat.ephemeralOwner = request.is_ephemeral ? session_id : 0;

        new_deltas.emplace_back(
            std::move(path_created),
            zxid,
            KeeperStorage::CreateNodeDelta{stat, std::move(node_acls), request.data});


        digest = storage.calculateNodesDigest(digest, new_deltas);
        return new_deltas;
    }

    Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const override 
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperCreateResponse & response = dynamic_cast<Coordination::ZooKeeperCreateResponse &>(*response_ptr);

        if (deltas.empty())
        {
            response.path_created = zk_request->getPath();
            response.error = Coordination::Error::ZOK;
            return response_ptr;
        }

        std::string created_path;
        auto create_delta_it = std::find_if(
            deltas.begin(),
            deltas.end(),
            [](const auto & delta)
            { return std::holds_alternative<KeeperStorage::CreateNodeDelta>(delta.operation); });

        if (create_delta_it != deltas.end())
            created_path = create_delta_it->path;

        if (const auto result = storage.commit(std::move(deltas)); result != Coordination::Error::ZOK)
        {
            response.error = result;
            return response_ptr;
        }

        response.path_created = std::move(created_path);
        response.error = Coordination::Error::ZOK;
        return response_ptr;
    }
};

struct KeeperStorageGetRequestProcessor final : public KeeperStorageRequestProcessor
{
    bool checkAuth(KeeperStorage & storage, int64_t session_id, bool is_local) const override
    {
        return storage.checkACL(zk_request->getPath(), Coordination::ACL::Read, session_id, is_local);
    }

    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;

    std::list<KeeperStorage::Delta>
    preprocess(KeeperStorage & storage, int64_t zxid, int64_t /*session_id*/, int64_t /*time*/, uint64_t & /*digest*/, const KeeperContext & /*keeper_context*/) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperGetRequest);
        Coordination::ZooKeeperGetRequest & request = dynamic_cast<Coordination::ZooKeeperGetRequest &>(*zk_request);

        if (request.path == Coordination::keeper_api_feature_flags_path
            || request.path == Coordination::keeper_config_path
            || request.path == Coordination::keeper_availability_zone_path)
            return {};

        if (!storage.uncommitted_state.getNode(request.path))
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNONODE}};

        return {};
    }

    template <bool local>
    Coordination::ZooKeeperResponsePtr processImpl(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const 
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperGetResponse & response = dynamic_cast<Coordination::ZooKeeperGetResponse &>(*response_ptr);
        Coordination::ZooKeeperGetRequest & request = dynamic_cast<Coordination::ZooKeeperGetRequest &>(*zk_request);

        if constexpr (!local)
        {
            if (const auto result = storage.commit(std::move(deltas)); result != Coordination::Error::ZOK)
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


    Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const override 
    {
        return processImpl<false>(storage, std::move(deltas));
    }

    Coordination::ZooKeeperResponsePtr processLocal(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const override 
    {
        ProfileEvents::increment(ProfileEvents::KeeperGetRequest);
        return processImpl<true>(storage, std::move(deltas));
    }
};

struct KeeperStorageRemoveRequestProcessor final : public KeeperStorageRequestProcessor
{
    bool checkAuth(KeeperStorage & storage, int64_t session_id, bool is_local) const override
    {
        return storage.checkACL(parentNodePath(zk_request->getPath()), Coordination::ACL::Delete, session_id, is_local);
    }

    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;
    std::list<KeeperStorage::Delta>
    preprocess(KeeperStorage & storage, int64_t zxid, int64_t /*session_id*/, int64_t /*time*/, uint64_t & digest, const KeeperContext & keeper_context) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperRemoveRequest);
        Coordination::ZooKeeperRemoveRequest & request = dynamic_cast<Coordination::ZooKeeperRemoveRequest &>(*zk_request);

        std::list<KeeperStorage::Delta> new_deltas;

        if (Coordination::matchPath(request.path, keeper_system_path) != Coordination::PathMatchResult::NOT_MATCH)
        {
            auto error_msg = fmt::format("Trying to delete an internal Keeper path ({}) which is not allowed", request.path);

            handleSystemNodeModification(keeper_context, error_msg);
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};
        }

        auto parent_path = parentNodePath(request.path);
        auto parent_node = storage.uncommitted_state.getNode(parent_path);

        KeeperStorage::UpdateNodeStatDelta update_parent_delta(*parent_node);

        const auto add_parent_update_delta = [&]
        {
            new_deltas.emplace_back(
                std::string{parent_path},
                zxid,
                std::move(update_parent_delta)
            );
        };

        const auto update_parent_pzxid = [&]()
        {
            if (!parent_node)
                return;

            if (update_parent_delta.old_stats.pzxid < zxid)
                update_parent_delta.new_stats.pzxid = zxid;
        };

        auto node = storage.uncommitted_state.getNode(request.path);

        if (!node)
        {
            if (request.restored_from_zookeeper_log)
            {
                update_parent_pzxid();
                add_parent_update_delta();
            }
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNONODE}};
        }
        else if (request.version != -1 && request.version != node->stats.version)
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZBADVERSION}};
        else if (node->stats.numChildren() != 0)
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNOTEMPTY}};

        if (request.restored_from_zookeeper_log)
            update_parent_pzxid();

        ++update_parent_delta.new_stats.cversion;
        update_parent_delta.new_stats.decreaseNumChildren();
        add_parent_update_delta();

        new_deltas.emplace_back(
            request.path,
            zxid,
            KeeperStorage::RemoveNodeDelta{request.version, node->stats, storage.uncommitted_state.getACLs(request.path), std::string{node->getData()}});

        if (node->stats.isEphemeral())
            storage.unregisterEphemeralPath(node->stats.ephemeralOwner(), request.path);

        digest = storage.calculateNodesDigest(digest, new_deltas);

        return new_deltas;
    }

    Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const override 
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperRemoveResponse & response = dynamic_cast<Coordination::ZooKeeperRemoveResponse &>(*response_ptr);

        response.error = storage.commit(std::move(deltas));
        return response_ptr;
    }

    KeeperStorage::ResponsesForSessions
    processWatches(KeeperStorage::Watches & watches, KeeperStorage::Watches & list_watches) const override
    {
        return processWatchesImpl(zk_request->getPath(), watches, list_watches, Coordination::Event::DELETED);
    }
};

struct KeeperStorageExistsRequestProcessor final : public KeeperStorageRequestProcessor
{
    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;

    std::list<KeeperStorage::Delta>
    preprocess(KeeperStorage & storage, int64_t zxid, int64_t /*session_id*/, int64_t /*time*/, uint64_t & /*digest*/, const KeeperContext & /*keeper_context*/) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperExistsRequest);
        Coordination::ZooKeeperExistsRequest & request = dynamic_cast<Coordination::ZooKeeperExistsRequest &>(*zk_request);

        if (!storage.uncommitted_state.getNode(request.path))
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNONODE}};

        return {};
    }

    template <bool local>
    Coordination::ZooKeeperResponsePtr processImpl(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const 
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperExistsResponse & response = dynamic_cast<Coordination::ZooKeeperExistsResponse &>(*response_ptr);
        Coordination::ZooKeeperExistsRequest & request = dynamic_cast<Coordination::ZooKeeperExistsRequest &>(*zk_request);

        if constexpr (!local)
        {
            if (const auto result = storage.commit(std::move(deltas)); result != Coordination::Error::ZOK)
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

    Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const override 
    {
        return processImpl<false>(storage, std::move(deltas));
    }

    Coordination::ZooKeeperResponsePtr processLocal(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const override 
    {
        ProfileEvents::increment(ProfileEvents::KeeperExistsRequest);
        return processImpl<true>(storage, std::move(deltas));
    }
};

struct KeeperStorageSetRequestProcessor final : public KeeperStorageRequestProcessor
{
    bool checkAuth(KeeperStorage & storage, int64_t session_id, bool is_local) const override
    {
        return storage.checkACL(zk_request->getPath(), Coordination::ACL::Write, session_id, is_local);
    }

    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;
    std::list<KeeperStorage::Delta>
    preprocess(KeeperStorage & storage, int64_t zxid, int64_t /*session_id*/, int64_t time, uint64_t & digest, const KeeperContext & keeper_context) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperSetRequest);
        Coordination::ZooKeeperSetRequest & request = dynamic_cast<Coordination::ZooKeeperSetRequest &>(*zk_request);

        std::list<KeeperStorage::Delta> new_deltas;

        if (Coordination::matchPath(request.path, keeper_system_path) != Coordination::PathMatchResult::NOT_MATCH)
        {
            auto error_msg = fmt::format("Trying to update an internal Keeper path ({}) which is not allowed", request.path);

            handleSystemNodeModification(keeper_context, error_msg);
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};
        }

        if (!storage.uncommitted_state.getNode(request.path))
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNONODE}};

        auto node = storage.uncommitted_state.getNode(request.path);

        if (request.version != -1 && request.version != node->stats.version)
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZBADVERSION}};

        KeeperStorage::UpdateNodeStatDelta node_delta(*node);
        node_delta.version = request.version;
        auto & new_stats = node_delta.new_stats;
        new_stats.version++;
        new_stats.mzxid = zxid;
        new_stats.mtime = time;

        new_deltas.emplace_back(request.path, zxid, std::move(node_delta));
        new_deltas.emplace_back(
            request.path,
            zxid,
            KeeperStorage::UpdateNodeDataDelta{.old_data = std::string{node->getData()}, .new_data = request.data, .version = request.version});

        auto parent_path = parentNodePath(request.path);
        auto parent_node = storage.uncommitted_state.getNode(parent_path);
        KeeperStorage::UpdateNodeStatDelta parent_delta(*parent_node);
        ++parent_delta.new_stats.cversion;
        new_deltas.emplace_back(std::string{parent_path}, zxid, std::move(parent_delta));

        digest = storage.calculateNodesDigest(digest, new_deltas);
        return new_deltas;
    }

    Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const override 
    {
        auto & container = storage.container;

        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperSetResponse & response = dynamic_cast<Coordination::ZooKeeperSetResponse &>(*response_ptr);
        Coordination::ZooKeeperSetRequest & request = dynamic_cast<Coordination::ZooKeeperSetRequest &>(*zk_request);

        if (const auto result = storage.commit(std::move(deltas)); result != Coordination::Error::ZOK)
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

    KeeperStorage::ResponsesForSessions
    processWatches(KeeperStorage::Watches & watches, KeeperStorage::Watches & list_watches) const override
    {
        return processWatchesImpl(zk_request->getPath(), watches, list_watches, Coordination::Event::CHANGED);
    }
};

struct KeeperStorageListRequestProcessor final : public KeeperStorageRequestProcessor
{
    bool checkAuth(KeeperStorage & storage, int64_t session_id, bool is_local) const override
    {
        return storage.checkACL(zk_request->getPath(), Coordination::ACL::Read, session_id, is_local);
    }

    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;
    std::list<KeeperStorage::Delta>
    preprocess(KeeperStorage & storage, int64_t zxid, int64_t /*session_id*/, int64_t /*time*/, uint64_t & /*digest*/, const KeeperContext & /*keeper_context*/) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperListRequest);
        Coordination::ZooKeeperListRequest & request = dynamic_cast<Coordination::ZooKeeperListRequest &>(*zk_request);

        if (!storage.uncommitted_state.getNode(request.path))
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNONODE}};

        return {};
    }


    template <bool local>
    Coordination::ZooKeeperResponsePtr processImpl(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const 
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperListResponse & response = dynamic_cast<Coordination::ZooKeeperListResponse &>(*response_ptr);
        Coordination::ZooKeeperListRequest & request = dynamic_cast<Coordination::ZooKeeperListRequest &>(*zk_request);

        if constexpr (!local)
        {
            if (const auto result = storage.commit(std::move(deltas)); result != Coordination::Error::ZOK)
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

            const auto & children = node_it->value.getChildren();
            response.names.reserve(children.size());

            const auto add_child = [&](const auto child) 
            {
                using enum Coordination::ListRequestType;

                auto list_request_type = ALL;
                if (auto * filtered_list = dynamic_cast<Coordination::ZooKeeperFilteredListRequest *>(&request))
                    list_request_type = filtered_list->list_request_type;

                if (list_request_type == ALL)
                    return true;

                auto child_path = (std::filesystem::path(request.path) / child.toView()).generic_string();
                auto child_it = container.find(child_path);
                if (child_it == container.end())
                    onStorageInconsistency();

                const auto is_ephemeral = child_it->value.stats.isEphemeral();
                return (is_ephemeral && list_request_type == EPHEMERAL_ONLY) || (!is_ephemeral && list_request_type == PERSISTENT_ONLY);
            };

            for (const auto child : children)
            {
                if (add_child(child))
                    response.names.push_back(child.toString());
            }

            node_it->value.setResponseStat(response.stat);
            response.error = Coordination::Error::ZOK;
        }

        return response_ptr;
    }

    Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const override  
    {
        return processImpl<false>(storage, std::move(deltas));
    }

    Coordination::ZooKeeperResponsePtr processLocal(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const override  
    {
        ProfileEvents::increment(ProfileEvents::KeeperListRequest);
        return processImpl<true>(storage, std::move(deltas));
    }
};

struct KeeperStorageCheckRequestProcessor final : public KeeperStorageRequestProcessor
{
    explicit KeeperStorageCheckRequestProcessor(const Coordination::ZooKeeperRequestPtr & zk_request_)
        : KeeperStorageRequestProcessor(zk_request_)
    {
        check_not_exists = zk_request->getOpNum() == Coordination::OpNum::CheckNotExists;
    }

    bool checkAuth(KeeperStorage & storage, int64_t session_id, bool is_local) const override
    {
        auto path = zk_request->getPath();
        return storage.checkACL(check_not_exists ? parentNodePath(path) : path, Coordination::ACL::Read, session_id, is_local);
    }

    std::list<KeeperStorage::Delta>
    preprocess(KeeperStorage & storage, int64_t zxid, int64_t /*session_id*/, int64_t /*time*/, uint64_t & /*digest*/, const KeeperContext & /*keeper_context*/) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperCheckRequest);

        Coordination::ZooKeeperCheckRequest & request = dynamic_cast<Coordination::ZooKeeperCheckRequest &>(*zk_request);

        auto node = storage.uncommitted_state.getNode(request.path);
        if (check_not_exists)
        {
            if (node && (request.version == -1 || request.version == node->stats.version))
                return {KeeperStorage::Delta{zxid, Coordination::Error::ZNODEEXISTS}};
        }
        else
        {
            if (!node)
                return {KeeperStorage::Delta{zxid, Coordination::Error::ZNONODE}};

            if (request.version != -1 && request.version != node->stats.version)
                return {KeeperStorage::Delta{zxid, Coordination::Error::ZBADVERSION}};
        }

        return {};
    }

    template <bool local>
    Coordination::ZooKeeperResponsePtr processImpl(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const 
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperCheckResponse & response = dynamic_cast<Coordination::ZooKeeperCheckResponse &>(*response_ptr);
        Coordination::ZooKeeperCheckRequest & request = dynamic_cast<Coordination::ZooKeeperCheckRequest &>(*zk_request);

        if constexpr (!local)
        {
            if (const auto result = storage.commit(std::move(deltas)); result != Coordination::Error::ZOK)
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
            if (node_it != container.end() && (request.version == -1 || request.version == node_it->value.stats.version))
                on_error(Coordination::Error::ZNODEEXISTS);
            else
                response.error = Coordination::Error::ZOK;
        }
        else
        {
            if (node_it == container.end())
                on_error(Coordination::Error::ZNONODE);
            else if (request.version != -1 && request.version != node_it->value.stats.version)
                on_error(Coordination::Error::ZBADVERSION);
            else
                response.error = Coordination::Error::ZOK;
        }

        return response_ptr;
    }

    Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const override 
    {
        return processImpl<false>(storage, std::move(deltas));
    }

    Coordination::ZooKeeperResponsePtr processLocal(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const override 
    {
        ProfileEvents::increment(ProfileEvents::KeeperCheckRequest);
        return processImpl<true>(storage, std::move(deltas));
    }

private:
    bool check_not_exists;
};


struct KeeperStorageSetACLRequestProcessor final : public KeeperStorageRequestProcessor
{
    bool checkAuth(KeeperStorage & storage, int64_t session_id, bool is_local) const override
    {
        return storage.checkACL(zk_request->getPath(), Coordination::ACL::Admin, session_id, is_local);
    }

    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;

    std::list<KeeperStorage::Delta>
    preprocess(KeeperStorage & storage, int64_t zxid, int64_t session_id, int64_t /*time*/, uint64_t & digest, const KeeperContext & keeper_context) const override
    {
        Coordination::ZooKeeperSetACLRequest & request = dynamic_cast<Coordination::ZooKeeperSetACLRequest &>(*zk_request);

        if (Coordination::matchPath(request.path, keeper_system_path) != Coordination::PathMatchResult::NOT_MATCH)
        {
            auto error_msg = fmt::format("Trying to update an internal Keeper path ({}) which is not allowed", request.path);

            handleSystemNodeModification(keeper_context, error_msg);
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};
        }

        auto & uncommitted_state = storage.uncommitted_state;
        if (!uncommitted_state.getNode(request.path))
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNONODE}};

        auto node = uncommitted_state.getNode(request.path);

        if (request.version != -1 && request.version != node->stats.aversion)
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZBADVERSION}};


        Coordination::ACLs node_acls;
        if (!fixupACL(request.acls, session_id, uncommitted_state, node_acls))
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZINVALIDACL}};

        KeeperStorage::UpdateNodeStatDelta update_stat_delta(*node);
        ++update_stat_delta.new_stats.aversion;
        std::list<KeeperStorage::Delta> new_deltas{
            {request.path,
             zxid,
             KeeperStorage::SetACLDelta{std::move(node_acls), uncommitted_state.getACLs(request.path), request.version}},
            {request.path, zxid, std::move(update_stat_delta)}};

        digest = storage.calculateNodesDigest(digest, new_deltas);

        return new_deltas;
    }

    Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const override 
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperSetACLResponse & response = dynamic_cast<Coordination::ZooKeeperSetACLResponse &>(*response_ptr);
        Coordination::ZooKeeperSetACLRequest & request = dynamic_cast<Coordination::ZooKeeperSetACLRequest &>(*zk_request);

        if (const auto result = storage.commit(std::move(deltas)); result != Coordination::Error::ZOK)
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

struct KeeperStorageGetACLRequestProcessor final : public KeeperStorageRequestProcessor
{
    bool checkAuth(KeeperStorage & storage, int64_t session_id, bool is_local) const override
    {
        return storage.checkACL(zk_request->getPath(), Coordination::ACL::Admin | Coordination::ACL::Read, session_id, is_local);
    }

    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;

    std::list<KeeperStorage::Delta>
    preprocess(KeeperStorage & storage, int64_t zxid, int64_t /*session_id*/, int64_t /*time*/, uint64_t & /*digest*/, const KeeperContext & /*keeper_context*/) const override
    {
        Coordination::ZooKeeperGetACLRequest & request = dynamic_cast<Coordination::ZooKeeperGetACLRequest &>(*zk_request);

        if (!storage.uncommitted_state.getNode(request.path))
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNONODE}};

        return {};
    }

    template <bool local>
    Coordination::ZooKeeperResponsePtr processImpl(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const 
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperGetACLResponse & response = dynamic_cast<Coordination::ZooKeeperGetACLResponse &>(*response_ptr);
        Coordination::ZooKeeperGetACLRequest & request = dynamic_cast<Coordination::ZooKeeperGetACLRequest &>(*zk_request);

        if constexpr (!local)
        {
            if (const auto result = storage.commit(std::move(deltas)); result != Coordination::Error::ZOK)
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
            response.acl = storage.acl_map.convertNumber(node_it->value.stats.acl_id);
        }

        return response_ptr;
    }

    Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const override 
    {
        return processImpl<false>(storage, std::move(deltas));
    }

    Coordination::ZooKeeperResponsePtr processLocal(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const override 
    {
        return processImpl<true>(storage, std::move(deltas));
    }
};

struct KeeperStorageMultiRequestProcessor final : public KeeperStorageRequestProcessor
{
    using OperationType = Coordination::ZooKeeperMultiRequest::OperationType;
    std::optional<OperationType> operation_type;

    bool checkAuth(KeeperStorage & storage, int64_t session_id, bool is_local) const override
    {
        for (const auto & concrete_request : concrete_requests)
            if (!concrete_request->checkAuth(storage, session_id, is_local))
                return false;
        return true;
    }

    std::vector<KeeperStorageRequestProcessorPtr> concrete_requests;
    explicit KeeperStorageMultiRequestProcessor(const Coordination::ZooKeeperRequestPtr & zk_request_)
        : KeeperStorageRequestProcessor(zk_request_)
    {
        Coordination::ZooKeeperMultiRequest & request = dynamic_cast<Coordination::ZooKeeperMultiRequest &>(*zk_request);
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
                    concrete_requests.push_back(std::make_shared<KeeperStorageCreateRequestProcessor>(sub_zk_request));
                    break;
                case Coordination::OpNum::Remove:
                    check_operation_type(OperationType::Write);
                    concrete_requests.push_back(std::make_shared<KeeperStorageRemoveRequestProcessor>(sub_zk_request));
                    break;
                case Coordination::OpNum::Set:
                    check_operation_type(OperationType::Write);
                    concrete_requests.push_back(std::make_shared<KeeperStorageSetRequestProcessor>(sub_zk_request));
                    break;
                case Coordination::OpNum::Check:
                case Coordination::OpNum::CheckNotExists:
                    check_operation_type(OperationType::Write);
                    concrete_requests.push_back(std::make_shared<KeeperStorageCheckRequestProcessor>(sub_zk_request));
                    break;
                case Coordination::OpNum::Get:
                    check_operation_type(OperationType::Read);
                    concrete_requests.push_back(std::make_shared<KeeperStorageGetRequestProcessor>(sub_zk_request));
                    break;
                case Coordination::OpNum::Exists:
                    check_operation_type(OperationType::Read);
                    concrete_requests.push_back(std::make_shared<KeeperStorageExistsRequestProcessor>(sub_zk_request));
                    break;
                case Coordination::OpNum::List:
                case Coordination::OpNum::FilteredList:
                case Coordination::OpNum::SimpleList:
                    check_operation_type(OperationType::Read);
                    concrete_requests.push_back(std::make_shared<KeeperStorageListRequestProcessor>(sub_zk_request));
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

    std::list<KeeperStorage::Delta>
    preprocess(KeeperStorage & storage, int64_t zxid, int64_t session_id, int64_t time, uint64_t & digest, const KeeperContext & keeper_context) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperMultiRequest);
        std::vector<Coordination::Error> response_errors;
        response_errors.reserve(concrete_requests.size());
        uint64_t current_digest = digest;
        std::list<KeeperStorage::Delta> new_deltas;
        for (size_t i = 0; i < concrete_requests.size(); ++i)
        {
            auto new_subdeltas = concrete_requests[i]->preprocess(storage, zxid, session_id, time, current_digest, keeper_context);

            if (!new_subdeltas.empty())
            {
                if (auto * error = std::get_if<KeeperStorage::ErrorDelta>(&new_subdeltas.back().operation);
                    error && *operation_type == OperationType::Write)
                {
                    storage.uncommitted_state.rollback(std::move(new_deltas));
                    response_errors.push_back(error->error);

                    for (size_t j = i + 1; j < concrete_requests.size(); ++j)
                        response_errors.push_back(Coordination::Error::ZRUNTIMEINCONSISTENCY);

                    return {KeeperStorage::Delta{zxid, KeeperStorage::FailedMultiDelta{std::move(response_errors)}}};
                }
            }

            new_subdeltas.emplace_back(zxid, KeeperStorage::SubDeltaEnd{});
            response_errors.push_back(Coordination::Error::ZOK);

            // manually add deltas so that the result of previous request in the transaction is used in the next request
            storage.uncommitted_state.applyDeltas(new_subdeltas);
            new_deltas.splice(new_deltas.end(), std::move(new_subdeltas));
        }

        digest = current_digest;
        storage.uncommitted_state.addDeltas(std::move(new_deltas));
        return {};
    }

    std::list<KeeperStorage::Delta> getSubdeltas(std::list<KeeperStorage::Delta> & deltas) const
    {
        std::list<KeeperStorage::Delta> subdeltas;
        auto it = deltas.begin();

        for (; it != deltas.end(); ++it)
        {
            if (std::holds_alternative<KeeperStorage::SubDeltaEnd>(it->operation))
            {
                ++it;
                break;
            }
        }

        if (it == deltas.end())
            subdeltas = std::move(deltas);
        else
            subdeltas.splice(subdeltas.end(), deltas, deltas.begin(), it);

        return subdeltas;
    }

    Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperMultiResponse & response = dynamic_cast<Coordination::ZooKeeperMultiResponse &>(*response_ptr);

        // the deltas will have at least SubDeltaEnd or FailedMultiDelta
        chassert(!deltas.empty());
        if (auto * failed_multi = std::get_if<KeeperStorage::FailedMultiDelta>(&deltas.front().operation))
        {
            for (size_t i = 0; i < concrete_requests.size(); ++i)
            {
                response.responses[i] = std::make_shared<Coordination::ZooKeeperErrorResponse>();
                response.responses[i]->error = failed_multi->error_codes[i];
            }

            return response_ptr;
        }

        for (size_t i = 0; i < concrete_requests.size(); ++i)
        {
            std::list<KeeperStorage::Delta> subdeltas = getSubdeltas(deltas);
            response.responses[i] = concrete_requests[i]->process(storage, std::move(subdeltas));
        }

        response.error = Coordination::Error::ZOK;
        return response_ptr;
    }

    Coordination::ZooKeeperResponsePtr processLocal(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const override
    {
        ProfileEvents::increment(ProfileEvents::KeeperMultiReadRequest);
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperMultiResponse & response = dynamic_cast<Coordination::ZooKeeperMultiResponse &>(*response_ptr);

        for (size_t i = 0; i < concrete_requests.size(); ++i)
        {
            std::list<KeeperStorage::Delta> subdeltas = getSubdeltas(deltas);
            response.responses[i] = concrete_requests[i]->processLocal(storage, std::move(subdeltas));
        }

        response.error = Coordination::Error::ZOK;
        return response_ptr;
    }

    KeeperStorage::ResponsesForSessions
    processWatches(KeeperStorage::Watches & watches, KeeperStorage::Watches & list_watches) const override
    {
        KeeperStorage::ResponsesForSessions result;
        for (const auto & generic_request : concrete_requests)
        {
            auto responses = generic_request->processWatches(watches, list_watches);
            result.insert(result.end(), responses.begin(), responses.end());
        }
        return result;
    }
};

struct KeeperStorageCloseRequestProcessor final : public KeeperStorageRequestProcessor
{
    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;
    Coordination::ZooKeeperResponsePtr process(KeeperStorage &, std::list<KeeperStorage::Delta> /* deltas */) const override
    {
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Called process on close request");
    }
};

struct KeeperStorageAuthRequestProcessor final : public KeeperStorageRequestProcessor
{
    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;
    std::list<KeeperStorage::Delta>
    preprocess(KeeperStorage & storage, int64_t zxid, int64_t session_id, int64_t /*time*/, uint64_t & /*digest*/, const KeeperContext & /*keeper_context*/) const override
    {
        Coordination::ZooKeeperAuthRequest & auth_request = dynamic_cast<Coordination::ZooKeeperAuthRequest &>(*zk_request);
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();

        if (auth_request.scheme != "digest" || std::count(auth_request.data.begin(), auth_request.data.end(), ':') != 1)
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZAUTHFAILED}};

        std::list<KeeperStorage::Delta> new_deltas;
        auto auth_digest = KeeperStorage::generateDigest(auth_request.data);
        if (auth_digest == storage.superdigest)
        {
            auto auth = std::make_shared<KeeperStorage::AuthID>();
            auth->scheme = "super";
            new_deltas.emplace_back(zxid, KeeperStorage::AddAuthDelta{session_id, std::move(auth)});
        }
        else
        {
            auto new_auth = std::make_shared<KeeperStorage::AuthID>();
            new_auth->scheme = auth_request.scheme;
            new_auth->id = std::move(auth_digest);
            if (!storage.uncommitted_state.hasACL(session_id, false, [&](const auto & auth_id) { return *new_auth == auth_id; }))
                new_deltas.emplace_back(zxid, KeeperStorage::AddAuthDelta{session_id, std::move(new_auth)});
        }

        return new_deltas;
    }

    Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, std::list<KeeperStorage::Delta> deltas) const override 
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperAuthResponse & auth_response = dynamic_cast<Coordination::ZooKeeperAuthResponse &>(*response_ptr);

        if (const auto result = storage.commit(std::move(deltas)); result != Coordination::Error::ZOK)
            auth_response.error = result;

        return response_ptr;
    }
};

void KeeperStorage::finalize()
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

bool KeeperStorage::isFinalized() const
{
    return finalized;
}


class KeeperStorageRequestProcessorsFactory final : private boost::noncopyable
{
public:
    using Creator = std::function<KeeperStorageRequestProcessorPtr(const Coordination::ZooKeeperRequestPtr &)>;
    using OpNumToRequest = std::unordered_map<Coordination::OpNum, Creator>;

    static KeeperStorageRequestProcessorsFactory & instance()
    {
        static KeeperStorageRequestProcessorsFactory factory;
        return factory;
    }

    KeeperStorageRequestProcessorPtr get(const Coordination::ZooKeeperRequestPtr & zk_request) const
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

template <Coordination::OpNum num, typename RequestT>
void registerKeeperRequestProcessor(KeeperStorageRequestProcessorsFactory & factory)
{
    factory.registerRequest(
        num, [](const Coordination::ZooKeeperRequestPtr & zk_request) { return std::make_shared<RequestT>(zk_request); });
}


KeeperStorageRequestProcessorsFactory::KeeperStorageRequestProcessorsFactory()
{
    registerKeeperRequestProcessor<Coordination::OpNum::Heartbeat, KeeperStorageHeartbeatRequestProcessor>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::Sync, KeeperStorageSyncRequestProcessor>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::Auth, KeeperStorageAuthRequestProcessor>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::Close, KeeperStorageCloseRequestProcessor>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::Create, KeeperStorageCreateRequestProcessor>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::Remove, KeeperStorageRemoveRequestProcessor>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::Exists, KeeperStorageExistsRequestProcessor>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::Get, KeeperStorageGetRequestProcessor>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::Set, KeeperStorageSetRequestProcessor>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::List, KeeperStorageListRequestProcessor>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::SimpleList, KeeperStorageListRequestProcessor>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::FilteredList, KeeperStorageListRequestProcessor>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::Check, KeeperStorageCheckRequestProcessor>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::Multi, KeeperStorageMultiRequestProcessor>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::MultiRead, KeeperStorageMultiRequestProcessor>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::CreateIfNotExists, KeeperStorageCreateRequestProcessor>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::SetACL, KeeperStorageSetACLRequestProcessor>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::GetACL, KeeperStorageGetACLRequestProcessor>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::CheckNotExists, KeeperStorageCheckRequestProcessor>(*this);
}


UInt64 KeeperStorage::calculateNodesDigest(UInt64 current_digest, const std::list<Delta> & new_deltas) const
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
                [&](const UpdateNodeStatDelta & update_delta)
                {
                    std::shared_ptr<Node> node{nullptr};

                    auto updated_node_it = updated_nodes.find(delta.path);
                    if (updated_node_it == updated_nodes.end())
                    {
                        node = std::make_shared<KeeperStorage::Node>();
                        node->shallowCopy(*uncommitted_state.getNode(delta.path));
                        current_digest -= node->getDigest(delta.path);
                        updated_nodes.emplace(delta.path, node);
                    }
                    else
                        node = updated_node_it->second;

                    node->stats = update_delta.new_stats;
                },
                [&](const UpdateNodeDataDelta & update_delta)
                {
                    std::shared_ptr<Node> node{nullptr};

                    auto updated_node_it = updated_nodes.find(delta.path);
                    if (updated_node_it == updated_nodes.end())
                    {
                        node = std::make_shared<KeeperStorage::Node>();
                        node->shallowCopy(*uncommitted_state.getNode(delta.path));
                        current_digest -= node->getDigest(delta.path);
                        updated_nodes.emplace(delta.path, node);
                    }
                    else
                        node = updated_node_it->second;

                    node->setData(update_delta.new_data);
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

void KeeperStorage::preprocessRequest(
    const Coordination::ZooKeeperRequestPtr & zk_request,
    int64_t session_id,
    int64_t time,
    int64_t new_last_zxid,
    bool check_acl,
    std::optional<Digest> digest,
    int64_t log_idx)
{
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
                // we found the preprocessed request with the same ZXID, we can skip it
                return;

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
        if (keeper_context->digestEnabled())
            // if the version of digest we got from the leader is the same as the one this instances has, we can simply copy the value
            // and just check the digest on the commit
            // a mistake can happen while applying the changes to the uncommitted_state so for now let's just recalculate the digest here also
            transaction->nodes_digest = Digest{CURRENT_DIGEST_VERSION, new_digest};
        else
            transaction->nodes_digest = Digest{DigestVersion::NO_DIGEST};

        uncommitted_state.applyDeltas(new_deltas);
        uncommitted_state.addDeltas(std::move(new_deltas));
    });

    KeeperStorageRequestProcessorPtr request_processor = KeeperStorageRequestProcessorsFactory::instance().get(zk_request);

    if (zk_request->getOpNum() == Coordination::OpNum::Close) /// Close request is special
    {
        {
            std::lock_guard lock(ephemerals_mutex);
            auto session_ephemerals = ephemerals.find(session_id);
            if (session_ephemerals != ephemerals.end())
            {
                for (const auto & ephemeral_path : session_ephemerals->second)
                {
                    auto parent_node_path = parentNodePath(ephemeral_path);
                    auto parent_node = uncommitted_state.getNode(parent_node_path);
                    UpdateNodeStatDelta parent_update_delta(*parent_node);
                    ++parent_update_delta.new_stats.cversion;
                    parent_update_delta.new_stats.decreaseNumChildren();
                    new_deltas.emplace_back
                    (
                        parent_node_path.toString(),
                        new_last_zxid,
                        std::move(parent_update_delta)
                    );

                    auto node = uncommitted_state.getNode(ephemeral_path);
                    new_deltas.emplace_back(
                        ephemeral_path,
                        transaction->zxid,
                        RemoveNodeDelta{.stat = node->stats, .acls = uncommitted_state.getACLs(ephemeral_path), .data = std::string{node->getData()}});
                }

                ephemerals.erase(session_ephemerals);
            }
        }

        new_digest = calculateNodesDigest(new_digest, new_deltas);
        return;
    }

    if (check_acl && !request_processor->checkAuth(*this, session_id, false))
    {
        new_deltas.emplace_back(new_last_zxid, Coordination::Error::ZNOAUTH);
        return;
    }

    new_deltas = request_processor->preprocess(*this, transaction->zxid, session_id, time, new_digest, *keeper_context);

    uncommitted_state.cleanup(getZXID());
}

KeeperStorage::ResponsesForSessions KeeperStorage::processRequest(
    const Coordination::ZooKeeperRequestPtr & zk_request,
    int64_t session_id,
    std::optional<int64_t> new_last_zxid,
    bool check_acl,
    bool is_local)
{
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "KeeperStorage system nodes are not initialized");

    int64_t commit_zxid = 0;
    {
        std::lock_guard lock(transaction_mutex);
        if (new_last_zxid)
        {
            if (uncommitted_transactions.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to commit a ZXID ({}) which was not preprocessed", *new_last_zxid);

            if (auto & front_transaction = uncommitted_transactions.front();
                front_transaction.zxid != *new_last_zxid)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Trying to commit a ZXID {} while the next ZXID to commit is {}",
                    *new_last_zxid,
                    front_transaction.zxid);

            commit_zxid = *new_last_zxid;
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

        deltas.splice(deltas.end(), uncommitted_state.deltas, uncommitted_state.deltas.begin(), it);
    }

    KeeperStorage::ResponsesForSessions results;

    /// ZooKeeper update sessions expirity for each request, not only for heartbeats
    session_expiry_queue.addNewSessionOrUpdate(session_id, session_and_timeout[session_id]);

    if (zk_request->getOpNum() == Coordination::OpNum::Close) /// Close request is special
    {
        for (const auto & delta : deltas)
        {
            if (std::holds_alternative<RemoveNodeDelta>(delta.operation))
            {
                auto responses = processWatchesImpl(delta.path, watches, list_watches, Coordination::Event::DELETED);
                results.insert(results.end(), responses.begin(), responses.end());
            }
        }

        {
            std::lock_guard lock(storage_mutex);
            commit(std::move(deltas));
            auto auth_it = session_and_auth.find(session_id);
            if (auth_it != session_and_auth.end())
                session_and_auth.erase(auth_it);
        }

        clearDeadWatches(session_id);

        /// Finish connection
        auto response = std::make_shared<Coordination::ZooKeeperCloseResponse>();
        response->xid = zk_request->xid;
        response->zxid = commit_zxid;
        session_expiry_queue.remove(session_id);
        session_and_timeout.erase(session_id);
        results.push_back(ResponseForSession{session_id, response});
    }
    else if (zk_request->getOpNum() == Coordination::OpNum::Heartbeat) /// Heartbeat request is also special
    {
        KeeperStorageRequestProcessorPtr storage_request = KeeperStorageRequestProcessorsFactory::instance().get(zk_request);
        Coordination::ZooKeeperResponsePtr response = nullptr;
        {
            std::lock_guard lock(storage_mutex);
            response = storage_request->process(*this, std::move(deltas));
        }
        response->xid = zk_request->xid;
        response->zxid = commit_zxid;

        results.push_back(ResponseForSession{session_id, response});
    }
    else /// normal requests proccession
    {
        KeeperStorageRequestProcessorPtr request_processor = KeeperStorageRequestProcessorsFactory::instance().get(zk_request);
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
                std::shared_lock lock(storage_mutex);
                response = request_processor->processLocal(*this, std::move(deltas));
            }
        }
        else
        {
            std::lock_guard lock(storage_mutex);
            response = request_processor->process(*this, std::move(deltas));
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
            auto watch_responses = request_processor->processWatches(watches, list_watches);
            results.insert(results.end(), watch_responses.begin(), watch_responses.end());
        }

        response->xid = zk_request->xid;
        response->zxid = commit_zxid;

        results.push_back(ResponseForSession{session_id, response});
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

void KeeperStorage::rollbackRequest(int64_t rollback_zxid, bool allow_missing) TSA_NO_THREAD_SAFETY_ANALYSIS
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

KeeperStorage::Digest KeeperStorage::getNodesDigest(bool committed, bool lock_transaction_mutex) const TSA_NO_THREAD_SAFETY_ANALYSIS
{
    if (!keeper_context->digestEnabled())
        return {.version = DigestVersion::NO_DIGEST};

    if (committed)
    {
        std::shared_lock storage_lock(storage_mutex);
        return {CURRENT_DIGEST_VERSION, nodes_digest};
    }

    std::unique_lock transaction_lock(transaction_mutex, std::defer_lock);
    if (lock_transaction_mutex)
        transaction_lock.lock();

    if (uncommitted_transactions.empty())
    {
        if (lock_transaction_mutex)
            transaction_lock.unlock();
        std::shared_lock storage_lock(storage_mutex);
        return {CURRENT_DIGEST_VERSION, nodes_digest};
    }

    return uncommitted_transactions.back().nodes_digest;
}

void KeeperStorage::removeDigest(const Node & node, const std::string_view path)
{
    if (keeper_context->digestEnabled())
        nodes_digest -= node.getDigest(path);
}

void KeeperStorage::addDigest(const Node & node, const std::string_view path)
{
    if (keeper_context->digestEnabled())
    {
        node.invalidateDigestCache();
        nodes_digest += node.getDigest(path);
    }
}

/// Allocate new session id with the specified timeouts
int64_t KeeperStorage::getSessionID(int64_t session_timeout_ms)
{
    auto result = session_id_counter++;
    session_and_timeout.emplace(result, session_timeout_ms);
    session_expiry_queue.addNewSessionOrUpdate(result, session_timeout_ms);
    return result;
}

/// Add session id. Used when restoring KeeperStorage from snapshot.
void KeeperStorage::addSessionID(int64_t session_id, int64_t session_timeout_ms)
{
    session_and_timeout.emplace(session_id, session_timeout_ms);
    session_expiry_queue.addNewSessionOrUpdate(session_id, session_timeout_ms);
}

std::vector<int64_t> KeeperStorage::getDeadSessions() const
{
    return session_expiry_queue.getExpiredSessions();
}

SessionAndTimeout KeeperStorage::getActiveSessions() const
{
    return session_and_timeout;
}

/// Turn on snapshot mode, so data inside Container is not deleted, but replaced with new version.
void KeeperStorage::enableSnapshotMode(size_t up_to_version)
{
    container.enableSnapshotMode(up_to_version);
}

/// Turn off snapshot mode.
void KeeperStorage::disableSnapshotMode()
{
    container.disableSnapshotMode();
}

KeeperStorage::Container::const_iterator KeeperStorage::getSnapshotIteratorBegin() const
{
    return container.begin();
}

/// Clear outdated data from internal container.
void KeeperStorage::clearGarbageAfterSnapshot()
{
    container.clearOutdatedNodes();
}

/// Introspection functions mostly used in 4-letter commands
uint64_t KeeperStorage::getNodesCount() const
{
    return container.size();
}

uint64_t KeeperStorage::getApproximateDataSize() const
{
    return container.getApproximateDataSize();
}

uint64_t KeeperStorage::getArenaDataSize() const
{
    return container.keyArenaSize();
}

uint64_t KeeperStorage::getWatchedPathsCount() const
{
    return watches.size() + list_watches.size();
}

void KeeperStorage::clearDeadWatches(int64_t session_id)
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

void KeeperStorage::dumpWatches(WriteBufferFromOwnString & buf) const
{
    for (const auto & [session_id, watches_paths] : sessions_and_watchers)
    {
        buf << "0x" << getHexUIntLowercase(session_id) << "\n";
        for (const String & path : watches_paths)
            buf << "\t" << path << "\n";
    }
}

void KeeperStorage::dumpWatchesByPath(WriteBufferFromOwnString & buf) const
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

void KeeperStorage::dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const
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

    buf << "Sessions with Ephemerals (" << getSessionWithEphemeralNodesCountLocked() << "):\n";
    for (const auto & [session_id, ephemeral_paths] : ephemerals)
    {
        buf << "0x" << getHexUIntLowercase(session_id) << "\n";
        write_str_set(ephemeral_paths);
    }
}

void KeeperStorage::updateStats()
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

const KeeperStorage::Stats & KeeperStorage::getStorageStats() const
{
    return stats;
}

uint64_t KeeperStorage::getTotalWatchesCount() const
{
    uint64_t ret = 0;
    for (const auto & [session, paths] : sessions_and_watchers)
        ret += paths.size();

    return ret;
}

uint64_t KeeperStorage::getSessionWithEphemeralNodesCount() const
{
    return getSessionWithEphemeralNodesCountLocked();
}

uint64_t KeeperStorage::getSessionWithEphemeralNodesCountLocked() const
{
    return ephemerals.size();
}

uint64_t KeeperStorage::getSessionsWithWatchesCount() const
{
    return sessions_and_watchers.size();
}

uint64_t KeeperStorage::getTotalEphemeralNodesCount() const
{
    uint64_t ret = 0;
    for (const auto & [session_id, nodes] : ephemerals)
        ret += nodes.size();

    return ret;
}

void KeeperStorage::recalculateStats()
{
    container.recalculateDataSize();
}

bool KeeperStorage::checkDigest(const Digest & first, const Digest & second)
{
    if (first.version != second.version)
        return true;

    if (first.version == DigestVersion::NO_DIGEST)
        return true;

    return first.value == second.value;
}

String KeeperStorage::generateDigest(const String & userdata)
{
    std::vector<String> user_password;
    boost::split(user_password, userdata, [](char character) { return character == ':'; });
    return user_password[0] + ":" + base64Encode(getSHA1(userdata));
}


}

// NOLINTEND(clang-analyzer-optin.core.EnumCastOutOfRange)
