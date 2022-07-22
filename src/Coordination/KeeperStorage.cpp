#include <iterator>
#include <variant>
#include <Coordination/KeeperStorage.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <boost/algorithm/string.hpp>
#include <Poco/Base64Encoder.h>
#include <Poco/SHA1Engine.h>
#include "Common/ZooKeeper/ZooKeeperCommon.h"
#include <Common/SipHash.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/hex.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Coordination/pathUtils.h>
#include <Coordination/KeeperConstants.h>
#include <sstream>
#include <iomanip>
#include <mutex>
#include <functional>
#include <base/defines.h>
#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{

String base64Encode(const String & decoded)
{
    std::ostringstream ostr; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    ostr.exceptions(std::ios::failbit);
    Poco::Base64Encoder encoder(ostr);
    encoder.rdbuf()->setLineLength(0);
    encoder << decoded;
    encoder.close();
    return ostr.str();
}

String getSHA1(const String & userdata)
{
    Poco::SHA1Engine engine;
    engine.update(userdata);
    const auto & digest_id = engine.digest();
    return String{digest_id.begin(), digest_id.end()};
}

String generateDigest(const String & userdata)
{
    std::vector<String> user_password;
    boost::split(user_password, userdata, [](char character) { return character == ':'; });
    return user_password[0] + ":" + base64Encode(getSHA1(userdata));
}

bool fixupACL(
    const std::vector<Coordination::ACL> & request_acls,
    const std::vector<KeeperStorage::AuthID> & current_ids,
    std::vector<Coordination::ACL> & result_acls)
{
    if (request_acls.empty())
        return true;

    bool valid_found = false;
    for (const auto & request_acl : request_acls)
    {
        if (request_acl.scheme == "auth")
        {
            for (const auto & current_id : current_ids)
            {
                valid_found = true;
                Coordination::ACL new_acl = request_acl;
                new_acl.scheme = current_id.scheme;
                new_acl.id = current_id.id;
                result_acls.push_back(new_acl);
            }
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

    auto parent_path = parentPath(path);

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
uint64_t calculateDigest(std::string_view path, std::string_view data, const Coordination::Stat & stat)
{
    SipHash hash;

    hash.update(path);

    hash.update(data);

    hash.update(stat.czxid);
    hash.update(stat.czxid);
    hash.update(stat.mzxid);
    hash.update(stat.ctime);
    hash.update(stat.mtime);
    hash.update(stat.version);
    hash.update(stat.cversion);
    hash.update(stat.aversion);
    hash.update(stat.ephemeralOwner);
    hash.update(stat.dataLength);
    hash.update(stat.numChildren);
    hash.update(stat.pzxid);

    return hash.get64();
}

}

void KeeperStorage::Node::setData(String new_data)
{
    size_bytes = size_bytes - data.size() + new_data.size();
    data = std::move(new_data);
}

void KeeperStorage::Node::addChild(StringRef child_path)
{
    size_bytes += sizeof child_path;
    children.insert(child_path);
}

void KeeperStorage::Node::removeChild(StringRef child_path)
{
    size_bytes -= sizeof child_path;
    children.erase(child_path);
}

void KeeperStorage::Node::invalidateDigestCache() const
{
    cached_digest.reset();
}

UInt64 KeeperStorage::Node::getDigest(const std::string_view path) const
{
    if (!cached_digest)
        cached_digest = calculateDigest(path, data, stat);

    return *cached_digest;
};

void KeeperStorage::Node::shallowCopy(const KeeperStorage::Node & other)
{
    stat = other.stat;
    seq_num = other.seq_num;
    setData(other.getData());
    cached_digest = other.cached_digest;
}

KeeperStorage::KeeperStorage(
    int64_t tick_time_ms, const String & superdigest_, const bool digest_enabled_, const bool initialize_system_nodes)
    : session_expiry_queue(tick_time_ms), digest_enabled(digest_enabled_), superdigest(superdigest_)
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
        assert(current_root_it != container.end());
        removeDigest(current_root_it->value, "/");
        auto updated_root_it = container.updateValue(
            "/",
            [](auto & node)
            {
                ++node.stat.numChildren;
                node.addChild(getBaseName(keeper_system_path));
            }
        );
        addDigest(updated_root_it->value, "/");
    }

    // insert child system nodes
    for (const auto & [path, data] : child_system_paths_with_data)
    {
        assert(keeper_api_version_path.starts_with(keeper_system_path));
        Node child_system_node;
        child_system_node.setData(data);
        auto [map_key, _] = container.insert(std::string{path}, child_system_node);
        /// Take child path from key owned by map.
        auto child_path = getBaseName(map_key->getKey());
        container.updateValue(
            parentPath(StringRef(path)),
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
    assert(!delta.path.empty());
    if (!nodes.contains(delta.path))
    {
        if (auto storage_node = tryGetNodeFromStorage(delta.path))
            nodes.emplace(delta.path, UncommittedNode{.node = std::move(storage_node)});
        else
            nodes.emplace(delta.path, UncommittedNode{.node = nullptr});
    }

    auto & [node, acls, last_applied_zxid] = nodes.at(delta.path);

    std::visit(
        [&, &node = node, &acls = acls, &last_applied_zxid = last_applied_zxid]<typename DeltaType>(const DeltaType & operation)
        {
            if constexpr (std::same_as<DeltaType, CreateNodeDelta>)
            {
                assert(!node);
                node = std::make_shared<Node>();
                node->stat = operation.stat;
                node->setData(operation.data);
                acls = operation.acls;
                last_applied_zxid = delta.zxid;
            }
            else if constexpr (std::same_as<DeltaType, RemoveNodeDelta>)
            {
                assert(node);
                node = nullptr;
                last_applied_zxid = delta.zxid;
            }
            else if constexpr (std::same_as<DeltaType, UpdateNodeDelta>)
            {
                assert(node);
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

void KeeperStorage::UncommittedState::addDeltas(std::vector<Delta> new_deltas)
{
    for (auto & delta : new_deltas)
    {
        if (!delta.path.empty())
            applyDelta(delta);

        deltas.push_back(std::move(delta));
    }
}

void KeeperStorage::UncommittedState::commit(int64_t commit_zxid)
{
    assert(deltas.empty() || deltas.front().zxid >= commit_zxid);

    while (!deltas.empty() && deltas.front().zxid == commit_zxid)
    {
        if (std::holds_alternative<SubDeltaEnd>(deltas.front().operation))
        {
            deltas.pop_front();
            break;
        }

        deltas.pop_front();
    }

    // delete all cached nodes that were not modified after the commit_zxid
    // the commit can end on SubDeltaEnd so we don't want to clear cached nodes too soon
    if (deltas.empty() || deltas.front().zxid > commit_zxid)
        std::erase_if(nodes, [commit_zxid](const auto & node) { return node.second.zxid == commit_zxid; });
}

void KeeperStorage::UncommittedState::rollback(int64_t rollback_zxid)
{
    // we can only rollback the last zxid (if there is any)
    // if there is a delta with a larger zxid, we have invalid state
    if (!deltas.empty() && deltas.back().zxid > rollback_zxid)
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Invalid state of deltas found while trying to rollback request. Last ZXID ({}) is larger than the requested ZXID ({})",
            deltas.back().zxid,
            rollback_zxid);

    // we need to undo ephemeral mapping modifications
    // CreateNodeDelta added ephemeral for session id -> we need to remove it
    // RemoveNodeDelta removed ephemeral for session id -> we need to add it back
    for (auto delta_it = deltas.rbegin(); delta_it != deltas.rend(); ++delta_it)
    {
        if (delta_it->zxid < rollback_zxid)
            break;

        assert(delta_it->zxid == rollback_zxid);
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
        }
    }

    std::erase_if(deltas, [rollback_zxid](const auto & delta) { return delta.zxid == rollback_zxid; });

    std::unordered_set<std::string> deleted_nodes;
    std::erase_if(
        nodes,
        [&, rollback_zxid](const auto & node)
        {
            if (node.second.zxid == rollback_zxid)
            {
                deleted_nodes.emplace(node.first);
                return true;
            }
            return false;
        });

    // recalculate all the uncommitted deleted nodes
    for (const auto & delta : deltas)
    {
        if (!delta.path.empty() && deleted_nodes.contains(delta.path))
            applyDelta(delta);
    }
}

std::shared_ptr<KeeperStorage::Node> KeeperStorage::UncommittedState::getNode(StringRef path) const
{
    if (auto node_it = nodes.find(std::string{path}); node_it != nodes.end())
        return node_it->second.node;

    return tryGetNodeFromStorage(path);
}

Coordination::ACLs KeeperStorage::UncommittedState::getACLs(StringRef path) const
{
    if (auto node_it = nodes.find(std::string{path}); node_it != nodes.end())
        return node_it->second.acls;

    auto node_it = storage.container.find(path);
    if (node_it == storage.container.end())
        return {};

    return storage.acl_map.convertNumber(node_it->value.acl_id);
}

namespace
{

[[noreturn]] void onStorageInconsistency()
{
    LOG_ERROR(
        &Poco::Logger::get("KeeperStorage"),
        "Inconsistency found between uncommitted and committed data. Keeper will terminate to avoid undefined behaviour.");
    std::terminate();
}

}

Coordination::Error KeeperStorage::commit(int64_t commit_zxid)
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
                if constexpr (std::same_as<DeltaType, KeeperStorage::CreateNodeDelta>)
                {
                    if (!createNode(
                            path,
                            std::move(operation.data),
                            operation.stat,
                            operation.is_sequental,
                            std::move(operation.acls)))
                        onStorageInconsistency();

                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, KeeperStorage::UpdateNodeDelta>)
                {
                    auto node_it = container.find(path);
                    if (node_it == container.end())
                        onStorageInconsistency();

                    if (operation.version != -1 && operation.version != node_it->value.stat.version)
                        onStorageInconsistency();

                    removeDigest(node_it->value, path);
                    auto updated_node = container.updateValue(path, operation.update_fn);
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

                    if (operation.version != -1 && operation.version != node_it->value.stat.aversion)
                        onStorageInconsistency();

                    acl_map.removeUsage(node_it->value.acl_id);

                    uint64_t acl_id = acl_map.convertACLs(operation.acls);
                    acl_map.addUsage(acl_id);

                    container.updateValue(path, [acl_id](KeeperStorage::Node & node) { node.acl_id = acl_id; });

                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, KeeperStorage::ErrorDelta>)
                    return operation.error;
                else if constexpr (std::same_as<DeltaType, KeeperStorage::SubDeltaEnd>)
                {
                    finish_subdelta = true;
                    return Coordination::Error::ZOK;
                }
                else if constexpr (std::same_as<DeltaType, KeeperStorage::AddAuthDelta>)
                {
                    session_and_auth[operation.session_id].emplace_back(std::move(operation.auth_id));
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

bool KeeperStorage::createNode(
    const std::string & path,
    String data,
    const Coordination::Stat & stat,
    bool is_sequental,
    Coordination::ACLs node_acls)
{
    auto parent_path = parentPath(path);
    auto node_it = container.find(parent_path);

    if (node_it == container.end())
        return false;

    if (node_it->value.stat.ephemeralOwner != 0)
        return false;

    if (container.contains(path))
        return false;

    KeeperStorage::Node created_node;

    uint64_t acl_id = acl_map.convertACLs(node_acls);
    acl_map.addUsage(acl_id);

    created_node.acl_id = acl_id;
    created_node.stat = stat;
    created_node.setData(std::move(data));
    created_node.is_sequental = is_sequental;
    auto [map_key, _] = container.insert(path, created_node);
    /// Take child path from key owned by map.
    auto child_path = getBaseName(map_key->getKey());
    container.updateValue(
            parent_path,
            [child_path](KeeperStorage::Node & parent)
            {
                parent.addChild(child_path);
                chassert(parent.stat.numChildren == static_cast<int32_t>(parent.getChildren().size()));
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

    if (version != -1 && version != node_it->value.stat.version)
        return false;

    if (node_it->value.stat.numChildren)
        return false;

    auto prev_node = node_it->value;
    acl_map.removeUsage(prev_node.acl_id);

    container.updateValue(
        parentPath(path),
        [child_basename = getBaseName(node_it->key)](KeeperStorage::Node & parent)
        {
            parent.removeChild(child_basename);
            chassert(parent.stat.numChildren == static_cast<int32_t>(parent.getChildren().size()));
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
    virtual Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, int64_t zxid) const = 0;
    virtual std::vector<KeeperStorage::Delta>
    preprocess(KeeperStorage & /*storage*/, int64_t /*zxid*/, int64_t /*session_id*/, int64_t /*time*/, uint64_t & /*digest*/) const
    {
        return {};
    }

    // process the request using locally committed data
    virtual Coordination::ZooKeeperResponsePtr
    processLocal(KeeperStorage & /*storage*/, int64_t /*zxid*/) const
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
    process(KeeperStorage & /* storage */, int64_t /* zxid */) const override
    {
        return zk_request->makeResponse();
    }
};

struct KeeperStorageSyncRequestProcessor final : public KeeperStorageRequestProcessor
{
    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;
    Coordination::ZooKeeperResponsePtr
    process(KeeperStorage & /* storage */, int64_t /* zxid */) const override
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
            auto node_it = storage.container.find(path);
            if (node_it == storage.container.end())
                return {};

            return storage.acl_map.convertNumber(node_it->value.acl_id);
        }

        return storage.uncommitted_state.getACLs(path);
    }

}
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
    auto ephemerals_it = ephemerals.find(session_id);
    assert(ephemerals_it != ephemerals.end());
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
        return storage.checkACL(parentPath(path), Coordination::ACL::Create, session_id, is_local);
    }

    std::vector<KeeperStorage::Delta>
    preprocess(KeeperStorage & storage, int64_t zxid, int64_t session_id, int64_t time, uint64_t & digest) const override
    {
        Coordination::ZooKeeperCreateRequest & request = dynamic_cast<Coordination::ZooKeeperCreateRequest &>(*zk_request);

        std::vector<KeeperStorage::Delta> new_deltas;

        auto parent_path = parentPath(request.path);
        auto parent_node = storage.uncommitted_state.getNode(parent_path);
        if (parent_node == nullptr)
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNONODE}};

        else if (parent_node->stat.ephemeralOwner != 0)
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNOCHILDRENFOREPHEMERALS}};

        std::string path_created = request.path;
        if (request.is_sequential)
        {
            auto seq_num = parent_node->seq_num;

            std::stringstream seq_num_str; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            seq_num_str.exceptions(std::ios::failbit);
            seq_num_str << std::setw(10) << std::setfill('0') << seq_num;

            path_created += seq_num_str.str();
        }

        if (storage.uncommitted_state.getNode(path_created))
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNODEEXISTS}};

        if (getBaseName(path_created).size == 0)
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};

        Coordination::ACLs node_acls;
        if (!fixupACL(request.acls, storage.session_and_auth[session_id], node_acls))
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZINVALIDACL}};

        if (request.is_ephemeral)
            storage.ephemerals[session_id].emplace(path_created);

        int32_t parent_cversion = request.parent_cversion;

        auto parent_update = [parent_cversion, zxid](KeeperStorage::Node & node)
        {
            /// Increment sequential number even if node is not sequential
            ++node.seq_num;
            if (parent_cversion == -1)
                ++node.stat.cversion;
            else if (parent_cversion > node.stat.cversion)
                node.stat.cversion = parent_cversion;

            if (zxid > node.stat.pzxid)
                node.stat.pzxid = zxid;
            ++node.stat.numChildren;
        };

        new_deltas.emplace_back(std::string{parent_path}, zxid, KeeperStorage::UpdateNodeDelta{std::move(parent_update)});

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
        stat.dataLength = request.data.length();
        stat.ephemeralOwner = request.is_ephemeral ? session_id : 0;

        new_deltas.emplace_back(
            std::move(path_created),
            zxid,
            KeeperStorage::CreateNodeDelta{stat, request.is_sequential, std::move(node_acls), request.data});

        digest = storage.calculateNodesDigest(digest, new_deltas);
        return new_deltas;
    }

    Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, int64_t zxid) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperCreateResponse & response = dynamic_cast<Coordination::ZooKeeperCreateResponse &>(*response_ptr);

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
            { return delta.zxid == zxid && std::holds_alternative<KeeperStorage::CreateNodeDelta>(delta.operation); });

        response.path_created = create_delta_it->path;
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

    std::vector<KeeperStorage::Delta>
    preprocess(KeeperStorage & storage, int64_t zxid, int64_t /*session_id*/, int64_t /*time*/, uint64_t & /*digest*/) const override
    {
        Coordination::ZooKeeperGetRequest & request = dynamic_cast<Coordination::ZooKeeperGetRequest &>(*zk_request);

        if (request.path == Coordination::keeper_api_version_path)
            return {};

        if (!storage.uncommitted_state.getNode(request.path))
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNONODE}};

        return {};
    }

    template <bool local>
    Coordination::ZooKeeperResponsePtr processImpl(KeeperStorage & storage, int64_t zxid) const
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperGetResponse & response = dynamic_cast<Coordination::ZooKeeperGetResponse &>(*response_ptr);
        Coordination::ZooKeeperGetRequest & request = dynamic_cast<Coordination::ZooKeeperGetRequest &>(*zk_request);

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
            response.stat = node_it->value.stat;
            response.data = node_it->value.getData();
            response.error = Coordination::Error::ZOK;
        }

        return response_ptr;
    }


    Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, int64_t zxid) const override
    {
        return processImpl<false>(storage, zxid);
    }

    Coordination::ZooKeeperResponsePtr processLocal(KeeperStorage & storage, int64_t zxid) const override
    {
        return processImpl<true>(storage, zxid);
    }
};

struct KeeperStorageRemoveRequestProcessor final : public KeeperStorageRequestProcessor
{
    bool checkAuth(KeeperStorage & storage, int64_t session_id, bool is_local) const override
    {
        return storage.checkACL(parentPath(zk_request->getPath()), Coordination::ACL::Delete, session_id, is_local);
    }

    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;
    std::vector<KeeperStorage::Delta>
    preprocess(KeeperStorage & storage, int64_t zxid, int64_t /*session_id*/, int64_t /*time*/, uint64_t & digest) const override
    {
        Coordination::ZooKeeperRemoveRequest & request = dynamic_cast<Coordination::ZooKeeperRemoveRequest &>(*zk_request);

        std::vector<KeeperStorage::Delta> new_deltas;

        if (request.path.starts_with(keeper_system_path))
        {
            LOG_ERROR(&Poco::Logger::get("KeeperStorage"), "Trying to delete an internal Keeper path ({}) which is not allowed", request.path);
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};
        }

        const auto update_parent_pzxid = [&]()
        {
            auto parent_path = parentPath(request.path);
            if (!storage.uncommitted_state.getNode(parent_path))
                return;

            new_deltas.emplace_back(
                std::string{parent_path},
                zxid,
                KeeperStorage::UpdateNodeDelta
                {
                    [zxid](KeeperStorage::Node & parent)
                    {
                        if (parent.stat.pzxid < zxid)
                            parent.stat.pzxid = zxid;
                   }
                }
            );
        };

        auto node = storage.uncommitted_state.getNode(request.path);

        if (!node)
        {
            if (request.restored_from_zookeeper_log)
                update_parent_pzxid();
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNONODE}};
        }
        else if (request.version != -1 && request.version != node->stat.version)
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZBADVERSION}};
        else if (node->stat.numChildren != 0)
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNOTEMPTY}};

        if (request.restored_from_zookeeper_log)
            update_parent_pzxid();

        new_deltas.emplace_back(
            std::string{parentPath(request.path)},
            zxid,
            KeeperStorage::UpdateNodeDelta{[](KeeperStorage::Node & parent)
                                           {
                                               ++parent.stat.cversion;
                                               --parent.stat.numChildren;
                                           }});

        new_deltas.emplace_back(request.path, zxid, KeeperStorage::RemoveNodeDelta{request.version, node->stat.ephemeralOwner});

        if (node->stat.ephemeralOwner != 0)
            storage.unregisterEphemeralPath(node->stat.ephemeralOwner, request.path);

        digest = storage.calculateNodesDigest(digest, new_deltas);

        return new_deltas;
    }

    Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, int64_t zxid) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperRemoveResponse & response = dynamic_cast<Coordination::ZooKeeperRemoveResponse &>(*response_ptr);

        response.error = storage.commit(zxid);
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

    std::vector<KeeperStorage::Delta>
    preprocess(KeeperStorage & storage, int64_t zxid, int64_t /*session_id*/, int64_t /*time*/, uint64_t & /*digest*/) const override
    {
        Coordination::ZooKeeperExistsRequest & request = dynamic_cast<Coordination::ZooKeeperExistsRequest &>(*zk_request);

        if (!storage.uncommitted_state.getNode(request.path))
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNONODE}};

        return {};
    }

    template <bool local>
    Coordination::ZooKeeperResponsePtr processImpl(KeeperStorage & storage, int64_t zxid) const
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperExistsResponse & response = dynamic_cast<Coordination::ZooKeeperExistsResponse &>(*response_ptr);
        Coordination::ZooKeeperExistsRequest & request = dynamic_cast<Coordination::ZooKeeperExistsRequest &>(*zk_request);

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
            response.stat = node_it->value.stat;
            response.error = Coordination::Error::ZOK;
        }

        return response_ptr;
    }

    Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, int64_t zxid) const override
    {
        return processImpl<false>(storage, zxid);
    }

    Coordination::ZooKeeperResponsePtr processLocal(KeeperStorage & storage, int64_t zxid) const override
    {
        return processImpl<true>(storage, zxid);
    }
};

struct KeeperStorageSetRequestProcessor final : public KeeperStorageRequestProcessor
{
    bool checkAuth(KeeperStorage & storage, int64_t session_id, bool is_local) const override
    {
        return storage.checkACL(zk_request->getPath(), Coordination::ACL::Write, session_id, is_local);
    }

    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;
    std::vector<KeeperStorage::Delta>
    preprocess(KeeperStorage & storage, int64_t zxid, int64_t /*session_id*/, int64_t time, uint64_t & digest) const override
    {
        Coordination::ZooKeeperSetRequest & request = dynamic_cast<Coordination::ZooKeeperSetRequest &>(*zk_request);

        std::vector<KeeperStorage::Delta> new_deltas;

        if (request.path.starts_with(keeper_system_path))
        {
            LOG_ERROR(&Poco::Logger::get("KeeperStorage"), "Trying to update an internal Keeper path ({}) which is not allowed", request.path);
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};
        }

        if (!storage.uncommitted_state.getNode(request.path))
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNONODE}};

        auto node = storage.uncommitted_state.getNode(request.path);

        if (request.version != -1 && request.version != node->stat.version)
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZBADVERSION}};

        new_deltas.emplace_back(
            request.path,
            zxid,
            KeeperStorage::UpdateNodeDelta{
                [zxid, data = request.data, time](KeeperStorage::Node & value)
                {
                    value.stat.version++;
                    value.stat.mzxid = zxid;
                    value.stat.mtime = time;
                    value.stat.dataLength = data.length();
                    value.setData(data);
                },
                request.version});

        new_deltas.emplace_back(
                parentPath(request.path).toString(),
                zxid,
                KeeperStorage::UpdateNodeDelta
                {
                    [](KeeperStorage::Node & parent)
                    {
                        parent.stat.cversion++;
                    }
                }
        );

        digest = storage.calculateNodesDigest(digest, new_deltas);
        return new_deltas;
    }

    Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, int64_t zxid) const override
    {
        auto & container = storage.container;

        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperSetResponse & response = dynamic_cast<Coordination::ZooKeeperSetResponse &>(*response_ptr);
        Coordination::ZooKeeperSetRequest & request = dynamic_cast<Coordination::ZooKeeperSetRequest &>(*zk_request);

        if (const auto result = storage.commit(zxid); result != Coordination::Error::ZOK)
        {
            response.error = result;
            return response_ptr;
        }

        auto node_it = container.find(request.path);
        if (node_it == container.end())
            onStorageInconsistency();

        response.stat = node_it->value.stat;
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
    std::vector<KeeperStorage::Delta>
    preprocess(KeeperStorage & storage, int64_t zxid, int64_t /*session_id*/, int64_t /*time*/, uint64_t & /*digest*/) const override
    {
        Coordination::ZooKeeperListRequest & request = dynamic_cast<Coordination::ZooKeeperListRequest &>(*zk_request);

        if (!storage.uncommitted_state.getNode(request.path))
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNONODE}};

        return {};
    }


    template <bool local>
    Coordination::ZooKeeperResponsePtr processImpl(KeeperStorage & storage, int64_t zxid) const
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperListResponse & response = dynamic_cast<Coordination::ZooKeeperListResponse &>(*response_ptr);
        Coordination::ZooKeeperListRequest & request = dynamic_cast<Coordination::ZooKeeperListRequest &>(*zk_request);

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
                throw DB::Exception("Logical error: path cannot be empty", ErrorCodes::LOGICAL_ERROR);

            const auto & children = node_it->value.getChildren();
            response.names.reserve(children.size());

            const auto add_child = [&](const auto child)
            {
                using enum Coordination::ListRequestType;

                auto list_request_type = ALL;
                if (auto * filtered_list = dynamic_cast<Coordination::ZooKeeperFilteredListRequest *>(&request))
                {
                    list_request_type = filtered_list->list_request_type;
                }

                if (list_request_type == ALL)
                    return true;

                auto child_path = (std::filesystem::path(request.path) / child.toView()).generic_string();
                auto child_it = container.find(child_path);
                if (child_it == container.end())
                    onStorageInconsistency();

                const auto is_ephemeral = child_it->value.stat.ephemeralOwner != 0;
                return (is_ephemeral && list_request_type == EPHEMERAL_ONLY) || (!is_ephemeral && list_request_type == PERSISTENT_ONLY);
            };

            for (const auto child : children)
            {
                if (add_child(child))
                    response.names.push_back(child.toString());
            }

            response.stat = node_it->value.stat;
            response.error = Coordination::Error::ZOK;
        }

        return response_ptr;
    }

    Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, int64_t zxid) const override
    {
        return processImpl<false>(storage, zxid);
    }

    Coordination::ZooKeeperResponsePtr processLocal(KeeperStorage & storage, int64_t zxid) const override
    {
        return processImpl<true>(storage, zxid);
    }
};

struct KeeperStorageCheckRequestProcessor final : public KeeperStorageRequestProcessor
{
    bool checkAuth(KeeperStorage & storage, int64_t session_id, bool is_local) const override
    {
        return storage.checkACL(zk_request->getPath(), Coordination::ACL::Read, session_id, is_local);
    }

    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;
    std::vector<KeeperStorage::Delta>
    preprocess(KeeperStorage & storage, int64_t zxid, int64_t /*session_id*/, int64_t /*time*/, uint64_t & /*digest*/) const override
    {
        Coordination::ZooKeeperCheckRequest & request = dynamic_cast<Coordination::ZooKeeperCheckRequest &>(*zk_request);

        if (!storage.uncommitted_state.getNode(request.path))
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNONODE}};

        auto node = storage.uncommitted_state.getNode(request.path);
        if (request.version != -1 && request.version != node->stat.version)
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZBADVERSION}};

        return {};
    }

    template <bool local>
    Coordination::ZooKeeperResponsePtr processImpl(KeeperStorage & storage, int64_t zxid) const
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperCheckResponse & response = dynamic_cast<Coordination::ZooKeeperCheckResponse &>(*response_ptr);
        Coordination::ZooKeeperCheckRequest & request = dynamic_cast<Coordination::ZooKeeperCheckRequest &>(*zk_request);

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
        if (node_it == container.end())
        {
            on_error(Coordination::Error::ZNONODE);
        }
        else if (request.version != -1 && request.version != node_it->value.stat.version)
        {
            on_error(Coordination::Error::ZBADVERSION);
        }
        else
        {
            response.error = Coordination::Error::ZOK;
        }

        return response_ptr;
    }

    Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, int64_t zxid) const override
    {
        return processImpl<false>(storage, zxid);
    }

    Coordination::ZooKeeperResponsePtr processLocal(KeeperStorage & storage, int64_t zxid) const override
    {
        return processImpl<true>(storage, zxid);
    }
};


struct KeeperStorageSetACLRequestProcessor final : public KeeperStorageRequestProcessor
{
    bool checkAuth(KeeperStorage & storage, int64_t session_id, bool is_local) const override
    {
        return storage.checkACL(zk_request->getPath(), Coordination::ACL::Admin, session_id, is_local);
    }

    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;

    std::vector<KeeperStorage::Delta>
    preprocess(KeeperStorage & storage, int64_t zxid, int64_t session_id, int64_t /*time*/, uint64_t & digest) const override
    {
        Coordination::ZooKeeperSetACLRequest & request = dynamic_cast<Coordination::ZooKeeperSetACLRequest &>(*zk_request);

        if (request.path.starts_with(keeper_system_path))
        {
            LOG_ERROR(&Poco::Logger::get("KeeperStorage"), "Trying to update an internal Keeper path ({}) which is not allowed", request.path);
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZBADARGUMENTS}};
        }

        auto & uncommitted_state = storage.uncommitted_state;
        if (!uncommitted_state.getNode(request.path))
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNONODE}};

        auto node = uncommitted_state.getNode(request.path);

        if (request.version != -1 && request.version != node->stat.aversion)
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZBADVERSION}};


        auto & session_auth_ids = storage.session_and_auth[session_id];
        Coordination::ACLs node_acls;

        if (!fixupACL(request.acls, session_auth_ids, node_acls))
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZINVALIDACL}};

        std::vector<KeeperStorage::Delta> new_deltas
        {
            {
                request.path,
                zxid,
                KeeperStorage::SetACLDelta{std::move(node_acls), request.version}
            },
            {
                request.path,
                zxid,
                KeeperStorage::UpdateNodeDelta
                {
                    [](KeeperStorage::Node & n) { ++n.stat.aversion; }
                }
            }
        };

        digest = storage.calculateNodesDigest(digest, new_deltas);

        return new_deltas;
    }

    Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, int64_t zxid) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperSetACLResponse & response = dynamic_cast<Coordination::ZooKeeperSetACLResponse &>(*response_ptr);
        Coordination::ZooKeeperSetACLRequest & request = dynamic_cast<Coordination::ZooKeeperSetACLRequest &>(*zk_request);

        if (const auto result = storage.commit(zxid); result != Coordination::Error::ZOK)
        {
            response.error = result;
            return response_ptr;
        }

        auto node_it = storage.container.find(request.path);
        if (node_it == storage.container.end())
            onStorageInconsistency();
        response.stat = node_it->value.stat;
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

    std::vector<KeeperStorage::Delta>
    preprocess(KeeperStorage & storage, int64_t zxid, int64_t /*session_id*/, int64_t /*time*/, uint64_t & /*digest*/) const override
    {
        Coordination::ZooKeeperGetACLRequest & request = dynamic_cast<Coordination::ZooKeeperGetACLRequest &>(*zk_request);

        if (!storage.uncommitted_state.getNode(request.path))
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZNONODE}};

        return {};
    }

    template <bool local>
    Coordination::ZooKeeperResponsePtr processImpl(KeeperStorage & storage, int64_t zxid) const
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperGetACLResponse & response = dynamic_cast<Coordination::ZooKeeperGetACLResponse &>(*response_ptr);
        Coordination::ZooKeeperGetACLRequest & request = dynamic_cast<Coordination::ZooKeeperGetACLRequest &>(*zk_request);

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
            response.stat = node_it->value.stat;
            response.acl = storage.acl_map.convertNumber(node_it->value.acl_id);
        }

        return response_ptr;
    }

    Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, int64_t zxid) const override
    {
        return processImpl<false>(storage, zxid);
    }

    Coordination::ZooKeeperResponsePtr processLocal(KeeperStorage & storage, int64_t zxid) const override
    {
        return processImpl<true>(storage, zxid);
    }
};

struct KeeperStorageMultiRequestProcessor final : public KeeperStorageRequestProcessor
{
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

        for (const auto & sub_request : request.requests)
        {
            auto sub_zk_request = std::dynamic_pointer_cast<Coordination::ZooKeeperRequest>(sub_request);
            switch (sub_zk_request->getOpNum())
            {
                case Coordination::OpNum::Create:
                    concrete_requests.push_back(std::make_shared<KeeperStorageCreateRequestProcessor>(sub_zk_request));
                    break;
                case Coordination::OpNum::Remove:
                    concrete_requests.push_back(std::make_shared<KeeperStorageRemoveRequestProcessor>(sub_zk_request));
                    break;
                case Coordination::OpNum::Set:
                    concrete_requests.push_back(std::make_shared<KeeperStorageSetRequestProcessor>(sub_zk_request));
                    break;
                case Coordination::OpNum::Check:
                    concrete_requests.push_back(std::make_shared<KeeperStorageCheckRequestProcessor>(sub_zk_request));
                    break;
                default:
                    throw DB::Exception(
                        ErrorCodes::BAD_ARGUMENTS, "Illegal command as part of multi ZooKeeper request {}", sub_zk_request->getOpNum());
            }
        }
    }

    std::vector<KeeperStorage::Delta>
    preprocess(KeeperStorage & storage, int64_t zxid, int64_t session_id, int64_t time, uint64_t & digest) const override
    {
        std::vector<Coordination::Error> response_errors;
        response_errors.reserve(concrete_requests.size());
        uint64_t current_digest = digest;
        for (size_t i = 0; i < concrete_requests.size(); ++i)
        {
            auto new_deltas = concrete_requests[i]->preprocess(storage, zxid, session_id, time, current_digest);

            if (!new_deltas.empty())
            {
                if (auto * error = std::get_if<KeeperStorage::ErrorDelta>(&new_deltas.back().operation))
                {
                    storage.uncommitted_state.rollback(zxid);
                    response_errors.push_back(error->error);

                    for (size_t j = i + 1; j < concrete_requests.size(); ++j)
                    {
                        response_errors.push_back(Coordination::Error::ZRUNTIMEINCONSISTENCY);
                    }

                    return {KeeperStorage::Delta{zxid, KeeperStorage::FailedMultiDelta{std::move(response_errors)}}};
                }
            }
            new_deltas.emplace_back(zxid, KeeperStorage::SubDeltaEnd{});
            response_errors.push_back(Coordination::Error::ZOK);

            // manually add deltas so that the result of previous request in the transaction is used in the next request
            storage.uncommitted_state.addDeltas(std::move(new_deltas));
        }

        digest = current_digest;

        return {};
    }

    Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, int64_t zxid) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperMultiResponse & response = dynamic_cast<Coordination::ZooKeeperMultiResponse &>(*response_ptr);

        auto & deltas = storage.uncommitted_state.deltas;
        // the deltas will have at least SubDeltaEnd or FailedMultiDelta
        assert(!deltas.empty());
        if (auto * failed_multi = std::get_if<KeeperStorage::FailedMultiDelta>(&deltas.front().operation))
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
            auto cur_response = concrete_requests[i]->process(storage, zxid);
            response.responses[i] = cur_response;
            storage.uncommitted_state.commit(zxid);
        }

        response.error = Coordination::Error::ZOK;
        return response_ptr;
    }

    Coordination::ZooKeeperResponsePtr processLocal(KeeperStorage & storage, int64_t zxid) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperMultiResponse & response = dynamic_cast<Coordination::ZooKeeperMultiResponse &>(*response_ptr);

        for (size_t i = 0; i < concrete_requests.size(); ++i)
        {
            auto cur_response = concrete_requests[i]->processLocal(storage, zxid);

            response.responses[i] = cur_response;
            if (cur_response->error != Coordination::Error::ZOK)
            {
                for (size_t j = 0; j <= i; ++j)
                {
                    auto response_error = response.responses[j]->error;
                    response.responses[j] = std::make_shared<Coordination::ZooKeeperErrorResponse>();
                    response.responses[j]->error = response_error;
                }

                for (size_t j = i + 1; j < response.responses.size(); ++j)
                {
                    response.responses[j] = std::make_shared<Coordination::ZooKeeperErrorResponse>();
                    response.responses[j]->error = Coordination::Error::ZRUNTIMEINCONSISTENCY;
                }

                return response_ptr;
            }
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
    Coordination::ZooKeeperResponsePtr process(KeeperStorage &, int64_t) const override
    {
        throw DB::Exception("Called process on close request", ErrorCodes::LOGICAL_ERROR);
    }
};

struct KeeperStorageAuthRequestProcessor final : public KeeperStorageRequestProcessor
{
    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;
    std::vector<KeeperStorage::Delta>
    preprocess(KeeperStorage & storage, int64_t zxid, int64_t session_id, int64_t /*time*/, uint64_t & /*digest*/) const override
    {
        Coordination::ZooKeeperAuthRequest & auth_request = dynamic_cast<Coordination::ZooKeeperAuthRequest &>(*zk_request);
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();

        if (auth_request.scheme != "digest" || std::count(auth_request.data.begin(), auth_request.data.end(), ':') != 1)
            return {KeeperStorage::Delta{zxid, Coordination::Error::ZAUTHFAILED}};

        std::vector<KeeperStorage::Delta> new_deltas;
        auto auth_digest = generateDigest(auth_request.data);
        if (auth_digest == storage.superdigest)
        {
            KeeperStorage::AuthID auth{"super", ""};
            new_deltas.emplace_back(zxid, KeeperStorage::AddAuthDelta{session_id, std::move(auth)});
        }
        else
        {
            KeeperStorage::AuthID new_auth{auth_request.scheme, auth_digest};
            if (!storage.uncommitted_state.hasACL(session_id, false, [&](const auto & auth_id) { return new_auth == auth_id; }))
                new_deltas.emplace_back(zxid, KeeperStorage::AddAuthDelta{session_id, std::move(new_auth)});
        }

        return new_deltas;
    }

    Coordination::ZooKeeperResponsePtr process(KeeperStorage & storage, int64_t zxid) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperAuthResponse & auth_response = dynamic_cast<Coordination::ZooKeeperAuthResponse &>(*response_ptr);

        if (const auto result = storage.commit(zxid); result != Coordination::Error::ZOK)
            auth_response.error = result;

        return response_ptr;
    }
};

void KeeperStorage::finalize()
{
    if (finalized)
        throw DB::Exception("KeeperStorage already finalized", ErrorCodes::LOGICAL_ERROR);

    finalized = true;

    ephemerals.clear();

    watches.clear();
    list_watches.clear();
    sessions_and_watchers.clear();
    session_expiry_queue.clear();
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
            throw DB::Exception("Unknown operation type " + toString(zk_request->getOpNum()), ErrorCodes::LOGICAL_ERROR);

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
    registerKeeperRequestProcessor<Coordination::OpNum::SetACL, KeeperStorageSetACLRequestProcessor>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::GetACL, KeeperStorageGetACLRequestProcessor>(*this);
}


UInt64 KeeperStorage::calculateNodesDigest(UInt64 current_digest, const std::vector<Delta> & new_deltas) const
{
    if (!digest_enabled)
        return current_digest;

    std::unordered_map<std::string, std::shared_ptr<Node>> updated_nodes;

    for (const auto & delta : new_deltas)
    {
        std::visit(
            Overloaded{
                [&](const CreateNodeDelta & create_delta)
                {
                    auto node = std::make_shared<Node>();
                    node->stat = create_delta.stat;
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
                        node = std::make_shared<KeeperStorage::Node>();
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

void KeeperStorage::preprocessRequest(
    const Coordination::ZooKeeperRequestPtr & zk_request,
    int64_t session_id,
    int64_t time,
    int64_t new_last_zxid,
    bool check_acl,
    std::optional<Digest> digest)
{
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "KeeperStorage system nodes are not initialized");

    int64_t last_zxid = getNextZXID() - 1;

    if (uncommitted_transactions.empty())
    {
        // if we have no uncommitted transactions it means the last zxid is possibly loaded from snapshot
        if (last_zxid != old_snapshot_zxid && new_last_zxid <= last_zxid)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Got new ZXID ({}) smaller or equal to current ZXID ({}). It's a bug", new_last_zxid, last_zxid);
    }
    else
    {
        if (last_zxid == new_last_zxid && digest && checkDigest(*digest, getNodesDigest(false)))
            // we found the preprocessed request with the same ZXID, we can skip it
            return;

        if (new_last_zxid <= last_zxid)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Got new ZXID ({}) smaller or equal to current ZXID ({}). It's a bug", new_last_zxid, last_zxid);
    }

    std::vector<Delta> new_deltas;
    TransactionInfo transaction{.zxid = new_last_zxid};
    uint64_t new_digest = getNodesDigest(false).value;
    SCOPE_EXIT({
        if (digest_enabled)
            // if the version of digest we got from the leader is the same as the one this instances has, we can simply copy the value
            // and just check the digest on the commit
            // a mistake can happen while applying the changes to the uncommitted_state so for now let's just recalculate the digest here also
            transaction.nodes_digest = Digest{CURRENT_DIGEST_VERSION, new_digest};
        else
            transaction.nodes_digest = Digest{DigestVersion::NO_DIGEST};

        uncommitted_transactions.emplace_back(transaction);
        uncommitted_state.addDeltas(std::move(new_deltas));
    });

    KeeperStorageRequestProcessorPtr request_processor = KeeperStorageRequestProcessorsFactory::instance().get(zk_request);

    if (zk_request->getOpNum() == Coordination::OpNum::Close) /// Close request is special
    {
        auto session_ephemerals = ephemerals.find(session_id);
        if (session_ephemerals != ephemerals.end())
        {
            for (const auto & ephemeral_path : session_ephemerals->second)
            {
                new_deltas.emplace_back
                (
                    parentPath(ephemeral_path).toString(),
                    new_last_zxid,
                    UpdateNodeDelta
                    {
                        [ephemeral_path](Node & parent)
                        {
                            ++parent.stat.cversion;
                            --parent.stat.numChildren;
                        }
                    }
                );

                new_deltas.emplace_back(ephemeral_path, transaction.zxid, RemoveNodeDelta{.ephemeral_owner = session_id});
            }

            ephemerals.erase(session_ephemerals);
        }

        new_digest = calculateNodesDigest(new_digest, new_deltas);
        return;
    }

    if (check_acl && !request_processor->checkAuth(*this, session_id, false))
    {
        uncommitted_state.deltas.emplace_back(new_last_zxid, Coordination::Error::ZNOAUTH);
        return;
    }

    new_deltas = request_processor->preprocess(*this, transaction.zxid, session_id, time, new_digest);
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

    if (new_last_zxid)
    {
        if (uncommitted_transactions.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to commit a ZXID ({}) which was not preprocessed", *new_last_zxid);

        if (uncommitted_transactions.front().zxid != *new_last_zxid)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Trying to commit a ZXID {} while the next ZXID to commit is {}",
                *new_last_zxid,
                uncommitted_transactions.front().zxid);

        zxid = *new_last_zxid;
        uncommitted_transactions.pop_front();
    }

    KeeperStorage::ResponsesForSessions results;

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

        uncommitted_state.commit(zxid);

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
        KeeperStorageRequestProcessorPtr storage_request = KeeperStorageRequestProcessorsFactory::instance().get(zk_request);
        auto response = storage_request->process(*this, zxid);
        response->xid = zk_request->xid;
        response->zxid = getZXID();

        results.push_back(ResponseForSession{session_id, response});
    }
    else /// normal requests proccession
    {
        KeeperStorageRequestProcessorPtr request_processor = KeeperStorageRequestProcessorsFactory::instance().get(zk_request);
        Coordination::ZooKeeperResponsePtr response;

        if (is_local)
        {
            assert(zk_request->isReadRequest());
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
            uncommitted_state.commit(zxid);
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

                watches_type[zk_request->getPath()].emplace(session_id);
                sessions_and_watchers[session_id].emplace(zk_request->getPath());
            }
            else if (response->error == Coordination::Error::ZNONODE && zk_request->getOpNum() == Coordination::OpNum::Exists)
            {
                watches[zk_request->getPath()].emplace(session_id);
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
        response->zxid = getZXID();

        results.push_back(ResponseForSession{session_id, response});
    }

    return results;
}

void KeeperStorage::rollbackRequest(int64_t rollback_zxid)
{
    if (uncommitted_transactions.empty() || uncommitted_transactions.back().zxid != rollback_zxid)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Trying to rollback invalid ZXID ({}). It should be the last preprocessed.", rollback_zxid);

    uncommitted_transactions.pop_back();
    uncommitted_state.rollback(rollback_zxid);
}

KeeperStorage::Digest KeeperStorage::getNodesDigest(bool committed) const
{
    if (!digest_enabled)
        return {.version = DigestVersion::NO_DIGEST};

    if (committed || uncommitted_transactions.empty())
        return {CURRENT_DIGEST_VERSION, nodes_digest};

    return uncommitted_transactions.back().nodes_digest;
}

void KeeperStorage::removeDigest(const Node & node, const std::string_view path)
{
    if (digest_enabled)
        nodes_digest -= node.getDigest(path);
}

void KeeperStorage::addDigest(const Node & node, const std::string_view path)
{
    if (digest_enabled)
    {
        node.invalidateDigestCache();
        nodes_digest += node.getDigest(path);
    }
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

    buf << "Sessions with Ephemerals (" << getSessionWithEphemeralNodesCount() << "):\n";
    for (const auto & [session_id, ephemeral_paths] : ephemerals)
    {
        buf << "0x" << getHexUIntLowercase(session_id) << "\n";
        write_str_set(ephemeral_paths);
    }
}

uint64_t KeeperStorage::getTotalWatchesCount() const
{
    uint64_t ret = 0;
    for (const auto & [path, subscribed_sessions] : watches)
        ret += subscribed_sessions.size();

    for (const auto & [path, subscribed_sessions] : list_watches)
        ret += subscribed_sessions.size();

    return ret;
}

uint64_t KeeperStorage::getSessionsWithWatchesCount() const
{
    std::unordered_set<int64_t> counter;
    for (const auto & [path, subscribed_sessions] : watches)
        counter.insert(subscribed_sessions.begin(), subscribed_sessions.end());

    for (const auto & [path, subscribed_sessions] : list_watches)
        counter.insert(subscribed_sessions.begin(), subscribed_sessions.end());

    return counter.size();
}

uint64_t KeeperStorage::getTotalEphemeralNodesCount() const
{
    uint64_t ret = 0;
    for (const auto & [session_id, nodes] : ephemerals)
        ret += nodes.size();

    return ret;
}


}
