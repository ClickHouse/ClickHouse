#include <Coordination/KeeperStorage.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/setThreadName.h>
#include <mutex>
#include <functional>
#include <base/logger_useful.h>
#include <Common/StringUtils/StringUtils.h>
#include <sstream>
#include <iomanip>
#include <Poco/SHA1Engine.h>
#include <Poco/Base64Encoder.h>
#include <boost/algorithm/string.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

static String parentPath(const String & path)
{
    auto rslash_pos = path.rfind('/');
    if (rslash_pos > 0)
        return path.substr(0, rslash_pos);
    return "/";
}

static std::string getBaseName(const String & path)
{
    size_t basename_start = path.rfind('/');
    return std::string{&path[basename_start + 1], path.length() - basename_start - 1};
}

static String base64Encode(const String & decoded)
{
    std::ostringstream ostr; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    ostr.exceptions(std::ios::failbit);
    Poco::Base64Encoder encoder(ostr);
    encoder.rdbuf()->setLineLength(0);
    encoder << decoded;
    encoder.close();
    return ostr.str();
}

static String getSHA1(const String & userdata)
{
    Poco::SHA1Engine engine;
    engine.update(userdata);
    const auto & digest_id = engine.digest();
    return String{digest_id.begin(), digest_id.end()};
}

static String generateDigest(const String & userdata)
{
    std::vector<String> user_password;
    boost::split(user_password, userdata, [](char c) { return c == ':'; });
    return user_password[0] + ":" + base64Encode(getSHA1(userdata));
}

static bool checkACL(int32_t permission, const Coordination::ACLs & node_acls, const std::vector<KeeperStorage::AuthID> & session_auths)
{
    if (node_acls.empty())
        return true;

    for (const auto & session_auth : session_auths)
        if (session_auth.scheme == "super")
            return true;

    for (const auto & node_acl : node_acls)
    {
        if (node_acl.permissions & permission)
        {
            if (node_acl.scheme == "world" && node_acl.id == "anyone")
                return true;

            for (const auto & session_auth : session_auths)
            {
                if (node_acl.scheme == session_auth.scheme && node_acl.id == session_auth.id)
                    return true;
            }
        }
    }

    return false;
}

static bool fixupACL(
    const std::vector<Coordination::ACL> & request_acls,
    const std::vector<KeeperStorage::AuthID> & current_ids,
    std::vector<Coordination::ACL> & result_acls,
    bool hash_acls)
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
            if (hash_acls)
                new_acl.id = generateDigest(new_acl.id);
            result_acls.push_back(new_acl);
        }
    }
    return valid_found;
}

static KeeperStorage::ResponsesForSessions processWatchesImpl(const String & path, KeeperStorage::Watches & watches, KeeperStorage::Watches & list_watches, Coordination::Event event_type)
{
    KeeperStorage::ResponsesForSessions result;
    auto it = watches.find(path);
    if (it != watches.end())
    {
        std::shared_ptr<Coordination::ZooKeeperWatchResponse> watch_response = std::make_shared<Coordination::ZooKeeperWatchResponse>();
        watch_response->path = path;
        watch_response->xid = Coordination::WATCH_XID;
        watch_response->zxid = -1;
        watch_response->type = event_type;
        watch_response->state = Coordination::State::CONNECTED;
        for (auto watcher_session : it->second)
            result.push_back(KeeperStorage::ResponseForSession{watcher_session, watch_response});

        watches.erase(it);
    }

    auto parent_path = parentPath(path);

    Strings paths_to_check_for_list_watches;
    if (event_type == Coordination::Event::CREATED)
    {
        paths_to_check_for_list_watches.push_back(parent_path); /// Trigger list watches for parent
    }
    else if (event_type == Coordination::Event::DELETED)
    {
        paths_to_check_for_list_watches.push_back(path); /// Trigger both list watches for this path
        paths_to_check_for_list_watches.push_back(parent_path); /// And for parent path
    }
    /// CHANGED event never trigger list wathes

    for (const auto & path_to_check : paths_to_check_for_list_watches)
    {
        it = list_watches.find(path_to_check);
        if (it != list_watches.end())
        {
            std::shared_ptr<Coordination::ZooKeeperWatchResponse> watch_list_response = std::make_shared<Coordination::ZooKeeperWatchResponse>();
            watch_list_response->path = path_to_check;
            watch_list_response->xid = Coordination::WATCH_XID;
            watch_list_response->zxid = -1;
            if (path_to_check == parent_path)
                watch_list_response->type = Coordination::Event::CHILD;
            else
                watch_list_response->type = Coordination::Event::DELETED;

            watch_list_response->state = Coordination::State::CONNECTED;
            for (auto watcher_session : it->second)
                result.push_back(KeeperStorage::ResponseForSession{watcher_session, watch_list_response});

            list_watches.erase(it);
        }
    }
    return result;
}

KeeperStorage::KeeperStorage(int64_t tick_time_ms, const String & superdigest_)
    : session_expiry_queue(tick_time_ms)
    , superdigest(superdigest_)
{
    container.insert("/", Node());
}

using Undo = std::function<void()>;

struct KeeperStorageRequestProcessor
{
    Coordination::ZooKeeperRequestPtr zk_request;

    explicit KeeperStorageRequestProcessor(const Coordination::ZooKeeperRequestPtr & zk_request_)
        : zk_request(zk_request_)
    {}
    virtual std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(KeeperStorage & storage, int64_t zxid, int64_t session_id) const = 0;
    virtual KeeperStorage::ResponsesForSessions processWatches(KeeperStorage::Watches & /*watches*/, KeeperStorage::Watches & /*list_watches*/) const { return {}; }
    virtual bool checkAuth(KeeperStorage & /*storage*/, int64_t /*session_id*/) const { return true; }

    virtual ~KeeperStorageRequestProcessor() = default;
};

struct KeeperStorageHeartbeatRequestProcessor final : public KeeperStorageRequestProcessor
{
    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;
    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(KeeperStorage & /* storage */, int64_t /* zxid */, int64_t /* session_id */) const override
    {
        return {zk_request->makeResponse(), {}};
    }
};

struct KeeperStorageSyncRequestProcessor final : public KeeperStorageRequestProcessor
{
    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;
    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(KeeperStorage & /* storage */, int64_t /* zxid */, int64_t /* session_id */) const override
    {
        auto response = zk_request->makeResponse();
        dynamic_cast<Coordination::ZooKeeperSyncResponse &>(*response).path
            = dynamic_cast<Coordination::ZooKeeperSyncRequest &>(*zk_request).path;
        return {response, {}};
    }
};

struct KeeperStorageCreateRequestProcessor final : public KeeperStorageRequestProcessor
{
    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;

    KeeperStorage::ResponsesForSessions processWatches(KeeperStorage::Watches & watches, KeeperStorage::Watches & list_watches) const override
    {
        return processWatchesImpl(zk_request->getPath(), watches, list_watches, Coordination::Event::CREATED);
    }

    bool checkAuth(KeeperStorage & storage, int64_t session_id) const override
    {
        auto & container = storage.container;
        auto parent_path = parentPath(zk_request->getPath());

        auto it = container.find(parent_path);
        if (it == container.end())
            return true;

        const auto & node_acls = storage.acl_map.convertNumber(it->value.acl_id);
        if (node_acls.empty())
            return true;

        const auto & session_auths = storage.session_and_auth[session_id];
        return checkACL(Coordination::ACL::Create, node_acls, session_auths);
    }

    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(KeeperStorage & storage, int64_t zxid, int64_t session_id) const override
    {
        auto & container = storage.container;
        auto & ephemerals = storage.ephemerals;

        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Undo undo;
        Coordination::ZooKeeperCreateResponse & response = dynamic_cast<Coordination::ZooKeeperCreateResponse &>(*response_ptr);
        Coordination::ZooKeeperCreateRequest & request = dynamic_cast<Coordination::ZooKeeperCreateRequest &>(*zk_request);

        auto parent_path = parentPath(request.path);
        auto it = container.find(parent_path);

        if (it == container.end())
        {
            response.error = Coordination::Error::ZNONODE;
            return { response_ptr, undo };
        }
        else if (it->value.stat.ephemeralOwner != 0)
        {
            response.error = Coordination::Error::ZNOCHILDRENFOREPHEMERALS;
            return { response_ptr, undo };
        }
        std::string path_created = request.path;
        if (request.is_sequential)
        {
            auto seq_num = it->value.seq_num;

            std::stringstream seq_num_str;      // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            seq_num_str.exceptions(std::ios::failbit);
            seq_num_str << std::setw(10) << std::setfill('0') << seq_num;

            path_created += seq_num_str.str();
        }
        if (container.contains(path_created))
        {
            response.error = Coordination::Error::ZNODEEXISTS;
            return { response_ptr, undo };
        }
        auto child_path = getBaseName(path_created);
        if (child_path.empty())
        {
            response.error = Coordination::Error::ZBADARGUMENTS;
            return { response_ptr, undo };
        }

        auto & session_auth_ids = storage.session_and_auth[session_id];

        KeeperStorage::Node created_node;

        Coordination::ACLs node_acls;
        if (!fixupACL(request.acls, session_auth_ids, node_acls, !request.restored_from_zookeeper_log))
        {
            response.error = Coordination::Error::ZINVALIDACL;
            return {response_ptr, {}};
        }

        uint64_t acl_id = storage.acl_map.convertACLs(node_acls);
        storage.acl_map.addUsage(acl_id);

        created_node.acl_id = acl_id;
        created_node.stat.czxid = zxid;
        created_node.stat.mzxid = zxid;
        created_node.stat.pzxid = zxid;
        created_node.stat.ctime = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
        created_node.stat.mtime = created_node.stat.ctime;
        created_node.stat.numChildren = 0;
        created_node.stat.dataLength = request.data.length();
        created_node.stat.ephemeralOwner = request.is_ephemeral ? session_id : 0;
        created_node.data = request.data;
        created_node.is_sequental = request.is_sequential;

        int32_t parent_cversion = request.parent_cversion;
        int64_t prev_parent_zxid;
        int32_t prev_parent_cversion;
        container.updateValue(parent_path, [child_path, zxid, &prev_parent_zxid,
                                            parent_cversion, &prev_parent_cversion] (KeeperStorage::Node & parent)
        {

            parent.children.insert(child_path);
            prev_parent_cversion = parent.stat.cversion;
            prev_parent_zxid = parent.stat.pzxid;

            /// Increment sequential number even if node is not sequential
            ++parent.seq_num;

            if (parent_cversion == -1)
                ++parent.stat.cversion;
            else if (parent_cversion > parent.stat.cversion)
                parent.stat.cversion = parent_cversion;

            if (zxid > parent.stat.pzxid)
                parent.stat.pzxid = zxid;
            ++parent.stat.numChildren;
        });

        response.path_created = path_created;
        container.insert(path_created, std::move(created_node));

        if (request.is_ephemeral)
            ephemerals[session_id].emplace(path_created);

        undo = [&storage, prev_parent_zxid, prev_parent_cversion, session_id, path_created, is_ephemeral = request.is_ephemeral, parent_path, child_path, acl_id]
        {
            storage.container.erase(path_created);
            storage.acl_map.removeUsage(acl_id);

            if (is_ephemeral)
                storage.ephemerals[session_id].erase(path_created);

            storage.container.updateValue(parent_path, [child_path, prev_parent_zxid, prev_parent_cversion] (KeeperStorage::Node & undo_parent)
            {
                --undo_parent.stat.numChildren;
                --undo_parent.seq_num;
                undo_parent.stat.cversion = prev_parent_cversion;
                undo_parent.stat.pzxid = prev_parent_zxid;
                undo_parent.children.erase(child_path);
            });
        };

        response.error = Coordination::Error::ZOK;
        return { response_ptr, undo };
    }
};

struct KeeperStorageGetRequestProcessor final : public KeeperStorageRequestProcessor
{

    bool checkAuth(KeeperStorage & storage, int64_t session_id) const override
    {
        auto & container = storage.container;
        auto it = container.find(zk_request->getPath());
        if (it == container.end())
            return true;

        const auto & node_acls = storage.acl_map.convertNumber(it->value.acl_id);
        if (node_acls.empty())
            return true;

        const auto & session_auths = storage.session_and_auth[session_id];
        return checkACL(Coordination::ACL::Read, node_acls, session_auths);
    }

    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;
    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(KeeperStorage & storage, int64_t /* zxid */, int64_t /* session_id */) const override
    {
        auto & container = storage.container;
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperGetResponse & response = dynamic_cast<Coordination::ZooKeeperGetResponse &>(*response_ptr);
        Coordination::ZooKeeperGetRequest & request = dynamic_cast<Coordination::ZooKeeperGetRequest &>(*zk_request);

        auto it = container.find(request.path);
        if (it == container.end())
        {
            response.error = Coordination::Error::ZNONODE;
        }
        else
        {
            response.stat = it->value.stat;
            response.data = it->value.data;
            response.error = Coordination::Error::ZOK;
        }

        return { response_ptr, {} };
    }
};

namespace
{
    /// Garbage required to apply log to "fuzzy" zookeeper snapshot
    void updateParentPzxid(const std::string & child_path, int64_t zxid, KeeperStorage::Container & container)
    {
        auto parent_path = parentPath(child_path);
        auto parent_it = container.find(parent_path);
        if (parent_it != container.end())
        {
            container.updateValue(parent_path, [zxid](KeeperStorage::Node & parent)
            {
                if (parent.stat.pzxid < zxid)
                    parent.stat.pzxid = zxid;
            });
        }
    }
}

struct KeeperStorageRemoveRequestProcessor final : public KeeperStorageRequestProcessor
{
    bool checkAuth(KeeperStorage & storage, int64_t session_id) const override
    {
        auto & container = storage.container;
        auto it = container.find(parentPath(zk_request->getPath()));
        if (it == container.end())
            return true;

        const auto & node_acls = storage.acl_map.convertNumber(it->value.acl_id);
        if (node_acls.empty())
            return true;

        const auto & session_auths = storage.session_and_auth[session_id];
        return checkACL(Coordination::ACL::Delete, node_acls, session_auths);
    }

    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;
    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(KeeperStorage & storage, int64_t zxid, int64_t /*session_id*/) const override
    {
        auto & container = storage.container;
        auto & ephemerals = storage.ephemerals;

        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperRemoveResponse & response = dynamic_cast<Coordination::ZooKeeperRemoveResponse &>(*response_ptr);
        Coordination::ZooKeeperRemoveRequest & request = dynamic_cast<Coordination::ZooKeeperRemoveRequest &>(*zk_request);
        Undo undo;

        auto it = container.find(request.path);
        if (it == container.end())
        {
            if (request.restored_from_zookeeper_log)
                updateParentPzxid(request.path, zxid, container);
            response.error = Coordination::Error::ZNONODE;
        }
        else if (request.version != -1 && request.version != it->value.stat.version)
        {
            response.error = Coordination::Error::ZBADVERSION;
        }
        else if (it->value.stat.numChildren)
        {
            response.error = Coordination::Error::ZNOTEMPTY;
        }
        else
        {
            if (request.restored_from_zookeeper_log)
                updateParentPzxid(request.path, zxid, container);

            auto prev_node = it->value;
            if (prev_node.stat.ephemeralOwner != 0)
            {
                auto ephemerals_it = ephemerals.find(prev_node.stat.ephemeralOwner);
                ephemerals_it->second.erase(request.path);
                if (ephemerals_it->second.empty())
                    ephemerals.erase(ephemerals_it);
            }

            storage.acl_map.removeUsage(prev_node.acl_id);

            auto child_basename = getBaseName(it->key);
            container.updateValue(parentPath(request.path), [&child_basename] (KeeperStorage::Node & parent)
            {
                --parent.stat.numChildren;
                ++parent.stat.cversion;
                parent.children.erase(child_basename);
            });

            response.error = Coordination::Error::ZOK;

            container.erase(request.path);

            undo = [prev_node, &storage, path = request.path, child_basename]
            {
                if (prev_node.stat.ephemeralOwner != 0)
                    storage.ephemerals[prev_node.stat.ephemeralOwner].emplace(path);

                storage.acl_map.addUsage(prev_node.acl_id);

                storage.container.insert(path, prev_node);
                storage.container.updateValue(parentPath(path), [&child_basename] (KeeperStorage::Node & parent)
                {
                    ++parent.stat.numChildren;
                    --parent.stat.cversion;
                    parent.children.insert(child_basename);
                });
            };
        }

        return { response_ptr, undo };
    }

    KeeperStorage::ResponsesForSessions processWatches(KeeperStorage::Watches & watches, KeeperStorage::Watches & list_watches) const override
    {
        return processWatchesImpl(zk_request->getPath(), watches, list_watches, Coordination::Event::DELETED);
    }
};

struct KeeperStorageExistsRequestProcessor final : public KeeperStorageRequestProcessor
{
    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;
    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(KeeperStorage & storage, int64_t /*zxid*/, int64_t /* session_id */) const override
    {
        auto & container = storage.container;

        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperExistsResponse & response = dynamic_cast<Coordination::ZooKeeperExistsResponse &>(*response_ptr);
        Coordination::ZooKeeperExistsRequest & request = dynamic_cast<Coordination::ZooKeeperExistsRequest &>(*zk_request);

        auto it = container.find(request.path);
        if (it != container.end())
        {
            response.stat = it->value.stat;
            response.error = Coordination::Error::ZOK;
        }
        else
        {
            response.error = Coordination::Error::ZNONODE;
        }

        return { response_ptr, {} };
    }
};

struct KeeperStorageSetRequestProcessor final : public KeeperStorageRequestProcessor
{
    bool checkAuth(KeeperStorage & storage, int64_t session_id) const override
    {
        auto & container = storage.container;
        auto it = container.find(zk_request->getPath());
        if (it == container.end())
            return true;

        const auto & node_acls = storage.acl_map.convertNumber(it->value.acl_id);
        if (node_acls.empty())
            return true;

        const auto & session_auths = storage.session_and_auth[session_id];
        return checkACL(Coordination::ACL::Write, node_acls, session_auths);
    }

    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;
    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(KeeperStorage & storage, int64_t zxid, int64_t /* session_id */) const override
    {
        auto & container = storage.container;

        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperSetResponse & response = dynamic_cast<Coordination::ZooKeeperSetResponse &>(*response_ptr);
        Coordination::ZooKeeperSetRequest & request = dynamic_cast<Coordination::ZooKeeperSetRequest &>(*zk_request);
        Undo undo;

        auto it = container.find(request.path);
        if (it == container.end())
        {
            response.error = Coordination::Error::ZNONODE;
        }
        else if (request.version == -1 || request.version == it->value.stat.version)
        {

            auto prev_node = it->value;

            auto itr = container.updateValue(request.path, [zxid, request] (KeeperStorage::Node & value)
            {
                value.data = request.data;
                value.stat.version++;
                value.stat.mzxid = zxid;
                value.stat.mtime = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
                value.stat.dataLength = request.data.length();
                value.data = request.data;
            });

            container.updateValue(parentPath(request.path), [] (KeeperStorage::Node & parent)
            {
                parent.stat.cversion++;
            });

            response.stat = itr->value.stat;
            response.error = Coordination::Error::ZOK;

            undo = [prev_node, &container, path = request.path]
            {
                container.updateValue(path, [&prev_node] (KeeperStorage::Node & value) { value = prev_node; });
                container.updateValue(parentPath(path), [] (KeeperStorage::Node & parent)
                {
                    parent.stat.cversion--;
                });
            };
        }
        else
        {
            response.error = Coordination::Error::ZBADVERSION;
        }

        return { response_ptr, undo };
    }

    KeeperStorage::ResponsesForSessions processWatches(KeeperStorage::Watches & watches, KeeperStorage::Watches & list_watches) const override
    {
        return processWatchesImpl(zk_request->getPath(), watches, list_watches, Coordination::Event::CHANGED);
    }
};

struct KeeperStorageListRequestProcessor final : public KeeperStorageRequestProcessor
{
    bool checkAuth(KeeperStorage & storage, int64_t session_id) const override
    {
        auto & container = storage.container;
        auto it = container.find(zk_request->getPath());
        if (it == container.end())
            return true;

        const auto & node_acls = storage.acl_map.convertNumber(it->value.acl_id);
        if (node_acls.empty())
            return true;

        const auto & session_auths = storage.session_and_auth[session_id];
        return checkACL(Coordination::ACL::Read, node_acls, session_auths);
    }

    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;
    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(KeeperStorage & storage, int64_t /*zxid*/, int64_t /*session_id*/) const override
    {
        auto & container = storage.container;
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperListResponse & response = dynamic_cast<Coordination::ZooKeeperListResponse &>(*response_ptr);
        Coordination::ZooKeeperListRequest & request = dynamic_cast<Coordination::ZooKeeperListRequest &>(*zk_request);
        auto it = container.find(request.path);
        if (it == container.end())
        {
            response.error = Coordination::Error::ZNONODE;
        }
        else
        {
            auto path_prefix = request.path;
            if (path_prefix.empty())
                throw DB::Exception("Logical error: path cannot be empty", ErrorCodes::LOGICAL_ERROR);

            response.names.insert(response.names.end(), it->value.children.begin(), it->value.children.end());

            response.stat = it->value.stat;
            response.error = Coordination::Error::ZOK;
        }

        return { response_ptr, {} };
    }
};

struct KeeperStorageCheckRequestProcessor final : public KeeperStorageRequestProcessor
{
    bool checkAuth(KeeperStorage & storage, int64_t session_id) const override
    {
        auto & container = storage.container;
        auto it = container.find(zk_request->getPath());
        if (it == container.end())
            return true;

        const auto & node_acls = storage.acl_map.convertNumber(it->value.acl_id);
        if (node_acls.empty())
            return true;

        const auto & session_auths = storage.session_and_auth[session_id];
        return checkACL(Coordination::ACL::Read, node_acls, session_auths);
    }

    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;
    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(KeeperStorage & storage, int64_t /*zxid*/, int64_t /*session_id*/) const override
    {
        auto & container = storage.container;

        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperCheckResponse & response = dynamic_cast<Coordination::ZooKeeperCheckResponse &>(*response_ptr);
        Coordination::ZooKeeperCheckRequest & request = dynamic_cast<Coordination::ZooKeeperCheckRequest &>(*zk_request);
        auto it = container.find(request.path);
        if (it == container.end())
        {
            response.error = Coordination::Error::ZNONODE;
        }
        else if (request.version != -1 && request.version != it->value.stat.version)
        {
            response.error = Coordination::Error::ZBADVERSION;
        }
        else
        {
            response.error = Coordination::Error::ZOK;
        }

        return { response_ptr, {} };
    }
};


struct KeeperStorageSetACLRequestProcessor final : public KeeperStorageRequestProcessor
{
    bool checkAuth(KeeperStorage & storage, int64_t session_id) const override
    {
        auto & container = storage.container;
        auto it = container.find(zk_request->getPath());
        if (it == container.end())
            return true;

        const auto & node_acls = storage.acl_map.convertNumber(it->value.acl_id);
        if (node_acls.empty())
            return true;

        const auto & session_auths = storage.session_and_auth[session_id];
        return checkACL(Coordination::ACL::Admin, node_acls, session_auths);
    }

    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;

    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(KeeperStorage & storage, int64_t /*zxid*/, int64_t session_id) const override
    {
        auto & container = storage.container;

        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperSetACLResponse & response = dynamic_cast<Coordination::ZooKeeperSetACLResponse &>(*response_ptr);
        Coordination::ZooKeeperSetACLRequest & request = dynamic_cast<Coordination::ZooKeeperSetACLRequest &>(*zk_request);
        auto it = container.find(request.path);
        if (it == container.end())
        {
            response.error = Coordination::Error::ZNONODE;
        }
        else if (request.version != -1 && request.version != it->value.stat.aversion)
        {
            response.error = Coordination::Error::ZBADVERSION;
        }
        else
        {
            auto & session_auth_ids = storage.session_and_auth[session_id];
            Coordination::ACLs node_acls;

            if (!fixupACL(request.acls, session_auth_ids, node_acls, !request.restored_from_zookeeper_log))
            {
                response.error = Coordination::Error::ZINVALIDACL;
                return {response_ptr, {}};
            }

            uint64_t acl_id = storage.acl_map.convertACLs(node_acls);
            storage.acl_map.addUsage(acl_id);

            storage.container.updateValue(request.path, [acl_id] (KeeperStorage::Node & node)
            {
                node.acl_id = acl_id;
                ++node.stat.aversion;
            });

            response.stat = it->value.stat;
            response.error = Coordination::Error::ZOK;
        }

        /// It cannot be used insied multitransaction?
        return { response_ptr, {} };
    }
};

struct KeeperStorageGetACLRequestProcessor final : public KeeperStorageRequestProcessor
{
    bool checkAuth(KeeperStorage & storage, int64_t session_id) const override
    {
        auto & container = storage.container;
        auto it = container.find(zk_request->getPath());
        if (it == container.end())
            return true;

        const auto & node_acls = storage.acl_map.convertNumber(it->value.acl_id);
        if (node_acls.empty())
            return true;

        const auto & session_auths = storage.session_and_auth[session_id];
        /// LOL, GetACL require more permissions, then SetACL...
        return checkACL(Coordination::ACL::Admin | Coordination::ACL::Read, node_acls, session_auths);
    }
    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;

    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(KeeperStorage & storage, int64_t /*zxid*/, int64_t /*session_id*/) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperGetACLResponse & response = dynamic_cast<Coordination::ZooKeeperGetACLResponse &>(*response_ptr);
        Coordination::ZooKeeperGetACLRequest & request = dynamic_cast<Coordination::ZooKeeperGetACLRequest &>(*zk_request);
        auto & container = storage.container;
        auto it = container.find(request.path);
        if (it == container.end())
        {
            response.error = Coordination::Error::ZNONODE;
        }
        else
        {
            response.stat = it->value.stat;
            response.acl = storage.acl_map.convertNumber(it->value.acl_id);
        }

        return {response_ptr, {}};
    }
};

struct KeeperStorageMultiRequestProcessor final : public KeeperStorageRequestProcessor
{
    bool checkAuth(KeeperStorage & storage, int64_t session_id) const override
    {
        for (const auto & concrete_request : concrete_requests)
            if (!concrete_request->checkAuth(storage, session_id))
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
            if (sub_zk_request->getOpNum() == Coordination::OpNum::Create)
            {
                concrete_requests.push_back(std::make_shared<KeeperStorageCreateRequestProcessor>(sub_zk_request));
            }
            else if (sub_zk_request->getOpNum() == Coordination::OpNum::Remove)
            {
                concrete_requests.push_back(std::make_shared<KeeperStorageRemoveRequestProcessor>(sub_zk_request));
            }
            else if (sub_zk_request->getOpNum() == Coordination::OpNum::Set)
            {
                concrete_requests.push_back(std::make_shared<KeeperStorageSetRequestProcessor>(sub_zk_request));
            }
            else if (sub_zk_request->getOpNum() == Coordination::OpNum::Check)
            {
                concrete_requests.push_back(std::make_shared<KeeperStorageCheckRequestProcessor>(sub_zk_request));
            }
            else
                throw DB::Exception(ErrorCodes::BAD_ARGUMENTS, "Illegal command as part of multi ZooKeeper request {}", sub_zk_request->getOpNum());
        }
    }

    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(KeeperStorage & storage, int64_t zxid, int64_t session_id) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperMultiResponse & response = dynamic_cast<Coordination::ZooKeeperMultiResponse &>(*response_ptr);
        std::vector<Undo> undo_actions;

        try
        {
            size_t i = 0;
            for (const auto & concrete_request : concrete_requests)
            {
                auto [ cur_response, undo_action ] = concrete_request->process(storage, zxid, session_id);

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

                    for (auto it = undo_actions.rbegin(); it != undo_actions.rend(); ++it)
                        if (*it)
                            (*it)();

                    return { response_ptr, {} };
                }
                else
                    undo_actions.emplace_back(std::move(undo_action));

                ++i;
            }

            response.error = Coordination::Error::ZOK;
            return { response_ptr, {} };
        }
        catch (...)
        {
            for (auto it = undo_actions.rbegin(); it != undo_actions.rend(); ++it)
                if (*it)
                    (*it)();
            throw;
        }
    }

    KeeperStorage::ResponsesForSessions processWatches(KeeperStorage::Watches & watches, KeeperStorage::Watches & list_watches) const override
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
    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(KeeperStorage &, int64_t, int64_t) const override
    {
        throw DB::Exception("Called process on close request", ErrorCodes::LOGICAL_ERROR);
    }
};

struct KeeperStorageAuthRequestProcessor final : public KeeperStorageRequestProcessor
{
    using KeeperStorageRequestProcessor::KeeperStorageRequestProcessor;
    std::pair<Coordination::ZooKeeperResponsePtr, Undo> process(KeeperStorage & storage, int64_t /*zxid*/, int64_t session_id) const override
    {
        Coordination::ZooKeeperAuthRequest & auth_request = dynamic_cast<Coordination::ZooKeeperAuthRequest &>(*zk_request);
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperAuthResponse & auth_response =  dynamic_cast<Coordination::ZooKeeperAuthResponse &>(*response_ptr);
        auto & sessions_and_auth = storage.session_and_auth;

        if (auth_request.scheme != "digest" || std::count(auth_request.data.begin(), auth_request.data.end(), ':') != 1)
        {
            auth_response.error = Coordination::Error::ZAUTHFAILED;
        }
        else
        {
            auto digest = generateDigest(auth_request.data);
            if (digest == storage.superdigest)
            {
                KeeperStorage::AuthID auth{"super", ""};
                sessions_and_auth[session_id].emplace_back(auth);
            }
            else
            {
                KeeperStorage::AuthID auth{auth_request.scheme, digest};
                auto & session_ids = sessions_and_auth[session_id];
                if (std::find(session_ids.begin(), session_ids.end(), auth) == session_ids.end())
                    sessions_and_auth[session_id].emplace_back(auth);
            }

        }

        return { response_ptr, {} };
    }
};

void KeeperStorage::finalize()
{
    if (finalized)
        throw DB::Exception("Testkeeper storage already finalized", ErrorCodes::LOGICAL_ERROR);

    finalized = true;

    for (const auto & [session_id, ephemerals_paths] : ephemerals)
        for (const String & ephemeral_path : ephemerals_paths)
            container.erase(ephemeral_path);

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
        auto it = op_num_to_request.find(zk_request->getOpNum());
        if (it == op_num_to_request.end())
            throw DB::Exception("Unknown operation type " + toString(zk_request->getOpNum()), ErrorCodes::LOGICAL_ERROR);

        return it->second(zk_request);
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

template<Coordination::OpNum num, typename RequestT>
void registerKeeperRequestProcessor(KeeperStorageRequestProcessorsFactory & factory)
{
    factory.registerRequest(num, [] (const Coordination::ZooKeeperRequestPtr & zk_request) { return std::make_shared<RequestT>(zk_request); });
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
    registerKeeperRequestProcessor<Coordination::OpNum::Check, KeeperStorageCheckRequestProcessor>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::Multi, KeeperStorageMultiRequestProcessor>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::SetACL, KeeperStorageSetACLRequestProcessor>(*this);
    registerKeeperRequestProcessor<Coordination::OpNum::GetACL, KeeperStorageGetACLRequestProcessor>(*this);
}


KeeperStorage::ResponsesForSessions KeeperStorage::processRequest(const Coordination::ZooKeeperRequestPtr & zk_request, int64_t session_id, std::optional<int64_t> new_last_zxid, bool check_acl)
{
    KeeperStorage::ResponsesForSessions results;
    if (new_last_zxid)
    {
        if (zxid >= *new_last_zxid)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got new ZXID {} smaller or equal than current {}. It's a bug", *new_last_zxid, zxid);
        zxid = *new_last_zxid;
    }

    /// ZooKeeper update sessions expirity for each request, not only for heartbeats
    session_expiry_queue.addNewSessionOrUpdate(session_id, session_and_timeout[session_id]);

    if (zk_request->getOpNum() == Coordination::OpNum::Close) /// Close request is special
    {
        auto it = ephemerals.find(session_id);
        if (it != ephemerals.end())
        {
            for (const auto & ephemeral_path : it->second)
            {
                container.erase(ephemeral_path);
                container.updateValue(parentPath(ephemeral_path), [&ephemeral_path] (KeeperStorage::Node & parent)
                {
                    --parent.stat.numChildren;
                    ++parent.stat.cversion;
                    parent.children.erase(getBaseName(ephemeral_path));
                });

                auto responses = processWatchesImpl(ephemeral_path, watches, list_watches, Coordination::Event::DELETED);
                results.insert(results.end(), responses.begin(), responses.end());
            }
            ephemerals.erase(it);
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
        KeeperStorageRequestProcessorPtr storage_request = KeeperStorageRequestProcessorsFactory::instance().get(zk_request);
        auto [response, _] = storage_request->process(*this, zxid, session_id);
        response->xid = zk_request->xid;
        response->zxid = getZXID();

        results.push_back(ResponseForSession{session_id, response});
    }
    else /// normal requests proccession
    {
        KeeperStorageRequestProcessorPtr request_processor = KeeperStorageRequestProcessorsFactory::instance().get(zk_request);
        Coordination::ZooKeeperResponsePtr response;

        if (check_acl && !request_processor->checkAuth(*this, session_id))
        {
            response = zk_request->makeResponse();
            /// Original ZooKeeper always throws no auth, even when user provided some credentials
            response->error = Coordination::Error::ZNOAUTH;
        }
        else
        {
            std::tie(response, std::ignore) = request_processor->process(*this, zxid, session_id);
        }

        /// Watches for this requests are added to the watches lists
        if (zk_request->has_watch)
        {
            if (response->error == Coordination::Error::ZOK)
            {
                auto & watches_type = zk_request->getOpNum() == Coordination::OpNum::List || zk_request->getOpNum() == Coordination::OpNum::SimpleList
                    ? list_watches
                    : watches;

                watches_type[zk_request->getPath()].emplace_back(session_id);
                sessions_and_watchers[session_id].emplace(zk_request->getPath());
            }
            else if (response->error == Coordination::Error::ZNONODE && zk_request->getOpNum() == Coordination::OpNum::Exists)
            {
                watches[zk_request->getPath()].emplace_back(session_id);
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
                for (auto w_it = watches_for_path.begin(); w_it != watches_for_path.end();)
                {
                    if (*w_it == session_id)
                        w_it = watches_for_path.erase(w_it);
                    else
                        ++w_it;
                }
                if (watches_for_path.empty())
                    watches.erase(watch);
            }

            /// Maybe it's a list watch
            auto list_watch = list_watches.find(watch_path);
            if (list_watch != list_watches.end())
            {
                auto & list_watches_for_path = list_watch->second;
                for (auto w_it = list_watches_for_path.begin(); w_it != list_watches_for_path.end();)
                {
                    if (*w_it == session_id)
                        w_it = list_watches_for_path.erase(w_it);
                    else
                        ++w_it;
                }
                if (list_watches_for_path.empty())
                    list_watches.erase(list_watch);
            }
        }

        sessions_and_watchers.erase(watches_it);
    }
}

}
