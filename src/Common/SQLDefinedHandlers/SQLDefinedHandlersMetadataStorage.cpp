#include <Common/SQLDefinedHandlers/SQLDefinedHandlersMetadataStorage.h>
#include <Common/SQLDefinedHandlers/SQLDefinedHandlerFromAST.h>

#include <filesystem>
#include <Core/Settings.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateHandlerQuery.h>
#include <Parsers/ParserCreateHandlerQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsBool fsync_metadata;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

namespace ErrorCodes
{
    extern const int HANDLER_ALREADY_EXISTS;
    extern const int HANDLER_DOESNT_EXIST;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int LOGICAL_ERROR;
}

static const std::string query_rules_storage_config_path = "query_rules_storage";

namespace
{
    std::string getFileName(const std::string & handler_name)
    {
        return escapeForFileName(handler_name) + ".sql";
    }
}

class SQLDefinedHandlersMetadataStorage::IStorage
{
public:
    virtual ~IStorage() = default;

    virtual bool exists(const std::string & path) const = 0;
    virtual std::vector<std::string> list() const = 0;
    /// List children together with a version token of the whole set (the Keeper root-node data version
    /// for replicated storage; 0 for local). The version is fed back to `write` to make the cross-replica
    /// "read set -> check ambiguity -> persist" sequence atomic via optimistic concurrency.
    virtual std::vector<std::string> list(Int32 & version) const { version = 0; return list(); }
    virtual std::string read(const std::string & path) const = 0;
    /// Persist data. When `expected_root_version >= 0` the write is conditional: it commits only if the
    /// set has not changed since it was read at that version, otherwise nothing is written and `false` is
    /// returned so the caller can re-read and retry. With `expected_root_version == -1` the write is
    /// unconditional. Returns true if the data was written.
    virtual bool write(const std::string & path, const std::string & data, bool replace, Int32 expected_root_version) = 0;
    virtual void remove(const std::string & path) = 0;
    virtual bool removeIfExists(const std::string & path) = 0;
    virtual bool isReplicated() const = 0;
    virtual bool waitUpdate(size_t /* timeout */) { return false; }
};


class SQLDefinedHandlersMetadataStorage::LocalStorage : public IStorage, protected WithContext
{
private:
    std::string root_path;

public:
    LocalStorage(ContextPtr context_, const std::string & path_)
        : WithContext(context_)
        , root_path(path_)
    {
        if (fs::exists(root_path))
            cleanup();
    }

    bool isReplicated() const override { return false; }

    std::vector<std::string> list() const override
    {
        if (!fs::exists(root_path))
            return {};

        std::vector<std::string> elements;
        for (fs::directory_iterator it{root_path}; it != fs::directory_iterator{}; ++it)
        {
            const auto & current_path = it->path();
            if (current_path.extension() == ".sql")
                elements.push_back(it->path());
            else
                LOG_WARNING(getLogger("SQLDefinedHandlersLocalStorage"),
                    "Unexpected file {} in handlers directory", current_path.filename().string());
        }
        return elements;
    }

    bool exists(const std::string & file_name) const override
    {
        return fs::exists(getPath(file_name));
    }

    std::string read(const std::string & file_name) const override
    {
        ReadBufferFromFile in(getPath(file_name));
        std::string data;
        readStringUntilEOF(data, in);
        return data;
    }

    bool write(const std::string & file_name, const std::string & data, bool replace, Int32 /* expected_root_version */) override
    {
        /// Local storage is single-node: the factory's mutex already serializes writes, so there is no
        /// version to check (`expected_root_version` is meaningful only for replicated storage).
        if (!replace && fs::exists(getPath(file_name)))
            throw Exception(ErrorCodes::HANDLER_ALREADY_EXISTS, "Metadata file for handler already exists: {}", file_name);

        fs::create_directories(root_path);

        auto tmp_path = getPath(file_name + ".tmp");
        WriteBufferFromFile out(tmp_path, data.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(data, out);

        out.next();
        if (getContext()->getSettingsRef()[Setting::fsync_metadata])
            out.sync();
        out.close();

        fs::rename(tmp_path, getPath(file_name));
        return true;
    }

    void remove(const std::string & file_name) override
    {
        if (!removeIfExists(file_name))
            throw Exception(ErrorCodes::HANDLER_DOESNT_EXIST, "Cannot remove `{}`, because it doesn't exist", file_name);
    }

    bool removeIfExists(const std::string & file_name) override
    {
        return fs::remove(getPath(file_name));
    }

private:
    std::string getPath(const std::string & file_name) const
    {
        const auto file_name_as_path = fs::path(file_name);
        if (file_name_as_path.is_absolute())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Filename {} cannot be an absolute path", file_name);
        return fs::path(root_path) / file_name_as_path;
    }

    void cleanup()
    {
        std::vector<std::string> files_to_remove;
        for (fs::directory_iterator it{root_path}; it != fs::directory_iterator{}; ++it)
        {
            if (it->path().extension() == ".tmp")
                files_to_remove.push_back(it->path());
        }
        for (const auto & file : files_to_remove)
            fs::remove(file);
    }
};


class SQLDefinedHandlersMetadataStorage::ZooKeeperStorage : public IStorage, protected WithContext
{
private:
    std::string root_path;
    mutable zkutil::ZooKeeperPtr zookeeper_client{nullptr};
    mutable Coordination::EventPtr wait_event;
    /// Version of the root node's data. It is bumped on every create/drop/alter (see `bumpVersionRequest`),
    /// so a single data-watch on the root notifies replicas of all kinds of changes - in particular
    /// ALTER, which only changes a child's data and would not be observed by a children-list watch.
    mutable Int32 root_version = 0;

public:
    ZooKeeperStorage(ContextPtr context_, const std::string & path_)
        : WithContext(context_)
        , root_path(path_)
    {
        auto component_guard = Coordination::setCurrentComponent("SQLDefinedHandlersMetadataStorage::ZooKeeperStorage");
        if (root_path.empty())
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Handlers path cannot be empty");

        if (root_path != "/" && root_path.back() == '/')
            root_path.resize(root_path.size() - 1);
        if (root_path.front() != '/')
            root_path = "/" + root_path;

        auto client = getClient();
        if (root_path != "/" && !client->exists(root_path))
        {
            client->createAncestors(root_path);
            client->createIfNotExists(root_path, "");
        }
    }

    bool isReplicated() const override { return true; }

    bool waitUpdate(size_t timeout) override
    {
        auto component_guard = Coordination::setCurrentComponent("SQLDefinedHandlersMetadataStorage::waitUpdate");
        if (!wait_event)
            return true;

        if (wait_event->tryWait(timeout))
            return true;

        std::string res;
        Coordination::Stat stat;
        if (!getClient()->tryGet(root_path, res, &stat))
        {
            chassert(false);
            return false;
        }
        return stat.version != root_version;
    }

    std::vector<std::string> list() const override
    {
        auto component_guard = Coordination::setCurrentComponent("SQLDefinedHandlersMetadataStorage::list");
        if (!wait_event)
            wait_event = std::make_shared<Poco::Event>();

        /// Set a data-watch on the root node and remember its version. Every modification bumps the
        /// root version (see `bumpVersionRequest`), so this watch fires for create, drop and alter alike.
        Coordination::Stat stat;
        getClient()->get(root_path, &stat, wait_event);
        root_version = stat.version;

        return getClient()->getChildren(root_path);
    }

    std::vector<std::string> list(Int32 & version) const override
    {
        auto component_guard = Coordination::setCurrentComponent("SQLDefinedHandlersMetadataStorage::list");
        /// Read the children and the root-node version in one snapshot, without touching the background
        /// update watch (`wait_event`/`root_version`). The version is bumped by every mutation (see
        /// `bumpVersionRequest`), so conditioning a later write on it detects any concurrent change.
        Coordination::Stat stat;
        auto children = getClient()->getChildren(root_path, &stat);
        version = stat.version;
        return children;
    }

    bool exists(const std::string & file_name) const override
    {
        auto component_guard = Coordination::setCurrentComponent("SQLDefinedHandlersMetadataStorage::exists");
        return getClient()->exists(getPath(file_name));
    }

    std::string read(const std::string & file_name) const override
    {
        auto component_guard = Coordination::setCurrentComponent("SQLDefinedHandlersMetadataStorage::read");
        return getClient()->get(getPath(file_name));
    }

    bool write(const std::string & file_name, const std::string & data, bool replace, Int32 expected_root_version) override
    {
        auto component_guard = Coordination::setCurrentComponent("SQLDefinedHandlersMetadataStorage::write");

        /// Mutate the child node and bump the root version (see `bumpVersionRequest`) in a single Keeper
        /// transaction, so the watched root version always advances together with the change. Otherwise a
        /// connection loss between the child mutation and a separate version bump would commit the change
        /// while leaving other replicas unnotified, so they would keep serving a stale set of handlers.
        ///
        /// When `expected_root_version >= 0` the bump is conditional on that version: the caller read the
        /// whole set at that version and validated ambiguity against it, so the write must commit only if
        /// no other replica changed the set in the meantime. This serializes the ambiguity check with the
        /// write, closing the race where two replicas concurrently create overlapping handlers (each
        /// passing its own local ambiguity check). A version mismatch is reported as a non-fatal retry
        /// signal (`false`), prompting the caller to re-read and re-validate.
        Coordination::Requests requests;
        if (replace)
            /// ALTER must update an existing handler only; using `set` (not create-or-update) prevents a
            /// delayed ALTER from resurrecting a handler that was concurrently dropped on another replica.
            requests.push_back(zkutil::makeSetRequest(getPath(file_name), data, -1));
        else
            requests.push_back(zkutil::makeCreateRequest(getPath(file_name), data, zkutil::CreateMode::Persistent));
        requests.push_back(bumpVersionRequest(expected_root_version));

        Coordination::Responses responses;
        auto code = getClient()->tryMulti(requests, responses);
        if (code == Coordination::Error::ZOK)
            return true;

        /// On the optimistic (replicated) path, treat every precondition mismatch as a retry signal: the
        /// caller will re-read the set and decide. A version mismatch means the set changed; a node
        /// existence mismatch means the same handler was concurrently created/dropped.
        if (expected_root_version >= 0
            && (code == Coordination::Error::ZBADVERSION
                || (!replace && code == Coordination::Error::ZNODEEXISTS)
                || (replace && code == Coordination::Error::ZNONODE)))
            return false;

        if (replace && code == Coordination::Error::ZNONODE)
            throw Exception(ErrorCodes::HANDLER_DOESNT_EXIST, "Handler `{}` doesn't exist", file_name);
        if (!replace && code == Coordination::Error::ZNODEEXISTS)
            throw Exception(ErrorCodes::HANDLER_ALREADY_EXISTS, "Metadata file for handler already exists: {}", file_name);
        zkutil::KeeperMultiException::check(code, requests, responses);
        return false;
    }

    void remove(const std::string & file_name) override
    {
        auto component_guard = Coordination::setCurrentComponent("SQLDefinedHandlersMetadataStorage::remove");
        Coordination::Requests requests;
        requests.push_back(zkutil::makeRemoveRequest(getPath(file_name), -1));
        requests.push_back(bumpVersionRequest());

        Coordination::Responses responses;
        auto code = getClient()->tryMulti(requests, responses);
        if (code != Coordination::Error::ZOK)
            zkutil::KeeperMultiException::check(code, requests, responses);
    }

    bool removeIfExists(const std::string & file_name) override
    {
        auto component_guard = Coordination::setCurrentComponent("SQLDefinedHandlersMetadataStorage::removeIfExists");
        Coordination::Requests requests;
        requests.push_back(zkutil::makeRemoveRequest(getPath(file_name), -1));
        requests.push_back(bumpVersionRequest());

        Coordination::Responses responses;
        auto code = getClient()->tryMulti(requests, responses);
        if (code == Coordination::Error::ZOK)
            return true;
        /// The node is absent: the whole transaction is rejected atomically, so the root version is not
        /// bumped and nothing changed - this is exactly the IF EXISTS no-op contract.
        if (code == Coordination::Error::ZNONODE)
            return false;
        zkutil::KeeperMultiException::check(code, requests, responses);
        return false;
    }

private:
    /// A request that bumps the root node's data version to notify all replicas (including for ALTER,
    /// which only changes child data and would otherwise be invisible to a children-list watch). It is
    /// always issued together with the child mutation in a single `multi`, so the two never diverge.
    /// `expected_version == -1` bumps unconditionally; a non-negative value makes the whole transaction
    /// (and therefore the child mutation) commit only if the set was not changed since it was read.
    Coordination::RequestPtr bumpVersionRequest(Int32 expected_version = -1) const
    {
        return zkutil::makeSetRequest(root_path, "", expected_version);
    }

    zkutil::ZooKeeperPtr getClient() const
    {
        if (!zookeeper_client || zookeeper_client->expired())
        {
            zookeeper_client = getContext()->getZooKeeper();
            zookeeper_client->sync(root_path);
        }
        return zookeeper_client;
    }

    std::string getPath(const std::string & file_name) const
    {
        const auto file_name_as_path = fs::path(file_name);
        if (file_name_as_path.is_absolute())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Filename {} cannot be an absolute path", file_name);
        return fs::path(root_path) / file_name_as_path;
    }
};


SQLDefinedHandlersMetadataStorage::SQLDefinedHandlersMetadataStorage(std::shared_ptr<IStorage> storage_, ContextPtr context_)
    : WithContext(context_)
    , storage(std::move(storage_))
{
}

static std::vector<std::string> handlerNamesFromPaths(const std::vector<std::string> & paths)
{
    std::vector<std::string> handlers;
    handlers.reserve(paths.size());
    for (const auto & path : paths)
        handlers.push_back(unescapeForFileName(fs::path(path).stem()));
    return handlers;
}

std::vector<std::string> SQLDefinedHandlersMetadataStorage::listHandlers() const
{
    /// Use the unversioned `list`, which for Keeper storage arms the background update watch. This is the
    /// path taken by the normal reload (`getAll`); the versioned overload below must not be routed here, or
    /// the watch would never be installed and the update thread would spin in a tight reload loop.
    return handlerNamesFromPaths(storage->list());
}

std::vector<std::string> SQLDefinedHandlersMetadataStorage::listHandlers(Int32 & version) const
{
    return handlerNamesFromPaths(storage->list(version));
}

SQLDefinedHandlerPtr SQLDefinedHandlersMetadataStorage::readHandler(const std::string & handler_name) const
{
    const auto path = getFileName(handler_name);
    auto statement = storage->read(path);
    const auto & settings = getContext()->getSettingsRef();

    ParserCreateHandlerQuery parser(statement.data() + statement.size());
    auto ast = parseQuery(parser, statement, "in handler " + path, 0, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
    const auto & create_query = ast->as<const ASTCreateHandlerQuery &>();
    return makeSQLDefinedHandler(create_query);
}

SQLDefinedHandlers SQLDefinedHandlersMetadataStorage::getAll() const
{
    /// The normal reload path: list via the watch-arming `listHandlers()` (see the note there).
    return readHandlers(listHandlers());
}

SQLDefinedHandlers SQLDefinedHandlersMetadataStorage::getAll(Int32 & version) const
{
    /// The replicated read-check-write path: list at a known root version without touching the watch.
    return readHandlers(listHandlers(version));
}

SQLDefinedHandlers SQLDefinedHandlersMetadataStorage::readHandlers(const std::vector<std::string> & handler_names) const
{
    SQLDefinedHandlers result;
    for (const auto & handler_name : handler_names)
    {
        if (result.contains(handler_name))
            throw Exception(ErrorCodes::HANDLER_ALREADY_EXISTS, "Found duplicate handler `{}`", handler_name);
        try
        {
            result.emplace(handler_name, readHandler(handler_name));
        }
        catch (const Coordination::Exception & e)
        {
            if (e.code == Coordination::Error::ZNONODE)
            {
                LOG_DEBUG(getLogger("SQLDefinedHandlersMetadataStorage"),
                    "Handler '{}' was removed while reading, skipping", handler_name);
                continue;
            }
            throw;
        }
    }
    return result;
}

SQLDefinedHandlerPtr SQLDefinedHandlersMetadataStorage::buildUpdatedHandler(const ASTCreateHandlerQuery & alter_query) const
{
    return buildUpdatedHandler(storage->read(getFileName(alter_query.handler_name)), alter_query);
}

SQLDefinedHandlerPtr SQLDefinedHandlersMetadataStorage::buildUpdatedHandler(const String & base_create_statement, const ASTCreateHandlerQuery & alter_query) const
{
    const auto & settings = getContext()->getSettingsRef();

    ParserCreateHandlerQuery parser(base_create_statement.data() + base_create_statement.size());
    auto ast = parseQuery(parser, base_create_statement, "in handler " + alter_query.handler_name, 0, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
    auto & create_query = ast->as<ASTCreateHandlerQuery &>();

    mergeAlterIntoCreateHandler(create_query, alter_query);

    return makeSQLDefinedHandler(create_query);
}

void SQLDefinedHandlersMetadataStorage::remove(const std::string & handler_name)
{
    storage->remove(getFileName(handler_name));
}

bool SQLDefinedHandlersMetadataStorage::removeIfExists(const std::string & handler_name)
{
    return storage->removeIfExists(getFileName(handler_name));
}

bool SQLDefinedHandlersMetadataStorage::exists(const std::string & handler_name) const
{
    return storage->exists(getFileName(handler_name));
}

bool SQLDefinedHandlersMetadataStorage::store(const std::string & handler_name, const String & create_statement, bool replace, Int32 expected_root_version)
{
    return storage->write(getFileName(handler_name), create_statement, replace, expected_root_version);
}

bool SQLDefinedHandlersMetadataStorage::isReplicated() const
{
    return storage->isReplicated();
}

void SQLDefinedHandlersMetadataStorage::shutdown()
{
    storage.reset();
}

bool SQLDefinedHandlersMetadataStorage::waitUpdate()
{
    if (!storage->isReplicated())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Periodic updates are not supported for non-replicated storage");

    const auto & config = Context::getGlobalContextInstance()->getConfigRef();
    const size_t timeout = config.getUInt(query_rules_storage_config_path + ".update_timeout_ms", 5000);
    return storage->waitUpdate(timeout);
}

std::unique_ptr<SQLDefinedHandlersMetadataStorage> SQLDefinedHandlersMetadataStorage::create(const ContextPtr & context_)
{
    const auto & config = context_->getConfigRef();
    const auto storage_type = config.getString(query_rules_storage_config_path + ".type", "local");

    if (storage_type == "local")
    {
        const auto path = config.getString(
            query_rules_storage_config_path + ".path",
            fs::path(context_->getPath()) / "handlers");

        LOG_TRACE(getLogger("SQLDefinedHandlersMetadataStorage"), "Using local storage for handlers at path: {}", path);

        auto local_storage = std::make_shared<LocalStorage>(context_, path);
        return std::unique_ptr<SQLDefinedHandlersMetadataStorage>(
            new SQLDefinedHandlersMetadataStorage(std::move(local_storage), context_));
    }
    if (storage_type == "zookeeper" || storage_type == "keeper")
    {
        const auto path = config.getString(query_rules_storage_config_path + ".path");

        LOG_TRACE(getLogger("SQLDefinedHandlersMetadataStorage"), "Using zookeeper storage for handlers at path: {}", path);

        auto zk_storage = std::make_shared<ZooKeeperStorage>(context_, path);
        return std::unique_ptr<SQLDefinedHandlersMetadataStorage>(
            new SQLDefinedHandlersMetadataStorage(std::move(zk_storage), context_));
    }

    throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Unknown storage type for handlers: {}", storage_type);
}

}
