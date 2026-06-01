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
    virtual std::string read(const std::string & path) const = 0;
    virtual void write(const std::string & path, const std::string & data, bool replace) = 0;
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

    void write(const std::string & file_name, const std::string & data, bool replace) override
    {
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
    /// Version of the root node's data. It is bumped on every create/drop/alter (see `bumpVersion`),
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
        /// root version (see `bumpVersion`), so this watch fires for create, drop and alter alike.
        Coordination::Stat stat;
        getClient()->get(root_path, &stat, wait_event);
        root_version = stat.version;

        return getClient()->getChildren(root_path);
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

    void write(const std::string & file_name, const std::string & data, bool replace) override
    {
        auto component_guard = Coordination::setCurrentComponent("SQLDefinedHandlersMetadataStorage::write");
        if (replace)
        {
            /// ALTER must update an existing handler only; using `set` (not create-or-update) prevents a
            /// delayed ALTER from resurrecting a handler that was concurrently dropped on another replica.
            auto code = getClient()->trySet(getPath(file_name), data);
            if (code == Coordination::Error::ZNONODE)
                throw Exception(ErrorCodes::HANDLER_DOESNT_EXIST, "Handler `{}` doesn't exist", file_name);
            if (code != Coordination::Error::ZOK)
                throw Coordination::Exception::fromPath(code, getPath(file_name));
        }
        else
        {
            auto code = getClient()->tryCreate(getPath(file_name), data, zkutil::CreateMode::Persistent);
            if (code == Coordination::Error::ZNODEEXISTS)
                throw Exception(ErrorCodes::HANDLER_ALREADY_EXISTS, "Metadata file for handler already exists: {}", file_name);
            if (code != Coordination::Error::ZOK)
                throw Coordination::Exception::fromPath(code, getPath(file_name));
        }
        bumpVersion();
    }

    void remove(const std::string & file_name) override
    {
        auto component_guard = Coordination::setCurrentComponent("SQLDefinedHandlersMetadataStorage::remove");
        getClient()->remove(getPath(file_name));
        bumpVersion();
    }

    bool removeIfExists(const std::string & file_name) override
    {
        auto component_guard = Coordination::setCurrentComponent("SQLDefinedHandlersMetadataStorage::removeIfExists");
        auto code = getClient()->tryRemove(getPath(file_name));
        if (code == Coordination::Error::ZOK)
        {
            bumpVersion();
            return true;
        }
        if (code == Coordination::Error::ZNONODE)
            return false;
        throw Coordination::Exception::fromPath(code, getPath(file_name));
    }

private:
    /// Bump the root node's data version to notify all replicas (including for ALTER, which only
    /// changes child data and would otherwise be invisible to a children-list watch).
    void bumpVersion()
    {
        getClient()->set(root_path, "");
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

std::vector<std::string> SQLDefinedHandlersMetadataStorage::listHandlers() const
{
    auto paths = storage->list();
    std::vector<std::string> handlers;
    handlers.reserve(paths.size());
    for (const auto & path : paths)
        handlers.push_back(unescapeForFileName(fs::path(path).stem()));
    return handlers;
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
    SQLDefinedHandlers result;
    for (const auto & handler_name : listHandlers())
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
    const auto path = getFileName(alter_query.handler_name);
    auto statement = storage->read(path);
    const auto & settings = getContext()->getSettingsRef();

    ParserCreateHandlerQuery parser(statement.data() + statement.size());
    auto ast = parseQuery(parser, statement, "in handler " + path, 0, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
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

void SQLDefinedHandlersMetadataStorage::store(const std::string & handler_name, const String & create_statement, bool replace)
{
    storage->write(getFileName(handler_name), create_statement, replace);
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
