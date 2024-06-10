#include <Common/NamedCollections/NamedCollectionsMetadataStorage.h>
#include <Common/NamedCollections/NamedCollectionConfiguration.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{
namespace ErrorCodes
{
    extern const int NAMED_COLLECTION_ALREADY_EXISTS;
    extern const int NAMED_COLLECTION_DOESNT_EXIST;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

static const std::string named_collections_storage_config_path = "named_collections_storage";

namespace
{
    MutableNamedCollectionPtr createNamedCollectionFromAST(const ASTCreateNamedCollectionQuery & query)
    {
        const auto & collection_name = query.collection_name;
        const auto config = NamedCollectionConfiguration::createConfiguration(collection_name, query.changes, query.overridability);

        std::set<std::string, std::less<>> keys;
        for (const auto & [name, _] : query.changes)
            keys.insert(name);

        return NamedCollection::create(
            *config, collection_name, "", keys, NamedCollection::SourceId::SQL, /* is_mutable */true);
    }

    std::string getFileName(const std::string & collection_name)
    {
        return escapeForFileName(collection_name) + ".sql";
    }
}

class NamedCollectionsMetadataStorage::INamedCollectionsStorage
{
public:
    virtual ~INamedCollectionsStorage() = default;

    virtual bool exists(const std::string & path) const = 0;

    virtual std::vector<std::string> list() const = 0;

    virtual std::string read(const std::string & path) const = 0;

    virtual void write(const std::string & path, const std::string & data, bool replace) = 0;

    virtual void remove(const std::string & path) = 0;

    virtual bool removeIfExists(const std::string & path) = 0;

    virtual bool supportsPeriodicUpdate() const = 0;

    virtual bool waitUpdate(size_t /* timeout */) { return false; }
};


class NamedCollectionsMetadataStorage::LocalStorage : public INamedCollectionsStorage, private WithContext
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

    ~LocalStorage() override = default;

    bool supportsPeriodicUpdate() const override { return false; }

    std::vector<std::string> list() const override
    {
        if (!fs::exists(root_path))
            return {};

        std::vector<std::string> elements;
        for (fs::directory_iterator it{root_path}; it != fs::directory_iterator{}; ++it)
        {
            const auto & current_path = it->path();
            if (current_path.extension() == ".sql")
            {
                elements.push_back(it->path());
            }
            else
            {
                LOG_WARNING(
                    getLogger("LocalStorage"),
                    "Unexpected file {} in named collections directory",
                    current_path.filename().string());
            }
        }
        return elements;
    }

    bool exists(const std::string & path) const override
    {
        return fs::exists(getPath(path));
    }

    std::string read(const std::string & path) const override
    {
        ReadBufferFromFile in(getPath(path));
        std::string data;
        readStringUntilEOF(data, in);
        return data;
    }

    void write(const std::string & path, const std::string & data, bool replace) override
    {
        if (!replace && fs::exists(path))
        {
            throw Exception(
                ErrorCodes::NAMED_COLLECTION_ALREADY_EXISTS,
                "Metadata file {} for named collection already exists",
                path);
        }

        fs::create_directories(root_path);

        auto tmp_path = getPath(path + ".tmp");
        WriteBufferFromFile out(tmp_path, data.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(data, out);

        out.next();
        if (getContext()->getSettingsRef().fsync_metadata)
            out.sync();
        out.close();

        fs::rename(tmp_path, getPath(path));
    }

    void remove(const std::string & path) override
    {
        if (!removeIfExists(getPath(path)))
        {
            throw Exception(
                ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
                "Cannot remove `{}`, because it doesn't exist", path);
        }
    }

    bool removeIfExists(const std::string & path) override
    {
        return fs::remove(getPath(path));
    }

private:
    std::string getPath(const std::string & path) const
    {
        return fs::path(root_path) / path;
    }

    /// Delete .tmp files. They could be left undeleted in case of
    /// some exception or abrupt server restart.
    void cleanup()
    {
        std::vector<std::string> files_to_remove;
        for (fs::directory_iterator it{root_path}; it != fs::directory_iterator{}; ++it)
        {
            const auto & current_path = it->path();
            if (current_path.extension() == ".tmp")
                files_to_remove.push_back(current_path);
        }
        for (const auto & file : files_to_remove)
            fs::remove(file);
    }
};


class NamedCollectionsMetadataStorage::ZooKeeperStorage : public INamedCollectionsStorage, private WithContext
{
private:
    std::string root_path;
    mutable zkutil::ZooKeeperPtr zookeeper_client{nullptr};
    mutable zkutil::EventPtr wait_event;
    mutable Int32 collections_node_cversion = 0;

public:
    ZooKeeperStorage(ContextPtr context_, const std::string & path_)
        : WithContext(context_)
        , root_path(path_)
    {
        if (root_path.empty())
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Collections path cannot be empty");

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

    ~ZooKeeperStorage() override = default;

    bool supportsPeriodicUpdate() const override { return true; }

    /// Return true if children changed.
    bool waitUpdate(size_t timeout) override
    {
        if (!wait_event)
        {
            /// We did not yet made any list() attempt, so do that.
            return true;
        }

        if (wait_event->tryWait(timeout))
        {
            /// Children changed before timeout.
            return true;
        }

        std::string res;
        Coordination::Stat stat;

        if (!getClient()->tryGet(root_path, res, &stat))
        {
            /// We do create root_path in constructor of this class,
            /// so this case is not really possible.
            chassert(false);
            return false;
        }

        return stat.cversion != collections_node_cversion;
    }

    std::vector<std::string> list() const override
    {
        if (!wait_event)
            wait_event = std::make_shared<Poco::Event>();

        Coordination::Stat stat;
        auto children = getClient()->getChildren(root_path, &stat, wait_event);
        collections_node_cversion = stat.cversion;
        return children;
    }

    bool exists(const std::string & path) const override
    {
        return getClient()->exists(getPath(path));
    }

    std::string read(const std::string & path) const override
    {
        return getClient()->get(getPath(path));
    }

    void write(const std::string & path, const std::string & data, bool replace) override
    {
        if (replace)
        {
            getClient()->createOrUpdate(getPath(path), data, zkutil::CreateMode::Persistent);
        }
        else
        {
            auto code = getClient()->tryCreate(getPath(path), data, zkutil::CreateMode::Persistent);

            if (code == Coordination::Error::ZNODEEXISTS)
            {
                throw Exception(
                    ErrorCodes::NAMED_COLLECTION_ALREADY_EXISTS,
                    "Metadata file {} for named collection already exists",
                    path);
            }
        }
    }

    void remove(const std::string & path) override
    {
        getClient()->remove(getPath(path));
    }

    bool removeIfExists(const std::string & path) override
    {
        auto code = getClient()->tryRemove(getPath(path));
        if (code == Coordination::Error::ZOK)
            return true;
        if (code == Coordination::Error::ZNONODE)
            return false;
        throw Coordination::Exception::fromPath(code, getPath(path));
    }

private:
    zkutil::ZooKeeperPtr getClient() const
    {
        if (!zookeeper_client || zookeeper_client->expired())
        {
            zookeeper_client = getContext()->getZooKeeper();
            zookeeper_client->sync(root_path);
        }
        return zookeeper_client;
    }

    std::string getPath(const std::string & path) const
    {
        return fs::path(root_path) / path;
    }
};

NamedCollectionsMetadataStorage::NamedCollectionsMetadataStorage(
    std::shared_ptr<INamedCollectionsStorage> storage_,
    ContextPtr context_)
    : WithContext(context_)
    , storage(std::move(storage_))
{
}

MutableNamedCollectionPtr NamedCollectionsMetadataStorage::get(const std::string & collection_name) const
{
    const auto query = readCreateQuery(collection_name);
    return createNamedCollectionFromAST(query);
}

NamedCollectionsMap NamedCollectionsMetadataStorage::getAll() const
{
    NamedCollectionsMap result;
    for (const auto & collection_name : listCollections())
    {
        if (result.contains(collection_name))
        {
            throw Exception(
                ErrorCodes::NAMED_COLLECTION_ALREADY_EXISTS,
                "Found duplicate named collection `{}`",
                collection_name);
        }
        result.emplace(collection_name, get(collection_name));
    }
    return result;
}

MutableNamedCollectionPtr NamedCollectionsMetadataStorage::create(const ASTCreateNamedCollectionQuery & query)
{
    writeCreateQuery(query);
    return createNamedCollectionFromAST(query);
}

void NamedCollectionsMetadataStorage::remove(const std::string & collection_name)
{
    storage->remove(getFileName(collection_name));
}

bool NamedCollectionsMetadataStorage::removeIfExists(const std::string & collection_name)
{
    return storage->removeIfExists(getFileName(collection_name));
}

void NamedCollectionsMetadataStorage::update(const ASTAlterNamedCollectionQuery & query)
{
    auto create_query = readCreateQuery(query.collection_name);

    std::unordered_map<std::string, Field> result_changes_map;
    for (const auto & [name, value] : query.changes)
    {
        auto [it, inserted] = result_changes_map.emplace(name, value);
        if (!inserted)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Value with key `{}` is used twice in the SET query (collection name: {})",
                name, query.collection_name);
        }
    }

    for (const auto & [name, value] : create_query.changes)
        result_changes_map.emplace(name, value);

    std::unordered_map<std::string, bool> result_overridability_map;
    for (const auto & [name, value] : query.overridability)
        result_overridability_map.emplace(name, value);
    for (const auto & [name, value] : create_query.overridability)
        result_overridability_map.emplace(name, value);

    for (const auto & delete_key : query.delete_keys)
    {
        auto it = result_changes_map.find(delete_key);
        if (it == result_changes_map.end())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot delete key `{}` because it does not exist in collection",
                delete_key);
        }
        else
        {
            result_changes_map.erase(it);
            auto it_override = result_overridability_map.find(delete_key);
            if (it_override != result_overridability_map.end())
                result_overridability_map.erase(it_override);
        }
    }

    create_query.changes.clear();
    for (const auto & [name, value] : result_changes_map)
        create_query.changes.emplace_back(name, value);
    create_query.overridability = std::move(result_overridability_map);

    if (create_query.changes.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Named collection cannot be empty (collection name: {})",
            query.collection_name);

    chassert(create_query.collection_name == query.collection_name);
    writeCreateQuery(create_query, true);
}

std::vector<std::string> NamedCollectionsMetadataStorage::listCollections() const
{
    auto paths = storage->list();
    std::vector<std::string> collections;
    collections.reserve(paths.size());
    for (const auto & path : paths)
        collections.push_back(std::filesystem::path(path).stem());
    return collections;
}

ASTCreateNamedCollectionQuery NamedCollectionsMetadataStorage::readCreateQuery(const std::string & collection_name) const
{
    const auto path = getFileName(collection_name);
    auto query = storage->read(path);
    const auto & settings = getContext()->getSettingsRef();

    ParserCreateNamedCollectionQuery parser;
    auto ast = parseQuery(parser, query, "in file " + path, 0, settings.max_parser_depth, settings.max_parser_backtracks);
    const auto & create_query = ast->as<const ASTCreateNamedCollectionQuery &>();
    return create_query;
}

void NamedCollectionsMetadataStorage::writeCreateQuery(const ASTCreateNamedCollectionQuery & query, bool replace)
{
    auto normalized_query = query.clone();
    auto & changes = typeid_cast<ASTCreateNamedCollectionQuery *>(normalized_query.get())->changes;
    ::sort(
        changes.begin(), changes.end(),
        [](const SettingChange & lhs, const SettingChange & rhs) { return lhs.name < rhs.name; });

    storage->write(getFileName(query.collection_name), serializeAST(*normalized_query), replace);
}

bool NamedCollectionsMetadataStorage::supportsPeriodicUpdate() const
{
    return storage->supportsPeriodicUpdate();
}

bool NamedCollectionsMetadataStorage::waitUpdate()
{
    if (!storage->supportsPeriodicUpdate())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Periodic updates are not supported");

    const auto & config = Context::getGlobalContextInstance()->getConfigRef();
    const size_t timeout = config.getUInt(named_collections_storage_config_path + ".update_timeout_ms", 5000);

    return storage->waitUpdate(timeout);
}

std::unique_ptr<NamedCollectionsMetadataStorage> NamedCollectionsMetadataStorage::create(const ContextPtr & context_)
{
    const auto & config = context_->getConfigRef();
    const auto storage_type = config.getString(named_collections_storage_config_path + ".type", "local");

    if (storage_type == "local")
    {
        const auto path = config.getString(
            named_collections_storage_config_path + ".path",
            std::filesystem::path(context_->getPath()) / "named_collections");

        LOG_TRACE(getLogger("NamedCollectionsMetadataStorage"),
                  "Using local storage for named collections at path: {}", path);

        auto local_storage = std::make_unique<NamedCollectionsMetadataStorage::LocalStorage>(context_, path);
        return std::unique_ptr<NamedCollectionsMetadataStorage>(
            new NamedCollectionsMetadataStorage(std::move(local_storage), context_));
    }
    if (storage_type == "zookeeper" || storage_type == "keeper")
    {
        const auto path = config.getString(named_collections_storage_config_path + ".path");
        auto zk_storage = std::make_unique<NamedCollectionsMetadataStorage::ZooKeeperStorage>(context_, path);

        LOG_TRACE(getLogger("NamedCollectionsMetadataStorage"),
                  "Using zookeeper storage for named collections at path: {}", path);

        return std::unique_ptr<NamedCollectionsMetadataStorage>(
            new NamedCollectionsMetadataStorage(std::move(zk_storage), context_));
    }

    throw Exception(
        ErrorCodes::INVALID_CONFIG_PARAMETER,
        "Unknown storage for named collections: {}", storage_type);
}

}
