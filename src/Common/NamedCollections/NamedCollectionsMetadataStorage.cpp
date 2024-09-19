#include <filesystem>
#include <Core/Settings.h>
#include <IO/FileEncryptionCommon.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <boost/algorithm/hex.hpp>
#include <Common/NamedCollections/NamedCollectionConfiguration.h>
#include <Common/NamedCollections/NamedCollectionsMetadataStorage.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>

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
    extern const int NAMED_COLLECTION_ALREADY_EXISTS;
    extern const int NAMED_COLLECTION_DOESNT_EXIST;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
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

    virtual bool isReplicated() const = 0;

    virtual bool waitUpdate(size_t /* timeout */) { return false; }
};


class NamedCollectionsMetadataStorage::LocalStorage : public INamedCollectionsStorage, protected WithContext
{
protected:
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

    bool exists(const std::string & file_name) const override
    {
        return fs::exists(getPath(file_name));
    }

    std::string read(const std::string & file_name) const override
    {
        ReadBufferFromFile in(getPath(file_name));
        std::string data;
        readStringUntilEOF(data, in);
        return readHook(data);
    }

    virtual std::string readHook(const std::string & data) const
    {
        return data;
    }

    void write(const std::string & file_name, const std::string & data, bool replace) override
    {
        if (!replace && fs::exists(file_name))
        {
            throw Exception(
                ErrorCodes::NAMED_COLLECTION_ALREADY_EXISTS,
                "Metadata file {} for named collection already exists",
                file_name);
        }

        fs::create_directories(root_path);

        auto tmp_path = getPath(file_name + ".tmp");
        auto write_data = writeHook(data);
        WriteBufferFromFile out(tmp_path, write_data.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(write_data, out);

        out.next();
        if (getContext()->getSettingsRef()[Setting::fsync_metadata])
            out.sync();
        out.close();

        fs::rename(tmp_path, getPath(file_name));
    }

    virtual std::string writeHook(const std::string & data) const
    {
        return data;
    }

    void remove(const std::string & file_name) override
    {
        if (!removeIfExists(file_name))
        {
            throw Exception(
                ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
                "Cannot remove `{}`, because it doesn't exist", file_name);
        }
    }

    bool removeIfExists(const std::string & file_name) override
    {
        return fs::remove(getPath(file_name));
    }

protected:
    std::string getPath(const std::string & file_name) const
    {
        const auto file_name_as_path = fs::path(file_name);
        if (file_name_as_path.is_absolute())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Filename {} cannot be an absolute path", file_name);

        return fs::path(root_path) / file_name_as_path;
    }

private:
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

class NamedCollectionsMetadataStorage::ZooKeeperStorage : public INamedCollectionsStorage, protected WithContext
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

    bool isReplicated() const override { return true; }

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

    bool exists(const std::string & file_name) const override
    {
        return getClient()->exists(getPath(file_name));
    }

    std::string read(const std::string & file_name) const override
    {
        auto data = getClient()->get(getPath(file_name));
        return readHook(data);
    }

    virtual std::string readHook(const std::string & data) const
    {
        return data;
    }

    void write(const std::string & file_name, const std::string & data, bool replace) override
    {
        auto write_data = writeHook(data);
        if (replace)
        {
            getClient()->createOrUpdate(getPath(file_name), write_data, zkutil::CreateMode::Persistent);
        }
        else
        {
            auto code = getClient()->tryCreate(getPath(file_name), write_data, zkutil::CreateMode::Persistent);

            if (code == Coordination::Error::ZNODEEXISTS)
            {
                throw Exception(
                    ErrorCodes::NAMED_COLLECTION_ALREADY_EXISTS,
                    "Metadata file {} for named collection already exists",
                    file_name);
            }
        }
    }

    virtual std::string writeHook(const std::string & data) const
    {
        return data;
    }

    void remove(const std::string & file_name) override
    {
        getClient()->remove(getPath(file_name));
    }

    bool removeIfExists(const std::string & file_name) override
    {
        auto code = getClient()->tryRemove(getPath(file_name));
        if (code == Coordination::Error::ZOK)
            return true;
        if (code == Coordination::Error::ZNONODE)
            return false;
        throw Coordination::Exception::fromPath(code, getPath(file_name));
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

    std::string getPath(const std::string & file_name) const
    {
        const auto file_name_as_path = fs::path(file_name);
        if (file_name_as_path.is_absolute())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Filename {} cannot be an absolute path", file_name);

        return fs::path(root_path) / file_name_as_path;
    }
};

#if USE_SSL

template <typename BaseMetadataStorage>
class NamedCollectionsMetadataStorageEncrypted : public BaseMetadataStorage
{
public:
    NamedCollectionsMetadataStorageEncrypted(ContextPtr context_, const std::string & path_)
        : BaseMetadataStorage(context_, path_)
    {
        const auto & config = BaseMetadataStorage::getContext()->getConfigRef();
        auto key_hex = config.getRawString("named_collections_storage.key_hex", "");
        try
        {
            key = boost::algorithm::unhex(key_hex);
            key_fingerprint = FileEncryption::calculateKeyFingerprint(key);
        }
        catch (const std::exception &)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot read key_hex, check for valid characters [0-9a-fA-F] and length");
        }

        algorithm = FileEncryption::parseAlgorithmFromString(config.getString("named_collections_storage.algorithm", "aes_128_ctr"));
    }

    std::string readHook(const std::string & data) const override
    {
        ReadBufferFromString in(data);
        Memory<> encrypted_buffer(data.length());

        FileEncryption::Header header;
        try
        {
            header.read(in);
        }
        catch (Exception & e)
        {
            e.addMessage("While reading the header of encrypted data");
            throw;
        }

        size_t bytes_read = 0;
        while (bytes_read < encrypted_buffer.size() && !in.eof())
        {
            bytes_read += in.read(encrypted_buffer.data() + bytes_read, encrypted_buffer.size() - bytes_read);
        }

        std::string decrypted_buffer;
        decrypted_buffer.resize(bytes_read);
        FileEncryption::Encryptor encryptor(header.algorithm, key, header.init_vector);
        encryptor.decrypt(encrypted_buffer.data(), bytes_read, decrypted_buffer.data());

        return decrypted_buffer;
    }

    std::string writeHook(const std::string & data) const override
    {
        FileEncryption::Header header{
            .algorithm = algorithm,
            .key_fingerprint = key_fingerprint,
            .init_vector = FileEncryption::InitVector::random()
        };

        FileEncryption::Encryptor encryptor(header.algorithm, key, header.init_vector);
        WriteBufferFromOwnString out;
        header.write(out);
        encryptor.encrypt(data.data(), data.size(), out);
        return std::string(out.str());
    }

private:
    std::string key;
    UInt128 key_fingerprint;
    FileEncryption::Algorithm algorithm;
};

class NamedCollectionsMetadataStorage::LocalStorageEncrypted : public NamedCollectionsMetadataStorageEncrypted<NamedCollectionsMetadataStorage::LocalStorage>
{
    using NamedCollectionsMetadataStorageEncrypted<NamedCollectionsMetadataStorage::LocalStorage>::NamedCollectionsMetadataStorageEncrypted;
};

class NamedCollectionsMetadataStorage::ZooKeeperStorageEncrypted : public NamedCollectionsMetadataStorageEncrypted<NamedCollectionsMetadataStorage::ZooKeeperStorage>
{
    using NamedCollectionsMetadataStorageEncrypted<NamedCollectionsMetadataStorage::ZooKeeperStorage>::NamedCollectionsMetadataStorageEncrypted;
};

#endif

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
    auto ast = parseQuery(parser, query, "in file " + path, 0, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
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

bool NamedCollectionsMetadataStorage::isReplicated() const
{
    return storage->isReplicated();
}

bool NamedCollectionsMetadataStorage::waitUpdate()
{
    if (!storage->isReplicated())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Periodic updates are not supported");

    const auto & config = Context::getGlobalContextInstance()->getConfigRef();
    const size_t timeout = config.getUInt(named_collections_storage_config_path + ".update_timeout_ms", 5000);

    return storage->waitUpdate(timeout);
}

std::unique_ptr<NamedCollectionsMetadataStorage> NamedCollectionsMetadataStorage::create(const ContextPtr & context_)
{
    const auto & config = context_->getConfigRef();
    const auto storage_type = config.getString(named_collections_storage_config_path + ".type", "local");

    if (storage_type == "local" || storage_type == "local_encrypted")
    {
        const auto path = config.getString(
            named_collections_storage_config_path + ".path",
            std::filesystem::path(context_->getPath()) / "named_collections");

        LOG_TRACE(getLogger("NamedCollectionsMetadataStorage"),
                  "Using local storage for named collections at path: {}", path);

        std::unique_ptr<INamedCollectionsStorage> local_storage;
        if (storage_type == "local")
            local_storage = std::make_unique<NamedCollectionsMetadataStorage::LocalStorage>(context_, path);
        else if (storage_type == "local_encrypted")
        {
#if USE_SSL
            local_storage = std::make_unique<NamedCollectionsMetadataStorage::LocalStorageEncrypted>(context_, path);
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Named collections encryption requires building with SSL support");
#endif
        }

        return std::unique_ptr<NamedCollectionsMetadataStorage>(
            new NamedCollectionsMetadataStorage(std::move(local_storage), context_));
    }
    if (storage_type == "zookeeper" || storage_type == "keeper" || storage_type == "zookeeper_encrypted" || storage_type == "keeper_encrypted")
    {
        const auto path = config.getString(named_collections_storage_config_path + ".path");

        std::unique_ptr<INamedCollectionsStorage> zk_storage;
        if (!storage_type.ends_with("_encrypted"))
            zk_storage = std::make_unique<NamedCollectionsMetadataStorage::ZooKeeperStorage>(context_, path);
        else
        {
#if USE_SSL
            zk_storage = std::make_unique<NamedCollectionsMetadataStorage::ZooKeeperStorageEncrypted>(context_, path);
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Named collections encryption requires building with SSL support");
#endif
        }

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
