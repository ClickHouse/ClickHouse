#include <Common/SQLClusters/SQLClusterMetadataStorage.h>

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
#include <Poco/Util/AbstractConfiguration.h>
#include <Parsers/ParserSQLClusterQuery.h>
#include <Parsers/parseQuery.h>
#include <boost/algorithm/hex.hpp>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

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
    extern const int CLUSTER_ALREADY_EXISTS;
    extern const int CLUSTER_DOESNT_EXIST;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

static const std::string cluster_metadata_config_path = "cluster_metadata";

namespace
{

String getFileName(const String & cluster_name)
{
    return escapeForFileName(cluster_name) + ".sql";
}

}

class SQLClusterMetadataStorage::ISQLClusterStorage
{
public:
    virtual ~ISQLClusterStorage() = default;

    virtual bool exists(const std::string & path) const = 0;

    virtual std::vector<std::string> list() const = 0;

    virtual std::string read(const std::string & path) const = 0;

    virtual void write(const std::string & path, const std::string & data, bool replace) = 0;

    virtual void remove(const std::string & path) = 0;

    virtual bool removeIfExists(const std::string & path) = 0;

    virtual bool isReplicated() const = 0;

    virtual bool waitUpdate(size_t /* timeout */) { return false; }
};


class SQLClusterMetadataStorage::LocalStorage : public ISQLClusterStorage, protected WithContext
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
                elements.push_back(it->path());
            else
                LOG_WARNING(
                    getLogger("SQLClusterLocalStorage"),
                    "Unexpected file {} in SQL cluster metadata directory",
                    current_path.filename().string());
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
        if (!replace && exists(file_name))
        {
            throw Exception(
                ErrorCodes::CLUSTER_ALREADY_EXISTS,
                "Metadata file {} for SQL cluster already exists",
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
                ErrorCodes::CLUSTER_DOESNT_EXIST,
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


class SQLClusterMetadataStorage::ZooKeeperStorage : public ISQLClusterStorage, protected WithContext
{
private:
    std::string root_path;
    mutable zkutil::ZooKeeperPtr zookeeper_client{nullptr};
    mutable Coordination::EventPtr wait_event;
    mutable Int32 node_cversion = 0;

public:
    ZooKeeperStorage(ContextPtr context_, const std::string & path_)
        : WithContext(context_)
        , root_path(path_)
    {
        auto component_guard = Coordination::setCurrentComponent("SQLClusterMetadataStorage::ZooKeeperStorage");
        if (root_path.empty())
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "`cluster_metadata.path` cannot be empty");

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

    bool waitUpdate(size_t timeout) override
    {
        auto component_guard = Coordination::setCurrentComponent("SQLClusterMetadataStorage::waitUpdate");
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

        return stat.cversion != node_cversion;
    }

    std::vector<std::string> list() const override
    {
        auto component_guard = Coordination::setCurrentComponent("SQLClusterMetadataStorage::list");
        if (!wait_event)
            wait_event = std::make_shared<Poco::Event>();

        Coordination::Stat stat;
        auto children = getClient()->getChildren(root_path, &stat, wait_event);
        node_cversion = stat.cversion;
        return children;
    }

    bool exists(const std::string & file_name) const override
    {
        auto component_guard = Coordination::setCurrentComponent("SQLClusterMetadataStorage::exists");
        return getClient()->exists(getPath(file_name));
    }

    std::string read(const std::string & file_name) const override
    {
        auto component_guard = Coordination::setCurrentComponent("SQLClusterMetadataStorage::read");
        auto data = getClient()->get(getPath(file_name));
        return readHook(data);
    }

    virtual std::string readHook(const std::string & data) const
    {
        return data;
    }

    void write(const std::string & file_name, const std::string & data, bool replace) override
    {
        auto component_guard = Coordination::setCurrentComponent("SQLClusterMetadataStorage::write");
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
                    ErrorCodes::CLUSTER_ALREADY_EXISTS,
                    "Metadata file {} for SQL cluster already exists",
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
        auto component_guard = Coordination::setCurrentComponent("SQLClusterMetadataStorage::remove");
        getClient()->remove(getPath(file_name));
    }

    bool removeIfExists(const std::string & file_name) override
    {
        auto component_guard = Coordination::setCurrentComponent("SQLClusterMetadataStorage::removeIfExists");
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
class SQLClusterMetadataStorageEncrypted : public BaseMetadataStorage
{
public:
    SQLClusterMetadataStorageEncrypted(ContextPtr context_, const std::string & path_)
        : BaseMetadataStorage(context_, path_)
    {
        const auto & config = BaseMetadataStorage::getContext()->getConfigRef();
        auto key_hex = config.getRawString("cluster_metadata.key_hex", "");
        try
        {
            key = boost::algorithm::unhex(key_hex);
            key_fingerprint = FileEncryption::calculateKeyFingerprint(key);
        }
        catch (const std::exception &)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot read key_hex, check for valid characters [0-9a-fA-F] and length");
        }

        algorithm = FileEncryption::parseAlgorithmFromString(config.getString("cluster_metadata.algorithm", "aes_128_ctr"));
    }

    std::string readHook(const std::string & data) const override
    {
        ReadBufferFromString in(data);

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

        Memory<> encrypted_buffer(in.available());
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
    UInt128 key_fingerprint{};
    FileEncryption::Algorithm algorithm;
};

class SQLClusterMetadataStorage::LocalStorageEncrypted : public SQLClusterMetadataStorageEncrypted<SQLClusterMetadataStorage::LocalStorage>
{
    using SQLClusterMetadataStorageEncrypted<SQLClusterMetadataStorage::LocalStorage>::SQLClusterMetadataStorageEncrypted;
};

class SQLClusterMetadataStorage::ZooKeeperStorageEncrypted : public SQLClusterMetadataStorageEncrypted<SQLClusterMetadataStorage::ZooKeeperStorage>
{
    using SQLClusterMetadataStorageEncrypted<SQLClusterMetadataStorage::ZooKeeperStorage>::SQLClusterMetadataStorageEncrypted;
};

#endif

SQLClusterMetadataStorage::SQLClusterMetadataStorage(
    std::shared_ptr<ISQLClusterStorage> storage_,
    ContextPtr context_)
    : WithContext(context_)
    , storage(std::move(storage_))
{
}

std::vector<String> SQLClusterMetadataStorage::listClusterNames() const
{
    std::vector<String> result;
    for (const auto & path : storage->list())
    {
        if (path.ends_with(".sql"))
            result.push_back(unescapeForFileName(fs::path(path).stem().string()));
    }
    return result;
}

bool SQLClusterMetadataStorage::exists(const String & cluster_name) const
{
    return storage->exists(getFileName(cluster_name));
}

ASTCreateSQLClusterQuery SQLClusterMetadataStorage::readCreateQuery(const String & cluster_name) const
{
    const auto path = getFileName(cluster_name);
    const auto query = storage->read(path);
    const auto & settings = getContext()->getSettingsRef();

    ParserCreateSQLClusterQuery parser;
    auto ast = parseQuery(parser, query, "in file " + path, 0, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
    return ast->as<const ASTCreateSQLClusterQuery &>();
}

void SQLClusterMetadataStorage::writeCreateQuery(const String & cluster_name, const String & create_statement, bool replace)
{
    storage->write(getFileName(cluster_name), create_statement, replace);
}

void SQLClusterMetadataStorage::remove(const String & cluster_name)
{
    storage->remove(getFileName(cluster_name));
}

bool SQLClusterMetadataStorage::removeIfExists(const String & cluster_name)
{
    return storage->removeIfExists(getFileName(cluster_name));
}

bool SQLClusterMetadataStorage::isReplicated() const
{
    return storage->isReplicated();
}

bool SQLClusterMetadataStorage::waitUpdate()
{
    if (!storage->isReplicated())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Periodic updates are not supported");

    const auto & config = Context::getGlobalContextInstance()->getConfigRef();
    const size_t timeout = config.getUInt(cluster_metadata_config_path + ".update_timeout_ms", 5000);

    return storage->waitUpdate(timeout);
}

std::unique_ptr<SQLClusterMetadataStorage> SQLClusterMetadataStorage::create(const ContextPtr & context_)
{
    const auto & config = context_->getConfigRef();
    const auto storage_type = config.getString(cluster_metadata_config_path + ".type", "local");

    if (storage_type == "local" || storage_type == "local_encrypted")
    {
        const auto path = config.getString(
            cluster_metadata_config_path + ".path",
            std::filesystem::path(context_->getPath()) / "sql_clusters_metadata");

        LOG_TRACE(getLogger("SQLClusterMetadataStorage"),
                  "Using local storage for SQL clusters at path: {}", path);

        std::unique_ptr<ISQLClusterStorage> local_storage;
        if (storage_type == "local")
            local_storage = std::make_unique<SQLClusterMetadataStorage::LocalStorage>(context_, path);
        else if (storage_type == "local_encrypted")
        {
#if USE_SSL
            local_storage = std::make_unique<SQLClusterMetadataStorage::LocalStorageEncrypted>(context_, path);
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SQL cluster metadata encryption requires building with SSL support");
#endif
        }

        return std::unique_ptr<SQLClusterMetadataStorage>(
            new SQLClusterMetadataStorage(std::move(local_storage), context_));
    }
    if (storage_type == "zookeeper" || storage_type == "keeper" || storage_type == "zookeeper_encrypted" || storage_type == "keeper_encrypted")
    {
        const auto path = config.getString(cluster_metadata_config_path + ".path");
        if (path.empty())
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "`cluster_metadata.path` must be non-empty for keeper storage");

        std::unique_ptr<ISQLClusterStorage> zk_storage;
        if (!storage_type.ends_with("_encrypted"))
            zk_storage = std::make_unique<SQLClusterMetadataStorage::ZooKeeperStorage>(context_, path);
        else
        {
#if USE_SSL
            zk_storage = std::make_unique<SQLClusterMetadataStorage::ZooKeeperStorageEncrypted>(context_, path);
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SQL cluster metadata encryption requires building with SSL support");
#endif
        }

        LOG_TRACE(getLogger("SQLClusterMetadataStorage"),
                  "Using keeper storage for SQL clusters at path: {}", path);

        return std::unique_ptr<SQLClusterMetadataStorage>(
            new SQLClusterMetadataStorage(std::move(zk_storage), context_));
    }

    throw Exception(
        ErrorCodes::INVALID_CONFIG_PARAMETER,
        "Unknown storage for SQL cluster metadata: {}", storage_type);
}

}
