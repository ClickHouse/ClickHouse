#include <Common/Clusters/ClusterCatalogMetadataBackend.h>

#include <filesystem>
#include <Core/Settings.h>
#include <Core/Types.h>
#include <IO/FileEncryptionCommon.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <boost/algorithm/hex.hpp>
#include <Common/Exception.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/logger_useful.h>

namespace fs = std::filesystem;

namespace DB
{

namespace Setting
{
    extern const SettingsBool fsync_metadata;
}

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int BAD_CLUSTER_DEFINITION;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

/// Backend storage-type tag values accepted in the `<type>` key of this component's config section. Kept
/// together with the `_encrypted` suffix detector so that adding a new backend touches one place.
constexpr std::string_view STORAGE_TYPE_LOCAL = "local";
constexpr std::string_view STORAGE_TYPE_LOCAL_ENCRYPTED = "local_encrypted";
constexpr std::string_view STORAGE_TYPE_ZOOKEEPER = "zookeeper";
constexpr std::string_view STORAGE_TYPE_KEEPER = "keeper";
constexpr std::string_view STORAGE_TYPE_ZOOKEEPER_ENCRYPTED = "zookeeper_encrypted";
constexpr std::string_view STORAGE_TYPE_KEEPER_ENCRYPTED = "keeper_encrypted";
constexpr std::string_view STORAGE_TYPE_ENCRYPTED_SUFFIX = "_encrypted";

LoggerPtr getLog()
{
    return getLogger("ClusterCatalogMetadataBackend");
}

class ClusterCatalogLocalBackend : public IClusterCatalogMetadataBackend, protected WithContext
{
protected:
    std::string root_path;

public:
    ClusterCatalogLocalBackend(ContextPtr context_, const std::string & path_)
        : WithContext(context_)
        , root_path(path_)
    {
        if (fs::exists(root_path))
            cleanup();
    }

    ~ClusterCatalogLocalBackend() override = default;

    bool isReplicated() const override { return false; }

    bool waitUpdate(size_t /* timeout_ms */) override { return false; }

    std::vector<std::string> list() const override
    {
        if (!fs::exists(root_path))
            return {};

        std::vector<std::string> elements;
        for (fs::directory_iterator it{root_path}; it != fs::directory_iterator{}; ++it)
        {
            const auto & current_path = it->path();
            if (current_path.extension() == ".sql")
                elements.push_back(it->path().string());
            else
                LOG_WARNING(
                    getLog(),
                    "Unexpected file {} in cluster catalog directory {}",
                    current_path.filename().string(),
                    root_path);
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
        const std::string final_path = getPath(file_name);
        if (!replace && fs::exists(final_path))
        {
            throw Exception(
                ErrorCodes::BAD_CLUSTER_DEFINITION,
                "Metadata file {} for cluster catalog already exists",
                final_path);
        }

        fs::create_directories(root_path);

        const std::string tmp_path = final_path + ".tmp";
        auto write_data = writeHook(data);
        WriteBufferFromFile out(tmp_path, write_data.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(write_data, out);

        out.next();
        if (getContext()->getSettingsRef()[Setting::fsync_metadata])
            out.sync();
        out.close();

        fs::rename(tmp_path, final_path);
    }

    virtual std::string writeHook(const std::string & data) const
    {
        return data;
    }

    void remove(const std::string & file_name) override
    {
        if (!removeIfExists(file_name))
            throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Cannot remove `{}`, because it doesn't exist", file_name);
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

class ClusterCatalogKeeperBackend : public IClusterCatalogMetadataBackend, protected WithContext
{
private:
    std::string root_path;
    mutable zkutil::ZooKeeperPtr zookeeper_client{nullptr};
    mutable Coordination::EventPtr wait_event;
    mutable Int32 collections_node_cversion = 0;

public:
    ClusterCatalogKeeperBackend(ContextPtr context_, const std::string & path_)
        : WithContext(context_)
        , root_path(path_)
    {
        auto component_guard = Coordination::setCurrentComponent("ClusterCatalogKeeperBackend");
        if (root_path.empty())
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Cluster catalog Keeper path cannot be empty");

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

    ~ClusterCatalogKeeperBackend() override = default;

    bool isReplicated() const override { return true; }

    bool waitUpdate(size_t timeout_ms) override
    {
        auto component_guard = Coordination::setCurrentComponent("ClusterCatalogKeeperBackend::waitUpdate");
        if (!wait_event)
            return true;

        if (wait_event->tryWait(timeout_ms))
            return true;

        std::string res;
        Coordination::Stat stat;

        if (!getClient()->tryGet(root_path, res, &stat))
        {
            chassert(false);
            return false;
        }

        return stat.cversion != collections_node_cversion;
    }

    std::vector<std::string> list() const override
    {
        auto component_guard = Coordination::setCurrentComponent("ClusterCatalogKeeperBackend::list");
        if (!wait_event)
            wait_event = std::make_shared<Poco::Event>();

        Coordination::Stat stat;
        auto children = getClient()->getChildren(root_path, &stat, wait_event);
        collections_node_cversion = stat.cversion;
        return children;
    }

    bool exists(const std::string & file_name) const override
    {
        auto component_guard = Coordination::setCurrentComponent("ClusterCatalogKeeperBackend::exists");
        return getClient()->exists(getPath(file_name));
    }

    std::string read(const std::string & file_name) const override
    {
        auto component_guard = Coordination::setCurrentComponent("ClusterCatalogKeeperBackend::read");
        auto data = getClient()->get(getPath(file_name));
        return readHook(data);
    }

    virtual std::string readHook(const std::string & data) const
    {
        return data;
    }

    void write(const std::string & file_name, const std::string & data, bool replace) override
    {
        auto component_guard = Coordination::setCurrentComponent("ClusterCatalogKeeperBackend::write");
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
                    ErrorCodes::BAD_CLUSTER_DEFINITION,
                    "Metadata node {} for cluster catalog already exists",
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
        auto component_guard = Coordination::setCurrentComponent("ClusterCatalogKeeperBackend::remove");
        getClient()->remove(getPath(file_name));
    }

    bool removeIfExists(const std::string & file_name) override
    {
        auto component_guard = Coordination::setCurrentComponent("ClusterCatalogKeeperBackend::removeIfExists");
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

template <typename BaseBackend>
class ClusterCatalogEncryptedBackend : public BaseBackend
{
public:
    ClusterCatalogEncryptedBackend(ContextPtr context_, const std::string & path_, const std::string & config_prefix_)
        : BaseBackend(context_, path_)
        , config_prefix(config_prefix_)
    {
        const auto & config = BaseBackend::getContext()->getConfigRef();
        auto key_hex = config.getRawString(config_prefix + ".key_hex", "");
        try
        {
            key = boost::algorithm::unhex(key_hex);
            key_fingerprint = FileEncryption::calculateKeyFingerprint(key);
        }
        catch (const std::exception &)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot read key_hex, check for valid characters [0-9a-fA-F] and length");
        }

        algorithm = FileEncryption::parseAlgorithmFromString(config.getString(config_prefix + ".algorithm", "aes_128_ctr"));
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
            e.addMessage("While reading the header of encrypted cluster catalog metadata");
            throw;
        }

        Memory<> encrypted_buffer(in.available());
        size_t bytes_read = 0;
        while (bytes_read < encrypted_buffer.size() && !in.eof())
            bytes_read += in.read(encrypted_buffer.data() + bytes_read, encrypted_buffer.size() - bytes_read);

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
    std::string config_prefix;
    std::string key;
    UInt128 key_fingerprint = 0;
    FileEncryption::Algorithm algorithm{};
};

using ClusterCatalogLocalBackendEncrypted = ClusterCatalogEncryptedBackend<ClusterCatalogLocalBackend>;
using ClusterCatalogKeeperBackendEncrypted = ClusterCatalogEncryptedBackend<ClusterCatalogKeeperBackend>;

#endif

std::shared_ptr<IClusterCatalogMetadataBackend> createClusterCatalogMetadataBackendImpl(
    const ContextPtr & context_, const std::string & config_prefix, const std::string & default_local_path)
{
    const auto & config = context_->getConfigRef();
    const auto storage_type = config.getString(config_prefix + ".type", std::string{STORAGE_TYPE_LOCAL});

    if (storage_type == STORAGE_TYPE_LOCAL || storage_type == STORAGE_TYPE_LOCAL_ENCRYPTED)
    {
        const auto path = config.getString(config_prefix + ".path", default_local_path);

        LOG_TRACE(getLog(), "Using {} cluster catalog backend ({}) at path: {}", storage_type, config_prefix, path);

        std::shared_ptr<IClusterCatalogMetadataBackend> local_backend;
        if (storage_type == STORAGE_TYPE_LOCAL)
            local_backend = std::make_shared<ClusterCatalogLocalBackend>(context_, path);
        else
        {
#if USE_SSL
            local_backend = std::make_shared<ClusterCatalogLocalBackendEncrypted>(context_, path, config_prefix);
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Cluster catalog encryption requires building with SSL support");
#endif
        }

        return local_backend;
    }

    if (storage_type == STORAGE_TYPE_ZOOKEEPER || storage_type == STORAGE_TYPE_KEEPER
        || storage_type == STORAGE_TYPE_ZOOKEEPER_ENCRYPTED || storage_type == STORAGE_TYPE_KEEPER_ENCRYPTED)
    {
        const auto path = config.getString(config_prefix + ".path");

        std::shared_ptr<IClusterCatalogMetadataBackend> keeper_backend;
        if (!storage_type.ends_with(STORAGE_TYPE_ENCRYPTED_SUFFIX))
            keeper_backend = std::make_shared<ClusterCatalogKeeperBackend>(context_, path);
        else
        {
#if USE_SSL
            keeper_backend = std::make_shared<ClusterCatalogKeeperBackendEncrypted>(context_, path, config_prefix);
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Cluster catalog encryption requires building with SSL support");
#endif
        }

        LOG_TRACE(getLog(), "Using {} cluster catalog backend ({}) at path: {}", storage_type, config_prefix, path);

        return keeper_backend;
    }

    throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Unknown cluster catalog storage type for {}: {}", config_prefix, storage_type);
}

}

ClusterCatalogMetadataStorage::ClusterCatalogMetadataStorage(
    ContextPtr context_, std::shared_ptr<IClusterCatalogMetadataBackend> backend_, String config_prefix_)
    : WithContext(context_)
    , backend(std::move(backend_))
    , config_prefix(std::move(config_prefix_))
{
}

std::unique_ptr<ClusterCatalogMetadataStorage> ClusterCatalogMetadataStorage::create(
    const ContextPtr & context_, const std::string & config_prefix, const std::string & default_local_path)
{
    auto backend_ = createClusterCatalogMetadataBackendImpl(context_, config_prefix, default_local_path);
    return std::unique_ptr<ClusterCatalogMetadataStorage>(
        new ClusterCatalogMetadataStorage(context_, std::move(backend_), String{config_prefix}));
}

bool ClusterCatalogMetadataStorage::exists(const String & relative_file_name) const
{
    return backend->exists(relative_file_name);
}

std::vector<String> ClusterCatalogMetadataStorage::list() const
{
    const auto paths = backend->list();
    return std::vector<String>(paths.begin(), paths.end());
}

String ClusterCatalogMetadataStorage::read(const String & relative_file_name) const
{
    return backend->read(relative_file_name);
}

void ClusterCatalogMetadataStorage::write(const String & relative_file_name, const String & data, bool replace)
{
    backend->write(relative_file_name, data, replace);
}

void ClusterCatalogMetadataStorage::remove(const String & relative_file_name)
{
    backend->remove(relative_file_name);
}

bool ClusterCatalogMetadataStorage::removeIfExists(const String & relative_file_name)
{
    return backend->removeIfExists(relative_file_name);
}

bool ClusterCatalogMetadataStorage::isReplicated() const
{
    return backend && backend->isReplicated();
}

bool ClusterCatalogMetadataStorage::waitUpdate()
{
    if (!backend || !backend->isReplicated())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Periodic updates are not supported for local cluster catalog storage");

    const auto & config = Context::getGlobalContextInstance()->getConfigRef();
    const size_t timeout = config.getUInt(config_prefix + ".update_timeout_ms", 5000);
    return backend->waitUpdate(timeout);
}

void ClusterCatalogMetadataStorage::shutdown()
{
    backend.reset();
}

}
