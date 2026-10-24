#include <Common/SQLClusters/SQLClusterMetadataStorage.h>

#include <atomic>
#include <filesystem>
#include <mutex>
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
#include <Common/ZooKeeper/Types.h>
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
    extern const int KEEPER_EXCEPTION;
}

static const std::string cluster_metadata_config_path = "cluster_metadata";

/// How many times a snapshot read (list the children, then read each of them) is retried when a concurrent
/// mutation bumped the root version midway, so the read never returns a mixed set that never existed.
static constexpr size_t max_snapshot_read_attempts = 100;

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

    /// Promote the version observed when the update watch was last armed (in `list`) to the
    /// "successfully loaded" version that `waitUpdate` compares against. No-op for local storage.
    virtual void commitReload() const {}

    /// Current root-node data version (replicated) or 0 (local). Used by `getAll` for snapshot retries.
    virtual Int32 getVersion() const { return 0; }

    /// Root version observed when the update watch was last armed in `list`.
    virtual Int32 getArmedVersion() const { return 0; }
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
    /// Guards the lazy (re)creation of `zookeeper_client` in `getClient`, which can run concurrently on
    /// the background reload task and a foreground DDL request.
    mutable std::mutex client_mutex;
    mutable zkutil::ZooKeeperPtr zookeeper_client{nullptr};
    /// Created once in the constructor and never reassigned, so it can be read from any thread without
    /// synchronization (Poco::Event's own operations are thread-safe). The same event is reused as the
    /// data-watch on the root node across reloads.
    mutable Coordination::EventPtr wait_event;
    /// Version of the root node's data that has been successfully loaded into the in-memory snapshot. It
    /// is bumped on every create/drop/alter (see `bumpVersionRequest`), so a single data-watch on the
    /// root notifies replicas of all kinds of changes - in particular ALTER, which only changes a child's
    /// data and would not be observed by a children-list watch. `waitUpdate` compares it against Keeper's
    /// current root version to decide whether a reload is due. It is advanced only by `commitReload`,
    /// after the snapshot read at `armed_version` has been fully applied, so a reload that throws midway
    /// does not lose the retry.
    mutable std::atomic<Int32> root_version = 0;
    /// Root version observed when the update watch was last armed in `list`. Promoted to `root_version`
    /// by `commitReload` once the corresponding snapshot has been successfully loaded.
    mutable std::atomic<Int32> armed_version = 0;

public:
    ZooKeeperStorage(ContextPtr context_, const std::string & path_)
        : WithContext(context_)
        , root_path(path_)
        , wait_event(std::make_shared<Poco::Event>())
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
        auto component_guard = Coordination::setCurrentComponent("SQLClusterMetadataStorage::list");
        /// Set a data-watch on the root node and remember its version as the armed version. Every
        /// modification bumps the root version (see `bumpVersionRequest`), so this watch fires for create,
        /// drop and alter alike. The armed version is promoted to `root_version` only by `commitReload`,
        /// after the caller has fully read and applied this snapshot.
        Coordination::Stat stat;
        getClient()->get(root_path, &stat, wait_event);
        armed_version = stat.version;

        return getClient()->getChildren(root_path);
    }

    void commitReload() const override
    {
        root_version = armed_version.load();
    }

    Int32 getVersion() const override
    {
        auto component_guard = Coordination::setCurrentComponent("SQLClusterMetadataStorage::getVersion");
        Coordination::Stat stat;
        getClient()->get(root_path, &stat);
        return stat.version;
    }

    Int32 getArmedVersion() const override { return armed_version.load(); }

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

        /// Mutate the child node and bump the root version (see `bumpVersionRequest`) in a single Keeper
        /// transaction, so the watched root version always advances together with the change. Otherwise a
        /// connection loss between the child mutation and a separate version bump would commit the change
        /// while leaving other replicas unnotified. ALTER only changes child data and would otherwise be
        /// invisible to a children-list watch.
        Coordination::Requests requests;
        if (replace)
            /// ALTER must update an existing cluster only; using `set` (not create-or-update) prevents a
            /// delayed ALTER from resurrecting a cluster that was concurrently dropped on another replica.
            requests.push_back(zkutil::makeSetRequest(getPath(file_name), write_data, -1));
        else
            requests.push_back(zkutil::makeCreateRequest(getPath(file_name), write_data, zkutil::CreateMode::Persistent));
        requests.push_back(bumpVersionRequest());

        Coordination::Responses responses;
        auto code = getClient()->tryMulti(requests, responses);
        if (code == Coordination::Error::ZOK)
            return;

        if (replace && code == Coordination::Error::ZNONODE)
        {
            throw Exception(
                ErrorCodes::CLUSTER_DOESNT_EXIST,
                "Metadata file {} for SQL cluster doesn't exist",
                file_name);
        }
        if (!replace && code == Coordination::Error::ZNODEEXISTS)
        {
            throw Exception(
                ErrorCodes::CLUSTER_ALREADY_EXISTS,
                "Metadata file {} for SQL cluster already exists",
                file_name);
        }
        zkutil::KeeperMultiException::check(code, requests, responses);
    }

    virtual std::string writeHook(const std::string & data) const
    {
        return data;
    }

    void remove(const std::string & file_name) override
    {
        auto component_guard = Coordination::setCurrentComponent("SQLClusterMetadataStorage::remove");
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
        auto component_guard = Coordination::setCurrentComponent("SQLClusterMetadataStorage::removeIfExists");
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
    Coordination::RequestPtr bumpVersionRequest() const
    {
        return zkutil::makeSetRequest(root_path, "", -1);
    }

    zkutil::ZooKeeperPtr getClient() const
    {
        std::lock_guard lock(client_mutex);
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

SQLClusterCreateQueries SQLClusterMetadataStorage::readClusters(const std::vector<String> & cluster_names) const
{
    SQLClusterCreateQueries result;
    for (const auto & cluster_name : cluster_names)
    {
        if (result.contains(cluster_name))
            throw Exception(ErrorCodes::CLUSTER_ALREADY_EXISTS, "Found duplicate SQL cluster `{}`", cluster_name);
        try
        {
            result.emplace(cluster_name, readCreateQuery(cluster_name));
        }
        catch (const Coordination::Exception & e)
        {
            /// A concurrent update may have removed the cluster between listing and reading.
            if (e.code == Coordination::Error::ZNONODE)
            {
                LOG_DEBUG(
                    getLogger("SQLClusterMetadataStorage"),
                    "Cluster '{}' was removed while reading, skipping",
                    cluster_name);
                continue;
            }
            throw;
        }
    }
    return result;
}

SQLClusterCreateQueries SQLClusterMetadataStorage::getAll() const
{
    /// Listing only fixes the *child list* at one root version - the per-child reads happen afterwards, so
    /// without the version re-check below the reader could assemble a map that never existed atomically.
    /// Retry until the root version is unchanged across the child reads. Concurrently dropped children
    /// are skipped in `readClusters` (same pattern as SQLDefinedHandlersMetadataStorage).
    for (size_t attempt = 0; attempt < max_snapshot_read_attempts; ++attempt)
    {
        auto cluster_names = listClusterNames();
        const Int32 listed_version = storage->getArmedVersion();
        auto clusters = readClusters(cluster_names);
        if (storage->getVersion() == listed_version)
            return clusters;
    }

    throw Exception(
        ErrorCodes::KEEPER_EXCEPTION,
        "Cannot read a consistent snapshot of SQL clusters: the set kept changing during {} attempts",
        max_snapshot_read_attempts);
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

void SQLClusterMetadataStorage::commitReload() const
{
    storage->commitReload();
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
