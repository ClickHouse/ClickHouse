#include <Coordination/KeeperStateManager.h>

#include <expected>
#include <filesystem>
#include <Coordination/Defines.h>
#include <Common/DNSResolver.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/isLocalAddress.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Disks/DiskLocal.h>
#include <Disks/supportWritingWithAppend.h>
#include <Common/logger_useful.h>
#include <Coordination/CoordinationSettings.h>

namespace DB
{

namespace CoordinationSetting
{
    extern const CoordinationSettingsBool async_replication;
    extern const CoordinationSettingsBool compress_logs;
    extern const CoordinationSettingsBool force_sync;
    extern const CoordinationSettingsUInt64 latest_logs_cache_size_threshold;
    extern const CoordinationSettingsUInt64 log_file_overallocate_size;
    extern const CoordinationSettingsUInt64 max_flush_batch_size;
    extern const CoordinationSettingsUInt64 max_log_file_size;
    extern const CoordinationSettingsNonZeroUInt64 log_readahead_chunk_size;
    extern const CoordinationSettingsUInt64 log_readahead_commit_window_bytes;
    extern const CoordinationSettingsNonZeroUInt64 log_readahead_eviction_timeout_ms;
    extern const CoordinationSettingsBool log_readahead_enabled;
    extern const CoordinationSettingsNonZeroUInt64 log_readahead_max_peer_readers;
    extern const CoordinationSettingsUInt64 log_readahead_pool_threads;
    extern const CoordinationSettingsUInt64 log_readahead_serve_wait_timeout_ms;
    extern const CoordinationSettingsNonZeroUInt64 log_readahead_window_bytes;
    extern const CoordinationSettingsUInt64 min_time_between_fsyncs_ms;
    extern const CoordinationSettingsNonZeroUInt64 rotate_log_storage_interval;
}

namespace ErrorCodes
{
    extern const int RAFT_ERROR;
    extern const int CORRUPTED_DATA;
    extern const int BAD_ARGUMENTS;
}

namespace
{

const std::string copy_lock_file = "STATE_COPY_LOCK";

bool isLocalhost(const std::string & hostname)
{
    try
    {
        return isLocalAddress(DNSResolver::instance().resolveHostAllInOriginOrder(hostname).front());
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    return false;
}

std::unordered_map<UInt64, std::string> getClientPorts(const Poco::Util::AbstractConfiguration & config)
{
    using namespace std::string_literals;
    static const std::array config_port_names = {
        "keeper_server.tcp_port"s,
        "keeper_server.tcp_port_secure"s,
        "interserver_http_port"s,
        "interserver_https_port"s,
        "tcp_port"s,
        "tcp_with_proxy_port"s,
        "tcp_port_secure"s,
        "mysql_port"s,
        "postgresql_port"s,
        "grpc_port"s,
        "prometheus.port"s,
    };

    std::unordered_map<UInt64, std::string> ports;
    for (const auto & config_port_name : config_port_names)
    {
        if (config.has(config_port_name))
            ports[config.getUInt64(config_port_name)] = config_port_name;
    }
    return ports;
}


std::optional<AuthenticationData> getClientPasswordAuthentication(const Poco::Util::AbstractConfiguration & config)
{
    static const std::unordered_map<std::string, AuthenticationType> AUTH_TYPE_MAP
    {
        {"client_password", AuthenticationType::PLAINTEXT_PASSWORD},
        {"keeper_server.client_password", AuthenticationType::PLAINTEXT_PASSWORD},
        {"client_password_sha256_hex", AuthenticationType::SHA256_PASSWORD},
        {"keeper_server.client_password_sha256_hex", AuthenticationType::SHA256_PASSWORD},
        {"client_password_double_sha1_hex", AuthenticationType::DOUBLE_SHA1_PASSWORD},
        {"keeper_server.client_password_double_sha1_hex", AuthenticationType::DOUBLE_SHA1_PASSWORD},
    };

    std::optional<AuthenticationData> data;
    for (const auto & [config_password_name, auth_type] : AUTH_TYPE_MAP)
    {
        if (config.has(config_password_name))
        {
            if (data.has_value())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only one authentication type is allowed in config");

            data.emplace(AuthenticationData(auth_type));
            if (config_password_name.ends_with("client_password"))
            {
                auto password = config.getString(config_password_name);
                if (password.length() > Coordination::PASSWORD_LENGTH)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Password cannot be longer than {} characters, specified {}", Coordination::PASSWORD_LENGTH, password.size());

                data->setPassword(password, /* second_factor */ {}, /* validate */ true);
            }
            else
                data->setPasswordHashHex(config.getString(config_password_name), /* second_factor */ {}, /* validate */ true);
        }
    }

    return data;
}

ReadAheadSettings buildReadAheadSettings(const KeeperContextPtr & keeper_context)
{
    ReadAheadSettings settings
    {
        .enabled = keeper_context->getCoordinationSettings()[CoordinationSetting::log_readahead_enabled],
        .window_bytes = keeper_context->getCoordinationSettings()[CoordinationSetting::log_readahead_window_bytes],
        .max_peer_readers = keeper_context->getCoordinationSettings()[CoordinationSetting::log_readahead_max_peer_readers],
        .eviction_timeout_ms = keeper_context->getCoordinationSettings()[CoordinationSetting::log_readahead_eviction_timeout_ms],
        .pool_threads = keeper_context->getCoordinationSettings()[CoordinationSetting::log_readahead_pool_threads],
        .serve_wait_timeout_ms = keeper_context->getCoordinationSettings()[CoordinationSetting::log_readahead_serve_wait_timeout_ms],
        .chunk_size = keeper_context->getCoordinationSettings()[CoordinationSetting::log_readahead_chunk_size],
        .commit_window_bytes = keeper_context->getCoordinationSettings()[CoordinationSetting::log_readahead_commit_window_bytes],
    };
    validateReadAheadSettings(settings);
    return settings;
}

}

/// this function is quite long because it contains a lot of sanity checks in config:
/// 1. No duplicate endpoints
/// 2. No "localhost" or "127.0.0.1" or another local addresses mixed with normal addresses
/// 3. Raft internal port is not equal to any other port for client
/// 4. No duplicate IDs
/// 5. Our ID present in hostnames list
KeeperStateManager::KeeperConfigurationWrapper
KeeperStateManager::parseServersConfiguration(const Poco::Util::AbstractConfiguration & config, bool allow_without_us, bool enable_async_replication) const
{
    const bool hostname_checks_enabled = config.getBool(config_prefix + ".hostname_checks_enabled", true);

    KeeperConfigurationWrapper result;
    result.cluster_config = std::make_shared<nuraft::cluster_config>();
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix + ".raft_configuration", keys);

    auto client_ports = getClientPorts(config);

    /// Sometimes (especially in cloud envs) users can provide incorrect
    /// configuration with duplicated raft ids or endpoints. We check them
    /// on config parsing stage and never commit to quorum.
    std::unordered_map<std::string, int> check_duplicated_hostnames;

    size_t total_servers = 0;
    bool localhost_present = false;
    std::string non_local_hostname;
    size_t local_address_counter = 0;
    for (const auto & server_key : keys)
    {
        if (!startsWith(server_key, "server"))
            continue;

        std::string full_prefix = config_prefix + ".raft_configuration." + server_key;

        if (getMultipleValuesFromConfig(config, full_prefix, "id").size() > 1
            || getMultipleValuesFromConfig(config, full_prefix, "hostname").size() > 1
            || getMultipleValuesFromConfig(config, full_prefix, "port").size() > 1)
        {
            throw Exception(ErrorCodes::RAFT_ERROR, "Multiple <id> or <hostname> or <port> specified for a single <server>");
        }

        int new_server_id = config.getInt(full_prefix + ".id");
        std::string hostname = config.getString(full_prefix + ".hostname");
        int port = config.getInt(full_prefix + ".port");
        bool can_become_leader = config.getBool(full_prefix + ".can_become_leader", true);
        int32_t priority = config.getInt(full_prefix + ".priority", 1);
        bool start_as_follower = config.getBool(full_prefix + ".start_as_follower", false);

        if (client_ports.contains(port))
        {
            throw Exception(
                ErrorCodes::RAFT_ERROR,
                "Raft configuration contains hostname '{}' with port '{}' which is equal to '{}' in server configuration",
                hostname,
                port,
                client_ports[port]);
        }

        if (hostname_checks_enabled)
        {
            if (hostname == "localhost")
            {
                localhost_present = true;
                local_address_counter++;
            }
            else if (isLocalhost(hostname))
            {
                local_address_counter++;
            }
            else
            {
                non_local_hostname = hostname;
            }
        }

        if (start_as_follower)
            result.servers_start_as_followers.insert(new_server_id);

        auto endpoint = hostname + ":" + std::to_string(port);
        if (check_duplicated_hostnames.contains(endpoint))
        {
            throw Exception(
                ErrorCodes::RAFT_ERROR,
                "Raft config contains duplicate endpoints: "
                "endpoint {} has been already added with id {}, but going to add it one more time with id {}",
                endpoint,
                check_duplicated_hostnames[endpoint],
                new_server_id);
        }

        /// Fullscan to check duplicated ids
        for (const auto & [id_endpoint, id] : check_duplicated_hostnames)
        {
            if (new_server_id == id)
                throw Exception(
                    ErrorCodes::RAFT_ERROR,
                    "Raft config contains duplicate ids: id {} has been already added with endpoint {}, "
                    "but going to add it one more time with endpoint {}",
                    id,
                    id_endpoint,
                    endpoint);
        }
        check_duplicated_hostnames.emplace(endpoint, new_server_id);


        auto peer_config = nuraft::cs_new<nuraft::srv_config>(new_server_id, 0, endpoint, "", !can_become_leader, priority);
        if (my_server_id == new_server_id)
        {
            result.config = peer_config;
            result.port = port;
        }

        result.cluster_config->get_servers().push_back(peer_config);
        total_servers++;
    }

    result.cluster_config->set_async_replication(enable_async_replication);

    if (!result.config && !allow_without_us)
        throw Exception(ErrorCodes::RAFT_ERROR, "Our server id {} not found in raft_configuration section", my_server_id);

    if (result.servers_start_as_followers.size() == total_servers)
        throw Exception(ErrorCodes::RAFT_ERROR, "At least one of servers should be able to start as leader (without <start_as_follower>)");

    if (hostname_checks_enabled)
    {
        if (localhost_present && !non_local_hostname.empty())
        {
            throw Exception(
                ErrorCodes::RAFT_ERROR,
                "Mixing 'localhost' and non-local hostnames ('{}') in raft_configuration is not allowed. "
                "Different hosts can resolve 'localhost' to themselves so it's not allowed.",
                non_local_hostname);
        }

        if (!non_local_hostname.empty() && local_address_counter > 1)
        {
            throw Exception(
                ErrorCodes::RAFT_ERROR,
                "Local address specified more than once ({} times) and non-local hostnames also exists ('{}') in raft_configuration. "
                "Such configuration is not allowed because single host can vote multiple times.",
                local_address_counter,
                non_local_hostname);
        }
    }

    result.auth_data = getClientPasswordAuthentication(config);

    return result;
}

/// Constructor for tests
KeeperStateManager::KeeperStateManager(int server_id_, const std::string & host, int port, KeeperContextPtr keeper_context_)
    : my_server_id(server_id_)
    , secure(false)
    , log_store(nuraft::cs_new<KeeperLogStore>(
          LogFileSettings{.force_sync = false, .compress_logs = false, .rotate_interval = 5000},
          FlushSettings{},
          ReadAheadSettings{},
          keeper_context_))
    , server_state_file_name("state")
    , keeper_context(keeper_context_)
    , logger(getLogger("KeeperStateManager"))
{
    auto peer_config = nuraft::cs_new<nuraft::srv_config>(my_server_id, host + ":" + std::to_string(port));
    configuration_wrapper.cluster_config = nuraft::cs_new<nuraft::cluster_config>();
    configuration_wrapper.port = port;
    configuration_wrapper.config = peer_config;
    configuration_wrapper.cluster_config->get_servers().push_back(peer_config);
}

KeeperStateManager::KeeperStateManager(
    int my_server_id_,
    const std::string & config_prefix_,
    const std::string & server_state_file_name_,
    const Poco::Util::AbstractConfiguration & config,
    KeeperContextPtr keeper_context_)
    : my_server_id(my_server_id_)
    , secure(config.getBool(config_prefix_ + ".raft_configuration.secure", false))
    , config_prefix(config_prefix_)
    , configuration_wrapper(parseServersConfiguration(config, false, keeper_context_->getCoordinationSettings()[CoordinationSetting::async_replication]))
    , log_store(nuraft::cs_new<KeeperLogStore>(
          LogFileSettings
          {
              .force_sync = keeper_context_->getCoordinationSettings()[CoordinationSetting::force_sync],
              .compress_logs = keeper_context_->getCoordinationSettings()[CoordinationSetting::compress_logs],
              .rotate_interval = keeper_context_->getCoordinationSettings()[CoordinationSetting::rotate_log_storage_interval],
              .max_size = keeper_context_->getCoordinationSettings()[CoordinationSetting::max_log_file_size],
              .overallocate_size = keeper_context_->getCoordinationSettings()[CoordinationSetting::log_file_overallocate_size],
              .latest_logs_cache_size_threshold = keeper_context_->getCoordinationSettings()[CoordinationSetting::latest_logs_cache_size_threshold],
          },
          FlushSettings
          {
              .max_flush_batch_size = keeper_context_->getCoordinationSettings()[CoordinationSetting::max_flush_batch_size],
              .min_time_between_fsyncs_ms = keeper_context_->getCoordinationSettings()[CoordinationSetting::min_time_between_fsyncs_ms],
          },
          buildReadAheadSettings(keeper_context_),
          keeper_context_))
    , server_state_file_name(server_state_file_name_)
    , keeper_context(keeper_context_)
    , logger(getLogger("KeeperStateManager"))
{
}

void KeeperStateManager::loadLogStore(uint64_t last_commited_index, uint64_t logs_to_keep)
{
    log_store->init(last_commited_index, logs_to_keep);
    log_store_initialized = true;
}

void KeeperStateManager::system_exit(const int /* exit_code */)
{
    /// NuRaft itself calls exit() which will call atexit handlers
    /// and this may lead to an issues in multi-threaded program.
    ///
    /// Override this with abort().
    abort();
}

ClusterConfigPtr KeeperStateManager::getLatestConfigFromLogStore() const
{
    auto entry_with_change = log_store->getLatestConfigChange();
    if (entry_with_change)
        return ClusterConfig::deserialize(entry_with_change->get_buf());
    return nullptr;
}

std::optional<AuthenticationData> KeeperStateManager::getAuthenticationData() const
{
    std::lock_guard lock(configuration_wrapper_mutex);
    return configuration_wrapper.auth_data;
}

void KeeperStateManager::flushAndShutDownLogStore()
{
    log_store->flushChangelogAndShutdown();
}

void KeeperStateManager::save_config(const nuraft::cluster_config & config)
{
    std::lock_guard lock(configuration_wrapper_mutex);
    nuraft::ptr<nuraft::buffer> buf = config.serialize();
    configuration_wrapper.cluster_config = nuraft::cluster_config::deserialize(*buf);
}

const String & KeeperStateManager::getOldServerStatePath()
{
    static auto old_path = [this]
    {
        return server_state_file_name + "-OLD";
    }();

    return old_path;
}

DiskPtr KeeperStateManager::getStateFileDisk() const
{
    return keeper_context->getStateFileDisk();
}

namespace
{
enum ServerStateVersion : uint8_t
{
    V1 = 0
};

constexpr auto current_server_state_version = ServerStateVersion::V1;

/// Makes an existing file durable, contents and directory entry, writing nothing to it:
/// `WriteMode::Append` does not truncate and there is nothing buffered to flush. Not usable on
/// object storage, where an empty append records a key for an object it never creates.
void syncExistingFile(const DiskPtr & disk, const String & path)
{
    if (disk->isRemote() || !supportWritingWithAppend(disk))
        return;

    {
        auto buf = disk->writeFile(path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, {});
        buf->sync();
        buf->finalize();
    }

    /// The directory entry can be lost on its own, taking the file with it.
    SyncGuardPtr dir_sync_guard = disk->getDirectorySyncGuard("");
}

enum class StateFileError : uint8_t
{
    /// Nothing worth keeping: too short to hold the header, or NuRaft could not parse it.
    UNUSABLE,
    /// The checksum did not match. Callers keep such a file instead of deleting it.
    CORRUPTED_DATA,
};

using StateFileOrError = std::expected<nuraft::ptr<nuraft::srv_state>, StateFileError>;

/// A failure to read is propagated rather than reported as an error verdict, because callers act
/// on a verdict by deleting or truncating the file.
String readStateFile(const DiskPtr & disk, const String & path)
{
    String content;
    auto read_buf = disk->readFile(path, getReadSettings());
    readStringUntilEOF(content, *read_buf);
    return content;
}

/// A bad checksum is reported rather than thrown, because the backup must still be tried;
/// `read_state` re-raises it at its tail once nothing was recoverable.
StateFileOrError verifyStateFile(const String & content, const DiskPtr & disk, const String & path, LoggerPtr logger)
{
    /// Checksum plus version, written by `save_state` ahead of the serialized state.
    constexpr size_t header_size = sizeof(uint64_t) + sizeof(uint8_t);

    try
    {
        if (content.size() < header_size)
            return std::unexpected(StateFileError::UNUSABLE);

        uint64_t read_checksum = 0;
        uint8_t version = 0;

        ReadBufferFromString content_buf(content);
        readIntBinary(read_checksum, content_buf);
        readIntBinary(version, content_buf);

        auto state_buf = nuraft::buffer::alloc(content.size() - header_size);
        content_buf.readStrict(reinterpret_cast<char *>(state_buf->data_begin()), state_buf->size());

        SipHash hash;
        hash.update(version);
        hash.update(reinterpret_cast<const char *>(state_buf->data_begin()), state_buf->size());

        if (read_checksum != hash.get64())
        {
            LOG_ERROR(logger, "Invalid checksum while reading state from {}. Got {}, expected {}", path, read_checksum, hash.get64());
            return std::unexpected(StateFileError::CORRUPTED_DATA);
        }

        return nuraft::srv_state::deserialize(*state_buf);
    }
    /// A truncated state file makes NuRaft read past the end of the buffer, which it reports as
    /// `std::overflow_error`. Anything else must not be mistaken for a verdict about the content.
    catch (const std::overflow_error &)
    {
        LOG_ERROR(logger, "Failed to deserialize state from {}", disk->getPath() + path);
        return std::unexpected(StateFileError::UNUSABLE);
    }
}

}

void KeeperStateManager::save_state(const nuraft::srv_state & state)
{
    const auto & old_path = getOldServerStatePath();

    auto disk = getStateFileDisk();

    /// Only a live file that reads back and verifies may become the backup, so that a torn one
    /// cannot overwrite a still-valid `state-OLD`. The content read here is reused as the backup
    /// below, so the file is not read a second time.
    const bool live_state_exists = disk->existsFile(server_state_file_name);
    const String live_state = live_state_exists ? readStateFile(disk, server_state_file_name) : String{};

    if (live_state_exists && verifyStateFile(live_state, disk, server_state_file_name, logger).has_value())
    {
        /// These bytes are complete but, on a retry after a failed sync, maybe not yet durable,
        /// while the `state-OLD` about to be overwritten is.
        syncExistingFile(disk, server_state_file_name);

        /// Back up the current state so it survives the rewrite below. The backup is kept
        /// until the new state file is fully written and synced (removed at the end), so a
        /// crash during the rewrite can always recover the previous state via `read_state`.
        auto lock_buf = disk->writeFile(copy_lock_file);
        lock_buf->finalize();

        /// Not `IDisk::copyFile`: it only finalizes the write, so the backup would stay in the
        /// page cache. It must be durable before the live file is truncated below.
        {
            auto out = disk->writeFile(old_path);
            writeString(live_state, *out);
            out->finalize();
            out->sync();
        }

        disk->removeFile(copy_lock_file);

        /// `state-OLD` is newly created, so its directory entry needs a sync too. The guard syncs
        /// on destruction, i.e. still before the truncation below; a no-op on object storage.
        SyncGuardPtr dir_sync_guard = disk->getDirectorySyncGuard("");
    }

    auto server_state_file = disk->writeFile(server_state_file_name);
    auto buf = state.serialize();

    // calculate checksum
    SipHash hash;
    hash.update(current_server_state_version);
    hash.update(reinterpret_cast<const char *>(buf->data_begin()), buf->size());
    writeIntBinary(hash.get64(), *server_state_file);

    writeIntBinary(static_cast<uint8_t>(current_server_state_version), *server_state_file);

    server_state_file->write(reinterpret_cast<const char *>(buf->data_begin()), buf->size());
    server_state_file->sync();
    server_state_file->finalize();

    {
        /// The new state file may also have just been created, so sync its directory entry
        /// before the backup is dropped below.
        SyncGuardPtr dir_sync_guard = disk->getDirectorySyncGuard("");
    }

    disk->removeFileIfExists(old_path);
}

nuraft::ptr<nuraft::srv_state> KeeperStateManager::read_state()
{
    chassert(log_store_initialized);

    const auto & old_path = getOldServerStatePath();

    auto disk = getStateFileDisk();

    /// A read failure is deliberately not caught: every nullptr below deletes the file just read,
    /// and no state at all makes NuRaft restart from term 0 with an empty vote.
    /// A checksum mismatch is only remembered here, so that the backup is still tried.
    std::optional<String> corrupted_path;
    const auto try_read_file = [&](const auto & path) -> nuraft::ptr<nuraft::srv_state>
    {
        auto state = verifyStateFile(readStateFile(disk, path), disk, path, logger);
        if (!state)
        {
            if (state.error() == StateFileError::CORRUPTED_DATA && !corrupted_path.has_value())
                corrupted_path = path;
            return nullptr;
        }

        LOG_INFO(logger, "Read state from {}", fs::path(disk->getPath()) / path);
        return *state;
    };

    if (disk->existsFile(server_state_file_name))
    {
        auto state = try_read_file(server_state_file_name);

        if (state)
        {
            /// Parsing shows the bytes are complete, not that they reached the disk, and the term
            /// is about to be acted on.
            syncExistingFile(disk, server_state_file_name);

            /// The backup is kept regardless: `save_state` drops it once it has synced a replacement.
            return state;
        }

        /// A file that failed its checksum is kept for now: if nothing turns out to be
        /// recoverable, the tail below has to be able to fail the same way on every restart
        /// rather than leaving the next one with no state at all.
        if (corrupted_path != server_state_file_name)
            disk->removeFile(server_state_file_name);
    }

    if (disk->existsFile(old_path))
    {
        if (disk->existsFile(copy_lock_file))
        {
            disk->removeFile(old_path);
            disk->removeFile(copy_lock_file);
        }
        else
        {
            auto state = try_read_file(old_path);
            if (state)
            {
                /// The backup is deliberately left in place: copying it back would drop it
                /// before the copy is durable. The next successful `save_state` clears it.
                return state;
            }
            if (corrupted_path != old_path)
                disk->removeFile(old_path);
        }
    }
    else if (disk->existsFile(copy_lock_file))
    {
        disk->removeFile(copy_lock_file);
    }

#ifndef NDEBUG
    /// Nothing was recoverable, so a checksum mismatch seen on the way here is a real corruption
    /// rather than a torn file the backup covered for.
    if (corrupted_path.has_value())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid checksum while reading state from {}", disk->getPath() + *corrupted_path);
#endif

    if (log_store->next_slot() != 1)
        LOG_ERROR(
            logger,
            "No state was read but Keeper contains data which indicates that the state file was lost. This is dangerous and can lead to "
            "data loss.");

    return nullptr;
}

ClusterUpdateActions KeeperStateManager::getRaftConfigurationDiff(
    const Poco::Util::AbstractConfiguration & config, const CoordinationSettings & coordination_settings) const
{
    auto new_configuration_wrapper = parseServersConfiguration(config, true, coordination_settings[CoordinationSetting::async_replication]);

    std::unordered_map<int, KeeperServerConfigPtr> new_ids;
    std::unordered_map<int, KeeperServerConfigPtr> old_ids;
    for (const auto & new_server : new_configuration_wrapper.cluster_config->get_servers())
        new_ids[new_server->get_id()] = new_server;

    {
        std::lock_guard lock(configuration_wrapper_mutex);
        for (const auto & old_server : configuration_wrapper.cluster_config->get_servers())
            old_ids[old_server->get_id()] = old_server;
    }

    ClusterUpdateActions result;

    /// First of all add new servers
    for (const auto & [new_id, server_config] : new_ids)
    {
        auto old_server_it = old_ids.find(new_id);
        if (old_server_it == old_ids.end())
            result.emplace_back(AddRaftServer{RaftServerConfig{*server_config}});
        else
        {
            const auto & old_endpoint = old_server_it->second->get_endpoint();
            if (old_endpoint != server_config->get_endpoint())
            {
                LOG_WARNING(
                    getLogger("RaftConfiguration"),
                    "Config will be ignored because a server with ID {} is already present in the cluster on a different endpoint ({}). "
                    "The endpoint of the current servers should not be changed. For servers on a new endpoint, please use a new ID.",
                    new_id,
                    old_endpoint);
                return {};
            }
        }
    }

    /// After that remove old ones
    for (const auto & [old_id, server_config] : old_ids)
        if (!new_ids.contains(old_id))
            result.emplace_back(RemoveRaftServer{old_id});

    {
        std::lock_guard lock(configuration_wrapper_mutex);
        /// And update priority if required
        for (const auto & old_server : configuration_wrapper.cluster_config->get_servers())
        {
            for (const auto & new_server : new_configuration_wrapper.cluster_config->get_servers())
            {
                if (old_server->get_id() == new_server->get_id())
                {
                    if (old_server->get_priority() != new_server->get_priority())
                    {
                        result.emplace_back(UpdateRaftServerPriority{
                            .id = new_server->get_id(),
                            .priority = new_server->get_priority()
                        });
                    }
                    break;
                }
            }
        }
    }

    return result;
}

}
