#include <Coordination/KeeperStateManager.h>

#include <filesystem>
#include <Coordination/Defines.h>
#include <Common/DNSResolver.h>
#include <Common/Exception.h>
#include <Common/isLocalAddress.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Disks/DiskLocal.h>
#include <Common/logger_useful.h>
#include "Coordination/CoordinationSettings.h"

namespace DB
{

namespace CoordinationSetting
{
    extern const CoordinationSettingsBool async_replication;
    extern const CoordinationSettingsUInt64 commit_logs_cache_size_threshold;
    extern const CoordinationSettingsBool compress_logs;
    extern const CoordinationSettingsBool force_sync;
    extern const CoordinationSettingsUInt64 latest_logs_cache_size_threshold;
    extern const CoordinationSettingsUInt64 log_file_overallocate_size;
    extern const CoordinationSettingsUInt64 max_flush_batch_size;
    extern const CoordinationSettingsUInt64 max_log_file_size;
    extern const CoordinationSettingsUInt64 rotate_log_storage_interval;
}

namespace ErrorCodes
{
    extern const int RAFT_ERROR;
    extern const int CORRUPTED_DATA;
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

}

/// this function quite long because contains a lot of sanity checks in config:
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

    return result;
}

KeeperStateManager::KeeperStateManager(int server_id_, const std::string & host, int port, KeeperContextPtr keeper_context_)
    : my_server_id(server_id_)
    , secure(false)
    , log_store(nuraft::cs_new<KeeperLogStore>(
          LogFileSettings{.force_sync = false, .compress_logs = false, .rotate_interval = 5000},
          FlushSettings{},
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
              .commit_logs_cache_size_threshold = keeper_context_->getCoordinationSettings()[CoordinationSetting::commit_logs_cache_size_threshold]
          },
          FlushSettings
          {
              .max_flush_batch_size = keeper_context_->getCoordinationSettings()[CoordinationSetting::max_flush_batch_size],
          },
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

}

void KeeperStateManager::save_state(const nuraft::srv_state & state)
{
    const auto & old_path = getOldServerStatePath();

    auto disk = getStateFileDisk();

    if (disk->existsFile(server_state_file_name))
    {
        auto buf = disk->writeFile(copy_lock_file);
        buf->finalize();
        disk->copyFile(server_state_file_name, *disk, old_path, ReadSettings{});
        disk->removeFile(copy_lock_file);
        disk->removeFile(old_path);
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

    disk->removeFileIfExists(old_path);
}

nuraft::ptr<nuraft::srv_state> KeeperStateManager::read_state()
{
    chassert(log_store_initialized);

    const auto & old_path = getOldServerStatePath();

    auto disk = getStateFileDisk();

    const auto try_read_file = [&](const auto & path) -> nuraft::ptr<nuraft::srv_state>
    {
        try
        {
            auto read_buf = disk->readFile(path, getReadSettings());
            auto content_size = read_buf->getFileSize();

            if (content_size == 0)
                return nullptr;

            uint64_t read_checksum{0};
            readIntBinary(read_checksum, *read_buf);

            uint8_t version;
            readIntBinary(version, *read_buf);

            auto buffer_size = content_size - sizeof read_checksum - sizeof version;

            auto state_buf = nuraft::buffer::alloc(buffer_size);
            read_buf->readStrict(reinterpret_cast<char *>(state_buf->data_begin()), buffer_size);

            SipHash hash;
            hash.update(version);
            hash.update(reinterpret_cast<const char *>(state_buf->data_begin()), state_buf->size());

            if (read_checksum != hash.get64())
            {
                constexpr auto error_format = "Invalid checksum while reading state from {}. Got {}, expected {}";
#ifdef NDEBUG
                LOG_ERROR(logger, error_format, path, hash.get64(), read_checksum);
                return nullptr;
#else
                throw Exception(ErrorCodes::CORRUPTED_DATA, error_format, disk->getPath() + path, hash.get64(), read_checksum);
#endif
            }

            auto state = nuraft::srv_state::deserialize(*state_buf);
            LOG_INFO(logger, "Read state from {}", fs::path(disk->getPath()) / path);
            return state;
        }
        catch (const std::exception & e)
        {
            if (const auto * exception = dynamic_cast<const Exception *>(&e);
                exception != nullptr && exception->code() == ErrorCodes::CORRUPTED_DATA)
            {
                throw;
            }

            LOG_ERROR(logger, "Failed to deserialize state from {}", disk->getPath() + path);
            return nullptr;
        }
    };

    if (disk->existsFile(server_state_file_name))
    {
        auto state = try_read_file(server_state_file_name);

        if (state)
        {
            disk->removeFileIfExists(old_path);
            return state;
        }

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
                disk->moveFile(old_path, server_state_file_name);
                return state;
            }
            disk->removeFile(old_path);
        }
    }
    else if (disk->existsFile(copy_lock_file))
    {
        disk->removeFile(copy_lock_file);
    }

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

    std::unordered_map<int, KeeperServerConfigPtr> new_ids, old_ids;
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
