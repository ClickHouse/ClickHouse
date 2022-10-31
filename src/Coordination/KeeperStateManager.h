#pragma once

#include <Core/Types.h>
#include <string>
#include <Coordination/KeeperLogStore.h>
#include <Coordination/CoordinationSettings.h>
#include <libnuraft/nuraft.hxx>
#include <Poco/Util/AbstractConfiguration.h>
#include "Coordination/KeeperStateMachine.h"
#include <Coordination/KeeperSnapshotManager.h>

namespace DB
{

using KeeperServerConfigPtr = nuraft::ptr<nuraft::srv_config>;

/// When our configuration changes the following action types
/// can happen
enum class ConfigUpdateActionType
{
    RemoveServer,
    AddServer,
    UpdatePriority,
};

/// Action to update configuration
struct ConfigUpdateAction
{
    ConfigUpdateActionType action_type;
    KeeperServerConfigPtr server;
};

using ConfigUpdateActions = std::vector<ConfigUpdateAction>;

/// Responsible for managing our and cluster configuration
class KeeperStateManager : public nuraft::state_mgr
{
public:
    KeeperStateManager(
        int server_id_,
        const std::string & config_prefix_,
        const std::string & log_storage_path,
        const std::string & state_file_path,
        const Poco::Util::AbstractConfiguration & config,
        const CoordinationSettingsPtr & coordination_settings);

    /// Constructor for tests
    KeeperStateManager(
        int server_id_,
        const std::string & host,
        int port,
        const std::string & logs_path,
        const std::string & state_file_path);

    void loadLogStore(uint64_t last_commited_index, uint64_t logs_to_keep);

    /// Flush logstore and call shutdown of background thread
    void flushAndShutDownLogStore();

    /// Called on server start, in our case we don't use any separate logic for load
    nuraft::ptr<nuraft::cluster_config> load_config() override
    {
        std::lock_guard lock(configuration_wrapper_mutex);
        return configuration_wrapper.cluster_config;
    }

    /// Save cluster config (i.e. nodes, their priorities and so on)
    void save_config(const nuraft::cluster_config & config) override;

    void save_state(const nuraft::srv_state & state) override;

    nuraft::ptr<nuraft::srv_state> read_state() override;

    nuraft::ptr<nuraft::log_store> load_log_store() override { return log_store; }

    int32_t server_id() override { return my_server_id; }

    nuraft::ptr<nuraft::srv_config> get_srv_config() const { return configuration_wrapper.config; } /// NOLINT

    void system_exit(const int exit_code) override; /// NOLINT

    int getPort() const
    {
        std::lock_guard lock(configuration_wrapper_mutex);
        return configuration_wrapper.port;
    }

    bool shouldStartAsFollower() const
    {
        std::lock_guard lock(configuration_wrapper_mutex);
        return configuration_wrapper.servers_start_as_followers.contains(my_server_id);
    }

    bool isSecure() const
    {
        return secure;
    }

    nuraft::ptr<KeeperLogStore> getLogStore() const { return log_store; }

    uint64_t getTotalServers() const
    {
        std::lock_guard lock(configuration_wrapper_mutex);
        return configuration_wrapper.cluster_config->get_servers().size();
    }

    /// Read all log entries in log store from the begging and return latest config (with largest log_index)
    ClusterConfigPtr getLatestConfigFromLogStore() const;

    /// Get configuration diff between proposed XML and current state in RAFT
    ConfigUpdateActions getConfigurationDiff(const Poco::Util::AbstractConfiguration & config) const;

private:
    const std::filesystem::path & getOldServerStatePath();

    /// Wrapper struct for Keeper cluster config. We parse this
    /// info from XML files.
    struct KeeperConfigurationWrapper
    {
        /// Our port
        int port;
        /// Our config
        KeeperServerConfigPtr config;
        /// Servers id's to start as followers
        std::unordered_set<int> servers_start_as_followers;
        /// Cluster config
        ClusterConfigPtr cluster_config;
    };

    int my_server_id;
    bool secure;
    std::string config_prefix;

    mutable std::mutex configuration_wrapper_mutex;
    KeeperConfigurationWrapper configuration_wrapper;

    nuraft::ptr<KeeperLogStore> log_store;

    const std::filesystem::path server_state_path;

    Poco::Logger * logger;

public:
    /// Parse configuration from xml config.
    KeeperConfigurationWrapper parseServersConfiguration(const Poco::Util::AbstractConfiguration & config, bool allow_without_us) const;
};

}
