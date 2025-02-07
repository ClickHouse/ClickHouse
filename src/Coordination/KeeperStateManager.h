#pragma once

#include <string>
#include <Coordination/KeeperLogStore.h>
#include <Coordination/KeeperSnapshotManager.h>
#include <Core/Types.h>
#include <libnuraft/nuraft.hxx>
#include <Poco/Util/AbstractConfiguration.h>
#include "Coordination/KeeperStateMachine.h"
#include "Coordination/RaftServerConfig.h"

namespace DB
{
using KeeperServerConfigPtr = nuraft::ptr<nuraft::srv_config>;

/// Responsible for managing our and cluster configuration
class KeeperStateManager : public nuraft::state_mgr
{
public:
    KeeperStateManager(
        int server_id_,
        const std::string & config_prefix_,
        const std::string & server_state_file_name_,
        const Poco::Util::AbstractConfiguration & config,
        KeeperContextPtr keeper_context_);

    /// Constructor for tests
    KeeperStateManager(
        int server_id_,
        const std::string & host,
        int port,
        KeeperContextPtr keeper_context_);

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

    nuraft::ptr<nuraft::srv_config> get_srv_config() const
    {
        std::lock_guard lk(configuration_wrapper_mutex);
        return configuration_wrapper.config;
    }

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

    // TODO (myrrc) This should be removed once "reconfig" is stabilized
    ClusterUpdateActions getRaftConfigurationDiff(const Poco::Util::AbstractConfiguration & config, const CoordinationSettings & coordination_settings) const;

private:
    const String & getOldServerStatePath();

    DiskPtr getStateFileDisk() const;

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
    KeeperConfigurationWrapper configuration_wrapper TSA_GUARDED_BY(configuration_wrapper_mutex);

    bool log_store_initialized = false;
    nuraft::ptr<KeeperLogStore> log_store;

    const String server_state_file_name;

    KeeperContextPtr keeper_context;

    LoggerPtr logger;

public:
    /// Parse configuration from xml config.
    KeeperConfigurationWrapper parseServersConfiguration(const Poco::Util::AbstractConfiguration & config, bool allow_without_us, bool enable_async_replication) const;
};

}
