#pragma once

#include <Core/Types.h>
#include <string>
#include <Coordination/KeeperLogStore.h>
#include <Coordination/CoordinationSettings.h>
#include <libnuraft/nuraft.hxx> // Y_IGNORE
#include <Poco/Util/AbstractConfiguration.h>
#include <Coordination/KeeperSnapshotManager.h>

namespace DB
{

using KeeperServerConfigPtr = nuraft::ptr<nuraft::srv_config>;

struct KeeperServersConfiguration
{
    int port;
    KeeperServerConfigPtr config;
    std::unordered_set<int> servers_start_as_followers;
    ClusterConfigPtr cluster_config;
};

enum class ConfigUpdateActionType
{
    RemoveServer,
    AddServer,
    UpdatePriority,
};

struct ConfigUpdateAction
{
    ConfigUpdateActionType action_type;
    KeeperServerConfigPtr server;
};

using ConfigUpdateActions = std::vector<ConfigUpdateAction>;

class KeeperStateManager : public nuraft::state_mgr
{
public:
    KeeperStateManager(
        int server_id_,
        const std::string & config_prefix_,
        const Poco::Util::AbstractConfiguration & config,
        const CoordinationSettingsPtr & coordination_settings,
        bool standalone_keeper);

    KeeperStateManager(
        int server_id_,
        const std::string & host,
        int port,
        const std::string & logs_path);

    void loadLogStore(uint64_t last_commited_index, uint64_t logs_to_keep);

    void flushLogStore();

    nuraft::ptr<nuraft::cluster_config> load_config() override { return servers_configuration.cluster_config; }

    void save_config(const nuraft::cluster_config & config) override;

    void save_state(const nuraft::srv_state & state) override;

    nuraft::ptr<nuraft::srv_state> read_state() override { return server_state; }

    nuraft::ptr<nuraft::log_store> load_log_store() override { return log_store; }

    int32_t server_id() override { return my_server_id; }

    nuraft::ptr<nuraft::srv_config> get_srv_config() const { return servers_configuration.config; }

    void system_exit(const int /* exit_code */) override {}

    int getPort() const { return servers_configuration.port; }

    bool shouldStartAsFollower() const
    {
        return servers_configuration.servers_start_as_followers.count(my_server_id);
    }

    bool isSecure() const
    {
        return secure;
    }

    nuraft::ptr<KeeperLogStore> getLogStore() const { return log_store; }

    uint64_t getTotalServers() const { return servers_configuration.cluster_config->get_servers().size(); }

    ClusterConfigPtr getLatestConfigFromLogStore() const;

    void setClusterConfig(const ClusterConfigPtr & cluster_config);

    ConfigUpdateActions getConfigurationDiff(const Poco::Util::AbstractConfiguration & config) const;

private:
    int my_server_id;
    bool secure;
    std::string config_prefix;
    KeeperServersConfiguration servers_configuration;
    nuraft::ptr<KeeperLogStore> log_store;
    nuraft::ptr<nuraft::srv_state> server_state;

    Poco::Logger * log;

    KeeperServersConfiguration parseServersConfiguration(const Poco::Util::AbstractConfiguration & config) const;
};

}
