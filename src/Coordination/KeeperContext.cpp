#include <Coordination/KeeperContext.h>

#include <Coordination/Defines.h>
#include <Disks/DiskLocal.h>
#include <Interpreters/Context.h>

namespace DB
{


KeeperContext::KeeperContext(bool standalone_keeper_)
    : disk_selector(std::make_shared<DiskSelector>())
    , standalone_keeper(standalone_keeper_)
{}

void KeeperContext::initialize(const Poco::Util::AbstractConfiguration & config)
{
    digest_enabled = config.getBool("keeper_server.digest_enabled", false);
    ignore_system_path_on_startup = config.getBool("keeper_server.ignore_system_path_on_startup", false);

    disk_selector->initialize(config, "storage_configuration.disks", Context::getGlobalContextInstance());

    log_storage_path = getLogsPathFromConfig(config);
    snapshot_storage_path = getSnapshotsPathFromConfig(config);

    state_file_path = getStateFilePathFromConfig(config);
}

KeeperContext::Phase KeeperContext::getServerState() const
{
    return server_state;
}

void KeeperContext::setServerState(KeeperContext::Phase server_state_)
{
    server_state = server_state_;
}

bool KeeperContext::ignoreSystemPathOnStartup() const
{
    return ignore_system_path_on_startup;
}

bool KeeperContext::digestEnabled() const
{
    return digest_enabled;
}

void KeeperContext::setDigestEnabled(bool digest_enabled_)
{
    digest_enabled = digest_enabled_;
}

KeeperContext::Storage KeeperContext::getLogsPathFromConfig(const Poco::Util::AbstractConfiguration & config) const
{
    /// the most specialized path
    if (config.has("keeper_server.log_storage_path"))
        return std::make_shared<DiskLocal>("LogDisk", config.getString("keeper_server.log_storage_path"), 0);

    if (config.has("keeper_server.log_storage_disk"))
        return config.getString("keeper_server.log_storage_disk");

    if (config.has("keeper_server.storage_path"))
        return std::make_shared<DiskLocal>("LogDisk", std::filesystem::path{config.getString("keeper_server.storage_path")} / "logs", 0);

    if (standalone_keeper)
        return std::make_shared<DiskLocal>("LogDisk", std::filesystem::path{config.getString("path", KEEPER_DEFAULT_PATH)} / "logs", 0);
    else
        return std::make_shared<DiskLocal>("LogDisk", std::filesystem::path{config.getString("path", DBMS_DEFAULT_PATH)} / "coordination/logs", 0);
}

std::string KeeperContext::getSnapshotsPathFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    /// the most specialized path
    if (config.has("keeper_server.snapshot_storage_path"))
        return config.getString("keeper_server.snapshot_storage_path");

    if (config.has("keeper_server.storage_path"))
        return std::filesystem::path{config.getString("keeper_server.storage_path")} / "snapshots";

    if (standalone_keeper)
        return std::filesystem::path{config.getString("path", KEEPER_DEFAULT_PATH)} / "snapshots";
    else
        return std::filesystem::path{config.getString("path", DBMS_DEFAULT_PATH)} / "coordination/snapshots";
}

std::string KeeperContext::getStateFilePathFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    if (config.has("keeper_server.storage_path"))
        return std::filesystem::path{config.getString("keeper_server.storage_path")} / "state";

    if (config.has("keeper_server.snapshot_storage_path"))
        return std::filesystem::path(config.getString("keeper_server.snapshot_storage_path")).parent_path() / "state";

    if (config.has("keeper_server.log_storage_path"))
        return std::filesystem::path(config.getString("keeper_server.log_storage_path")).parent_path() / "state";

    if (standalone_keeper)
        return std::filesystem::path{config.getString("path", KEEPER_DEFAULT_PATH)} / "state";
    else
        return std::filesystem::path{config.getString("path", DBMS_DEFAULT_PATH)} / "coordination/state";
}

}
