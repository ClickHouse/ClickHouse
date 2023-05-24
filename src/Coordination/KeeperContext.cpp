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

    log_storage = getLogsPathFromConfig(config);

    if (config.has("keeper_server.current_log_storage_disk"))
        current_log_storage = config.getString("keeper_server.current_log_storage_disk");
    else
        current_log_storage = log_storage;

    snapshot_storage = getSnapshotsPathFromConfig(config);

    state_file_storage = getStatePathFromConfig(config);
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

DiskPtr KeeperContext::getDisk(const Storage & storage) const
{
    if (const auto * storage_disk = std::get_if<DiskPtr>(&storage))
        return *storage_disk;

    const auto & disk_name = std::get<std::string>(storage);
    return disk_selector->get(disk_name);
}

DiskPtr KeeperContext::getLogDisk() const
{
    return getDisk(log_storage);
}

DiskPtr KeeperContext::getCurrentLogDisk() const
{
    return getDisk(current_log_storage);
}

void KeeperContext::setLogDisk(DiskPtr disk)
{
    log_storage = disk;
    current_log_storage = std::move(disk);
}

DiskPtr KeeperContext::getSnapshotDisk() const
{
    return getDisk(snapshot_storage);
}

void KeeperContext::setSnapshotDisk(DiskPtr disk)
{
    snapshot_storage = std::move(disk);
}

DiskPtr KeeperContext::getStateFileDisk() const
{
    return getDisk(state_file_storage);
}

void KeeperContext::setStateFileDisk(DiskPtr disk)
{
    state_file_storage = std::move(disk);
}

KeeperContext::Storage KeeperContext::getLogsPathFromConfig(const Poco::Util::AbstractConfiguration & config) const
{
    const auto create_local_disk = [](const auto & path)
    {
        if (!fs::exists(path))
            fs::create_directories(path);

        return std::make_shared<DiskLocal>("LogDisk", path, 0);
    };

    /// the most specialized path
    if (config.has("keeper_server.log_storage_path"))
        return create_local_disk(config.getString("keeper_server.log_storage_path"));

    if (config.has("keeper_server.log_storage_disk"))
        return config.getString("keeper_server.log_storage_disk");

    if (config.has("keeper_server.storage_path"))
        return create_local_disk(std::filesystem::path{config.getString("keeper_server.storage_path")} / "logs");

    if (standalone_keeper)
        return create_local_disk(std::filesystem::path{config.getString("path", KEEPER_DEFAULT_PATH)} / "logs");
    else
        return create_local_disk(std::filesystem::path{config.getString("path", DBMS_DEFAULT_PATH)} / "coordination/logs");
}

KeeperContext::Storage KeeperContext::getSnapshotsPathFromConfig(const Poco::Util::AbstractConfiguration & config) const
{
    const auto create_local_disk = [](const auto & path)
    {
        if (!fs::exists(path))
            fs::create_directories(path);

        return std::make_shared<DiskLocal>("SnapshotDisk", path, 0);
    };

    /// the most specialized path
    if (config.has("keeper_server.snapshot_storage_path"))
        return create_local_disk(config.getString("keeper_server.snapshot_storage_path"));

    if (config.has("keeper_server.snapshot_storage_disk"))
        return config.getString("keeper_server.snapshot_storage_disk");

    if (config.has("keeper_server.storage_path"))
        return create_local_disk(std::filesystem::path{config.getString("keeper_server.storage_path")} / "snapshots");

    if (standalone_keeper)
        return create_local_disk(std::filesystem::path{config.getString("path", KEEPER_DEFAULT_PATH)} / "snapshots");
    else
        return create_local_disk(std::filesystem::path{config.getString("path", DBMS_DEFAULT_PATH)} / "coordination/snapshots");
}

KeeperContext::Storage KeeperContext::getStatePathFromConfig(const Poco::Util::AbstractConfiguration & config) const
{
    const auto create_local_disk = [](const auto & path)
    {
        if (!fs::exists(path))
            fs::create_directories(path);

        return std::make_shared<DiskLocal>("SnapshotDisk", path, 0);
    };

    if (config.has("keeper_server.state_storage_disk"))
        return config.getString("keeper_server.state_storage_disk");

    if (config.has("keeper_server.storage_path"))
        return create_local_disk(std::filesystem::path{config.getString("keeper_server.storage_path")});

    if (config.has("keeper_server.snapshot_storage_path"))
        return create_local_disk(std::filesystem::path(config.getString("keeper_server.snapshot_storage_path")).parent_path());

    if (config.has("keeper_server.log_storage_path"))
        return create_local_disk(std::filesystem::path(config.getString("keeper_server.log_storage_path")).parent_path());

    if (standalone_keeper)
        return create_local_disk(std::filesystem::path{config.getString("path", KEEPER_DEFAULT_PATH)});
    else
        return create_local_disk(std::filesystem::path{config.getString("path", DBMS_DEFAULT_PATH)} / "coordination");
}

}
