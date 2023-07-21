#include <Coordination/KeeperContext.h>

#include <Coordination/Defines.h>
#include <Disks/DiskLocal.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Coordination/KeeperConstants.h>
#include <Common/logger_useful.h>
#include <Coordination/KeeperFeatureFlags.h>
#include <boost/algorithm/string.hpp>

namespace DB
{

namespace ErrorCodes
{

extern const int BAD_ARGUMENTS;

}

KeeperContext::KeeperContext(bool standalone_keeper_)
    : disk_selector(std::make_shared<DiskSelector>())
    , standalone_keeper(standalone_keeper_)
{
    /// enable by default some feature flags
    feature_flags.enableFeatureFlag(KeeperFeatureFlag::FILTERED_LIST);
    feature_flags.enableFeatureFlag(KeeperFeatureFlag::MULTI_READ);
    system_nodes_with_data[keeper_api_feature_flags_path] = feature_flags.getFeatureFlags();

    /// for older clients, the default is equivalent to WITH_MULTI_READ version
    system_nodes_with_data[keeper_api_version_path] = toString(static_cast<uint8_t>(KeeperApiVersion::WITH_MULTI_READ));
}

void KeeperContext::initialize(const Poco::Util::AbstractConfiguration & config)
{
    digest_enabled = config.getBool("keeper_server.digest_enabled", false);
    ignore_system_path_on_startup = config.getBool("keeper_server.ignore_system_path_on_startup", false);

    initializeFeatureFlags(config);
    initializeDisks(config);
}

namespace
{

bool diskValidator(const Poco::Util::AbstractConfiguration & config, const std::string & disk_config_prefix)
{
    const auto disk_type = config.getString(disk_config_prefix + ".type", "local");

    using namespace std::literals;
    static constexpr std::array supported_disk_types
    {
        "s3"sv,
        "s3_plain"sv,
        "local"sv
    };

    if (std::all_of(
            supported_disk_types.begin(),
            supported_disk_types.end(),
            [&](const auto supported_type) { return disk_type != supported_type; }))
    {
        LOG_INFO(&Poco::Logger::get("KeeperContext"), "Disk type '{}' is not supported for Keeper", disk_type);
        return false;
    }

    return true;
}

}

void KeeperContext::initializeDisks(const Poco::Util::AbstractConfiguration & config)
{
    disk_selector->initialize(config, "storage_configuration.disks", Context::getGlobalContextInstance(), diskValidator);

    log_storage = getLogsPathFromConfig(config);

    if (config.has("keeper_server.latest_log_storage_disk"))
        latest_log_storage = config.getString("keeper_server.latest_log_storage_disk");
    else
        latest_log_storage = log_storage;

    const auto collect_old_disk_names = [&](const std::string_view key_prefix, std::vector<std::string> & disk_names)
    {
        Poco::Util::AbstractConfiguration::Keys disk_name_keys;
        config.keys("keeper_server", disk_name_keys);
        for (const auto & key : disk_name_keys)
        {
            if (key.starts_with(key_prefix))
                disk_names.push_back(config.getString(fmt::format("keeper_server.{}", key)));
        }
    };

    collect_old_disk_names("old_log_storage_disk", old_log_disk_names);
    collect_old_disk_names("old_snapshot_storage_disk", old_snapshot_disk_names);

    snapshot_storage = getSnapshotsPathFromConfig(config);

    if (config.has("keeper_server.latest_snapshot_storage_disk"))
        latest_snapshot_storage = config.getString("keeper_server.latest_snapshot_storage_disk");
    else
        latest_snapshot_storage = snapshot_storage;

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

std::vector<DiskPtr> KeeperContext::getOldLogDisks() const
{
    std::vector<DiskPtr> old_log_disks;
    old_log_disks.reserve(old_log_disk_names.size());

    for (const auto & disk_name : old_log_disk_names)
        old_log_disks.push_back(disk_selector->get(disk_name));

    return old_log_disks;
}

DiskPtr KeeperContext::getLatestLogDisk() const
{
    return getDisk(latest_log_storage);
}

void KeeperContext::setLogDisk(DiskPtr disk)
{
    log_storage = disk;
    latest_log_storage = std::move(disk);
}

DiskPtr KeeperContext::getLatestSnapshotDisk() const
{
    return getDisk(latest_snapshot_storage);
}

DiskPtr KeeperContext::getSnapshotDisk() const
{
    return getDisk(snapshot_storage);
}

std::vector<DiskPtr> KeeperContext::getOldSnapshotDisks() const
{
    std::vector<DiskPtr> old_snapshot_disks;
    old_snapshot_disks.reserve(old_snapshot_disk_names.size());

    for (const auto & disk_name : old_snapshot_disk_names)
        old_snapshot_disks.push_back(disk_selector->get(disk_name));

    return old_snapshot_disks;
}

void KeeperContext::setSnapshotDisk(DiskPtr disk)
{
    snapshot_storage = std::move(disk);
    latest_snapshot_storage = snapshot_storage;
}

DiskPtr KeeperContext::getStateFileDisk() const
{
    return getDisk(state_file_storage);
}

void KeeperContext::setStateFileDisk(DiskPtr disk)
{
    state_file_storage = std::move(disk);
}

const std::unordered_map<std::string, std::string> & KeeperContext::getSystemNodesWithData() const
{
    return system_nodes_with_data;
}

const KeeperFeatureFlags & KeeperContext::getFeatureFlags() const
{
    return feature_flags;
}

void KeeperContext::dumpConfiguration(WriteBufferFromOwnString & buf) const
{
    auto dump_disk_info = [&](const std::string_view prefix, const IDisk & disk)
    {
        writeText(fmt::format("{}_path=", prefix), buf);
        writeText(disk.getPath(), buf);
        buf.write('\n');

        writeText(fmt::format("{}_disk=", prefix), buf);
        writeText(disk.getName(), buf);
        buf.write('\n');

    };

    {
        auto log_disk = getDisk(log_storage);
        dump_disk_info("log_storage", *log_disk);

        auto latest_log_disk = getDisk(latest_log_storage);
        if (log_disk != latest_log_disk)
            dump_disk_info("latest_log_storage", *latest_log_disk);
    }

    {
        auto snapshot_disk = getDisk(snapshot_storage);
        dump_disk_info("snapshot_storage", *snapshot_disk);
    }
}

KeeperContext::Storage KeeperContext::getLogsPathFromConfig(const Poco::Util::AbstractConfiguration & config) const
{
    const auto create_local_disk = [](const auto & path)
    {
        if (!fs::exists(path))
            fs::create_directories(path);

        return std::make_shared<DiskLocal>("LocalLogDisk", path);
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

        return std::make_shared<DiskLocal>("LocalSnapshotDisk", path);
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

        return std::make_shared<DiskLocal>("LocalStateFileDisk", path);
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

void KeeperContext::initializeFeatureFlags(const Poco::Util::AbstractConfiguration & config)
{
    static const std::string feature_flags_key = "keeper_server.feature_flags";
    if (config.has(feature_flags_key))
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(feature_flags_key, keys);
        for (const auto & key : keys)
        {
            auto feature_flag_string = boost::to_upper_copy(key);
            auto feature_flag = magic_enum::enum_cast<KeeperFeatureFlag>(feature_flag_string);

            if (!feature_flag.has_value())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid feature flag defined in config for Keeper: {}", key);

            auto is_enabled = config.getBool(feature_flags_key + "." + key);
            if (is_enabled)
                feature_flags.enableFeatureFlag(feature_flag.value());
            else
                feature_flags.disableFeatureFlag(feature_flag.value());
        }

        system_nodes_with_data[keeper_api_feature_flags_path] = feature_flags.getFeatureFlags();
    }

    feature_flags.logFlags(&Poco::Logger::get("KeeperContext"));
}

}
