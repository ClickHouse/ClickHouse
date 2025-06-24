#include <atomic>
#include <chrono>

#include <Coordination/KeeperContext.h>

#include <Coordination/CoordinationSettings.h>
#include <Coordination/Defines.h>
#include <Disks/DiskLocal.h>
#include <Interpreters/Context.h>
#include <IO/S3/Credentials.h>
#include <IO/WriteHelpers.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/JSONConfiguration.h>
#include <Coordination/KeeperConstants.h>
#include <Server/CloudPlacementInfo.h>
#include <Coordination/KeeperFeatureFlags.h>
#include <Disks/DiskSelector.h>
#include <Common/logger_useful.h>

#include <boost/algorithm/string.hpp>

#include "config.h"
#if USE_ROCKSDB
#include <rocksdb/table.h>
#include <rocksdb/convenience.h>
#include <rocksdb/statistics.h>
#include <rocksdb/utilities/db_ttl.h>
#endif

namespace DB
{

namespace ErrorCodes
{

extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int ROCKSDB_ERROR;

}

KeeperContext::KeeperContext(bool standalone_keeper_, CoordinationSettingsPtr coordination_settings_)
    : disk_selector(std::make_shared<DiskSelector>())
    , standalone_keeper(standalone_keeper_)
    , coordination_settings(std::move(coordination_settings_))
{
    /// enable by default some feature flags
    feature_flags.enableFeatureFlag(KeeperFeatureFlag::FILTERED_LIST);
    feature_flags.enableFeatureFlag(KeeperFeatureFlag::MULTI_READ);
    system_nodes_with_data[keeper_api_feature_flags_path] = feature_flags.getFeatureFlags();

    /// for older clients, the default is equivalent to WITH_MULTI_READ version
    system_nodes_with_data[keeper_api_version_path] = toString(static_cast<uint8_t>(KeeperApiVersion::WITH_MULTI_READ));
}

#if USE_ROCKSDB
using RocksDBOptions = std::unordered_map<std::string, std::string>;

static RocksDBOptions getOptionsFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & path)
{
    RocksDBOptions options;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(path, keys);

    for (const auto & key : keys)
    {
        const String key_path = path + "." + key;
        options[key] = config.getString(key_path);
    }

    return options;
}

static rocksdb::Options getRocksDBOptionsFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    rocksdb::Status status;
    rocksdb::Options base;

    base.create_if_missing = true;
    base.compression = rocksdb::CompressionType::kZSTD;
    base.statistics = rocksdb::CreateDBStatistics();
    /// It is too verbose by default, and in fact we don't care about rocksdb logs at all.
    base.info_log_level = rocksdb::ERROR_LEVEL;

    rocksdb::Options merged = base;
    rocksdb::BlockBasedTableOptions table_options;

    if (config.has("keeper_server.rocksdb.options"))
    {
        auto config_options = getOptionsFromConfig(config, "keeper_server.rocksdb.options");
        status = rocksdb::GetDBOptionsFromMap({}, merged, config_options, &merged);
        if (!status.ok())
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to merge rocksdb options from 'rocksdb.options' : {}",
                status.ToString());
        }
    }
    if (config.has("rocksdb.column_family_options"))
    {
        auto column_family_options = getOptionsFromConfig(config, "rocksdb.column_family_options");
        status = rocksdb::GetColumnFamilyOptionsFromMap({}, merged, column_family_options, &merged);
        if (!status.ok())
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to merge rocksdb options from 'rocksdb.column_family_options' at: {}", status.ToString());
        }
    }
    if (config.has("rocksdb.block_based_table_options"))
    {
        auto block_based_table_options = getOptionsFromConfig(config, "rocksdb.block_based_table_options");
        status = rocksdb::GetBlockBasedTableOptionsFromMap({}, table_options, block_based_table_options, &table_options);
        if (!status.ok())
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to merge rocksdb options from 'rocksdb.block_based_table_options' at: {}", status.ToString());
        }
    }

    merged.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    return merged;
}
#endif

KeeperContext::Storage KeeperContext::getRocksDBPathFromConfig(const Poco::Util::AbstractConfiguration & config) const
{
    const auto create_local_disk = [](const auto & path)
    {
        if (fs::exists(path))
            fs::remove_all(path);
        fs::create_directories(path);

        return std::make_shared<DiskLocal>("LocalRocksDBDisk", path);
    };
    if (config.has("keeper_server.rocksdb_path"))
        return create_local_disk(config.getString("keeper_server.rocksdb_path"));

    if (config.has("keeper_server.storage_path"))
        return create_local_disk(std::filesystem::path{config.getString("keeper_server.storage_path")} / "rocksdb");

    if (standalone_keeper)
        return create_local_disk(std::filesystem::path{config.getString("path", KEEPER_DEFAULT_PATH)} / "rocksdb");
    else
        return create_local_disk(std::filesystem::path{config.getString("path", DBMS_DEFAULT_PATH)} / "coordination/rocksdb");
}

void KeeperContext::initialize(const Poco::Util::AbstractConfiguration & config, KeeperDispatcher * dispatcher_)
{
    dispatcher = dispatcher_;

    const auto keeper_az = PlacementInfo::PlacementInfo::instance().getAvailabilityZone();
    if (!keeper_az.empty())
    {
        system_nodes_with_data[keeper_availability_zone_path] = keeper_az;
        LOG_INFO(getLogger("KeeperContext"), "Initialize the KeeperContext with availability zone: '{}'", keeper_az);
    }

    updateKeeperMemorySoftLimit(config);

    digest_enabled = config.getBool("keeper_server.digest_enabled", false);
    ignore_system_path_on_startup = config.getBool("keeper_server.ignore_system_path_on_startup", false);

    initializeFeatureFlags(config);
    initializeDisks(config);

    #if USE_ROCKSDB
    if (config.getBool("keeper_server.coordination_settings.experimental_use_rocksdb", false))
    {
        rocksdb_options = std::make_shared<rocksdb::Options>(getRocksDBOptionsFromConfig(config));
        digest_enabled = false; /// TODO: support digest
    }
    #endif
}

namespace
{

bool diskValidator(const Poco::Util::AbstractConfiguration & config, const std::string & disk_config_prefix, const std::string &)
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
        LOG_INFO(getLogger("KeeperContext"), "Disk type '{}' is not supported for Keeper", disk_type);
        return false;
    }

    return true;
}

}

void KeeperContext::initializeDisks(const Poco::Util::AbstractConfiguration & config)
{
    disk_selector->initialize(config, "storage_configuration.disks", Context::getGlobalContextInstance(), diskValidator);

    rocksdb_storage = getRocksDBPathFromConfig(config);

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


void KeeperContext::setRocksDBDisk(DiskPtr disk)
{
    rocksdb_storage = std::move(disk);
}

DiskPtr KeeperContext::getTemporaryRocksDBDisk() const
{
    DiskPtr rocksdb_disk = getDisk(rocksdb_storage);
    if (!rocksdb_disk)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "rocksdb storage is not initialized");
    }
    auto uuid_str = formatUUID(UUIDHelpers::generateV4());
    String path_to_create = "rocks_" + std::string(uuid_str.data(), uuid_str.size());
    rocksdb_disk->createDirectory(path_to_create);
    return std::make_shared<DiskLocal>("LocalTmpRocksDBDisk", fullPath(rocksdb_disk, path_to_create));
}

void KeeperContext::setRocksDBOptions(std::shared_ptr<rocksdb::Options> rocksdb_options_)
{
    if (rocksdb_options_ != nullptr)
        rocksdb_options = rocksdb_options_;
    else
    {
        #if USE_ROCKSDB
        rocksdb_options = std::make_shared<rocksdb::Options>(getRocksDBOptionsFromConfig(Poco::Util::JSONConfiguration()));
        #endif
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

    feature_flags.logFlags(getLogger("KeeperContext"));
}

void KeeperContext::updateKeeperMemorySoftLimit(const Poco::Util::AbstractConfiguration & config)
{
    if (config.hasProperty("keeper_server.max_memory_usage_soft_limit"))
        memory_soft_limit = config.getUInt64("keeper_server.max_memory_usage_soft_limit");
}

bool KeeperContext::setShutdownCalled()
{
    std::unique_lock local_logs_preprocessed_lock(local_logs_preprocessed_cv_mutex);
    std::unique_lock last_committed_log_idx_lock(last_committed_log_idx_cv_mutex);

    if (!shutdown_called.exchange(true))
    {
        local_logs_preprocessed_lock.unlock();
        last_committed_log_idx_lock.unlock();

        local_logs_preprocessed_cv.notify_all();
        last_committed_log_idx_cv.notify_all();
        return true;
    }

    return false;
}

void KeeperContext::setLocalLogsPreprocessed()
{
    {
        std::lock_guard lock(local_logs_preprocessed_cv_mutex);
        local_logs_preprocessed = true;
    }
    local_logs_preprocessed_cv.notify_all();
}

bool KeeperContext::localLogsPreprocessed() const
{
    return local_logs_preprocessed;
}

void KeeperContext::waitLocalLogsPreprocessedOrShutdown()
{
    std::unique_lock lock(local_logs_preprocessed_cv_mutex);
    local_logs_preprocessed_cv.wait(lock, [this]{ return shutdown_called || local_logs_preprocessed; });
}

const CoordinationSettingsPtr & KeeperContext::getCoordinationSettings() const
{
    return coordination_settings;
}

uint64_t KeeperContext::lastCommittedIndex() const
{
    return last_committed_log_idx.load(std::memory_order_relaxed);
}

void KeeperContext::setLastCommitIndex(uint64_t commit_index)
{
    bool should_notify;
    {
        std::lock_guard lock(last_committed_log_idx_cv_mutex);
        last_committed_log_idx.store(commit_index, std::memory_order_relaxed);

        should_notify = wait_commit_upto_idx.has_value() && commit_index >= wait_commit_upto_idx;
    }

    if (should_notify)
        last_committed_log_idx_cv.notify_all();
}

bool KeeperContext::waitCommittedUpto(uint64_t log_idx, uint64_t wait_timeout_ms)
{
    std::unique_lock lock(last_committed_log_idx_cv_mutex);
    wait_commit_upto_idx = log_idx;
    bool success = last_committed_log_idx_cv.wait_for(
        lock,
        std::chrono::milliseconds(wait_timeout_ms),
        [&] { return shutdown_called || lastCommittedIndex() >= wait_commit_upto_idx; });

    wait_commit_upto_idx.reset();
    return success;
}

}
