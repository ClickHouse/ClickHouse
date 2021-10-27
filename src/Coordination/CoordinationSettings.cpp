#include <Coordination/CoordinationSettings.h>
#include <Core/Settings.h>
#include <base/logger_useful.h>
#include <filesystem>
#include <Coordination/Defines.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

IMPLEMENT_SETTINGS_TRAITS(CoordinationSettingsTraits, LIST_OF_COORDINATION_SETTINGS)

void CoordinationSettings::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(config_elem))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    try
    {
        for (const String & key : config_keys)
            set(key, config.getString(config_elem + "." + key));
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::UNKNOWN_SETTING)
            e.addMessage("in Coordination settings config");
        throw;
    }
}

void KeeperSettings::dump(WriteBufferFromOwnString & buf) const
{
    auto write = [&buf](const String & content) { buf.write(content.data(), content.size()); };

    auto write_int = [&buf](Int64 value)
    {
        String str_val = std::to_string(value);
        buf.write(str_val.data(), str_val.size());
        buf.write('\n');
    };

    auto write_bool = [&buf](bool value)
    {
        String str_val = value ? "true" : "false";
        buf.write(str_val.data(), str_val.size());
        buf.write('\n');
    };

    write("server_id=");
    write_int(server_id);

    if (tcp_port != NO_PORT)
    {
        write("tcp_port=");
        write_int(tcp_port);
    }
    if (tcp_port_secure != NO_PORT)
    {
        write("tcp_port_secure=");
        write_int(tcp_port_secure);
    }
    if (!super_digest.empty())
    {
        write("superdigest=");
        write(super_digest);
        buf.write('\n');
    }

    write("log_storage_path=");
    write(log_storage_path);
    buf.write('\n');

    write("snapshot_storage_path=");
    write(snapshot_storage_path);
    buf.write('\n');

    /// coordination_settings

    write("max_requests_batch_size=");
    write_int(coordination_settings->max_requests_batch_size);
    write("session_timeout_ms=");
    write_int(UInt64(coordination_settings->session_timeout_ms));
    write("operation_timeout_ms=");
    write_int(UInt64(coordination_settings->operation_timeout_ms));
    write("dead_session_check_period_ms=");
    write_int(UInt64(coordination_settings->dead_session_check_period_ms));

    write("heart_beat_interval_ms=");
    write_int(UInt64(coordination_settings->heart_beat_interval_ms));
    write("election_timeout_lower_bound_ms=");
    write_int(UInt64(coordination_settings->election_timeout_lower_bound_ms));
    write("election_timeout_upper_bound_ms=");
    write_int(UInt64(coordination_settings->election_timeout_upper_bound_ms));

    write("reserved_log_items=");
    write_int(coordination_settings->reserved_log_items);
    write("snapshot_distance=");
    write_int(coordination_settings->snapshot_distance);

    write("auto_forwarding=");
    write_bool(coordination_settings->auto_forwarding);
    write("shutdown_timeout=");
    write_int(UInt64(coordination_settings->shutdown_timeout));
    write("startup_timeout=");
    write_int(UInt64(coordination_settings->startup_timeout));

    write("raft_logs_level=");
    write(coordination_settings->raft_logs_level.toString());
    buf.write('\n');

    write("snapshots_to_keep=");
    write_int(coordination_settings->snapshots_to_keep);
    write("rotate_log_storage_interval=");
    write_int(coordination_settings->rotate_log_storage_interval);
    write("stale_log_gap=");
    write_int(coordination_settings->stale_log_gap);
    write("fresh_log_gap=");
    write_int(coordination_settings->fresh_log_gap);

    write("max_requests_batch_size=");
    write_int(coordination_settings->max_requests_batch_size);
    write("quorum_reads=");
    write_bool(coordination_settings->quorum_reads);
    write("force_sync=");
    write_bool(coordination_settings->force_sync);

    write("compress_logs=");
    write_bool(coordination_settings->compress_logs);
    write("compress_snapshots_with_zstd_format=");
    write_bool(coordination_settings->compress_snapshots_with_zstd_format);
    write("configuration_change_tries_count=");
    write_int(coordination_settings->configuration_change_tries_count);
}

std::shared_ptr<KeeperSettings>
KeeperSettings::loadFromConfig(const Poco::Util::AbstractConfiguration & config, bool standalone_keeper_)
{
    std::shared_ptr<KeeperSettings> ret = std::make_shared<KeeperSettings>();

    ret->server_id = config.getInt("keeper_server.server_id");
    ret->standalone_keeper = standalone_keeper_;

    if (config.has("keeper_server.tcp_port"))
    {
        ret->tcp_port = config.getInt("keeper_server.tcp_port");
    }
    if (config.has("keeper_server.tcp_port_secure"))
    {
        ret->tcp_port_secure = config.getInt("keeper_server.tcp_port_secure");
    }
    if (config.has("keeper_server.superdigest"))
    {
        ret->super_digest = config.getString("keeper_server.tcp_port_secure");
    }

    ret->log_storage_path = getLogsPathFromConfig(config, standalone_keeper_);
    ret->snapshot_storage_path = getSnapshotsPathFromConfig(config, standalone_keeper_);

    ret->coordination_settings = std::make_shared<CoordinationSettings>();
    ret->coordination_settings->loadFromConfig("keeper_server.coordination_settings", config);

    return ret;
}

String KeeperSettings::getLogsPathFromConfig(const Poco::Util::AbstractConfiguration & config, bool standalone_keeper_)
{
    /// the most specialized path
    if (config.has("keeper_server.log_storage_path"))
        return config.getString("keeper_server.log_storage_path");

    if (config.has("keeper_server.storage_path"))
        return std::filesystem::path{config.getString("keeper_server.storage_path")} / "logs";

    if (standalone_keeper_)
        return std::filesystem::path{config.getString("path", KEEPER_DEFAULT_PATH)} / "logs";
    else
        return std::filesystem::path{config.getString("path", DBMS_DEFAULT_PATH)} / "coordination/logs";
}

String KeeperSettings::getSnapshotsPathFromConfig(const Poco::Util::AbstractConfiguration & config, bool standalone_keeper_)
{
    /// the most specialized path
    if (config.has("keeper_server.snapshot_storage_path"))
        return config.getString("keeper_server.snapshot_storage_path");

    if (config.has("keeper_server.storage_path"))
        return std::filesystem::path{config.getString("keeper_server.storage_path")} / "snapshots";

    if (standalone_keeper_)
        return std::filesystem::path{config.getString("path", KEEPER_DEFAULT_PATH)} / "snapshots";
    else
        return std::filesystem::path{config.getString("path", DBMS_DEFAULT_PATH)} / "coordination/snapshots";
}

}
