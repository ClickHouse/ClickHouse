#include <Coordination/CoordinationSettings.h>
#include <Core/Settings.h>
#include <base/logger_useful.h>
#include <filesystem>
#include <Coordination/Defines.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteIntText.h>

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


const String KeeperConfigurationAndSettings::DEFAULT_FOUR_LETTER_WORD_CMD = "conf,cons,crst,envi,ruok,srst,srvr,stat,wchs,dirs,mntr,isro";

KeeperConfigurationAndSettings::KeeperConfigurationAndSettings()
    : server_id(NOT_EXIST)
    , enable_ipv6(true)
    , tcp_port(NOT_EXIST)
    , tcp_port_secure(NOT_EXIST)
    , standalone_keeper(false)
    , coordination_settings(std::make_shared<CoordinationSettings>())
{
}


void KeeperConfigurationAndSettings::dump(WriteBufferFromOwnString & buf) const
{
    auto write_int = [&buf](int64_t value)
    {
        writeIntText(value, buf);
        buf.write('\n');
    };

    auto write_bool = [&buf](bool value)
    {
        String str_val = value ? "true" : "false";
        writeText(str_val, buf);
        buf.write('\n');
    };

    writeText("server_id=", buf);
    write_int(server_id);

    writeText("enable_ipv6=", buf);
    write_bool(enable_ipv6);

    if (tcp_port != NOT_EXIST)
    {
        writeText("tcp_port=", buf);
        write_int(tcp_port);
    }
    if (tcp_port_secure != NOT_EXIST)
    {
        writeText("tcp_port_secure=", buf);
        write_int(tcp_port_secure);
    }

    writeText("four_letter_word_allow_list=", buf);
    writeText(four_letter_word_allow_list, buf);
    buf.write('\n');

    writeText("log_storage_path=", buf);
    writeText(log_storage_path, buf);
    buf.write('\n');

    writeText("snapshot_storage_path=", buf);
    writeText(snapshot_storage_path, buf);
    buf.write('\n');

    /// coordination_settings

    writeText("max_requests_batch_size=", buf);
    write_int(coordination_settings->max_requests_batch_size);
    writeText("min_session_timeout_ms=", buf);
    write_int(uint64_t(coordination_settings->min_session_timeout_ms));
    writeText("session_timeout_ms=", buf);
    write_int(uint64_t(coordination_settings->session_timeout_ms));
    writeText("operation_timeout_ms=", buf);
    write_int(uint64_t(coordination_settings->operation_timeout_ms));
    writeText("dead_session_check_period_ms=", buf);
    write_int(uint64_t(coordination_settings->dead_session_check_period_ms));

    writeText("heart_beat_interval_ms=", buf);
    write_int(uint64_t(coordination_settings->heart_beat_interval_ms));
    writeText("election_timeout_lower_bound_ms=", buf);
    write_int(uint64_t(coordination_settings->election_timeout_lower_bound_ms));
    writeText("election_timeout_upper_bound_ms=", buf);
    write_int(uint64_t(coordination_settings->election_timeout_upper_bound_ms));

    writeText("reserved_log_items=", buf);
    write_int(coordination_settings->reserved_log_items);
    writeText("snapshot_distance=", buf);
    write_int(coordination_settings->snapshot_distance);

    writeText("auto_forwarding=", buf);
    write_bool(coordination_settings->auto_forwarding);
    writeText("shutdown_timeout=", buf);
    write_int(uint64_t(coordination_settings->shutdown_timeout));
    writeText("startup_timeout=", buf);
    write_int(uint64_t(coordination_settings->startup_timeout));

    writeText("raft_logs_level=", buf);
    writeText(coordination_settings->raft_logs_level.toString(), buf);
    buf.write('\n');

    writeText("snapshots_to_keep=", buf);
    write_int(coordination_settings->snapshots_to_keep);
    writeText("rotate_log_storage_interval=", buf);
    write_int(coordination_settings->rotate_log_storage_interval);
    writeText("stale_log_gap=", buf);
    write_int(coordination_settings->stale_log_gap);
    writeText("fresh_log_gap=", buf);
    write_int(coordination_settings->fresh_log_gap);

    writeText("max_requests_batch_size=", buf);
    write_int(coordination_settings->max_requests_batch_size);
    writeText("quorum_reads=", buf);
    write_bool(coordination_settings->quorum_reads);
    writeText("force_sync=", buf);
    write_bool(coordination_settings->force_sync);

    writeText("compress_logs=", buf);
    write_bool(coordination_settings->compress_logs);
    writeText("compress_snapshots_with_zstd_format=", buf);
    write_bool(coordination_settings->compress_snapshots_with_zstd_format);
    writeText("configuration_change_tries_count=", buf);
    write_int(coordination_settings->configuration_change_tries_count);
}

KeeperConfigurationAndSettingsPtr
KeeperConfigurationAndSettings::loadFromConfig(const Poco::Util::AbstractConfiguration & config, bool standalone_keeper_)
{
    std::shared_ptr<KeeperConfigurationAndSettings> ret = std::make_shared<KeeperConfigurationAndSettings>();

    ret->server_id = config.getInt("keeper_server.server_id");
    ret->standalone_keeper = standalone_keeper_;

    ret->enable_ipv6 = config.getBool("keeper_server.enable_ipv6", true);

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
        ret->super_digest = config.getString("keeper_server.superdigest");
    }

    ret->four_letter_word_allow_list = config.getString(
        "keeper_server.four_letter_word_allow_list",
        config.getString("keeper_server.four_letter_word_white_list",
                         DEFAULT_FOUR_LETTER_WORD_CMD));


    ret->log_storage_path = getLogsPathFromConfig(config, standalone_keeper_);
    ret->snapshot_storage_path = getSnapshotsPathFromConfig(config, standalone_keeper_);

    ret->coordination_settings->loadFromConfig("keeper_server.coordination_settings", config);

    return ret;
}

String KeeperConfigurationAndSettings::getLogsPathFromConfig(const Poco::Util::AbstractConfiguration & config, bool standalone_keeper_)
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

String KeeperConfigurationAndSettings::getSnapshotsPathFromConfig(const Poco::Util::AbstractConfiguration & config, bool standalone_keeper_)
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
