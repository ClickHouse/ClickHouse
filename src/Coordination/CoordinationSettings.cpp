#include <Coordination/CoordinationSettings.h>
#include <Core/BaseSettings.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteIntText.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

/** These settings represent fine tunes for internal details of Coordination storages
  * and should not be changed by the user without a reason.
  */
#define LIST_OF_COORDINATION_SETTINGS(M, ALIAS) \
    M(Milliseconds, min_session_timeout_ms, Coordination::DEFAULT_MIN_SESSION_TIMEOUT_MS, "Min client session timeout", 0) \
    M(Milliseconds, session_timeout_ms, Coordination::DEFAULT_MAX_SESSION_TIMEOUT_MS, "Max client session timeout", 0) \
    M(Milliseconds, operation_timeout_ms, Coordination::DEFAULT_OPERATION_TIMEOUT_MS, "Default client operation timeout", 0) \
    M(Milliseconds, dead_session_check_period_ms, 500, "How often leader will check sessions to consider them dead and remove", 0) \
    M(Milliseconds, heart_beat_interval_ms, 500, "Heartbeat interval between quorum nodes", 0) \
    M(Milliseconds, election_timeout_lower_bound_ms, 1000, "Lower bound of election timer (avoid too often leader elections)", 0) \
    M(Milliseconds, election_timeout_upper_bound_ms, 2000, "Upper bound of election timer (avoid too often leader elections)", 0) \
    M(Milliseconds, leadership_expiry_ms, 0, "Duration after which a leader will expire if it fails to receive responses from peers. Set it lower or equal to election_timeout_lower_bound_ms to avoid multiple leaders.", 0) \
    M(UInt64, reserved_log_items, 100000, "How many log items to store (don't remove during compaction)", 0) \
    M(UInt64, snapshot_distance, 100000, "How many log items we have to collect to write new snapshot", 0) \
    M(Bool, auto_forwarding, true, "Allow to forward write requests from followers to leader", 0) \
    M(Milliseconds, shutdown_timeout, 5000, "How much time we will wait until RAFT shutdown", 0) \
    M(Milliseconds, session_shutdown_timeout, 10000, "How much time we will wait until sessions are closed during shutdown", 0) \
    M(Milliseconds, startup_timeout, 180000, "How much time we will wait until RAFT to start.", 0) \
    M(Milliseconds, sleep_before_leader_change_ms, 8000, "How much time we will wait before removing leader (so as leader could commit accepted but non-committed commands and they won't be lost -- leader removal is not synchronized with committing)", 0) \
    M(LogsLevel, raft_logs_level, LogsLevel::information, "Log internal RAFT logs into main server log level. Valid values: 'trace', 'debug', 'information', 'warning', 'error', 'fatal', 'none'", 0) \
    M(UInt64, rotate_log_storage_interval, 100000, "How many records will be stored in one log storage file", 0) \
    M(UInt64, snapshots_to_keep, 3, "How many compressed snapshots to keep on disk", 0) \
    M(UInt64, stale_log_gap, 10000, "When node became stale and should receive snapshots from leader", 0) \
    M(UInt64, fresh_log_gap, 200, "When node became fresh", 0) \
    M(UInt64, max_request_queue_size, 100000, "Maximum number of request that can be in queue for processing", 0) \
    M(UInt64, max_requests_batch_size, 100, "Max size of batch of requests that can be sent to RAFT", 0) \
    M(UInt64, max_requests_batch_bytes_size, 100*1024, "Max size in bytes of batch of requests that can be sent to RAFT", 0) \
    M(UInt64, max_requests_append_size, 100, "Max size of batch of requests that can be sent to replica in append request", 0) \
    M(UInt64, max_flush_batch_size, 1000, "Max size of batch of requests that can be flushed together", 0) \
    M(UInt64, max_requests_quick_batch_size, 100, "Max size of batch of requests to try to get before proceeding with RAFT. Keeper will not wait for requests but take only requests that are already in queue" , 0) \
    M(Bool, quorum_reads, false, "Execute read requests as writes through whole RAFT consesus with similar speed", 0) \
    M(Bool, force_sync, true, "Call fsync on each change in RAFT changelog", 0) \
    M(Bool, compress_logs, false, "Write compressed coordination logs in ZSTD format", 0) \
    M(Bool, compress_snapshots_with_zstd_format, true, "Write compressed snapshots in ZSTD format (instead of custom LZ4)", 0) \
    M(UInt64, configuration_change_tries_count, 20, "How many times we will try to apply configuration change (add/remove server) to the cluster", 0) \
    M(UInt64, max_log_file_size, 50 * 1024 * 1024, "Max size of the Raft log file. If possible, each created log file will preallocate this amount of bytes on disk. Set to 0 to disable the limit", 0) \
    M(UInt64, log_file_overallocate_size, 50 * 1024 * 1024, "If max_log_file_size is not set to 0, this value will be added to it for preallocating bytes on disk. If a log record is larger than this value, it could lead to uncaught out-of-space issues so a larger value is preferred", 0) \
    M(UInt64, min_request_size_for_cache, 50 * 1024, "Minimal size of the request to cache the deserialization result. Caching can have negative effect on latency for smaller requests, set to 0 to disable", 0) \
    M(UInt64, raft_limits_reconnect_limit, 50, "If connection to a peer is silent longer than this limit * (multiplied by heartbeat interval), we re-establish the connection.", 0) \
    M(UInt64, raft_limits_response_limit, 20, "Total wait time for a response is calculated by multiplying response_limit with heart_beat_interval_ms", 0) \
    M(Bool, async_replication, false, "Enable async replication. All write and read guarantees are preserved while better performance is achieved. Settings is disabled by default to not break backwards compatibility.", 0) \
    M(Bool, experimental_use_rocksdb, false, "Use rocksdb as backend storage", 0) \
    M(UInt64, latest_logs_cache_size_threshold, 1 * 1024 * 1024 * 1024, "Maximum total size of in-memory cache of latest log entries.", 0) \
    M(UInt64, commit_logs_cache_size_threshold, 500 * 1024 * 1024, "Maximum total size of in-memory cache of log entries needed next for commit.", 0) \
    M(UInt64, disk_move_retries_wait_ms, 1000, "How long to wait between retries after a failure which happened while a file was being moved between disks.", 0) \
    M(UInt64, disk_move_retries_during_init, 100, "The amount of retries after a failure which happened while a file was being moved between disks during initialization.", 0) \
    M(UInt64, log_slow_total_threshold_ms, 5000, "Requests for which the total latency is larger than this settings will be logged", 0) \
    M(UInt64, log_slow_cpu_threshold_ms, 100, "Requests for which the CPU (preprocessing and processing) latency is larger than this settings will be logged", 0) \
    M(UInt64, log_slow_connection_operation_threshold_ms, 1000, "Log message if a certain operation took too long inside a single connection", 0)

DECLARE_SETTINGS_TRAITS(CoordinationSettingsTraits, LIST_OF_COORDINATION_SETTINGS)

struct CoordinationSettingsImpl : public BaseSettings<CoordinationSettingsTraits>
{
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);
};

IMPLEMENT_SETTINGS_TRAITS(CoordinationSettingsTraits, LIST_OF_COORDINATION_SETTINGS)

void CoordinationSettingsImpl::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config)
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

    /// for backwards compatibility we set max_requests_append_size to max_requests_batch_size
    /// if max_requests_append_size was not changed
    if (!max_requests_append_size.changed)
        max_requests_append_size = max_requests_batch_size;
}

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) \
    CoordinationSettings##TYPE NAME = &CoordinationSettings##Impl ::NAME;

namespace CoordinationSetting
{
LIST_OF_COORDINATION_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

CoordinationSettings::CoordinationSettings() : impl(std::make_unique<CoordinationSettingsImpl>())
{
}

CoordinationSettings::CoordinationSettings(const CoordinationSettings & settings)
    : impl(std::make_unique<CoordinationSettingsImpl>(*settings.impl))
{
}

CoordinationSettings::~CoordinationSettings() = default;

#define IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR(CLASS_NAME, TYPE) \
    const SettingField##TYPE & CoordinationSettings::operator[](CLASS_NAME##TYPE t) const \
    { \
        return impl.get()->*t; \
    } \
    SettingField##TYPE & CoordinationSettings::operator[](CLASS_NAME##TYPE t) \
    { \
        return impl.get()->*t; \
    }

COORDINATION_SETTINGS_SUPPORTED_TYPES(CoordinationSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)
#undef IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR

void CoordinationSettings::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    impl->loadFromConfig(config_elem, config);
}

const String KeeperConfigurationAndSettings::DEFAULT_FOUR_LETTER_WORD_CMD =
#if USE_JEMALLOC
"jmst,jmfp,jmep,jmdp,"
#endif
"conf,cons,crst,envi,ruok,srst,srvr,stat,wchs,dirs,mntr,isro,rcvr,apiv,csnp,lgif,rqld,rclc,clrs,ftfl,ydld,pfev";

KeeperConfigurationAndSettings::KeeperConfigurationAndSettings()
    : server_id(NOT_EXIST)
    , enable_ipv6(true)
    , tcp_port(NOT_EXIST)
    , tcp_port_secure(NOT_EXIST)
    , standalone_keeper(false)
    , coordination_settings()
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

    /// coordination_settings

    writeText("max_requests_batch_size=", buf);
    write_int(coordination_settings[CoordinationSetting::max_requests_batch_size]);
    writeText("min_session_timeout_ms=", buf);
    write_int(static_cast<uint64_t>(coordination_settings[CoordinationSetting::min_session_timeout_ms]));
    writeText("session_timeout_ms=", buf);
    write_int(static_cast<uint64_t>(coordination_settings[CoordinationSetting::session_timeout_ms]));
    writeText("operation_timeout_ms=", buf);
    write_int(static_cast<uint64_t>(coordination_settings[CoordinationSetting::operation_timeout_ms]));
    writeText("dead_session_check_period_ms=", buf);
    write_int(static_cast<uint64_t>(coordination_settings[CoordinationSetting::dead_session_check_period_ms]));

    writeText("heart_beat_interval_ms=", buf);
    write_int(static_cast<uint64_t>(coordination_settings[CoordinationSetting::heart_beat_interval_ms]));
    writeText("election_timeout_lower_bound_ms=", buf);
    write_int(static_cast<uint64_t>(coordination_settings[CoordinationSetting::election_timeout_lower_bound_ms]));
    writeText("election_timeout_upper_bound_ms=", buf);
    write_int(static_cast<uint64_t>(coordination_settings[CoordinationSetting::election_timeout_upper_bound_ms]));
    writeText("leadership_expiry_ms=", buf);
    write_int(static_cast<uint64_t>(coordination_settings[CoordinationSetting::leadership_expiry_ms]));

    writeText("reserved_log_items=", buf);
    write_int(coordination_settings[CoordinationSetting::reserved_log_items]);
    writeText("snapshot_distance=", buf);
    write_int(coordination_settings[CoordinationSetting::snapshot_distance]);

    writeText("auto_forwarding=", buf);
    write_bool(coordination_settings[CoordinationSetting::auto_forwarding]);
    writeText("shutdown_timeout=", buf);
    write_int(static_cast<uint64_t>(coordination_settings[CoordinationSetting::shutdown_timeout]));
    writeText("startup_timeout=", buf);
    write_int(static_cast<uint64_t>(coordination_settings[CoordinationSetting::startup_timeout]));

    writeText("raft_logs_level=", buf);
    writeText(coordination_settings[CoordinationSetting::raft_logs_level].toString(), buf);
    buf.write('\n');

    writeText("snapshots_to_keep=", buf);
    write_int(coordination_settings[CoordinationSetting::snapshots_to_keep]);
    writeText("rotate_log_storage_interval=", buf);
    write_int(coordination_settings[CoordinationSetting::rotate_log_storage_interval]);
    writeText("stale_log_gap=", buf);
    write_int(coordination_settings[CoordinationSetting::stale_log_gap]);
    writeText("fresh_log_gap=", buf);
    write_int(coordination_settings[CoordinationSetting::fresh_log_gap]);

    writeText("max_requests_batch_size=", buf);
    write_int(coordination_settings[CoordinationSetting::max_requests_batch_size]);
    writeText("max_requests_batch_bytes_size=", buf);
    write_int(coordination_settings[CoordinationSetting::max_requests_batch_bytes_size]);
    writeText("max_flush_batch_size=", buf);
    write_int(coordination_settings[CoordinationSetting::max_flush_batch_size]);
    writeText("max_request_queue_size=", buf);
    write_int(coordination_settings[CoordinationSetting::max_request_queue_size]);
    writeText("max_requests_quick_batch_size=", buf);
    write_int(coordination_settings[CoordinationSetting::max_requests_quick_batch_size]);
    writeText("quorum_reads=", buf);
    write_bool(coordination_settings[CoordinationSetting::quorum_reads]);
    writeText("force_sync=", buf);
    write_bool(coordination_settings[CoordinationSetting::force_sync]);

    writeText("compress_logs=", buf);
    write_bool(coordination_settings[CoordinationSetting::compress_logs]);
    writeText("compress_snapshots_with_zstd_format=", buf);
    write_bool(coordination_settings[CoordinationSetting::compress_snapshots_with_zstd_format]);
    writeText("configuration_change_tries_count=", buf);
    write_int(coordination_settings[CoordinationSetting::configuration_change_tries_count]);

    writeText("raft_limits_reconnect_limit=", buf);
    write_int(static_cast<uint64_t>(coordination_settings[CoordinationSetting::raft_limits_reconnect_limit]));

    writeText("async_replication=", buf);
    write_bool(coordination_settings[CoordinationSetting::async_replication]);

    writeText("latest_logs_cache_size_threshold=", buf);
    write_int(coordination_settings[CoordinationSetting::latest_logs_cache_size_threshold]);
    writeText("commit_logs_cache_size_threshold=", buf);
    write_int(coordination_settings[CoordinationSetting::commit_logs_cache_size_threshold]);

    writeText("disk_move_retries_wait_ms=", buf);
    write_int(coordination_settings[CoordinationSetting::disk_move_retries_wait_ms]);
    writeText("disk_move_retries_during_init=", buf);
    write_int(coordination_settings[CoordinationSetting::disk_move_retries_during_init]);

    writeText("log_slow_total_threshold_ms=", buf);
    write_int(coordination_settings[CoordinationSetting::log_slow_total_threshold_ms]);
    writeText("log_slow_cpu_threshold_ms=", buf);
    write_int(coordination_settings[CoordinationSetting::log_slow_cpu_threshold_ms]);
    writeText("log_slow_connection_operation_threshold_ms=", buf);
    write_int(coordination_settings[CoordinationSetting::log_slow_connection_operation_threshold_ms]);
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


    ret->coordination_settings.loadFromConfig("keeper_server.coordination_settings", config);

    return ret;
}

}
