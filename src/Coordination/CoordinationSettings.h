#pragma once

#include <Core/Defines.h>
#include <Core/BaseSettings.h>
#include <Core/SettingsEnums.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

struct Settings;


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
    M(UInt64, reserved_log_items, 100000, "How many log items to store (don't remove during compaction)", 0) \
    M(UInt64, snapshot_distance, 100000, "How many log items we have to collect to write new snapshot", 0) \
    M(Bool, auto_forwarding, true, "Allow to forward write requests from followers to leader", 0) \
    M(Milliseconds, shutdown_timeout, 5000, "How much time we will wait until RAFT shutdown", 0) \
    M(Milliseconds, session_shutdown_timeout, 10000, "How much time we will wait until sessions are closed during shutdown", 0) \
    M(Milliseconds, startup_timeout, 180000, "How much time we will wait until RAFT to start.", 0) \
    M(LogsLevel, raft_logs_level, LogsLevel::information, "Log internal RAFT logs into main server log level. Valid values: 'trace', 'debug', 'information', 'warning', 'error', 'fatal', 'none'", 0) \
    M(UInt64, rotate_log_storage_interval, 100000, "How many records will be stored in one log storage file", 0) \
    M(UInt64, snapshots_to_keep, 3, "How many compressed snapshots to keep on disk", 0) \
    M(UInt64, stale_log_gap, 10000, "When node became stale and should receive snapshots from leader", 0) \
    M(UInt64, fresh_log_gap, 200, "When node became fresh", 0) \
    M(UInt64, max_request_queue_size, 100000, "Maximum number of request that can be in queue for processing", 0) \
    M(UInt64, max_requests_batch_size, 100, "Max size of batch of requests that can be sent to RAFT", 0) \
    M(UInt64, max_requests_batch_bytes_size, 100*1024, "Max size in bytes of batch of requests that can be sent to RAFT", 0) \
    M(UInt64, max_requests_quick_batch_size, 100, "Max size of batch of requests to try to get before proceeding with RAFT. Keeper will not wait for requests but take only requests that are already in queue" , 0) \
    M(Bool, quorum_reads, false, "Execute read requests as writes through whole RAFT consesus with similar speed", 0) \
    M(Bool, force_sync, true, "Call fsync on each change in RAFT changelog", 0) \
    M(Bool, compress_logs, true, "Write compressed coordination logs in ZSTD format", 0) \
    M(Bool, compress_snapshots_with_zstd_format, true, "Write compressed snapshots in ZSTD format (instead of custom LZ4)", 0) \
    M(UInt64, configuration_change_tries_count, 20, "How many times we will try to apply configuration change (add/remove server) to the cluster", 0) \
    M(UInt64, max_log_file_size, 50 * 1024 * 1024, "Max size of the Raft log file. If possible, each created log file will preallocate this amount of bytes on disk. Set to 0 to disable the limit", 0) \
    M(UInt64, log_file_overallocate_size, 50 * 1024 * 1024, "If max_log_file_size is not set to 0, this value will be added to it for preallocating bytes on disk. If a log record is larger than this value, it could lead to uncaught out-of-space issues so a larger value is preferred", 0) \
    M(UInt64, min_request_size_for_cache, 50 * 1024, "Minimal size of the request to cache the deserialization result. Caching can have negative effect on latency for smaller requests, set to 0 to disable", 0) \
    M(UInt64, raft_limits_reconnect_limit, 50, "If connection to a peer is silent longer than this limit * (multiplied by heartbeat interval), we re-establish the connection.", 0)

DECLARE_SETTINGS_TRAITS(CoordinationSettingsTraits, LIST_OF_COORDINATION_SETTINGS)


struct CoordinationSettings : public BaseSettings<CoordinationSettingsTraits>
{
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);
};

using CoordinationSettingsPtr = std::shared_ptr<CoordinationSettings>;

/// Coordination settings + some other parts of keeper configuration
/// which are not stored in settings. Allows to dump configuration
/// with 4lw commands.
struct KeeperConfigurationAndSettings
{
    static constexpr int NOT_EXIST = -1;
    static const String DEFAULT_FOUR_LETTER_WORD_CMD;

    KeeperConfigurationAndSettings();
    int server_id;

    bool enable_ipv6;
    int tcp_port;
    int tcp_port_secure;

    String four_letter_word_allow_list;

    String super_digest;

    bool standalone_keeper;
    CoordinationSettingsPtr coordination_settings;

    void dump(WriteBufferFromOwnString & buf) const;
    static std::shared_ptr<KeeperConfigurationAndSettings> loadFromConfig(const Poco::Util::AbstractConfiguration & config, bool standalone_keeper_);
};

using KeeperConfigurationAndSettingsPtr = std::shared_ptr<KeeperConfigurationAndSettings>;

}
