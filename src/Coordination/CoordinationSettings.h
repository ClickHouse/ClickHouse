#pragma once

#include <Core/Defines.h>
#include <Core/BaseSettings.h>
#include <Core/SettingsEnums.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

struct Settings;

/** These settings represent fine tunes for internal details of Coordination storages
  * and should not be changed by the user without a reason.
  */

#define LIST_OF_COORDINATION_SETTINGS(M) \
    M(Milliseconds, session_timeout_ms, Coordination::DEFAULT_SESSION_TIMEOUT_MS, "Default client session timeout", 0) \
    M(Milliseconds, operation_timeout_ms, Coordination::DEFAULT_OPERATION_TIMEOUT_MS, "Default client operation timeout", 0) \
    M(Milliseconds, dead_session_check_period_ms, 500, "How often leader will check sessions to consider them dead and remove", 0) \
    M(Milliseconds, heart_beat_interval_ms, 500, "Heartbeat interval between quorum nodes", 0) \
    M(Milliseconds, election_timeout_lower_bound_ms, 1000, "Lower bound of election timer (avoid too often leader elections)", 0) \
    M(Milliseconds, election_timeout_upper_bound_ms, 2000, "Lower bound of election timer (avoid too often leader elections)", 0) \
    M(UInt64, reserved_log_items, 100000, "How many log items to store (don't remove during compaction)", 0) \
    M(UInt64, snapshot_distance, 100000, "How many log items we have to collect to write new snapshot", 0) \
    M(Bool, auto_forwarding, true, "Allow to forward write requests from followers to leader", 0) \
    M(Milliseconds, shutdown_timeout, 5000, "How many time we will until RAFT shutdown", 0) \
    M(Milliseconds, startup_timeout, 30000, "How many time we will until RAFT to start", 0) \
    M(LogsLevel, raft_logs_level, LogsLevel::information, "Log internal RAFT logs into main server log level. Valid values: 'trace', 'debug', 'information', 'warning', 'error', 'fatal', 'none'", 0) \
    M(UInt64, rotate_log_storage_interval, 100000, "How many records will be stored in one log storage file", 0) \
    M(UInt64, snapshots_to_keep, 3, "How many compressed snapshots to keep on disk", 0) \
    M(UInt64, stale_log_gap, 10000, "When node became stale and should receive snapshots from leader", 0) \
    M(UInt64, fresh_log_gap, 200, "When node became fresh", 0) \
    M(UInt64, max_requests_batch_size, 100, "Max size of batch in requests count before it will be sent to RAFT", 0) \
    M(Bool, quorum_reads, false, "Execute read requests as writes through whole RAFT consesus with similar speed", 0) \
    M(Bool, force_sync, true, "Call fsync on each change in RAFT changelog", 0)

DECLARE_SETTINGS_TRAITS(CoordinationSettingsTraits, LIST_OF_COORDINATION_SETTINGS)


struct CoordinationSettings : public BaseSettings<CoordinationSettingsTraits>
{
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);
};

using CoordinationSettingsPtr = std::shared_ptr<CoordinationSettings>;

}
