#pragma once

#include <base/types.h>

#include <ctime>

namespace DB::SelectiveReplication
{

/// Parse/serialize the `/selective/config` JSON stored in Keeper.
UInt64 parseSelectiveConfig(const String & json_str);
String serializeSelectiveConfig(UInt64 replication_factor);

inline constexpr const char * MIGRATIONS_SUBPATH = "migrations";
inline constexpr const char * REBALANCE_LOCK_SUBPATH = "rebalance_lock";

inline constexpr const char * MIGRATION_STATE_CLONE = "CLONE";
inline constexpr const char * MIGRATION_STATE_SWITCH = "SWITCH";
inline constexpr const char * MIGRATION_STATE_DONE = "DONE";
inline constexpr const char * MIGRATION_STATE_FAILED = "FAILED";

inline constexpr const char * CLONING_SUFFIX = ":cloning";
/// NOTE: CLONING_SUFFIX uses ':' as separator. Replica names must not end with ":cloning"
/// to avoid false positives in hasCloningSuffix(). This is enforced by the constraint that
/// replica names come from <replica> config or {replica} macros, typically hostnames or
/// IPs, which cannot end with ":cloning". IPv6 addresses like "[::1]" are safe because
/// the closing ']' prevents the ":cloning" suffix match.

inline constexpr time_t MIGRATION_MONITOR_INTERVAL_SECONDS = 30;

/// Maximum number of CAS retries for migration assignment updates.
inline constexpr int MAX_MIGRATION_CAS_RETRIES = 5;

/// Maximum distributed_depth for selective replication forwarding.
/// Write path: both Phase 1 (pre-routing) and Phase 2 (race handling) are
/// gated by `depth < MAX_FORWARDING_DEPTH`. A legitimate chain can reach
/// depth 4: distributed/remote entry -> initial replicated sink -> Phase 1
/// forward -> target replicated sink -> Phase 2 re-forward. The bound of 5
/// leaves one layer of safety margin.
/// Read path: fallback to selective routing at depth > 0 && depth < MAX_FORWARDING_DEPTH.
inline constexpr int MAX_FORWARDING_DEPTH = 5;

}
