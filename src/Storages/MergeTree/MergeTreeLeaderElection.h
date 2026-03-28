#pragma once

#include <atomic>
#include <chrono>

#include <Core/BackgroundSchedulePool.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Common/Logger.h>


namespace DB
{

/** Leader election for non-replicated MergeTree tables on shared storage.
  *
  * Uses conditional writes (If-Match / If-None-Match) on object storage (S3, Azure, GCS)
  * to implement a lease-based leader election protocol without external coordination.
  *
  * Protocol:
  * - A lease file is stored on the object storage at a well-known path.
  * - The leader periodically renews the lease using a conditional write (If-Match: current_etag).
  * - Followers periodically read the lease. If it has expired (timestamp + session_timeout < now),
  *   they try to claim leadership with a conditional write (If-Match: stale_etag).
  * - If the lease file doesn't exist, any replica can create it with (If-None-Match: *).
  * - If a conditional write fails (PreconditionFailed), the writer lost the race and stays a follower.
  */
class MergeTreeLeaderElection
{
public:
    MergeTreeLeaderElection(
        const StorageID & storage_id_,
        ObjectStoragePtr object_storage_,
        String lease_path_,
        ContextPtr context_,
        UInt64 heartbeat_interval_ms_,
        UInt64 session_timeout_ms_);

    ~MergeTreeLeaderElection();

    /// Start the background heartbeat task.
    void start();

    /// Stop the background heartbeat task and relinquish leadership.
    void stop();

    /// Returns true if this instance currently holds the leader lease
    /// and the heartbeat thread has renewed it recently enough (within session_timeout).
    /// This protects against the case when the heartbeat thread stalls due to scheduling delays.
    bool isLeader() const;

    /// Throw TABLE_IS_READ_ONLY if not the leader.
    void assertIsLeader() const;

    using CallbackOnLeadershipChange = std::function<void(bool /* is_leader */)>;

    /// Set a callback to be invoked when leadership status changes.
    /// Used by StorageMergeTree to start/stop background threads.
    void setOnLeadershipChangeCallback(CallbackOnLeadershipChange callback) { on_leadership_change = std::move(callback); }

private:
    /// The periodic task body.
    void run();

    /// Try to write the lease file with conditional headers.
    /// Returns true if the write succeeded (we are the leader).
    bool tryWriteLease(const String & if_match, const String & if_none_match);

    /// Build the lease file content as JSON.
    String buildLeaseContent() const;

    /// Parse the lease file content. Returns {leader_id, timestamp}.
    /// On parse failure, returns empty leader_id and zero timestamp,
    /// allowing the caller to treat a corrupted lease as stale.
    static std::pair<String, time_t> parseLeaseContent(const String & content);

    /// Generate a unique leader ID for this server instance.
    static String generateLeaderId();

    StorageID storage_id;
    ObjectStoragePtr object_storage;
    String lease_path;
    ContextPtr context;
    UInt64 heartbeat_interval_ms;
    UInt64 session_timeout_ms;

    std::atomic<bool> is_leader{false};

    /// Monotonic time of the last successful lease renewal.
    /// Used to detect stalled heartbeat threads.
    std::atomic<std::chrono::steady_clock::time_point> last_renewal_time{std::chrono::steady_clock::time_point{}};

    String current_etag;
    String leader_id;

    CallbackOnLeadershipChange on_leadership_change;

    BackgroundSchedulePoolTaskHolder task;
    LoggerPtr log;
};

}
