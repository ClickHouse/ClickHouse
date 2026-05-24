#pragma once

#include <atomic>
#include <chrono>
#include <mutex>

#include <Core/BackgroundSchedulePool.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Common/Logger.h>


namespace DB
{

/** Leader election for non-replicated MergeTree tables on shared storage.
  *
  * Uses conditional writes (If-Match / If-None-Match) on object storage (S3, Azure)
  * to implement a lease-based leader election protocol without external coordination.
  *
  * Protocol:
  * - A lease file is stored on the object storage at a well-known path.
  * - The leader periodically renews the lease using a conditional write (If-Match: current_etag).
  * - Followers periodically read the lease. If it has expired (timestamp + session_timeout < now),
  *   they try to claim leadership with a conditional write (If-Match: stale_etag).
  * - If the lease file doesn't exist, any replica can create it with (If-None-Match: *).
  * - If a conditional write fails (PreconditionFailed), the writer lost the race and stays a follower.
  *
  * Clock skew assumption:
  * - The protocol relies on wall-clock timestamps embedded in the lease file. The maximum
  *   clock skew tolerated without a dual-writer window is approximately
  *   `session_timeout - 2 * heartbeat_interval`, which is at least `heartbeat_interval`
  *   given the enforced `session_timeout >= 3 * heartbeat_interval` rule. Beyond that bound,
  *   a follower with a fast clock can declare a healthy leader's lease expired and claim
  *   leadership before the old leader notices via its next heartbeat, opening a brief
  *   window (up to `heartbeat_interval`) where both nodes consider themselves leaders.
  *   Outside this window, the conditional ETag protocol prevents both nodes from
  *   committing successive lease renewals; only one observer wins each round. Use NTP
  *   or an equivalent time-sync service to keep skew well under `heartbeat_interval`.
  *   Future-dated timestamps beyond `session_timeout` are treated as stale to bound the
  *   impact in the opposite direction.
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
    /// and the heartbeat thread has renewed it recently enough.
    /// Normally the freshness threshold is `2 * heartbeat_interval`, which protects against
    /// a stalled heartbeat thread by failing closed before the remote lease can expire.
    /// During an in-progress takeover-sync callback (see `TakeoverSyncScope` below), the
    /// threshold is relaxed to `session_timeout` instead: the heartbeat thread is busy
    /// executing the callback itself, so the "stalled thread" interpretation does not
    /// apply, and the remote lease is still valid for the full session-timeout window.
    ///
    /// This check answers "do we hold the lease?" and is used by internal commit paths
    /// (the takeover-sync callback itself commits parts) and by background-job gates.
    /// User-facing write paths must use `isLeaderAndWritable` / `assertIsLeaderAndWritable`
    /// instead so that writes are blocked until the takeover-sync callback finishes
    /// `loadNewlyAppearedParts` and advances the block-number counter.
    bool isLeader() const;

    /// Throw `TABLE_IS_READ_ONLY` if not the leader. See `isLeader` for the semantics.
    void assertIsLeader() const;

    /// Returns true iff this instance holds the lease AND the takeover-sync callback
    /// has finished. User-facing write paths consult this so that a client `INSERT`
    /// cannot slip into the window where `is_leader` has been published but the
    /// callback has not yet refreshed the part view or advanced the block-number
    /// counter — both prerequisites for safe failover writes.
    bool isLeaderAndWritable() const;

    /// Throw `TABLE_IS_READ_ONLY` if not the leader or if takeover sync is still in
    /// progress. Used by user-facing write entry points (`assertNotReadonly`).
    void assertIsLeaderAndWritable() const;

    using CallbackOnLeadershipChange = std::function<void(bool /* is_leader */)>;

    /// Set a callback to be invoked when leadership status changes.
    /// Used by StorageMergeTree to start/stop background threads.
    void setOnLeadershipChangeCallback(CallbackOnLeadershipChange callback) { on_leadership_change = std::move(callback); }

    /// RAII scope that relaxes the `isLeader` freshness threshold while a synchronous
    /// takeover-sync callback (e.g. `loadNewlyAppearedParts`) is running. The heartbeat
    /// task is the caller of the callback, so the next heartbeat cannot run until the
    /// callback returns — making the usual "stalled thread" check spuriously fail any
    /// commit that the callback itself performs. Constructed by `run` around the
    /// `on_leadership_change(true)` invocation.
    class TakeoverSyncScope
    {
    public:
        explicit TakeoverSyncScope(MergeTreeLeaderElection & election_);
        ~TakeoverSyncScope();
    private:
        MergeTreeLeaderElection & election;
    };

private:
    /// The periodic task body.
    void run();

    /// Try to write the lease file with conditional headers.
    /// Returns true if the write succeeded (we are the leader).
    bool tryWriteLease(const String & if_match, const String & if_none_match);

    /// Build the lease file content as JSON.
    String buildLeaseContent() const;

    /// Result of parsing a lease file. The `status` field disambiguates how the caller
    /// should react to non-`Ok` outcomes — in particular, an unknown payload version
    /// must not be treated the same as a parse error, otherwise older binaries would
    /// silently overwrite leases written by newer binaries during a rolling upgrade.
    enum class LeaseParseStatus
    {
        Ok,
        ParseError,        /// Unparsable / out-of-range timestamp — treat as a stale lease and self-heal.
        UnknownVersion,    /// Valid JSON, version not understood — fail closed: stay a follower.
    };

    struct ParsedLease
    {
        String leader_id;
        time_t timestamp = 0;
        LeaseParseStatus status = LeaseParseStatus::ParseError;
    };

    /// Parse the lease file content. See `LeaseParseStatus` for the meaning of each outcome.
    static ParsedLease parseLeaseContent(const String & content);

    /// Generate a unique leader ID for this server instance.
    static String generateLeaderId();

    StorageID storage_id;
    ObjectStoragePtr object_storage;
    String lease_path;
    ContextPtr context;
    UInt64 heartbeat_interval_ms;
    UInt64 session_timeout_ms;

    std::atomic<bool> is_leader{false};
    std::atomic<bool> stopped{false};

    /// True after the takeover-sync callback has finished and external user writes
    /// can be served. Distinct from `is_leader`: `is_leader` is published as soon
    /// as the lease is acquired, but `writes_enabled` is only flipped after
    /// `loadNewlyAppearedParts` and the block-number counter advance have completed.
    /// Gating user writes on this flag (rather than `is_leader` alone) prevents a
    /// client `INSERT` from allocating a stale block number during the takeover
    /// window — the data-loss race that the failover stress test exercises.
    std::atomic<bool> writes_enabled{false};

    /// True while the heartbeat task is synchronously executing the takeover-sync
    /// callback (`on_leadership_change(true)`). Set via `TakeoverSyncScope`.
    /// `isLeader` relaxes its freshness check while this is true.
    std::atomic<bool> in_takeover_sync{false};

    /// Serializes leadership transitions in `run` and `stop` so that a heartbeat task
    /// in flight when `stop` is called cannot re-enable leadership or fire the
    /// `on_leadership_change(true)` callback after shutdown has begun.
    std::mutex leadership_change_mutex;

    /// Monotonic time of the last successful lease renewal.
    /// Used to detect stalled heartbeat threads.
    std::atomic<std::chrono::steady_clock::time_point> last_renewal_time{std::chrono::steady_clock::time_point{}};

    String leader_id;

    CallbackOnLeadershipChange on_leadership_change;

    BackgroundSchedulePoolTaskHolder task;
    LoggerPtr log;
};

}
