#pragma once

#include <atomic>
#include <chrono>
#include <mutex>
#include <string_view>

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
  * - Followers periodically read the lease. If its ETag stayed unchanged for `session_timeout`
  *   of their own steady time, they try to claim leadership with a conditional write
  *   (If-Match: stale_etag).
  * - If the lease file doesn't exist, any replica can create it with (If-None-Match: *).
  * - If a conditional write fails (PreconditionFailed), the writer lost the race and stays a follower.
  *
  * Clock assumption:
  * - No wall clock of one node is ever compared with the wall clock of another (the Chubby /
  *   Raft-lease approach). The leader anchors the freshness of its lease to its steady clock
  *   right before the write; a follower counts `session_timeout` on its own steady clock from
  *   the moment it first observed the ETag of that write, which is necessarily later. So a
  *   follower cannot claim the lease before the leader's own `isLeader` check stops passing,
  *   regardless of the clock offset between the nodes, including during the takeover sync,
  *   when that check is relaxed to the full `session_timeout`. Only the rates of the steady
  *   clocks are assumed to be equal. The wall-clock `timestamp` persisted in the lease is for
  *   diagnostics; together with `sequence` it keeps the content of successive writes, and thus
  *   their ETags (which are content hashes on S3), distinct.
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

    /// Immediately stop serving writes while keeping the heartbeat task alive. The lease is
    /// deliberately not renewed after this transition, so it expires and the normal takeover
    /// path rebuilds all write-side state before this or another replica serves writes again.
    void relinquishLeadership();

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

    /// A monotonically increasing counter incremented on every leadership acquisition (each
    /// follower -> leader transition). A write path captures this value when it is admitted /
    /// allocates block numbers and re-checks it before publishing the part: if leadership was
    /// lost and reacquired in between, the epoch differs and the stale write must be rejected.
    /// This is what distinguishes "we are the leader now" from "we are the same leader that
    /// admitted this write" — `isLeader` alone cannot, because a reacquired lease looks fresh.
    UInt64 leadershipEpoch() const { return leadership_epoch.load(std::memory_order_acquire); }

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

    /// Publish `Follower` (running the leadership-loss callback) with the given reason for the log,
    /// if this node is not already a follower. Serialized with `run` and `stop`.
    void demoteToFollower(std::string_view reason);

    /// Called by `run` right before it creates or claims a lease: a node that still considers itself
    /// the leader at that point lost its lease unnoticed and must go through a full re-election.
    void demoteBeforeTakeover();

    /// Try to write the lease file with conditional headers.
    /// Returns true if the write succeeded (we are the leader).
    bool tryWriteLease(const String & if_match, const String & if_none_match);

    /// Build the lease file content as JSON.
    String buildLeaseContent();

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

    enum class LeadershipState : UInt8
    {
        Follower,
        LeaderSyncing,
        LeaderWritable,
    };

    /// Published as a single state so that user-write admission cannot combine
    /// leadership from one instant with writability from another.
    std::atomic<LeadershipState> leadership_state{LeadershipState::Follower};
    std::atomic<bool> stopped{false};

    /// Incremented under `leadership_change_mutex` on every follower -> leader transition.
    /// See `leadershipEpoch`.
    std::atomic<UInt64> leadership_epoch{0};

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

    /// The ETag of the lease seen by the last successful read, and the steady time at which it was
    /// first seen. Lease expiry is judged by these, not by the persisted wall-clock `timestamp`.
    /// Only accessed by `run`.
    String observed_etag;
    std::chrono::steady_clock::time_point observed_etag_since;

    /// Incremented on every lease write and persisted as `sequence`, so that two writes of this
    /// instance never produce the same content, and thus the same ETag, even if the wall clock steps
    /// back. Only accessed by `run`.
    UInt64 lease_write_sequence = 0;

    String leader_id;

    CallbackOnLeadershipChange on_leadership_change;

    BackgroundSchedulePoolTaskHolder task;
    LoggerPtr log;
};

}
