#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <mutex>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <Core/BackgroundSchedulePool.h>
#include <Core/Names.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Common/Logger.h>

#include <base/defines.h>


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
  *   (If-Match: stale_etag). The lease records the writer's `session_timeout`, and a follower
  *   waits for the longer of its own and the writer's, so nodes configured with different
  *   timing settings for the same table cannot expire each other's leases early.
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
  *   diagnostics; together with `sequence` and `incarnation` it keeps the content of successive
  *   writes, and thus their ETags (which are content hashes on S3), distinct, also across
  *   instances of the same table (re-attach, restart).
  *
  * Readers:
  * - The lease decides who writes. It does not tell the leader what the other nodes read: a query on a
  *   follower holds its parts only in the memory of the follower, and the leader would remove an outdated
  *   part from the shared storage while that query still reads it. So every node that is not the leader
  *   keeps a reader file next to the lease (`<lease>_readers/<node>`) and rewrites it on every heartbeat.
  * - The reader file names the latest write of the lease (`incarnation` and `sequence`, the logical clock
  *   of the leader) that the node had read before it listed the storage for its current set of parts,
  *   and the parts that the leader must keep although that set no longer needs them: the outdated parts
  *   that its queries still hold, and the parts it keeps active because it could not load their replacement.
  * - The leader removes an outdated part only if every reader names a write of the lease that started
  *   after the part became outdated, and does not list the part. Such a reader listed the storage after
  *   the replacement of the part was committed, so the part is not active for it anymore, and no new query
  *   of it can take the part. A reader file that names no write, or a write of another leader, keeps
  *   everything.
  * - A node writes its reader file before it lists the storage for the first time, so the leader, which
  *   lists the reader files after the part became outdated, cannot miss a node that still considers the
  *   part active.
  * - A reader file expires like the lease: the leader ignores it once its ETag stayed unchanged for
  *   `session_timeout` of the leader's own steady time (the longer of its own and the reader's). A node
  *   that stalled for that long may find its parts removed; its queries then fail with an exception
  *   (see `IDataPartStorage::pinSharedStorageIdentity` for the case of a part name that was reused).
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

    /// ---- Readers. See the description of the class. ----

    /// Writes the reader file of this node right now. Called before the storage lists its parts for the first
    /// time, so that the registration precedes the listing; the heartbeat task keeps the file fresh afterwards.
    void registerReader();

    /// Called by the storage after it has refreshed its set of parts from the shared storage as a follower.
    /// `view_time` is a moment of this node's steady clock not later than the start of the listing the set was
    /// built from. `pinned_parts` are the parts the leader must not remove although the set does not need them
    /// (see the description of the class); they must be collected after the refresh, under the lock of the parts.
    void publishReaderView(std::chrono::steady_clock::time_point view_time, Names pinned_parts);

    /// The number of the latest write of the lease that this instance has started. Outdated parts are stamped
    /// with it (while the lock of the parts is held), see `ReadersSnapshot::allowsRemoval`.
    UInt64 currentLeaseSequence() const { return lease_write_sequence.load(std::memory_order_acquire); }

    /// The state of the reader files, as needed by the leader to decide which outdated parts it may remove.
    class ReadersSnapshot
    {
    public:
        /// Whether no reader can still use the part. `removal_sequence` is the value of `currentLeaseSequence`
        /// sampled when the part was already outdated, before the reader files were listed for this snapshot.
        bool allowsRemoval(const String & part_name, UInt64 removal_sequence) const;

        size_t numReaders() const { return readers.size(); }

        struct Reader
        {
            String id;
            /// The reader file could not be understood: it keeps everything.
            bool keeps_everything = false;
            /// Whether the write of the lease named by the reader is of this leader instance, and its number.
            bool names_own_lease = false;
            UInt64 lease_sequence = 0;
            NameSet pinned_parts;
        };

    private:
        friend class MergeTreeLeaderElection;

        std::vector<Reader> readers;
    };

    /// Lists and reads the reader files. Only for the leader. Does I/O on the object storage and may throw:
    /// the caller then must not remove anything.
    ReadersSnapshot scanReaders();

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
        /// `session_timeout` of the node that wrote the lease, 0 if the lease does not record it.
        UInt64 writer_session_timeout_ms = 0;
        /// The instance that wrote the lease and the number of the write, empty and 0 if the lease does not
        /// record them. Together they are the logical clock of the leader that the readers refer to.
        String incarnation;
        UInt64 sequence = 0;
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
    /// back. Only modified by `run`. It is also the logical clock that the readers refer to, see
    /// `currentLeaseSequence`.
    std::atomic<UInt64> lease_write_sequence{0};

    String leader_id;

    /// A random identifier of this instance, persisted as `incarnation`. `leader_id` survives
    /// restarts and `DETACH`/`ATTACH` and `lease_write_sequence` restarts from zero in every
    /// instance, so without it a new instance whose wall clock stepped back could reproduce the
    /// exact content, and thus the ETag, of a lease written by the previous instance, and a
    /// follower that observed that earlier write would wrongly consider the fresh lease expired.
    String incarnation;

    CallbackOnLeadershipChange on_leadership_change;

    /// ---- Readers: the side of a node that is not the leader. ----

    /// Records a write of the lease read by `run`, for `publishReaderView`.
    void observeLeaseWrite(const ParsedLease & lease, std::chrono::steady_clock::time_point observed_at);
    /// Forgets what this node has published as a reader: after a change of its role, its previous view of the
    /// parts says nothing about the queries it runs now. The reader file then keeps everything until the next
    /// `publishReaderView`.
    void resetReaderView();
    /// Writes the reader file with the current view. Throws on failure.
    void writeReaderFile();
    /// Removes the reader file of this node, logging a failure.
    void removeReaderFileNoThrow();
    String buildReaderFileContent();

    struct ObservedLeaseWrite
    {
        String incarnation;
        UInt64 sequence = 0;
        /// Steady time after the read of the lease that showed this write returned.
        std::chrono::steady_clock::time_point observed_at;
    };

    String readers_path_prefix;
    String reader_path;

    std::mutex reader_mutex;
    /// The writes of the lease seen by `run`, the oldest first: one entry per write, with the time it was first seen.
    std::deque<ObservedLeaseWrite> observed_lease_writes TSA_GUARDED_BY(reader_mutex);
    /// The write of the lease that the published set of parts is not older than; empty if nothing is published.
    String reader_view_lease_incarnation TSA_GUARDED_BY(reader_mutex);
    UInt64 reader_view_lease_sequence TSA_GUARDED_BY(reader_mutex) = 0;
    Names reader_pinned_parts TSA_GUARDED_BY(reader_mutex);
    /// Makes the content, and thus the ETag, of every write of the reader file unique.
    UInt64 reader_write_sequence TSA_GUARDED_BY(reader_mutex) = 0;

    /// ---- Readers: the side of the leader. ----

    struct ObservedReaderFile
    {
        String etag;
        /// Steady time at which this ETag was first seen, for the expiration.
        std::chrono::steady_clock::time_point etag_since;
        UInt64 session_timeout_ms = 0;
        ReadersSnapshot::Reader reader;
    };

    /// Parses a reader file. A file that cannot be understood yields a reader that keeps everything.
    ObservedReaderFile parseReaderFile(const String & reader_key, const String & content) const;

    std::mutex readers_scan_mutex;
    /// By the key of the reader file.
    std::unordered_map<String, ObservedReaderFile> observed_reader_files TSA_GUARDED_BY(readers_scan_mutex);

    BackgroundSchedulePoolTaskHolder task;
    LoggerPtr log;
};

}
