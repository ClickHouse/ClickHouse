#include <Storages/MergeTree/MergeTreeLeaderElection.h>

#include <Core/ServerUUID.h>
#include <Core/UUID.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Common/CurrentMetrics.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>

#include <base/JSON.h>
#include <base/defines.h>
#include <base/getFQDNOrHostName.h>

#include <unordered_set>


namespace ProfileEvents
{
    extern const Event MergeTreeLeaderElectionAcquired;
    extern const Event MergeTreeLeaderElectionLost;
    extern const Event MergeTreeLeaderElectionLeaseRenewals;
    extern const Event MergeTreeLeaderElectionLeaseTakeovers;
    extern const Event MergeTreeLeaderElectionLeaseConflicts;
    extern const Event MergeTreeLeaderElectionLeaseParseErrors;
    extern const Event MergeTreeLeaderElectionUnknownVersionRejections;
    extern const Event MergeTreeLeaderElectionHeartbeatErrors;
}

namespace CurrentMetrics
{
    extern const Metric MergeTreeLeaderElectionLeader;
    extern const Metric MergeTreeLeaderElectionFollower;
}


namespace DB
{

namespace FailPoints
{
    extern const char merge_tree_leader_election_pause_heartbeat[];
}

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int AZURE_BLOB_STORAGE_ERROR;
    extern const int TABLE_IS_READ_ONLY;
}

static constexpr size_t MAX_LEASE_FILE_SIZE = 4096;

/// A reader file lists the parts that the queries of a node still hold, so it is not bounded like the lease.
/// A larger file is not read: its node keeps everything.
static constexpr size_t MAX_READER_FILE_SIZE = 64 * 1024 * 1024;

/// How many writes of the lease a node remembers until it publishes a view of the parts that is newer than them.
static constexpr size_t MAX_OBSERVED_LEASE_WRITES = 1024;

MergeTreeLeaderElection::MergeTreeLeaderElection(
    const StorageID & storage_id_,
    ObjectStoragePtr object_storage_,
    String lease_path_,
    ContextPtr context_,
    UInt64 heartbeat_interval_ms_,
    UInt64 session_timeout_ms_)
    : storage_id(storage_id_)
    , object_storage(std::move(object_storage_))
    , lease_path(std::move(lease_path_))
    , context(std::move(context_))
    , heartbeat_interval_ms(heartbeat_interval_ms_)
    , session_timeout_ms(session_timeout_ms_)
    , leader_id(generateLeaderId())
    , incarnation(toString(UUIDHelpers::generateV4()))
    , readers_path_prefix(lease_path + "_readers/")
    , reader_path(readers_path_prefix + escapeForFileName(leader_id))
    , log(getLogger("MergeTreeLeaderElection"))
{
    /// Every participating table starts as a follower. `stop` always brings the
    /// instance back to the follower state before the destructor decrements this
    /// gauge, so the increment + decrement pair is balanced regardless of how
    /// leadership transitions during the table's lifetime.
    CurrentMetrics::add(CurrentMetrics::MergeTreeLeaderElectionFollower);
}

MergeTreeLeaderElection::~MergeTreeLeaderElection()
{
    stop();
    CurrentMetrics::sub(CurrentMetrics::MergeTreeLeaderElectionFollower);
}

void MergeTreeLeaderElection::start()
{
    task = context->getSchedulePool()->createTask(storage_id, "MergeTreeLeaderElection", [this] { run(); });
    task->activateAndSchedule();
}

void MergeTreeLeaderElection::stop()
{
    stopped.store(true, std::memory_order_release);

    if (task)
        task->deactivate();

    /// Hold the same lock that `run` uses while updating leadership state.
    /// `task->deactivate` waits for the current scheduled execution to finish, but
    /// the task may have been already running concurrently when `stop` was called,
    /// so we serialize the final transition here to avoid a stale `on_leadership_change(true)`
    /// being invoked after `stop` returns.
    std::lock_guard lock(leadership_change_mutex);
    bool was_leader = leadership_state.exchange(LeadershipState::Follower, std::memory_order_acq_rel) != LeadershipState::Follower;
    if (was_leader)
    {
        ProfileEvents::increment(ProfileEvents::MergeTreeLeaderElectionLost);
        CurrentMetrics::sub(CurrentMetrics::MergeTreeLeaderElectionLeader);
        CurrentMetrics::add(CurrentMetrics::MergeTreeLeaderElectionFollower);
        if (on_leadership_change)
        {
            /// Shield the callback so an exception cannot escape the destructor
            /// (which calls `stop`) and trigger `std::terminate`.
            try
            {
                on_leadership_change(false);
            }
            catch (...)
            {
                tryLogCurrentException(log, "Exception in leadership-loss callback during stop");
            }
        }
    }
}

void MergeTreeLeaderElection::relinquishLeadership()
{
    demoteToFollower("after a write-side state reconciliation failure");
}

void MergeTreeLeaderElection::demoteToFollower(std::string_view reason)
{
    std::lock_guard lock(leadership_change_mutex);

    bool was_leader = leadership_state.exchange(LeadershipState::Follower, std::memory_order_acq_rel) != LeadershipState::Follower;
    if (!was_leader)
        return;

    LOG_WARNING(log, "Relinquishing leadership for lease at '{}' {}", lease_path, reason);
    ProfileEvents::increment(ProfileEvents::MergeTreeLeaderElectionLost);
    CurrentMetrics::sub(CurrentMetrics::MergeTreeLeaderElectionLeader);
    CurrentMetrics::add(CurrentMetrics::MergeTreeLeaderElectionFollower);

    if (on_leadership_change)
    {
        try
        {
            on_leadership_change(false);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Exception in leadership-loss callback while relinquishing leadership");
        }
    }
}

bool MergeTreeLeaderElection::isLeader() const
{
    if (leadership_state.load(std::memory_order_acquire) == LeadershipState::Follower)
        return false;

    /// Protect against stalled heartbeat thread: if the last successful renewal
    /// was too long ago, we cannot be sure the lease is still valid.
    /// We use 2x heartbeat interval as the threshold — this gives a comfortable margin
    /// for scheduling jitter while being well within the session timeout.
    ///
    /// During an in-progress takeover-sync callback, the heartbeat task is busy executing
    /// the callback itself (no other heartbeat can run until it returns), so the "stalled
    /// thread" interpretation does not apply. Relax the threshold to `session_timeout_ms`
    /// — the remote lease is valid for that long, so commits issued by the callback are
    /// still backed by a legitimate lease. Beyond `session_timeout_ms`, another node may
    /// have legitimately taken over and commits must fail.
    auto elapsed = std::chrono::steady_clock::now() - last_renewal_time.load(std::memory_order_acquire);
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
    UInt64 threshold_ms = in_takeover_sync.load(std::memory_order_acquire)
        ? session_timeout_ms
        : heartbeat_interval_ms * 2;
    return static_cast<UInt64>(elapsed_ms) < threshold_ms;
}

MergeTreeLeaderElection::TakeoverSyncScope::TakeoverSyncScope(MergeTreeLeaderElection & election_)
    : election(election_)
{
    election.in_takeover_sync.store(true, std::memory_order_release);
}

MergeTreeLeaderElection::TakeoverSyncScope::~TakeoverSyncScope()
{
    election.in_takeover_sync.store(false, std::memory_order_release);
}

void MergeTreeLeaderElection::assertIsLeader() const
{
    if (!isLeader())
        throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is in readonly mode because this instance is not the leader");
}

bool MergeTreeLeaderElection::isLeaderAndWritable() const
{
    /// Take one state snapshot. A concurrent loss publishes `Follower` with one
    /// atomic exchange, so the admission gate cannot combine a stale leader bit
    /// with a stale writable bit from another atomic variable.
    if (leadership_state.load(std::memory_order_acquire) != LeadershipState::LeaderWritable)
        return false;

    return isLeader();
}

void MergeTreeLeaderElection::assertIsLeaderAndWritable() const
{
    if (!isLeaderAndWritable())
        throw Exception(ErrorCodes::TABLE_IS_READ_ONLY,
            "Table is in readonly mode because this instance is not the leader "
            "or the post-failover sync is still in progress");
}

void MergeTreeLeaderElection::run()
{
    /// Test hook: stall the heartbeat (all leader-election tables of this server) for as long as the
    /// failpoint is enabled, so a lease can expire, be taken over and be dropped again unnoticed.
    FailPointInjection::pauseFailPoint(FailPoints::merge_tree_leader_election_pause_heartbeat);

    try
    {
        bool became_leader = false;
        /// Whether the successful write (if any) was a renewal of an already-held lease
        /// or a takeover of a missing/expired lease. Used to attribute the operation
        /// to the right ProfileEvent counter.
        bool was_renewal_attempt = false;

        /// Try to read the existing lease file.
        /// Disable filesystem cache for lease reads — the lease file is tiny and
        /// must not go through CachedOnDiskReadBufferFromFile, which can cause
        /// use-after-free during table shutdown when the cache is destroyed
        /// before the background task fully stops.
        auto read_settings = context->getReadSettings();
        read_settings.enable_filesystem_cache = false;
        auto result = object_storage->tryGetObjectMetadata(lease_path, /* with_tags= */ false);

        if (!result)
        {
            /// Lease file does not exist. Try to create it.
            LOG_TRACE(log, "Lease file does not exist at '{}', trying to create", lease_path);
            demoteBeforeTakeover();
            became_leader = tryWriteLease(/* if_match= */ "", /* if_none_match= */ "*");
        }
        else if (result->is_size_known && result->size_bytes > MAX_LEASE_FILE_SIZE)
        {
            /// A lease is a few hundred bytes of JSON, and `MAX_LEASE_FILE_SIZE` is part of the
            /// format contract: no version of this code writes a larger one, so a larger object is
            /// corrupt by definition. It must be recognized here, from the metadata we already
            /// have: `readSmallObjectAndGetObjectMetadata` below throws `CANNOT_READ_ALL_DATA`
            /// before `parseLeaseContent` ever sees the payload, and the outer catch would then
            /// demote and retry against the very same object on every heartbeat — leaving the table
            /// read-only forever, until someone deletes the object by hand.
            ///
            /// Self-heal exactly like the `ParseError` branch below: claim the lease with a
            /// conditional write against the `etag` of the object we just inspected, so a
            /// concurrent writer (another node healing the same lease, or the legitimate leader
            /// rewriting it) still wins the race and this node stays a follower.
            LOG_WARNING(log,
                "Lease at '{}' is {} bytes, larger than the maximum lease size of {} bytes: it cannot have been "
                "written by any version of the lease protocol. Treating it as corrupted and trying to claim",
                lease_path, result->size_bytes, MAX_LEASE_FILE_SIZE);
            ProfileEvents::increment(ProfileEvents::MergeTreeLeaderElectionLeaseParseErrors);
            demoteBeforeTakeover();
            became_leader = tryWriteLease(/* if_match= */ result->etag, /* if_none_match= */ "");
        }
        else
        {
            /// Lease file exists. Read its content and ETag.
            auto data_with_metadata = object_storage->readSmallObjectAndGetObjectMetadata(
                StoredObject(lease_path), read_settings, MAX_LEASE_FILE_SIZE);

            String etag = data_with_metadata.metadata.etag;
            auto parsed = parseLeaseContent(data_with_metadata.data);

            /// Expiry never compares the wall clock of the writer with ours: the persisted
            /// `timestamp` is diagnostic only. A foreign lease expires once its ETag stayed unchanged
            /// for `session_timeout` of our own steady time, counted from when we first observed that
            /// ETag. The first observation happens after the write completed, so after the writer
            /// sampled its freshness anchor (`tryWriteLease`), hence we cannot expire a lease before
            /// the writer's own `isLeader` check stops passing - whatever the clock offset between the
            /// nodes, even during the takeover sync, which relaxes that check to the full
            /// `session_timeout`. Only the clock rate is assumed equal. The sample is taken after the
            /// read returned, never before it: a write completing during the read must not be
            /// attributed an earlier observation time. The cost is that a node that has just started
            /// waits a full `session_timeout` before it can claim an orphaned lease.
            const auto steady_now = std::chrono::steady_clock::now();
            if (etag != observed_etag)
            {
                observed_etag = etag;
                observed_etag_since = steady_now;
            }

            /// For the reader file of this node: a set of parts listed from the storage after this moment is not
            /// older than this write of the lease (see `publishReaderView`).
            if (parsed.status == LeaseParseStatus::Ok)
                observeLeaseWrite(parsed, steady_now);

            /// A foreign lease is valid for as long as its writer may keep passing its own `isLeader`
            /// check, which is bounded by the writer's `session_timeout`, not ours: nodes may run the
            /// same table with different timing settings. So wait for the longer of the two. A lease
            /// that does not record the writer's timeout is judged by ours.
            const auto session_timeout = std::chrono::milliseconds(std::max(session_timeout_ms, parsed.writer_session_timeout_ms));
            const bool serving_as_leader = leadership_state.load(std::memory_order_acquire) != LeadershipState::Follower;
            const bool own_lease = parsed.status == LeaseParseStatus::Ok && parsed.leader_id == leader_id;

            /// Our own lease, while we serve as leader, is judged by our own freshness anchor: no
            /// other node can observe the ETag of our last write before that anchor, so it is the
            /// earliest instant any of them could consider the lease expired. This catches a heartbeat
            /// stalled past `session_timeout` - it then goes through `demoteBeforeTakeover` and a full
            /// re-acquisition below. Every other lease, including one still carrying our `leader_id`
            /// while we are a follower (e.g. after a failed takeover sync, or after a restart), is
            /// judged by the observation time computed above, like a foreign lease, so that a node
            /// which cannot complete takeover does not monopolize the lease.
            const bool lease_expired = (own_lease && serving_as_leader)
                ? steady_now - last_renewal_time.load(std::memory_order_acquire) >= session_timeout
                : steady_now - observed_etag_since >= session_timeout;

            if (parsed.status == LeaseParseStatus::UnknownVersion)
            {
                /// Fail closed for forward compatibility: a newer binary may write a lease
                /// in a format we do not understand. Taking it over with our older format
                /// would let an old node steal leadership from a healthy newer leader and
                /// silently downgrade the on-disk lease format. Stay a follower until the
                /// binary is upgraded.
                LOG_WARNING(log, "Lease at '{}' has unknown payload version, refusing to take over (rolling-upgrade safety)", lease_path);
                ProfileEvents::increment(ProfileEvents::MergeTreeLeaderElectionUnknownVersionRejections);
                became_leader = false;
            }
            else if (own_lease && !lease_expired)
            {
                /// The remote lease still carries our `leader_id`. Only renew it while we are
                /// actually serving as the local leader. If a previous takeover-sync callback
                /// threw, the catch handler cleared `is_leader`/`writes_enabled` locally, yet the
                /// remote lease kept our id. Renewing it here would let a node that cannot complete
                /// takeover monopolize the lease: it would keep the lease alive (so healthy
                /// followers never observe it expire) while never enabling writes itself, livelocking
                /// failover. By not renewing, the lease ages out and another node — or this one, via
                /// the expiry branch below on a later heartbeat — can claim it and retry takeover.
                if (serving_as_leader)
                {
                    LOG_TRACE(log, "Renewing leader lease at '{}'", lease_path);
                    was_renewal_attempt = true;
                    became_leader = tryWriteLease(/* if_match= */ etag, /* if_none_match= */ "");
                }
                else
                {
                    LOG_WARNING(log,
                        "Remote lease at '{}' still carries our leader id, but this node is not "
                        "serving as leader (a previous takeover sync likely failed). Not renewing, "
                        "so the lease can expire and another node can take over.",
                        lease_path);
                    became_leader = false;
                }
            }
            else if (parsed.status == LeaseParseStatus::ParseError || lease_expired)
            {
                /// The lease has expired (`lease_expired`, see above), or the content was corrupted.
                /// This also covers an expired lease that still carries our own `leader_id`, so this
                /// node can reclaim it after a failed takeover. Try to claim leadership.
                LOG_INFO(log, "Leader lease at '{}' expired or corrupted (leader_id: {}, timestamp: {}), trying to claim",
                    lease_path, parsed.leader_id, parsed.timestamp);
                demoteBeforeTakeover();
                became_leader = tryWriteLease(/* if_match= */ etag, /* if_none_match= */ "");
            }
            else
            {
                /// Another leader holds a valid lease.
                LOG_TRACE(log, "Another leader holds the lease at '{}' (leader_id: {}, unchanged for {} ms)",
                    lease_path, parsed.leader_id,
                    std::chrono::duration_cast<std::chrono::milliseconds>(steady_now - observed_etag_since).count());
                became_leader = false;
            }
        }

        if (became_leader)
        {
            ProfileEvents::increment(was_renewal_attempt
                ? ProfileEvents::MergeTreeLeaderElectionLeaseRenewals
                : ProfileEvents::MergeTreeLeaderElectionLeaseTakeovers);
        }

        /// Serialize the leadership transition with `stop`. Without this lock, a heartbeat
        /// task in flight when `stop` is called could re-acquire leadership and invoke
        /// `on_leadership_change(true)` after `stop` has already relinquished it,
        /// leaving background tasks running during shutdown.
        std::lock_guard lock(leadership_change_mutex);

        /// Re-check `stopped` while holding the lock — another thread may have called
        /// `stop` between the slow lease I/O above and acquiring this mutex.
        if (stopped.load(std::memory_order_acquire))
            return;

        /// A loss is published as one atomic transition to `Follower`, so user
        /// writes cannot observe independent leader and writable values. A lease
        /// renewal keeps its existing state: demoting a writable leader to
        /// `LeaderSyncing` on every successful heartbeat would block writes.
        bool was_leader = leadership_state.load(std::memory_order_acquire) != LeadershipState::Follower;

        /// A successful write through the create/claim path is always an acquisition here:
        /// `demoteBeforeTakeover` published `Follower` before the write, so a node that still
        /// considered itself the leader (its lease expired unnoticed) takes the epoch bump and the
        /// takeover sync below like any other new leader.
        chassert(!(became_leader && was_leader && !was_renewal_attempt));

        if (!became_leader)
            leadership_state.store(LeadershipState::Follower, std::memory_order_release);
        else if (!was_leader)
            leadership_state.store(LeadershipState::LeaderSyncing, std::memory_order_release);

        if (became_leader && !was_leader)
        {
            /// New leadership epoch. Incremented before publishing writability
            /// and before the takeover callback, so any write admitted under a previous lease
            /// observes a different epoch at commit time and is rejected (see `leadershipEpoch`).
            leadership_epoch.fetch_add(1, std::memory_order_acq_rel);

            /// What this node published as a reader describes the set of parts it had as a follower. If the
            /// takeover fails, or when leadership is lost later, its reader file must keep everything until
            /// the next refresh as a follower publishes a new view.
            resetReaderView();

            LOG_INFO(log, "Acquired leadership for lease at '{}'", lease_path);
            ProfileEvents::increment(ProfileEvents::MergeTreeLeaderElectionAcquired);
            CurrentMetrics::sub(CurrentMetrics::MergeTreeLeaderElectionFollower);
            CurrentMetrics::add(CurrentMetrics::MergeTreeLeaderElectionLeader);
            if (on_leadership_change)
            {
                /// Relax the `isLeader` freshness check while the takeover callback is
                /// running. The callback (`loadNewlyAppearedParts` + counter advance)
                /// commits parts via `Transaction::commit`, which goes through
                /// `assertCanCommitTransaction` -> `assertIsLeader`. Without this scope,
                /// any sync that exceeds `2 * heartbeat_interval` would self-fail and
                /// drop leadership, livelocking failover with a non-trivial part backlog.
                ///
                /// The state remains `LeaderSyncing` at this point, so user `INSERT`s
                /// are still rejected by
                /// `assertIsLeaderAndWritable`. Only after the callback returns —
                /// when the part view reflects the previous leader's commits and the
                /// block-number counter has been advanced past them — do we publish
                /// the writable flag.
                TakeoverSyncScope sync_scope(*this);
                on_leadership_change(true);
            }
            leadership_state.store(LeadershipState::LeaderWritable, std::memory_order_release);

            /// The leader holds its parts by itself: its queries are seen by its own cleanup. The reader file is
            /// written again by the first heartbeat after leadership is lost.
            removeReaderFileNoThrow();
        }
        else if (!became_leader && was_leader)
        {
            LOG_INFO(log, "Lost leadership for lease at '{}'", lease_path);
            ProfileEvents::increment(ProfileEvents::MergeTreeLeaderElectionLost);
            CurrentMetrics::sub(CurrentMetrics::MergeTreeLeaderElectionLeader);
            CurrentMetrics::add(CurrentMetrics::MergeTreeLeaderElectionFollower);
            if (on_leadership_change)
            {
                /// Shield the callback, like the `stop` and error paths do. Leadership-change
                /// callbacks fire only on transitions: the state is already `Follower` here, so a
                /// later heartbeat would see `was_leader == false` and never retry the follower
                /// transition — an escaping exception would otherwise permanently skip whatever
                /// part of it had not run yet. Writes are already fail-closed by the state
                /// transition above, and the storage-side callback orders its own
                /// steps fail-closed, so logging is the correct handling here.
                try
                {
                    on_leadership_change(false);
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Exception in leadership-loss callback");
                }
            }
        }
    }
    catch (...)
    {
        /// On any error, conservatively assume we are not the leader.
        ProfileEvents::increment(ProfileEvents::MergeTreeLeaderElectionHeartbeatErrors);
        std::lock_guard lock(leadership_change_mutex);
        /// A single state exchange closes the user-write gate, including when the
        /// exception was raised by a takeover-sync callback.
        bool was_leader = leadership_state.exchange(LeadershipState::Follower, std::memory_order_acq_rel) != LeadershipState::Follower;
        if (was_leader)
        {
            LOG_WARNING(log, "Lost leadership due to exception for lease at '{}'", lease_path);
            ProfileEvents::increment(ProfileEvents::MergeTreeLeaderElectionLost);
            CurrentMetrics::sub(CurrentMetrics::MergeTreeLeaderElectionLeader);
            CurrentMetrics::add(CurrentMetrics::MergeTreeLeaderElectionFollower);
            if (on_leadership_change)
            {
                /// Shield the callback so that an exception thrown from the
                /// leadership-loss notification cannot escape `run` and skip the
                /// `scheduleAfter` call below, which would permanently stop the
                /// heartbeat task with no recovery path.
                try
                {
                    on_leadership_change(false);
                }
                catch (...)
                {
                    tryLogCurrentException(log, "Exception in leadership-loss callback");
                }
            }
        }

        tryLogCurrentException(log, "Error in leader election heartbeat");
    }

    /// Every node that is not the leader keeps its reader file fresh, see the description of the class.
    /// A failure is not fatal for the node: the leader ignores the file once it has expired, and then a query
    /// of this node may find its parts removed.
    if (!stopped.load(std::memory_order_acquire) && leadership_state.load(std::memory_order_acquire) == LeadershipState::Follower)
    {
        try
        {
            writeReaderFile();
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Cannot write the reader file at '{}'", reader_path));
        }
    }

    if (!stopped.load(std::memory_order_acquire))
        task->scheduleAfter(heartbeat_interval_ms);
}

void MergeTreeLeaderElection::demoteBeforeTakeover()
{
    if (leadership_state.load(std::memory_order_acquire) == LeadershipState::Follower)
        return;

    /// We still consider ourselves the leader, yet the lease is not ours to renew: it has expired,
    /// was taken over by another node and expired again, or disappeared - all unnoticed, typically
    /// because this heartbeat was stalled past `session_timeout`. Another node may have written into
    /// the table in between, so continuing as the same leader after re-taking the lease would keep
    /// serving with a part set, block-number counter and deduplication log that predate its writes,
    /// and would let writes admitted under the old lease commit. Publish `Follower` BEFORE the write:
    /// `tryWriteLease` refreshes the lease freshness anchor, and a fresh lease must never be observed
    /// together with the stale leader state. The successful write is then a regular acquisition -
    /// new epoch, takeover sync - in `run`.
    demoteToFollower("before re-taking a lease that expired unnoticed while this node was serving as leader");
}

bool MergeTreeLeaderElection::tryWriteLease(const String & if_match, const String & if_none_match)
{
    try
    {
        /// Anchor local lease freshness to the instant before the write starts, NOT to the moment
        /// the write finishes. Remote observers start counting `session_timeout` when they first see
        /// the ETag of this write, which cannot happen before the write starts, so this anchor is
        /// never later than the earliest instant at which another node may consider the lease
        /// expired. If the conditional write / `finalize` stalls, anchoring `last_renewal_time` to
        /// the end of the write would not have that property. Capturing the monotonic anchor here
        /// can only make us fail closed slightly sooner, which is the safe direction.
        const auto renewal_anchor = std::chrono::steady_clock::now();
        String content = buildLeaseContent();

        auto write_settings = context->getWriteSettings();
        write_settings.object_storage_write_if_match = if_match;
        write_settings.object_storage_write_if_none_match = if_none_match;
        /// Disable filesystem cache for lease writes — the lease file is tiny and
        /// rewritten frequently. Writing through CachedOnDiskWriteBufferFromFile
        /// causes "Having intersection with already existing cache" errors.
        write_settings.enable_filesystem_cache_on_write_operations = false;

        auto buffer = object_storage->writeObject(
            StoredObject(lease_path),
            WriteMode::Rewrite,
            /* attributes= */ std::nullopt,
            DBMS_DEFAULT_BUFFER_SIZE,
            write_settings);

        buffer->write(content.data(), content.size());
        buffer->finalize();

        /// The next heartbeat will read the lease back together with its ETag
        /// (single round-trip via `readSmallObjectAndGetObjectMetadata`), so an
        /// extra `getObjectMetadata` call here would only add a chance for a
        /// transient remote failure to surface as spurious leadership loss
        /// without providing any value to subsequent renewals.
        last_renewal_time.store(renewal_anchor, std::memory_order_release);

        return true;
    }
    catch (const Exception & e)
    {
        if ((e.code() == ErrorCodes::S3_ERROR || e.code() == ErrorCodes::AZURE_BLOB_STORAGE_ERROR)
            && (e.message().contains("PreconditionFailed")
                || e.message().contains("ConditionNotMet")))
        {
            LOG_TRACE(log, "Conditional write failed (precondition not met) for lease at '{}'", lease_path);
            ProfileEvents::increment(ProfileEvents::MergeTreeLeaderElectionLeaseConflicts);
            return false;
        }
        throw;
    }
}

String MergeTreeLeaderElection::buildLeaseContent()
{
    /// `sequence` and `incarnation` make the content, and thus the ETag, of every write unique (see
    /// `lease_write_sequence` and `incarnation`). The other nodes also read them back as the logical clock
    /// of the leader, which they name in their reader files. Older binaries ignore unknown fields of version 1.
    WriteBufferFromOwnString out;
    writeString(R"({"version":1,"leader_id":")", out);
    writeString(leader_id, out);
    writeString(R"(","timestamp":)", out);
    writeIntText(time(nullptr), out);
    writeString(R"(,"sequence":)", out);
    writeIntText(++lease_write_sequence, out);
    writeString(R"(,"incarnation":")", out);
    writeString(incarnation, out);
    writeChar('"', out);
    /// The validity interval this writer relies on: other nodes must not expire the lease sooner
    /// than this, even if their own `session_timeout` is shorter (see `run`).
    writeString(R"(,"session_timeout_ms":)", out);
    writeIntText(session_timeout_ms, out);
    writeChar('}', out);
    return out.str();
}

MergeTreeLeaderElection::ParsedLease MergeTreeLeaderElection::parseLeaseContent(const String & content)
{
    try
    {
        JSON json(content);

        /// An unknown payload version is a forward-compatibility signal — a newer binary
        /// may have written this lease. Distinguishing it from a parse error matters: a
        /// parse error self-heals via lease takeover, but an unknown version must NOT,
        /// otherwise an older node would silently downgrade the lease format mid-cluster.
        Int64 version = json["version"].getInt();
        if (version != 1)
        {
            LOG_WARNING(
                getLogger("MergeTreeLeaderElection"),
                "Lease file has unknown version {}; treating as held by a newer binary",
                version);
            ParsedLease unknown;
            unknown.status = LeaseParseStatus::UnknownVersion;
            return unknown;
        }

        ParsedLease result;
        result.leader_id = json["leader_id"].getString();
        result.timestamp = json["timestamp"].getInt();
        if (json.has("session_timeout_ms"))
            result.writer_session_timeout_ms = json["session_timeout_ms"].getUInt();
        if (json.has("sequence") && json.has("incarnation"))
        {
            result.sequence = json["sequence"].getUInt();
            result.incarnation = json["incarnation"].getString();
        }
        result.status = LeaseParseStatus::Ok;
        return result;
    }
    catch (...)
    {
        /// Corrupted lease file: caller will treat it as expired and overwrite it.
        tryLogCurrentException("MergeTreeLeaderElection", "Failed to parse lease file content");
        ProfileEvents::increment(ProfileEvents::MergeTreeLeaderElectionLeaseParseErrors);
        ParsedLease error;
        error.status = LeaseParseStatus::ParseError;
        return error;
    }
}

String MergeTreeLeaderElection::generateLeaderId()
{
    return getFQDNOrHostName() + ":" + toString(ServerUUID::get());
}

void MergeTreeLeaderElection::observeLeaseWrite(const ParsedLease & lease, std::chrono::steady_clock::time_point observed_at)
{
    /// A lease that does not record the number of its write cannot be named in a reader file.
    if (lease.incarnation.empty())
        return;

    std::lock_guard lock(reader_mutex);

    /// One entry per write, with the time it was first seen: the earlier, the more views it applies to.
    if (!observed_lease_writes.empty()
        && observed_lease_writes.back().incarnation == lease.incarnation
        && observed_lease_writes.back().sequence == lease.sequence)
        return;

    observed_lease_writes.push_back(ObservedLeaseWrite{lease.incarnation, lease.sequence, observed_at});

    /// The entries are dropped once a newer view is published. If no view is published for long (the refresh of
    /// the parts fails), dropping the oldest ones can only make the node name an older write than it could, or none.
    if (observed_lease_writes.size() > MAX_OBSERVED_LEASE_WRITES)
        observed_lease_writes.pop_front();
}

void MergeTreeLeaderElection::resetReaderView()
{
    std::lock_guard lock(reader_mutex);
    reader_view_lease_incarnation.clear();
    reader_view_lease_sequence = 0;
    reader_pinned_parts.clear();
}

void MergeTreeLeaderElection::publishReaderView(std::chrono::steady_clock::time_point view_time, Names pinned_parts)
{
    std::lock_guard lock(reader_mutex);

    /// Only a follower has a set of parts built from a listing. The state is read under the mutex: `run` publishes
    /// the new state before it calls `resetReaderView`, so a refresh that was still running when this node acquired
    /// leadership either is undone by that reset or sees the new state here.
    if (leadership_state.load(std::memory_order_acquire) != LeadershipState::Follower)
        return;

    /// The newest write of the lease that this node had read before the listing started: the listing shows
    /// everything that the leader had committed before it started that write. If there is none, the previous
    /// value stays: a newer listing shows at least what an older one showed.
    for (size_t i = observed_lease_writes.size(); i-- > 0;)
    {
        if (observed_lease_writes[i].observed_at <= view_time)
        {
            reader_view_lease_incarnation = observed_lease_writes[i].incarnation;
            reader_view_lease_sequence = observed_lease_writes[i].sequence;
            /// The older writes are not needed anymore. This one is kept for a later view that finds no newer one.
            observed_lease_writes.erase(observed_lease_writes.begin(), observed_lease_writes.begin() + static_cast<std::ptrdiff_t>(i));
            break;
        }
    }

    reader_pinned_parts = std::move(pinned_parts);
}

String MergeTreeLeaderElection::buildReaderFileContent()
{
    std::lock_guard lock(reader_mutex);

    /// `sequence` and `incarnation` make the content, and thus the ETag, of every write unique, like in the lease:
    /// the leader tells a live reader from a dead one by the ETag of its file.
    const FormatSettings format_settings;
    WriteBufferFromOwnString out;
    writeString(R"({"version":1,"reader_id":)", out);
    writeJSONString(leader_id, out, format_settings);
    writeString(R"(,"incarnation":)", out);
    writeJSONString(incarnation, out, format_settings);
    writeString(R"(,"sequence":)", out);
    writeIntText(++reader_write_sequence, out);
    /// The leader must not consider this file expired sooner than this, even if its own `session_timeout` is shorter.
    writeString(R"(,"session_timeout_ms":)", out);
    writeIntText(session_timeout_ms, out);
    /// Empty and zero until the first view is published: the file then keeps everything.
    writeString(R"(,"lease_incarnation":)", out);
    writeJSONString(reader_view_lease_incarnation, out, format_settings);
    writeString(R"(,"lease_sequence":)", out);
    writeIntText(reader_view_lease_sequence, out);
    writeString(R"(,"pinned_parts":[)", out);
    for (size_t i = 0; i < reader_pinned_parts.size(); ++i)
    {
        if (i)
            writeChar(',', out);
        writeJSONString(reader_pinned_parts[i], out, format_settings);
    }
    writeString("]}", out);
    return out.str();
}

void MergeTreeLeaderElection::writeReaderFile()
{
    String content = buildReaderFileContent();

    auto write_settings = context->getWriteSettings();
    /// As for the lease: a tiny object that is rewritten all the time must not go through the filesystem cache.
    write_settings.enable_filesystem_cache_on_write_operations = false;

    auto buffer = object_storage->writeObject(
        StoredObject(reader_path),
        WriteMode::Rewrite,
        /* attributes= */ std::nullopt,
        DBMS_DEFAULT_BUFFER_SIZE,
        write_settings);

    buffer->write(content.data(), content.size());
    buffer->finalize();
}

void MergeTreeLeaderElection::registerReader()
{
    writeReaderFile();
}

void MergeTreeLeaderElection::unregisterReader() const
{
    removeReaderFileNoThrow();
}

void MergeTreeLeaderElection::removeReaderFileNoThrow() const
{
    try
    {
        object_storage->removeObjectIfExists(StoredObject(reader_path));
    }
    catch (...)
    {
        /// The leader skips its own reader file, and the other nodes ignore it once it has expired.
        tryLogCurrentException(log, fmt::format("Cannot remove the reader file at '{}'", reader_path));
    }
}

MergeTreeLeaderElection::ObservedReaderFile MergeTreeLeaderElection::parseReaderFile(const String & reader_key, const String & content) const
{
    ObservedReaderFile observed;
    observed.reader.id = reader_key;

    try
    {
        JSON json(content);

        /// A newer binary may keep parts in a way this one does not understand, so its file keeps everything.
        Int64 version = json["version"].getInt();
        if (version != 1)
        {
            LOG_WARNING(log, "Reader file '{}' has unknown version {}; no outdated part is removed while it is fresh", reader_key, version);
            observed.reader.keeps_everything = true;
            return observed;
        }

        if (json.has("session_timeout_ms"))
            observed.session_timeout_ms = json["session_timeout_ms"].getUInt();

        const String lease_incarnation = json["lease_incarnation"].getString();
        observed.reader.names_own_lease = !lease_incarnation.empty() && lease_incarnation == incarnation;
        observed.reader.lease_sequence = json["lease_sequence"].getUInt();

        const JSON pinned_parts = json["pinned_parts"];
        for (auto && pinned_part : pinned_parts)
            observed.reader.pinned_parts.insert(pinned_part.getString());
    }
    catch (...)
    {
        /// The node keeps rewriting its file, so this is not permanent unless the node is broken; and the file
        /// of a node that is gone expires.
        tryLogCurrentException(log, fmt::format("Cannot parse the reader file '{}'; no outdated part is removed while it is fresh", reader_key));
        observed.reader.keeps_everything = true;
        observed.reader.pinned_parts.clear();
    }

    return observed;
}

MergeTreeLeaderElection::ReadersSnapshot MergeTreeLeaderElection::scanReaders()
{
    std::lock_guard lock(readers_scan_mutex);

    RelativePathsWithMetadata files;
    object_storage->listObjects(readers_path_prefix, files, /* max_keys= */ 0);

    /// Sampled after the listing returned, like the observation time of the lease in `run`: a reader file is
    /// considered expired only after its ETag stayed unchanged for the session timeout of our own steady time,
    /// counted from when we first saw that ETag, which is after its writer anchored its own freshness.
    const auto steady_now = std::chrono::steady_clock::now();

    auto read_settings = context->getReadSettings();
    read_settings.enable_filesystem_cache = false;

    ReadersSnapshot snapshot;
    std::unordered_set<String> listed_keys;
    size_t expired_readers = 0;

    for (const auto & file : files)
    {
        const String & key = file->relative_path;

        /// A leftover of the time this node was a follower: the leader holds its parts by itself.
        if (key == reader_path)
            continue;

        listed_keys.insert(key);

        /// The token that changes with every write of the file. An object storage that does not report an ETag
        /// in the listing gets the content itself, which is unique for every write.
        String version_token = file->metadata ? file->metadata->etag : String();
        std::optional<String> content;
        const bool too_large = file->metadata && file->metadata->is_size_known && file->metadata->size_bytes > MAX_READER_FILE_SIZE;
        if (version_token.empty() && !too_large)
        {
            content = object_storage->readSmallObjectAndGetObjectMetadata(StoredObject(key), read_settings, MAX_READER_FILE_SIZE).data;
            version_token = *content;
        }

        auto it = observed_reader_files.find(key);
        if (it == observed_reader_files.end() || it->second.etag != version_token)
        {
            ObservedReaderFile observed;
            if (too_large)
            {
                LOG_WARNING(log, "Reader file '{}' is {} bytes, larger than the maximum of {} bytes; no outdated part is removed while it is fresh",
                    key, file->metadata->size_bytes, MAX_READER_FILE_SIZE);
                observed.reader.id = key;
                observed.reader.keeps_everything = true;
            }
            else
            {
                if (!content)
                    content = object_storage->readSmallObjectAndGetObjectMetadata(StoredObject(key), read_settings, MAX_READER_FILE_SIZE).data;
                observed = parseReaderFile(key, *content);
            }

            /// The content may be of a later write than the listed ETag. The next scan then sees another ETag and
            /// reads the file again; meanwhile a newer content only says more precisely what the reader keeps now.
            observed.etag = std::move(version_token);
            observed.etag_since = steady_now;
            it = observed_reader_files.insert_or_assign(key, std::move(observed)).first;
        }

        const auto & reader_file = it->second;
        const auto timeout = std::chrono::milliseconds(std::max(session_timeout_ms, reader_file.session_timeout_ms));
        const auto unchanged_for = steady_now - reader_file.etag_since;
        if (unchanged_for >= timeout)
        {
            /// Its node did not rewrite it for the whole session timeout: the node is gone or stalled, and, like a
            /// leader in that state, it cannot rely on its registration anymore.
            ///
            /// The file is only ignored, never removed from here: the removal cannot be made conditional on the
            /// version that was observed, and the key is the same for every instance of the table on that node,
            /// so it could remove the registration of a node that has just come back. A node removes its own file
            /// when it becomes the leader and when the table is dropped on it.
            ++expired_readers;
            continue;
        }

        snapshot.readers.push_back(reader_file.reader);
    }

    std::erase_if(observed_reader_files, [&](const auto & entry) { return !listed_keys.contains(entry.first); });

    LOG_TRACE(log, "Found {} reader files at '{}': {} fresh, {} expired",
        listed_keys.size(), readers_path_prefix, snapshot.readers.size(), expired_readers);

    return snapshot;
}

bool MergeTreeLeaderElection::ReadersSnapshot::allowsRemoval(const String & part_name, UInt64 removal_sequence) const
{
    for (const auto & reader : readers)
    {
        if (reader.keeps_everything)
            return false;

        /// The reader has not listed the storage since this leader instance started to write the lease, or it
        /// listed it before the write that followed `removal_sequence` started. The part may still be active for it,
        /// and then any of its queries may take the part.
        if (!reader.names_own_lease || reader.lease_sequence <= removal_sequence)
            return false;

        /// The part is not active for the reader, but its queries still hold it, or it could not load the replacement.
        if (reader.pinned_parts.contains(part_name))
            return false;
    }

    return true;
}

}
