#include <Storages/MergeTree/MergeTreeLeaderElection.h>

#include <Core/ServerUUID.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>
#include <Common/CurrentMetrics.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <base/JSON.h>
#include <base/getFQDNOrHostName.h>


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

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int AZURE_BLOB_STORAGE_ERROR;
    extern const int TABLE_IS_READ_ONLY;
}

static constexpr size_t MAX_LEASE_FILE_SIZE = 4096;

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
    task = context->getSchedulePool().createTask(storage_id, "MergeTreeLeaderElection", [this] { run(); });
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
    /// Block user writes first, before clearing `is_leader`, so a concurrent
    /// `INSERT` observing `is_leader == true` cannot also observe `writes_enabled == true`.
    writes_enabled.store(false, std::memory_order_release);
    bool was_leader = is_leader.exchange(false, std::memory_order_acq_rel);
    if (was_leader)
    {
        ProfileEvents::increment(ProfileEvents::MergeTreeLeaderElectionLost);
        CurrentMetrics::sub(CurrentMetrics::MergeTreeLeaderElectionLeader);
        CurrentMetrics::add(CurrentMetrics::MergeTreeLeaderElectionFollower);
        if (on_leadership_change)
            on_leadership_change(false);
    }
}

bool MergeTreeLeaderElection::isLeader() const
{
    if (!is_leader.load(std::memory_order_acquire))
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
    /// Order matters: `is_leader` is the cheaper, more frequently flipped check.
    /// `writes_enabled` is only true if takeover sync has completed at least once
    /// since the lease was acquired, so any `INSERT` reaching the second load is
    /// safe to commit against the current part view.
    return isLeader() && writes_enabled.load(std::memory_order_acquire);
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
            became_leader = tryWriteLease(/* if_match= */ "", /* if_none_match= */ "*");
        }
        else
        {
            /// Lease file exists. Read its content and ETag.
            auto data_with_metadata = object_storage->readSmallObjectAndGetObjectMetadata(
                StoredObject(lease_path), read_settings, MAX_LEASE_FILE_SIZE);

            String etag = data_with_metadata.metadata.etag;
            auto parsed = parseLeaseContent(data_with_metadata.data);

            time_t now = time(nullptr);
            time_t session_timeout_seconds = static_cast<time_t>(session_timeout_ms / 1000);

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
            else if (parsed.status == LeaseParseStatus::Ok && parsed.leader_id == leader_id)
            {
                /// We are the current leader. Renew the lease.
                LOG_TRACE(log, "Renewing leader lease at '{}'", lease_path);
                was_renewal_attempt = true;
                became_leader = tryWriteLease(/* if_match= */ etag, /* if_none_match= */ "");
            }
            else if (parsed.status == LeaseParseStatus::ParseError
                     || parsed.timestamp < now - session_timeout_seconds
                     || parsed.timestamp > now + session_timeout_seconds)
            {
                /// The lease has expired, or its timestamp is far in the future (leader clock
                /// skew — otherwise a follower would wait indefinitely until local time catches
                /// up), or the content was corrupted. Try to claim leadership.
                ///
                /// The two timestamp comparisons are written as `parsed.timestamp < now - X` /
                /// `parsed.timestamp > now + X` (rather than `now - parsed.timestamp > X`) to
                /// avoid signed overflow when a malformed lease parses to an extreme timestamp
                /// value. `now ± X` is safe because `now` is the current Unix time and `X` is
                /// at most `leader_election_session_timeout`.
                LOG_INFO(log, "Leader lease at '{}' expired, corrupted, or has future timestamp (leader_id: {}), trying to claim",
                    lease_path, parsed.leader_id);
                became_leader = tryWriteLease(/* if_match= */ etag, /* if_none_match= */ "");
            }
            else
            {
                /// Another leader holds a valid lease.
                LOG_TRACE(log, "Another leader holds the lease at '{}' (leader_id: {}, age: {} s)",
                    lease_path, parsed.leader_id, now - parsed.timestamp);
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

        bool was_leader = is_leader.exchange(became_leader, std::memory_order_acq_rel);

        if (became_leader && !was_leader)
        {
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
                /// `writes_enabled` is still false at this point, so user `INSERT`s
                /// observing `is_leader == true` are still rejected by
                /// `assertIsLeaderAndWritable`. Only after the callback returns —
                /// when the part view reflects the previous leader's commits and the
                /// block-number counter has been advanced past them — do we publish
                /// the writable flag.
                TakeoverSyncScope sync_scope(*this);
                on_leadership_change(true);
            }
            writes_enabled.store(true, std::memory_order_release);
        }
        else if (!became_leader && was_leader)
        {
            /// Block user writes first, before clearing `is_leader`, so a concurrent
            /// `INSERT` cannot observe `is_leader == true && writes_enabled == true`
            /// during the loss path.
            writes_enabled.store(false, std::memory_order_release);
            LOG_INFO(log, "Lost leadership for lease at '{}'", lease_path);
            ProfileEvents::increment(ProfileEvents::MergeTreeLeaderElectionLost);
            CurrentMetrics::sub(CurrentMetrics::MergeTreeLeaderElectionLeader);
            CurrentMetrics::add(CurrentMetrics::MergeTreeLeaderElectionFollower);
            if (on_leadership_change)
                on_leadership_change(false);
        }
    }
    catch (...)
    {
        /// On any error, conservatively assume we are not the leader.
        ProfileEvents::increment(ProfileEvents::MergeTreeLeaderElectionHeartbeatErrors);
        std::lock_guard lock(leadership_change_mutex);
        /// Block user writes first, before clearing `is_leader`, mirroring the
        /// graceful loss path above. This matters if the exception was thrown by
        /// the takeover-sync callback itself: at that point `is_leader` is true and
        /// `writes_enabled` may have been set by a previous successful takeover,
        /// so we must clear both.
        writes_enabled.store(false, std::memory_order_release);
        bool was_leader = is_leader.exchange(false, std::memory_order_acq_rel);
        if (was_leader)
        {
            LOG_WARNING(log, "Lost leadership due to exception for lease at '{}'", lease_path);
            ProfileEvents::increment(ProfileEvents::MergeTreeLeaderElectionLost);
            CurrentMetrics::sub(CurrentMetrics::MergeTreeLeaderElectionLeader);
            CurrentMetrics::add(CurrentMetrics::MergeTreeLeaderElectionFollower);
            if (on_leadership_change)
                on_leadership_change(false);
        }

        tryLogCurrentException(log, "Error in leader election heartbeat");
    }

    if (!stopped.load(std::memory_order_acquire))
        task->scheduleAfter(heartbeat_interval_ms);
}

bool MergeTreeLeaderElection::tryWriteLease(const String & if_match, const String & if_none_match)
{
    try
    {
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
        last_renewal_time.store(std::chrono::steady_clock::now(), std::memory_order_release);

        return true;
    }
    catch (const Exception & e)
    {
        if ((e.code() == ErrorCodes::S3_ERROR || e.code() == ErrorCodes::AZURE_BLOB_STORAGE_ERROR)
            && (e.message().find("PreconditionFailed") != String::npos
                || e.message().find("ConditionNotMet") != String::npos))
        {
            LOG_TRACE(log, "Conditional write failed (precondition not met) for lease at '{}'", lease_path);
            ProfileEvents::increment(ProfileEvents::MergeTreeLeaderElectionLeaseConflicts);
            return false;
        }
        throw;
    }
}

String MergeTreeLeaderElection::buildLeaseContent() const
{
    WriteBufferFromOwnString out;
    writeString(R"({"version":1,"leader_id":")", out);
    writeString(leader_id, out);
    writeString(R"(","timestamp":)", out);
    writeIntText(time(nullptr), out);
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

}
