#include <Interpreters/TransactionSession.h>

#include <algorithm>
#include <chrono>
#include <filesystem>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/SipHash.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>

namespace fs = std::filesystem;

namespace DB
{

namespace
{
    /// Steady clock: a wall clock stepping backwards would stall dead-replica detection.
    Int64 monotonicNowMs()
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }

    struct ReplicaScan
    {
        struct SessionState
        {
            ZooKeeperNodeVersion session_version;
            bool already_marked_dead = false;
        };
        std::unordered_set<UUID> seen_active;
        std::unordered_map<UUID, SessionState> session_states;
    };

    ReplicaScan parseSessionState(
        const Coordination::ListResponse & list_resp, const String & parent_path, const LoggerPtr & log)
    {
        ReplicaScan scan;
        for (size_t i = 0; i < list_resp.names.size(); ++i)
        {
            const String & child = list_resp.names[i];
            static constexpr size_t UUID_LEN = 36;
            if (child.size() < UUID_LEN + 1)
                continue;

            UUID replica_id;
            try { replica_id = parseFromString<UUID>(child.substr(0, UUID_LEN)); }
            catch (...)
            {
                LOG_WARNING(log, "Skipping unexpected Keeper child '{}' under {} (unparsable UUID prefix)", child, parent_path);
                continue;
            }

            const std::string_view suffix = std::string_view(child).substr(UUID_LEN);
            if (suffix == "_active")
                scan.seen_active.insert(replica_id);
            else if (suffix == "_session")
                scan.session_states[replica_id] = {ZooKeeperNodeVersion(list_resp.stats[i].version), list_resp.data[i] == "DEAD"};
        }
        return scan;
    }

    constexpr UInt64 INVALID_TID_RECORD_FORMAT_V1 = 1;

    /// `<format> <tid>[ <reason>]`. The reason is never parsed back.
    String serializeInvalidTidRecord(const TransactionID & tid, const String & reason = {})
    {
        WriteBufferFromOwnString buf;
        writeText(INVALID_TID_RECORD_FORMAT_V1, buf);
        writeChar(' ', buf);
        TransactionID::write(tid, buf);
        if (!reason.empty())
        {
            writeChar(' ', buf);
            writeString(reason, buf);
        }
        return buf.str();
    }

    std::optional<TransactionID> parseInvalidTidRecord(const String & data)
    {
        try
        {
            ReadBufferFromString buf{data};
            UInt64 version = 0;
            readText(version, buf);
            if (version != INVALID_TID_RECORD_FORMAT_V1)
                return std::nullopt;
            assertChar(' ', buf);
            return TransactionID::read(buf);
        }
        catch (...)
        {
            /// Skip a malformed record rather than abort cleanup.
            tryLogCurrentException(
                getLogger("TransactionSession"),
                fmt::format("Failed to parse invalid_tid record, content: '{}'", data));
            return std::nullopt;
        }
    }

    /// `Tx::MainJobId` (0) maps to the plain TID hash, so a whole-transaction key reads exactly like
    /// a TID hash; a non-zero job mixes in the job to get a key of its own.
    TIDHash invalidationKey(TIDHash tid_hash, JobId job_id)
    {
        if (job_id == Tx::MainJobId)
            return tid_hash;
        SipHash hash;
        hash.update(tid_hash);
        hash.update(job_id);
        return hash.get64();
    }
}

TransactionSession::TransactionSession(
    String replicas_path_, String invalid_tids_path_, UUID my_replica_id_, Int64 dead_replica_threshold_ms_, LoggerPtr log_)
    : replicas_path(std::move(replicas_path_))
    , invalid_tids_path(std::move(invalid_tids_path_))
    , my_replica_id(my_replica_id_)
    , dead_replica_threshold_ms(dead_replica_threshold_ms_)
    , log(std::move(log_))
{
}

String TransactionSession::replicaTailPtrPath() const { return fs::path(replicas_path) / (toString(my_replica_id) + "_tail_ptr"); }
String TransactionSession::replicaActivePath() const  { return fs::path(replicas_path) / (toString(my_replica_id) + "_active"); }
String TransactionSession::replicaSessionPath() const { return fs::path(replicas_path) / (toString(my_replica_id) + "_session"); }

String TransactionSession::replicaTailPtrPath(const UUID & id) const { return fs::path(replicas_path) / (toString(id) + "_tail_ptr"); }
String TransactionSession::replicaActivePath(const UUID & id) const  { return fs::path(replicas_path) / (toString(id) + "_active"); }
String TransactionSession::replicaSessionPath(const UUID & id) const { return fs::path(replicas_path) / (toString(id) + "_session"); }

void TransactionSession::initSessionNode(const zkutil::ZooKeeperPtr & zookeeper)
{
    zookeeper->createAncestors(replicas_path + "/");

    /// Overwrite unconditionally and read the version back from the write. A guarded write would
    /// race `markDeadReplicas`, and `ZBADVERSION` here lands before `_active` exists, so nothing
    /// would repair it.
    zookeeper->createIfNotExists(replicaSessionPath(), toString(my_replica_id));
    Coordination::Stat stat;
    zookeeper->set(replicaSessionPath(), toString(my_replica_id), -1, &stat);
    session_node_version.store(ZooKeeperNodeVersion(stat.version));
}

void TransactionSession::initInvalidTidsNode(const zkutil::ZooKeeperPtr & zookeeper)
{
    zookeeper->createAncestors(invalid_tids_path + "/");
}

void TransactionSession::createActiveNode(const zkutil::ZooKeeperPtr & zookeeper)
{
    /// A session that is not reaped yet still owns its node, and a bare create would fail with
    /// `ZNODEEXISTS`. The replica UUID is ours, so we are the rightful owner.
    if (zookeeper->tryRemove(replicaActivePath()) == Coordination::Error::ZOK)
        LOG_DEBUG(log, "Removed a stale _active node at {}", replicaActivePath());
    active_node_holder = zkutil::EphemeralNodeHolder::create(replicaActivePath(), *zookeeper, "");
}

void TransactionSession::releaseActiveNode()
{
    active_node_holder.reset();
}

std::optional<ZooKeeperNodeVersion> TransactionSession::renewSession(const zkutil::ZooKeeperPtr & zookeeper)
{
    /// `_active` first, or a peer can declare us dead again between the two writes.
    createActiveNode(zookeeper);
    return updateSessionVersionIfChanged(zookeeper, SessionCheck::AfterReconnect);
}

std::optional<ZooKeeperNodeVersion> TransactionSession::updateSessionVersionIfChanged(
    const zkutil::ZooKeeperPtr & zookeeper, SessionCheck check)
{
    Coordination::Stat stat;
    String content;
    if (!zookeeper->tryGet(replicaSessionPath(), content, &stat))
        return std::nullopt;

    const ZooKeeperNodeVersion observed_version{stat.version};
    if (observed_version == session_node_version.load())
        return std::nullopt;

    const std::string_view when = check == SessionCheck::AfterReconnect
        ? "during disconnection" : "while we were starting up";
    LOG_WARNING(log, "Our _session node version changed from {} to {} — a peer declared us dead {}. "
        "Claiming a new session.", session_node_version.load(), observed_version, when);
    Coordination::Stat claimed;
    zookeeper->set(replicaSessionPath(), toString(my_replica_id), stat.version, &claimed);
    return ZooKeeperNodeVersion{claimed.version};
}

void TransactionSession::loadReplicaMap(const zkutil::ZooKeeperPtr & zookeeper)
{
    /// Children of `/replicas` with stat and data in one round-trip, rather than 1 list + N
    /// `tryGet`. A `_session` child holds the peer's UUID, or "DEAD" if a peer marked it.
    auto responses = zookeeper->tryGetChildren(
        std::vector<std::string>{replicas_path},
        Coordination::ListRequestType::ALL,
        /*with_stat=*/true,
        /*with_data=*/true);
    const auto & list_resp = responses[0];
    if (list_resp.error != Coordination::Error::ZOK)
        return;

    const auto [seen_active, session_states] = parseSessionState(list_resp, replicas_path, log);

    Int64 now_ms = monotonicNowMs();

    std::lock_guard lock{replicas_mutex};
    for (auto & [rid, info] : replica_info_map)
    {
        if (auto it = session_states.find(rid); it != session_states.end())
        {
            info.session_node_version = it->second.session_version;
            info.already_marked_dead = it->second.already_marked_dead;
        }
        if (seen_active.contains(rid))
            info.last_active_ts_ms = now_ms;
    }
    for (const auto & [rid, state] : session_states)
    {
        if (!replica_info_map.contains(rid))
        {
            ReplicaInfo info;
            info.session_node_version = state.session_version;
            info.already_marked_dead = state.already_marked_dead;
            /// Seed with now_ms even though `_active` was not seen: left at 0, the replica stays
            /// invisible to `markDeadReplicas` forever and freezes `global_min` cleanup.
            info.last_active_ts_ms = now_ms;
            replica_info_map.emplace(rid, info);
        }
    }

    /// Still missing after a fresh scan is a real anomaly, so warn. One that appeared was cache lag.
    for (auto it = replicas_pending_session_check.begin(); it != replicas_pending_session_check.end();)
    {
        if (!replica_info_map.contains(it->first))
            LOG_WARNING(
                log,
                "Cannot determine session state for TID {} — replica {} absent from replica_info_map "
                "after a fresh scan. Treating session as alive.",
                it->second,
                it->first);
        it = replicas_pending_session_check.erase(it);
    }
}

void TransactionSession::markDeadReplicas(const zkutil::ZooKeeperPtr & zookeeper)
{
    Int64 now_ms = monotonicNowMs();
    /// The observed `_session` version travels with each candidate, so the Set below is guarded.
    std::vector<std::pair<UUID, ZooKeeperNodeVersion>> dead_candidates;
    {
        std::lock_guard lock{replicas_mutex};
        for (const auto & [rid, info] : replica_info_map)
        {
            if (rid == my_replica_id)
                continue;
            /// Already "DEAD": a prior pass marked it, and re-firing would just bump the version
            /// every iteration until the peer reconnects and writes its UUID back.
            if (info.already_marked_dead)
                continue;
            if (info.last_active_ts_ms > 0 && now_ms - info.last_active_ts_ms > dead_replica_threshold_ms)
            {
                LOG_INFO(log, "Replica {} is a dead candidate: last_active_ts {} ms ago (> {} ms threshold), session_v {}",
                    rid, now_ms - info.last_active_ts_ms, dead_replica_threshold_ms, info.session_node_version);
                dead_candidates.emplace_back(rid, info.session_node_version);
            }
        }
    }

    for (const auto & [rid, observed_session_version] : dead_candidates)
    {
        /// Create _active (fails if the replica reconnected) → bump _session → remove _active, as
        /// one Multi. The Set is version-guarded, so a peer that bumped `_session` after we read it
        /// keeps its own version and we retry next pass with a fresh map.
        Coordination::Requests ops;
        ops.push_back(zkutil::makeCreateRequest(replicaActivePath(rid), "", zkutil::CreateMode::Ephemeral));
        ops.push_back(zkutil::makeSetRequest(replicaSessionPath(rid), "DEAD", observed_session_version.toInt32()));
        ops.push_back(zkutil::makeRemoveRequest(replicaActivePath(rid), -1));

        Coordination::Responses responses;
        auto code = zookeeper->tryMulti(ops, responses);
        if (code == Coordination::Error::ZOK)
        {
            /// The new version correlates this with later takeovers of locks stamped by the dead TIDs.
            const auto * set_response = dynamic_cast<const Coordination::SetResponse *>(responses[1].get());
            const int32_t new_session_v = set_response ? set_response->stat.version : -1;
            LOG_WARNING(log, "Marked replica {} as dead (absent for >{}ms); _session bumped to version {}",
                rid, dead_replica_threshold_ms, new_session_v);
        }
        else if (code == Coordination::Error::ZNODEEXISTS)
            LOG_INFO(log, "Replica {} reconnected before we could mark it dead", rid);
        else if (code == Coordination::Error::ZBADVERSION)
            LOG_INFO(log, "Replica {} bumped its _session (version was {}) before we could mark it dead", rid, observed_session_version);
        else
            LOG_WARNING(log, "Failed to mark replica {} as dead: {}", rid, Coordination::toString(code));
    }
}

bool TransactionSession::isTIDInvalid(const TransactionID & tid, JobId job_id) const
{
    {
        /// A job is invalid once its whole transaction is, and that invalidation is keyed by
        /// `Tx::MainJobId`, so check both keys.
        std::lock_guard lock{invalid_tids_mutex};
        if (invalid_tids.contains(invalidationKey(tid.getHash(), job_id))
            || invalid_tids.contains(invalidationKey(tid.getHash(), Tx::MainJobId)))
            return true;
    }

    /// Our own session was bumped by a restart or by a peer's `markDeadReplicas`.
    if (tid.host_id == my_replica_id)
        return ZooKeeperNodeVersion{tid.session_node_version} < session_node_version.load();

    std::lock_guard lock{replicas_mutex};
    auto it = replica_info_map.find(tid.host_id);
    if (it == replica_info_map.end())
    {
        /// Usually bootstrap lag. `loadReplicaMap` re-checks and warns if it is not.
        replicas_pending_session_check.emplace(tid.host_id, tid);
        LOG_DEBUG(
            log,
            "Session state for TID {} unknown — replica {} not in cache yet. Treating as alive.",
            tid,
            tid.host_id);
        return false;
    }
    replicas_pending_session_check.erase(tid.host_id);
    return ZooKeeperNodeVersion{tid.session_node_version} < it->second.session_node_version;
}

bool TransactionSession::isInvalidated(TIDHash tid_hash) const
{
    std::lock_guard lock{invalid_tids_mutex};
    return invalid_tids.contains(tid_hash);
}

void TransactionSession::invalidateTID(const TransactionID & tid, JobId job_id, const String & reason) noexcept
{
    std::lock_guard lock{invalid_tids_mutex};
    /// In memory now so this replica acts on it immediately; the worker publishes it to Keeper for
    /// the others.
    invalid_tids.insert(invalidationKey(tid.getHash(), job_id));
    const auto same_entry = [&](const PendingInvalidation & e) { return e.tid == tid && e.job_id == job_id; };
    if (std::find_if(pending_invalid_store.begin(), pending_invalid_store.end(), same_entry) == pending_invalid_store.end())
        pending_invalid_store.push_back(PendingInvalidation{tid, job_id, reason});
}

void TransactionSession::markTIDInvalidInMemory(const TransactionID & tid, JobId job_id) noexcept
{
    std::lock_guard lock{invalid_tids_mutex};
    invalid_tids.insert(invalidationKey(tid.getHash(), job_id));
}

String TransactionSession::getInvalidTIDRecordPath(const TransactionID & tid, JobId job_id) const
{
    return invalid_tids_path + "/" + toString(invalidationKey(tid.getHash(), job_id));
}

Coordination::RequestPtr TransactionSession::makeInvalidateTIDRequest(
    const TransactionID & tid, JobId job_id, const String & reason) const
{
    return zkutil::makeCreateRequest(
        getInvalidTIDRecordPath(tid, job_id), serializeInvalidTidRecord(tid, reason), zkutil::CreateMode::Persistent);
}

void TransactionSession::storePendingInvalidTids(const zkutil::ZooKeeperPtr & zookeeper)
{
    std::vector<PendingInvalidation> to_store;
    {
        std::lock_guard lock{invalid_tids_mutex};
        to_store = pending_invalid_store;  /// Keep the entries queued until Keeper confirms the write.
    }
    if (to_store.empty())
        return;

    std::vector<TIDHash> stored;
    ZooKeeperRetriesControl retries{
        "TransactionSession::storePendingInvalidTids", log,
        ZooKeeperRetriesInfo{/*max_retries=*/10, /*initial_backoff_ms=*/100, /*max_backoff_ms=*/5000, /*query_status=*/nullptr}};
    retries.retryLoop([&]
    {
        auto key = [&](const PendingInvalidation & e) { return invalidationKey(e.tid.getHash(), e.job_id); };
        auto record_path = [&](const PendingInvalidation & e) { return invalid_tids_path + "/" + toString(key(e)); };
        auto mark_stored = [&](const PendingInvalidation & e) { stored.push_back(key(e)); };
        auto already_stored = [&](const PendingInvalidation & e)
        { return std::find(stored.begin(), stored.end(), key(e)) != stored.end(); };

        Coordination::Requests ops;
        std::vector<const PendingInvalidation *> batch;
        for (const auto & entry : to_store)
        {
            if (already_stored(entry))
                continue;
            ops.push_back(zkutil::makeCreateRequest(
                record_path(entry), serializeInvalidTidRecord(entry.tid, entry.reason), zkutil::CreateMode::Persistent));
            batch.push_back(&entry);
        }
        if (ops.empty())
            return;

        Coordination::Responses resp;
        const Coordination::Error code = zookeeper->tryMulti(ops, resp);
        if (code == Coordination::Error::ZOK)
        {
            for (const auto * entry : batch)
                mark_stored(*entry);
            return;
        }
        if (Coordination::isHardwareError(code))
            throw Coordination::Exception::fromMessage(code, "invalid_tids Multi failed");

        /// The Multi is all-or-nothing, so one already-existing record fails the whole batch and
        /// nothing is created. Retry each on its own so it doesn't block the rest.
        for (const auto * entry : batch)
        {
            const Coordination::Error c = zookeeper->tryCreate(
                record_path(*entry), serializeInvalidTidRecord(entry->tid, entry->reason), zkutil::CreateMode::Persistent);
            if (c == Coordination::Error::ZOK || c == Coordination::Error::ZNODEEXISTS)
                mark_stored(*entry);
            else if (Coordination::isHardwareError(c))
                throw Coordination::Exception::fromMessage(c, "create invalid_tids failed");
            else
                LOG_WARNING(log, "Failed to store invalid TID {} (job {}): {}", entry->tid, entry->job_id, Coordination::toString(c));
        }
    });

    if (!stored.empty())
    {
        std::lock_guard lock{invalid_tids_mutex};
        std::erase_if(pending_invalid_store,
            [&](const PendingInvalidation & e)
            { return std::find(stored.begin(), stored.end(), invalidationKey(e.tid.getHash(), e.job_id)) != stored.end(); });
    }
}

void TransactionSession::loadInvalidTids(const zkutil::ZooKeeperPtr & zookeeper)
{
    Strings children;
    if (zookeeper->tryGetChildren(invalid_tids_path, children) != Coordination::Error::ZOK)
        return;

    std::unordered_set<TIDHash> fresh;
    fresh.reserve(children.size());
    for (const String & name : children)
        fresh.insert(parseFromString<TIDHash>(name));

    std::lock_guard lock{invalid_tids_mutex};
    /// Keep what we invalidated but have not written yet, so the rebuild cannot forget it.
    for (const auto & entry : pending_invalid_store)
        fresh.insert(invalidationKey(entry.tid.getHash(), entry.job_id));
    invalid_tids = std::move(fresh);
}

void TransactionSession::evictInvalidTids(const zkutil::ZooKeeperPtr & zookeeper)
{
    auto responses = zookeeper->tryGetChildren(
        std::vector<std::string>{invalid_tids_path},
        Coordination::ListRequestType::ALL,
        /*with_stat=*/false,
        /*with_data=*/true);
    const auto & resp = responses[0];
    if (resp.error != Coordination::Error::ZOK)
        return;

    std::unordered_map<UUID, ZooKeeperNodeVersion> replica_sessions;
    {
        std::lock_guard lock{replicas_mutex};
        for (const auto & [rid, info] : replica_info_map)
            replica_sessions[rid] = info.session_node_version;
    }
    const ZooKeeperNodeVersion own_session = session_node_version.load();

    std::vector<TIDHash> evicted;
    for (size_t i = 0; i < resp.names.size(); ++i)
    {
        const auto tid = parseInvalidTidRecord(resp.data[i]);
        if (!tid)
            continue;

        ZooKeeperNodeVersion owner_session;
        if (tid->host_id == my_replica_id)
            owner_session = own_session;
        else
        {
            auto it = replica_sessions.find(tid->host_id);
            if (it == replica_sessions.end())
                continue;  /// Owning replica unknown — can't prove the record redundant, so keep it.
            owner_session = it->second;
        }

        if (ZooKeeperNodeVersion{tid->session_node_version} < owner_session)
        {
            zookeeper->tryRemove(invalid_tids_path + "/" + resp.names[i]);
            evicted.push_back(parseFromString<TIDHash>(resp.names[i]));
        }
    }

    if (!evicted.empty())
    {
        std::lock_guard lock{invalid_tids_mutex};
        for (const TIDHash & hash : evicted)
            invalid_tids.erase(hash);
    }
}

}
