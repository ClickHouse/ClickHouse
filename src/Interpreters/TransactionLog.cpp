#include <Interpreters/TransactionLog.h>

#include <algorithm>
#include <filesystem>
#include <limits>
#include <numeric>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <base/defines.h>
#include <base/sort.h>
#include <fmt/ranges.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>
#include <Common/noexcept_scope.h>

namespace fs = std::filesystem;

namespace DB
{

namespace FailPoints
{
    extern const char tx_log_abort_cleanup_multi[];
}

TransactionLog::TransactionLog(
    const String & zookeeper_path_,
    const TransactionSession & session_,
    const std::atomic_bool & stop_flag_,
    LoggerPtr log_)
    : zookeeper_path_log(zookeeper_path_ + "/log")
    , zookeeper_path_tables(zookeeper_path_ + "/tables")
    , zookeeper_path_tables_stamp(zookeeper_path_ + "/tables/stamp_csn")
    , zookeeper_path_tables_processed(zookeeper_path_ + "/tables/processed_csn")
    , session(session_)
    , stop_flag(stop_flag_)
    , log(std::move(log_))
{
}

void TransactionLog::notifyUpdated()
{
    log_updated_event->set();
    latest_snapshot.notify_all();
}

void TransactionLog::initLogRoot(const zkutil::ZooKeeperPtr & zookeeper)
{
    {
        std::lock_guard lock{mutex};
        chassert(tid_to_csn.empty());
        chassert(last_loaded_csn == Tx::UnknownCSN);
    }

    /// The local-TID counter lives only in memory, so allocate a fresh CSN to start counting from
    /// and avoid duplicating TIDs of a previous run.
    Coordination::Error code = zookeeper->tryCreate(zookeeper_path_log + "/csn-", "", zkutil::CreateMode::PersistentSequential);
    if (code != Coordination::Error::ZOK)
    {
        /// Log probably does not exist, create it
        chassert(code == Coordination::Error::ZNONODE);
        zookeeper->createAncestors(zookeeper_path_log);
        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path_log, "", zkutil::CreateMode::Persistent));

        /// Fast-forward sequential counter to skip reserved CSNs
        for (size_t i = 0; i <= Tx::MaxReservedCSN; ++i)
            ops.emplace_back(zkutil::makeCreateRequest(zookeeper_path_log + "/csn-", "", zkutil::CreateMode::PersistentSequential));
        Coordination::Responses res;
        code = zookeeper->tryMulti(ops, res);
        if (code != Coordination::Error::ZNODEEXISTS)
            zkutil::KeeperMultiException::check(code, ops, res);
    }
}

void TransactionLog::initTableNodes(const zkutil::ZooKeeperPtr & zookeeper)
{
    zookeeper->createAncestors(zookeeper_path_tables_stamp + "/");
    zookeeper->createAncestors(zookeeper_path_tables_processed + "/");
}

void TransactionLog::waitForUpdate(size_t milliseconds)
{
    log_updated_event->tryWait(milliseconds);
}

CSN TransactionLog::lookupGapCSN(const TIDHash & tid_hash) const
{
    std::lock_guard lock{gap_csn_cache_mutex};
    auto it = gap_csn_cache.find(tid_hash);
    return it == gap_csn_cache.end() ? Tx::UnknownCSN : it->second;
}

void TransactionLog::restoreOwnTailPtr(const zkutil::ZooKeeperPtr & zookeeper)
{
    /// Do NOT seed `global_tail_ptr` here; it is computed from the live replica set.
    String own_tail_str;
    if (zookeeper->tryGet(session.replicaTailPtrPath(), own_tail_str))
        tail_ptr.store(Tx::deserializeCSN(own_tail_str));
}

void TransactionLog::publishSnapshot(CSN csn)
{
    latest_snapshot = csn;
    /// Wakes `waitForCSNLoaded`, which a synchronous COMMIT blocks on.
    latest_snapshot.notify_all();
}

void TransactionLog::assertLoaded() const
{
    std::lock_guard lock{mutex};
    chassert(last_loaded_csn != Tx::UnknownCSN);
    chassert(latest_snapshot == last_loaded_csn);
}

String TransactionLog::tableLastCommittedTidPath(Int64 cross_replica_id) const { return fs::path(zookeeper_path_tables_stamp) / std::to_string(cross_replica_id); }

String TransactionLog::tableProcessedCSNPath(Int64 cross_replica_id) const { return fs::path(zookeeper_path_tables_processed) / std::to_string(cross_replica_id); }

void TransactionLog::forgetDroppedTable(const zkutil::ZooKeeperPtr & zookeeper, Int64 cross_replica_id)
{
    /// `try_remove=true` lets either znode be already absent without rolling
    /// back the Multi. On any other failure (hardware error, etc.) the
    /// orphan sweep is the fallback — log and proceed with the in-memory
    /// cleanup so the floor on this replica advances regardless.
    Coordination::Requests ops;
    ops.emplace_back(zkutil::makeRemoveRequest(tableLastCommittedTidPath(cross_replica_id), -1, /*try_remove=*/true));
    ops.emplace_back(zkutil::makeRemoveRequest(tableProcessedCSNPath(cross_replica_id), -1, /*try_remove=*/true));
    Coordination::Responses responses;
    const auto code = zookeeper->tryMultiNoThrow(ops, responses);
    if (code != Coordination::Error::ZOK)
        LOG_WARNING(log, "forgetDroppedTable: Multi for cross_replica_id={} failed ({}); orphan sweep will retry",
            cross_replica_id, code);

    std::lock_guard lock{mutex};
    table_affected_csns.erase(cross_replica_id);
    table_stamp_csns.erase(cross_replica_id);
    table_processed_csns.erase(cross_replica_id);
}

std::optional<CSN> TransactionLog::loadEntries(const zkutil::ZooKeeperPtr & zookeeper, Strings::const_iterator beg, Strings::const_iterator end)
{
    size_t entries_count = std::distance(beg, end);
    if (!entries_count)
        return {};

    std::vector<std::string> entry_paths;
    entry_paths.reserve(entries_count);
    for (auto it = beg; it != end; ++it)
        entry_paths.emplace_back(fs::path(zookeeper_path_log) / *it);

    auto entries = zookeeper->get(entry_paths);

    Strings names(beg, end);
    Strings data;
    data.reserve(entries_count);
    for (size_t i = 0; i < entries_count; ++i)
        data.push_back(entries[i].data);

    return processCSNLogs(names, data);
}

std::optional<CSN> TransactionLog::processCSNLogs(const Strings & names, const Strings & data)
{
    chassert(names.size() == data.size());
    size_t entries_count = names.size();
    if (!entries_count)
        return {};

    const String & last_entry = names.back();
    LOG_TRACE(log, "Loading {} entries from {}: {}..{}", entries_count, zookeeper_path_log, names.front(), last_entry);

    std::vector<std::pair<TIDHash, CSNEntry>> loaded;
    loaded.reserve(entries_count);
    for (size_t i = 0; i < entries_count; ++i)
    {
        CSN csn = Tx::deserializeCSN(names[i]);
        auto entry_data = Tx::CSNEntryData::deserialize(data[i]);
        const TIDHash tid_hash = entry_data.tid.getHash();
        LOG_TEST(log, "Got entry {} -> {}", entry_data.tid, csn);
        loaded.emplace_back(tid_hash, CSNEntry{csn, std::move(entry_data)});
    }

    NOEXCEPT_SCOPE_STRICT({
        std::lock_guard lock{mutex};
        for (const auto & entry : loaded)
        {
            if (entry.first == Tx::EmptyTID.getHash())
                continue;

            tid_to_csn.emplace(entry.first, entry.second);
        }
        last_loaded_csn = loaded.back().second.csn;
    });

    return loaded.back().second.csn;
}
std::optional<CSN> TransactionLog::reloadCSNLogs(const zkutil::ZooKeeperPtr & zookeeper)
{
    /// `sync` first: a full reload published unconditionally, so a node trailing the leader would
    /// move `latest_snapshot` backwards. Pipelined with the read, so the fence is one round trip.
    auto log_sync = zookeeper->asyncSync(zookeeper_path_log);
    auto log_children = zookeeper->asyncTryGetChildren(
        zookeeper_path_log,
        Coordination::ListRequestType::ALL,
        /*with_stat=*/false,
        /*with_data=*/true);
    [[maybe_unused]] const auto log_synced = log_sync.get();
    auto log_resp = log_children.get();
    if (log_resp.error != Coordination::Error::ZOK)
        throw Coordination::Exception::fromMessage(log_resp.error,
            "reloadCSNLogs: failed to list " + zookeeper_path_log);
    chassert(!log_resp.names.empty());

    /// By CSN, not by name (see `serializeCSN`).
    std::vector<CSN> csns(log_resp.names.size());
    for (size_t i = 0; i < log_resp.names.size(); ++i)
        csns[i] = Tx::deserializeCSN(log_resp.names[i]);
    std::vector<size_t> order(log_resp.names.size());
    std::iota(order.begin(), order.end(), 0);
    std::sort(order.begin(), order.end(), [&](size_t a, size_t b) { return csns[a] < csns[b]; });
    Strings sorted_names;
    Strings sorted_data;
    sorted_names.reserve(order.size());
    sorted_data.reserve(order.size());
    for (size_t i : order)
    {
        sorted_names.push_back(std::move(log_resp.names[i]));
        sorted_data.push_back(std::move(log_resp.data[i]));
    }

    return processCSNLogs(sorted_names, sorted_data);
}

void TransactionLog::advanceOwnTailPtr(
    const zkutil::ZooKeeperPtr & zookeeper, CSN oldest_snapshot, CSN oldest_unfinalized_start_csn)
{
    CSN new_tail = std::min(oldest_snapshot, oldest_unfinalized_start_csn);
    CSN old_tail = tail_ptr.load();
    if (new_tail <= old_tail)
        return;

    LOG_TRACE(log, "Updating own tail_ptr from {} to {}", old_tail, new_tail);
    auto code = zookeeper->trySet(session.replicaTailPtrPath(), Tx::serializeCSN(new_tail));
    if (code == Coordination::Error::ZNONODE)
        code = zookeeper->tryCreate(session.replicaTailPtrPath(), Tx::serializeCSN(new_tail), zkutil::CreateMode::Persistent);
    if (code != Coordination::Error::ZOK)
    {
        LOG_WARNING(log, "Failed to persist own tail_ptr to Keeper ({}); will retry on next iteration", code);
        return;
    }
    tail_ptr.store(new_tail);
}

void TransactionLog::removeOldEntries(const zkutil::ZooKeeperPtr & zookeeper, const String & lease_path, int64_t lease_czxid)
{
    auto global_min_opt = computeGlobalMinTailPtr(zookeeper);
    if (!global_min_opt)
        return;

    const CSN global_min = *global_min_opt;

    const CSN prev_global_tail = global_tail_ptr.load();
    if (global_min <= prev_global_tail)
        return;

    const CSN latest_entry_csn = latest_snapshot.load();
    auto removable_list = collectRemovableEntries(global_min, latest_entry_csn);
    if (removable_list.empty())
        return;


    /// List `/log` children so we can skip csn-N entries another replica's
    /// cleanup already removed, which would otherwise trip ZNONODE in the Multi.
    auto list_responses = zookeeper->tryGetChildren(
        std::vector<std::string>{zookeeper_path_log},
        Coordination::ListRequestType::ALL,
        /*with_stat=*/false,
        /*with_data=*/false);
    const auto & log_list = list_responses[0];
    if (log_list.error != Coordination::Error::ZOK)
    {
        LOG_WARNING(log, "removeOldEntries: failed to list /log ({}); will retry next pass", log_list.error);
        return;
    }
    std::unordered_set<String> existing_log_entries(log_list.names.begin(), log_list.names.end());

    const CleanupPlan plan = computeCleanupPlan(removable_list);
    if (plan.log_removes.empty())
        return;

    /// One Multi for all writes. First op is CheckStat(czxid=my_lease_czxid)
    /// — atomically catches both a delete and a peer's re-create.
    Coordination::Stat lease_check{
        .czxid = lease_czxid,
        .mzxid = -1,
        .ctime = -1,
        .mtime = -1,
        .version = -1,
        .cversion = -1,
        .aversion = -1,
        .ephemeralOwner = -1,
        .dataLength = -1,
        .numChildren = -1,
        .pzxid = -1,
    };
    Coordination::Requests requests;
    requests.emplace_back(zkutil::makeCheckRequest(lease_path, /*version=*/-1, /*not_exists=*/false, std::move(lease_check)));

    for (const CSN csn : plan.log_removes)
    {
        const String name = Tx::serializeCSN(csn);
        /// Already removed by another replica's cleanup — `evictInMemoryPrefix`
        /// still drops the in-memory `tid_to_csn` entry after the Multi succeeds.
        if (!existing_log_entries.contains(name))
            continue;
        requests.emplace_back(zkutil::makeRemoveRequest(
            zookeeper_path_log + "/" + name, -1));
    }

    /// Test-only: simulate Multi abort so the next pass retries cleanly.
    fiu_do_on(FailPoints::tx_log_abort_cleanup_multi, { return; });

    Coordination::Responses responses;
    const auto err = zookeeper->tryMulti(requests, responses);
    if (err != Coordination::Error::ZOK)
    {
        /// Diagnostic: pinpoint which op tripped the rollback.
        for (size_t i = 0; i < responses.size() && i < requests.size(); ++i)
        {
            if (responses[i] && responses[i]->error != Coordination::Error::ZOK)
            {
                const String op_path = requests[i] ? requests[i]->getPath() : String("?");
                LOG_WARNING(log,
                    "removeOldEntries Multi op {}/{} failed: {} on path {}",
                    i, requests.size(), responses[i]->error, op_path);
            }
        }
        LOG_WARNING(log,
            "removeOldEntries Multi failed ({} ops): {}; will retry next pass",
            requests.size(), err);
        return;
    }

    evictInMemoryPrefix(removable_list, plan.log_removes.size());

    /// `std::max` keeps the watermark monotonic; a regression would make
    /// `assertTIDIsNotOutdated` accept outdated TIDs.
    global_tail_ptr.store(std::max(prev_global_tail, plan.new_watermark));

    LOG_INFO(log, "removeOldEntries: removed {} entries from /log, watermark {} -> {}",
        plan.log_removes.size(), prev_global_tail, global_tail_ptr.load());
}

void TransactionLog::pruneInMemoryEntriesRemovedFromLog(const zkutil::ZooKeeperPtr & zookeeper)
{
    Strings entries;
    if (zookeeper->tryGetChildren(zookeeper_path_log, entries) != Coordination::Error::ZOK)
        return;
    if (entries.empty())
        return;

    /// `/log` is only trimmed from the front, so the oldest surviving entry is the boundary. The
    /// reserved `csn-*` nodes are never removed, so counting them would pin the boundary at zero.
    CSN min_present = Tx::UnknownCSN;
    for (const auto & entry : entries)
    {
        const CSN csn = Tx::deserializeCSN(entry);
        if (csn > Tx::MaxReservedCSN && (min_present == Tx::UnknownCSN || csn < min_present))
            min_present = csn;
    }

    /// Only reserved nodes, so nothing has been trimmed yet.
    if (min_present == Tx::UnknownCSN || min_present <= last_pruned_below)
        return;

    {
        std::lock_guard lock{mutex};
        for (auto it = tid_to_csn.begin(); it != tid_to_csn.end();)
        {
            if (it->second.csn >= min_present)
            {
                ++it;
                continue;
            }
            /// Unindex `table_affected_csns` in lockstep, as `evictInMemoryPrefix` does.
            for (const auto & smt_row : it->second.data.smt)
            {
                auto map_it = table_affected_csns.find(smt_row.cross_replica_id);
                if (map_it == table_affected_csns.end())
                    continue;
                map_it->second.erase(it->second.csn);
                if (map_it->second.empty())
                    table_affected_csns.erase(map_it);
            }
            it = tid_to_csn.erase(it);
        }
    }

    last_pruned_below = min_present;
}

std::optional<CSN> TransactionLog::computeGlobalMinTailPtr(const zkutil::ZooKeeperPtr & zookeeper) const
{
    /// One round-trip: list `/replicas` with each child's data inline. The `_session`
    /// payload tells us whether to skip a dead replica; the `_tail_ptr` payload IS the
    /// CSN we need. Replaces 1 list + N×2 per-replica `tryGet` (one per `_session`
    /// plus one per `_tail_ptr`).
    auto responses = zookeeper->tryGetChildren(
        std::vector<std::string>{session.replicasPath()},
        Coordination::ListRequestType::ALL,
        /*with_stat=*/false,
        /*with_data=*/true);
    const auto & list_resp = responses[0];
    if (list_resp.error != Coordination::Error::ZOK)
        return std::nullopt;

    /// Classify by `_session`: dead replicas (data == "DEAD") are excluded so a crashed
    /// node doesn't freeze GC; live replicas must each contribute a `_tail_ptr` below.
    std::unordered_set<UUID> dead_replicas;
    std::unordered_set<UUID> live_replicas;
    static constexpr size_t UUID_LEN = 36;
    for (size_t i = 0; i < list_resp.names.size(); ++i)
    {
        const String & child = list_resp.names[i];
        if (child.size() < UUID_LEN + 1)
            continue;
        std::string_view suffix = std::string_view(child).substr(UUID_LEN);
        if (suffix != "_session")
            continue;
        UUID rid;
        try
        {
            rid = parseFromString<UUID>(child.substr(0, UUID_LEN));
        }
        catch (...)
        {
            LOG_WARNING(log, "Skipping unexpected Keeper child '{}' under {} (unparsable UUID prefix)",
                child, session.replicasPath());
            continue;
        }
        if (list_resp.data[i] == "DEAD")
            dead_replicas.insert(rid);
        else
            live_replicas.insert(rid);
    }

    /// Fold each live replica's `_tail_ptr` into `global_min`. Defer on empty data:
    /// GC would otherwise prune past what that replica still needs.
    CSN global_min = latest_snapshot.load();
    std::unordered_set<UUID> live_replicas_with_tail_ptr;
    for (size_t i = 0; i < list_resp.names.size(); ++i)
    {
        const String & child = list_resp.names[i];
        if (!child.ends_with("_tail_ptr"))
            continue;
        if (child.size() < UUID_LEN + 1)
            continue;

        UUID rid;
        try { rid = parseFromString<UUID>(child.substr(0, UUID_LEN)); }
        catch (...)
        {
            LOG_WARNING(log, "Skipping unexpected Keeper child '{}' under {} (unparsable UUID prefix)",
                child, session.replicasPath());
            continue;
        }

        if (dead_replicas.contains(rid))
            continue;

        if (list_resp.data[i].empty())
        {
            LOG_WARNING(log, "Replica {} has empty `_tail_ptr` — deferring cluster log cleanup to next pass", rid);
            return std::nullopt;
        }
        global_min = std::min(global_min, Tx::deserializeCSN(list_resp.data[i]));
        live_replicas_with_tail_ptr.insert(rid);
    }

    /// Defer if any live replica is missing `_tail_ptr` (init race or operator interference).
    for (const UUID & rid : live_replicas)
    {
        if (!live_replicas_with_tail_ptr.contains(rid))
        {
            LOG_WARNING(log, "Live replica {} has no `_tail_ptr` znode — deferring cluster log cleanup", rid);
            return std::nullopt;
        }
    }

    /// Do NOT advance `global_tail_ptr` here. The watermark moves at the end of the
    /// orchestrator, once we know which CSNs we actually removed.
    /// Advancing up front would stall: any failure below would return early with the
    /// watermark already advanced, and the next pass would short-circuit here.
    if (global_min <= global_tail_ptr.load())
        return std::nullopt;
    return global_min;
}

std::vector<TransactionLog::RemovableEntry>
TransactionLog::collectRemovableEntries(CSN global_min, CSN latest_entry_csn)
{
    /// Walk in CSN-ascending order, stop at the first non-removable entry.
    /// Removing a higher CSN past a lower live one would break the per-table
    /// monotonic invariant on the next restart.
    std::vector<std::pair<TIDHash, CSNEntry>> sorted;
    {
        std::lock_guard lock{mutex};
        sorted.assign(tid_to_csn.begin(), tid_to_csn.end());
    }
    std::sort(sorted.begin(), sorted.end(),
        [](const auto & a, const auto & b) { return a.second.csn < b.second.csn; });

    std::vector<RemovableEntry> removable_list;
    std::string_view stop_reason;
    for (const auto & [hash, entry] : sorted)
    {
        /// Gate removal by commit CSN, not TID `start_csn`: a long-lived transaction
        /// with `start_csn << csn` would otherwise get removed while its commit CSN is
        /// still above some live replica's floor.
        if (global_min <= entry.csn)
        {
            stop_reason = "above_floor";
            break;
        }
        if (entry.csn == latest_entry_csn)
        {
            stop_reason = "is_latest";
            break;
        }

        removable_list.push_back({entry.csn, entry.data.tid, entry.data.replica_id, entry.data.smt, hash});
    }

    LOG_TEST(log, "collectRemovableEntries: global_min={} latest_entry_csn={} tid_to_csn_size={} eligible={} stop_reason={}",
        global_min, latest_entry_csn, sorted.size(), removable_list.size(),
        stop_reason.empty() ? std::string_view{"end_of_list"} : stop_reason);

    return removable_list;
}

TransactionLog::CleanupPlan TransactionLog::computeCleanupPlan(
    const std::vector<RemovableEntry> & removable_list) const
{
    CleanupPlan plan;

    if (removable_list.empty())
        return plan;
    const size_t remove_count = removable_list.size();

    plan.log_removes.reserve(remove_count);
    for (size_t i = 0; i < remove_count; ++i)
        plan.log_removes.push_back(removable_list[i].csn);

    plan.new_watermark = removable_list[remove_count - 1].csn;
    return plan;
}


void TransactionLog::evictInMemoryPrefix(const std::vector<RemovableEntry> & removable_list, size_t log_removed_idx)
{
    std::lock_guard lock{mutex};
    for (size_t i = 0; i < log_removed_idx; ++i)
    {
        auto it = tid_to_csn.find(removable_list[i].hash);
        if (it == tid_to_csn.end())
            continue;
        /// The CSN is now below `global_tail_ptr`. Aged-out TIDs are reported as
        /// removed by `assertTIDIsNotOutdated`.
        tid_to_csn.erase(it);
    }
}

std::optional<CSN> TransactionLog::loadNewEntries(const zkutil::ZooKeeperPtr & zookeeper)
{
    Strings entries_list = zookeeper->getChildren(zookeeper_path_log, nullptr, log_updated_event);
    chassert(!entries_list.empty());
    /// Order and boundary both by CSN, not by name (see `serializeCSN`).
    ::sort(entries_list.begin(), entries_list.end(), [](const String & a, const String & b)
        { return Tx::deserializeCSN(a) < Tx::deserializeCSN(b); });
    auto it = std::upper_bound(entries_list.begin(), entries_list.end(),
        TSA_READ_ONE_THREAD(last_loaded_csn),
        [](CSN csn, const String & name) { return csn < Tx::deserializeCSN(name); });
    return loadEntries(zookeeper, it, entries_list.end());
}

CSN TransactionLog::getLatestSnapshot() const
{
    return latest_snapshot.load();
}

void TransactionLog::updateTableStamp(Int64 cross_replica_id, CSN stamp_csn)
{
    std::lock_guard lock{mutex};
    auto [it, inserted] = table_stamp_csns.emplace(cross_replica_id, stamp_csn);
    /// Monotonic advancement: never move the stamp backwards. A later
    /// discovery may pick up a fresher stamp; an older fetch racing in
    /// must not undo that progress.
    if (!inserted && stamp_csn > it->second)
        it->second = stamp_csn;
}

void TransactionLog::updateTableProcessedCSN(Int64 cross_replica_id, CSN processed_csn)
{
    /// Monotonic update — a racing older fetch must not move the value back.
    std::lock_guard lock{mutex};
    auto [it, inserted] = table_processed_csns.emplace(cross_replica_id, processed_csn);
    if (!inserted && processed_csn > it->second)
        it->second = processed_csn;
}

void TransactionLog::advanceAffectedTablesStamps(const std::vector<MergeTreeTransaction::AffectedSMTTable> & affected_tables, CSN csn)
{
    for (const auto & at : affected_tables)
        updateTableStamp(at.cross_replica_id, csn);
}

bool TransactionLog::waitForCSNLoaded(CSN csn) const
{
    auto current_latest_snapshot = latest_snapshot.load();
    while (current_latest_snapshot < csn && !stop_flag)
    {
        latest_snapshot.wait(current_latest_snapshot);
        current_latest_snapshot = latest_snapshot.load();
    }
    return csn <= current_latest_snapshot;
}

CSN TransactionLog::lookupCSNInMap(const TIDHash & tid_hash) const
{
    chassert(tid_hash);
    chassert(tid_hash != Tx::EmptyTID.getHash());

    std::lock_guard lock{mutex};
    if (auto it = tid_to_csn.find(tid_hash); it != tid_to_csn.end())
        return it->second.csn;
    return Tx::UnknownCSN;
}

CSN TransactionLog::resolveGapCSNFromKeeper(const GetZooKeeper & get_zookeeper, const TIDHash & tid_hash)
{
    auto component_guard = Coordination::setCurrentComponent("TransactionLog::resolveGapCSNFromKeeper");

    CSN result = Tx::UnknownCSN;

    /// One retry loop around all Keeper access (same policy as `sync()`); re-running the cache
    /// update changes nothing, so a retried iteration just re-writes the same TID -> CSN pairs.
    ZooKeeperRetriesControl retries{
        "TransactionLog::resolveGapCSNFromKeeper", log,
        ZooKeeperRetriesInfo{/*max_retries=*/10, /*initial_backoff_ms=*/100, /*max_backoff_ms=*/5000, /*query_status=*/nullptr}};
    retries.retryLoop([&]
    {
        auto zk = get_zookeeper();

        /// `sync` first: a node that trails the leader reports a committed TID as absent, and this
        /// function turns absence into `RolledBackCSN`. Presence needs no fence - the log only grows.
        /// Pipelined with the read, so the fence costs one round trip.
        auto log_sync = zk->asyncSync(zookeeper_path_log);
        auto log_children = zk->asyncGetChildren(zookeeper_path_log);
        [[maybe_unused]] const auto log_synced = log_sync.get();
        Strings entries_list = log_children.get().names;
        chassert(!entries_list.empty());

        /// The gap is everything newer than what `runUpdatingThread` has absorbed. Entries at or
        /// below `latest_snapshot` are already in `tid_to_csn`, which the fast path checked.
        const CSN snapshot = latest_snapshot.load();
        std::vector<std::string> gap_paths;
        std::vector<CSN> gap_csns;
        for (const auto & name : entries_list)
        {
            CSN csn = Tx::deserializeCSN(name);
            if (csn > snapshot)
            {
                gap_paths.emplace_back(fs::path(zookeeper_path_log) / name);
                gap_csns.push_back(csn);
            }
        }

        result = Tx::UnknownCSN;
        if (!gap_paths.empty())
        {
            auto entries = zk->get(gap_paths);
            std::lock_guard lock{gap_csn_cache_mutex};
            /// Drop entries `tid_to_csn` now owns, so the cache stays about the size of the gap.
            std::erase_if(gap_csn_cache, [snapshot](const auto & kv) { return kv.second <= snapshot; });
            for (size_t i = 0; i < gap_paths.size(); ++i)
            {
                TIDHash entry_tid_hash = Tx::CSNEntryData::deserialize(entries[i].data).tid.getHash();
                if (entry_tid_hash == Tx::EmptyTID.getHash())
                    continue;
                gap_csn_cache[entry_tid_hash] = gap_csns[i];
                if (entry_tid_hash == tid_hash)
                    result = gap_csns[i];
            }
        }

        /// Not committed in the `/log` tail. Check the invalidation list in Keeper directly, so a
        /// TID a peer just invalidated reads as rolled back even before our in-memory copy catches up.
        /// Fenced like the tail read above: it is the other half of the same verdict.
        if (result == Tx::UnknownCSN)
        {
            /// Sync the parent, which always exists - the record we are asking about may not.
            auto invalidation_sync = zk->asyncSync(session.invalidTidsPath());
            auto invalidation_exists
                = zk->asyncTryExistsNoThrow(session.invalidTidsPath() + "/" + toString(tid_hash));
            [[maybe_unused]] const auto invalidation_synced = invalidation_sync.get();
            if (invalidation_exists.get().error == Coordination::Error::ZOK)
                result = Tx::RolledBackCSN;
        }
    });

    /// Nudge `runUpdatingThread` to load these entries into `tid_to_csn` and advance the snapshot.
    log_updated_event->set();
    return result;
}

void TransactionLog::sync(const GetZooKeeper & get_zookeeper) const
{
    auto component_guard = Coordination::setCurrentComponent("TransactionLog::sync");

    /// Retry the Keeper `getChildren` on transient hardware errors. The retry policy
    /// mirrors the general-purpose Keeper read defaults used elsewhere in the codebase
    /// (e.g. `StorageKeeperMap`): up to 10 retries with 100ms→5000ms exponential backoff.
    Strings entries_list;
    ZooKeeperRetriesControl retries{
        "TransactionLog::sync",
        log,
        ZooKeeperRetriesInfo{/*max_retries=*/10, /*initial_backoff_ms=*/100, /*max_backoff_ms=*/5000, /*query_status=*/nullptr}};
    retries.retryLoop([&]
    {
        entries_list = get_zookeeper()->getChildren(zookeeper_path_log);
    });

    chassert(!entries_list.empty());
    /// By CSN, not by name (see `serializeCSN`).
    CSN newest_csn = Tx::UnknownCSN;
    for (const auto & name : entries_list)
        newest_csn = std::max(newest_csn, Tx::deserializeCSN(name));
    waitForCSNLoaded(newest_csn);
}


}
