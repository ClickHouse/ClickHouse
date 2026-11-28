#pragma once

#include <atomic>
#include <functional>
#include <mutex>
#include <optional>
#include <set>
#include <unordered_map>
#include <vector>

#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/TransactionPayloads.h>
#include <Interpreters/TransactionSession.h>
#include <base/types.h>
#include <Common/TransactionID.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>

namespace DB
{

/// The CSN log at `/clickhouse/txn/log/csn-<N>` and this replica's in-memory view of it. A commit
/// appends one sequential znode and its number is the CSN; every replica mirrors the log into
/// `tid_to_csn` so `getCSN` answers locally, and the cleanup-lease holder trims the front once
/// every replica's `_tail_ptr` has passed it.
///
/// Owned by `TransactionManager`. Keeper-facing methods take the handle as an argument, because
/// the Manager owns it and its reconnection; the two that retry across a reconnection take a
/// getter instead, so each attempt re-reads the current handle.
class TransactionLog
{
public:
    using GetZooKeeper = std::function<zkutil::ZooKeeperPtr()>;

    TransactionLog(
        const String & zookeeper_path_,
        const TransactionSession & session_,
        const std::atomic_bool & stop_flag_,
        LoggerPtr log_);

    const String & logPath() const { return zookeeper_path_log; }
    String tableLastCommittedTidPath(Int64 cross_replica_id) const;
    String tableProcessedCSNPath(Int64 cross_replica_id) const;

    CSN getLatestSnapshot() const;
    CSN getOwnTailPtr() const { return tail_ptr.load(); }
    CSN getGlobalTailPtr() const { return global_tail_ptr.load(); }
    void notifyUpdated();

    void initLogRoot(const zkutil::ZooKeeperPtr & zookeeper);
    /// Parents for the per-table stamp and processed-CSN znodes.
    void initTableNodes(const zkutil::ZooKeeperPtr & zookeeper);
    void restoreOwnTailPtr(const zkutil::ZooKeeperPtr & zookeeper);
    /// Block until a commit or the timeout, so the periodic tasks still run on an idle cluster.
    void waitForUpdate(size_t milliseconds);
    /// Return the newest CSN loaded, or nullopt if nothing was new.
    std::optional<CSN> reloadCSNLogs(const zkutil::ZooKeeperPtr & zookeeper);
    std::optional<CSN> loadNewEntries(const zkutil::ZooKeeperPtr & zookeeper);
    /// Call under the lock `beginTransaction` uses: it pairs this with the local-TID counter, and
    /// an old `start_csn` beside a rewound `local_tid` mints a TID that already exists.
    void publishSnapshot(CSN csn);
    void assertLoaded() const;

    /// Waits for `csn` and everything before it. False if interrupted by shutdown.
    bool waitForCSNLoaded(CSN csn) const;
    void sync(const GetZooKeeper & get_zookeeper) const;

    CSN lookupCSNInMap(const TIDHash & tid_hash) const;
    /// A CSN an earlier gap read already resolved. `UnknownCSN` on miss.
    CSN lookupGapCSN(const TIDHash & tid_hash) const;
    /// For a TID newer than `latest_snapshot`: read it from Keeper rather than waiting for the
    /// updating thread, which may be blocked on a lock the caller holds.
    CSN resolveGapCSNFromKeeper(const GetZooKeeper & get_zookeeper, const TIDHash & tid_hash);

    void updateTableStamp(Int64 cross_replica_id, CSN stamp_csn);
    void updateTableProcessedCSN(Int64 cross_replica_id, CSN processed_csn);
    void advanceAffectedTablesStamps(const std::vector<MergeTreeTransaction::AffectedSMTTable> & affected_tables, CSN csn);
    void forgetDroppedTable(const zkutil::ZooKeeperPtr & zookeeper, Int64 cross_replica_id);

    /// The caller supplies the clamps only it knows.
    void advanceOwnTailPtr(const zkutil::ZooKeeperPtr & zookeeper, CSN oldest_snapshot, CSN oldest_unfinalized_start_csn);

    /// Cleanup-lease holder only. `lease_czxid` guards every write, so a lost lease cannot trim.
    void removeOldEntries(const zkutil::ZooKeeperPtr & zookeeper, const String & lease_path, int64_t lease_czxid);

    /// Drop `tid_to_csn` entries whose CSN is already gone from `/log`. Runs on every replica, or
    /// a peer's map grows forever. Prunes by what Keeper no longer holds, never by a floor of our
    /// own: the map has to stay a superset of `/log`.
    void pruneInMemoryEntriesRemovedFromLog(const zkutil::ZooKeeperPtr & zookeeper);

private:
    std::optional<CSN> loadEntries(const zkutil::ZooKeeperPtr & zookeeper, Strings::const_iterator beg, Strings::const_iterator end);
    std::optional<CSN> processCSNLogs(const Strings & names, const Strings & data);
    std::optional<CSN> computeGlobalMinTailPtr(const zkutil::ZooKeeperPtr & zookeeper) const;

    const String zookeeper_path_log;
    const String zookeeper_path_tables;
    const String zookeeper_path_tables_stamp;
    const String zookeeper_path_tables_processed;
    const TransactionSession & session;
    const std::atomic_bool & stop_flag;
    LoggerPtr log;

    std::atomic<CSN> latest_snapshot;
    Coordination::EventPtr log_updated_event = std::make_shared<Poco::Event>();

    mutable std::mutex mutex;

    /// `csn` is separate from the payload because the log entry carries it in the znode name.
    struct CSNEntry
    {
        CSN csn{};
        Tx::CSNEntryData data;
    };
    using TIDMap = std::unordered_map<TIDHash, CSNEntry>;
    TIDMap tid_to_csn TSA_GUARDED_BY(mutex);
    /// Highest CSN absorbed into `tid_to_csn`. A number, not a znode name (see `serializeCSN`).
    CSN last_loaded_csn TSA_GUARDED_BY(mutex) = Tx::UnknownCSN;

    /// Kept out of `tid_to_csn` on purpose: feeding it there would trip the dedup guard in
    /// `loadEntries` and starve `table_affected_csns`. A CSN never changes, so a hit is correct.
    mutable std::mutex gap_csn_cache_mutex;
    std::unordered_map<TIDHash, CSN> gap_csn_cache TSA_GUARDED_BY(gap_csn_cache_mutex);

    /// `cross_replica_id -> CSNs that touched the table`. `std::set` gives `upper_bound` in
    /// O(log N) for the watermark walk. Pruned in lockstep with `tid_to_csn`.
    using TableAffectedCSNs = std::unordered_map<Int64, std::set<CSN>>;
    TableAffectedCSNs table_affected_csns TSA_GUARDED_BY(mutex);
    /// What this replica's local view has absorbed. Missing entry treated as 0.
    using TableStampCSNs = std::unordered_map<Int64, CSN>;
    TableStampCSNs table_stamp_csns TSA_GUARDED_BY(mutex);
    /// What `processPartsUpdate` has reconciled. Holds the cleanup floor below anything unapplied.
    using TableProcessedCSNs = std::unordered_map<Int64, CSN>;
    TableProcessedCSNs table_processed_csns TSA_GUARDED_BY(mutex);

    /// One entry eligible for removal from `/log`: below `global_min`, and all its SMT mutations
    /// CSN-stamped.
    struct RemovableEntry
    {
        CSN csn = Tx::UnknownCSN;
        TransactionID tid;
        UUID replica_id;
        std::vector<MergeTreeTransaction::AffectedSMTTable> smt;
        TIDHash hash = 0;
    };

    /// One cleanup pass's Keeper writes, batched into a single Multi and built offline from a
    /// read-only snapshot.
    struct CleanupPlan
    {
        std::vector<CSN> log_removes;
        CSN new_watermark = Tx::UnknownCSN;
    };

    /// Walks `tid_to_csn` ascending by CSN, stopping at the first ineligible entry. Ascending
    /// order matters: the Multi removes a `/log/csn-N` prefix and a retry replays that prefix.
    std::vector<RemovableEntry> collectRemovableEntries(CSN global_min, CSN latest_entry_csn);
    CleanupPlan computeCleanupPlan(const std::vector<RemovableEntry> & removable_list) const;
    /// Drop the in-memory entries matching the prefix the Multi just removed, and no more, so the
    /// mirror stays consistent with Keeper.
    void evictInMemoryPrefix(const std::vector<RemovableEntry> & removable_list, size_t log_removed_idx);

    std::atomic<CSN> tail_ptr = Tx::UnknownCSN;
    /// min(`tail_ptr`) over live replicas — what cleanup may trim below.
    std::atomic<CSN> global_tail_ptr = Tx::UnknownCSN;
    CSN last_pruned_below = Tx::UnknownCSN;
};

}
