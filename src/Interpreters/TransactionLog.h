#pragma once
#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/MergeTreeTransactionHolder.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ThreadPool.h>
#include <boost/noncopyable.hpp>
#include <mutex>
#include <unordered_map>

namespace DB
{

/// We want to create a TransactionLog object lazily and avoid creation if it's not needed.
/// But we also want to call shutdown() in a specific place to avoid race conditions.
/// We cannot simply use return-static-variable pattern,
/// because a call to shutdown() may construct unnecessary object in this case.
template <typename Derived>
class SingletonHelper : private boost::noncopyable
{
public:
    static Derived & instance()
    {
        Derived * ptr = instance_raw_ptr.load();
        if (likely(ptr))
            return *ptr;

        return createInstanceOrThrow();
    }

    static void shutdownIfAny()
    {
        std::lock_guard lock{instance_mutex};
        if (instance_holder)
            instance_holder->shutdown();
    }

private:
    static Derived & createInstanceOrThrow();

    static inline std::atomic<Derived *> instance_raw_ptr;
    /// It was supposed to be std::optional, but gcc fails to compile it for some reason
    static inline std::shared_ptr<Derived> instance_holder;
    static inline std::mutex instance_mutex;
};

class TransactionsInfoLog;
using TransactionsInfoLogPtr = std::shared_ptr<TransactionsInfoLog>;
using ZooKeeperPtr = std::shared_ptr<zkutil::ZooKeeper>;

/// This class maintains transaction log in ZooKeeper and a list of currently running transactions in memory.
///
/// Each transaction has unique ID (TID, see details below).
/// TransactionID is allocated when transaction begins.
///
/// We use TransactionID to associate changes (created/removed data parts) with transaction that has made/is going to make these changes.
/// To commit a transaction we create sequential node "/path_to_log/log/csn-" in ZK and write TID into this node.
/// Allocated sequential number is a commit timestamp or Commit Sequence Number (CSN). It indicates a (logical) point in time
/// when transaction is committed and all its changes became visible. So we have total order of all changes.
///
/// Also CSNs are used as snapshots: all changes that were made by a transaction that was committed with a CSN less or equal than some_csn
/// are visible in some_csn snapshot.
///
/// TransactionID consists of three parts: (start_csn, local_tid, host_id)
///   - start_csn is the newest CSN that existed when the transaction was started and also it's snapshot that is visible for this transaction
///   - local_tid is local sequential number of the transaction, each server allocates local_tids independently without requests to ZK
///   - host_id is persistent UUID of host that has started the transaction, it's kind of tie-breaker that makes ID unique across all servers
///
/// To check if some transaction is committed or not we fetch "csn-xxxxxx" nodes from ZK and construct TID -> CSN mapping,
/// so for committed transactions we know commit timestamps.
/// However, if we did not find a mapping for some TID, it means one of the following cases:
///    1. Transaction is not committed (yet)
///    2. Transaction is rolled back (quite similar to the first case, but it will never be committed)
///    3. Transactions was committed a long time ago and we removed its entry from the log
/// To distinguish the third case we store a "tail pointer" in "/path_to_log/tail_ptr". It's a CSN such that it's safe to remove from log
/// entries with tid.start_csn < tail_ptr, because CSNs for those TIDs are already written into data parts
/// and we will not do a CSN lookup for those TIDs anymore.
///
/// (however, transactions involving multiple hosts and/or ReplicatedMergeTree tables are currently not supported)
class TransactionLog final : public SingletonHelper<TransactionLog>
{
public:

    TransactionLog();

    ~TransactionLog();

    void shutdown();

    /// Returns the newest snapshot available for reading
    CSN getLatestSnapshot() const;
    /// Returns the oldest snapshot that is visible for some running transaction
    CSN getOldestSnapshot() const;

    /// Allocates TID, returns new transaction object
    MergeTreeTransactionPtr beginTransaction();

    /// Tries to commit transaction. Returns Commit Sequence Number.
    /// Throw if transaction was concurrently killed or if some precommit check failed.
    /// May throw if ZK connection is lost. Transaction status is unknown in this case.
    /// Returns CommittingCSN if throw_on_unknown_status is false and connection was lost.
    CSN commitTransaction(const MergeTreeTransactionPtr & txn, bool throw_on_unknown_status);

    /// Releases locks that that were acquired by transaction, releases snapshot, removes transaction from the list of active transactions.
    /// Normally it should not throw, but if it does for some reason (global memory limit exceeded, disk failure, etc)
    /// then we should terminate server and reinitialize it to avoid corruption of data structures. That's why it's noexcept.
    void rollbackTransaction(const MergeTreeTransactionPtr & txn) noexcept;

    /// Returns CSN if transaction with specified ID was committed and UnknownCSN if it was not.
    /// Returns PrehistoricCSN for PrehistoricTID without creating a TransactionLog instance as a special case.
    static CSN getCSN(const TransactionID & tid);
    static CSN getCSN(const TIDHash & tid);

    /// Ensures that getCSN returned UnknownCSN because transaction is not committed and not because entry was removed from the log.
    static void assertTIDIsNotOutdated(const TransactionID & tid);

    /// Returns a pointer to transaction object if it's running or nullptr.
    MergeTreeTransactionPtr tryGetRunningTransaction(const TIDHash & tid);

    using TransactionsList = std::unordered_map<TIDHash, MergeTreeTransactionPtr>;
    /// Returns copy of list of running transactions.
    TransactionsList getTransactionsList() const;

    /// Waits for provided CSN (and all previous ones) to be loaded from the log.
    /// Returns false if waiting was interrupted (e.g. by shutdown)
    bool waitForCSNLoaded(CSN csn) const;

    bool isShuttingDown() const { return stop_flag.load(); }

    void sync() const;

private:
    void loadLogFromZooKeeper();
    void runUpdatingThread();

    void loadEntries(Strings::const_iterator beg, Strings::const_iterator end);
    void loadNewEntries();
    void removeOldEntries();

    CSN finalizeCommittedTransaction(MergeTreeTransaction * txn, CSN allocated_csn, scope_guard & state_guard) noexcept;

    void tryFinalizeUnknownStateTransactions();

    static UInt64 deserializeCSN(const String & csn_node_name);
    static String serializeCSN(CSN csn);
    static TransactionID deserializeTID(const String & csn_node_content);
    static String serializeTID(const TransactionID & tid);

    ZooKeeperPtr getZooKeeper() const;

    CSN getCSNImpl(const TIDHash & tid_hash) const;

    ContextPtr global_context;
    Poco::Logger * log;

    /// The newest snapshot available for reading
    std::atomic<CSN> latest_snapshot;

    /// Local part of TransactionID number. We reset this counter for each new snapshot.
    std::atomic<LocalTID> local_tid_counter;

    mutable std::mutex mutex;
    /// Mapping from TransactionID to CSN for recently committed transactions.
    /// Allows to check if some transactions is committed.
    struct CSNEntry
    {
        CSN csn;
        TransactionID tid;
    };
    using TIDMap = std::unordered_map<TIDHash, CSNEntry>;
    TIDMap tid_to_csn;

    mutable std::mutex running_list_mutex;
    /// Transactions that are currently processed
    TransactionsList running_list;
    /// If we lost connection on attempt to create csn- node then we don't know transaction's state.
    using UnknownStateList = std::vector<std::pair<MergeTreeTransaction *, scope_guard>>;
    UnknownStateList unknown_state_list;
    UnknownStateList unknown_state_list_loaded;
    /// Ordered list of snapshots that are currently used by some transactions. Needed for background cleanup.
    std::list<CSN> snapshots_in_use;

    ZooKeeperPtr zookeeper;
    String zookeeper_path;

    String zookeeper_path_log;
    /// Name of the newest entry that was loaded from log in ZK
    String last_loaded_entry;
    /// The oldest CSN such that we store in log entries with TransactionIDs containing this CSN.
    std::atomic<CSN> tail_ptr = Tx::UnknownCSN;

    zkutil::EventPtr log_updated_event = std::make_shared<Poco::Event>();

    std::atomic_bool stop_flag = false;
    ThreadFromGlobalPool updating_thread;

    Float64 fault_probability_before_commit = 0;
    Float64 fault_probability_after_commit = 0;
};

template <typename Derived>
Derived & SingletonHelper<Derived>::createInstanceOrThrow()
{
    std::lock_guard lock{instance_mutex};
    if (!instance_holder)
    {
        instance_holder = std::make_shared<Derived>();
        instance_raw_ptr = instance_holder.get();
    }
    return *instance_holder;
}

}
