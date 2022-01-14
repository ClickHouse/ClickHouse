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

class TransactionsInfoLog;
using TransactionsInfoLogPtr = std::shared_ptr<TransactionsInfoLog>;
using ZooKeeperPtr = std::shared_ptr<zkutil::ZooKeeper>;

class TransactionLog final : private boost::noncopyable
{
public:
    static TransactionLog & instance();

    TransactionLog();

    ~TransactionLog();

    Snapshot getLatestSnapshot() const;
    Snapshot getOldestSnapshot() const;

    /// Allocated TID, returns transaction object
    MergeTreeTransactionPtr beginTransaction();

    CSN commitTransaction(const MergeTreeTransactionPtr & txn);

    void rollbackTransaction(const MergeTreeTransactionPtr & txn) noexcept;

    CSN getCSN(const TransactionID & tid) const;
    CSN getCSN(const TIDHash & tid) const;

    MergeTreeTransactionPtr tryGetRunningTransaction(const TIDHash & tid);


private:
    void loadLogFromZooKeeper();
    void runUpdatingThread();

    void loadEntries(Strings::const_iterator beg, Strings::const_iterator end);
    void loadNewEntries();

    static UInt64 parseCSN(const String & csn_node_name);
    static TransactionID parseTID(const String & csn_node_content);
    static String writeTID(const TransactionID & tid);

    ContextPtr global_context;
    Poco::Logger * log;

    std::atomic<CSN> latest_snapshot;
    std::atomic<LocalTID> local_tid_counter;

    /// FIXME Transactions: it's probably a bad idea to use global mutex here
    mutable std::mutex commit_mutex;
    std::unordered_map<TIDHash, CSN> tid_to_csn;

    mutable std::mutex running_list_mutex;
    std::unordered_map<TIDHash, MergeTreeTransactionPtr> running_list;
    std::list<Snapshot> snapshots_in_use;

    String zookeeper_path;
    ZooKeeperPtr zookeeper;
    String last_loaded_entry;
    zkutil::EventPtr log_updated_event = std::make_shared<Poco::Event>();

    std::atomic_bool stop_flag = false;
    ThreadFromGlobalPool updating_thread;

};

}
