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

        std::lock_guard lock{instance_mutex};
        if (!instance_holder.has_value())
        {
            instance_holder.emplace();
            instance_raw_ptr = &instance_holder.value();
        }
        return instance_holder.value();
    }

    static void shutdownIfAny()
    {
        std::lock_guard lock{instance_mutex};
        if (instance_holder.has_value())
            instance_holder->shutdown();
    }

private:
    static inline std::atomic<Derived *> instance_raw_ptr;
    static inline std::optional<Derived> instance_holder;
    static inline std::mutex instance_mutex;
};

class TransactionsInfoLog;
using TransactionsInfoLogPtr = std::shared_ptr<TransactionsInfoLog>;
using ZooKeeperPtr = std::shared_ptr<zkutil::ZooKeeper>;

class TransactionLog final : public SingletonHelper<TransactionLog>
{
public:

    TransactionLog();

    ~TransactionLog();

    void shutdown();

    Snapshot getLatestSnapshot() const;
    Snapshot getOldestSnapshot() const;

    /// Allocated TID, returns transaction object
    MergeTreeTransactionPtr beginTransaction();

    CSN commitTransaction(const MergeTreeTransactionPtr & txn);

    void rollbackTransaction(const MergeTreeTransactionPtr & txn) noexcept;

    static CSN getCSN(const TransactionID & tid);
    static CSN getCSN(const TIDHash & tid);

    MergeTreeTransactionPtr tryGetRunningTransaction(const TIDHash & tid);

    using TransactionsList = std::unordered_map<TIDHash, MergeTreeTransactionPtr>;
    TransactionsList getTransactionsList() const;

private:
    void loadLogFromZooKeeper();
    void runUpdatingThread();

    void loadEntries(Strings::const_iterator beg, Strings::const_iterator end);
    void loadNewEntries();

    static UInt64 parseCSN(const String & csn_node_name);
    static TransactionID parseTID(const String & csn_node_content);
    static String writeTID(const TransactionID & tid);

    ZooKeeperPtr getZooKeeper() const;

    CSN getCSNImpl(const TIDHash & tid) const;

    ContextPtr global_context;
    Poco::Logger * log;

    std::atomic<CSN> latest_snapshot;
    std::atomic<LocalTID> local_tid_counter;

    mutable std::mutex mutex;
    std::unordered_map<TIDHash, CSN> tid_to_csn;

    mutable std::mutex running_list_mutex;
    TransactionsList running_list;
    std::list<Snapshot> snapshots_in_use;

    String zookeeper_path;
    ZooKeeperPtr zookeeper;
    String last_loaded_entry;
    zkutil::EventPtr log_updated_event = std::make_shared<Poco::Event>();

    std::atomic_bool stop_flag = false;
    ThreadFromGlobalPool updating_thread;
};

}
