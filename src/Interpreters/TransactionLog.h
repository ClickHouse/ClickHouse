#pragma once
#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/MergeTreeTransactionHolder.h>
#include <boost/noncopyable.hpp>
#include <mutex>
#include <unordered_map>

namespace DB
{

class TransactionLog final : private boost::noncopyable
{
public:
    static TransactionLog & instance();

    TransactionLog();

    Snapshot getLatestSnapshot() const;

    /// Allocated TID, returns transaction object
    MergeTreeTransactionPtr beginTransaction();

    CSN commitTransaction(const MergeTreeTransactionPtr & txn);

    void rollbackTransaction(const MergeTreeTransactionPtr & txn) noexcept;

    CSN getCSN(const TransactionID & tid) const;
    CSN getCSN(const TIDHash & tid) const;

    MergeTreeTransactionPtr tryGetRunningTransaction(const TIDHash & tid);

private:
    Poco::Logger * log;

    std::atomic<CSN> latest_snapshot;
    std::atomic<CSN> csn_counter;
    std::atomic<LocalTID> local_tid_counter;

    /// FIXME Transactions: it's probably a bad idea to use global mutex here
    mutable std::mutex commit_mutex;
    std::unordered_map<TIDHash, CSN> tid_to_csn;

    mutable std::mutex running_list_mutex;
    std::unordered_map<TIDHash, MergeTreeTransactionPtr> running_list;
};

}
