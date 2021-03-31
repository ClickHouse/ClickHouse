#pragma once
#include <Interpreters/MergeTreeTransaction.h>
#include <boost/noncopyable.hpp>
#include <mutex>

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

    void rollbackTransaction(const MergeTreeTransactionPtr & txn);

private:
    std::atomic<CSN> csn_counter;
    std::atomic<LocalTID> local_tid_counter;
};

}
