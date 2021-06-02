#pragma once
#include <Common/TransactionMetadata.h>
#include <boost/noncopyable.hpp>

namespace DB
{

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using DataPartsVector = std::vector<DataPartPtr>;

class MergeTreeTransaction : public std::enable_shared_from_this<MergeTreeTransaction>, private boost::noncopyable
{
    friend class TransactionLog;
public:
    enum State
    {
        RUNNING,
        COMMITTED,
        ROLLED_BACK,
    };

    Snapshot getSnapshot() const { return snapshot; }
    State getState() const;

    const TransactionID tid;

    MergeTreeTransaction() = delete;
    MergeTreeTransaction(Snapshot snapshot_, LocalTID local_tid_, UUID host_id);

    void addNewPart(const DataPartPtr & new_part);
    void removeOldPart(const DataPartPtr & part_to_remove);

    static void addNewPart(const DataPartPtr & new_part, MergeTreeTransaction * txn);
    static void removeOldPart(const DataPartPtr & part_to_remove, MergeTreeTransaction * txn);
    static void addNewPartAndRemoveCovered(const DataPartPtr & new_part, const DataPartsVector & covered_parts, MergeTreeTransaction * txn);

    bool isReadOnly() const;

    void onException();

    String dumpDescription() const;

private:
    void beforeCommit() const;
    void afterCommit(CSN assigned_csn) noexcept;
    void rollback() noexcept;

    Snapshot snapshot;

    DataPartsVector creating_parts;
    DataPartsVector removing_parts;

    CSN csn;
};

using MergeTreeTransactionPtr = std::shared_ptr<MergeTreeTransaction>;

}
