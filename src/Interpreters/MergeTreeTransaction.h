#pragma once
#include <Common/TransactionMetadata.h>

namespace DB
{

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using DataPartsVector = std::vector<DataPartPtr>;

class MergeTreeTransaction
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
    State getState() const { return state; }

    const TransactionID tid;

    MergeTreeTransaction() = delete;
    MergeTreeTransaction(Snapshot snapshot_, LocalTID local_tid_, UUID host_id);

    void addNewPart(const DataPartPtr & new_part);
    void removeOldPart(const DataPartPtr & part_to_remove);

    static void addNewPart(const DataPartPtr & new_part, MergeTreeTransaction * txn);
    static void removeOldPart(const DataPartPtr & part_to_remove, MergeTreeTransaction * txn);
    static void addNewPartAndRemoveCovered(const DataPartPtr & new_part, const DataPartsVector & covered_parts, MergeTreeTransaction * txn);

    bool isReadOnly() const;

private:
    void beforeCommit();
    void afterCommit();
    void rollback();

    Snapshot snapshot;
    State state;

    DataPartsVector creating_parts;
    DataPartsVector removing_parts;

    CSN csn = Tx::UnknownCSN;
};

using MergeTreeTransactionPtr = std::shared_ptr<MergeTreeTransaction>;

}
