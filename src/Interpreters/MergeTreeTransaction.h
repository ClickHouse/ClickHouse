#pragma once
#include <Common/TransactionMetadata.h>

namespace DB
{

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

private:
    Snapshot snapshot;
    State state;

    CSN csn = Tx::UnknownCSN;
};

using MergeTreeTransactionPtr = std::shared_ptr<MergeTreeTransaction>;

}
