#pragma once
#include <Common/TransactionID.h>

namespace DB
{

struct VersionMetadata
{
    const TransactionID mintid = Tx::EmptyTID;
    TransactionID maxtid = Tx::EmptyTID;

    std::atomic<TIDHash> maxtid_lock = 0;

    std::atomic<CSN> mincsn = Tx::UnknownCSN;
    std::atomic<CSN> maxcsn = Tx::UnknownCSN;

    bool isVisible(const MergeTreeTransaction & txn);
    bool isVisible(Snapshot snapshot_version, TransactionID current_tid = Tx::EmptyTID);

    TransactionID getMinTID() const { return mintid; }
    TransactionID getMaxTID() const;

    void lockMaxTID(const TransactionID & tid, const String & error_context = {});
    void unlockMaxTID(const TransactionID & tid);

    bool isMaxTIDLocked() const;

    /// It can be called only from MergeTreeTransaction or on server startup
    void setMinTID(const TransactionID & tid);

    bool canBeRemoved(Snapshot oldest_snapshot_version);
};

DataTypePtr getTransactionIDDataType();

}
