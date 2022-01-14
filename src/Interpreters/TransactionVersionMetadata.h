#pragma once
#include <Common/TransactionID.h>
#include <Interpreters/StorageID.h>

namespace Poco
{
class Logger;
}

namespace DB
{

struct TransactionInfoContext
{
    StorageID table = StorageID::createEmpty();
    String part_name;
    String covering_part;

    TransactionInfoContext(StorageID id, String part) : table(std::move(id)), part_name(std::move(part)) {}
};

struct VersionMetadata
{
    TransactionID mintid = Tx::EmptyTID;
    TransactionID maxtid = Tx::EmptyTID;

    std::atomic<TIDHash> maxtid_lock = 0;

    std::atomic<CSN> mincsn = Tx::UnknownCSN;
    std::atomic<CSN> maxcsn = Tx::UnknownCSN;

    bool isVisible(const MergeTreeTransaction & txn);
    bool isVisible(Snapshot snapshot_version, TransactionID current_tid = Tx::EmptyTID);

    TransactionID getMinTID() const { return mintid; }
    TransactionID getMaxTID() const;

    bool tryLockMaxTID(const TransactionID & tid, const TransactionInfoContext & context, TIDHash * locked_by_id = nullptr);
    void lockMaxTID(const TransactionID & tid, const TransactionInfoContext & context);
    void unlockMaxTID(const TransactionID & tid, const TransactionInfoContext & context);

    bool isMaxTIDLocked() const;

    /// It can be called only from MergeTreeTransaction or on server startup
    void setMinTID(const TransactionID & tid, const TransactionInfoContext & context);

    bool canBeRemoved(Snapshot oldest_snapshot_version);

    void write(WriteBuffer & buf) const;
    void read(ReadBuffer & buf);

    String toString(bool one_line = true) const;

    Poco::Logger * log;
    VersionMetadata();
};

DataTypePtr getTransactionIDDataType();

}
