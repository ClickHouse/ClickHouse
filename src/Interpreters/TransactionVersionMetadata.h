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
    TransactionID creation_tid = Tx::EmptyTID;
    TransactionID removal_tid = Tx::EmptyTID;

    std::atomic<TIDHash> removal_tid_lock = 0;

    std::atomic<CSN> creation_csn = Tx::UnknownCSN;
    std::atomic<CSN> removal_csn = Tx::UnknownCSN;

    /// Checks if an object is visible for transaction or not.
    bool isVisible(const MergeTreeTransaction & txn);
    bool isVisible(Snapshot snapshot_version, TransactionID current_tid = Tx::EmptyTID);

    TransactionID getCreationTID() const { return creation_tid; }
    TransactionID getRemovalTID() const;

    bool tryLockMaxTID(const TransactionID & tid, const TransactionInfoContext & context, TIDHash * locked_by_id = nullptr);
    void lockMaxTID(const TransactionID & tid, const TransactionInfoContext & context);
    void unlockMaxTID(const TransactionID & tid, const TransactionInfoContext & context);

    bool isRemovalTIDLocked() const;

    /// It can be called only from MergeTreeTransaction or on server startup
    void setCreationTID(const TransactionID & tid, TransactionInfoContext * context);

    /// Checks if it's safe to remove outdated version of an object
    bool canBeRemoved();
    bool canBeRemovedImpl(Snapshot oldest_snapshot_version);

    void write(WriteBuffer & buf) const;
    void read(ReadBuffer & buf);

    enum WhichCSN { CREATION, REMOVAL };
    void writeCSN(WriteBuffer & buf, WhichCSN which_csn, bool internal = false) const;

    String toString(bool one_line = true) const;

    Poco::Logger * log;
    VersionMetadata();
};

DataTypePtr getTransactionIDDataType();

}
