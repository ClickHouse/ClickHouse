#pragma once
#include <Common/TransactionID.h>
#include <Interpreters/StorageID.h>

namespace Poco
{
class Logger;
}

namespace DB
{

/// This structure allows to pass more information about a part that transaction is trying to create/remove.
/// It's useful for logging and for exception messages.
struct TransactionInfoContext
{
    /// To which table a part belongs
    StorageID table = StorageID::createEmpty();
    /// Name of a part that transaction is trying to create/remove
    String part_name;
    /// Optional: name of part that covers `part_name` if transaction is trying to remove `part_name`
    String covering_part;

    TransactionInfoContext(StorageID id, String part) : table(std::move(id)), part_name(std::move(part)) {}
};

/// This structure contains metadata of an object (currently it's used for data parts in MergeTree only)
/// that allows to determine when and by which transaction it has been created/removed
struct VersionMetadata
{
    /// ID of transaction that has created/is trying to create this object
    TransactionID creation_tid = Tx::EmptyTID;
    /// ID of transaction that has removed/is trying to remove this object
    TransactionID removal_tid = Tx::EmptyTID;

    /// Hash of removal_tid, used to lock an object for removal
    std::atomic<TIDHash> removal_tid_lock = 0;

    /// CSN of transaction that has created this object
    std::atomic<CSN> creation_csn = Tx::UnknownCSN;
    /// CSN of transaction that has removed this object
    std::atomic<CSN> removal_csn = Tx::UnknownCSN;

    /// Checks if an object is visible for transaction or not.
    bool isVisible(const MergeTreeTransaction & txn);
    bool isVisible(CSN snapshot_version, TransactionID current_tid = Tx::EmptyTID);

    TransactionID getCreationTID() const { return creation_tid; }
    TransactionID getRemovalTID() const;

    /// Looks an object for removal, throws if it's already locked by concurrent transaction
    bool tryLockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context, TIDHash * locked_by_id = nullptr);
    void lockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context);
    /// Unlocks an object for removal (when transaction is rolling back)
    void unlockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context);

    bool isRemovalTIDLocked() const;

    /// It can be called only from MergeTreeTransaction or on server startup
    void setCreationTID(const TransactionID & tid, TransactionInfoContext * context);

    /// Checks if it's safe to remove outdated version of an object
    bool canBeRemoved();
    bool canBeRemovedImpl(CSN oldest_snapshot_version);

    void write(WriteBuffer & buf) const;
    void read(ReadBuffer & buf);

    enum WhichCSN { CREATION, REMOVAL };
    void writeCSN(WriteBuffer & buf, WhichCSN which_csn, bool internal = false) const;
    void writeRemovalTID(WriteBuffer & buf, bool clear = false) const;

    String toString(bool one_line = true) const;

    LoggerPtr log;
    VersionMetadata();
};

DataTypePtr getTransactionIDDataType();

}
