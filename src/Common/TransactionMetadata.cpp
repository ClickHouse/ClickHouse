#include <Common/TransactionMetadata.h>
#include <Common/SipHash.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <Interpreters/TransactionLog.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SERIALIZATION_ERROR;
extern const int LOGICAL_ERROR;
}

DataTypePtr TransactionID::getDataType()
{
    DataTypes types;
    types.push_back(std::make_shared<DataTypeUInt64>());
    types.push_back(std::make_shared<DataTypeUInt64>());
    types.push_back(std::make_shared<DataTypeUUID>());
    return std::make_shared<DataTypeTuple>(std::move(types));
}

TIDHash TransactionID::getHash() const
{
    SipHash hash;
    hash.update(start_csn);
    hash.update(local_tid);
    hash.update(host_id);
    return hash.get64();
}

/// It can be used fro introspection purposes only
TransactionID VersionMetadata::getMaxTID() const
{
    TIDHash max_lock = maxtid_lock.load();
    if (max_lock)
    {
        if (auto txn = TransactionLog::instance().tryGetRunningTransaction(max_lock))
            return txn->tid;
    }

    if (maxcsn.load(std::memory_order_relaxed))
    {
        /// maxtid cannot be changed since we have maxcsn, so it's readonly
        return maxtid;
    }

    return Tx::EmptyTID;
}

void VersionMetadata::lockMaxTID(const TransactionID & tid, const String & error_context)
{
    TIDHash max_lock_value = tid.getHash();
    TIDHash expected_max_lock_value = 0;
    bool locked = maxtid_lock.compare_exchange_strong(expected_max_lock_value, max_lock_value);
    if (!locked)
    {
        throw Exception(ErrorCodes::SERIALIZATION_ERROR, "Serialization error: "
                        "Transaction {} tried to remove data part, "
                        "but it's locked ({}) by another transaction {} which is currently removing this part. {}",
                        tid, expected_max_lock_value, getMaxTID(), error_context);
    }

    maxtid = tid;
}

void VersionMetadata::unlockMaxTID(const TransactionID & tid)
{
    TIDHash max_lock_value = tid.getHash();
    TIDHash locked_by = maxtid_lock.load();

    auto throw_cannot_unlock = [&]()
    {
        auto locked_by_txn = TransactionLog::instance().tryGetRunningTransaction(locked_by);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot unlock maxtid, it's a bug. Current: {} {}, actual: {} {}",
                        max_lock_value, tid, locked_by, locked_by_txn ? locked_by_txn->tid : Tx::EmptyTID);
    };

    if (locked_by != max_lock_value)
        throw_cannot_unlock();

    maxtid = Tx::EmptyTID;
    bool unlocked = maxtid_lock.compare_exchange_strong(locked_by, 0);
    if (!unlocked)
        throw_cannot_unlock();
}

void VersionMetadata::setMinTID(const TransactionID & tid)
{
    /// TODO Transactions: initialize it in constructor on part creation and remove this method
    /// FIXME ReplicatedMergeTreeBlockOutputStream may add one part multiple times
    assert(!mintid || mintid == tid);
    const_cast<TransactionID &>(mintid) = tid;
}

bool VersionMetadata::isVisible(const MergeTreeTransaction & txn)
{
    Snapshot snapshot_version = txn.getSnapshot();
    assert(mintid);
    CSN min = mincsn.load(std::memory_order_relaxed);
    TIDHash max_lock = maxtid_lock.load(std::memory_order_relaxed);
    CSN max = maxcsn.load(std::memory_order_relaxed);

    [[maybe_unused]] bool had_mincsn = min;
    [[maybe_unused]] bool had_maxtid = max_lock;
    [[maybe_unused]] bool had_maxcsn = max;
    assert(!had_maxcsn || had_maxtid);
    assert(!had_maxcsn || had_mincsn);

    /// Fast path:

    /// Part is definitely not visible if:
    /// - creation was committed after we took the snapshot
    /// - removal was committed before we took the snapshot
    /// - current transaction is removing it
    if (min && snapshot_version < min)
        return false;
    if (max && max <= snapshot_version)
        return false;
    if (max_lock && max_lock == txn.tid.getHash())
        return false;

    /// Otherwise, part is definitely visible if:
    /// - creation was committed before we took the snapshot and nobody tried to remove the part
    /// - creation was committed before and removal was committed after
    /// - current transaction is creating it
    if (min && min <= snapshot_version && !max_lock)
        return true;
    if (min && min <= snapshot_version && max && snapshot_version < max)
        return true;
    if (mintid == txn.tid)
        return true;

    /// End of fast path.

    /// Data part has mintid/maxtid, but does not have mincsn/maxcsn.
    /// It means that some transaction is creating/removing the part right now or has done it recently
    /// and we don't know if it was already committed ot not.
    assert(!had_mincsn || (had_maxtid && !had_maxcsn));
    assert(mintid != txn.tid && max_lock != txn.tid.getHash());

    /// Before doing CSN lookup, let's check some extra conditions.
    /// If snapshot_version <= some_tid.start_csn, then changes of transaction with some_tid
    /// are definitely not visible for us, so we don't need to check if it was committed.
    if (snapshot_version <= mintid.start_csn)
        return false;

    /// Check if mintid/maxtid transactions are committed and write CSNs
    /// TODO Transactions: we probably need some optimizations here
    /// to avoid some CSN lookups or make the lookups cheaper.
    /// NOTE: Old enough committed parts always have written CSNs,
    /// so we can determine their visibility through fast path.
    /// But for long-running writing transactions we will always do
    /// CNS lookup and get 0 (UnknownCSN) until the transaction is committer/rolled back.
    min = TransactionLog::instance().getCSN(mintid);
    if (!min)
        return false;   /// Part creation is not committed yet

    /// We don't need to check if CSNs are already writen or not,
    /// because once writen CSN cannot be changed, so it's safe to overwrite it (with tha same value).
    mincsn.store(min, std::memory_order_relaxed);

    if (max_lock)
    {
        max = TransactionLog::instance().getCSN(max_lock);
        if (max)
            maxcsn.store(max, std::memory_order_relaxed);
    }

    return min <= snapshot_version && (!max || snapshot_version < max);
}

}
