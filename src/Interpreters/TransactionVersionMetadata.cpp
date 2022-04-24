#include <Interpreters/TransactionVersionMetadata.h>
#include <Interpreters/TransactionsInfoLog.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <Interpreters/TransactionLog.h>
#include <Interpreters/Context.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <base/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SERIALIZATION_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_PARSE_TEXT;
}

inline static CSN getCSNAndAssert(TIDHash tid_hash, std::atomic<CSN> & csn, const TransactionID * tid = nullptr)
{
    CSN maybe_csn = TransactionLog::getCSN(tid_hash);
    if (maybe_csn)
        return maybe_csn;

    /// Either transaction is not committed (yet) or it was committed and then the CSN entry was cleaned up from the log.
    /// We should load CSN again to distinguish the second case.
    /// If entry was cleaned up, then CSN is already stored in VersionMetadata and we will get it.
    /// And for the first case we will get UnknownCSN again.
    maybe_csn = csn.load();
    if (maybe_csn)
        return maybe_csn;

    if (tid)
        TransactionLog::assertTIDIsNotOutdated(*tid);

    return Tx::UnknownCSN;
}

VersionMetadata::VersionMetadata()
{
    /// It would be better to make it static, but static loggers do not work for some reason (initialization order?)
    log = &Poco::Logger::get("VersionMetadata");
}

/// It can be used for introspection purposes only
TransactionID VersionMetadata::getRemovalTID() const
{
    TIDHash removal_lock = removal_tid_lock.load();
    if (removal_lock)
    {
        if (removal_lock == Tx::PrehistoricTID.getHash())
            return Tx::PrehistoricTID;
        if (auto txn = TransactionLog::instance().tryGetRunningTransaction(removal_lock))
            return txn->tid;
    }

    if (removal_csn.load(std::memory_order_relaxed))
    {
        /// removal_tid cannot be changed since we have removal_csn, so it's readonly
        return removal_tid;
    }

    return Tx::EmptyTID;
}

void VersionMetadata::lockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context)
{
    LOG_TEST(log, "Trying to lock removal_tid by {}, table: {}, part: {}", tid, context.table.getNameForLogs(), context.part_name);
    TIDHash locked_by = 0;
    if (tryLockRemovalTID(tid, context, &locked_by))
        return;

    String part_desc;
    if (context.covering_part.empty())
        part_desc = context.part_name;
    else
        part_desc = fmt::format("{} (covered by {})", context.part_name, context.covering_part);
    throw Exception(ErrorCodes::SERIALIZATION_ERROR,
                    "Serialization error: "
                    "Transaction {} tried to remove data part {} from {}, "
                    "but it's locked by another transaction (TID: {}, TIDH: {}) which is currently removing this part.",
                    tid, part_desc, context.table.getNameForLogs(), getRemovalTID(), locked_by);
}

bool VersionMetadata::tryLockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context, TIDHash * locked_by_id)
{
    assert(!tid.isEmpty());
    assert(!creation_tid.isEmpty());
    TIDHash removal_lock_value = tid.getHash();
    TIDHash expected_removal_lock_value = 0;
    bool locked = removal_tid_lock.compare_exchange_strong(expected_removal_lock_value, removal_lock_value);
    if (!locked)
    {
        if (tid == Tx::PrehistoricTID && expected_removal_lock_value == Tx::PrehistoricTID.getHash())
        {
            /// Don't need to lock part for queries without transaction
            LOG_TEST(log, "Assuming removal_tid is locked by {}, table: {}, part: {}", tid, context.table.getNameForLogs(), context.part_name);
            return true;
        }

        if (locked_by_id)
            *locked_by_id = expected_removal_lock_value;
        return false;
    }

    removal_tid = tid;
    tryWriteEventToSystemLog(log, TransactionsInfoLogElement::LOCK_PART, tid, context);
    return true;
}

void VersionMetadata::unlockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context)
{
    LOG_TEST(log, "Unlocking removal_tid by {}, table: {}, part: {}", tid, context.table.getNameForLogs(), context.part_name);
    assert(!tid.isEmpty());
    TIDHash removal_lock_value = tid.getHash();
    TIDHash locked_by = removal_tid_lock.load();

    auto throw_cannot_unlock = [&]()
    {
        auto locked_by_txn = TransactionLog::instance().tryGetRunningTransaction(locked_by);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot unlock removal_tid, it's a bug. Current: {} {}, actual: {} {}",
                        removal_lock_value, tid, locked_by, locked_by_txn ? locked_by_txn->tid : Tx::EmptyTID);
    };

    if (locked_by != removal_lock_value)
        throw_cannot_unlock();

    removal_tid = Tx::EmptyTID;
    bool unlocked = removal_tid_lock.compare_exchange_strong(locked_by, 0);
    if (!unlocked)
        throw_cannot_unlock();

    tryWriteEventToSystemLog(log, TransactionsInfoLogElement::UNLOCK_PART, tid, context);
}

bool VersionMetadata::isRemovalTIDLocked() const
{
    return removal_tid_lock.load() != 0;
}

void VersionMetadata::setCreationTID(const TransactionID & tid, TransactionInfoContext * context)
{
    /// NOTE ReplicatedMergeTreeBlockOutputStream may add one part multiple times
    assert(creation_tid.isEmpty() || creation_tid == tid);
    creation_tid = tid;
    if (context)
        tryWriteEventToSystemLog(log, TransactionsInfoLogElement::ADD_PART, tid, *context);
}

bool VersionMetadata::isVisible(const MergeTreeTransaction & txn)
{
    return isVisible(txn.getSnapshot(), txn.tid);
}

bool VersionMetadata::isVisible(CSN snapshot_version, TransactionID current_tid)
{
    assert(!creation_tid.isEmpty());
    CSN creation = creation_csn.load(std::memory_order_relaxed);
    TIDHash removal_lock = removal_tid_lock.load(std::memory_order_relaxed);
    CSN removal = removal_csn.load(std::memory_order_relaxed);

    [[maybe_unused]] bool had_creation_csn = creation;
    [[maybe_unused]] bool had_removal_tid = removal_lock;
    [[maybe_unused]] bool had_removal_csn = removal;
    assert(!had_removal_csn || had_removal_tid);
    assert(!had_removal_csn || had_creation_csn);
    assert(creation == Tx::UnknownCSN || creation == Tx::PrehistoricCSN || Tx::MaxReservedCSN < creation);
    assert(removal == Tx::UnknownCSN || removal == Tx::PrehistoricCSN || Tx::MaxReservedCSN < removal);

    /// Special snapshot for introspection purposes
    if (unlikely(snapshot_version == Tx::EverythingVisibleCSN))
        return true;

    /// Fast path:

    /// Part is definitely not visible if:
    /// - creation was committed after we took the snapshot
    /// - removal was committed before we took the snapshot
    /// - current transaction is removing it
    if (creation && snapshot_version < creation)
        return false;
    if (removal && removal <= snapshot_version)
        return false;
    if (!current_tid.isEmpty() && removal_lock && removal_lock == current_tid.getHash())
        return false;

    /// Otherwise, part is definitely visible if:
    /// - creation was committed before we took the snapshot and nobody tried to remove the part
    /// - creation was committed before and removal was committed after
    /// - current transaction is creating it
    if (creation && creation <= snapshot_version && !removal_lock)
        return true;
    if (creation && creation <= snapshot_version && removal && snapshot_version < removal)
        return true;
    if (!current_tid.isEmpty() && creation_tid == current_tid)
        return true;

    /// End of fast path.

    /// Data part has creation_tid/removal_tid, but does not have creation_csn/removal_csn.
    /// It means that some transaction is creating/removing the part right now or has done it recently
    /// and we don't know if it was already committed or not.
    assert(!had_creation_csn || (had_removal_tid && !had_removal_csn));
    assert(current_tid.isEmpty() || (creation_tid != current_tid && removal_lock != current_tid.getHash()));

    /// Before doing CSN lookup, let's check some extra conditions.
    /// If snapshot_version <= some_tid.start_csn, then changes of the transaction with some_tid
    /// are definitely not visible for us (because the transaction can be committed with greater CSN only),
    /// so we don't need to check if it was committed.
    if (snapshot_version <= creation_tid.start_csn)
        return false;

    /// Check if creation_tid/removal_tid transactions are committed and write CSNs
    /// TODO Transactions: we probably need more optimizations here
    /// to avoid some CSN lookups or make the lookups cheaper.
    /// NOTE: Old enough committed parts always have written CSNs,
    /// so we can determine their visibility through fast path.
    /// But for long-running writing transactions we will always do
    /// CNS lookup and get 0 (UnknownCSN) until the transaction is committed/rolled back.
    creation = getCSNAndAssert(creation_tid.getHash(), creation_csn, &creation_tid);
    if (!creation)
    {
        return false;   /// Part creation is not committed yet
    }

    /// We don't need to check if CSNs are already written or not,
    /// because once written CSN cannot be changed, so it's safe to overwrite it (with the same value).
    creation_csn.store(creation, std::memory_order_relaxed);

    if (removal_lock)
    {
        removal = getCSNAndAssert(removal_lock, removal_csn);
        if (removal)
            removal_csn.store(removal, std::memory_order_relaxed);
    }

    return creation <= snapshot_version && (!removal || snapshot_version < removal);
}

bool VersionMetadata::canBeRemoved()
{
    if (creation_tid == Tx::PrehistoricTID)
    {
        /// Avoid access to Transaction log if transactions are not involved

        TIDHash removal_lock = removal_tid_lock.load(std::memory_order_relaxed);
        if (!removal_lock)
            return false;

        if (removal_lock == Tx::PrehistoricTID.getHash())
            return true;
    }

    return canBeRemovedImpl(TransactionLog::instance().getOldestSnapshot());
}

bool VersionMetadata::canBeRemovedImpl(CSN oldest_snapshot_version)
{
    CSN creation = creation_csn.load(std::memory_order_relaxed);
    /// We can safely remove part if its creation was rolled back
    if (creation == Tx::RolledBackCSN)
        return true;

    if (!creation)
    {
        /// Cannot remove part if its creation not committed yet
        creation = getCSNAndAssert(creation_tid.getHash(), creation_csn, &creation_tid);
        if (creation)
            creation_csn.store(creation, std::memory_order_relaxed);
        else
            return false;
    }

    /// Part is probably visible for some transactions (part is too new or the oldest snapshot is too old)
    if (oldest_snapshot_version < creation)
        return false;

    TIDHash removal_lock = removal_tid_lock.load(std::memory_order_relaxed);
    /// Part is active
    if (!removal_lock)
        return false;

    CSN removal = removal_csn.load(std::memory_order_relaxed);
    if (!removal)
    {
        /// Part removal is not committed yet
        removal = getCSNAndAssert(removal_lock, removal_csn);
        if (removal)
            removal_csn.store(removal, std::memory_order_relaxed);
        else
            return false;
    }

    /// We can safely remove part if all running transactions were started after part removal was committed
    return removal <= oldest_snapshot_version;
}

#define CREATION_TID_STR "creation_tid: "
#define CREATION_CSN_STR "creation_csn: "
#define REMOVAL_TID_STR  "removal_tid:  "
#define REMOVAL_CSN_STR  "removal_csn:  "


void VersionMetadata::writeCSN(WriteBuffer & buf, WhichCSN which_csn, bool internal /* = false*/) const
{
    if (which_csn == CREATION)
    {
        if (CSN creation = creation_csn.load())
        {
            writeCString("\n" CREATION_CSN_STR, buf);
            writeText(creation, buf);
        }
        else if (!internal)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "writeCSN called for creation_csn = 0, it's a bug");
    }
    else /// if (which_csn == REMOVAL)
    {
        if (CSN removal = removal_csn.load())
        {
            writeCString("\n" REMOVAL_CSN_STR, buf);
            writeText(removal, buf);
        }
        else if (!internal)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "writeCSN called for removal_csn = 0, it's a bug");
    }
}

void VersionMetadata::writeRemovalTID(WriteBuffer & buf, bool clear) const
{
    writeCString("\n" REMOVAL_TID_STR, buf);
    if (clear)
        TransactionID::write(Tx::EmptyTID, buf);
    else
        TransactionID::write(removal_tid, buf);
}

void VersionMetadata::write(WriteBuffer & buf) const
{
    writeCString("version: 1", buf);
    writeCString("\n" CREATION_TID_STR, buf);
    TransactionID::write(creation_tid, buf);
    writeCSN(buf, CREATION, /* internal */ true);

    if (removal_tid_lock)
    {
        assert(!removal_tid.isEmpty());
        assert(removal_tid.getHash() == removal_tid_lock);
        writeRemovalTID(buf);
        writeCSN(buf, REMOVAL, /* internal */ true);
    }
}

void VersionMetadata::read(ReadBuffer & buf)
{
    constexpr size_t size = sizeof(CREATION_TID_STR) - 1;
    static_assert(sizeof(CREATION_CSN_STR) - 1 == size);
    static_assert(sizeof(REMOVAL_TID_STR) - 1 == size);
    static_assert(sizeof(REMOVAL_CSN_STR) - 1 == size);

    assertString("version: 1", buf);
    assertString("\n" CREATION_TID_STR, buf);
    creation_tid = TransactionID::read(buf);
    if (buf.eof())
        return;

    String name;
    name.resize(size);

    auto read_csn = [&]()
    {
        UInt64 val;
        readText(val, buf);
        return val;
    };

    while (!buf.eof())
    {
        assertChar('\n', buf);
        buf.readStrict(name.data(), size);

        if (name == CREATION_CSN_STR)
        {
            assert(!creation_csn);
            creation_csn = read_csn();
        }
        else if (name == REMOVAL_TID_STR)
        {
            /// NOTE Metadata file may actually contain multiple creation TIDs, we need the last one.
            removal_tid = TransactionID::read(buf);
            if (!removal_tid.isEmpty())
                removal_tid_lock = removal_tid.getHash();
        }
        else if (name == REMOVAL_CSN_STR)
        {
            if (removal_tid.isEmpty())
                throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Found removal_csn in metadata file, but removal_tid is {}", removal_tid);
            assert(!removal_csn);
            removal_csn = read_csn();
        }
        else
        {
            throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Got unexpected content: {}", name);
        }
    }
}

String VersionMetadata::toString(bool one_line) const
{
    WriteBufferFromOwnString buf;
    write(buf);
    String res = buf.str();
    if (one_line)
        std::replace(res.begin(), res.end(), '\n', ' ');
    return res;
}


DataTypePtr getTransactionIDDataType()
{
    DataTypes types;
    types.push_back(std::make_shared<DataTypeUInt64>());
    types.push_back(std::make_shared<DataTypeUInt64>());
    types.push_back(std::make_shared<DataTypeUUID>());
    return std::make_shared<DataTypeTuple>(std::move(types));
}

}
