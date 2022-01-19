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

static void tryWriteEventToSystemLog(Poco::Logger * log,
                                     TransactionsInfoLogElement::Type type, const TransactionID & tid,
                                     const TransactionInfoContext & context)
try
{
    auto system_log =  Context::getGlobalContextInstance()->getTransactionsInfoLog();
    if (!system_log)
        return;

    TransactionsInfoLogElement elem;
    elem.type = type;
    elem.tid = tid;
    elem.fillCommonFields(&context);
    system_log->add(elem);
}
catch (...)
{
    tryLogCurrentException(log);
}

VersionMetadata::VersionMetadata()
{
    /// It would be better to make it static, but static loggers do not work for some reason (initialization order?)
    log = &Poco::Logger::get("VersionMetadata");
}

/// It can be used for introspection purposes only
TransactionID VersionMetadata::getMaxTID() const
{
    TIDHash max_lock = maxtid_lock.load();
    if (max_lock)
    {
        if (auto txn = TransactionLog::instance().tryGetRunningTransaction(max_lock))
            return txn->tid;
        if (max_lock == Tx::PrehistoricTID.getHash())
            return Tx::PrehistoricTID;
    }

    if (maxcsn.load(std::memory_order_relaxed))
    {
        /// maxtid cannot be changed since we have maxcsn, so it's readonly
        return maxtid;
    }

    return Tx::EmptyTID;
}

void VersionMetadata::lockMaxTID(const TransactionID & tid, const TransactionInfoContext & context)
{
    LOG_TEST(log, "Trying to lock maxtid by {}, table: {}, part: {}", tid, context.table.getNameForLogs(), context.part_name);
    TIDHash locked_by = 0;
    if (tryLockMaxTID(tid, context, &locked_by))
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
                    tid, part_desc, context.table.getNameForLogs(), getMaxTID(), locked_by);
}

bool VersionMetadata::tryLockMaxTID(const TransactionID & tid, const TransactionInfoContext & context, TIDHash * locked_by_id)
{
    assert(!tid.isEmpty());
    TIDHash max_lock_value = tid.getHash();
    TIDHash expected_max_lock_value = 0;
    bool locked = maxtid_lock.compare_exchange_strong(expected_max_lock_value, max_lock_value);
    if (!locked)
    {
        if (tid == Tx::PrehistoricTID && expected_max_lock_value == Tx::PrehistoricTID.getHash())
        {
            /// Don't need to lock part for queries without transaction
            //FIXME Transactions: why is it possible?
            LOG_TEST(log, "Assuming maxtid is locked by {}, table: {}, part: {}", tid, context.table.getNameForLogs(), context.part_name);
            return true;
        }

        if (locked_by_id)
            *locked_by_id = expected_max_lock_value;
        return false;
    }

    maxtid = tid;
    tryWriteEventToSystemLog(log, TransactionsInfoLogElement::LOCK_PART, tid, context);
    return true;
}

void VersionMetadata::unlockMaxTID(const TransactionID & tid, const TransactionInfoContext & context)
{
    LOG_TEST(log, "Unlocking maxtid by {}, table: {}, part: {}", tid, context.table.getNameForLogs(), context.part_name);
    assert(!tid.isEmpty());
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

    tryWriteEventToSystemLog(log, TransactionsInfoLogElement::UNLOCK_PART, tid, context);
}

bool VersionMetadata::isMaxTIDLocked() const
{
    return maxtid_lock.load() != 0;
}

void VersionMetadata::setMinTID(const TransactionID & tid, const TransactionInfoContext & context)
{
    /// TODO Transactions: initialize it in constructor on part creation and remove this method
    /// FIXME ReplicatedMergeTreeBlockOutputStream may add one part multiple times
    assert(mintid.isEmpty() || mintid == tid);
    const_cast<TransactionID &>(mintid) = tid;

    tryWriteEventToSystemLog(log, TransactionsInfoLogElement::ADD_PART, tid, context);
}

bool VersionMetadata::isVisible(const MergeTreeTransaction & txn)
{
    return isVisible(txn.getSnapshot(), txn.tid);
}

bool VersionMetadata::isVisible(Snapshot snapshot_version, TransactionID current_tid)
{
    assert(!mintid.isEmpty());
    CSN min = mincsn.load(std::memory_order_relaxed);
    TIDHash max_lock = maxtid_lock.load(std::memory_order_relaxed);
    CSN max = maxcsn.load(std::memory_order_relaxed);

    //LOG_TEST(log, "Checking if mintid {} mincsn {} maxtidhash {} maxcsn {} visible for {} {}", mintid, min, max_lock, max, snapshot_version, current_tid);

    [[maybe_unused]] bool had_mincsn = min;
    [[maybe_unused]] bool had_maxtid = max_lock;
    [[maybe_unused]] bool had_maxcsn = max;
    assert(!had_maxcsn || had_maxtid);
    assert(!had_maxcsn || had_mincsn);
    assert(min == Tx::UnknownCSN || min == Tx::PrehistoricCSN || Tx::MaxReservedCSN < min);
    assert(max == Tx::UnknownCSN || max == Tx::PrehistoricCSN || Tx::MaxReservedCSN < max);

    /// Fast path:

    /// Part is definitely not visible if:
    /// - creation was committed after we took the snapshot
    /// - removal was committed before we took the snapshot
    /// - current transaction is removing it
    if (min && snapshot_version < min)
        return false;
    if (max && max <= snapshot_version)
        return false;
    if (!current_tid.isEmpty() && max_lock && max_lock == current_tid.getHash())
        return false;

    /// Otherwise, part is definitely visible if:
    /// - creation was committed before we took the snapshot and nobody tried to remove the part
    /// - creation was committed before and removal was committed after
    /// - current transaction is creating it
    if (min && min <= snapshot_version && !max_lock)
        return true;
    if (min && min <= snapshot_version && max && snapshot_version < max)
        return true;
    if (!current_tid.isEmpty() && mintid == current_tid)
        return true;

    /// End of fast path.

    /// Data part has mintid/maxtid, but does not have mincsn/maxcsn.
    /// It means that some transaction is creating/removing the part right now or has done it recently
    /// and we don't know if it was already committed or not.
    assert(!had_mincsn || (had_maxtid && !had_maxcsn));
    assert(current_tid.isEmpty() || (mintid != current_tid && max_lock != current_tid.getHash()));

    /// Before doing CSN lookup, let's check some extra conditions.
    /// If snapshot_version <= some_tid.start_csn, then changes of the transaction with some_tid
    /// are definitely not visible for us (because the transaction can be committed with greater CSN only),
    /// so we don't need to check if it was committed.
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

    /// We don't need to check if CSNs are already written or not,
    /// because once written CSN cannot be changed, so it's safe to overwrite it (with the same value).
    mincsn.store(min, std::memory_order_relaxed);

    if (max_lock)
    {
        max = TransactionLog::instance().getCSN(max_lock);
        if (max)
            maxcsn.store(max, std::memory_order_relaxed);
    }

    return min <= snapshot_version && (!max || snapshot_version < max);
}

bool VersionMetadata::canBeRemoved(Snapshot oldest_snapshot_version)
{
    CSN min = mincsn.load(std::memory_order_relaxed);
    /// We can safely remove part if its creation was rolled back
    if (min == Tx::RolledBackCSN)
        return true;

    if (!min)
    {
        /// Cannot remove part if its creation not committed yet
        min = TransactionLog::instance().getCSN(mintid);
        if (min)
            mincsn.store(min, std::memory_order_relaxed);
        else
            return false;
    }

    /// Part is probably visible for some transactions (part is too new or the oldest snapshot is too old)
    if (oldest_snapshot_version < min)
        return false;

    TIDHash max_lock = maxtid_lock.load(std::memory_order_relaxed);
    /// Part is active
    if (!max_lock)
        return false;

    CSN max = maxcsn.load(std::memory_order_relaxed);
    if (!max)
    {
        /// Part removal is not committed yet
        max = TransactionLog::instance().getCSN(max_lock);
        if (max)
            maxcsn.store(max, std::memory_order_relaxed);
        else
            return false;
    }

    /// We can safely remove part if all running transactions were started after part removal was committed
    return max <= oldest_snapshot_version;
}

void VersionMetadata::write(WriteBuffer & buf) const
{
    writeCString("version: 1", buf);
    writeCString("\nmintid: ", buf);
    TransactionID::write(mintid, buf);
    if (CSN min = mincsn.load())
    {
        writeCString("\nmincsn: ", buf);
        writeText(min, buf);
    }

    if (maxtid_lock)
    {
        assert(!maxtid.isEmpty());
        assert(maxtid.getHash() == maxtid_lock);
        writeCString("\nmaxtid: ", buf);
        TransactionID::write(maxtid, buf);
        if (CSN max = maxcsn.load())
        {
            writeCString("\nmaxcsn: ", buf);
            writeText(max, buf);
        }
    }
}

void VersionMetadata::read(ReadBuffer & buf)
{
    assertString("version: 1", buf);
    assertString("\nmintid: ", buf);
    mintid = TransactionID::read(buf);
    if (buf.eof())
        return;

    String name;
    constexpr size_t size = 8;
    name.resize(size);

    assertChar('\n', buf);
    buf.readStrict(name.data(), size);
    if (name == "mincsn: ")
    {
        UInt64 min;
        readText(min, buf);
        mincsn = min;
        if (buf.eof())
            return;

        assertChar('\n', buf);
        buf.readStrict(name.data(), size);
    }

    if (name == "maxtid: ")
    {
        maxtid = TransactionID::read(buf);
        maxtid_lock = maxtid.getHash();
        if (buf.eof())
            return;

        assertChar('\n', buf);
        buf.readStrict(name.data(), size);
    }

    if (name == "maxcsn: ")
    {
        if (maxtid.isEmpty())
            throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Found maxcsn in metadata file, but maxtid is {}", maxtid);
        UInt64 max;
        readText(max, buf);
        maxcsn = max;
    }

    assertEOF(buf);
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
