#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>

#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/TransactionLog.h>
#include <Interpreters/TransactionsInfoLog.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/TransactionID.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int CANNOT_PARSE_TEXT;
extern const int SERIALIZATION_ERROR;
extern const int CORRUPTED_DATA;
}


bool VersionMetadata::isVisible(const MergeTreeTransaction & txn)
{
    return isVisible(txn.getSnapshot(), txn.tid);
}

bool VersionMetadata::isVisible(CSN snapshot_version, TransactionID current_tid)
{
    LOG_TEST(log, "Check visible of object {} for snapshot_version {} and current_tid {}", getObjectName(), snapshot_version, current_tid);
    chassert(!creation_tid.isEmpty());
    CSN creation = getCreationCSN();
    TIDHash removal_lock = getRemovalTIDLock();
    CSN removal = getRemovalCSN();

    [[maybe_unused]] bool had_creation_csn = creation;
    [[maybe_unused]] bool had_removal_tid_lock = removal_lock;
    [[maybe_unused]] bool had_removal_csn = removal;

    chassert(!had_removal_csn || had_removal_csn && !getRemovalTID().isEmpty());
    chassert(!had_removal_csn || had_creation_csn);
    chassert(creation == Tx::UnknownCSN || creation == Tx::PrehistoricCSN || Tx::MaxReservedCSN < creation);
    chassert(removal == Tx::UnknownCSN || removal == Tx::PrehistoricCSN || Tx::MaxReservedCSN < removal);

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
    chassert(!had_creation_csn || (had_removal_tid_lock && !had_removal_csn));
    chassert(current_tid.isEmpty() || (creation_tid != current_tid && removal_lock != current_tid.getHash()));

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
    creation = TransactionLog::getCSNAndAssert(creation_tid, creation_csn);
    if (!creation)
    {
        return false; /// Part creation is not committed yet
    }

    /// We don't need to check if CSNs are already written or not,
    /// because once written CSN cannot be changed, so it's safe to overwrite it (with the same value).
    creation_csn.store(creation, std::memory_order_relaxed);

    if (removal_lock)
    {
        removal = TransactionLog::getCSN(removal_lock, &removal_csn);
        if (removal)
            removal_csn.store(removal, std::memory_order_relaxed);
    }

    return creation <= snapshot_version && (!removal || snapshot_version < removal);
}

void VersionMetadata::appendCreationCSNToStoredMetadata()
{
    chassert(!creation_tid.isEmpty());
    chassert(!creation_tid.isPrehistoric());
    chassert(creation_csn != 0);
    appendCreationCSNToStoredMetadataImpl();
}

void VersionMetadata::appendRemovalCSNToStoredMetadata()
{
    LOG_TEST(log, "Object {}, appendRemovalCSNToStoredMetadata {}", getObjectName(), getRemovalCSN());
    chassert(!creation_tid.isEmpty());
    chassert(!removal_tid.isEmpty());
    if (removal_tid == Tx::PrehistoricTID)
        chassert(removal_csn.load() == Tx::PrehistoricCSN);
    chassert(removal_csn != 0);
    appendRemovalCSNToStoredMetadataImpl();
}

void VersionMetadata::appendRemovalTIDToStoredMetadata(const TransactionID & tid)
{
    LOG_TEST(log, "Object {}, appendRemovalTIDToStoredMetadata {}, removal_csn {}", getObjectName(), tid, getRemovalCSN());
    chassert(!creation_tid.isEmpty());
    chassert(removal_csn == 0 || (removal_csn == Tx::PrehistoricCSN && tid.isPrehistoric()));

    if (creation_tid.isPrehistoric() && tid != Tx::EmptyTID)
    {
        /// Concurrent writes are not possible, because creation_csn is prehistoric and we own removal_tid_lock.

        /// It can happen that VersionMetadata::isVisible sets creation_csn to PrehistoricCSN when creation_tid is Prehistoric
        /// In order to avoid a race always write creation_csn as PrehistoricCSN for Prehistoric creation_tid
        assert(creation_csn == Tx::UnknownCSN || creation_csn == Tx::PrehistoricCSN);
        creation_csn.store(Tx::PrehistoricCSN);

        // Must update `removal_tid` first before storing
        auto prev_removal_tid = removal_tid;
        removal_tid = tid;
        try
        {
            storeMetadata(false);
        }
        catch (...)
        {
            removal_tid = prev_removal_tid;
            throw;
        }
        return;
    }

    appendRemovalTIDToStoredMetadataImpl(tid);
    removal_tid = tid;
}

/// It can be used for introspection purposes only
TransactionID VersionMetadata::getRemovalTIDForLogging() const
{
    TIDHash removal_lock = getRemovalTIDLock();
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
    throw Exception(
        ErrorCodes::SERIALIZATION_ERROR,
        "Serialization error: "
        "Transaction {} tried to remove data object {} from {}, "
        "but it's locked by another transaction (TID: {}, TIDH: {}) which is currently removing this part.",
        tid,
        part_desc,
        context.table.getNameForLogs(),
        getRemovalTIDForLogging(),
        locked_by);
}

void VersionMetadata::setCreationTID(const TransactionID & tid, TransactionInfoContext * context)
{
    /// NOTE ReplicatedMergeTreeSink may add one part multiple times
    chassert(creation_tid.isEmpty() || creation_tid == tid);
    creation_tid = tid;
    if (context)
        tryWriteEventToSystemLog(log, TransactionsInfoLogElement::ADD_PART, tid, *context);
}


bool VersionMetadata::canBeRemoved()
{
    if (creation_tid == Tx::PrehistoricTID)
    {
        /// Avoid access to Transaction log if transactions are not involved

        if (creation_csn.load(std::memory_order_relaxed) == Tx::RolledBackCSN)
            return true;

        TIDHash removal_lock = getRemovalTIDLock();

        if (removal_lock == Tx::PrehistoricTID.getHash())
            return true;
    }

    return canBeRemovedImpl(TransactionLog::instance().getOldestSnapshot());
}

String VersionMetadata::getObjectName() const
{
    return merge_tree_data_part->storage.getStorageID().getNameForLogs() + "|" + merge_tree_data_part->name;
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
        creation = TransactionLog::getCSNAndAssert(creation_tid, creation_csn);
        if (creation)
            setCreationCSN(creation);
        else
            return false;
    }

    /// Part is probably visible for some transactions (part is too new or the oldest snapshot is too old)
    if (oldest_snapshot_version < creation)
        return false;

    CSN removal = getRemovalCSN();
    if (removal && removal <= oldest_snapshot_version)
        return true;

    TIDHash removal_lock = getRemovalTIDLock();
    /// Part is active
    if (!removal_lock)
        return false;

    if (!removal)
    {
        /// Part removal is not committed yet
        removal = TransactionLog::getCSN(removal_lock, &removal_csn);
        if (removal)
            removal_csn.store(removal, std::memory_order_relaxed);
        else
            return false;
    }

    /// We can safely remove part if all running transactions were started after part removal was committed
    return removal <= oldest_snapshot_version;
}

bool VersionMetadata::verifyMetadata(LoggerPtr logger)
{
    /// Check if CSNs were written after committing transaction, update and write if needed.
    bool version_updated = false;
    chassert(!getCreationTID().isEmpty());

    if (!creation_csn)
    {
        LOG_TRACE(logger, "Object {} does not have creation_csn, try to get it from creation_tid {}", getObjectName(), creation_tid);

        auto csn_of_creation_tid = TransactionLog::getCSNAndAssert(creation_tid, creation_csn);
        if (!csn_of_creation_tid)
        {
            LOG_TRACE(
                logger,
                "Object {}, unable to find creation_csn of creation_tid {}, resetting it to {}",
                getObjectName(),
                creation_tid,
                Tx::RolledBackCSN);
            setCreationCSN(Tx::RolledBackCSN);
            version_updated = true;
        }
        else
        {
            LOG_TRACE(
                logger,
                "Object {}, found creation_csn {} of creation_tid {}, updating it",
                getObjectName(),
                csn_of_creation_tid,
                creation_tid);
            setCreationCSN(csn_of_creation_tid);
            version_updated = true;
        }
    }

    if (!getRemovalCSN())
    {
        if (!getRemovalTID().isEmpty())
        {
            auto tid_running = TransactionLog::instance().tryGetRunningTransaction(getRemovalTID().getHash());
            LOG_TRACE(logger, "Object {} does not have removal_csn, try to get it from removal_tid {}", getObjectName(), removal_tid);
            auto csn_of_removal_tid = TransactionLog::getCSNAndAssert(removal_tid, removal_csn);
            if (csn_of_removal_tid)
            {
                LOG_TRACE(
                    logger,
                    "Object {}, found removal_csn {} of removal_tid {}, updating it",
                    getObjectName(),
                    csn_of_removal_tid,
                    removal_tid);
                setRemovalCSN(csn_of_removal_tid);
                version_updated = true;
            }
            else if (tid_running == NO_TRANSACTION_PTR)
            {
                LOG_TRACE(
                    logger,
                    "Object {}, unable to find removal_csn of non-running removal_tid {}, restting removal_tid and removal lock",
                    getObjectName(),
                    removal_tid);
                setRemovalTIDLock(0);
                if (!removal_tid.isEmpty())
                {
                    removal_tid = Tx::EmptyTID;
                    version_updated = true;
                }
            }
            else
            {
                LOG_TRACE(logger, "Object {}, unable to find removal_csn of removal_tid {}", getObjectName(), removal_tid);
                TIDHash current_removal_tid_lock_hash = getRemovalTIDLock();
                TIDHash removal_tid_hash = removal_tid.getHash();

                if (!current_removal_tid_lock_hash)
                {
                    LOG_TRACE(
                        logger,
                        "Object {}, no removal_tid_lock, the transaction was not committed, resetting removal_tid",
                        merge_tree_data_part->name);
                }
                else if (current_removal_tid_lock_hash == removal_tid_hash)
                {
                    LOG_TRACE(
                        logger,
                        "Object {}, removal_tid_lock hash {} is matched to removal_tid hash",
                        getObjectName(),
                        current_removal_tid_lock_hash);
                }
                else
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Object {}, removal_tid_lock hash {} is not matched to the removal_tid hash {}",
                        getObjectName(),
                        current_removal_tid_lock_hash,
                        removal_tid_hash);
                }
            }
        }
    }


    /// Sanity checks
    bool csn_order = !getRemovalCSN() || getCreationCSN() <= getRemovalCSN() || getRemovalCSN() == Tx::PrehistoricCSN;
    bool min_start_csn_order = getCreationTID().start_csn <= getCreationCSN();
    bool max_start_csn_order = !getRemovalCSN() || getRemovalTID().start_csn <= getRemovalCSN();
    // bool max_start_csn_order = getRemovalTID().start_csn <= getRemovalCSN();
    bool creation_csn_known = getCreationCSN();

    if (!csn_order || !min_start_csn_order || !max_start_csn_order || !creation_csn_known)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Object {} has invalid version metadata: {}", getObjectName(), toString());

    return version_updated;
}


VersionMetadata::Info VersionMetadata::readFromBufferHelper(ReadBuffer & buf)
{
    constexpr size_t size = sizeof(CREATION_TID_STR) - 1;
    static_assert(sizeof(CREATION_CSN_STR) - 1 == size);
    static_assert(sizeof(REMOVAL_TID_STR) - 1 == size);
    static_assert(sizeof(REMOVAL_CSN_STR) - 1 == size);

    Info result;
    assertString("version: 1", buf);
    assertString(String("\n") + CREATION_TID_STR, buf);
    result.creation_tid = TransactionID::read(buf);
    if (buf.eof())
        return result;

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
            auto new_val = read_csn();
            chassert(!result.creation_csn || (result.creation_csn == new_val && result.creation_csn == Tx::PrehistoricCSN));
            result.creation_csn = new_val;
        }
        else if (name == REMOVAL_TID_STR)
        {
            /// NOTE Metadata file may actually contain multiple creation TIDs, we need the last one.
            result.removal_tid = TransactionID::read(buf);
        }
        else if (name == REMOVAL_CSN_STR)
        {
            auto reading_csn = read_csn();
            if (result.removal_tid.isEmpty())
                throw Exception(
                    ErrorCodes::CANNOT_PARSE_TEXT,
                    "Found removal_csn {} in metadata file, but removal_tid is {}",
                    reading_csn,
                    result.removal_tid);
            chassert(!result.removal_csn);
            result.removal_csn = reading_csn;
        }
        else
        {
            throw Exception(ErrorCodes::CANNOT_PARSE_TEXT, "Got unexpected content: {}", name);
        }
    }
    return result;
}

void VersionMetadata::readFromBuffer(ReadBuffer & buf)
{
    auto info = readFromBufferHelper(buf);
    creation_tid = info.creation_tid;
    removal_tid = info.removal_tid;
    creation_csn = info.creation_csn;
    removal_csn = info.removal_csn;
}


String VersionMetadata::toString(bool one_line) const
{
    WriteBufferFromOwnString buf;
    writeToBuffer(buf);
    String res = buf.str();
    if (one_line)
        std::replace(res.begin(), res.end(), '\n', ' ');
    return res;
}

void VersionMetadata::loadAndVerifyMetadata(LoggerPtr logger)
{
    loadMetadata();

    auto updated = verifyMetadata(logger);
    if (updated)
        storeMetadata(/*force=*/true);

    if (getRemovalCSN())
        setRemovalTIDLock(0);
}


bool VersionMetadata::assertHasValidMetadata() const
{
    auto current_removal_tid_lock = getRemovalTIDLock();
    String content;

    try
    {
        auto persisted_info = readStoredMetadata(content);

        if (creation_tid != persisted_info.creation_tid)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Invalid version metadata, creation_tid mismatched {} and {}",
                creation_tid,
                persisted_info.creation_tid);

        if (removal_tid != persisted_info.removal_tid && removal_tid != Tx::PrehistoricTID)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Invalid version metadata, removal_tid mismatched {} and  {}",
                removal_tid,
                persisted_info.removal_tid);

        if (creation_csn != persisted_info.creation_csn && creation_csn == Tx::RolledBackCSN)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Invalid version metadata, creation_csn mismatched {} and  {}",
                creation_csn.load(),
                persisted_info.creation_csn);

        if (removal_csn != persisted_info.removal_csn && removal_csn != Tx::PrehistoricCSN)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Invalid version metadata, removal_csn mismatched {} and  {}",
                removal_csn.load(),
                persisted_info.removal_csn);

        if (persisted_info.removal_csn != 0 && current_removal_tid_lock == 0)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Invalid removal_tid_lock, removal_csn {}, removal_tid_lock {}",
                persisted_info.removal_csn,
                current_removal_tid_lock);

        if (persisted_info.removal_csn != 0 && persisted_info.removal_tid.isEmpty())
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Invalid removal_tid, removal_csn {}, removal_tid {}",
                persisted_info.removal_csn,
                persisted_info.removal_tid);


        return true;
    }
    catch (...)
    {
        WriteBufferFromOwnString expected;
        writeToBuffer(expected);
        tryLogCurrentException(
            merge_tree_data_part->storage.log,
            fmt::format(
                "Metadata content {}\nexpected:\n{}\nlock: {}\nname: {}",
                content,
                expected.str(),
                current_removal_tid_lock,
                merge_tree_data_part->name));
        return false;
    }
}


void VersionMetadata::writeToBuffer(WriteBuffer & buf) const
{
    writeCString("version: 1", buf);
    writeCString("\n", buf);
    writeCString(CREATION_TID_STR, buf);

    TransactionID::write(creation_tid, buf);
    writeCreationCSNToBuffer(buf, /*throw_if_csn_unknown=*/true);
    if (!removal_tid.isEmpty())
        writeRemovalTIDToBuffer(buf, removal_tid);

    if (getRemovalCSN())
    {
        chassert(!removal_tid.isEmpty());
        writeRemovalCSNToBuffer(buf, /*throw_if_csn_unknown=*/true);
    }
}

void VersionMetadata::writeCreationCSNToBuffer(WriteBuffer & buf, bool throw_if_csn_unknown) const
{
    if (CSN creation = creation_csn.load())
    {
        writeCString("\n", buf);
        writeCString(CREATION_CSN_STR, buf);
        writeText(creation, buf);
    }
    else if (!throw_if_csn_unknown)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "writeCreationCSNToBuffer called for creation_csn = 0, it's a bug");
}

void VersionMetadata::writeRemovalCSNToBuffer(WriteBuffer & buf, bool throw_if_csn_unknown) const
{
    if (CSN removal = removal_csn.load())
    {
        writeCString("\n", buf);
        writeCString(REMOVAL_CSN_STR, buf);
        writeText(removal, buf);
    }
    else if (!throw_if_csn_unknown)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "writeRemovalCSNToBuffer called for removal_csn = 0, it's a bug");
}

void VersionMetadata::writeRemovalTIDToBuffer(WriteBuffer & buf, const TransactionID & tid) const
{
    writeCString("\n", buf);
    writeCString(REMOVAL_TID_STR, buf);
    TransactionID::write(tid, buf);
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
