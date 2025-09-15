#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>

#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/TransactionLog.h>
#include <Interpreters/TransactionsInfoLog.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <base/sleep.h>
#include <Common/TransactionID.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int SERIALIZATION_ERROR;
extern const int CORRUPTED_DATA;
}

VersionMetadata::VersionMetadata(IMergeTreeDataPart * merge_tree_data_part_)
    : merge_tree_data_part(merge_tree_data_part_)
{
}

bool VersionMetadata::isVisible(const MergeTreeTransaction & txn)
{
    return isVisible(txn.getSnapshot(), txn.tid);
}

bool VersionMetadata::isVisible(CSN snapshot_version, TransactionID current_tid)
{
    auto info = getInfo();

    if (auto visible = info.isVisible(snapshot_version, current_tid))
        return *visible;

    // Data part has creation_tid/removal_tid, but does not have creation_csn/removal_csn

    /// Before doing CSN lookup, let's check some extra conditions.
    /// If snapshot_version <= some_tid.start_csn, then changes of the transaction with some_tid
    /// are definitely not visible for us (because the transaction can be committed with greater CSN only),
    /// so we don't need to check if it was committed.
    if (snapshot_version <= info.creation_tid.start_csn)
        return false;

    /// Check if creation_tid/removal_tid transactions are committed and write CSNs
    /// TODO Transactions: we probably need more optimizations here
    /// to avoid some CSN lookups or make the lookups cheaper.
    /// NOTE: Old enough committed parts always have written CSNs,
    /// so we can determine their visibility through fast path.
    /// But for long-running writing transactions we will always do
    /// CSN lookup and get 0 (UnknownCSN) until the transaction is committed/rolled back.
    auto creation = TransactionLog::getCSNAndAssert(info.creation_tid, creation_csn);
    if (!creation)
        return false; /// Part creation is not committed yet

    /// We don't need to check if CSNs are already written or not,
    /// because once written CSN cannot be changed, so it's safe to overwrite it (with the same value).
    setCreationCSN(creation);

    auto fresh_removal_csn = info.removal_csn;
    auto current_removal_tid_hash = info.getCurrentRemovalTIDHash();
    if (info.removal_tid_lock)
        fresh_removal_csn = TransactionLog::getCSN(current_removal_tid_hash, &removal_csn);

    return creation <= snapshot_version && (!fresh_removal_csn || snapshot_version < fresh_removal_csn);
}

void VersionMetadata::storeRemovalCSN(CSN csn)
{
    setRemovalCSN(csn);
    storeRemovalCSNToStoredMetadata();
}

void VersionMetadata::setRemovalCSN(CSN csn)
{
    LOG_DEBUG(log, "Object {}, setRemovalCSN {}", getObjectName(), csn);
    chassert(!getRemovalTID().isEmpty());
    removal_csn.store(csn);
}

void VersionMetadata::storeCreationCSN(CSN csn)
{
    LOG_DEBUG(log, "Object {}, storeCreationCSN {}", getObjectName(), csn);
    setCreationCSN(csn);
    storeCreationCSNToStoredMetadata();
}

void VersionMetadata::storeRemovalTID(const TransactionID & tid)
{
    LOG_DEBUG(log, "Object {}, storeRemovalTID {}", getObjectName(), tid);
    setRemovalTID(tid);
    storeRemovalTIDToStoredMetadata();
}

void VersionMetadata::setRemovalTID(const TransactionID & tid)
{
    LOG_DEBUG(
        log, "Object {}, setRemovalTID {}, hash {}, removal_tid_lock {}", getObjectName(), tid, tid.getHash(), removal_tid_lock.load());
    chassert(tid.isEmpty() || tid.getHash() == removal_tid_lock.load());
    std::lock_guard lock{creation_and_removal_tid_mutex};
    removal_tid = tid;
}

TransactionID VersionMetadata::getRemovalTID() const
{
    std::lock_guard lock{creation_and_removal_tid_mutex};
    return removal_tid;
}

/// It can be used for introspection purposes only
TransactionID VersionMetadata::getRemovalTIDForLogging() const
{
    if (removal_csn.load(std::memory_order_relaxed))
    {
        /// removal_tid cannot be changed since we have removal_csn, so it's readonly
        return getRemovalTID();
    }

    auto removal_tid_val = getRemovalTID();
    if (removal_tid_val.isEmpty())
        return Tx::EmptyTID;


    if (removal_tid_val == Tx::PrehistoricTID)
        return Tx::PrehistoricTID;

    if (auto txn = TransactionLog::instance().tryGetRunningTransaction(removal_tid_val.getHash()))
        return txn->tid;

    return Tx::EmptyTID;
}

void VersionMetadata::lockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context)
{
    LOG_TEST(
        log,
        "Object {}, trying to lock removal_tid by {}, table: {}, part: {}",
        getObjectName(),
        tid,
        context.table.getNameForLogs(),
        context.part_name);

    String part_desc;
    auto removal = getRemovalCSN();
    if (removal != 0)
    {
        if (context.covering_part.empty())
            part_desc = context.part_name;
        else
            part_desc = fmt::format("{} (covered by {})", context.part_name, context.covering_part);
        throw Exception(
            ErrorCodes::SERIALIZATION_ERROR,
            "Serialization error: Transaction {} tried to remove data object {} from {}, but it's removed by another transaction with csn "
            "{}",
            tid,
            part_desc,
            context.table.getNameForLogs(),
            removal);
    }

    TIDHash locked_by = 0;
    if (tryLockRemovalTID(tid, context, &locked_by))
        return;

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
    LOG_DEBUG(log, "Object {}, setCreationTID {}", getObjectName(), tid);
    /// NOTE ReplicatedMergeTreeSink may add one part multiple times
    [[maybe_unused]] auto creation_tid_val = getCreationTID();
    chassert(creation_tid_val.isEmpty() || creation_tid_val == tid);
    {
        std::lock_guard lock{creation_and_removal_tid_mutex};
        creation_tid = tid;
    }
    if (context)
        tryWriteEventToSystemLog(log, TransactionsInfoLogElement::ADD_PART, tid, *context);
}

TransactionID VersionMetadata::getCreationTID() const
{
    std::lock_guard lock{creation_and_removal_tid_mutex};
    return creation_tid;
}


bool VersionMetadata::canBeRemoved()
{
    if (getCreationTID() == Tx::PrehistoricTID)
    {
        /// Avoid access to Transaction log if transactions are not involved

        if (creation_csn.load(std::memory_order_relaxed) == Tx::RolledBackCSN)
            return true;

        if (getRemovalTID() == Tx::PrehistoricTID)
            return true;

        TIDHash current_removal_tid_hash = getCurrentRemovalTIDHash();
        if (current_removal_tid_hash == Tx::PrehistoricTID.getHash())
            return true;
    }

    return canBeRemovedImpl(TransactionLog::instance().getOldestSnapshot());
}


void VersionMetadata::storeCreationCSNToStoredMetadata()
{
    [[maybe_unused]] auto creation_tid_val = getCreationTID();
    [[maybe_unused]] auto creation_csn_val = getCreationCSN();
    chassert(!creation_tid_val.isEmpty());
    chassert(
        creation_csn_val == Tx::RolledBackCSN || ///
        Tx::PrehistoricCSN && creation_tid_val.isPrehistoric() || ///
        creation_csn_val != Tx::PrehistoricCSN && !creation_tid_val.isPrehistoric());
    chassert(creation_csn_val != 0);
    storeCreationCSNToStoredMetadataImpl();
}

void VersionMetadata::storeRemovalCSNToStoredMetadata()
{
    LOG_DEBUG(log, "Object {}, storeRemovalCSNToStoredMetadata {}", getObjectName(), getRemovalCSN());
    chassert(!getCreationTID().isEmpty());

    auto removal_tid_val = getRemovalTID();
    chassert(!removal_tid_val.isEmpty());
    if (removal_tid_val == Tx::PrehistoricTID)
        chassert(removal_csn.load() == Tx::PrehistoricCSN);
    chassert(removal_csn != 0);
    storeRemovalCSNToStoredMetadataImpl();
}

void VersionMetadata::storeRemovalTIDToStoredMetadata()
{
    auto creation_tid_val = getCreationTID();
    auto removal_tid_val = getRemovalTID();
    LOG_DEBUG(log, "Object {}, storeRemovalTIDToStoredMetadata {}, removal_csn {}", getObjectName(), removal_tid_val, getRemovalCSN());
    chassert(!creation_tid_val.isEmpty());
    chassert(removal_csn == 0 || (removal_csn == Tx::PrehistoricCSN && removal_tid_val.isPrehistoric()));

    if (creation_tid_val.isPrehistoric() && removal_tid_val != Tx::EmptyTID)
    {
        /// Concurrent writes are not possible, because creation_csn is prehistoric and we own removal_tid_lock.

        /// It can happen that VersionMetadata::isVisible sets creation_csn to PrehistoricCSN when creation_tid is Prehistoric
        /// In order to avoid a race always write creation_csn as PrehistoricCSN for Prehistoric creation_tid
        assert(creation_csn == Tx::UnknownCSN || creation_csn == Tx::PrehistoricCSN);
        creation_csn.store(Tx::PrehistoricCSN);

        storeMetadata(false);
        return;
    }

    storeRemovalTIDToStoredMetadataImpl();
}

TIDHash VersionMetadata::getCurrentRemovalTIDHash()
{
    // Check if the object is locked by any transaction first
    auto removal_tid_val = getRemovalTID();
    TIDHash current_removal_tid_hash = getRemovalTIDLock();
    if (!current_removal_tid_hash && !removal_tid_val.isEmpty())
    {
        // The object might be unlocked, check removal_tid
        current_removal_tid_hash = removal_tid_val.getHash();
    }

    return current_removal_tid_hash;
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
        creation = TransactionLog::getCSNAndAssert(getCreationTID(), creation_csn);
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

    TIDHash current_removal_tid_hash = getCurrentRemovalTIDHash();
    /// Part is active
    if (!current_removal_tid_hash)
        return false;

    if (!removal)
    {
        /// Part removal is not committed yet
        removal = TransactionLog::getCSN(current_removal_tid_hash, &removal_csn);
        if (!removal)
            return false;
    }

    /// We can safely remove part if all running transactions were started after part removal was committed
    return removal <= oldest_snapshot_version;
}

bool VersionMetadata::adjustMetadata(LoggerPtr logger)
{
    /// Check if CSNs were written after committing transaction, update and write if needed.
    bool version_updated = false;

    auto creation_tid_val = getCreationTID();
    auto removal_tid_val = getRemovalTID();

    chassert(!creation_tid_val.isEmpty());

    bool creation_tid_running = false;
    if (!creation_csn)
    {
        LOG_TRACE(logger, "Object {} does not have creation_csn {}", getObjectName(), creation_tid_val);

        creation_tid_running = TransactionLog::instance().tryGetRunningTransaction(creation_tid_val.getHash()) != nullptr;
        if (creation_tid_running)
        {
            LOG_TRACE(logger, "Creation_tid {} is running", creation_tid_val);
        }
        else
        {
            LOG_TRACE(logger, "Creation_tid {} is not running, trying to get its creation CSN", creation_tid_val);
            auto csn_of_creation_tid = TransactionLog::getCSNAndAssert(creation_tid_val, creation_csn);
            if (!csn_of_creation_tid)
            {
                LOG_TRACE(
                    logger,
                    "Object {}, unable to find creation_csn of creation_tid {}, resetting it to {}",
                    getObjectName(),
                    creation_tid_val,
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
                    creation_tid_val);
                setCreationCSN(csn_of_creation_tid);
                version_updated = true;
            }
        }
    }
    if (getRemovalCSN())
    {
        setRemovalTIDLock(0);
    }
    else
    {
        if (!removal_tid_val.isEmpty())
        {
            auto tid_running = TransactionLog::instance().tryGetRunningTransaction(removal_tid_val.getHash());
            LOG_TRACE(logger, "Object {} does not have removal_csn, try to get it from removal_tid {}", getObjectName(), removal_tid_val);
            auto csn_of_removal_tid = TransactionLog::getCSNAndAssert(removal_tid_val, removal_csn);
            if (csn_of_removal_tid)
            {
                LOG_TRACE(
                    logger,
                    "Object {}, found removal_csn {} of removal_tid {}, updating it",
                    getObjectName(),
                    csn_of_removal_tid,
                    removal_tid_val);
                setRemovalCSN(csn_of_removal_tid);
                version_updated = true;
            }
            else if (tid_running == NO_TRANSACTION_PTR)
            {
                LOG_TRACE(
                    logger,
                    "Object {}, unable to find removal_csn of non-running removal_tid {}, resetting removal_tid and removal lock",
                    getObjectName(),
                    removal_tid_val);
                setRemovalTIDLock(0);
                if (!removal_tid_val.isEmpty())
                {
                    setRemovalTID(Tx::EmptyTID);
                    version_updated = true;
                }
            }
            else
            {
                LOG_TRACE(logger, "Object {}, unable to find removal_csn of removal_tid {}", getObjectName(), removal_tid_val);
                TIDHash current_removal_tid_lock_hash = getRemovalTIDLock();
                TIDHash current_removal_tid_hash = removal_tid_val.getHash();

                if (!current_removal_tid_lock_hash)
                {
                    LOG_TRACE(
                        logger,
                        "Object {}, no removal_tid_lock, the transaction was not committed, resetting removal_tid",
                        merge_tree_data_part->name);
                }
                else if (current_removal_tid_lock_hash == current_removal_tid_hash)
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
                        current_removal_tid_hash);
                }
            }
        }
    }
    return version_updated;
}
void VersionMetadata::verifyMetadata() const
{
    verifyMetadata(getObjectName(), getInfo());
}

void VersionMetadata::verifyMetadata(const String & object_name, const VersionInfo & info)
{
    MergeTreeTransactionPtr txn{nullptr};
    if (!info.creation_tid.isPrehistoric())
        txn = TransactionLog::instance().tryGetRunningTransaction(info.creation_tid.getHash());

    /// Sanity checks
    if (txn != nullptr)
    {
        if (info.creation_csn && info.creation_csn != Tx::RolledBackCSN && txn->getCSN() != info.creation_csn)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Object {}, creation_tid {} (with csn {}) is running, invalid csn {}",
                object_name,
                info.creation_tid,
                txn->getCSN(),
                info.creation_csn);

        if (info.removal_csn)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Object {}, creation_tid {} is running, expect no removal_csn, actual {}",
                object_name,
                info.creation_tid,
                info.removal_csn);

        if (!info.removal_tid.isEmpty() && info.removal_tid != info.creation_tid)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Object {}, creation_tid {} is running, got invalid removal_tid {}",
                object_name,
                info.creation_tid,
                info.removal_tid);
    }
    else
    {
        if (!info.creation_tid.isPrehistoric() && !info.creation_csn)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Object {}, creation_tid {} is not running, expect valid creation_csn, actual {}",
                object_name,
                info.creation_tid,
                info.creation_csn);

        if (info.removal_csn && info.removal_csn != Tx::PrehistoricCSN && info.creation_csn > info.removal_csn)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Object {}, creation_csn {} should not be greater than removal_csn {}",
                object_name,
                info.creation_csn,
                info.removal_csn);

        if (!info.creation_tid.isPrehistoric() && info.creation_tid.start_csn > info.creation_csn)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Object {}, start_csn of creation_tid {} should not be greater than creation_csn {}",
                object_name,
                info.creation_tid,
                info.creation_csn);

        if (info.removal_csn && info.removal_tid.start_csn > info.removal_csn)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Object {}, start_csn of removal_tid {} should not be greater than removal_csn {}",
                object_name,
                info.creation_tid,
                info.creation_csn);
    }

    if (info.removal_tid_lock != 0)
    {
        if (!info.removal_tid.isEmpty() && info.removal_tid.getHash() != info.removal_tid_lock)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Object {}, removal_tid {} and removal_tid_lock {} mismatched",
                object_name,
                info.removal_tid,
                info.removal_tid_lock);
    }
}

void VersionMetadata::updateFromInfo(const VersionInfo & info)
{
    LOG_DEBUG(
        log,
        "Object {}, update version info {} -> {}",
        getObjectName(),
        getInfo().toString(/*one_line=*/true),
        info.toString(/*one_line=*/true));

    std::lock_guard lock{creation_and_removal_tid_mutex};
    removal_tid_lock = info.removal_tid_lock;
    creation_tid = info.creation_tid;
    removal_tid = info.removal_tid;
    creation_csn = info.creation_csn;
    removal_csn = info.removal_csn;
}

void VersionMetadata::readFromBuffer(ReadBuffer & buf, bool one_line)
{
    VersionInfo info;
    info.readFromBuffer(buf, one_line);
    updateFromInfo(info);
}


String VersionMetadata::toString(bool one_line) const
{
    return getInfo().toString(one_line);
}

void VersionMetadata::loadAndVerifyMetadata(LoggerPtr logger)
{
    static constexpr size_t ZK_MAX_RETRIES = 10;
    static constexpr Int64 ZK_RETRY_INTERVAL_IN_MS = 100;

    for (size_t attempt = 1; attempt <= ZK_MAX_RETRIES; ++attempt)
    {
        loadMetadata();
        auto updated = adjustMetadata(logger);
        verifyMetadata();

        try
        {
            if (updated)
                storeMetadata(/*force=*/true);
        }
        catch (const Coordination::Exception & e)
        {
            if (e.code == Coordination::Error::ZBADVERSION && attempt < ZK_MAX_RETRIES)
            {
                sleepForMilliseconds(ZK_RETRY_INTERVAL_IN_MS);
                continue;
            }
            throw;
        }

        return;
    }
}

bool VersionMetadata::wasInvolvedInTransaction()
{
    auto removal = getRemovalCSN();
    auto current_removal_tid_hash = getCurrentRemovalTIDHash();
    bool created_by_transaction = !getCreationTID().isPrehistoric();
    bool removed_by_transaction = (removal != Tx::PrehistoricCSN)
        && ((removal != 0) || (current_removal_tid_hash != Tx::PrehistoricTID.getHash() && current_removal_tid_hash != 0));
    return created_by_transaction || removed_by_transaction;
}

bool VersionMetadata::assertHasValidMetadata()
{
    String content;
    try
    {
        auto persisted_info = readStoredMetadata(content);
        auto creation_tid_val = getCreationTID();
        auto removal_tid_val = getRemovalTID();

        if (creation_tid_val != persisted_info.creation_tid)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Invalid version metadata, creation_tid mismatched {} and {}",
                creation_tid_val,
                persisted_info.creation_tid);

        if (removal_tid_val != persisted_info.removal_tid && removal_tid_val != Tx::PrehistoricTID)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Invalid version metadata, removal_tid mismatched {} and  {}",
                removal_tid_val,
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
        writeToBuffer(expected, /*one_line=*/true);
        tryLogCurrentException(
            merge_tree_data_part->storage.log,
            fmt::format("Object {}, metadata content {}\nexpected:\n{}", getObjectName(), content, expected.str()));
        return false;
    }
}

void VersionMetadata::writeToBuffer(WriteBuffer & buf, bool one_line) const
{
    getInfo().writeToBuffer(buf, one_line);
}

void VersionMetadata::setRemovalTIDLock(TIDHash removal_tid_lock_hash)
{
    LOG_DEBUG(merge_tree_data_part->storage.log, "Object {}, setRemovalTIDLock {}", getObjectName(), removal_tid_lock_hash);
    removal_tid_lock = removal_tid_lock_hash;
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
