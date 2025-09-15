#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>

#include <mutex>
#include <optional>
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
#include <fmt/format.h>
#include <Common/Exception.h>
#include <Common/TransactionID.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/logger_useful.h>


namespace DB
{

static constexpr size_t ZK_MAX_RETRIES = 10;
static constexpr Int64 ZK_RETRY_INTERVAL_IN_MS = 100;

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

bool VersionMetadata::isVisible(CSN snapshot_version, TransactionID current_tid)
{
    auto current_info = getInfo();

    LOG_TEST(
        log,
        "Object {}, info {}, checking visible for snapshot_version {}, current_tid {}",
        getObjectName(),
        current_info.toString(true),
        snapshot_version,
        current_tid);

    if (auto visible = current_info.isVisible(snapshot_version, current_tid))
    {
        LOG_TEST(log, "Object {}, visible {}", getObjectName(), *visible);
        return *visible;
    }

    // Data part has creation_tid/removal_tid, but does not have creation_csn/removal_csn

    /// Before doing CSN lookup, let's check some extra conditions.
    /// If snapshot_version <= some_tid.start_csn, then changes of the transaction with some_tid
    /// are definitely not visible for us (because the transaction can be committed with greater CSN only),
    /// so we don't need to check if it was committed.
    if (snapshot_version <= current_info.creation_tid.start_csn)
        return false;

    /// Check if creation_tid/removal_tid transactions are committed and write CSNs
    /// TODO Transactions: we probably need more optimizations here
    /// to avoid some CSN lookups or make the lookups cheaper.
    /// NOTE: Old enough committed parts always have written CSNs,
    /// so we can determine their visibility through fast path.
    /// But for long-running writing transactions we will always do
    /// CSN lookup and get 0 (UnknownCSN) until the transaction is committed/rolled back.

    auto current_creation_csn = current_info.creation_csn;
    if (!current_info.creation_csn)
    {
        current_creation_csn = TransactionLog::getCSN(current_info.creation_tid);
        LOG_DEBUG(log, "Object {}, current_creation_csn {}", getObjectName(), current_creation_csn);
        if (!current_creation_csn)
            return false; /// Part creation is not committed yet
    }

    chassert(current_info.removal_csn == 0, fmt::format("removal_csn {}", current_info.removal_csn));

    auto current_removal_csn = current_info.removal_csn;
    if (!current_info.removal_tid.isEmpty())
        current_removal_csn = TransactionLog::getCSN(current_info.removal_tid);

    LOG_DEBUG(log, "Object {}, current_removal_csn {}", getObjectName(), current_removal_csn);
    return current_creation_csn <= snapshot_version && (!current_removal_csn || snapshot_version < current_removal_csn);
}

void VersionMetadata::setAndStoreRemovalCSN(CSN csn)
{
    LOG_DEBUG(log, "Object {}, setAndStoreRemovalCSN {}", getObjectName(), csn);
    auto update_function = [csn](VersionInfo & info) { info.removal_csn = csn; };
    updateStoreAndSetMetadataWithRefresh(update_function);
}

void VersionMetadata::setAndStoreCreationCSN(CSN csn)
{
    LOG_DEBUG(log, "Object {}, setAndStoreCreationCSN {}", getObjectName(), csn);

    auto update_function = [csn](VersionInfo & info)
    {
        chassert(!info.creation_tid.isEmpty());
        info.creation_csn = csn;
    };
    updateStoreAndSetMetadataWithRefresh(update_function);
}

void VersionMetadata::setAndStoreRemovalTID(const TransactionID & tid)
{
    LOG_DEBUG(log, "Object {}, setAndStoreRemovalTID {}", getObjectName(), tid);

    auto update_function = [tid](VersionInfo & info)
    {
        chassert(info.removal_csn == 0, fmt::format("removal_csn must be 0"));
        info.removal_tid = tid;
        if (tid.isNonTransactional())
            info.removal_csn = Tx::NonTransactionalCSN;
    };
    updateStoreAndSetMetadataWithRefresh(update_function);
}


/// It can be used for introspection purposes only
TransactionID VersionMetadata::getRemovalTIDForLogging() const
{
    auto current_info = getInfo();
    if (current_info.removal_csn)
    {
        /// removal_tid cannot be changed since we have removal_csn, so it's readonly
        return current_info.removal_tid;
    }

    if (current_info.removal_tid.isEmpty() || current_info.removal_tid == Tx::NonTransactionalTID)
        return current_info.removal_tid;

    if (auto txn = TransactionLog::instance().tryGetRunningTransaction(current_info.removal_tid.getHash()))
        return txn->tid;

    return Tx::EmptyTID;
}

void VersionMetadata::lockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context)
{
    LOG_DEBUG(
        log,
        "Object {}, trying to lock removal_tid by {}, table: {}, part: {}",
        getObjectName(),
        tid,
        context.table.getNameForLogs(),
        context.part_name);

    auto current_info = getInfo();

    String part_desc;
    if (current_info.removal_csn != 0)
    {
        if (context.covering_part.empty())
            part_desc = context.part_name;
        else
            part_desc = fmt::format("{} (covered by {})", context.part_name, context.covering_part);
        throw Exception(
            ErrorCodes::SERIALIZATION_ERROR,
            "Transaction {} tried to remove data object {} from {}, but it's removed by another transaction with csn "
            "{}",
            tid,
            part_desc,
            context.table.getNameForLogs(),
            current_info.removal_csn);
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
        "Transaction {} tried to remove data object {} from {}, "
        "but it's locked by another transaction (TID: {}, TIDH: {}) which is currently removing this part.",
        tid,
        part_desc,
        context.table.getNameForLogs(),
        getRemovalTIDForLogging(),
        locked_by);
}

void VersionMetadata::setAndStoreCreationTID(const TransactionID & tid, TransactionInfoContext * context)
{
    LOG_DEBUG(log, "Object {}, setAndStoreCreationTID {}", getObjectName(), tid);
    auto update_function = [tid](VersionInfo & info)
    {
        /// NOTE ReplicatedMergeTreeSink may add one part multiple times
        chassert(info.creation_tid.isEmpty() || info.creation_tid == tid);

        if (tid.isNonTransactional())
            info.creation_csn = Tx::NonTransactionalCSN;
        info.creation_tid = tid;
    };
    updateStoreAndSetMetadataWithRefresh(update_function);

    if (context)
        tryWriteEventToSystemLog(log, TransactionsInfoLogElement::ADD_PART, tid, *context);
}

bool VersionMetadata::canBeRemoved() const
{
    auto current_info = getInfo();
    LOG_TEST(log, "Object {}, canBeRemoved {}", getObjectName(), current_info.toString(/*one_line=*/true));

    if (current_info.removal_tid.isNonTransactional())
        return true;

    /// We can safely remove part if its creation was rolled back
    if (current_info.creation_csn == Tx::RolledBackCSN)
        return true;

    if (current_info.removal_tid.isEmpty())
        return false;

    auto fresh_creation_csn = current_info.creation_csn;
    if (!current_info.creation_csn)
    {
        /// Cannot remove part if its creation not committed yet
        fresh_creation_csn = TransactionLog::getCSN(current_info.creation_tid);
        if (!fresh_creation_csn)
            return false;
    }

    auto oldest_snapshot_version = TransactionLog::instance().getOldestSnapshot();

    /// Part is probably visible for some transactions (part is too new or the oldest snapshot is too old)
    if (oldest_snapshot_version < fresh_creation_csn)
        return false;

    if (current_info.removal_csn && current_info.removal_csn <= oldest_snapshot_version)
        return true;

    auto fresh_removal_csn = current_info.removal_csn;
    if (!current_info.removal_csn && !current_info.removal_tid.isEmpty())
    {
        /// Part removal is not committed yet
        fresh_removal_csn = TransactionLog::getCSN(current_info.removal_tid);
        if (!fresh_removal_csn)
            return false;
    }

    /// We can safely remove part if all running transactions were started after part removal was committed
    return fresh_removal_csn && fresh_removal_csn <= oldest_snapshot_version;
}

VersionInfo VersionMetadata::getInfo() const
{
    std::shared_lock lock(version_info_mutex);
    return version_info;
}

void VersionMetadata::validateAdjustAndStoreMetadata(VersionInfo & new_info)
{
    LOG_DEBUG(log, "Object {}, validateAdjustAndStoreMetadata {}", getObjectName(), new_info.toString(/*one_line=*/true));

    try
    {
        validateInfo(getObjectName(), new_info);
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::SERIALIZATION_ERROR)
            throw;

        if (!adjustInfo(new_info))
            throw;

        LOG_DEBUG(log, "Object {}, validateAdjustAndStoreMetadata adjusted {}", getObjectName(), new_info.toString(/*one_line=*/true));
        validateInfo(getObjectName(), new_info);
    }

    new_info.storing_version = storeInfoImpl(new_info);
}

void VersionMetadata::updateStoreAndSetMetadataWithRefresh(std::function<void(VersionInfo & current_info)> update_info_func)
{
    /// Try to update with in-memory info first
    VersionInfo new_info = getInfo();
    update_info_func(new_info);

    try
    {
        validateAdjustAndStoreMetadata(new_info);
        setInfo(new_info);
        return;
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::SERIALIZATION_ERROR)
            throw;

        LOG_INFO(log, "Object {}, updateStoreMetadataWithRefreshInfo trying with in-memory got error {}", getObjectName(), e.what());
    }

    LOG_DEBUG(log, "Object {}, updateStoreMetadataWithRefreshInfo try with persisted info", getObjectName());

    /// Try to update info from storage
    for (size_t attempt = 1; attempt <= ZK_MAX_RETRIES; ++attempt)
    {
        new_info = loadMetadata();
        update_info_func(new_info);
        adjustInfo(new_info);
        validateInfo(getObjectName(), new_info);
        try
        {
            new_info.storing_version = storeInfoImpl(new_info);
        }
        /// Retry logic for `VersionMetadataOnKeeper` implementations.
        catch (const Coordination::Exception & e)
        {
            if (attempt == ZK_MAX_RETRIES)
                throw;

            if (e.code != Coordination::Error::ZBADVERSION && !Coordination::isHardwareError(e.code))
                throw;

            sleepForMilliseconds(ZK_RETRY_INTERVAL_IN_MS);
            continue;
        }
        catch (const Exception & e)
        {
            if (attempt == ZK_MAX_RETRIES)
                throw;

            if (e.code() != ErrorCodes::SERIALIZATION_ERROR)
                throw;

            sleepForMilliseconds(ZK_RETRY_INTERVAL_IN_MS);
            continue;
        }

        setInfo(new_info);
        return;
    }
}

void VersionMetadata::validateAndSetInfo(const VersionInfo & new_info)
{
    validateInfo(getObjectName(), new_info);
    setInfo(new_info);
}

void VersionMetadata::setInfo(const VersionInfo & new_info)
{
    std::lock_guard lock(version_info_mutex);
    setInfoUnlocked(new_info);
}

void VersionMetadata::setInfoUnlocked(const VersionInfo & new_info)
{
    LOG_DEBUG(log, "Object {}, setInfo {}", getObjectName(), new_info.toString(/*one_line=*/true));

    if (new_info.storing_version < version_info.storing_version)
    {
        LOG_INFO(
            log,
            "Object {}, setInfo {} with lower storing version, current storing version {}",
            getObjectName(),
            new_info.toString(/*one_line=*/true),
            version_info.storing_version);
        return;
    }

    version_info = new_info;
}


String VersionMetadata::getObjectName() const
{
    return merge_tree_data_part->storage.getStorageID().getNameForLogs() + "|" + merge_tree_data_part->name;
}

bool VersionMetadata::adjustInfo(VersionInfo & current_info)
{
    bool info_updated = false;

    chassert(
        !current_info.creation_tid.isEmpty(),
        fmt::format("Object {}, invalid info {}", getObjectName(), current_info.toString(/*one_line=*/true)));

    bool creation_tid_running = false;
    if (!current_info.creation_csn)
    {
        LOG_TRACE(log, "Object {} does not have creation_csn {}", getObjectName(), current_info.creation_tid);

        creation_tid_running = TransactionLog::instance().tryGetRunningTransaction(current_info.creation_tid.getHash()) != nullptr;
        if (creation_tid_running)
        {
            LOG_TRACE(log, "Creation_tid {} is running", current_info.creation_tid);
        }
        else
        {
            LOG_TRACE(log, "Creation_tid {} is not running, trying to get its creation CSN", current_info.creation_tid);
            auto csn_of_creation_tid = TransactionLog::getCSN(current_info.creation_tid);
            if (!csn_of_creation_tid)
            {
                LOG_TRACE(
                    log,
                    "Object {}, unable to find creation_csn of creation_tid {}, resetting it",
                    getObjectName(),
                    current_info.creation_tid);
                current_info.creation_csn = Tx::RolledBackCSN;
                info_updated = true;
            }
            else
            {
                LOG_TRACE(
                    log,
                    "Object {}, found creation_csn {} of creation_tid {}",
                    getObjectName(),
                    csn_of_creation_tid,
                    current_info.creation_tid);
                current_info.creation_csn = csn_of_creation_tid;
                info_updated = true;
            }
        }
    }

    if (!current_info.removal_csn)
    {
        if (!current_info.removal_tid.isEmpty())
        {
            auto tid_running = TransactionLog::instance().tryGetRunningTransaction(current_info.removal_tid.getHash());
            LOG_TRACE(
                log, "Object {} does not have removal_csn, try to get it from removal_tid {}", getObjectName(), current_info.removal_tid);
            auto csn_of_removal_tid = TransactionLog::getCSN(current_info.removal_tid);
            if (csn_of_removal_tid)
            {
                LOG_TRACE(
                    log,
                    "Object {}, found removal_csn {} of removal_tid {}",
                    getObjectName(),
                    csn_of_removal_tid,
                    current_info.removal_tid);

                current_info.removal_csn = csn_of_removal_tid;
                info_updated = true;
            }
            else if (tid_running == NO_TRANSACTION_PTR)
            {
                LOG_TRACE(
                    log,
                    "Object {}, unable to find removal_csn of non-running removal_tid {}, resetting removal_tid",
                    getObjectName(),
                    current_info.removal_tid);

                if (!current_info.removal_tid.isEmpty())
                    current_info.removal_tid = Tx::EmptyTID;
                info_updated = true;
            }
            else
            {
                LOG_TRACE(log, "Object {}, unable to find removal_csn of removal_tid {}", getObjectName(), current_info.removal_tid);
                TIDHash current_removal_tid_lock_hash = getRemovalTIDLockHash();
                TIDHash current_removal_tid_hash = current_info.removal_tid.getHash();

                if (!current_removal_tid_lock_hash)
                {
                    LOG_TRACE(
                        log,
                        "Object {}, no removal_tid_lock, the transaction was not committed, resetting removal_tid",
                        merge_tree_data_part->name);
                }
                else if (current_removal_tid_lock_hash == current_removal_tid_hash)
                {
                    LOG_TRACE(
                        log,
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

    if (info_updated)
        LOG_DEBUG(log, "Object {}, info is updated", getObjectName());

    return info_updated;
}

void VersionMetadata::validateInfo(const String & object_name, const VersionInfo & info)
{
    MergeTreeTransactionPtr creating_txn{nullptr};
    if (!info.creation_tid.isNonTransactional())
        creating_txn = TransactionLog::instance().tryGetRunningTransaction(info.creation_tid.getHash());

    /// Sanity checks
    if (creating_txn != nullptr)
    {
        chassert(
            creating_txn->tid == info.creation_tid,
            fmt::format("creating_txn {} != creation_tid {}", creating_txn->tid, info.creation_tid));

        if (info.creation_csn && ///
            info.creation_csn != Tx::RolledBackCSN && ///
            creating_txn->getCSN() != info.creation_csn ///
        )
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Object {}, creation_tid {} (with csn {}) is running, invalid csn {}",
                object_name,
                info.creation_tid,
                creating_txn->getCSN(),
                info.creation_csn);

        /// Part has not committed yet, other transactions cannot see it.
        /// It is possible that the csn of `creating_txn` has not been updated.
        /// So we throw SERIALIZATION_ERROR.
        if (creating_txn->getCSN() == 0 && ///
            !info.removal_tid.isNonTransactional() && ///
            info.removal_tid != info.creation_tid ///
        )
        {
            if (info.removal_csn)
                throw Exception(
                    ErrorCodes::SERIALIZATION_ERROR,
                    "Object {}, creation_tid {} is running, expect no removal_csn, actual {}",
                    object_name,
                    info.creation_tid,
                    info.removal_csn);

            if (!info.removal_tid.isEmpty())
                throw Exception(
                    ErrorCodes::SERIALIZATION_ERROR,
                    "Object {}, creation_tid {} is running, got invalid removal_tid {}",
                    object_name,
                    info.creation_tid,
                    info.removal_tid);
        }
    }
    else
    {
        /// When creating a new part in a merge and mutation thread, the transaction might be cancelled.
        /// So we throw SERIALIZATION_ERROR.
        if (!info.creation_tid.isNonTransactional() && !info.creation_csn)
            throw Exception(
                ErrorCodes::SERIALIZATION_ERROR,
                "Object {}, creation_tid {} is not running, expect valid creation_csn, actual {}",
                object_name,
                info.creation_tid,
                info.creation_csn);

        if (info.removal_csn && info.removal_csn != Tx::NonTransactionalCSN && info.creation_csn > info.removal_csn)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Object {}, creation_csn {} should not be greater than removal_csn {}",
                object_name,
                info.creation_csn,
                info.removal_csn);

        if (!info.creation_tid.isNonTransactional() && info.creation_tid.start_csn > info.creation_csn)
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
}

void VersionMetadata::readFromBuffer(ReadBuffer & buf, bool one_line)
{
    VersionInfo info;
    info.readFromBuffer(buf, one_line);
    validateAndSetInfo(info);
}

void VersionMetadata::loadAndAdjustMetadata()
{
    for (size_t attempt = 1; attempt <= ZK_MAX_RETRIES; ++attempt)
    {
        auto current_info = loadMetadata();
        auto updated = adjustInfo(current_info);
        validateInfo(getObjectName(), current_info);
        if (updated)
        {
            try
            {
                current_info.storing_version = storeInfoImpl(current_info);
            }
            /// Retry logic for `VersionMetadataOnKeeper` implementations.
            catch (const Coordination::Exception & e)
            {
                if (attempt == ZK_MAX_RETRIES)
                    throw;

                if (e.code != Coordination::Error::ZBADVERSION && !Coordination::isHardwareError(e.code))
                    throw;

                sleepForMilliseconds(ZK_RETRY_INTERVAL_IN_MS);
                continue;
            }
            catch (const Exception & e)
            {
                if (attempt == ZK_MAX_RETRIES)
                    throw;

                if (e.code() != ErrorCodes::SERIALIZATION_ERROR)
                    throw;

                sleepForMilliseconds(ZK_RETRY_INTERVAL_IN_MS);
                continue;
            }
        }

        setInfo(current_info);
        return;
    }
}

bool VersionMetadata::hasValidMetadata()
{
    auto current_info = getInfo();
    VersionInfo persisted_info;
    try
    {
        persisted_info = readMetadata();
        if (current_info.creation_tid != persisted_info.creation_tid)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Invalid version metadata, creation_tid mismatched {} and {}",
                current_info.creation_tid,
                persisted_info.creation_tid);

        if (current_info.removal_tid != persisted_info.removal_tid && current_info.removal_tid != Tx::NonTransactionalTID)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Invalid version metadata, removal_tid mismatched {} and  {}",
                current_info.removal_tid,
                persisted_info.removal_tid);

        if (current_info.creation_csn != persisted_info.creation_csn && current_info.creation_csn == Tx::RolledBackCSN)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Invalid version metadata, creation_csn mismatched {} and  {}",
                current_info.creation_csn,
                persisted_info.creation_csn);

        if (current_info.removal_csn != persisted_info.removal_csn && current_info.removal_csn != Tx::NonTransactionalCSN)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Invalid version metadata, removal_csn mismatched {} and  {}",
                current_info.removal_csn,
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
        tryLogCurrentException(
            merge_tree_data_part->storage.log,
            fmt::format(
                "Object {}, persisted_info: {}, current_info: {}",
                getObjectName(),
                persisted_info.toString(/*one_line*/ true),
                current_info.toString(/*one_line=*/true)));
        return false;
    }
}

void VersionMetadata::writeToBuffer(WriteBuffer & buf, bool one_line) const
{
    getInfo().writeToBuffer(buf, one_line);
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
