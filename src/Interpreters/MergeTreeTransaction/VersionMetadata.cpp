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
#include <base/defines.h>
#include <base/scope_guard.h>
#include <fmt/format.h>
#include <Common/Exception.h>
#include <Common/TransactionID.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/logger_useful.h>


namespace DB
{

static constexpr size_t MAX_RETRIES = 20;

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int SERIALIZATION_ERROR;
extern const int STALE_VERSION;
extern const int CORRUPTED_DATA;
extern const int CANNOT_OPEN_FILE;
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

    /// `isVisible` returned std::nullopt, meaning at least one CSN is still unknown.
    chassert(
        current_info.creation_csn == Tx::UnknownCSN || current_info.removal_csn == Tx::UnknownCSN,
        fmt::format("creation_csn {}, removal_csn {}", current_info.creation_csn, current_info.removal_csn));

    auto current_removal_csn = current_info.removal_csn;
    if (!current_info.removal_tid.isEmpty())
        current_removal_csn = TransactionLog::getCSN(current_info.removal_tid);

    LOG_DEBUG(log, "Object {}, current_removal_csn {}", getObjectName(), current_removal_csn);
    return current_creation_csn <= snapshot_version && (!current_removal_csn || snapshot_version < current_removal_csn);
}

void VersionMetadata::setAndStoreRemovalCSN(CSN csn)
{
    LOG_DEBUG(log, "Object {}, setAndStoreRemovalCSN {}", getObjectName(), csn);
    auto update_function = [csn](VersionInfo & info)
    {
        if (info.removal_csn == csn)
            return false;
        chassert(info.removal_csn == 0, fmt::format("removal_csn {}", info.removal_csn));
        info.removal_csn = csn;
        return true;
    };
    updateInfoWithRefreshDataThenStoreAndSetMetadata(update_function);
}

void VersionMetadata::setAndStoreCreationCSN(CSN csn)
{
    LOG_DEBUG(log, "Object {}, setAndStoreCreationCSN {}", getObjectName(), csn);

    auto update_function = [csn](VersionInfo & info)
    {
        if (info.creation_csn == csn)
            return false;
        /// In ReplicatedMergeTree, executeDropRange creates a temporary empty part with NO_TRANSACTION_PTR,
        /// which gives it creation_csn = NonTransactionalCSN. The part is then added to a Transaction
        /// and immediately rolled back to move it to Outdated state. Allow that transition here.
        chassert(
            info.creation_csn == 0 || (csn == Tx::RolledBackCSN && info.creation_csn == Tx::NonTransactionalCSN),
            fmt::format("creation_csn {}, csn {}", info.creation_csn, csn));
        info.creation_csn = csn;
        return true;
    };
    updateInfoWithRefreshDataThenStoreAndSetMetadata(update_function);
}

void VersionMetadata::setAndStoreRemovalTID(const TransactionID & tid)
{
    LOG_DEBUG(log, "Object {}, setAndStoreRemovalTID {}", getObjectName(), tid);

    auto update_function = [tid](VersionInfo & info)
    {
        if (info.removal_tid == tid)
            return false;

        chassert(info.removal_tid.isEmpty() || tid == Tx::EmptyTID, fmt::format("removal_tid {}, tid {}", info.removal_tid, tid));
        info.removal_tid = tid;
        if (tid.isNonTransactional())
            info.removal_csn = Tx::NonTransactionalCSN;
        return true;
    };
    updateInfoWithRefreshDataThenStoreAndSetMetadata(update_function);
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
    TIDHash locked_by = 0;

    auto on_error = [&current_info, &locked_by, &tid, &context]()
    {
        String part_desc;
        if (context.covering_part.empty())
            part_desc = context.part_name;
        else
            part_desc = fmt::format("{} (covered by {})", context.part_name, context.covering_part);

        if (current_info.removal_csn != 0)
            throw Exception(
                ErrorCodes::SERIALIZATION_ERROR,
                "Transaction {} tried to remove data object {} from {}, but it's removed by another transaction with csn "
                "{}",
                tid,
                part_desc,
                context.table.getNameForLogs(),
                current_info.removal_csn);
        else
            throw Exception(
                ErrorCodes::SERIALIZATION_ERROR,
                "Transaction {} tried to remove data object {} from {}, "
                "but it's locked by another transaction (TID: {}, TIDH: {}) which is currently removing this part.",
                tid,
                part_desc,
                context.table.getNameForLogs(),
                current_info.removal_tid,
                locked_by);
    };

    if (current_info.removal_csn != 0)
    {
        on_error();
        UNREACHABLE();
    }

    if (tryLockRemovalTID(tid, context, &locked_by))
        return;

    on_error();
    UNREACHABLE();
}


void VersionMetadata::setAndStoreCreationTID(const TransactionID & tid, TransactionInfoContext * context)
{
    LOG_DEBUG(log, "Object {}, setAndStoreCreationTID {}", getObjectName(), tid);
    auto update_function = [tid](VersionInfo & info)
    {
        /// NOTE ReplicatedMergeTreeSink may add one part multiple times — skip if already set.
        if (info.creation_tid == tid)
            return false;
        chassert(info.creation_tid.isEmpty());

        if (tid.isNonTransactional())
            info.creation_csn = Tx::NonTransactionalCSN;
        info.creation_tid = tid;
        return true;
    };
    updateInfoWithRefreshDataThenStoreAndSetMetadata(update_function);

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
    std::lock_guard lock(version_info_mutex);
    return version_info;
}

void VersionMetadata::updateInfoWithRefreshDataThenStoreAndSetMetadata(std::function<bool(VersionInfo & current_info)> update_info_func)
{
    for (size_t attempt = 1; attempt <= MAX_RETRIES; ++attempt)
    {
        VersionInfo new_info = (attempt == 1) ? getInfo() : loadMetadata();

        if (!update_info_func(new_info))
        {
            /// On subsequent attempts we loaded from storage — sync in-memory state even though
            /// `update_info_func` had nothing to update.
            if (attempt > 1)
                setInfo(new_info);
            return;
        }

        auto update_result = updateCSNIfNeeded(new_info);
        if (!update_result)
        {
            /// Stale metadata: the removal lock changed between the load and the hash check — reload and retry.
            continue;
        }
        validateInfo(getObjectName(), new_info);

        auto store_result = storeInfo(new_info);
        if (store_result)
        {
            new_info.storing_version = *store_result;
            setInfo(new_info);
            return;
        }

        /// `TOO_OLD_VERSION`: the stored version changed since we read it — reload and retry.
    }
    throw Exception(
        ErrorCodes::STALE_VERSION, "Object {}, storing version is still outdated after {} retries", getObjectName(), MAX_RETRIES);
}

void VersionMetadata::validateAndSetInfo(const VersionInfo & new_info)
{
    validateInfo(getObjectName(), new_info);
    setInfo(new_info);
}

void VersionMetadata::setInfo(const VersionInfo & new_info)
{
    std::lock_guard lock(version_info_mutex);

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

std::optional<bool> VersionMetadata::updateCSNIfNeeded(VersionInfo & current_info)
{
    bool info_updated = false;

    chassert(
        !current_info.creation_tid.isEmpty(),
        fmt::format("Object {}, invalid info {}", getObjectName(), current_info.toString(/*one_line=*/true)));

    if (!current_info.creation_csn)
    {
        LOG_TRACE(log, "Object {} has no creation_csn {}", getObjectName(), current_info.creation_tid);
        auto csn_of_creation_tid = tryGetCSN(current_info.creation_tid);

        if (csn_of_creation_tid)
        {
            current_info.creation_csn = csn_of_creation_tid;
            info_updated = true;
        }
    }

    if (!current_info.removal_csn)
    {
        if (!current_info.removal_tid.isEmpty())
        {
            LOG_TRACE(
                log, "Object {} does not have removal_csn, try to get it from removal_tid {}", getObjectName(), current_info.removal_tid);

            auto csn_of_removal_tid = tryGetCSN(current_info.removal_tid);

            if (csn_of_removal_tid == Tx::RolledBackCSN)
            {
                LOG_TRACE(log, "Object {}, tid {} is rolled back, resetting removal_tid", getObjectName(), current_info.removal_tid);
                if (!current_info.removal_tid.isEmpty())
                    current_info.removal_tid = Tx::EmptyTID;
                info_updated = true;
            }
            else if (csn_of_removal_tid)
            {
                current_info.removal_csn = csn_of_removal_tid;
                info_updated = true;
            }
            else
            {
                LOG_TRACE(log, "Object {}, unable to find removal_csn of removal_tid {}", getObjectName(), current_info.removal_tid);
                TIDHash current_removal_tid_lock_hash = getRemovalTIDLockHash();
                TIDHash current_removal_tid_hash = current_info.removal_tid.getHash();

                if (!current_removal_tid_lock_hash)
                {
                    LOG_TRACE(log, "Object {}, no removal_tid_lock, the transaction was not committed", getObjectName());
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
                    /// The lock hash belongs to a different transaction than the one recorded in `current_info`.
                    /// This is a normal race: the removal lock was updated by another transaction between the
                    /// time `current_info` was read and `getRemovalTIDLockHash` was called.
                    /// Signal the caller to reload metadata and retry rather than treating it as a hard error.
                    LOG_TRACE(
                        log,
                        "Object {}, removal_tid_lock hash {} does not match removal_tid hash {} — "
                        "metadata is stale, will reload and retry",
                        getObjectName(),
                        current_removal_tid_lock_hash,
                        current_removal_tid_hash);
                    return std::nullopt;
                }
            }
        }
    }

    if (info_updated)
        LOG_DEBUG(log, "Object {}, info is updated", getObjectName());

    return info_updated;
}

CSN VersionMetadata::tryGetCSN(TransactionID tid)
{
    auto csn = TransactionLog::getCSN(tid);
    if (!csn && TransactionLog::instance().tryGetRunningTransaction(tid.getHash()) == nullptr)
    {
        /// CSN might be updated during the gap between TransactionLog::getCSN and TransactionLog::instance().tryGetRunningTransaction
        csn = TransactionLog::getCSN(tid);

        /// The transaction is not running, and has no CSN -> it is rolled back
        if (!csn)
        {
            LOG_TRACE(log, "Object {}, tid {} is rolled back", getObjectName(), tid);
            return Tx::RolledBackCSN;
        }
    }
    LOG_TRACE(log, "Object {}, tid {}, try get csn {}", getObjectName(), tid, csn);
    return csn;
}

void VersionMetadata::validateInfo(const String & object_name, const VersionInfo & info)
{
    chassert(!info.creation_tid.isEmpty());

    /// A rolled-back part is a transient state produced by `VersionMetadataOnDisk::loadMetadata`
    /// when only a `txn_version.txt.tmp` file exists on disk (i.e. the previous write was
    /// interrupted before the atomic rename). In that case `loadMetadata` returns a
    /// `VersionInfo` with `creation_tid == Tx::DummyTID`, `creation_csn == Tx::RolledBackCSN`,
    /// and default-constructed removal fields (`removal_tid.isEmpty()` and
    /// `removal_csn == Tx::UnknownCSN`). Skip the rest of validation only for this exact
    /// transient shape because:
    ///  - `DummyTID` has `start_csn == NonTransactionalCSN` but `local_tid == DummyLocalTID`,
    ///    which would trip the `assert` inside `TransactionID::isNonTransactional` in debug /
    ///    sanitizer builds and abort the server during startup or `ATTACH`.
    ///  - The part will be marked `Outdated` immediately after loading (see
    ///    `MergeTreeData::loadDataPart`) and subsequently cleaned up, so the invariants that
    ///    `validateInfo` enforces for live parts do not apply here.
    ///
    /// Any other shape with `creation_csn == Tx::RolledBackCSN` (for example a regular
    /// transactional part whose creating transaction was found rolled back by
    /// `updateCSNIfNeeded`) must still go through the full validation below.
    if (info.creation_csn == Tx::RolledBackCSN
        && info.creation_tid == Tx::DummyTID
        && info.removal_tid.isEmpty()
        && info.removal_csn == Tx::UnknownCSN)
        return;

    MergeTreeTransactionPtr creating_txn{nullptr};
    if (!info.creation_tid.isNonTransactional())
        creating_txn = TransactionLog::instance().tryGetRunningTransaction(info.creation_tid.getHash());

    if (creating_txn != nullptr)
    {
        chassert(
            creating_txn->tid == info.creation_tid,
            fmt::format("creating_txn {} != creation_tid {}", creating_txn->tid, info.creation_tid));

        if (info.creation_csn && ///
            info.creation_csn != Tx::RolledBackCSN && ///
            creating_txn->getCSN() != Tx::CommittingCSN && ///
            creating_txn->getCSN() != info.creation_csn ///
        )
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Object {}, creation_tid {} (with csn {}) is running, invalid csn {}",
                object_name,
                info.creation_tid,
                creating_txn->getCSN(),
                info.creation_csn);
    }

    if (!info.creation_csn)
    {
        if (info.removal_csn)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Object {}, creation_csn is not set while removal_csn is set to {}",
                object_name,
                info.removal_csn);

        if (info.creation_tid != info.removal_tid && !info.removal_tid.isEmpty())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Object {}, creation_csn is not set while removal_tid is not {}", object_name, info.removal_tid);
    }
    else
    {
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
    }

    if (info.removal_csn)
    {
        if (info.removal_tid.isEmpty())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Object {}, removal_csn is set {} but removal_tid is empty", object_name, info.removal_csn);

        if (info.removal_tid.start_csn > info.removal_csn)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Object {}, start_csn of removal_tid {} should not be greater than removal_csn {}",
                object_name,
                info.removal_tid,
                info.removal_csn);
    }
}

void VersionMetadata::readFromBuffer(ReadBuffer & buf, bool one_line)
{
    VersionInfo info;
    info.readFromBuffer(buf, one_line);
    validateAndSetInfo(info);
}

void VersionMetadata::loadAndUpdateMetadata()
{
    for (size_t attempt = 1; attempt <= MAX_RETRIES; ++attempt)
    {
        auto current_info = loadMetadata();
        auto update_result = updateCSNIfNeeded(current_info);
        if (!update_result)
        {
            /// Stale metadata: the removal lock changed between the load and the hash check — reload and retry.
            continue;
        }
        validateInfo(getObjectName(), current_info);

        if (*update_result)
        {
            auto store_result = storeInfo(current_info);
            if (!store_result)
            {
                /// `TOO_OLD_VERSION`: the stored version changed since we read it — reload and retry.
                continue;
            }
            current_info.storing_version = *store_result;
        }

        setInfo(current_info);
        return;
    }
    throw Exception(
        ErrorCodes::STALE_VERSION, "Object {}, storing version is still outdated after {} retries", getObjectName(), MAX_RETRIES);
}

bool VersionMetadata::hasValidMetadata()
{
    auto current_info = getInfo();

    /// Rolled-back parts produced by `VersionMetadataOnDisk::loadMetadata` case 2 exist only
    /// in-memory: there is no `txn_version.txt` on disk (the previous write was interrupted
    /// before the atomic rename and `loadMetadata` has since removed the `.tmp` file), so
    /// `readMetadata()` below would throw `CANNOT_OPEN_FILE`. The in-memory state IS the
    /// authoritative state for such parts — they are about to be removed from disk — so
    /// short-circuit with the same shape match used in `validateInfo`.
    if (current_info.creation_csn == Tx::RolledBackCSN
        && current_info.creation_tid == Tx::DummyTID
        && current_info.removal_tid.isEmpty()
        && current_info.removal_csn == Tx::UnknownCSN)
        return true;

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
                "Invalid version metadata, removal_tid mismatched {} and {}",
                current_info.removal_tid,
                persisted_info.removal_tid);

        /// In-memory creation_csn can be learned from TransactionLog after commit while
        /// the on-disk file still carries Tx::UnknownCSN — that is a valid transient state.
        /// Similarly, RolledBackCSN is only ever written to the in-memory state, never to disk.
        if (current_info.creation_csn != persisted_info.creation_csn
            && current_info.creation_csn != Tx::RolledBackCSN
            && persisted_info.creation_csn != Tx::UnknownCSN)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Invalid version metadata, creation_csn mismatched {} and {}",
                current_info.creation_csn,
                persisted_info.creation_csn);

        /// Same reasoning as creation_csn above: in-memory removal_csn can be learned from
        /// TransactionLog before the metadata file is rewritten with the final CSN.
        /// NonTransactionalCSN is set in-memory immediately but may not yet be on disk.
        if (current_info.removal_csn != persisted_info.removal_csn
            && current_info.removal_csn != Tx::NonTransactionalCSN
            && persisted_info.removal_csn != Tx::UnknownCSN)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Invalid version metadata, removal_csn mismatched {} and {}",
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
    catch (const Exception & e)
    {
        /// The part directory may have been removed externally (e.g., between
        /// DETACH and ATTACH in an Ordinary database). If the directory is gone,
        /// there is nothing to validate.
        /// Refer: https://github.com/ClickHouse/ClickHouse/pull/99399
        if (e.code() == ErrorCodes::CANNOT_OPEN_FILE && !merge_tree_data_part->getDataPartStorage().exists())
            return true;

        throw;
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
