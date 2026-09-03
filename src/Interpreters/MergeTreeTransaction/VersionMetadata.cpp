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
#include <Interpreters/TransactionManager.h>
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
extern const int NO_SUCH_DATA_PART;
}

VersionMetadata::VersionMetadata(String part_name_, const IStorage * storage_)
    : part_name(std::move(part_name_))
    , storage(storage_)
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
    const bool visible = isVisible(current_info, snapshot_version, current_tid);
    LOG_TEST(log, "Object {}, visible {}", getObjectName(), visible);
    return visible;
}

bool VersionMetadata::isVisible(const VersionInfo & current_info, CSN snapshot_version, TransactionID current_tid)
{
    if (auto visible = current_info.isVisible(snapshot_version, current_tid))
        return *visible;

    /// `isVisible` returned nullopt: the part has creation_tid/removal_tid but at least one CSN is
    /// still unknown, so fall back to a CSN lookup below.
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
        current_creation_csn = TransactionManager::getCSN(current_info.creation_tid);
        if (!current_creation_csn)
            return false; /// Part creation is not committed yet
    }

    /// `isVisible` returned std::nullopt, meaning at least one CSN is still unknown.
    chassert(
        current_info.creation_csn == Tx::UnknownCSN || current_info.removal_csn == Tx::UnknownCSN,
        fmt::format("creation_csn {}, removal_csn {}", current_info.creation_csn, current_info.removal_csn));

    auto current_removal_csn = current_info.removal_csn;
    if (!current_info.removal_tid.isEmpty())
        current_removal_csn = TransactionManager::getCSN(current_info.removal_tid);

    const bool result = current_creation_csn <= snapshot_version && (!current_removal_csn || snapshot_version < current_removal_csn);
    /// Log the CSNs resolved via `getCSN` that decided visibility. This overload is static, so it
    /// uses a standalone logger; correlate to the object name via the line logged by the non-static
    /// `isVisible` right before (same thread, TID, snapshot).
    if (!current_info.creation_tid.isNonTransactional())
        LOG_DEBUG(
            ::getLogger("VersionMetadata"),
            "Resolved creation_csn {}, removal_csn {} (creation_tid {}) for snapshot {} (TID {}), visible {}",
            current_creation_csn,
            current_removal_csn,
            current_info.creation_tid,
            snapshot_version,
            current_tid,
            result);
    return result;
}

bool isCreationCommitted(const VersionInfo & version_info)
{
    if (version_info.creation_tid.isNonTransactional())
        return true;
    if (version_info.creation_csn == Tx::RolledBackCSN)
        return false;
    if (Tx::isCommittedCSN(version_info.creation_csn))   /// stamped — no getCSN round-trip needed
        return true;
    return Tx::isCommittedCSN(TransactionManager::getCSN(version_info.creation_tid));
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

void VersionMetadata::setAndStoreRemovalTID(const TransactionID & tid, const LockFingerprint & lock_fingerprint)
{
    LOG_TEST(log, "Object {}, setAndStoreRemovalTID {}", getObjectName(), tid);

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

    if (lock_fingerprint.hasFingerprint())
        updateRemovalInfoUnderLockThenStore(update_function, lock_fingerprint);
    else
        updateInfoWithRefreshDataThenStoreAndSetMetadata(update_function);
}

bool VersionMetadata::resetRemovalTID(const LockFingerprint & lock_fingerprint)
{
    LOG_TEST(log, "Object {}, resetRemovalTID", getObjectName());

    /// Only reached from rollback, so a source this transaction also created is rolled back with it.
    auto update_function = [](VersionInfo & info) { return info.resetRemovalForOwnRollback(); };

    if (lock_fingerprint.hasFingerprint())
        return updateRemovalInfoUnderLockThenStore(update_function, lock_fingerprint);
    return updateInfoWithRefreshDataThenStoreAndSetMetadata(update_function);
}

bool VersionMetadata::updateRemovalInfoUnderLockThenStore(
    std::function<bool(VersionInfo & current_info)> update_info_func, const LockFingerprint &)
{
    /// No lock backend (disk-backed parts): the fingerprint is meaningless, persist unguarded.
    return updateInfoWithRefreshDataThenStoreAndSetMetadata(std::move(update_info_func));
}

bool VersionMetadata::lockRemovalTID(const TransactionID & tid, LockKind kind, const TransactionInfoContext & context, LockFingerprint * acquired)
{
    LOG_TEST(
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

    /// `tryLockRemovalTID` raises `NO_SUCH_DATA_PART` (Keeper path) when the part znode is gone
    /// (a peer's merge / TRUNCATE / DROP already removed it). Translate that into a `false`
    /// return so callers skip removal without a try/catch.
    try
    {
        if (tryLockRemovalTID(tid, kind, context, &locked_by, acquired))
            return true;
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::NO_SUCH_DATA_PART)
            return false;
        throw;
    }

    on_error();
    UNREACHABLE();
}


void VersionMetadata::setAndStoreCreationTID(const TransactionID & tid, TransactionInfoContext * context)
{
    LOG_TEST(log, "Object {}, setAndStoreCreationTID {}", getObjectName(), tid);
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
        /// Keep the part unless creation is committed. `getCSN`, not `tryGetCSN`: inferring rollback
        /// from a missing CSN is safe only with `csn_log_retention`, which this branch lacks.
        fresh_creation_csn = TransactionManager::getCSN(current_info.creation_tid);
        if (!fresh_creation_csn)
            return false;
    }

    auto oldest_snapshot_version = TransactionManager::instance().getOldestSnapshot();

    /// Part is probably visible for some transactions (part is too new or the oldest snapshot is too old)
    if (oldest_snapshot_version < fresh_creation_csn)
        return false;

    if (current_info.removal_csn && current_info.removal_csn <= oldest_snapshot_version)
        return true;

    auto fresh_removal_csn = current_info.removal_csn;
    if (!current_info.removal_csn && !current_info.removal_tid.isEmpty())
    {
        /// Removal not committed yet. A `RolledBackCSN` result here means the part is
        /// still alive — the `<=` check at the return falls through to false, so no
        /// explicit branch.
        fresh_removal_csn = TransactionManager::getCSN(current_info.removal_tid);
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

bool VersionMetadata::updateInfoWithRefreshDataThenStoreAndSetMetadata(std::function<bool(VersionInfo & current_info)> update_info_func)
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
            return false;
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
            return true;
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

    LOG_TEST(log, "Object {}, setInfo {}", getObjectName(), new_info.toString(/*one_line=*/true));

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
    return storage->getStorageID().getNameForLogs() + "|" + part_name;
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

CSN VersionMetadata::tryGetCSN(TransactionID tid) const
{
    auto csn = TransactionManager::getCSN(tid);
    if (csn)
    {
        LOG_TRACE(log, "Object {}, tid {}, try get csn {}", getObjectName(), tid, csn);
        return csn;
    }

    /// No CSN yet. Treat as rolled back only once `isTIDInvalid` says the writer session is dead
    /// (single-replica: the TID is no longer in the running list — see the NOTE on `isTIDInvalid`).
    if (!TransactionManager::instance().isTIDInvalid(tid, Tx::MainJobId))
    {
        LOG_TRACE(log, "Object {}, tid {} not invalid yet, CSN unknown", getObjectName(), tid);
        return Tx::UnknownCSN;
    }

    /// Re-check CSN in case the transaction committed concurrently just before isTIDInvalid.
    csn = TransactionManager::getCSN(tid);
    if (!csn)
    {
        LOG_TRACE(log, "Object {}, tid {} is rolled back", getObjectName(), tid);
        return Tx::RolledBackCSN;
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
        creating_txn = TransactionManager::instance().tryGetRunningTransaction(info.creation_tid.getHash());

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
    try
    {
        return validateAgainstPersistedMetadata(current_info);
    }
    catch (const Exception &)
    {
        /// Let `DB::Exception` (e.g. `CORRUPTED_DATA` from the in-memory vs persisted
        /// comparison) propagate. The part destructor's outer try/catch logs and swallows it;
        /// returning `false` here would instead fire `chassert(assertHasValidVersionMetadata())`
        /// in `removeIfNeeded`. `VersionMetadataOnDisk` overrides to catch `CANNOT_OPEN_FILE` first.
        throw;
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Object {}, current_info: {}", getObjectName(), current_info.toString(/*one_line=*/true)));
        return false;
    }
}

bool VersionMetadata::validateAgainstPersistedMetadata(const VersionInfo & current_info)
{
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

    VersionInfo persisted_info = readMetadata();

    if (current_info.creation_tid != persisted_info.creation_tid)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Invalid version metadata, creation_tid mismatched {} and {}",
            current_info.creation_tid,
            persisted_info.creation_tid);

    if (current_info.removal_tid != persisted_info.removal_tid && !current_info.removal_tid.isNonTransactional())
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Invalid version metadata, removal_tid mismatched {} and {}",
            current_info.removal_tid,
            persisted_info.removal_tid);

    /// In-memory creation_csn can be learned from TransactionManager after commit while
    /// the on-disk file still carries Tx::UnknownCSN — that is a valid transient state.
    /// Similarly, RolledBackCSN is only ever written to the in-memory state, never to disk.
    if (current_info.creation_csn != persisted_info.creation_csn && current_info.creation_csn != Tx::RolledBackCSN
        && persisted_info.creation_csn != Tx::UnknownCSN)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Invalid version metadata, creation_csn mismatched {} and {}",
            current_info.creation_csn,
            persisted_info.creation_csn);

    /// Same reasoning as creation_csn above: in-memory removal_csn can be learned from
    /// TransactionManager before the metadata file is rewritten with the final CSN.
    /// NonTransactionalCSN is set in-memory immediately but may not yet be on disk.
    if (current_info.removal_csn != persisted_info.removal_csn && current_info.removal_csn != Tx::NonTransactionalCSN
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
    types.push_back(std::make_shared<DataTypeInt64>());
    return std::make_shared<DataTypeTuple>(std::move(types));
}
}
