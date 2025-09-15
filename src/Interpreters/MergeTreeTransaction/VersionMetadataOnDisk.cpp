#include <optional>
#include <Interpreters/MergeTreeTransaction/VersionInfo.h>
#include <Interpreters/MergeTreeTransaction/VersionMetadataOnDisk.h>

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
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/Exception.h>
#include <Common/TransactionID.h>
#include <Common/logger_useful.h>

static constexpr auto TMP_TXN_VERSION_METADATA_FILE_NAME = "txn_version.txt.tmp";
namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int CANNOT_OPEN_FILE;
extern const int SERIALIZATION_ERROR;
}

namespace MergeTreeSetting
{
extern const MergeTreeSettingsBool fsync_part_directory;
}

VersionMetadataOnDisk::VersionMetadataOnDisk(IMergeTreeDataPart * merge_tree_data_part_)
    : VersionMetadata(merge_tree_data_part_)
    , can_write_metadata(merge_tree_data_part->storage.supportsTransactions() && !merge_tree_data_part->getDataPartStorage().isReadonly())
{
    log = ::getLogger("VersionMetadataOnDisk");
    is_persist_deferrable = !merge_tree_data_part->getDataPartStorage().existsFile(TXN_VERSION_METADATA_FILE_NAME);
    LOG_DEBUG(
        log, "Object {}, can_write_metadata {}, is_persist_deferrable {}", getObjectName(), can_write_metadata, is_persist_deferrable);
}

VersionInfo VersionMetadataOnDisk::loadMetadata()
{
    LOG_DEBUG(log, "Object {}, loading metadata", getObjectName());
    std::optional<VersionInfo> loading_info = std::nullopt;
    bool has_tmp_metadata_file = false;
    auto & data_part_storage = merge_tree_data_part->getDataPartStorage();
    {
        std::lock_guard lock_persisted_metadata{persisted_info_mutex};
        loading_info = readMetadataUnlock();
        has_tmp_metadata_file = data_part_storage.existsFile(TMP_TXN_VERSION_METADATA_FILE_NAME);
        if (!merge_tree_data_part->isStoredOnReadonlyDisk() && has_tmp_metadata_file)
            removeTmpMetadataFile();
    }

    if (loading_info)
    {
        LOG_DEBUG(log, "Object {}, load metadata: {}", getObjectName(), (*loading_info).toString(/*one_line=*/true));
        return *loading_info;
    }

    LOG_DEBUG(log, "Object {}, no metadata", getObjectName());

    /// Four (?) cases are possible:
    /// 1. Part was created without transactions.
    /// 2. Version metadata file was not renamed from *.tmp on part creation.
    /// 3. Version metadata were written to *.tmp file, but hard restart happened before fsync.
    /// 4. Fsyncs in storeMetadata() work incorrectly.

    if (merge_tree_data_part->isStoredOnReadonlyDisk() || !has_tmp_metadata_file)
    {
        /// Case 1.
        /// We do not have version metadata and transactions history for old parts,
        /// so let's consider that such parts were created by some ancient transaction
        /// and were committed with some NonTransactionalCSN.
        /// NOTE It might be Case 3, but version metadata file is written on part creation before other files,
        /// so it's not Case 3 if part is not broken.

        VersionInfo new_info;
        new_info.creation_tid = Tx::NonTransactionalTID;
        new_info.creation_csn = Tx::NonTransactionalCSN;
        return new_info;
    }

    /// Case 2.
    /// Content of *.tmp file may be broken, just use fake TID.
    /// Transaction was not committed if *.tmp file was not renamed, so we should complete rollback by removing part.
    VersionInfo new_info;
    new_info.creation_tid = Tx::DummyTID;
    new_info.creation_csn = Tx::RolledBackCSN;
    return new_info;
}

bool VersionMetadataOnDisk::tryLockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context, TIDHash * locked_by_id)
{
    LOG_DEBUG(
        log,
        "Object {}, tryLockRemovalTID by {}, table: {}, part: {}",
        getObjectName(),
        tid,
        context.table.getNameForLogs(),
        context.part_name);

    auto current_info = getInfo();

    chassert(!tid.isEmpty());
    chassert(!current_info.creation_tid.isEmpty());

    TIDHash removal_lock_value = tid.getHash();
    TIDHash expected_removal_lock_value = 0;

    bool locked = removal_tid_lock_hash.compare_exchange_strong(expected_removal_lock_value, removal_lock_value);
    if (!locked)
    {
        /// Non-transactional TIDs are the same, we need to exclude them from this check.
        if (!tid.isNonTransactional() && expected_removal_lock_value == removal_lock_value)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Tried to lock part {} for removal second time by {}", context.part_name, tid);

        if (locked_by_id)
            *locked_by_id = expected_removal_lock_value;
        return false;
    }

    tryWriteEventToSystemLog(log, TransactionsInfoLogElement::LOCK_PART, tid, context);
    return true;
}

void VersionMetadataOnDisk::unlockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context)
{
    LOG_DEBUG(
        log,
        "Object {}, unlockRemovalTID by {}, table: {}, part: {}",
        getObjectName(),
        tid,
        context.table.getNameForLogs(),
        context.part_name);

    chassert(!tid.isEmpty());
    TIDHash tid_hash = tid.getHash();

    bool unlocked = removal_tid_lock_hash.compare_exchange_strong(tid_hash, 0);
    if (!unlocked)
    {
        // `tid_hash` is now the hash of `removal_tid_lock_hash`
        auto locked_by_txn = TransactionLog::instance().tryGetRunningTransaction(tid_hash);
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot unlock removal_tid, it's a bug. Current: {} {}, actual: {} {}",
            tid.getHash(),
            tid,
            tid_hash,
            locked_by_txn ? locked_by_txn->tid : Tx::EmptyTID);
    }

    tryWriteEventToSystemLog(log, TransactionsInfoLogElement::UNLOCK_PART, tid, context);
}


bool VersionMetadataOnDisk::isRemovalTIDLocked()
{
    return removal_tid_lock_hash.load() != 0;
}

bool VersionMetadataOnDisk::hasPersistedMetadata() const
{
    std::lock_guard lock_persisted_metadata{persisted_info_mutex};
    if (deferred_persist_info)
        return true;

    return merge_tree_data_part->getDataPartStorage().existsFile(TXN_VERSION_METADATA_FILE_NAME);
}

Int32 VersionMetadataOnDisk::storeInfoImplUnlock(VersionInfo new_info)
{
    LOG_DEBUG(log, "Object {}, storeInfoImplUnlock {}", getObjectName(), new_info.toString(/*one_line=*/true));

    bool involved_in_transaction = new_info.wasInvolvedInTransaction();
    if (!can_write_metadata)
    {
        if (involved_in_transaction)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Object was involved in transaction but cannot write metadata");

        return VersionInfo::UNSTORED_VERSION;
    }

    if (!involved_in_transaction && is_persist_deferrable)
    {
        LOG_DEBUG(log, "Object {}, pending store metadata", getObjectName());
        deferred_persist_info = new_info;
        return ++(*deferred_persist_info).storing_version;
    }

    Int32 expected_storing_version = 0;
    if (deferred_persist_info)
    {
        LOG_DEBUG(log, "Object {}, get expected storing version from pending info", getObjectName());
        expected_storing_version = (*deferred_persist_info).storing_version;
    }
    else
    {
        expected_storing_version = getExpectedStoringVersionUnlock();
    }

    if (expected_storing_version != new_info.storing_version)
        throw Exception(
            ErrorCodes::SERIALIZATION_ERROR,
            "Mismatched storing version {}, expected {}",
            new_info.storing_version,
            expected_storing_version);

    // Increase storing version
    ++new_info.storing_version;

    storeInfoToDataPartStorage(merge_tree_data_part->storage, merge_tree_data_part->getDataPartStorage(), new_info);
    LOG_DEBUG(log, "Object {}, stored metadata content:\n{}", getObjectName(), new_info.toString(/*one_line=*/true));

    is_persist_deferrable = false;
    deferred_persist_info = std::nullopt;

    return new_info.storing_version;
}

Int32 VersionMetadataOnDisk::getExpectedStoringVersionUnlock()
{
    auto persisted_info = readMetadataUnlock();
    if (persisted_info)
        return (*persisted_info).storing_version;
    else
        return VersionInfo::UNSTORED_VERSION;
}

std::optional<VersionInfo> VersionMetadataOnDisk::readMetadataUnlock()
{
    if (deferred_persist_info)
    {
        auto ret = *deferred_persist_info;
        ret.storing_version = storeInfoImplUnlock(ret);
        return ret;
    }

    auto & data_part_storage = merge_tree_data_part->getDataPartStorage();

    if (!data_part_storage.existsFile(TXN_VERSION_METADATA_FILE_NAME))
        return std::nullopt;


    size_t small_file_size = 4096;
    auto read_settings = getReadSettings().adjustBufferSize(small_file_size);
    /// Avoid cannot allocated thread error. No need in threadpool read method here.
    read_settings.local_fs_method = LocalFSReadMethod::pread;
    auto buf = data_part_storage.readFile(TXN_VERSION_METADATA_FILE_NAME, read_settings, small_file_size);

    String content;
    readStringUntilEOF(content, *buf);
    LOG_DEBUG(log, "Object {}, read content:\n{}", getObjectName(), content);

    VersionInfo info;
    info.fromString(content, /*one_line=*/false);
    return info;
}

Int32 VersionMetadataOnDisk::storeInfoImpl(const VersionInfo & new_info)
{
    std::lock_guard lock_storing_version{persisted_info_mutex};
    return storeInfoImplUnlock(new_info);
}

VersionInfo VersionMetadataOnDisk::readMetadata()
{
    std::lock_guard lock_persisted_metadata{persisted_info_mutex};
    auto info = readMetadataUnlock();
    if (!info)
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Object {}, cannot read metadata", getObjectName());

    return *info;
}

void VersionMetadataOnDisk::removeTmpMetadataFile()
{
    auto & data_part_storage = merge_tree_data_part->getDataPartStorage();
    if (merge_tree_data_part->isStoredOnReadonlyDisk() || !data_part_storage.existsFile(TMP_TXN_VERSION_METADATA_FILE_NAME))
        return;

    auto last_modified = data_part_storage.getLastModified();
    size_t file_size = data_part_storage.getFileSize(TMP_TXN_VERSION_METADATA_FILE_NAME);
    auto buf = data_part_storage.readFile(TMP_TXN_VERSION_METADATA_FILE_NAME, getReadSettings().adjustBufferSize(file_size), file_size);

    String content;
    readStringUntilEOF(content, *buf);
    LOG_WARNING(
        log,
        "Object {}, found file {} that was last modified on {}, has size {} and the following content: {}",
        getObjectName(),
        TMP_TXN_VERSION_METADATA_FILE_NAME,
        last_modified.epochTime(),
        content.size(),
        content);
    data_part_storage.removeFile(TMP_TXN_VERSION_METADATA_FILE_NAME);
}

void VersionMetadataOnDisk::storeInfoToDataPartStorage(
    const MergeTreeData & storage, IDataPartStorage & data_part_storage, const VersionInfo & new_info)
{
    static constexpr auto filename = TXN_VERSION_METADATA_FILE_NAME;
    static constexpr auto tmp_filename = TMP_TXN_VERSION_METADATA_FILE_NAME;

    try
    {
        {
            /// TODO IDisk interface does not allow to open file with O_EXCL flag (for DiskLocal),
            /// so we create empty file at first (expecting that createFile throws if file already exists)
            /// and then overwrite it.
            data_part_storage.createFile(tmp_filename);
            auto write_settings = storage.getContext()->getWriteSettings();
            auto buf = data_part_storage.writeFile(tmp_filename, 256, write_settings);
            new_info.writeToBuffer(*buf, /*one_line=*/false);
            buf->finalize();
            buf->sync();
        }

        SyncGuardPtr sync_guard;
        if ((*storage.getSettings())[MergeTreeSetting::fsync_part_directory])
            sync_guard = data_part_storage.getDirectorySyncGuard();
        data_part_storage.replaceFile(tmp_filename, filename);
    }
    catch (...)
    {
        try
        {
            data_part_storage.removeFileIfExists(tmp_filename);
        }
        catch (...)
        {
            tryLogCurrentException("DataPartStorageOnDiskFull");
        }
        throw;
    }
}
}
