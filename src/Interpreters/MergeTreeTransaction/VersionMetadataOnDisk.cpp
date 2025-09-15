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
#include <Common/logger_useful.h>

static constexpr auto TMP_TXN_VERSION_METADATA_FILE_NAME = "txn_version.txt.tmp";
namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int CORRUPTED_DATA;
}

namespace MergeTreeSetting
{
extern const MergeTreeSettingsBool fsync_part_directory;
}

static std::unique_ptr<ReadBufferFromFileBase> openForReading(const IDataPartStorage & part_storage, const String & filename)
{
    size_t file_size = part_storage.getFileSize(filename);
    return part_storage.readFile(filename, getReadSettings().adjustBufferSize(file_size), file_size);
}

VersionMetadataOnDisk::VersionMetadataOnDisk(IMergeTreeDataPart * merge_tree_data_part_)
    : VersionMetadata(merge_tree_data_part_)
{
    log = ::getLogger("VersionMetadataOnDisk");
}

void VersionMetadataOnDisk::loadMetadata()
try
{
    auto & data_part_storage = const_cast<IDataPartStorage &>(merge_tree_data_part->getDataPartStorage());
    const auto & storage = merge_tree_data_part->storage;

    auto remove_tmp_file = [&]()
    {
        auto last_modified = data_part_storage.getLastModified();
        auto buf = openForReading(data_part_storage, TMP_TXN_VERSION_METADATA_FILE_NAME);

        String content;
        readStringUntilEOF(content, *buf);
        LOG_WARNING(
            storage.log,
            "Found file {} that was last modified on {}, has size {} and the following content: {}",
            TMP_TXN_VERSION_METADATA_FILE_NAME,
            last_modified.epochTime(),
            content.size(),
            content);
        data_part_storage.removeFile(TMP_TXN_VERSION_METADATA_FILE_NAME);
    };

    if (data_part_storage.existsFile(TXN_VERSION_METADATA_FILE_NAME))
    {
        auto buf = openForReading(data_part_storage, TXN_VERSION_METADATA_FILE_NAME);
        readFromBuffer(*buf);

        if (!merge_tree_data_part->isStoredOnReadonlyDisk() && data_part_storage.existsFile(TMP_TXN_VERSION_METADATA_FILE_NAME))
            remove_tmp_file();
        return;
    }

    /// Four (?) cases are possible:
    /// 1. Part was created without transactions.
    /// 2. Version metadata file was not renamed from *.tmp on part creation.
    /// 3. Version metadata were written to *.tmp file, but hard restart happened before fsync.
    /// 4. Fsyncs in storeMetadata() work incorrectly.

    if (merge_tree_data_part->isStoredOnReadonlyDisk() || !data_part_storage.existsFile(TMP_TXN_VERSION_METADATA_FILE_NAME))
    {
        /// Case 1.
        /// We do not have version metadata and transactions history for old parts,
        /// so let's consider that such parts were created by some ancient transaction
        /// and were committed with some prehistoric CSN.
        /// NOTE It might be Case 3, but version metadata file is written on part creation before other files,
        /// so it's not Case 3 if part is not broken.
        setCreationTID(Tx::PrehistoricTID, nullptr);
        setCreationCSN(Tx::PrehistoricCSN);
        return;
    }

    /// Case 2.
    /// Content of *.tmp file may be broken, just use fake TID.
    /// Transaction was not committed if *.tmp file was not renamed, so we should complete rollback by removing part.
    setCreationTID(Tx::DummyTID, nullptr);
    setCreationCSN(Tx::RolledBackCSN);

    if (!merge_tree_data_part->isStoredOnReadonlyDisk())
        remove_tmp_file();
}
catch (Exception & e)
{
    e.addMessage(
        "While loading version metadata from table {} part {}",
        merge_tree_data_part->storage.getStorageID().getNameForLogs(),
        merge_tree_data_part->name);
    throw;
}

void VersionMetadataOnDisk::storeMetadata(bool force)
{
    if (!merge_tree_data_part->wasInvolvedInTransaction() && !force)
        return;
    const auto & storage = merge_tree_data_part->storage;
    if (!storage.supportsTransactions())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Storage does not support transaction. It is a bug");

    LOG_TEST(
        storage.log,
        "Writing version for {} (creation: {}, removal {}, creation csn {})",
        merge_tree_data_part->name,
        creation_tid,
        removal_tid,
        creation_csn.load());

    static constexpr auto filename = TXN_VERSION_METADATA_FILE_NAME;
    static constexpr auto tmp_filename = "txn_version.txt.tmp";
    auto & data_part_storage = const_cast<IDataPartStorage &>(merge_tree_data_part->getDataPartStorage());

    try
    {
        {
            /// TODO IDisk interface does not allow to open file with O_EXCL flag (for DiskLocal),
            /// so we create empty file at first (expecting that createFile throws if file already exists)
            /// and then overwrite it.
            data_part_storage.createFile(tmp_filename);
            auto write_settings = storage.getContext()->getWriteSettings();
            auto buf = data_part_storage.writeFile(tmp_filename, 256, write_settings);
            writeToBuffer(*buf);
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


bool VersionMetadataOnDisk::assertHasValidMetadata() const
{
    String content;
    auto current_removal_tid_lock = getRemovalTIDLock();
    try
    {
        size_t small_file_size = 4096;
        auto read_settings = getReadSettings().adjustBufferSize(small_file_size);
        /// Avoid cannot allocated thread error. No need in threadpool read method here.
        read_settings.local_fs_method = LocalFSReadMethod::pread;
        auto buf
            = merge_tree_data_part->getDataPartStorage().readFileIfExists(TXN_VERSION_METADATA_FILE_NAME, read_settings, small_file_size);
        if (!buf)
            return false;
        readStringUntilEOF(content, *buf);
        ReadBufferFromString str_buf{content};
        auto persisted_info = readFromBufferHelper(str_buf);
        bool valid_creation_tid = creation_tid == persisted_info.creation_tid;
        bool valid_removal_tid = removal_tid == persisted_info.removal_tid || removal_tid == Tx::PrehistoricTID;
        bool valid_creation_csn = creation_csn == persisted_info.creation_csn || creation_csn == Tx::RolledBackCSN;
        bool valid_removal_csn = removal_csn == persisted_info.removal_csn || removal_csn == Tx::PrehistoricCSN;

        bool valid_removal_tid_lock
            = (removal_tid.isEmpty() && current_removal_tid_lock == 0) || (current_removal_tid_lock == removal_tid.getHash());

        if (!valid_creation_tid || !valid_removal_tid || !valid_creation_csn || !valid_removal_csn || !valid_removal_tid_lock)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid version metadata file");
        return true;
    }
    catch (...)
    {
        WriteBufferFromOwnString expected;
        writeToBuffer(expected);
        tryLogCurrentException(
            merge_tree_data_part->storage.log,
            fmt::format(
                "File {} contains:\n{}\nexpected:\n{}\nlock: {}\nname: {}",
                TXN_VERSION_METADATA_FILE_NAME,
                content,
                expected.str(),
                current_removal_tid_lock,
                merge_tree_data_part->name));
        return false;
    }
}


bool VersionMetadataOnDisk::tryLockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context, TIDHash * locked_by_id)
{
    chassert(!tid.isEmpty());
    chassert(!creation_tid.isEmpty());
    TIDHash removal_lock_value = tid.getHash();
    TIDHash expected_removal_lock_value = 0;
    bool locked = removal_tid_lock.compare_exchange_strong(expected_removal_lock_value, removal_lock_value);
    if (!locked)
    {
        if (expected_removal_lock_value == removal_lock_value)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Tried to lock part {} for removal second time by {}", context.part_name, tid);

        if (locked_by_id)
            *locked_by_id = expected_removal_lock_value;
        return false;
    }

    removal_tid = tid;
    tryWriteEventToSystemLog(log, TransactionsInfoLogElement::LOCK_PART, tid, context);
    return true;
}


void VersionMetadataOnDisk::unlockRemovalTID(const TransactionID & tid, const TransactionInfoContext & context)
{
    LOG_TEST(log, "Unlocking removal_tid by {}, table: {}, part: {}", tid, context.table.getNameForLogs(), context.part_name);
    chassert(!tid.isEmpty());
    TIDHash removal_lock_value = tid.getHash();
    TIDHash locked_by = removal_tid_lock.load();

    auto throw_cannot_unlock = [&]()
    {
        auto locked_by_txn = TransactionLog::instance().tryGetRunningTransaction(locked_by);
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot unlock removal_tid, it's a bug. Current: {} {}, actual: {} {}",
            removal_lock_value,
            tid,
            locked_by,
            locked_by_txn ? locked_by_txn->tid : Tx::EmptyTID);
    };

    if (locked_by != removal_lock_value)
        throw_cannot_unlock();

    removal_tid = Tx::EmptyTID;
    bool unlocked = removal_tid_lock.compare_exchange_strong(locked_by, 0);
    if (!unlocked)
        throw_cannot_unlock();

    tryWriteEventToSystemLog(log, TransactionsInfoLogElement::UNLOCK_PART, tid, context);
}


bool VersionMetadataOnDisk::isRemovalTIDLocked() const
{
    return removal_tid_lock.load() != 0;
}


bool VersionMetadataOnDisk::hasStoredMetadata() const
{
    return merge_tree_data_part->getDataPartStorage().existsFile(TXN_VERSION_METADATA_FILE_NAME);
}

void VersionMetadataOnDisk::setRemovalTIDLock(TIDHash removal_tid_hash)
{
    removal_tid_lock = removal_tid_hash;
}

void VersionMetadataOnDisk::appendCreationCSNToStoredMetadataImpl()
{
    auto out = merge_tree_data_part->getDataPartStorage().writeTransactionFile(TXN_VERSION_METADATA_FILE_NAME, WriteMode::Append);
    writeCreationCSNToBuffer(*out);
    out->finalize();
}

void VersionMetadataOnDisk::appendRemovalCSNToStoredMetadataImpl()
{
    auto out = merge_tree_data_part->getDataPartStorage().writeTransactionFile(TXN_VERSION_METADATA_FILE_NAME, WriteMode::Append);
    writeRemovalCSNToBuffer(*out);
    out->finalize();
}

void VersionMetadataOnDisk::appendRemovalTIDToStoredMetadataImpl(bool clear)
{
    LOG_TEST(
        merge_tree_data_part->storage.log,
        "{} removal TID for {} (creation: {}, removal {})",
        (clear ? "Clearing" : "Appending"),
        merge_tree_data_part->name,
        creation_tid,
        removal_tid);

    auto out = merge_tree_data_part->getDataPartStorage().writeTransactionFile(TXN_VERSION_METADATA_FILE_NAME, WriteMode::Append);
    writeRemovalTIDToBuffer(*out, clear);
    out->finalize();

    /// fsync is not required when we clearing removal TID, because after hard restart we will fix metadata
    if (!clear)
        out->sync();
}
}
