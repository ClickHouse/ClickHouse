#include <functional>
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
#include <Common/TransactionID.h>
#include <Common/logger_useful.h>

static constexpr auto TMP_TXN_VERSION_METADATA_FILE_NAME = "txn_version.txt.tmp";
namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int CANNOT_OPEN_FILE;
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

VersionMetadataOnDisk::VersionMetadataOnDisk(IMergeTreeDataPart * merge_tree_data_part_, bool support_writing_with_append_)
    : VersionMetadata(merge_tree_data_part_)
    , support_writing_with_append(support_writing_with_append_)
{
    log = ::getLogger("VersionMetadataOnDisk");
    LOG_DEBUG(log, "Object {}, support writing with append {}", getObjectName(), support_writing_with_append);
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
        {
            auto buf = openForReading(data_part_storage, TXN_VERSION_METADATA_FILE_NAME);
            String content;
            readStringUntilEOF(content, *buf);
            LOG_TEST(log, "Object {}, load metadata content\n{}", getObjectName(), content);
        }
        auto buf = openForReading(data_part_storage, TXN_VERSION_METADATA_FILE_NAME);
        readFromBuffer(*buf);

        if (!merge_tree_data_part->isStoredOnReadonlyDisk() && data_part_storage.existsFile(TMP_TXN_VERSION_METADATA_FILE_NAME))
            remove_tmp_file();
        return;
    }

    LOG_TEST(log, "Object {}, no metadata", getObjectName());

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
    e.addMessage("While loading version metadata from object ", getObjectName());
    throw;
}

void VersionMetadataOnDisk::storeMetadata(bool)
{
    const auto & storage = merge_tree_data_part->storage;
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

        LOG_TEST(log, "Object {}, store metadata content: {}", getObjectName(), toString());
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
    LOG_TEST(
        merge_tree_data_part->storage.log,
        "Object {}, setRemovalTIDLock removal_tid_hash {}",
        getObjectName(),
        getCreationCSN());
    removal_tid_lock = removal_tid_hash;
}

void appendToMetadataHelper(
    const String & metadata_file,
    IDataPartStorage & storage,
    bool support_writing_with_append,
    std::function<void(WriteBuffer & buf)> write_func,
    bool sync)
{
    if (support_writing_with_append)
    {
        auto out = storage.writeTransactionFile(metadata_file, WriteMode::Append);
        write_func(*out);
        out->finalize();
        if (sync)
            out->sync();
        return;
    }

    String content;
    {
        auto read_buf = openForReading(storage, metadata_file);
        readStringUntilEOF(content, *read_buf);
    }

    WriteBufferFromString write_buf(content, AppendModeTag{});
    write_func(write_buf);
    write_buf.finalize();

    auto out = storage.writeTransactionFile(metadata_file, WriteMode::Rewrite);
    out->write(content.data(), content.size());
    out->finalize();
    if (sync)
        out->sync();
}

void VersionMetadataOnDisk::appendCreationCSNToStoredMetadataImpl()
{
    LOG_TEST(
        merge_tree_data_part->storage.log,
        "Object {}, appending creation_csn {}",
        getObjectName(),
        getCreationCSN());

    if (!merge_tree_data_part->getDataPartStorage().existsFile(TXN_VERSION_METADATA_FILE_NAME))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Append creation CSN to non-existing metadata");

    auto write_func = [this](WriteBuffer & buf) { writeCreationCSNToBuffer(buf); };
    appendToMetadataHelper(
        TXN_VERSION_METADATA_FILE_NAME, merge_tree_data_part->getDataPartStorage(), support_writing_with_append, write_func, false);
}

void VersionMetadataOnDisk::appendRemovalCSNToStoredMetadataImpl()
{
    LOG_TEST(
        merge_tree_data_part->storage.log,
        "Object {}, appending removal_csn {}",
        getObjectName(),
        getRemovalCSN());

    if (!merge_tree_data_part->getDataPartStorage().existsFile(TXN_VERSION_METADATA_FILE_NAME))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Append removal CSN to non-existing metadata");

    auto write_func = [this](WriteBuffer & buf) { writeRemovalCSNToBuffer(buf); };
    appendToMetadataHelper(
        TXN_VERSION_METADATA_FILE_NAME, merge_tree_data_part->getDataPartStorage(), support_writing_with_append, write_func, false);
}

void VersionMetadataOnDisk::appendRemovalTIDToStoredMetadataImpl(const TransactionID & tid)
{
    LOG_TEST(
        merge_tree_data_part->storage.log,
        "Appending removal TID for {} (creation: {}, removal {})",
        merge_tree_data_part->name,
        creation_tid,
        tid);

    if (!merge_tree_data_part->getDataPartStorage().existsFile(TXN_VERSION_METADATA_FILE_NAME))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Append removal TID to non-existing metadata");


    auto write_func = [this, tid](WriteBuffer & buf) { writeRemovalTIDToBuffer(buf, tid); };
    /// fsync is not required when we clearing removal TID, because after hard restart we will fix metadata
    auto sync = tid != Tx::EmptyTID;
    appendToMetadataHelper(
        TXN_VERSION_METADATA_FILE_NAME, merge_tree_data_part->getDataPartStorage(), support_writing_with_append, write_func, sync);
}


VersionMetadata::Info VersionMetadataOnDisk::readStoredMetadata(String & content) const
{
    size_t small_file_size = 4096;
    auto read_settings = getReadSettings().adjustBufferSize(small_file_size);
    /// Avoid cannot allocated thread error. No need in threadpool read method here.
    read_settings.local_fs_method = LocalFSReadMethod::pread;
    auto buf = merge_tree_data_part->getDataPartStorage().readFileIfExists(TXN_VERSION_METADATA_FILE_NAME, read_settings, small_file_size);
    if (!buf)
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Unable to open file {}", TXN_VERSION_METADATA_FILE_NAME);

    readStringUntilEOF(content, *buf);
    ReadBufferFromString str_buf{content};
    return readFromBufferHelper(str_buf);
}
}
