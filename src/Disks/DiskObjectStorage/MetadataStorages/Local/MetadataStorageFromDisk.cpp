#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Local/MetadataStorageFromDisk.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Local/MetadataStorageFromDiskTransactionOperations.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Common/logger_useful.h>

#include <memory>
#include <shared_mutex>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MetadataStorageFromDisk::MetadataStorageFromDisk(DiskPtr disk_, std::string compatible_key_prefix_, ObjectStorageKeyGeneratorPtr key_generator_, bool persist_removal_queue_, size_t removal_log_compaction_threshold_)
    : disk(disk_)
    , compatible_key_prefix(std::move(compatible_key_prefix_))
    , key_generator(std::move(key_generator_))
    , persist_removal_queue(persist_removal_queue_)
    , removal_log_compaction_threshold(removal_log_compaction_threshold_)
    , log(getLogger("MetadataStorageFromDisk"))
{
}

const std::string & MetadataStorageFromDisk::getPath() const
{
    return disk->getPath();
}

bool MetadataStorageFromDisk::existsFile(const std::string & path) const
{
    return disk->existsFile(path);
}

bool MetadataStorageFromDisk::existsDirectory(const std::string & path) const
{
    return disk->existsDirectory(path);
}

bool MetadataStorageFromDisk::existsFileOrDirectory(const std::string & path) const
{
    return disk->existsFileOrDirectory(path);
}

Poco::Timestamp MetadataStorageFromDisk::getLastModified(const std::string & path) const
{
    return disk->getLastModified(path);
}

time_t MetadataStorageFromDisk::getLastChanged(const std::string & path) const
{
    return disk->getLastChanged(path);
}

uint64_t MetadataStorageFromDisk::getFileSize(const String & path) const
{
    return getTotalSize(readMetadata(path)->objects);
}

std::vector<std::string> MetadataStorageFromDisk::listDirectory(const std::string & path) const
{
    std::vector<std::string> result_files;
    disk->listFiles(path, result_files);
    return result_files;
}

DirectoryIteratorPtr MetadataStorageFromDisk::iterateDirectory(const std::string & path) const
{
    return disk->iterateDirectory(path);
}

std::string MetadataStorageFromDisk::readFileToString(const std::string & path) const
{
    auto buf = disk->readFile(path, ReadSettings{});
    std::string result;
    readStringUntilEOF(result, *buf);
    return result;
}

std::string MetadataStorageFromDisk::readInlineDataToString(const std::string & path) const
{
    return readMetadata(path)->inline_data;
}

DiskObjectStorageMetadataPtr MetadataStorageFromDisk::readMetadataUnlocked(const std::string & path, std::shared_lock<SharedMutex> &) const
{
    auto metadata = std::make_unique<DiskObjectStorageMetadata>(compatible_key_prefix, path);
    auto str = readFileToString(path);
    metadata->deserializeFromString(str);
    return metadata;
}

DiskObjectStorageMetadataPtr MetadataStorageFromDisk::readMetadataUnlocked(const std::string & path, std::unique_lock<SharedMutex> &) const
{
    auto metadata = std::make_unique<DiskObjectStorageMetadata>(compatible_key_prefix, path);
    auto str = readFileToString(path);
    metadata->deserializeFromString(str);
    return metadata;
}

DiskObjectStorageMetadataPtr MetadataStorageFromDisk::readMetadata(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    return readMetadataUnlocked(path, lock);
}

std::unordered_map<String, String> MetadataStorageFromDisk::getSerializedMetadata(const std::vector<String> & file_paths) const
{
    std::shared_lock lock(metadata_mutex);
    std::unordered_map<String, String> metadatas;

    for (const auto & path : file_paths)
    {
        auto metadata = readMetadataUnlocked(path, lock);
        metadata->ref_count = 0;
        WriteBufferFromOwnString buf;
        metadata->serialize(buf);
        metadatas[path] = buf.str();
    }

    return metadatas;
}

MetadataTransactionPtr MetadataStorageFromDisk::createTransaction()
{
    return std::make_shared<MetadataStorageFromDiskTransaction>(*this);
}

bool MetadataStorageFromDisk::supportWritingWithAppend() const
{
    return true;
}

StoredObjects MetadataStorageFromDisk::getStorageObjects(const std::string & path) const
{
    return readMetadata(path)->objects;
}

uint32_t MetadataStorageFromDisk::getHardlinkCount(const std::string & path) const
{
    return readMetadata(path)->ref_count;
}

void MetadataStorageFromDisk::startup()
{
    if (!persist_removal_queue)
        return;

    if (!disk->existsDirectory(String(SYSTEM_METADATA_DIR)))
        disk->createDirectory(String(SYSTEM_METADATA_DIR));

    std::lock_guard guard(removed_objects_mutex);
    bool needs_compaction = loadRemovalLog();

    if (needs_compaction)
        compactRemovalLog();

    LOG_INFO(log, "Loaded {} blobs pending removal from {}", objects_to_remove.size(), REMOVAL_LOG_FILE);
}

bool MetadataStorageFromDisk::loadRemovalLog()
{
    const auto path = std::string(REMOVAL_LOG_FILE);
    auto buf = disk->readFileIfExists(path, ReadSettings{});
    if (!buf)
        return false;

    bool needs_compaction = false;

    /// Read version header.
    UInt32 version = 0;
    try
    {
        readBinaryLittleEndian(version, *buf);
    }
    catch (...)
    {
        LOG_WARNING(log, "Truncated version header in {}, starting with empty queue: {}", REMOVAL_LOG_FILE, getCurrentExceptionMessage(false));
        return true;
    }

    if (version > REMOVAL_LOG_CURRENT_VERSION)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported removal log version: {}", version);

    if (version != REMOVAL_LOG_CURRENT_VERSION)
        needs_compaction = true;

    while (!buf->eof())
    {
        try
        {
            /// Binary format (V0): entry_type (UInt8) | remote_path (binary string) | local_path (binary string) | bytes_size (UInt64 LE)
            UInt8 entry_type = 0;
            readBinaryLittleEndian(entry_type, *buf);

            StoredObject object;
            readStringBinary(object.remote_path, *buf);
            readStringBinary(object.local_path, *buf);
            readBinaryLittleEndian(object.bytes_size, *buf);

            if (entry_type == RemovalLogEntryType::ADD)
            {
                objects_to_remove.submitForRemoval({std::move(object)});
            }
            else if (entry_type == RemovalLogEntryType::REMOVED)
            {
                objects_to_remove.markAsRemoved({object});
                ++removal_log_stale_entries;
            }
            else
            {
                LOG_WARNING(log, "Skipping unknown entry type '{}' in {}", static_cast<UInt16>(entry_type), REMOVAL_LOG_FILE);
            }
        }
        catch (...)
        {
            /// Truncated entry from a partial write (e.g. crash mid-append). Stop reading — everything
            /// loaded so far is valid. Compaction will rewrite the file cleanly.
            LOG_WARNING(log, "Truncated entry in {}, stopping: {}", REMOVAL_LOG_FILE, getCurrentExceptionMessage(false));
            needs_compaction = true;
            break;
        }
    }

    return needs_compaction;
}

void MetadataStorageFromDisk::appendToRemovalLog(RemovalLogEntryType entry_type, const StoredObjects & blobs)
{
    bool file_exists = disk->existsFile(String(REMOVAL_LOG_FILE));
    auto buf = disk->writeFile(String(REMOVAL_LOG_FILE), /* buf_size */ DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append, /* settings */ {});

    /// Write version header if this is a new file.
    /// An empty or corrupt file cannot exist here because loadRemovalLog on startup
    /// detects such cases and compactRemovalLog rewrites the file with a valid header.
    if (!file_exists)
    {
        UInt32 version = REMOVAL_LOG_CURRENT_VERSION;
        writeBinaryLittleEndian(version, *buf);
    }

    for (const auto & blob : blobs)
    {
        UInt8 type = entry_type;
        writeBinaryLittleEndian(type, *buf);
        writeStringBinary(blob.remote_path, *buf);
        writeStringBinary(blob.local_path, *buf);
        writeBinaryLittleEndian(blob.bytes_size, *buf);
    }
    buf->finalize();
    buf->sync();
}

void MetadataStorageFromDisk::compactRemovalLog()
{
    static constexpr std::string_view REMOVAL_LOG_TMP_FILE = ".metadata/blobs_to_remove.log.tmp";

    auto buf = disk->writeFile(String(REMOVAL_LOG_TMP_FILE), /* buf_size */ DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, /* settings */ {});

    UInt32 version = REMOVAL_LOG_CURRENT_VERSION;
    writeBinaryLittleEndian(version, *buf);

    auto all_blobs = objects_to_remove.takeFirst(0);
    for (const auto & blob : all_blobs)
    {
        UInt8 type = RemovalLogEntryType::ADD;
        writeBinaryLittleEndian(type, *buf);
        writeStringBinary(blob.remote_path, *buf);
        writeStringBinary(blob.local_path, *buf);
        writeBinaryLittleEndian(blob.bytes_size, *buf);
    }
    buf->finalize();
    buf->sync();

    disk->replaceFile(String(REMOVAL_LOG_TMP_FILE), String(REMOVAL_LOG_FILE));
    removal_log_stale_entries = 0;
}

IMetadataStorage::BlobsToRemove MetadataStorageFromDisk::getBlobsToRemove(const ClusterConfigurationPtr & cluster, int64_t max_count)
{
    std::lock_guard guard(removed_objects_mutex);

    BlobsToRemove blobs_to_remove;
    for (const auto & blob : objects_to_remove.takeFirst(max_count))
        blobs_to_remove[blob] = {cluster->getLocalLocation()};

    return blobs_to_remove;
}

int64_t MetadataStorageFromDisk::recordAsRemoved(const StoredObjects & blobs)
{
    std::lock_guard guard(removed_objects_mutex);

    /// Persist REMOVED entries to WAL before erasing from in-memory queue,
    /// so that on WAL failure the blobs remain tracked and will be retried.
    if (persist_removal_queue && !blobs.empty())
    {
        appendToRemovalLog(RemovalLogEntryType::REMOVED, blobs);
        removal_log_stale_entries += blobs.size();

        if (removal_log_stale_entries >= removal_log_compaction_threshold)
            compactRemovalLog();
    }

    return objects_to_remove.markAsRemoved(blobs);
}

bool MetadataStorageFromDisk::hasPendingRemovalBlobs(const StoredObjects & blobs) const
{
    std::lock_guard guard(removed_objects_mutex);
    return objects_to_remove.containsAny(blobs);
}

MetadataStorageFromDiskTransaction::MetadataStorageFromDiskTransaction(MetadataStorageFromDisk & metadata_storage_)
    : metadata_storage(metadata_storage_)
{
}

void MetadataStorageFromDiskTransaction::commit(const TransactionCommitOptionsVariant & options)
{
    if (!std::holds_alternative<NoCommitOptions>(options))
        throwNotImplemented();

    {
        std::unique_lock lock(metadata_storage.metadata_mutex);
        operations.commit();
    }

    operations.finalize();

    if (!objects_to_remove.empty())
    {
        std::lock_guard guard(metadata_storage.removed_objects_mutex);
        if (metadata_storage.persist_removal_queue)
        {
            try
            {
                metadata_storage.appendToRemovalLog(MetadataStorageFromDisk::RemovalLogEntryType::ADD, objects_to_remove);
            }
            catch (...)
            {
                LOG_ERROR(metadata_storage.log, "Failed to persist removal queue entry: {}. "
                    "Blobs are tracked in memory but may become orphaned on crash.",
                    getCurrentExceptionMessage(false));
            }
        }

        metadata_storage.objects_to_remove.submitForRemoval(objects_to_remove);
    }
}

TransactionCommitOutcomeVariant MetadataStorageFromDiskTransaction::tryCommit(const TransactionCommitOptionsVariant & options)
{
    if (!std::holds_alternative<NoCommitOptions>(options))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Metadata storage from disk supports only tryCommit without options");

    commit(NoCommitOptions{});
    return true;
}

void MetadataStorageFromDiskTransaction::writeStringToFile(
     const std::string & path,
     const std::string & data)
{
    operations.addOperation(std::make_unique<WriteFileOperation>(path, data, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::writeInlineDataToFile(
     const std::string & path,
     const std::string & data)
{
    operations.addOperation(std::make_unique<WriteInlineDataOperation>(path, data, metadata_storage.compatible_key_prefix, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::setLastModified(const std::string & path, const Poco::Timestamp & timestamp)
{
    operations.addOperation(std::make_unique<SetLastModifiedOperation>(path, timestamp, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::chmod(const String & path, mode_t mode)
{
    operations.addOperation(std::make_unique<ChmodOperation>(path, mode, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::unlinkFile(const std::string & path, bool if_exists, bool should_remove_objects)
{
    operations.addOperation(std::make_unique<UnlinkFileOperation>(path, if_exists, should_remove_objects, metadata_storage.compatible_key_prefix, *metadata_storage.getDisk(), objects_to_remove));
}

void MetadataStorageFromDiskTransaction::removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & should_remove_objects)
{
    operations.addOperation(std::make_unique<RemoveRecursiveOperation>(path, should_remove_objects, metadata_storage.compatible_key_prefix, *metadata_storage.getDisk(), objects_to_remove));
}

void MetadataStorageFromDiskTransaction::createHardLink(const std::string & path_from, const std::string & path_to)
{
    operations.addOperation(std::make_unique<CreateHardlinkOperation>(path_from, path_to, metadata_storage.compatible_key_prefix, *metadata_storage.disk));
}

void MetadataStorageFromDiskTransaction::createDirectory(const std::string & path)
{
    operations.addOperation(std::make_unique<CreateDirectoryOperation>(path, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::createDirectoryRecursive(const std::string & path)
{
    operations.addOperation(std::make_unique<CreateDirectoryRecursiveOperation>(path, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::removeDirectory(const std::string & path)
{
    operations.addOperation(std::make_unique<RemoveDirectoryOperation>(path, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    operations.addOperation(std::make_unique<MoveFileOperation>(path_from, path_to, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    operations.addOperation(std::make_unique<MoveDirectoryOperation>(path_from, path_to, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::replaceFile(const std::string & path_from, const std::string & path_to)
{
    operations.addOperation(std::make_unique<ReplaceFileOperation>(path_from, path_to, metadata_storage.compatible_key_prefix, *metadata_storage.getDisk(), objects_to_remove));
}

void MetadataStorageFromDiskTransaction::setReadOnly(const std::string & path)
{
    operations.addOperation(std::make_unique<SetReadonlyFileOperation>(path, metadata_storage.compatible_key_prefix, *metadata_storage.getDisk()));
}

void MetadataStorageFromDiskTransaction::createMetadataFile(const std::string & path, const StoredObjects & objects)
{
    operations.addOperation(std::make_unique<RewriteFileOperation>(path, objects, metadata_storage.compatible_key_prefix, *metadata_storage.disk, objects_to_remove));
}

void MetadataStorageFromDiskTransaction::addBlobToMetadata(const std::string & path, const StoredObject & object)
{
    operations.addOperation(std::make_unique<AddBlobOperation>(path, object, metadata_storage.compatible_key_prefix, *metadata_storage.disk));
}

void MetadataStorageFromDiskTransaction::truncateFile(const std::string & src_path, size_t target_size)
{
    operations.addOperation(std::make_unique<TruncateMetadataFileOperation>(src_path, target_size, metadata_storage.compatible_key_prefix, *metadata_storage.getDisk(), objects_to_remove));
}

ObjectStorageKey MetadataStorageFromDiskTransaction::generateObjectKeyForPath(const std::string & /*path*/)
{
    return metadata_storage.key_generator->generate();
}

StoredObjects MetadataStorageFromDiskTransaction::getSubmittedForRemovalBlobs()
{
    return objects_to_remove;
}

}
