#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsMetadata.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/PrefixPath.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/UncommittedState.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/Preconditions.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableMetrics.h>
#include <Disks/DiskObjectStorage/MetadataStorages/StaticDirectoryIterator.h>
#include <Disks/DiskObjectStorage/MetadataStorages/NormalizedPath.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIterator.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>
#include <IO/ReadHelpers.h>
#include <IO/S3Common.h>
#include <IO/SharedThreadPools.h>
#include <Poco/Timestamp.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/thread_local_rng.h>
#include <Common/threadPoolCallbackRunner.h>

#if USE_AZURE_BLOB_STORAGE
#    include <azure/storage/common/storage_exception.hpp>
#endif

namespace ProfileEvents
{
    extern const Event DiskPlainRewritableLegacyLayoutDiskCount;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
    extern const char plain_rewritable_object_storage_azure_not_found_on_init[];
}

namespace
{

fs::path normalizeDirectoryPath(const fs::path & path)
{
    return path / "";
}

std::optional<std::string> getBlobKeyIfExists(const FsSnapshot & snapshot, const NormalizedPath & path)
{
    const auto directory = snapshot.getDirectoryRemoteInfo(path.parent_path());
    if (!directory)
        return std::nullopt;

    const auto it = directory->files.find(path.filename());
    if (it == directory->files.end())
        return std::nullopt;

    return getBlobKey(*directory, path.filename(), it->second);
}

}

void MetadataStorageFromPlainRewritableObjectStorage::load(bool is_initial_load, bool do_not_load_unchanged_directories)
{
    ThreadPool & pool = getIOThreadPool().get();

    LoggerPtr log = getLogger("MetadataStorageFromPlainObjectStorage");

    auto settings = getReadSettings();
    settings.enable_filesystem_cache = false;
    settings.useForSmallRemoteRead(1024);  /// These files are small.

    LOG_DEBUG(log, "Loading metadata");

    /// This method can do both initial loading and incremental refresh of the metadata.
    ///
    /// We will list directories under __meta and compare it with the current list in memory.
    /// Some directories may be new and some no longer exist in the storage.
    /// We want to update the state in memory without holding a lock,
    /// and we can do it while allowing certain race-conditions.

    /// So, we obtain a list, then apply changes by:
    /// 1. Deleting every directory in memory that no longer present in the storage;
    ///    This works correctly under the assumption that if a directory with a certain name was deleted it cannot appear again.
    ///    And this assumption is satisfied, because every name is a unique random value.
    /// 2. Checking the value of `prefix.path` for every new directory and adding it to the state in memory.
    ///    There is (?) a race condition, leading to the possibility to add a directory that was just deleted.
    ///    This race condition can be ignored for MergeTree tables.
    /// 3. Checking if the value of `prefix.path` changed for any already existing directory
    ///    and apply the corresponding rename.

    bool has_metadata = object_storage->existsOrHasAnyChild(layout->constructMetadataDirectoryKey());

    std::mutex remote_layout_mutex;
    std::unordered_map<std::string, DirectoryRemoteInfo> remote_layout;
    remote_layout[""] = DirectoryRemoteInfo{PlainRewritableLayout::ROOT_DIRECTORY_TOKEN, "fake_etag", 0, {}};

    if (is_initial_load)
    {
        /// Use iteration to determine if the disk contains data.
        /// LocalObjectStorage creates an empty top-level directory even when no data is stored,
        /// unlike blob storage, which has no concept of directories, therefore existsOrHasAnyChild
        /// is not applicable.
        auto common_key_prefix = fs::path(object_storage->getCommonKeyPrefix()) / "";
        bool has_data = object_storage->isRemote() ? object_storage->existsOrHasAnyChild(common_key_prefix) : object_storage->iterate(common_key_prefix, 0, /*with_tags=*/ false, std::nullopt)->isValid();
        /// No metadata directory: legacy layout is likely in use.
        if (has_data && !has_metadata)
        {
            ProfileEvents::increment(ProfileEvents::DiskPlainRewritableLegacyLayoutDiskCount, 1);
            LOG_WARNING(log, "Legacy layout is likely used for disk '{}'", object_storage->getCommonKeyPrefix());
        }

        if (!has_data && !has_metadata)
        {
            LOG_DEBUG(log, "Loaded metadata (empty)");
            fs.applyLayout(std::move(remote_layout));
            return;
        }
    }

    const auto read_snapshot = fs.takeReadOnlySnapshot();

    ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::PLAIN_REWRITABLE_META_LOAD);
    try
    {
        for (auto iterator = object_storage->iterate(layout->constructMetadataDirectoryKey(), 0, /*with_tags=*/ false, std::nullopt); iterator->isValid(); iterator->next())
        {
            const auto file = iterator->current();
            const auto remote_path = layout->parseDirectoryObjectKey(file->getPath());
            if (!remote_path.has_value())
                continue;

            /// Passing by reference:
            /// log: Created before runner, so it will be destroyed after
            /// settings: Same as log
            /// remove_layout: Same
            /// remote_layout_mutex: Same
            /// In any case we have a try {} catch (...) around runner usage, so exceptions will call runner.waitForAllToFinish() first
            /// Thus the order of destruction of the variables is not important
            runner.enqueueAndKeepTrack([remote_path, object_path = file->getPath(), metadata = file->metadata, read_snapshot, do_not_load_unchanged_directories, &log, &settings, this, &remote_layout, &remote_layout_mutex]
            {
                DB::setThreadName(ThreadName::PLAIN_REWRITABLE_META_LOAD);

                StoredObject object{object_path};
                String local_path;
                bool has_explicit_file_list = false;
                /// Assuming that local and the object storage clocks are synchronized.
                Poco::Timestamp last_modified = metadata->last_modified;
                std::unordered_map<std::string, FileRemoteInfo> files;

                try
                {
                    if (metadata->size_bytes == 0)
                        LOG_TRACE(log, "The object with the key '{}' has size 0, skipping the read", object_path);
                    else
                    {
                        auto read_buf = object_storage->readObject(object, settings);
                        String contents;
                        readStringUntilEOF(contents, *read_buf);

                        auto prefix_path = parsePrefixPath(contents);
                        local_path = std::move(prefix_path.logical_path);
                        has_explicit_file_list = prefix_path.has_explicit_file_list;

                        /// In the explicit form, the list of files comes from the metadata object and the blobs are not listed.
                        for (auto & listed_file : prefix_path.files)
                        {
                            if (listed_file.blob_key == getDefaultBlobKey(remote_path.value(), listed_file.name))
                                listed_file.blob_key.clear();

                            files.emplace(std::move(listed_file.name), FileRemoteInfo{
                                .bytes_size = listed_file.bytes_size,
                                .last_modified = last_modified.epochTime(),
                                .blob_key = std::move(listed_file.blob_key),
                            });
                        }
                    }

                    /// The root directory has a metadata object only in the explicit form, and it is stored under the reserved remote path.
                    if (remote_path.value() == PlainRewritableLayout::ROOT_DIRECTORY_TOKEN)
                        local_path.clear();
                    else if (normalizePath(local_path).empty())
                    {
                        /// Only the reserved metadata object maps to the logical root, so an empty logical path here means that
                        /// the object does not describe a directory: it is either not written yet (`LocalObjectStorage` writes
                        /// to the final key directly, so an interrupted write can leave the object empty and visible),
                        /// or it is a leftover of a directory that has been removed. Loading it as the root would hide the real
                        /// root and send lookups under it to the prefix of this directory.
                        LOG_WARNING(log, "The object with the key '{}' does not contain the logical path of a directory, ignoring it", object_path);
                        return;
                    }

                    if (do_not_load_unchanged_directories)
                    {
                        if (const auto known_info = read_snapshot->getDirectoryRemoteInfo(local_path);
                            known_info && known_info->remote_path == remote_path.value() && known_info->etag == metadata->etag)
                        {
                            std::lock_guard guard(remote_layout_mutex);
                            remote_layout[local_path] = known_info.value();
                            return;
                        }
                    }

                    /// In the implicit form, the files of the directory are the blobs stored under its prefix.
                    if (!has_explicit_file_list)
                    {
                        for (auto dir_iterator = object_storage->iterate(layout->constructFilesDirectoryKey(remote_path.value()), 0, /*with_tags=*/ false, std::nullopt); dir_iterator->isValid(); dir_iterator->next())
                        {
                            const auto remote_file = dir_iterator->current();
                            const auto unpacked_remote_file_path = layout->parseFileObjectKey(remote_file->getPath());
                            if (!unpacked_remote_file_path.has_value())
                            {
                                LOG_WARNING(log, "Legacy layout is in use, ignoring '{}'", remote_file->getPath());
                                continue;
                            }

                            const auto & [directory_remote_path, filename] = unpacked_remote_file_path.value();
                            chassert(directory_remote_path == remote_path);

                            files.emplace(filename, FileRemoteInfo{
                                .bytes_size = remote_file->metadata->size_bytes,
                                .last_modified = remote_file->metadata->last_modified.epochTime(),
                                .blob_key = {},
                            });
                        }
                    }

#if USE_AZURE_BLOB_STORAGE
                    fiu_do_on(FailPoints::plain_rewritable_object_storage_azure_not_found_on_init, {
                        std::bernoulli_distribution fault(0.25);
                        if (fault(thread_local_rng))
                        {
                            LOG_TEST(log, "Fault injection");
                            throw Azure::Storage::StorageException::CreateFromResponse(std::make_unique<Azure::Core::Http::RawResponse>(
                                1, 0, Azure::Core::Http::HttpStatusCode::NotFound, "Fault injected"));
                        }
                    });
#endif
                }
#if USE_AWS_S3
                catch (const S3Exception & e)
                {
                    /// It is ok if a directory was removed just now.
                    if (e.getS3ErrorCode() == Aws::S3::S3Errors::NO_SUCH_KEY)
                        return;
                    throw;
                }
#endif
#if USE_AZURE_BLOB_STORAGE
                catch (const Azure::Storage::StorageException & e)
                {
                    if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound)
                        return;
                    throw;
                }
#endif
                catch (...)
                {
                    throw;
                }

                std::lock_guard guard(remote_layout_mutex);
                remote_layout[local_path] = DirectoryRemoteInfo{remote_path.value(), metadata->etag, last_modified.epochTime(), std::move(files), has_explicit_file_list};
            });
        }
    }
    catch (...)
    {
        runner.waitForAllToFinish();
        throw;
    }

    runner.waitForAllToFinishAndRethrowFirstError();

    /// Root folder is a special case. Files are stored as /__root/{file-name}, unless the root has switched to the explicit file list.
    if (!remote_layout[""].has_explicit_file_list)
    {
        for (auto iterator = object_storage->iterate(layout->constructRootFilesDirectoryKey(), 0, /*with_tags=*/ false, std::nullopt); iterator->isValid(); iterator->next())
        {
            auto remote_file = iterator->current();
            remote_layout[""].files.emplace(remote_file->getFileName(), FileRemoteInfo{
                .bytes_size = remote_file->metadata->size_bytes,
                .last_modified = remote_file->metadata->last_modified.epochTime(),
                .blob_key = {},
            });
        }
    }

    LOG_DEBUG(log, "Loaded metadata for {} directories", remote_layout.size());
    fs.applyLayout(std::move(remote_layout));
    previous_refresh.restart();
}

MetadataStorageFromPlainRewritableObjectStorage::MetadataStorageFromPlainRewritableObjectStorage(ObjectStoragePtr object_storage_, String storage_path_prefix_, bool hard_links_enabled_)
    : object_storage(std::move(object_storage_))
    , metrics(createPlainRewritableMetrics(object_storage->getType()))
    , storage_path_prefix(std::move(storage_path_prefix_))
    , storage_path_full(fs::path(object_storage->getRootPrefix()) / storage_path_prefix)
    , hard_links_enabled(hard_links_enabled_)
    , fs(metrics->directory_map_size, metrics->file_count)
    , layout(std::make_shared<PlainRewritableLayout>(object_storage->getCommonKeyPrefix()))
{
    load(/*is_initial_load=*/true, /*do_not_load_unchanged_directories=*/false);
}

MetadataTransactionPtr MetadataStorageFromPlainRewritableObjectStorage::createTransaction()
{
    return std::make_shared<MetadataStorageFromPlainRewritableObjectStorageTransaction>(*this);
}

void MetadataStorageFromPlainRewritableObjectStorage::dropCache()
{
    std::unique_lock reload_lock(load_mutex);
    std::unique_lock tx_lock(metadata_mutex);
    load(/*is_initial_load=*/false, /*do_not_load_unchanged_directories=*/false);
}

void MetadataStorageFromPlainRewritableObjectStorage::refresh(UInt64 not_sooner_than_milliseconds)
{
    if (!previous_refresh.compareAndRestart(0.001 * static_cast<double>(not_sooner_than_milliseconds)))
        return;

    std::unique_lock load_lock(load_mutex, std::defer_lock);
    if (load_lock.try_lock())
    {
        std::unique_lock metadata_lock(metadata_mutex);
        load(/*is_initial_load=*/false, /*do_not_load_unchanged_directories=*/true);
    }
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsFile(const std::string & path) const
{
    return fs.takeReadOnlySnapshot()->existsFile(path);
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsDirectory(const std::string & path) const
{
    return fs.takeReadOnlySnapshot()->existsDirectory(path);
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsFileOrDirectory(const std::string & path) const
{
    const auto tree = fs.takeReadOnlySnapshot();
    return tree->existsFile(path) || tree->existsDirectory(path);
}

uint64_t MetadataStorageFromPlainRewritableObjectStorage::getFileSize(const std::string & path) const
{
    if (auto file_size = getFileSizeIfExists(path))
        return file_size.value();

    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist", path);
}

std::optional<uint64_t> MetadataStorageFromPlainRewritableObjectStorage::getFileSizeIfExists(const std::string & path) const
{
    if (auto remote_info = fs.takeReadOnlySnapshot()->getFileRemoteInfo(path))
        return remote_info->bytes_size;

    return std::nullopt;
}

std::vector<std::string> MetadataStorageFromPlainRewritableObjectStorage::listDirectory(const std::string & path) const
{
    return fs.takeReadOnlySnapshot()->listDirectory(path);
}

DirectoryIteratorPtr MetadataStorageFromPlainRewritableObjectStorage::iterateDirectory(const std::string & path) const
{
    auto paths = listDirectory(path);

    /// Prepend path, since iterateDirectory() includes path, unlike listDirectory()
    std::for_each(paths.begin(), paths.end(), [&](auto & child) { child = fs::path(path) / child; });
    std::vector<fs::path> fs_paths(paths.begin(), paths.end());
    return std::make_unique<StaticDirectoryIterator>(std::move(fs_paths));
}

StoredObjects MetadataStorageFromPlainRewritableObjectStorage::getStorageObjects(const std::string & path) const
{
    if (auto objects = getStorageObjectsIfExist(path))
        return std::move(objects.value());

    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist", path);
}

std::optional<StoredObjects> MetadataStorageFromPlainRewritableObjectStorage::getStorageObjectsIfExist(const std::string & path) const
{
    const auto tree = fs.takeReadOnlySnapshot();

    const auto normalized_path = normalizePath(path);
    const auto directory_remote_info = tree->getDirectoryRemoteInfo(normalized_path.parent_path());
    if (!directory_remote_info)
        return std::nullopt;

    const auto file_it = directory_remote_info->files.find(normalized_path.filename());
    if (file_it == directory_remote_info->files.end())
        return std::nullopt;

    auto object_key = layout->constructBlobObjectKey(getBlobKey(*directory_remote_info, normalized_path.filename(), file_it->second));
    return StoredObjects{StoredObject(object_key, path, file_it->second.bytes_size)};
}

uint32_t MetadataStorageFromPlainRewritableObjectStorage::getHardlinkCount(const std::string & path) const
{
    const auto tree = fs.takeReadOnlySnapshot();

    const auto normalized_path = normalizePath(path);
    const auto directory_remote_info = tree->getDirectoryRemoteInfo(normalized_path.parent_path());
    if (!directory_remote_info)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist", path);

    const auto file_it = directory_remote_info->files.find(normalized_path.filename());
    if (file_it == directory_remote_info->files.end())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist", path);

    /// As for the other metadata storages, this is the number of links besides the file itself.
    return tree->getBlobLinkCount(getBlobKey(*directory_remote_info, normalized_path.filename(), file_it->second)) - 1;
}

Poco::Timestamp MetadataStorageFromPlainRewritableObjectStorage::getLastModified(const std::string & path) const
{
    if (auto last_modified = getLastModifiedIfExists(path))
        return last_modified.value();

    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File or directory {} does not exist", path);
}

std::optional<Poco::Timestamp> MetadataStorageFromPlainRewritableObjectStorage::getLastModifiedIfExists(const String & path) const
{
    const auto tree = fs.takeReadOnlySnapshot();

    if (tree->existsDirectory(path))
    {
        const auto remote_info = tree->getDirectoryRemoteInfo(path);

        if (remote_info)
            return Poco::Timestamp::fromEpochTime(remote_info->last_modified);

        /// Let's return something in this case to unblock fs garbage cleanup.
        return Poco::Timestamp::fromEpochTime(0);
    }

    if (auto remote_info = tree->getFileRemoteInfo(path))
        return Poco::Timestamp::fromEpochTime(remote_info->last_modified);

    return std::nullopt;
}

MetadataStorageFromPlainRewritableObjectStorageTransaction::MetadataStorageFromPlainRewritableObjectStorageTransaction(MetadataStorageFromPlainRewritableObjectStorage & metadata_storage_)
    : metadata_storage(metadata_storage_)
    , commit_snapshot(metadata_storage.fs.takeReadWriteSnapshot())
    , uncommitted_state(metadata_storage.fs.takeReadWriteSnapshot())
{
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::commit(const TransactionCommitOptionsVariant & options)
{
    if (!std::holds_alternative<NoCommitOptions>(options))
        throwNotImplemented();

    /// 0. Add preconditions for transaction commit.
    operations.prependOperation(std::make_unique<MetadataStorageFromPlainObjectStorageValidatePreconditionsOperation>(uncommitted_state.getTxPreconditions(), commit_snapshot));

    {
        std::unique_lock lock(metadata_storage.metadata_mutex);

        /// 1. Setup up-to-date fs into snapshot being used during commit.
        commit_snapshot->setRoot(metadata_storage.fs.takeReadWriteSnapshot()->getRoot());

        /// 2. Execute all operations on top of write set.
        operations.commit();

        /// 3. Exchange metadata with updated fs.
        metadata_storage.fs.applySnapshot(commit_snapshot);
    }

    operations.finalize();
}

TransactionCommitOutcomeVariant MetadataStorageFromPlainRewritableObjectStorageTransaction::tryCommit(const TransactionCommitOptionsVariant & /*options*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Plain-Rewritable Metadata storage supports only commit");
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::createMetadataFile(const std::string & path, const StoredObjects & objects)
{
    /// The blob has been written to the key chosen by `generateObjectKeyForPath`; if the key was not generated
    /// by this transaction, the blob is expected at the default location.
    std::string blob_key;
    if (const auto it = generated_blob_keys.find(normalizePath(path).string()); it != generated_blob_keys.end())
        blob_key = it->second;

    /// The following operations of this transaction have to see the file: a hard link to it makes its blob shared,
    /// and then rewriting it has to pick a new blob instead of clobbering the shared one.
    uncommitted_state.recordCreatedFile(path, blob_key);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageWriteFileOperation>(
        path,
        objects.front(),
        std::move(blob_key),
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics,
        removed_objects));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::createDirectory(const std::string & path)
{
    if (normalizePath(path).empty())
    {
        LOG_TRACE(getLogger("MetadataStorageFromPlainRewritableObjectStorageTransaction"), "Skipping creation of a directory '{}' with an empty normalized path", path);
        return;
    }

    uncommitted_state.createDirectory(path);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageCreateDirectoryOperation>(
        /*recursive=*/false,
        normalizeDirectoryPath(path),
        uncommitted_state.getDirectoryRemoteInfo(path)->remote_path,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::createDirectoryRecursive(const std::string & path)
{
    if (normalizePath(path).empty())
    {
        LOG_TRACE(getLogger("MetadataStorageFromPlainRewritableObjectStorageTransaction"), "Skipping creation of a directory '{}' with an empty normalized path", path);
        return;
    }

    uncommitted_state.createDirectory(path);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageCreateDirectoryOperation>(
        /*recursive=*/true,
        normalizeDirectoryPath(path),
        uncommitted_state.getDirectoryRemoteInfo(path)->remote_path,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    uncommitted_state.moveDirectory(path_from, path_to);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageMoveDirectoryOperation>(
        normalizeDirectoryPath(path_from),
        normalizeDirectoryPath(path_to),
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::unlinkFile(const std::string & path, bool if_exists, bool /*should_remove_objects*/)
{
    const auto normalized_path = normalizePath(path);
    uncommitted_state.useDirectory(normalized_path.parent_path());

    /// Removing a file whose blob is shared switches the directory to the explicit file list.
    if (const auto blob_key = getBlobKeyIfExists(uncommitted_state.getSnapshot(), normalized_path);
        blob_key && uncommitted_state.getSnapshot().getBlobLinkCount(*blob_key) > 1)
        uncommitted_state.markDirectoryExplicit(normalized_path.parent_path());

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation>(
        path,
        if_exists,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics,
        removed_objects));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::removeDirectory(const std::string & path)
{
    if (!normalizePath(path).empty())
        uncommitted_state.removeDirectory(path);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation>(
        normalizeDirectoryPath(path),
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & /*should_remove_objects*/)
{
    if (!normalizePath(path).empty())
        uncommitted_state.removeDirectory(path);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation>(
        path,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics,
        removed_objects));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::createHardLink(const std::string & path_from, const std::string & path_to)
{
    const auto normalized_path_from = normalizePath(path_from);
    const auto normalized_path_to = normalizePath(path_to);
    uncommitted_state.useDirectory(normalized_path_from.parent_path());
    uncommitted_state.useDirectory(normalized_path_to.parent_path());

    /// A real hard link would make the metadata of the target directory unreadable by older servers, so it is opt-in.
    if (!metadata_storage.hard_links_enabled)
    {
        operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageCopyFileOperation>(
            path_from,
            path_to,
            commit_snapshot,
            metadata_storage.object_storage,
            metadata_storage.layout,
            metadata_storage.metrics));
        return;
    }

    /// The target directory switches to the explicit file list and the blob becomes shared.
    uncommitted_state.recordHardLink(path_from, path_to);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageHardLinkOperation>(
        path_from,
        path_to,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::planFileMove(const NormalizedPath & path_from, const NormalizedPath & path_to)
{
    uncommitted_state.useDirectory(path_from.parent_path());
    uncommitted_state.useDirectory(path_to.parent_path());

    const auto & snapshot = uncommitted_state.getSnapshot();
    const auto directory_from = snapshot.getDirectoryRemoteInfo(path_from.parent_path());
    const auto directory_to = snapshot.getDirectoryRemoteInfo(path_to.parent_path());
    const auto blob_key_from = getBlobKeyIfExists(snapshot, path_from);
    if (!directory_from || !directory_to || !blob_key_from)
        return;

    const bool metadata_only = isMetadataOnlyMove(snapshot, *directory_from, *directory_to, *blob_key_from, getBlobKeyIfExists(snapshot, path_to));
    if (metadata_only)
    {
        uncommitted_state.markDirectoryExplicit(path_from.parent_path());
        uncommitted_state.markDirectoryExplicit(path_to.parent_path());
    }

    /// The moved file has to be visible at its new path: a hard link to it makes its blob shared, and then rewriting it
    /// has to pick a new blob instead of clobbering the shared one.
    uncommitted_state.recordMovedFile(path_from, path_to, /*keeps_blob=*/metadata_only);
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    planFileMove(normalizePath(path_from), normalizePath(path_to));

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageMoveFileOperation>(
        /*replaceable=*/false,
        path_from,
        path_to,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics,
        removed_objects));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::replaceFile(const std::string & path_from, const std::string & path_to)
{
    planFileMove(normalizePath(path_from), normalizePath(path_to));

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageMoveFileOperation>(
        /*replaceable=*/true,
        path_from,
        path_to,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics,
        removed_objects));
}

ObjectStorageKey MetadataStorageFromPlainRewritableObjectStorageTransaction::generateObjectKeyForPath(const std::string & path)
{
    const auto normalized_path = normalizePath(path);
    if (normalized_path.filename().empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File name is empty for path '{}'", path);

    const auto parent_path = normalized_path.parent_path();
    const auto parent_info = uncommitted_state.getDirectoryRemoteInfo(parent_path);

    if (!parent_info)
    {
        /// Validate during commit that directory will be created on S3.
        uncommitted_state.useMissingDirectory(parent_path);

        /// Materialize virtual parent.
        createDirectoryRecursive(parent_path);
    }
    else
    {
        /// Validate during commit that directory will not be recreated on S3.
        uncommitted_state.useDirectory(parent_path);
    }

    if (const auto directory_remote_info = uncommitted_state.getDirectoryRemoteInfo(parent_path))
    {
        const auto file_name = normalized_path.filename().string();

        /// In a directory with the explicit file list the blob names are random: a new file must not clobber the blob
        /// of a removed file that is still linked from elsewhere. The same applies to rewriting a file whose blob is shared.
        bool use_random_name = directory_remote_info->has_explicit_file_list;
        if (!use_random_name)
            if (const auto it = directory_remote_info->files.find(file_name); it != directory_remote_info->files.end())
                use_random_name = uncommitted_state.getSnapshot().getBlobLinkCount(getBlobKey(*directory_remote_info, file_name, it->second)) > 1;

        auto blob_key = getDefaultBlobKey(directory_remote_info->remote_path, use_random_name ? getRandomASCIIString(32) : file_name);
        auto object_key = ObjectStorageKey::createAsAbsolute(metadata_storage.layout->constructBlobObjectKey(blob_key));
        generated_blob_keys[normalized_path.string()] = std::move(blob_key);
        return object_key;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", parent_path.string());
}

StoredObjects MetadataStorageFromPlainRewritableObjectStorageTransaction::getSubmittedForRemovalBlobs()
{
    return removed_objects;
}

}
