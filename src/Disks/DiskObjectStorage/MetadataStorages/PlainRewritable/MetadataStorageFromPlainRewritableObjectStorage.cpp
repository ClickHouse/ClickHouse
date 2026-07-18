#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/InMemoryDirectoryTree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableMetrics.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableMetadataHelpers.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritablePrefixPath.h>
#include <Disks/DiskObjectStorage/MetadataStorages/StaticDirectoryIterator.h>
#include <Disks/DiskObjectStorage/MetadataStorages/NormalizedPath.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIterator.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <cstddef>
#include <optional>
#include <unordered_set>
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
    extern const int INCORRECT_DATA;
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
            rebuildBlobRefcounts(remote_layout);
            fs_tree->apply(std::move(remote_layout));
            return;
        }
    }

    ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::PLAIN_REWRITABLE_META_LOAD);
    try
    {
        for (auto iterator = object_storage->iterate(layout->constructMetadataDirectoryKey(), 0, /*with_tags=*/ false, std::nullopt); iterator->isValid(); iterator->next())
        {
            const auto file = iterator->current();
            const auto remote_path = layout->parseDirectoryObjectKey(file->getPath());
            if (!remote_path.has_value())
                continue;

            if (do_not_load_unchanged_directories)
            {
                if (auto directory_info = fs_tree->lookupDirectoryIfNotChanged(remote_path.value(), file->metadata->etag))
                {
                    /// Already loaded.
                    std::lock_guard guard(remote_layout_mutex);
                    auto & [local_path, remote_info] = directory_info.value();
                    remote_layout[local_path] = std::move(remote_info);
                    continue;
                }
            }

            /// Passing by reference:
            /// log: Created before runner, so it will be destroyed after
            /// settings: Same as log
            /// remove_layout: Same
            /// remote_layout_mutex: Same
            /// In any case we have a try {} catch (...) around runner usage, so exceptions will call runner.waitForAllToFinish() first
            /// Thus the order of destruction of the variables is not important
            runner.enqueueAndKeepTrack([remote_path, object_path = file->getPath(), metadata = file->metadata, &log, &settings, this, &remote_layout, &remote_layout_mutex]
            {
                DB::setThreadName(ThreadName::PLAIN_REWRITABLE_META_LOAD);

                StoredObject object{object_path};
                /// Assuming that local and the object storage clocks are synchronized.
                Poco::Timestamp last_modified = metadata->last_modified;
                std::unordered_map<std::string, FileRemoteInfo> files;
                bool explicit_files = false;
                String local_path;

                try
                {
                    PlainRewritablePrefixPath prefix_path;
                    if (metadata->size_bytes == 0)
                    {
                        LOG_TRACE(log, "The object with the key '{}' has size 0, skipping the read", object_path);
                    }
                    else
                    {
                        auto read_buf = object_storage->readObject(object, settings);
                        String content;
                        readStringUntilEOF(content, *read_buf);
                        prefix_path = parsePlainRewritablePrefixPath(content);
                        local_path = prefix_path.logical_path;
                        explicit_files = prefix_path.explicit_files;
                    }

                    /// Root directory metadata uses the reserved remote token and maps to logical "".
                    if (remote_path.value() == PlainRewritableLayout::ROOT_DIRECTORY_TOKEN)
                        local_path.clear();

                    if (explicit_files)
                    {
                        /// Explicit form: file list comes from prefix.path; blobs may live under other prefixes.
                        for (const auto & [filename, relative_object_key] : prefix_path.files)
                        {
                            const auto absolute_object_key = layout->constructObjectKey(relative_object_key);
                            auto object_metadata = object_storage->tryGetObjectMetadata(absolute_object_key, /*with_tags=*/ false);
                            if (!object_metadata)
                            {
                                throw Exception(
                                    ErrorCodes::INCORRECT_DATA,
                                    "Blob '{}' referenced by explicit prefix.path for directory '{}' does not exist",
                                    absolute_object_key,
                                    local_path);
                            }

                            files.emplace(filename, FileRemoteInfo{
                                .bytes_size = object_metadata->size_bytes,
                                .last_modified = object_metadata->last_modified.epochTime(),
                                .object_key = relative_object_key,
                            });
                        }
                    }
                    else
                    {
                        /// Implicit form: discover files by listing blobs under the directory remote prefix.
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
                                .object_key = layout->makeRelativeFileObjectKey(remote_path.value(), filename),
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
                remote_layout[local_path] = DirectoryRemoteInfo{
                    remote_path.value(),
                    metadata->etag,
                    last_modified.epochTime(),
                    std::move(files),
                    explicit_files};
            });
        }
    }
    catch (...)
    {
        runner.waitForAllToFinish();
        throw;
    }

    runner.waitForAllToFinishAndRethrowFirstError();

    /// Root folder is a special case. Files are stored as /__root/{file-name} unless an explicit
    /// prefix.path for the root remote token already provided the file list.
    if (!remote_layout[""].explicit_files)
    {
        for (auto iterator = object_storage->iterate(layout->constructRootFilesDirectoryKey(), 0, /*with_tags=*/ false, std::nullopt); iterator->isValid(); iterator->next())
        {
            auto remote_file = iterator->current();
            remote_layout[""].files.emplace(remote_file->getFileName(), FileRemoteInfo{
                .bytes_size = remote_file->metadata->size_bytes,
                .last_modified = remote_file->metadata->last_modified.epochTime(),
                .object_key = layout->makeRelativeFileObjectKey(PlainRewritableLayout::ROOT_DIRECTORY_TOKEN, remote_file->getFileName()),
            });
        }
    }

    LOG_DEBUG(log, "Loaded metadata for {} directories", remote_layout.size());
    rebuildBlobRefcounts(remote_layout);
    if (is_initial_load)
        removeOrphanBlobs(remote_layout);
    fs_tree->apply(std::move(remote_layout));
    previous_refresh.restart();
}

void MetadataStorageFromPlainRewritableObjectStorage::rebuildBlobRefcounts(
    const std::unordered_map<std::string, DirectoryRemoteInfo> & remote_layout)
{
    std::unordered_map<std::string, uint32_t> new_refcounts;
    for (const auto & [_, directory_info] : remote_layout)
    {
        for (const auto & [file_name, file_info] : directory_info.files)
        {
            const auto relative_key = resolvePlainRewritableRelativeObjectKey(*layout, directory_info, file_name, file_info);
            ++new_refcounts[relative_key];
        }
    }

    blob_refcounts->replaceAll(std::move(new_refcounts));
}

void MetadataStorageFromPlainRewritableObjectStorage::removeOrphanBlobs(
    const std::unordered_map<std::string, DirectoryRemoteInfo> & remote_layout)
{
    auto referenced = blob_refcounts->snapshot();
    std::unordered_set<std::string> referenced_absolute;
    referenced_absolute.reserve(referenced.size());
    for (const auto & [relative_key, _] : referenced)
        referenced_absolute.insert(layout->constructObjectKey(relative_key));

    /// All prefix.path objects are metadata and must be kept.
    for (const auto & [_, directory_info] : remote_layout)
        referenced_absolute.insert(layout->constructDirectoryObjectKey(directory_info.remote_path));

    StoredObjects orphans;
    const auto common_prefix = fs::path(object_storage->getCommonKeyPrefix()) / "";
    for (auto iterator = object_storage->iterate(common_prefix, 0, /*with_tags=*/ false, std::nullopt); iterator->isValid(); iterator->next())
    {
        const auto object_path = iterator->current()->getPath();
        if (referenced_absolute.contains(object_path))
            continue;

        /// Keep the metadata directory listing objects themselves out of GC if parsing failed.
        if (object_path.find(std::string(PlainRewritableLayout::METADATA_DIRECTORY_TOKEN) + "/") != std::string::npos
            && object_path.ends_with(PlainRewritableLayout::PREFIX_PATH_FILE_NAME))
            continue;

        LOG_INFO(getLogger("MetadataStorageFromPlainObjectStorage"), "Removing orphan blob '{}'", object_path);
        orphans.emplace_back(object_path);
    }

    if (!orphans.empty())
        object_storage->removeObjectsIfExist(orphans);
}

uint32_t MetadataStorageFromPlainRewritableObjectStorage::getHardlinkCount(const std::string & path) const
{
    const auto file_info = fs_tree->getFileRemoteInfo(path);
    if (!file_info)
        return 0;

    const auto normalized_path = normalizePath(path);
    const auto directory_info = fs_tree->getDirectoryRemoteInfo(normalized_path.parent_path());
    if (!directory_info)
        return 0;

    const auto relative_key = resolvePlainRewritableRelativeObjectKey(*layout, *directory_info, normalized_path.filename(), *file_info);
    return blob_refcounts->get(relative_key);
}

MetadataStorageFromPlainRewritableObjectStorage::MetadataStorageFromPlainRewritableObjectStorage(ObjectStoragePtr object_storage_, String storage_path_prefix_)
    : object_storage(std::move(object_storage_))
    , metrics(createPlainRewritableMetrics(object_storage->getType()))
    , storage_path_prefix(std::move(storage_path_prefix_))
    , storage_path_full(fs::path(object_storage->getRootPrefix()) / storage_path_prefix)
    , fs_tree(std::make_shared<InMemoryDirectoryTree>(metrics->directory_map_size, metrics->file_count))
    , layout(std::make_shared<PlainRewritableLayout>(object_storage->getCommonKeyPrefix()))
    , blob_refcounts(std::make_shared<PlainRewritableBlobRefcounts>())
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

    std::unique_lock lock(load_mutex, std::defer_lock);
    if (lock.try_lock())
        load(/*is_initial_load=*/false, /*do_not_load_unchanged_directories=*/true);
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsFile(const std::string & path) const
{
    return fs_tree->existsFile(path);
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsDirectory(const std::string & path) const
{
    return fs_tree->existsDirectory(path).first;
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsFileOrDirectory(const std::string & path) const
{
    return existsFile(path) || existsDirectory(path);
}

uint64_t MetadataStorageFromPlainRewritableObjectStorage::getFileSize(const std::string & path) const
{
    if (auto file_size = getFileSizeIfExists(path))
        return file_size.value();

    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist", path);
}

std::optional<uint64_t> MetadataStorageFromPlainRewritableObjectStorage::getFileSizeIfExists(const std::string & path) const
{
    if (auto remote_info = fs_tree->getFileRemoteInfo(path))
        return remote_info->bytes_size;

    return std::nullopt;
}

std::vector<std::string> MetadataStorageFromPlainRewritableObjectStorage::listDirectory(const std::string & path) const
{
    return fs_tree->listDirectory(path);
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
    const auto file_info = fs_tree->getFileRemoteInfo(path);
    if (!file_info)
        return std::nullopt;

    const auto normalized_path = normalizePath(path);
    const auto directory_remote_info = fs_tree->getDirectoryRemoteInfo(normalized_path.parent_path());
    if (!directory_remote_info)
        return std::nullopt;

    const auto relative_key
        = resolvePlainRewritableRelativeObjectKey(*layout, *directory_remote_info, normalized_path.filename(), *file_info);
    auto object_key = layout->constructObjectKey(relative_key);
    return StoredObjects{StoredObject(object_key, path, file_info->bytes_size)};
}

Poco::Timestamp MetadataStorageFromPlainRewritableObjectStorage::getLastModified(const std::string & path) const
{
    if (auto last_modified = getLastModifiedIfExists(path))
        return last_modified.value();

    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File or directory {} does not exist", path);
}

std::optional<Poco::Timestamp> MetadataStorageFromPlainRewritableObjectStorage::getLastModifiedIfExists(const String & path) const
{
    if (auto [exists, remote_info] = fs_tree->existsDirectory(path); exists)
    {
        if (remote_info)
            return Poco::Timestamp::fromEpochTime(remote_info->last_modified);

        /// Let's return something in this case to unblock fs garbage cleanup.
        return Poco::Timestamp::fromEpochTime(0);
    }

    if (auto remote_info = fs_tree->getFileRemoteInfo(path))
        return Poco::Timestamp::fromEpochTime(remote_info->last_modified);

    return std::nullopt;
}

MetadataStorageFromPlainRewritableObjectStorageTransaction::MetadataStorageFromPlainRewritableObjectStorageTransaction(MetadataStorageFromPlainRewritableObjectStorage & metadata_storage_)
    : metadata_storage(metadata_storage_)
    , uncommitted_fs_tree(std::make_shared<InMemoryDirectoryTree>(CurrentMetrics::end(), CurrentMetrics::end()))
{
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::commit(const TransactionCommitOptionsVariant & options)
{
    if (!std::holds_alternative<NoCommitOptions>(options))
        throwNotImplemented();

    {
        std::unique_lock lock(metadata_storage.metadata_mutex);
        operations.commit();
    }

    operations.finalize();
}

TransactionCommitOutcomeVariant MetadataStorageFromPlainRewritableObjectStorageTransaction::tryCommit(const TransactionCommitOptionsVariant & options)
{
    if (!std::holds_alternative<NoCommitOptions>(options))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Metadata storage from disk supports only tryCommit without options");

    commit(NoCommitOptions{});
    return true;
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::createMetadataFile(const std::string & path, const StoredObjects & objects)
{
    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageWriteFileOperation>(
        path,
        objects.front(),
        metadata_storage.object_storage,
        metadata_storage.fs_tree,
        metadata_storage.layout,
        metadata_storage.metrics,
        metadata_storage.blob_refcounts));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::createDirectory(const std::string & path)
{
    auto normalized_path = normalizeDirectoryPath(path);
    if (normalized_path.empty())
    {
        LOG_TRACE(getLogger("MetadataStorageFromPlainRewritableObjectStorageTransaction"), "Skipping creation of a directory '{}' with an empty normalized path", path);
        return;
    }

    if (!uncommitted_fs_tree->getDirectoryRemoteInfo(path))
        uncommitted_fs_tree->recordDirectoryPath(path, DirectoryRemoteInfo{ .remote_path = getRandomASCIIString(32), .etag = "", .files = {}});

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageCreateDirectoryOperation>(
        /*recursive=*/false,
        std::move(normalized_path),
        uncommitted_fs_tree->getDirectoryRemoteInfo(path)->remote_path,
        metadata_storage.object_storage,
        metadata_storage.fs_tree,
        metadata_storage.layout,
        metadata_storage.metrics));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::createDirectoryRecursive(const std::string & path)
{
    auto normalized_path = normalizeDirectoryPath(path);
    if (normalized_path.empty())
    {
        LOG_TRACE(getLogger("MetadataStorageFromPlainRewritableObjectStorageTransaction"), "Skipping creation of a directory '{}' with an empty normalized path", path);
        return;
    }

    if (!uncommitted_fs_tree->getDirectoryRemoteInfo(path))
        uncommitted_fs_tree->recordDirectoryPath(path, DirectoryRemoteInfo{ .remote_path = getRandomASCIIString(32), .etag = "", .files = {}});

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageCreateDirectoryOperation>(
        /*recursive=*/true,
        std::move(normalized_path),
        uncommitted_fs_tree->getDirectoryRemoteInfo(path)->remote_path,
        metadata_storage.object_storage,
        metadata_storage.fs_tree,
        metadata_storage.layout,
        metadata_storage.metrics));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    if (uncommitted_fs_tree->existsDirectory(path_from).first)
        uncommitted_fs_tree->moveDirectory(path_from, path_to);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageMoveDirectoryOperation>(
        normalizeDirectoryPath(path_from),
        normalizeDirectoryPath(path_to),
        metadata_storage.object_storage,
        metadata_storage.fs_tree,
        metadata_storage.layout,
        metadata_storage.metrics));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::unlinkFile(const std::string & path, bool if_exists, bool /*should_remove_objects*/)
{
    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation>(
        path,
        if_exists,
        metadata_storage.object_storage,
        metadata_storage.fs_tree,
        metadata_storage.layout,
        metadata_storage.metrics,
        metadata_storage.blob_refcounts,
        removed_objects));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::removeDirectory(const std::string & path)
{
    if (!normalizePath(path).empty())
        if (uncommitted_fs_tree->existsDirectory(path).first)
            uncommitted_fs_tree->unlinkTree(path);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation>(
        normalizeDirectoryPath(path),
        metadata_storage.object_storage,
        metadata_storage.fs_tree,
        metadata_storage.layout,
        metadata_storage.metrics));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & /*should_remove_objects*/)
{
    if (!normalizePath(path).empty())
        if (uncommitted_fs_tree->existsDirectory(path).first)
            uncommitted_fs_tree->unlinkTree(path);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation>(
        path,
        metadata_storage.object_storage,
        metadata_storage.fs_tree,
        metadata_storage.layout,
        metadata_storage.metrics,
        metadata_storage.blob_refcounts,
        removed_objects));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::createHardLink(const std::string & path_from, const std::string & path_to)
{
    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageHardLinkOperation>(
        path_from,
        path_to,
        metadata_storage.object_storage,
        metadata_storage.fs_tree,
        metadata_storage.layout,
        metadata_storage.metrics,
        metadata_storage.blob_refcounts));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageMoveFileOperation>(
        /*replaceable=*/false,
        path_from,
        path_to,
        metadata_storage.object_storage,
        metadata_storage.fs_tree,
        metadata_storage.layout,
        metadata_storage.metrics,
        metadata_storage.blob_refcounts,
        removed_objects));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::replaceFile(const std::string & path_from, const std::string & path_to)
{
    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageMoveFileOperation>(
        /*replaceable=*/true,
        path_from,
        path_to,
        metadata_storage.object_storage,
        metadata_storage.fs_tree,
        metadata_storage.layout,
        metadata_storage.metrics,
        metadata_storage.blob_refcounts,
        removed_objects));
}

ObjectStorageKey MetadataStorageFromPlainRewritableObjectStorageTransaction::generateObjectKeyForPath(const std::string & path)
{
    const auto normalized_path = normalizePath(path);
    if (normalized_path.filename().empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File name is empty for path '{}'", path);

    /// Materialize virtual parent.
    const auto parent_path = normalized_path.parent_path();
    if (uncommitted_fs_tree->existsVirtualDirectory(parent_path) || metadata_storage.fs_tree->existsVirtualDirectory(parent_path))
        createDirectoryRecursive(parent_path);

    auto resolve_key = [&](const DirectoryRemoteInfo & directory_remote_info)
    {
        /// Explicit directories use random blob names to avoid clobbering hard-linked blobs after delete+recreate.
        if (directory_remote_info.explicit_files)
        {
            return ObjectStorageKey::createAsAbsolute(metadata_storage.layout->constructFileObjectKey(
                directory_remote_info.remote_path, getRandomASCIIString(32)));
        }
        return ObjectStorageKey::createAsAbsolute(
            metadata_storage.layout->constructFileObjectKey(directory_remote_info.remote_path, normalized_path.filename()));
    };

    if (const auto directory_remote_info = uncommitted_fs_tree->getDirectoryRemoteInfo(parent_path))
        return resolve_key(*directory_remote_info);

    if (const auto directory_remote_info = metadata_storage.fs_tree->getDirectoryRemoteInfo(parent_path))
        return resolve_key(*directory_remote_info);

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", parent_path.string());
}

StoredObjects MetadataStorageFromPlainRewritableObjectStorageTransaction::getSubmittedForRemovalBlobs()
{
    return removed_objects;
}

}
