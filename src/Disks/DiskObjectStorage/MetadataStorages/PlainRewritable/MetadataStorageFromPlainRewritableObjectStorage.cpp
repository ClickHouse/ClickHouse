#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsMetadata.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/PlainRewritableSnapshotFile.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/UncommittedState.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/Preconditions.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableMetrics.h>
#include <Disks/DiskObjectStorage/MetadataStorages/StaticDirectoryIterator.h>
#include <Disks/DiskObjectStorage/MetadataStorages/NormalizedPath.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIterator.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Disks/WriteMode.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>
#include <IO/ReadHelpers.h>
#include <IO/S3Common.h>
#include <IO/WriteBufferFromFileBase.h>
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
    extern const Event DiskPlainRewritableSnapshotRead;
    extern const Event DiskPlainRewritableSnapshotUnchanged;
    extern const Event DiskPlainRewritableSnapshotWritten;
    extern const Event DiskPlainRewritableSnapshotWriteFailed;
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

DirectoryRemoteInfo makeRootDirectoryInfo()
{
    return DirectoryRemoteInfo{PlainRewritableLayout::ROOT_DIRECTORY_TOKEN, "fake_etag", 0, {}};
}

/// Only the root directory without files.
bool isEmptyLayout(const PlainRewritableRemoteLayout & remote_layout)
{
    if (remote_layout.empty())
        return true;
    if (remote_layout.size() > 1)
        return false;
    const auto & [path, info] = *remote_layout.begin();
    return path.empty() && info.files.empty();
}

/// How soon a failed snapshot write is retried, unless the configured delay is larger.
constexpr UInt64 SNAPSHOT_WRITE_RETRY_DELAY_MS = 1000;

}

PlainRewritableRemoteLayout MetadataStorageFromPlainRewritableObjectStorage::getCurrentLayout() const
{
    PlainRewritableRemoteLayout result;
    for (auto & [path, info] : fs.takeReadOnlySnapshot()->getSubtreeRemoteInfo(""))
    {
        /// Virtual directories (created only as parents of the real ones) are not stored anywhere.
        if (info)
            result.emplace(path, std::move(*info));
    }
    return result;
}

bool MetadataStorageFromPlainRewritableObjectStorage::isSnapshotWriter() const
{
    return snapshot_settings.enabled && !object_storage->isReadOnly();
}

MetadataStorageFromPlainRewritableObjectStorage::SnapshotFileContents MetadataStorageFromPlainRewritableObjectStorage::tryReadSnapshotFile(
    const LoggerPtr & log, const std::optional<std::string> & skip_if_etag) const
{
    SnapshotFileContents result;
    const auto key = layout->constructSnapshotObjectKey();

    const auto metadata = object_storage->tryGetObjectMetadata(key, /*with_tags=*/ false);
    if (!metadata)
    {
        LOG_DEBUG(log, "There is no snapshot file '{}'", key);
        return result;
    }

    result.exists = true;
    result.etag = metadata->etag;

    /// An empty ETag cannot tell whether the file changed.
    if (skip_if_etag && !result.etag.empty() && result.etag == *skip_if_etag)
    {
        result.unchanged = true;
        return result;
    }

    try
    {
        auto read_settings = getReadSettings();
        read_settings.enable_filesystem_cache = false;

        auto in = object_storage->readObject(StoredObject(key, /*local_path*/ "", metadata->size_bytes), read_settings, metadata->size_bytes);
        result.layout = readPlainRewritableSnapshot(*in);
        LOG_DEBUG(log, "Read the snapshot file '{}' ({} bytes) with {} directories", key, metadata->size_bytes, result.layout->size());
    }
    catch (...)
    {
        /// The snapshot is only a copy of the state that can always be rebuilt from the object storage, so a file that
        /// cannot be read (e.g. written by a newer version in a newer format, or removed just now) does not make the disk unusable.
        tryLogCurrentException(log, fmt::format("Cannot read the snapshot file '{}', the state will be loaded by listing the object storage", key));
    }

    return result;
}

PlainRewritableRemoteLayout MetadataStorageFromPlainRewritableObjectStorage::listRemoteLayout(
    const PlainRewritableRemoteLayout * base, bool & differs_from_base, const LoggerPtr & log) const
{
    ThreadPool & pool = getIOThreadPool().get();

    auto settings = getReadSettings();
    settings.enable_filesystem_cache = false;
    settings.useForSmallRemoteRead(1024);  /// These files are small.

    /// This method can do both initial loading and incremental refresh of the metadata.
    ///
    /// We will list directories under __meta and compare it with the base (the current list in memory or the snapshot).
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

    std::mutex remote_layout_mutex;
    PlainRewritableRemoteLayout remote_layout;
    remote_layout[""] = makeRootDirectoryInfo();

    /// A directory whose `prefix.path` object has the same ETag as in the base has the same logical path, and its files
    /// are assumed to be the same as well (MergeTree does not modify the files of a part after it is written and renamed).
    /// Such directories are taken from the base without reading `prefix.path` and listing the files.
    std::unordered_map<std::string_view, const PlainRewritableRemoteLayout::value_type *> base_by_remote_path;
    if (base)
    {
        for (const auto & entry : *base)
        {
            if (!entry.first.empty())
                base_by_remote_path.emplace(entry.second.remote_path, &entry);
        }
    }
    size_t reused_directories = 0;

    ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::PLAIN_REWRITABLE_META_LOAD);
    try
    {
        /// Root folder is a special case. Files are stored as /__root/{file-name}.
        for (auto iterator = object_storage->iterate(layout->constructRootFilesDirectoryKey(), 0, /*with_tags=*/ false, std::nullopt); iterator->isValid(); iterator->next())
        {
            auto remote_file = iterator->current();
            remote_layout[""].files.emplace(remote_file->getFileName(), FileRemoteInfo{
                .bytes_size = remote_file->metadata->size_bytes,
                .last_modified = remote_file->metadata->last_modified.epochTime(),
            });
        }

        for (auto iterator = object_storage->iterate(layout->constructMetadataDirectoryKey(), 0, /*with_tags=*/ false, std::nullopt); iterator->isValid(); iterator->next())
        {
            const auto file = iterator->current();
            const auto remote_path = layout->parseDirectoryObjectKey(file->getPath());
            /// Not a `prefix.path`, e.g. the snapshot file.
            if (!remote_path.has_value())
                continue;

            if (const auto it = base_by_remote_path.find(remote_path.value()); it != base_by_remote_path.end() && it->second->second.etag == file->metadata->etag)
            {
                std::lock_guard guard(remote_layout_mutex);
                remote_layout[it->second->first] = it->second->second;
                ++reused_directories;
                continue;
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
                String local_path;
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
                        readStringUntilEOF(local_path, *read_buf);
                    }

                    /// Load the list of files inside the directory.
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
                        });
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
                remote_layout[local_path] = DirectoryRemoteInfo{remote_path.value(), metadata->etag, last_modified.epochTime(), std::move(files)};
            });
        }
    }
    catch (...)
    {
        runner.waitForAllToFinish();
        throw;
    }

    runner.waitForAllToFinishAndRethrowFirstError();

    /// Every directory of the base is still there unchanged, nothing was added, and the root files are the same.
    differs_from_base = !base
        || reused_directories != base_by_remote_path.size()
        || remote_layout.size() != reused_directories + 1
        || !base->contains("")
        || base->at("").files != remote_layout.at("").files;

    return remote_layout;
}

void MetadataStorageFromPlainRewritableObjectStorage::load(LoadMode mode)
{
    LoggerPtr log = getLogger("MetadataStorageFromPlainObjectStorage");
    LOG_DEBUG(log, "Loading metadata");

    /// The state is obtained either from the snapshot file (see `PlainRewritableSnapshotFile.h`) in a single request,
    /// or by listing the object storage (see `listRemoteLayout`), which is a request per directory.
    ///
    /// The snapshot can lag behind the actual state: it is written after the changes, and the server writing it could
    /// have crashed in between (or the write is delayed by `write_delay_ms`).
    /// - The disk that writes the data (and the snapshot) uses the snapshot only at startup, and reconciles it with the
    ///   listing of the `__meta` directory. This is cheap (a request per thousand directories) and detects the directories
    ///   that were created, removed or renamed after the snapshot was written; only those are loaded from the object storage.
    ///   Later reloads of the writer (`SYSTEM RESTART DISK`, `SYSTEM CLEAR DISK METADATA CACHE`) list the object storage:
    ///   the state in memory is authoritative and the snapshot is derived from it, so it cannot be a source of the state.
    /// - A read-only disk uses the snapshot both at startup and on the periodic refreshes, skipping the refresh when the
    ///   ETag of the file did not change, and lists the object storage only when there is no snapshot or when the cache
    ///   is dropped explicitly. The lag of the snapshot is acceptable for read-only replicas of MergeTree tables.

    const bool writer = isSnapshotWriter();
    const bool use_snapshot = snapshot_settings.enabled && mode != LoadMode::Full && (mode == LoadMode::Initial || !writer);

    bool snapshot_file_exists = false;
    if (use_snapshot)
    {
        auto snapshot = tryReadSnapshotFile(log, mode == LoadMode::Incremental ? std::make_optional(loaded_snapshot_etag) : std::nullopt);
        snapshot_file_exists = snapshot.exists;

        if (snapshot.unchanged)
        {
            ProfileEvents::increment(ProfileEvents::DiskPlainRewritableSnapshotUnchanged);
            LOG_DEBUG(log, "The snapshot file did not change, skipping the refresh");
            previous_refresh.restart();
            return;
        }

        if (snapshot.layout)
        {
            ProfileEvents::increment(ProfileEvents::DiskPlainRewritableSnapshotRead);

            if (writer)
            {
                bool differs = false;
                auto remote_layout = listRemoteLayout(&snapshot.layout.value(), differs, log);
                LOG_DEBUG(log, "Loaded metadata for {} directories from the snapshot file{}", remote_layout.size(), differs ? ", the object storage had changes after the snapshot" : "");
                fs.applyLayout(std::move(remote_layout));
                previous_refresh.restart();

                if (differs)
                    onLayoutChanged();
            }
            else
            {
                LOG_DEBUG(log, "Loaded metadata for {} directories from the snapshot file", snapshot.layout->size());
                loaded_snapshot_etag = snapshot.etag;
                fs.applyLayout(std::move(snapshot.layout.value()));
                previous_refresh.restart();
            }

            return;
        }
    }

    /// The state comes from the listing and is not attributed to a snapshot, so the next refresh reads the file.
    loaded_snapshot_etag.clear();

    if (mode == LoadMode::Initial)
    {
        bool has_metadata = object_storage->existsOrHasAnyChild(layout->constructMetadataDirectoryKey());

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
            PlainRewritableRemoteLayout remote_layout;
            remote_layout[""] = makeRootDirectoryInfo();
            fs.applyLayout(std::move(remote_layout));
            return;
        }
    }

    std::optional<PlainRewritableRemoteLayout> base;
    if (mode == LoadMode::Incremental)
        base = getCurrentLayout();

    bool differs = false;
    auto remote_layout = listRemoteLayout(base ? &base.value() : nullptr, differs, log);
    LOG_DEBUG(log, "Loaded metadata for {} directories", remote_layout.size());
    const bool empty = isEmptyLayout(remote_layout);
    fs.applyLayout(std::move(remote_layout));
    previous_refresh.restart();

    /// Publish the state for the next start and for the readers.
    /// A disk that never had anything gets no snapshot until the first change, to not write to a storage that is only looked at.
    if (writer && (mode != LoadMode::Initial || !empty || snapshot_file_exists))
        onLayoutChanged();
}

void MetadataStorageFromPlainRewritableObjectStorage::onLayoutChanged()
{
    if (!isSnapshotWriter())
        return;

    snapshot_dirty = true;

    if (snapshot_settings.write_delay_ms == 0)
        writeSnapshotIfDirty();
    else
        snapshot_write_task->scheduleAfter(snapshot_settings.write_delay_ms, /*overwrite=*/ false);
}

void MetadataStorageFromPlainRewritableObjectStorage::writeSnapshotIfDirty()
{
    std::lock_guard lock(snapshot_write_mutex);

    /// The flag is cleared before taking the state: a change applied after this point sets it again and is written next time.
    if (!snapshot_dirty.exchange(false))
        return;

    LoggerPtr log = getLogger("MetadataStorageFromPlainObjectStorage");
    const auto key = layout->constructSnapshotObjectKey();

    std::unique_ptr<WriteBufferFromFileBase> out;
    try
    {
        auto remote_layout = getCurrentLayout();
        if (isEmptyLayout(remote_layout))
        {
            /// Everything was removed from the disk: do not leave the snapshot behind.
            object_storage->removeObjectIfExists(StoredObject(key));
            LOG_DEBUG(log, "Removed the snapshot file '{}' as the disk is empty", key);
        }
        else
        {
            out = object_storage->writeObject(StoredObject(key), WriteMode::Rewrite, /*object_attributes*/ std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, getWriteSettings());
            writePlainRewritableSnapshot(remote_layout, *out);
            out->finalize();
            ProfileEvents::increment(ProfileEvents::DiskPlainRewritableSnapshotWritten);
            LOG_DEBUG(log, "Written the snapshot file '{}' ({} bytes) with {} directories", key, out->count(), remote_layout.size());
        }
    }
    catch (...)
    {
        if (out)
            out->cancel();

        /// The state in memory is correct, only its copy in the object storage is stale, so a transaction commit
        /// must not fail because of this. The write is retried later.
        snapshot_dirty = true;
        ProfileEvents::increment(ProfileEvents::DiskPlainRewritableSnapshotWriteFailed);
        tryLogCurrentException(log, fmt::format("Cannot write the snapshot file '{}', will retry", key));
        snapshot_write_task->scheduleAfter(std::max(snapshot_settings.write_delay_ms, SNAPSHOT_WRITE_RETRY_DELAY_MS), /*overwrite=*/ false);
    }
}

void MetadataStorageFromPlainRewritableObjectStorage::snapshotWriteTask()
{
    writeSnapshotIfDirty();

    /// The changes that happened during the write are written after the next delay.
    if (snapshot_dirty && snapshot_settings.write_delay_ms > 0)
        snapshot_write_task->scheduleAfter(snapshot_settings.write_delay_ms, /*overwrite=*/ false);
}

MetadataStorageFromPlainRewritableObjectStorage::MetadataStorageFromPlainRewritableObjectStorage(
    ObjectStoragePtr object_storage_, String storage_path_prefix_, PlainRewritableSnapshotSettings snapshot_settings_)
    : object_storage(std::move(object_storage_))
    , metrics(createPlainRewritableMetrics(object_storage->getType()))
    , storage_path_prefix(std::move(storage_path_prefix_))
    , storage_path_full(fs::path(object_storage->getRootPrefix()) / storage_path_prefix)
    , snapshot_settings(snapshot_settings_)
    , fs(metrics->directory_map_size, metrics->file_count)
    , layout(std::make_shared<PlainRewritableLayout>(object_storage->getCommonKeyPrefix()))
{
    if (isSnapshotWriter())
    {
        auto context = Context::getGlobalContextInstance();
        if (!context)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The global context is required to write the snapshots of the plain_rewritable metadata");

        snapshot_write_task = context->getSchedulePool()->createTask(StorageID::createEmpty(), "PlainRewritableSnapshotWriter", [this] { snapshotWriteTask(); });
    }

    std::lock_guard lock(load_mutex);
    load(LoadMode::Initial);
}

MetadataTransactionPtr MetadataStorageFromPlainRewritableObjectStorage::createTransaction()
{
    return std::make_shared<MetadataStorageFromPlainRewritableObjectStorageTransaction>(*this);
}

void MetadataStorageFromPlainRewritableObjectStorage::dropCache()
{
    std::unique_lock reload_lock(load_mutex);
    std::unique_lock tx_lock(metadata_mutex);
    load(LoadMode::Full);
}

void MetadataStorageFromPlainRewritableObjectStorage::refresh(UInt64 not_sooner_than_milliseconds)
{
    if (!previous_refresh.compareAndRestart(0.001 * static_cast<double>(not_sooner_than_milliseconds)))
        return;

    std::unique_lock load_lock(load_mutex, std::defer_lock);
    if (load_lock.try_lock())
    {
        std::unique_lock metadata_lock(metadata_mutex);
        load(LoadMode::Incremental);
    }
}

void MetadataStorageFromPlainRewritableObjectStorage::shutdown()
{
    if (!snapshot_write_task)
        return;

    /// Stop the background writes and write the latest state synchronously: the next start loads it.
    snapshot_write_task->deactivate();
    writeSnapshotIfDirty();
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

    const auto file_remote_info = tree->getFileRemoteInfo(path);
    if (!file_remote_info)
        return std::nullopt;

    const auto normalized_path = normalizePath(path);
    const auto directory_remote_info = tree->getDirectoryRemoteInfo(normalized_path.parent_path());
    if (!directory_remote_info)
        return std::nullopt;

    auto object_key = layout->constructFileObjectKey(directory_remote_info->remote_path, normalized_path.filename());
    return StoredObjects{StoredObject(object_key, path, file_remote_info->bytes_size)};
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
    , commit_snapshot(std::make_shared<FsSnapshot>())
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

    /// 4. Publish the new state in the snapshot file.
    metadata_storage.onLayoutChanged();
}

TransactionCommitOutcomeVariant MetadataStorageFromPlainRewritableObjectStorageTransaction::tryCommit(const TransactionCommitOptionsVariant & /*options*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Plain-Rewritable Metadata storage supports only commit");
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::createMetadataFile(const std::string & path, const StoredObjects & objects)
{
    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageWriteFileOperation>(
        path,
        objects.front(),
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics));
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
    uncommitted_state.useDirectory(normalizePath(path).parent_path());

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
    uncommitted_state.useDirectory(normalizePath(path_from).parent_path());
    uncommitted_state.useDirectory(normalizePath(path_to).parent_path());

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageCopyFileOperation>(
        path_from,
        path_to,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    uncommitted_state.useDirectory(normalizePath(path_from).parent_path());
    uncommitted_state.useDirectory(normalizePath(path_to).parent_path());

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
    uncommitted_state.useDirectory(normalizePath(path_from).parent_path());
    uncommitted_state.useDirectory(normalizePath(path_to).parent_path());

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
        return ObjectStorageKey::createAsAbsolute(metadata_storage.layout->constructFileObjectKey(directory_remote_info->remote_path, normalized_path.filename()));

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", parent_path.string());
}

StoredObjects MetadataStorageFromPlainRewritableObjectStorageTransaction::getSubmittedForRemovalBlobs()
{
    return removed_objects;
}

}
