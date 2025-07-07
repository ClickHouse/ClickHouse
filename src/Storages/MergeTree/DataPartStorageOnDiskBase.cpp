#include <string_view>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/GinIndexStore.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Disks/IDiskTransaction.h>
#include <Disks/TemporaryFileOnDisk.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Common/logger_useful.h>
#include <Common/formatReadable.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/Backup.h>
#include <Backups/BackupEntryFromImmutableFile.h>
#include <Backups/BackupEntryWrappedWith.h>
#include <Backups/BackupSettings.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int NOT_ENOUGH_SPACE;
    extern const int LOGICAL_ERROR;
    extern const int FILE_DOESNT_EXIST;
    extern const int CORRUPTED_DATA;
}

DataPartStorageOnDiskBase::DataPartStorageOnDiskBase(VolumePtr volume_, std::string root_path_, std::string part_dir_)
    : volume(std::move(volume_)), root_path(std::move(root_path_)), part_dir(std::move(part_dir_))
{
}

DataPartStorageOnDiskBase::DataPartStorageOnDiskBase(
    VolumePtr volume_, std::string root_path_, std::string part_dir_, DiskTransactionPtr transaction_)
    : volume(std::move(volume_))
    , root_path(std::move(root_path_))
    , part_dir(std::move(part_dir_))
    , transaction(std::move(transaction_))
    , has_shared_transaction(transaction != nullptr)
{
}

DiskPtr DataPartStorageOnDiskBase::getDisk() const
{
    return volume->getDisk();
}

std::string DataPartStorageOnDiskBase::getFullPath() const
{
    return fs::path(volume->getDisk()->getPath()) / root_path / part_dir / "";
}

std::string DataPartStorageOnDiskBase::getRelativePath() const
{
    return fs::path(root_path) / part_dir / "";
}

std::string DataPartStorageOnDiskBase::getParentDirectory() const
{
    /// Cut last "/" if it exists (it shouldn't). Otherwise fs::path behave differently.
    fs::path part_dir_without_slash = part_dir.ends_with("/") ? part_dir.substr(0, part_dir.size() - 1) : part_dir;

    if (part_dir_without_slash.has_parent_path())
        return part_dir_without_slash.parent_path();
    return "";
}

std::optional<String> DataPartStorageOnDiskBase::getRelativePathForPrefix(LoggerPtr log, const String & prefix, bool detached, bool broken) const
{
    assert(!broken || detached);
    String res;

    auto full_relative_path = fs::path(root_path);
    if (detached)
        full_relative_path /= MergeTreeData::DETACHED_DIR_NAME;

    std::optional<String> original_checksums_content;
    std::optional<Strings> original_files_list;

    for (int try_no = 0; try_no < 10; ++try_no)
    {
        res = getPartDirForPrefix(prefix, detached, try_no);

        if (!volume->getDisk()->existsDirectory(full_relative_path / res))
            return res;

        /// If part with compacted storage is broken then we probably
        /// cannot read the single file with data and check its content.
        if (broken
            && isFullPartStorage(*this)
            && looksLikeBrokenDetachedPartHasTheSameContent(res, original_checksums_content, original_files_list))
        {
            LOG_WARNING(log, "Directory {} (to detach to) already exists, "
                        "but its content looks similar to content of the broken part which we are going to detach. "
                        "Assuming it was already cloned to detached, will not do it again to avoid redundant copies of broken part.", res);
            return {};
        }

        LOG_WARNING(log, "Directory {} (to detach to) already exists. Will detach to directory with '_tryN' suffix.", res);
    }

    return res;
}

String DataPartStorageOnDiskBase::getPartDirForPrefix(const String & prefix, bool detached, int try_no) const
{
    /// This function joins `prefix` and the part name and an attempt number returning something like "<prefix>_<part_name>_<tryN>".
    String res = prefix;
    if (!prefix.empty() && !prefix.ends_with("_"))
        res += "_";

    /// During RESTORE temporary part directories are created with names like "tmp_restore_all_2_2_0-XXXXXXXX".
    /// To detach such a directory we need to rename it replacing "tmp_restore_" with a specified prefix,
    /// and a random suffix with an attempt number.
    String part_name;
    if (detached && part_dir.starts_with("tmp_restore_"))
    {
        part_name = part_dir.substr(strlen("tmp_restore_"));
        size_t endpos = part_name.find('-');
        if (endpos != String::npos)
            part_name.erase(endpos, String::npos);
    }

    if (!part_name.empty())
        res += part_name;
    else
        res += part_dir;

    if (try_no)
        res += "_try" + DB::toString(try_no);

    return res;
}

bool DataPartStorageOnDiskBase::looksLikeBrokenDetachedPartHasTheSameContent(const String & detached_part_path,
                                                                         std::optional<String> & original_checksums_content,
                                                                         std::optional<Strings> & original_files_list) const
{
    /// We cannot know for sure that content of detached part is the same,
    /// but in most cases it's enough to compare checksums.txt and list of files.

    if (!existsFile("checksums.txt"))
        return false;

    auto storage_from_detached = create(volume, fs::path(root_path) / MergeTreeData::DETACHED_DIR_NAME, detached_part_path, /*initialize=*/ true);
    if (!storage_from_detached->existsFile("checksums.txt"))
        return false;

    if (!original_checksums_content)
    {
        auto in = storage_from_detached->readFile("checksums.txt", /* settings */ {}, /* read_hint */ {}, /* file_size */ {});
        original_checksums_content.emplace();
        readStringUntilEOF(*original_checksums_content, *in);
    }

    if (original_checksums_content->empty())
        return false;

    String detached_checksums_content;
    {
        auto in = readFile("checksums.txt", /* settings */ {}, /* read_hint */ {}, /* file_size */ {});
        readStringUntilEOF(detached_checksums_content, *in);
    }

    if (original_checksums_content != detached_checksums_content)
        return false;

    if (!original_files_list)
    {
        original_files_list.emplace();
        for (auto it = iterate(); it->isValid(); it->next())
            original_files_list->emplace_back(it->name());
        std::sort(original_files_list->begin(), original_files_list->end());
    }

    Strings detached_files_list;
    for (auto it = storage_from_detached->iterate(); it->isValid(); it->next())
        detached_files_list.emplace_back(it->name());
    std::sort(detached_files_list.begin(), detached_files_list.end());

    return original_files_list == detached_files_list;
}

void DataPartStorageOnDiskBase::setRelativePath(const std::string & path)
{
    part_dir = path;
}

std::string DataPartStorageOnDiskBase::getPartDirectory() const
{
    return part_dir;
}

std::string DataPartStorageOnDiskBase::getFullRootPath() const
{
    return fs::path(volume->getDisk()->getPath()) / root_path / "";
}

Poco::Timestamp DataPartStorageOnDiskBase::getLastModified() const
{
    return volume->getDisk()->getLastModified(fs::path(root_path) / part_dir);
}

static UInt64 calculateTotalSizeOnDiskImpl(const DiskPtr & disk, const String & from)
{
    if (disk->existsFile(from))
        return disk->getFileSize(from);

    std::vector<std::string> files;
    disk->listFiles(from, files);

    UInt64 res = 0;
    for (const auto & file : files)
        res += calculateTotalSizeOnDiskImpl(disk, fs::path(from) / file);

    return res;
}

UInt64 DataPartStorageOnDiskBase::calculateTotalSizeOnDisk() const
{
    return calculateTotalSizeOnDiskImpl(volume->getDisk(), fs::path(root_path) / part_dir);
}

std::string DataPartStorageOnDiskBase::getDiskName() const
{
    return volume->getDisk()->getName();
}

std::string DataPartStorageOnDiskBase::getDiskType() const
{
    return volume->getDisk()->getDataSourceDescription().toString();
}

bool DataPartStorageOnDiskBase::isStoredOnRemoteDisk() const
{
    return volume->getDisk()->isRemote();
}

std::optional<String> DataPartStorageOnDiskBase::getCacheName() const
{
    if (volume->getDisk()->supportsCache())
        return volume->getDisk()->getCacheName();
    return std::nullopt;
}

bool DataPartStorageOnDiskBase::supportZeroCopyReplication() const
{
    return volume->getDisk()->supportZeroCopyReplication();
}

bool DataPartStorageOnDiskBase::supportParallelWrite() const
{
    return volume->getDisk()->supportParallelWrite();
}

bool DataPartStorageOnDiskBase::isBroken() const
{
    return volume->getDisk()->isBroken();
}

bool DataPartStorageOnDiskBase::isReadonly() const
{
    return volume->getDisk()->isReadOnly() || volume->getDisk()->isWriteOnce();
}

std::string DataPartStorageOnDiskBase::getDiskPath() const
{
    return volume->getDisk()->getPath();
}

ReservationPtr DataPartStorageOnDiskBase::reserve(UInt64 bytes) const
{
    auto res = volume->reserve(bytes);
    if (!res)
        throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "Cannot reserve {}, not enough space", ReadableSize(bytes));

    return res;
}

ReservationPtr DataPartStorageOnDiskBase::tryReserve(UInt64 bytes) const
{
    return volume->reserve(bytes);
}

IDataPartStorage::ReplicatedFilesDescription
DataPartStorageOnDiskBase::getReplicatedFilesDescription(const NameSet & file_names) const
{
    ReplicatedFilesDescription description;
    auto relative_path = fs::path(root_path) / part_dir;
    auto disk = volume->getDisk();
    auto read_settings = getReadSettings();

    auto actual_file_names = getActualFileNamesOnDisk(file_names);
    for (const auto & name : actual_file_names)
    {
        auto path = relative_path / name;
        size_t file_size = disk->getFileSize(path);

        auto & file_desc = description.files[name];

        file_desc.file_size = file_size;
        file_desc.input_buffer_getter = [disk, path, file_size, read_settings]
        {
            return disk->readFile(path, read_settings.adjustBufferSize(file_size), file_size, file_size);
        };
    }

    return description;
}

IDataPartStorage::ReplicatedFilesDescription
DataPartStorageOnDiskBase::getReplicatedFilesDescriptionForRemoteDisk(const NameSet & file_names) const
{
    ReplicatedFilesDescription description;
    auto relative_path = fs::path(root_path) / part_dir;

    auto disk = volume->getDisk();
    if (!disk->supportZeroCopyReplication())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Disk {} doesn't support zero-copy replication", disk->getName());

    description.unique_id = getUniqueId();

    Names paths;
    auto actual_file_names = getActualFileNamesOnDisk(file_names);

    for (const auto & name : actual_file_names)
    {
        /// Just some additional checks
        auto metadata_full_file_path = fs::path(getFullPath()) / name;
        if (!fs::exists(metadata_full_file_path))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Remote metadata '{}' is not exists", name);
        if (!fs::is_regular_file(metadata_full_file_path))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Remote metadata '{}' is not a file", name);

        paths.emplace_back(relative_path / name);
    }

    auto serialized_metadata = disk->getSerializedMetadata(paths);
    for (const auto & name : actual_file_names)
    {
        auto & file_desc = description.files[name];
        const auto & metadata_str = serialized_metadata.at(relative_path / name);

        file_desc.file_size = metadata_str.size();
        file_desc.input_buffer_getter = [metadata_str]
        {
            return std::make_unique<ReadBufferFromString>(metadata_str);
        };
    }

    return description;
}

void DataPartStorageOnDiskBase::backup(
    const MergeTreeDataPartChecksums & checksums,
    const NameSet & files_without_checksums,
    const String & path_in_backup,
    const BackupSettings & backup_settings,
    bool make_temporary_hard_links,
    BackupEntries & backup_entries,
    TemporaryFilesOnDisks * temp_dirs,
    bool is_projection_part,
    bool allow_backup_broken_projection) const
{
    fs::path part_path_on_disk = fs::path{root_path} / part_dir;
    fs::path part_path_in_backup = fs::path{path_in_backup} / part_dir;

    auto disk = volume->getDisk();

    fs::path temp_part_dir;
    std::shared_ptr<TemporaryFileOnDisk> temp_dir_owner;
    if (make_temporary_hard_links)
    {
        assert(temp_dirs);
        auto temp_dir_it = temp_dirs->find(disk);
        if (temp_dir_it == temp_dirs->end())
            temp_dir_it = temp_dirs->emplace(disk, std::make_shared<TemporaryFileOnDisk>(disk, "tmp/")).first;

        temp_dir_owner = temp_dir_it->second;
        fs::path temp_dir = temp_dir_owner->getRelativePath();
        temp_part_dir = temp_dir / part_path_in_backup.relative_path();
        disk->createDirectories(temp_part_dir);
    }

    /// For example,
    /// part_path_in_backup = /data/test/table/0_1_1_0
    /// part_path_on_disk = store/f57/f5728353-44bb-4575-85e8-28deb893657a/0_1_1_0
    /// tmp_part_dir = tmp/1aaaaaa/data/test/table/0_1_1_0
    /// Or, for projections:
    /// part_path_in_backup = /data/test/table/0_1_1_0/prjmax.proj
    /// part_path_on_disk = store/f57/f5728353-44bb-4575-85e8-28deb893657a/0_1_1_0/prjmax.proj
    /// tmp_part_dir = tmp/1aaaaaa/data/test/table/0_1_1_0/prjmax.proj

    auto files_to_backup = files_without_checksums;
    for (const auto & [name, _] : checksums.files)
    {
        if (!name.ends_with(".proj"))
            files_to_backup.insert(name);
    }

    files_to_backup = getActualFileNamesOnDisk(files_to_backup);

    bool copy_encrypted = !backup_settings.decrypt_files_from_encrypted_disks;
    bool allow_checksums_from_remote_paths = backup_settings.allow_checksums_from_remote_paths;

    auto backup_file = [&](const String & filepath)
    {
        auto filepath_on_disk = part_path_on_disk / filepath;
        auto filepath_in_backup = part_path_in_backup / filepath;

        if (is_projection_part && allow_backup_broken_projection && !disk->existsFile(filepath_on_disk))
            return;

        if (make_temporary_hard_links)
        {
            String hardlink_filepath = temp_part_dir / filepath;
            disk->createHardLink(filepath_on_disk, hardlink_filepath);
            filepath_on_disk = hardlink_filepath;
        }

        std::optional<UInt64> file_size;
        std::optional<UInt128> file_hash;

        auto it = checksums.files.find(filepath);
        if (it != checksums.files.end())
        {
            file_size = it->second.file_size;
            file_hash = it->second.file_hash;
        }

        BackupEntryPtr backup_entry = std::make_unique<BackupEntryFromImmutableFile>(
            disk, filepath_on_disk, copy_encrypted, file_size, file_hash, allow_checksums_from_remote_paths);

        if (temp_dir_owner)
            backup_entry = wrapBackupEntryWith(std::move(backup_entry), temp_dir_owner);

        backup_entries.emplace_back(filepath_in_backup, std::move(backup_entry));
    };

    auto * log = &Poco::Logger::get("DataPartStorageOnDiskBase::backup");

    for (const auto & filepath : files_to_backup)
    {
        if (is_projection_part && allow_backup_broken_projection)
        {
            try
            {
                backup_file(filepath);
            }
            catch (Exception & e)
            {
                if (e.code() != ErrorCodes::FILE_DOESNT_EXIST)
                    throw;

                LOG_ERROR(log, "Cannot backup file {} of projection part {}. Will try to ignore it", filepath, part_dir);
                continue;
            }
        }
        else
        {
            backup_file(filepath);
        }
    }
}

MutableDataPartStoragePtr DataPartStorageOnDiskBase::freeze(
    const std::string & to,
    const std::string & dir_path,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    std::function<void(const DiskPtr &)> save_metadata_callback,
    const ClonePartParams & params) const
{
    auto disk = volume->getDisk();
    if (params.external_transaction)
        params.external_transaction->createDirectories(to);
    else
        disk->createDirectories(to);

    Backup(
        disk,
        disk,
        getRelativePath(),
        fs::path(to) / dir_path,
        read_settings,
        write_settings,
        params.make_source_readonly,
        /* max_level= */ {},
        params.copy_instead_of_hardlink,
        params.files_to_copy_instead_of_hardlinks,
        params.external_transaction);

    if (save_metadata_callback)
        save_metadata_callback(disk);

    if (params.external_transaction)
    {
        params.external_transaction->removeFileIfExists(fs::path(to) / dir_path / "delete-on-destroy.txt");
        params.external_transaction->removeFileIfExists(fs::path(to) / dir_path / IMergeTreeDataPart::TXN_VERSION_METADATA_FILE_NAME);
        if (!params.keep_metadata_version)
            params.external_transaction->removeFileIfExists(fs::path(to) / dir_path / IMergeTreeDataPart::METADATA_VERSION_FILE_NAME);
    }
    else
    {
        disk->removeFileIfExists(fs::path(to) / dir_path / "delete-on-destroy.txt");
        disk->removeFileIfExists(fs::path(to) / dir_path / IMergeTreeDataPart::TXN_VERSION_METADATA_FILE_NAME);
        if (!params.keep_metadata_version)
            disk->removeFileIfExists(fs::path(to) / dir_path / IMergeTreeDataPart::METADATA_VERSION_FILE_NAME);
    }

    auto single_disk_volume = std::make_shared<SingleDiskVolume>(disk->getName(), disk, 0);

    /// Do not initialize storage in case of DETACH because part may be broken.
    bool to_detached = dir_path.starts_with(std::string_view((fs::path(MergeTreeData::DETACHED_DIR_NAME) / "").string()));
    return create(single_disk_volume, to, dir_path, /*initialize=*/ !to_detached && !params.external_transaction);
}

MutableDataPartStoragePtr DataPartStorageOnDiskBase::freezeRemote(
    const std::string & to,
    const std::string & dir_path,
    const DiskPtr & dst_disk,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    std::function<void(const DiskPtr &)> save_metadata_callback,
    const ClonePartParams & params) const
{
    auto src_disk = volume->getDisk();
    if (params.external_transaction)
        params.external_transaction->createDirectories(to);
    else
        dst_disk->createDirectories(to);

    /// freezeRemote() using copy instead of hardlinks for all files
    /// In this case, files_to_copy_intead_of_hardlinks is set by empty
    Backup(
        src_disk,
        dst_disk,
        getRelativePath(),
        fs::path(to) / dir_path,
        read_settings,
        write_settings,
        params.make_source_readonly,
        /* max_level= */ {},
        true,
        /* files_to_copy_intead_of_hardlinks= */ {},
        params.external_transaction);

    /// The save_metadata_callback function acts on the target dist.
    if (save_metadata_callback)
        save_metadata_callback(dst_disk);

    if (params.external_transaction)
    {
        params.external_transaction->removeFileIfExists(fs::path(to) / dir_path / "delete-on-destroy.txt");
        params.external_transaction->removeFileIfExists(fs::path(to) / dir_path / IMergeTreeDataPart::TXN_VERSION_METADATA_FILE_NAME);
        if (!params.keep_metadata_version)
            params.external_transaction->removeFileIfExists(fs::path(to) / dir_path / IMergeTreeDataPart::METADATA_VERSION_FILE_NAME);
    }
    else
    {
        dst_disk->removeFileIfExists(fs::path(to) / dir_path / "delete-on-destroy.txt");
        dst_disk->removeFileIfExists(fs::path(to) / dir_path / IMergeTreeDataPart::TXN_VERSION_METADATA_FILE_NAME);
        if (!params.keep_metadata_version)
            dst_disk->removeFileIfExists(fs::path(to) / dir_path / IMergeTreeDataPart::METADATA_VERSION_FILE_NAME);
    }

    auto single_disk_volume = std::make_shared<SingleDiskVolume>(dst_disk->getName(), dst_disk, 0);

    /// Do not initialize storage in case of DETACH because part may be broken.
    bool to_detached = dir_path.starts_with(std::string_view((fs::path(MergeTreeData::DETACHED_DIR_NAME) / "").string()));
    return create(single_disk_volume, to, dir_path, /*initialize=*/ !to_detached && !params.external_transaction);
}

MutableDataPartStoragePtr DataPartStorageOnDiskBase::clonePart(
    const std::string & to,
    const std::string & dir_path,
    const DiskPtr & dst_disk,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    LoggerPtr log,
    const std::function<void()> & cancellation_hook) const
{
    String path_to_clone = fs::path(to) / dir_path / "";
    auto src_disk = volume->getDisk();

    if (dst_disk->existsDirectory(path_to_clone))
    {
        throw Exception(ErrorCodes::DIRECTORY_ALREADY_EXISTS,
                        "Cannot clone part {} from '{}' to '{}': path '{}' already exists",
                        dir_path, getRelativePath(), path_to_clone, fullPath(dst_disk, path_to_clone));
    }

    try
    {
        dst_disk->createDirectories(to);
        src_disk->copyDirectoryContent(getRelativePath(), dst_disk, path_to_clone, read_settings, write_settings, cancellation_hook);
    }
    catch (...)
    {
        /// It's safe to remove it recursively (even with zero-copy-replication)
        /// because we've just did full copy through copyDirectoryContent
        LOG_WARNING(log, "Removing directory {} after failed attempt to move a data part", path_to_clone);
        dst_disk->removeRecursive(path_to_clone);
        throw;
    }

    auto single_disk_volume = std::make_shared<SingleDiskVolume>(dst_disk->getName(), dst_disk, 0);
    return create(single_disk_volume, to, dir_path, /*initialize=*/ true);
}

void DataPartStorageOnDiskBase::rename(
    std::string new_root_path,
    std::string new_part_dir,
    LoggerPtr log,
    bool remove_new_dir_if_exists,
    bool fsync_part_dir)
{
    if (new_root_path.ends_with('/'))
        new_root_path.pop_back();
    if (new_part_dir.ends_with('/'))
        new_part_dir.pop_back();

    String to = fs::path(new_root_path) / new_part_dir / "";

    if (volume->getDisk()->existsDirectory(to))
    {
        /// FIXME it should be logical error
        if (remove_new_dir_if_exists)
        {
            Names files;
            volume->getDisk()->listFiles(to, files);

            if (log)
                LOG_WARNING(log,
                    "Part directory {} already exists and contains {} files. Removing it.",
                    fullPath(volume->getDisk(), to), files.size());

            /// Do not remove blobs if they exist
            executeWriteOperation([&](auto & disk) { disk.removeSharedRecursive(to, true, {}); });
        }
        else
        {
            throw Exception(
                ErrorCodes::DIRECTORY_ALREADY_EXISTS,
                "Part directory {} already exists",
                fullPath(volume->getDisk(), to));
        }
    }

    String from = getRelativePath();

    /// Why?
    executeWriteOperation([&](auto & disk)
    {
        disk.setLastModified(from, Poco::Timestamp::fromEpochTime(time(nullptr)));
        disk.moveDirectory(from, to);

        /// Only after moveDirectory() since before the directory does not exist.
        SyncGuardPtr to_sync_guard;
        if (fsync_part_dir)
            to_sync_guard = volume->getDisk()->getDirectorySyncGuard(to);
    });

    part_dir = new_part_dir;
    root_path = new_root_path;
}

void DataPartStorageOnDiskBase::remove(
    CanRemoveCallback && can_remove_callback,
    const MergeTreeDataPartChecksums & checksums,
    std::list<ProjectionChecksums> projections,
    bool is_temp,
    LoggerPtr log)
{
    /// NOTE We rename part to delete_tmp_<relative_path> instead of delete_tmp_<name> to avoid race condition
    /// when we try to remove two parts with the same name, but different relative paths,
    /// for example all_1_2_1 (in Deleting state) and tmp_merge_all_1_2_1 (in Temporary state).
    fs::path from = fs::path(root_path) / part_dir;
    // TODO directory delete_tmp_<name> is never removed if server crashes before returning from this function

    /// Cut last "/" if it exists (it shouldn't). Otherwise fs::path behave differently.
    fs::path part_dir_without_slash = part_dir.ends_with("/") ? part_dir.substr(0, part_dir.size() - 1) : part_dir;

    /// NOTE relative_path can contain not only part name itself, but also some prefix like
    /// "moving/all_1_1_1" or "detached/all_2_3_5". We should handle this case more properly.

    /// File might be already renamed on previous try
    bool has_delete_prefix = part_dir_without_slash.filename().string().starts_with("delete_tmp_");
    std::optional<CanRemoveDescription> can_remove_description;
    auto disk = volume->getDisk();
    fs::path to = fs::path(root_path) / part_dir_without_slash;

    if (!has_delete_prefix)
    {
        auto parent_path = getParentDirectory();
        if (!parent_path.empty())
        {
            if (parent_path == MergeTreeData::DETACHED_DIR_NAME)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Trying to remove detached part {} with path {} in remove function. It shouldn't happen",
                    part_dir,
                    root_path);

            part_dir_without_slash = fs::path(parent_path) / ("delete_tmp_" + std::string{part_dir_without_slash.filename()});
        }
        else
        {
            part_dir_without_slash = ("delete_tmp_" + std::string{part_dir_without_slash.filename()});
        }

        to = fs::path(root_path) / part_dir_without_slash;

        if (disk->existsDirectory(to))
        {
            LOG_WARNING(log, "Directory {} (to which part must be renamed before removing) already exists. "
                        "Most likely this is due to unclean restart or race condition. Removing it.", fullPath(disk, to));
            try
            {
                can_remove_description.emplace(can_remove_callback());
                disk->removeSharedRecursive(
                    fs::path(to) / "", !can_remove_description->can_remove_anything, can_remove_description->files_not_to_remove);
            }
            catch (...)
            {
                LOG_ERROR(
                    log, "Cannot recursively remove directory {}. Exception: {}", fullPath(disk, to), getCurrentExceptionMessage(false));
                throw;
            }
        }

        if (!disk->existsDirectory(from))
        {
            LOG_ERROR(log, "Directory {} (part to remove) doesn't exist or one of nested files has gone. Most likely this is due to manual removing. This should be discouraged. Ignoring.", fullPath(disk, from));
            /// We will never touch this part again, so unlocking it from zero-copy
            if (!can_remove_description)
                can_remove_description.emplace(can_remove_callback());
            return;
        }

        try
        {
            disk->moveDirectory(from, to);
            part_dir = part_dir_without_slash;
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::FILE_DOESNT_EXIST)
            {
                LOG_ERROR(log, "Directory {} (part to remove) doesn't exist or one of nested files has gone. Most likely this is due to manual removing. This should be discouraged. Ignoring.", fullPath(disk, from));
                /// We will never touch this part again, so unlocking it from zero-copy
                if (!can_remove_description)
                    can_remove_description.emplace(can_remove_callback());
                return;
            }
            throw;
        }
        catch (const fs::filesystem_error & e)
        {
            if (e.code() == std::errc::no_such_file_or_directory)
            {
                LOG_ERROR(log, "Directory {} (part to remove) doesn't exist or one of nested files has gone. "
                          "Most likely this is due to manual removing. This should be discouraged. Ignoring.", fullPath(disk, from));
                /// We will never touch this part again, so unlocking it from zero-copy
                if (!can_remove_description)
                    can_remove_description.emplace(can_remove_callback());

                return;
            }
            throw;
        }
    }

    if (!can_remove_description)
        can_remove_description.emplace(can_remove_callback());

    // Record existing projection directories so we don't remove them twice
    std::unordered_set<String> projection_directories;
    std::string proj_suffix = ".proj";
    for (const auto & projection : projections)
    {
        std::string proj_dir_name = projection.name + proj_suffix;
        projection_directories.emplace(proj_dir_name);

        NameSet files_not_to_remove_for_projection;
        for (const auto & file_name : can_remove_description->files_not_to_remove)
            if (file_name.starts_with(proj_dir_name))
                files_not_to_remove_for_projection.emplace(fs::path(file_name).filename());

        if (!files_not_to_remove_for_projection.empty())
            LOG_DEBUG(
                log, "Will not remove files [{}] for projection {}", fmt::join(files_not_to_remove_for_projection, ", "), projection.name);

        CanRemoveDescription proj_description
        {
            can_remove_description->can_remove_anything,
            std::move(files_not_to_remove_for_projection),
        };

        clearDirectory(fs::path(to) / proj_dir_name, proj_description, projection.checksums, is_temp, log);
    }

    /// It is possible that we are removing the part which have a written but not loaded projection.
    /// Such a part can appear server was restarted after DROP PROJECTION but before old part was removed.
    /// In this case, the old part will load only projections from metadata.
    /// See test 01701_clear_projection_and_part.
    for (const auto & [name, _] : checksums.files)
    {
        if (endsWith(name, proj_suffix) && !projection_directories.contains(name))
        {
            static constexpr auto checksums_name = "checksums.txt";
            auto projection_storage = create(volume, to, name, /*initialize=*/ true);

            /// If we have a directory with suffix '.proj' it is likely a projection.
            /// Try to load checksums for it (to avoid recursive removing fallback).
            if (projection_storage->existsFile(checksums_name))
            {
                try
                {
                    MergeTreeDataPartChecksums tmp_checksums;
                    auto in = projection_storage->readFile(checksums_name, {}, {}, {});
                    tmp_checksums.read(*in);

                    clearDirectory(fs::path(to) / name, *can_remove_description, tmp_checksums, is_temp, log);
                }
                catch (...)
                {
                    LOG_ERROR(log, "Cannot load checksums from {}", fs::path(projection_storage->getRelativePath()) / checksums_name);
                }
            }
        }
    }

    clearDirectory(to, *can_remove_description, checksums, is_temp, log);
}

void DataPartStorageOnDiskBase::clearDirectory(
    const std::string & dir,
    const CanRemoveDescription & can_remove_description,
    const MergeTreeDataPartChecksums & checksums,
    bool is_temp,
    LoggerPtr log)
{
    auto disk = volume->getDisk();
    auto [can_remove_shared_data, names_not_to_remove] = can_remove_description;
    names_not_to_remove = getActualFileNamesOnDisk(names_not_to_remove);

    /// It does not make sense to try fast path for incomplete temporary parts, because some files are probably absent.
    /// Sometimes we add something to checksums.files before actually writing checksums and columns on disk.
    /// Also sometimes we write checksums.txt and columns.txt in arbitrary order, so this check becomes complex...
    bool incomplete_temporary_part = is_temp && (!disk->existsFile(fs::path(dir) / "checksums.txt") || !disk->existsFile(fs::path(dir) / "columns.txt"));
    if (checksums.empty() || incomplete_temporary_part)
    {
        /// If the part is not completely written, we cannot use fast path by listing files.
        disk->removeSharedRecursive(fs::path(dir) / "", !can_remove_shared_data, names_not_to_remove);
        return;
    }

    try
    {
        NameSet names_to_remove = {"checksums.txt", "columns.txt"};
        for (const auto & [file, _] : checksums.files)
            if (!endsWith(file, ".proj"))
                names_to_remove.emplace(file);

        names_to_remove = getActualFileNamesOnDisk(names_to_remove);

        /// Remove each expected file in directory, then remove directory itself.
        RemoveBatchRequest request;
        for (const auto & file : names_to_remove)
        {
            if (isGinFile(file) && (!disk->existsFile(fs::path(dir) / file)))
                continue;

            request.emplace_back(fs::path(dir) / file);
        }
        request.emplace_back(fs::path(dir) / "default_compression_codec.txt", true);
        request.emplace_back(fs::path(dir) / "delete-on-destroy.txt", true);
        request.emplace_back(fs::path(dir) / IMergeTreeDataPart::TXN_VERSION_METADATA_FILE_NAME, true);
        request.emplace_back(fs::path(dir) / "metadata_version.txt", true);
        request.emplace_back(fs::path(dir) / "columns_substreams.txt", true);

        disk->removeSharedFiles(request, !can_remove_shared_data, names_not_to_remove);
        disk->removeDirectory(dir);
    }
    catch (...)
    {
        /// Recursive directory removal does many excessive "stat" syscalls under the hood.

        LOG_ERROR(log, "Cannot quickly remove directory {} by removing files; fallback to recursive removal. Reason: {}", fullPath(disk, dir), getCurrentExceptionMessage(false));
        disk->removeSharedRecursive(fs::path(dir) / "", !can_remove_shared_data, names_not_to_remove);
    }
}

void DataPartStorageOnDiskBase::changeRootPath(const std::string & from_root, const std::string & to_root)
{
    /// This is a very dumb implementation, here for root path like
    /// "some/current/path/to/part" and change like
    /// "some/current" -> "other/different", we just replace prefix to make new root like
    /// "other/different/path/to/part".
    /// Here we expect that actual move was done by somebody else.

    size_t prefix_size = from_root.size();
    if (prefix_size > 0 && from_root.back() == '/')
        --prefix_size;

    if (prefix_size > root_path.size()
        || std::string_view(from_root).substr(0, prefix_size) != std::string_view(root_path).substr(0, prefix_size))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot change part root to {} because it is not a prefix of current root {}",
            from_root, root_path);

    size_t dst_size = to_root.size();
    if (dst_size > 0 && to_root.back() == '/')
        --dst_size;

    root_path = to_root.substr(0, dst_size) + root_path.substr(prefix_size);
}

SyncGuardPtr DataPartStorageOnDiskBase::getDirectorySyncGuard() const
{
    return volume->getDisk()->getDirectorySyncGuard(fs::path(root_path) / part_dir);
}

std::unique_ptr<WriteBufferFromFileBase> DataPartStorageOnDiskBase::writeTransactionFile(WriteMode mode) const
{
    return volume->getDisk()->writeFile(fs::path(root_path) / part_dir / IMergeTreeDataPart::TXN_VERSION_METADATA_FILE_NAME, 256, mode);
}

void DataPartStorageOnDiskBase::removeRecursive()
{
    executeWriteOperation([&](auto & disk) { disk.removeRecursive(fs::path(root_path) / part_dir); });
}

void DataPartStorageOnDiskBase::removeSharedRecursive(bool keep_in_remote_fs)
{
    executeWriteOperation([&](auto & disk) { disk.removeSharedRecursive(fs::path(root_path) / part_dir, keep_in_remote_fs, {}); });
}

void DataPartStorageOnDiskBase::createDirectories()
{
    executeWriteOperation([&](auto & disk) { disk.createDirectories(fs::path(root_path) / part_dir); });
}

bool DataPartStorageOnDiskBase::hasActiveTransaction() const
{
    return transaction != nullptr;
}

}
