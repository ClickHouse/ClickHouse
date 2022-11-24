#include <Storages/MergeTree/DataPartStorageOnDisk.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Disks/IVolume.h>
#include <Disks/TemporaryFileOnDisk.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <Common/logger_useful.h>
#include <Backups/BackupEntryFromSmallFile.h>
#include <Backups/BackupEntryFromImmutableFile.h>
#include <Storages/MergeTree/localBackup.h>
#include <Disks/SingleDiskVolume.h>
#include <Interpreters/TransactionVersionMetadata.h>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int NOT_ENOUGH_SPACE;
    extern const int LOGICAL_ERROR;
    extern const int FILE_DOESNT_EXIST;
}

DataPartStorageOnDisk::DataPartStorageOnDisk(VolumePtr volume_, std::string root_path_, std::string part_dir_)
    : volume(std::move(volume_)), root_path(std::move(root_path_)), part_dir(std::move(part_dir_))
{
}

DataPartStorageOnDisk::DataPartStorageOnDisk(
    VolumePtr volume_, std::string root_path_, std::string part_dir_, DiskTransactionPtr transaction_)
    : volume(std::move(volume_))
    , root_path(std::move(root_path_))
    , part_dir(std::move(part_dir_))
    , transaction(std::move(transaction_))
    , has_shared_transaction(transaction != nullptr)
{
}

std::string DataPartStorageOnDisk::getFullPath() const
{
    return fs::path(volume->getDisk()->getPath()) / root_path / part_dir / "";
}

std::string DataPartStorageOnDisk::getRelativePath() const
{
    return fs::path(root_path) / part_dir / "";
}

void DataPartStorageOnDisk::setRelativePath(const std::string & path)
{
    part_dir = path;
}

std::string DataPartStorageOnDisk::getFullRootPath() const
{
    return fs::path(volume->getDisk()->getPath()) / root_path / "";
}

MutableDataPartStoragePtr DataPartStorageOnDisk::getProjection(const std::string & name)
{
    return std::shared_ptr<DataPartStorageOnDisk>(new DataPartStorageOnDisk(volume, std::string(fs::path(root_path) / part_dir), name, transaction));
}

DataPartStoragePtr DataPartStorageOnDisk::getProjection(const std::string & name) const
{
    return std::make_shared<DataPartStorageOnDisk>(volume, std::string(fs::path(root_path) / part_dir), name);
}

bool DataPartStorageOnDisk::exists() const
{
    return volume->getDisk()->exists(fs::path(root_path) / part_dir);
}

bool DataPartStorageOnDisk::exists(const std::string & name) const
{
    return volume->getDisk()->exists(fs::path(root_path) / part_dir / name);
}

bool DataPartStorageOnDisk::isDirectory(const std::string & name) const
{
    return volume->getDisk()->isDirectory(fs::path(root_path) / part_dir / name);
}

Poco::Timestamp DataPartStorageOnDisk::getLastModified() const
{
    return volume->getDisk()->getLastModified(fs::path(root_path) / part_dir);
}

class DataPartStorageIteratorOnDisk final : public IDataPartStorageIterator
{
public:
    DataPartStorageIteratorOnDisk(DiskPtr disk_, DirectoryIteratorPtr it_)
        : disk(std::move(disk_)), it(std::move(it_))
    {
    }

    void next() override { it->next(); }
    bool isValid() const override { return it->isValid(); }
    bool isFile() const override { return isValid() && disk->isFile(it->path()); }
    std::string name() const override { return it->name(); }

private:
    DiskPtr disk;
    DirectoryIteratorPtr it;
};

DataPartStorageIteratorPtr DataPartStorageOnDisk::iterate() const
{
    return std::make_unique<DataPartStorageIteratorOnDisk>(
        volume->getDisk(),
        volume->getDisk()->iterateDirectory(fs::path(root_path) / part_dir));
}

size_t DataPartStorageOnDisk::getFileSize(const String & file_name) const
{
    return volume->getDisk()->getFileSize(fs::path(root_path) / part_dir / file_name);
}

UInt32 DataPartStorageOnDisk::getRefCount(const String & file_name) const
{
    return volume->getDisk()->getRefCount(fs::path(root_path) / part_dir / file_name);
}

static UInt64 calculateTotalSizeOnDiskImpl(const DiskPtr & disk, const String & from)
{
    if (disk->isFile(from))
        return disk->getFileSize(from);

    std::vector<std::string> files;
    disk->listFiles(from, files);
    UInt64 res = 0;
    for (const auto & file : files)
        res += calculateTotalSizeOnDiskImpl(disk, fs::path(from) / file);
    return res;
}

UInt64 DataPartStorageOnDisk::calculateTotalSizeOnDisk() const
{
    return calculateTotalSizeOnDiskImpl(volume->getDisk(), fs::path(root_path) / part_dir);
}

std::unique_ptr<ReadBufferFromFileBase> DataPartStorageOnDisk::readFile(
    const std::string & name,
    const ReadSettings & settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    return volume->getDisk()->readFile(fs::path(root_path) / part_dir / name, settings, read_hint, file_size);
}

void DataPartStorageOnDisk::checkConsistency(const MergeTreeDataPartChecksums & checksums) const
{
    checksums.checkSizes(volume->getDisk(), getRelativePath());
}

void DataPartStorageOnDisk::remove(
    CanRemoveCallback && can_remove_callback,
    const MergeTreeDataPartChecksums & checksums,
    std::list<ProjectionChecksums> projections,
    bool is_temp,
    MergeTreeDataPartState state,
    Poco::Logger * log)
{
    /// NOTE We rename part to delete_tmp_<relative_path> instead of delete_tmp_<name> to avoid race condition
    /// when we try to remove two parts with the same name, but different relative paths,
    /// for example all_1_2_1 (in Deleting state) and tmp_merge_all_1_2_1 (in Temporary state).
    fs::path from = fs::path(root_path) / part_dir;
    // fs::path to = fs::path(root_path) / ("delete_tmp_" + part_dir);
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
        if (part_dir_without_slash.has_parent_path())
        {
            auto parent_path = part_dir_without_slash.parent_path();
            if (parent_path == "detached")
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Trying to remove detached part {} with path {} in remove function. It shouldn't happen",
                    part_dir,
                    root_path);

            part_dir_without_slash = parent_path / ("delete_tmp_" + std::string{part_dir_without_slash.filename()});
        }
        else
        {
            part_dir_without_slash = ("delete_tmp_" + std::string{part_dir_without_slash.filename()});
        }

        to = fs::path(root_path) / part_dir_without_slash;

        if (disk->exists(to))
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

        clearDirectory(
            fs::path(to) / proj_dir_name,
            can_remove_description->can_remove_anything, can_remove_description->files_not_to_remove, projection.checksums, {}, is_temp, state, log, true);
    }

    /// It is possible that we are removing the part which have a written but not loaded projection.
    /// Such a part can appear server was restarted after DROP PROJECTION but before old part was removed.
    /// In this case, the old part will load only projections from metadata.
    /// See test 01701_clear_projection_and_part.
    for (const auto & [name, _] : checksums.files)
    {
        if (endsWith(name, proj_suffix) && !projection_directories.contains(name) && disk->isDirectory(fs::path(to) / name))
        {

            /// If we have a directory with suffix '.proj' it is likely a projection.
            /// Try to load checksums for it (to avoid recursive removing fallback).
            std::string checksum_path = fs::path(to) / name / "checksums.txt";
            if (disk->exists(checksum_path))
            {
                try
                {
                    MergeTreeDataPartChecksums tmp_checksums;
                    auto in = disk->readFile(checksum_path, {});
                    tmp_checksums.read(*in);

                    projection_directories.emplace(name);

                    clearDirectory(
                        fs::path(to) / name,
                        can_remove_description->can_remove_anything, can_remove_description->files_not_to_remove, tmp_checksums, {}, is_temp, state, log, true);
                }
                catch (...)
                {
                    LOG_ERROR(log, "Cannot load checksums from {}", checksum_path);
                }
            }
        }
    }

    clearDirectory(to, can_remove_description->can_remove_anything, can_remove_description->files_not_to_remove, checksums, projection_directories, is_temp, state, log, false);
}

void DataPartStorageOnDisk::clearDirectory(
    const std::string & dir,
    bool can_remove_shared_data,
    const NameSet & names_not_to_remove,
    const MergeTreeDataPartChecksums & checksums,
    const std::unordered_set<String> & skip_directories,
    bool is_temp,
    MergeTreeDataPartState state,
    Poco::Logger * log,
    bool is_projection) const
{
    auto disk = volume->getDisk();

    /// It does not make sense to try fast path for incomplete temporary parts, because some files are probably absent.
    /// Sometimes we add something to checksums.files before actually writing checksums and columns on disk.
    /// Also sometimes we write checksums.txt and columns.txt in arbitrary order, so this check becomes complex...
    bool is_temporary_part = is_temp || state == MergeTreeDataPartState::Temporary;
    bool incomplete_temporary_part = is_temporary_part && (!disk->exists(fs::path(dir) / "checksums.txt") || !disk->exists(fs::path(dir) / "columns.txt"));
    if (checksums.empty() || incomplete_temporary_part)
    {
        /// If the part is not completely written, we cannot use fast path by listing files.
        disk->removeSharedRecursive(fs::path(dir) / "", !can_remove_shared_data, names_not_to_remove);
        return;
    }

    try
    {
        /// Remove each expected file in directory, then remove directory itself.
        RemoveBatchRequest request;

        for (const auto & [file, _] : checksums.files)
        {
            if (skip_directories.find(file) == skip_directories.end())
                request.emplace_back(fs::path(dir) / file);
        }

        for (const auto & file : {"checksums.txt", "columns.txt"})
            request.emplace_back(fs::path(dir) / file);

        request.emplace_back(fs::path(dir) / "default_compression_codec.txt", true);
        request.emplace_back(fs::path(dir) / "delete-on-destroy.txt", true);

        if (!is_projection)
            request.emplace_back(fs::path(dir) / "txn_version.txt", true);

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

std::optional<String> DataPartStorageOnDisk::getRelativePathForPrefix(Poco::Logger * log, const String & prefix, bool detached, bool broken) const
{
    assert(!broken || detached);
    String res;

    auto full_relative_path = fs::path(root_path);
    if (detached)
        full_relative_path /= "detached";

    std::optional<String> original_checksums_content;
    std::optional<Strings> original_files_list;

    for (int try_no = 0; try_no < 10; ++try_no)
    {
        res = (prefix.empty() ? "" : prefix + "_") + part_dir + (try_no ? "_try" + DB::toString(try_no) : "");

        if (!volume->getDisk()->exists(full_relative_path / res))
            return res;

        if (broken && looksLikeBrokenDetachedPartHasTheSameContent(res, original_checksums_content, original_files_list))
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

bool DataPartStorageOnDisk::looksLikeBrokenDetachedPartHasTheSameContent(const String & detached_part_path,
                                                                         std::optional<String> & original_checksums_content,
                                                                         std::optional<Strings> & original_files_list) const
{
    /// We cannot know for sure that content of detached part is the same,
    /// but in most cases it's enough to compare checksums.txt and list of files.

    if (!exists("checksums.txt"))
        return false;

    auto detached_full_path = fs::path(root_path) / "detached" / detached_part_path;
    auto disk = volume->getDisk();
    if (!disk->exists(detached_full_path / "checksums.txt"))
        return false;

    if (!original_checksums_content)
    {
        auto in = disk->readFile(detached_full_path / "checksums.txt", /* settings */ {}, /* read_hint */ {}, /* file_size */ {});
        original_checksums_content.emplace();
        readStringUntilEOF(*original_checksums_content, *in);
    }

    if (original_checksums_content->empty())
        return false;

    auto part_full_path = fs::path(root_path) / part_dir;
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
        disk->listFiles(part_full_path, *original_files_list);
        std::sort(original_files_list->begin(), original_files_list->end());
    }

    Strings detached_files_list;
    disk->listFiles(detached_full_path, detached_files_list);
    std::sort(detached_files_list.begin(), detached_files_list.end());

    return original_files_list == detached_files_list;
}

std::string DataPartStorageOnDisk::getDiskName() const
{
    return volume->getDisk()->getName();
}

std::string DataPartStorageOnDisk::getDiskType() const
{
    return toString(volume->getDisk()->getDataSourceDescription().type);
}

bool DataPartStorageOnDisk::isStoredOnRemoteDisk() const
{
    return volume->getDisk()->isRemote();
}

bool DataPartStorageOnDisk::supportZeroCopyReplication() const
{
    return volume->getDisk()->supportZeroCopyReplication();
}

bool DataPartStorageOnDisk::supportParallelWrite() const
{
    return volume->getDisk()->supportParallelWrite();
}

bool DataPartStorageOnDisk::isBroken() const
{
    return volume->getDisk()->isBroken();
}

void DataPartStorageOnDisk::syncRevision(UInt64 revision) const
{
    volume->getDisk()->syncRevision(revision);
}

UInt64 DataPartStorageOnDisk::getRevision() const
{
    return volume->getDisk()->getRevision();
}

std::unordered_map<String, String> DataPartStorageOnDisk::getSerializedMetadata(const std::vector<String> & paths) const
{
    return volume->getDisk()->getSerializedMetadata(paths);
}

std::string DataPartStorageOnDisk::getDiskPath() const
{
    return volume->getDisk()->getPath();
}

ReservationPtr DataPartStorageOnDisk::reserve(UInt64 bytes) const
{
    auto res = volume->reserve(bytes);
    if (!res)
        throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "Cannot reserve {}, not enough space", ReadableSize(bytes));

    return res;
}

ReservationPtr DataPartStorageOnDisk::tryReserve(UInt64 bytes) const
{
    return volume->reserve(bytes);
}

String DataPartStorageOnDisk::getUniqueId() const
{
    auto disk = volume->getDisk();
    if (!disk->supportZeroCopyReplication())
        throw Exception(fmt::format("Disk {} doesn't support zero-copy replication", disk->getName()), ErrorCodes::LOGICAL_ERROR);

    return disk->getUniqueId(fs::path(getRelativePath()) / "checksums.txt");
}

void DataPartStorageOnDisk::backup(
    const MergeTreeDataPartChecksums & checksums,
    const NameSet & files_without_checksums,
    const String & path_in_backup,
    BackupEntries & backup_entries,
    bool make_temporary_hard_links,
    TemporaryFilesOnDisks * temp_dirs) const
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
        fs::path temp_dir = temp_dir_owner->getPath();
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

    for (const auto & [filepath, checksum] : checksums.files)
    {
        if (filepath.ends_with(".proj"))
            continue; /// Skip *.proj files - they're actually directories and will be handled.
        String filepath_on_disk = part_path_on_disk / filepath;
        String filepath_in_backup = part_path_in_backup / filepath;

        if (make_temporary_hard_links)
        {
            String hardlink_filepath = temp_part_dir / filepath;
            disk->createHardLink(filepath_on_disk, hardlink_filepath);
            filepath_on_disk = hardlink_filepath;
        }

        UInt128 file_hash{checksum.file_hash.first, checksum.file_hash.second};
        backup_entries.emplace_back(
            filepath_in_backup,
            std::make_unique<BackupEntryFromImmutableFile>(disk, filepath_on_disk, checksum.file_size, file_hash, temp_dir_owner));
    }

    for (const auto & filepath : files_without_checksums)
    {
        String filepath_on_disk = part_path_on_disk / filepath;
        String filepath_in_backup = part_path_in_backup / filepath;
        backup_entries.emplace_back(filepath_in_backup, std::make_unique<BackupEntryFromSmallFile>(disk, filepath_on_disk));
    }
}

MutableDataPartStoragePtr DataPartStorageOnDisk::freeze(
    const std::string & to,
    const std::string & dir_path,
    bool make_source_readonly,
    std::function<void(const DiskPtr &)> save_metadata_callback,
    bool copy_instead_of_hardlink,
    const NameSet & files_to_copy_instead_of_hardlinks) const

{
    auto disk = volume->getDisk();
    disk->createDirectories(to);

    localBackup(disk, getRelativePath(), fs::path(to) / dir_path, make_source_readonly, {}, copy_instead_of_hardlink, files_to_copy_instead_of_hardlinks);

    if (save_metadata_callback)
        save_metadata_callback(disk);

    disk->removeFileIfExists(fs::path(to) / dir_path / "delete-on-destroy.txt");
    disk->removeFileIfExists(fs::path(to) / dir_path / "txn_version.txt");

    auto single_disk_volume = std::make_shared<SingleDiskVolume>(disk->getName(), disk, 0);
    return std::make_shared<DataPartStorageOnDisk>(single_disk_volume, to, dir_path);
}

MutableDataPartStoragePtr DataPartStorageOnDisk::clonePart(
    const std::string & to,
    const std::string & dir_path,
    const DiskPtr & disk,
    Poco::Logger * log) const
{
    String path_to_clone = fs::path(to) / dir_path / "";

    if (disk->exists(path_to_clone))
    {
        LOG_WARNING(log, "Path {} already exists. Will remove it and clone again.", fullPath(disk, path_to_clone));
        disk->removeRecursive(path_to_clone);
    }

    disk->createDirectories(to);
    volume->getDisk()->copy(getRelativePath(), disk, to);
    volume->getDisk()->removeFileIfExists(fs::path(path_to_clone) / "delete-on-destroy.txt");

    auto single_disk_volume = std::make_shared<SingleDiskVolume>(disk->getName(), disk, 0);
    return std::make_shared<DataPartStorageOnDisk>(single_disk_volume, to, dir_path);
}

void DataPartStorageOnDisk::rename(
    const std::string & new_root_path,
    const std::string & new_part_dir,
    Poco::Logger * log,
    bool remove_new_dir_if_exists,
    bool fsync_part_dir)
{
    String to = fs::path(new_root_path) / new_part_dir / "";

    if (volume->getDisk()->exists(to))
    {
        if (remove_new_dir_if_exists)
        {
            Names files;
            volume->getDisk()->listFiles(to, files);

            if (log)
                LOG_WARNING(log,
                    "Part directory {} already exists and contains {} files. Removing it.",
                    fullPath(volume->getDisk(), to), files.size());

            executeOperation([&](auto & disk) { disk.removeRecursive(to); });
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
    executeOperation([&](auto & disk)
    {
        disk.setLastModified(from, Poco::Timestamp::fromEpochTime(time(nullptr)));
        disk.moveDirectory(from, to);
    });

    part_dir = new_part_dir;
    root_path = new_root_path;

    SyncGuardPtr sync_guard;
    if (fsync_part_dir)
        sync_guard = volume->getDisk()->getDirectorySyncGuard(getRelativePath());
}

void DataPartStorageOnDisk::changeRootPath(const std::string & from_root, const std::string & to_root)
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

SyncGuardPtr DataPartStorageOnDisk::getDirectorySyncGuard() const
{
    return volume->getDisk()->getDirectorySyncGuard(fs::path(root_path) / part_dir);
}

template <typename Op>
void DataPartStorageOnDisk::executeOperation(Op && op)
{
    if (transaction)
        op(*transaction);
    else
        op(*volume->getDisk());
}

std::unique_ptr<WriteBufferFromFileBase> DataPartStorageOnDisk::writeFile(
    const String & name,
    size_t buf_size,
    const WriteSettings & settings)
{
    if (transaction)
        return transaction->writeFile(fs::path(root_path) / part_dir / name, buf_size, WriteMode::Rewrite, settings, /* autocommit = */ false);

    return volume->getDisk()->writeFile(fs::path(root_path) / part_dir / name, buf_size, WriteMode::Rewrite, settings);
}

std::unique_ptr<WriteBufferFromFileBase> DataPartStorageOnDisk::writeTransactionFile(WriteMode mode) const
{
    return volume->getDisk()->writeFile(fs::path(root_path) / part_dir / "txn_version.txt", 256, mode);
}

void DataPartStorageOnDisk::createFile(const String & name)
{
    executeOperation([&](auto & disk) { disk.createFile(fs::path(root_path) / part_dir / name); });
}

void DataPartStorageOnDisk::moveFile(const String & from_name, const String & to_name)
{
    executeOperation([&](auto & disk)
    {
        auto relative_path = fs::path(root_path) / part_dir;
        disk.moveFile(relative_path / from_name, relative_path / to_name);
    });
}

void DataPartStorageOnDisk::replaceFile(const String & from_name, const String & to_name)
{
    executeOperation([&](auto & disk)
    {
        auto relative_path = fs::path(root_path) / part_dir;
        disk.replaceFile(relative_path / from_name, relative_path / to_name);
    });
}

void DataPartStorageOnDisk::removeFile(const String & name)
{
    executeOperation([&](auto & disk) { disk.removeFile(fs::path(root_path) / part_dir / name); });
}

void DataPartStorageOnDisk::removeFileIfExists(const String & name)
{
    executeOperation([&](auto & disk) { disk.removeFileIfExists(fs::path(root_path) / part_dir / name); });
}

void DataPartStorageOnDisk::removeRecursive()
{
    executeOperation([&](auto & disk) { disk.removeRecursive(fs::path(root_path) / part_dir); });
}

void DataPartStorageOnDisk::removeSharedRecursive(bool keep_in_remote_fs)
{
    executeOperation([&](auto & disk) { disk.removeSharedRecursive(fs::path(root_path) / part_dir, keep_in_remote_fs, {}); });
}

void DataPartStorageOnDisk::createHardLinkFrom(const IDataPartStorage & source, const std::string & from, const std::string & to)
{
    const auto * source_on_disk = typeid_cast<const DataPartStorageOnDisk *>(&source);
    if (!source_on_disk)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create hardlink from different storage. Expected DataPartStorageOnDisk, got {}",
            typeid(source).name());

    executeOperation([&](auto & disk)
    {
        disk.createHardLink(
            fs::path(source_on_disk->getRelativePath()) / from,
            fs::path(root_path) / part_dir / to);
    });
}

void DataPartStorageOnDisk::createDirectories()
{
    executeOperation([&](auto & disk) { disk.createDirectories(fs::path(root_path) / part_dir); });
}

void DataPartStorageOnDisk::createProjection(const std::string & name)
{
    executeOperation([&](auto & disk) { disk.createDirectory(fs::path(root_path) / part_dir / name); });
}

void DataPartStorageOnDisk::beginTransaction()
{
    if (transaction)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Uncommitted {}transaction already exists", has_shared_transaction ? "shared " : "");

    transaction = volume->getDisk()->createTransaction();
}

void DataPartStorageOnDisk::commitTransaction()
{
    if (!transaction)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no uncommitted transaction");

    if (has_shared_transaction)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot commit shared transaction");

    transaction->commit();
    transaction.reset();
}

}
