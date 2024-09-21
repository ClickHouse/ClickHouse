#include "DiskOverlay.h"

#include <Disks/DiskFactory.h>
#include <Disks/DirectoryIterator.h>
#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/IObjectStorage_fwd.h>
#include <Disks/ObjectStorages/MetadataStorageFactory.h>

namespace DB
{

namespace
{
    String dataPath(String path)
    {
        if (!path.empty() && path.back() == '/')
        {
            path.pop_back();
        }
        path += ".data";
        for (char & i : path)
        {
            if (i == '/')
            {
                i = '_';
            }
        }
        return path;
    }

    String mergePath(String parent, const String & child)
    {
        if (!parent.empty() && parent.back() != '/')
        {
            parent += '/';
        }
        parent += child;
        return parent;
    }
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int FILE_DOESNT_EXIST;
    extern const int NOT_IMPLEMENTED;
}

DiskOverlay::DiskOverlay(
    const String & name_,
    DiskPtr disk_base_,
    DiskPtr disk_diff_,
    MetadataStoragePtr metadata_,
    MetadataStoragePtr tracked_metadata_)
    : IDisk(name_)
    , disk_base(disk_base_)
    , disk_diff(disk_diff_)
    , forward_metadata(metadata_)
    , tracked_metadata(tracked_metadata_)
{
}

DiskOverlay::DiskOverlay(const String & name_, const Poco::Util::AbstractConfiguration & config_, const String & config_prefix_, const DisksMap & map_) : IDisk(name_)
{
    String disk_base_name = config_.getString(config_prefix_ + ".disk_base");
    String disk_diff_name = config_.getString(config_prefix_ + ".disk_diff");
    String forward_metadata_name = config_.getString(config_prefix_ + ".forward_metadata.metadata_type");
    String tracked_metadata_name = config_.getString(config_prefix_ + ".tracked_metadata.metadata_type");

    disk_base = map_.at(disk_base_name);
    disk_diff = map_.at(disk_diff_name);

    if (disk_diff -> isReadOnly())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Diff disk has to be writable");
    }

    forward_metadata = MetadataStorageFactory::instance().create(forward_metadata_name, config_, config_prefix_ + ".forward_metadata", nullptr, "", false);
    tracked_metadata = MetadataStorageFactory::instance().create(tracked_metadata_name, config_, config_prefix_ + ".tracked_metadata", nullptr, "", false);
}

const String & DiskOverlay::getPath() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Overlay doesn't have its own path");
}

UInt64 DiskOverlay::getKeepingFreeSpace() const
{
    return disk_diff->getKeepingFreeSpace();
}

ReservationPtr DiskOverlay::reserve(UInt64 bytes)
{
    return disk_diff->reserve(bytes);
}

std::optional<UInt64> DiskOverlay::getTotalSpace() const
{
    return disk_diff->getTotalSpace();
}

std::optional<UInt64> DiskOverlay::getAvailableSpace() const
{
    return disk_diff->getAvailableSpace();
}

std::optional<UInt64> DiskOverlay::getUnreservedSpace() const
{
    return disk_diff->getUnreservedSpace();
}

bool DiskOverlay::isTracked(const String & path) const
{
    return tracked_metadata->exists(dataPath(path));
}

void DiskOverlay::setTracked(const String & path)
{
    auto trans = tracked_metadata->createTransaction();
    trans->writeInlineDataToFile(dataPath(path), "");
    trans->commit();
}

bool DiskOverlay::exists(const String & path) const
{
    return disk_diff->exists(path) || (!isTracked(path) && disk_base->exists(path));
}

bool DiskOverlay::isFile(const String & path) const
{
    return disk_diff->isFile(path) || (!isTracked(path) && disk_base->isFile(path));
}

bool DiskOverlay::isDirectory(const String & path) const
{
    return disk_diff->isDirectory(path) || (!isTracked(path) && disk_base->isDirectory(path));
}

size_t DiskOverlay::getFileSize(const String & path) const
{
    if (disk_diff->exists(path))
    {
        size_t size = disk_diff->getFileSize(path);
        // We check if there is a base path for this file and in that case add its size to the result
        if (auto bpath = basePath(path); bpath)
        {
            size += disk_base->getFileSize(*bpath);
        }
        return size;
    }
    if (!isTracked(path) && disk_base->exists(path))
    {
        return disk_base->getFileSize(path);
    }
    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File doesn't exist in getFileSize");
}

void DiskOverlay::ensureHaveDirectories(const String & path)
{
    if (!disk_diff->exists(path))
    {
        ensureHaveDirectories(parentPath(path));
        disk_diff->createDirectory(path);
        if (disk_base->exists(path))
        {
            setTracked(path);
        }
    }
}

void DiskOverlay::ensureHaveFile(const String & path)
{
    if (!disk_diff->exists(path))
    {
        ensureHaveDirectories(parentPath(path));
        disk_diff->createFile(path);
        if (disk_base->exists(path) && !isTracked(path))
        {
            setTracked(path);
            auto trans = forward_metadata->createTransaction();
            trans->writeInlineDataToFile(dataPath(path), path);
            trans->commit();
        }
    }
}

void DiskOverlay::createDirectory(const String & path)
{
    if (exists(path))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot create directory: path already exists");
    }
    if (!exists(parentPath(path)))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot create directory: parent path doesn't exist");
    }
    ensureHaveDirectories(path);
}

void DiskOverlay::createDirectories(const String & path)
{
    if (exists(path))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot create directories: path already exists");
    }
    ensureHaveDirectories(path);
}

void DiskOverlay::clearDirectory(const String & path)
{
    if (!exists(path))
    {
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Cannot clear directory: path doesn't exist");
    }
    std::vector<String> files;
    listFiles(path, files);
    for (const String & file : files)
    {
        if (isFile(file))
        {
            removeFile(file);
        }
    }
}

// Find the file in the base disk if it exists
std::optional<String> DiskOverlay::basePath(const String & path) const
{
    if (!forward_metadata->exists(dataPath(path)))
    {
        return {};
    }
    String res = forward_metadata->readInlineDataToString(dataPath(path));
    return res;
}

void DiskOverlay::moveDirectory(const String & from_path, const String & to_path)
{
    if (!exists(from_path))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move directory: from_path doesn't exist");
    }
    if (!isDirectory(from_path))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move directory: from_path is not a directory");
    }
    if (exists(to_path))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move directory: to_path already exists");
    }
    if (!exists(parentPath(to_path)) || !isDirectory(parentPath(to_path)))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move directory: parent path of to_path doesn't exist or is not a directory");
    }
    ensureHaveDirectories(to_path);

    std::vector<String> paths;
    listFiles(from_path, paths);

    for (const String & path : paths)
    {
        String fullfrompath = mergePath(from_path, path), fulltopath = mergePath(to_path, path);
        if (isFile(fullfrompath))
        {
            moveFile(fullfrompath, fulltopath);
        }
        else
        {
            moveDirectory(fullfrompath, fulltopath);
        }
    }

    removeDirectory(from_path);
}

class DiskOverlayDirectoryIterator final : public IDirectoryIterator
{
public:
    DiskOverlayDirectoryIterator() = default;
    DiskOverlayDirectoryIterator(DirectoryIteratorPtr diff_iter_, DirectoryIteratorPtr base_iter_, MetadataStoragePtr tracked_metadata_)
    : done_diff(false), diff_iter(std::move(diff_iter_)), base_iter(std::move(base_iter_)), tracked_metadata(tracked_metadata_)
    {
        upd();
    }

    void next() override
    {
        if (!done_diff)
        {
            diff_iter->next();
        }
        else
        {
            base_iter->next();
        }
        upd();
    }

    bool isValid() const override
    {
        return done_diff ? base_iter->isValid() : true;
    }

    String path() const override
    {
        return done_diff ? base_iter->path() : diff_iter->path();
    }

    String name() const override
    {
        return fileName(path());
    }

private:
    bool done_diff;
    DirectoryIteratorPtr diff_iter;
    DirectoryIteratorPtr base_iter;
    MetadataStoragePtr tracked_metadata;

    void upd()
    {
        if (!done_diff && !diff_iter->isValid())
        {
            done_diff = true;
        }
        while (done_diff && base_iter->isValid() && tracked_metadata->exists(dataPath(base_iter->path())))
        {
            next();
        }
    }
};

DirectoryIteratorPtr DiskOverlay::iterateDirectory(const String & path) const
{
    return std::make_unique<DiskOverlayDirectoryIterator>(disk_diff->iterateDirectory(path), disk_base->iterateDirectory(path), tracked_metadata);
}

void DiskOverlay::createFile(const String & path)
{
    if (exists(path))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot create file: path already exists");
    }
    if (!exists(parentPath(path)))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot create file: parent path doesn't exist");
    }
    ensureHaveFile(path);
}

void DiskOverlay::moveFile(const String & from_path, const String & to_path)
{
    if (!exists(from_path))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move file: from_path doesn't exist");
    }
    if (!isFile(from_path))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move file: from_path is not a file");
    }
    if (exists(to_path))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move file: to_path already exists");
    }
    if (!exists(parentPath(to_path)))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move file: parent of to_path doesn't exist");
    }

    ensureHaveDirectories(parentPath(to_path));

    if (disk_diff->exists(from_path))
    {
        // In this case we need to move file in diff disk and move metadata if there is any
        disk_diff->moveFile(from_path, to_path);

        auto transaction = forward_metadata->createTransaction();
        if (forward_metadata->exists(dataPath(from_path)))
        {
            String data = forward_metadata->readInlineDataToString(dataPath(from_path));
            transaction->writeInlineDataToFile(dataPath(to_path), data);
            transaction->unlinkFile(dataPath(from_path));
        }

        transaction->commit();
    }
    else
    {
        // In this case from_path exists on base disk. We want to create an empty file on diff, set metadata
        // to show that it should be appended to from_path on base disk, and set that from_path is now tracked
        // by metadata
        disk_diff->createFile(to_path);

        auto trans = forward_metadata->createTransaction();
        trans->writeInlineDataToFile(dataPath(to_path), from_path);
        trans->commit();

        setTracked(from_path);
    }
}

void DiskOverlay::replaceFile(const String & from_path, const String & to_path)
{
    if (!exists(from_path) || !isFile(from_path))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot replace old file with new: new doesn't exist or is not a file");
    }
    if (!exists(parentPath(to_path)))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot replace old file with new: parent path of old doesn't exist");
    }
    ensureHaveDirectories(parentPath(to_path));

    removeFileIfExists(to_path);
    moveFile(from_path, to_path);
}

void DiskOverlay::copyFile( /// NOLINT
const String & from_file_path,
IDisk & to_disk,
const String & to_file_path,
const ReadSettings & read_settings,
const WriteSettings & write_settings,
const std::function<void()> & cancellation_hook)
{
    if (&to_disk == this)
    {
        ensureHaveFile(from_file_path);
        disk_diff->copyFile(from_file_path, to_disk, to_file_path, read_settings, write_settings, cancellation_hook);

        if (forward_metadata->exists(dataPath(from_file_path)))
        {
            auto trans = forward_metadata->createTransaction();
            trans->writeInlineDataToFile(dataPath(to_file_path), forward_metadata->readInlineDataToString(dataPath(from_file_path)));
            trans->commit();
        }
    }
    else
    {
        IDisk::copyFile(from_file_path, to_disk, to_file_path, read_settings, write_settings, cancellation_hook);
    }
}

void DiskOverlay::listFiles(const String & path, std::vector<String> & file_names) const
{
    if (!exists(path) || !isDirectory(path))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can't list files: path doesn't exist or isn't a directory");
    }

    file_names.clear();
    std::vector<String> files;

    if (disk_base->exists(path) && disk_base->isDirectory(path))
    {
        disk_base->listFiles(path, files);
        for (const String & file : files)
        {
            if (!isTracked(mergePath(path, file)))
            {
                file_names.push_back(file);
            }
        }
    }

    if (disk_diff->exists(path))
    {
        disk_diff->listFiles(path, files);
        file_names.insert(file_names.end(), files.begin(), files.end());
    }
}


/**
 * This read buffer wraps around two read buffers, transparently concatenating them.
 */
class ReadBufferFromOverlayDisk : public ReadBufferFromFileBase
{
public:
    ReadBufferFromOverlayDisk(
        size_t buffer_size_,
        std::unique_ptr<ReadBufferFromFileBase> base_,
        std::unique_ptr<ReadBufferFromFileBase> diff_) : ReadBufferFromFileBase(buffer_size_, nullptr, 0),
                    base(std::move(base_)), diff(std::move(diff_)), base_size(base->getFileSize()), diff_size(diff->getFileSize())
    {
        working_buffer = base->buffer();
        pos = base->position();
    }

    off_t seek([[maybe_unused]] off_t off, [[maybe_unused]] int whence) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "seek hasn't been implemented yet");
    }

    off_t getPosition() override
    {
        size_t current_pos = done ? base_size + diff_size : (done_base ? base_size : 0);
        if (!done)
        {
            current_pos += done_base ? diff->getPosition() : base->getPosition();
        }
        return current_pos;
    }

    std::string getFileName() const override { return diff->getFileName(); }

    std::optional<size_t> tryGetFileSize() override
    {
        std::optional<size_t> basefs = base->tryGetFileSize(), difffs = diff->tryGetFileSize();
        if (basefs && difffs) return *basefs + *difffs;
        return {};
    }

private:
    bool nextImpl() override
    {
        if (!done)
        {
            if (!done_base)
            {
                base->position() = pos;
            }
            else
            {
                diff->position() = pos;
            }

            if (!done_base && base->eof())
            {
                done_base = true;
                diff->seek(0, SEEK_SET);
            }

            if (done_base && !done && diff->eof())
            {
                done = true;
            }
        }

        if (done)
        {
            set(nullptr, 0);
            return false;
        }

        working_buffer = done_base ? diff->buffer() : base->buffer();
        pos = done_base ? diff->position() : base->position();
        return true;
    }

    std::unique_ptr<ReadBufferFromFileBase> base, diff;
    bool done_base = false, done = false;
    size_t base_size, diff_size;

};


std::unique_ptr<ReadBufferFromFileBase> DiskOverlay::readFile(
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const
{

    if (!exists(path))
    {
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Cannot read file as it doesn't exist");
    }

    if (disk_diff->exists(path))
    {
        if (auto base_path = basePath(path); base_path)
        {
            return std::make_unique<ReadBufferFromOverlayDisk>(settings.local_fs_buffer_size,
                                disk_base->readFile(*base_path, settings, read_hint, file_size),
                                disk_diff->readFile(path, settings, read_hint, file_size)
                    );
        }
        else
        {
            return disk_diff->readFile(path, settings, read_hint, file_size);
        }
    }

    return disk_base->readFile(path, settings, read_hint, file_size);
}

std::unique_ptr<WriteBufferFromFileBase> DiskOverlay::writeFile(
        const String & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings)
{
    if (!exists(parentPath(path)) || !isDirectory(parentPath(path)))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can't write to file: parent path doesn't exist or is not a folder");
    }
    ensureHaveDirectories(parentPath(path));

    auto transaction = forward_metadata->createTransaction();
    if (mode == WriteMode::Rewrite)
    {
        // This means that we don't need to look for this file on disk_base
        if (forward_metadata->exists(dataPath(path)))
        {
            transaction->unlinkFile(dataPath(path));
        }
        else
        {
            if (disk_base->exists(path))
            {
                setTracked(path);
            }
        }
    }
    else
    {
        if (!disk_diff->exists(path))
        {
            if (disk_base->exists(path) && !isTracked(path))
            {
                // This means that the beginning of the file is on disk_base at the same path
                transaction->writeInlineDataToFile(dataPath(path), path);
            }
        }
    }
    if (disk_base->exists(path))
    {
        setTracked(path);
    }
    transaction->commit();

    return disk_diff->writeFile(path, buf_size, mode, settings);
}

Strings DiskOverlay::getBlobPath(const String &  /*path*/) const { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Overlay is not an object storage"); }
void DiskOverlay::writeFileUsingBlobWritingFunction(const String &  /*path*/, WriteMode  /*mode*/, WriteBlobFunction &&  /*write_blob_function*/) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Overlay is not an object storage"); }

void DiskOverlay::removeFile(const String & path)
{
    if (!exists(path))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot remove file as it doesn't exist");
    }
    removeFileIfExists(path);
}

void DiskOverlay::removeFileIfExists(const String & path)
{
    disk_diff->removeFileIfExists(path);

    auto transaction = forward_metadata->createTransaction();
    if (forward_metadata->exists(dataPath(path)))
    {
        transaction->unlinkFile(dataPath(path));
    }
    else
    {
        if (disk_base->exists(path))
        {
            setTracked(path); // This will ensure that this file will not appear in listing operations
        }
    }
    transaction->commit();
}

void DiskOverlay::removeDirectory(const String & path)
{
    if (!exists(path) || !isDirectoryEmpty(path))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot remove directory as path doesn't exist or is not an empty directory");
    }
    if (disk_base->exists(path))
    {
        setTracked(path);
    }
    if (disk_diff->exists(path))
    {
        disk_diff->removeDirectory(path);
    }
}

void DiskOverlay::removeRecursive(const String & dirpath)
{
    if (!exists(dirpath))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot remove path as it doesn't exist");
    }

    std::vector<String> paths;
    listFiles(dirpath, paths);

    for (const String & path : paths)
    {
        String full_path = mergePath(dirpath, path);
        if (isFile(full_path))
        {
            removeFile(full_path);
        }
        else
        {
            removeRecursive(full_path);
        }
    }
    removeDirectory(dirpath);
}

void DiskOverlay::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    if (!exists(path))
    {
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Cannot set modification time: path doesn't exist");
    }
    if (isFile(path))
    {
        ensureHaveFile(path);
    }
    else
    {
        ensureHaveDirectories(path);
    }

    disk_diff->setLastModified(path, timestamp);
}

Poco::Timestamp DiskOverlay::getLastModified(const String &  path) const
{
    if (disk_diff->exists(path))
    {
        return disk_diff->getLastModified(path);
    }
    if (disk_base->exists(path) && !isTracked(path))
    {
        return disk_base->getLastModified(path);
    }
    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Cannot get modification time: path doesn't exist");
}

time_t DiskOverlay::getLastChanged(const String &  path) const
{
    if (disk_diff->exists(path))
    {
        return disk_diff->getLastChanged(path);
    }
    if (disk_base->exists(path) && !isTracked(path))
    {
        return disk_base->getLastChanged(path);
    }
    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Cannot get modification time: path doesn't exist");
}

void DiskOverlay::setReadOnly(const String & path)
{
    if (!exists(path))
    {
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Cannot set read only: path doesn't exist");
    }
    if (isFile(path))
    {
        ensureHaveFile(path);
    }
    else
    {
        ensureHaveDirectories(path);
    }

    disk_diff->setReadOnly(path);
}

void DiskOverlay::createHardLink(const String & src_path, const String & dst_path)
{
    // This doesn't work correctly for subsequent rewrite or deletion of the hardlinked file
    if (!exists(src_path) || !isFile(src_path))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can't create hardlink: src_path isn't a file");
    }
    ensureHaveFile(src_path);

    disk_diff->createHardLink(src_path, dst_path);
    if (forward_metadata->exists(dataPath(src_path)))
    {
        auto trans = forward_metadata->createTransaction();
        auto str = forward_metadata->readInlineDataToString(dataPath(src_path));
        trans->writeInlineDataToFile(dataPath(dst_path), str);
        trans->commit();
    }
}

DataSourceDescription DiskOverlay::getDataSourceDescription() const { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "There are two disks in overlay, which one do you want?"); }

bool DiskOverlay::supportParallelWrite() const
{
    return disk_diff->supportParallelWrite();
}

bool DiskOverlay::isRemote() const
{
    return disk_diff->isRemote() || disk_base->isRemote();
}

void registerDiskOverlay(DiskFactory & factory, bool global_skip_access_check)
{
    auto creator = [global_skip_access_check](
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & map,
        bool, bool) -> DiskPtr
    {
        std::shared_ptr<IDisk> disk = std::make_shared<DiskOverlay>(name, config, config_prefix, map);

        bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);
        disk->startup(context, skip_access_check);
        return disk;
    };
    factory.registerDiskType("overlay", creator);
}

}
