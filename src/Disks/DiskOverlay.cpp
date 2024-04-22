#include "DiskOverlay.h"
#include "DiskFactory.h"
#include "Disks/DirectoryIterator.h"
#include "Disks/IDisk.h"
#include "Disks/ObjectStorages/IObjectStorage_fwd.h"
#include "loadLocalDiskConfig.h"

#include <iostream>

namespace DB {

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int FILE_DOESNT_EXIST;
}

void DiskOverlay::debugFunc() {
    std::cerr << "metadata: <" << metadata->readInlineDataToString("some/path") << ">\n";
}

DiskOverlay::DiskOverlay(const String & name_, DiskPtr disk_base_, DiskPtr disk_overlay_, MetadataStoragePtr metadata_, MetadataStoragePtr tracked_metadata_) : IDisk(name_),
disk_base(disk_base_), disk_overlay(disk_overlay_), metadata(metadata_), tracked_metadata(tracked_metadata_) {
}

DiskOverlay::DiskOverlay(const String & name_, const Poco::Util::AbstractConfiguration & config_, const String & config_prefix_, const DisksMap & map_) : IDisk(name_) {
    String disk_base_name = config_.getString(config_prefix_ + ".disk_base");
    String disk_overlay_name = config_.getString(config_prefix_ + ".disk_overlay");
    String metadata_pref = config_.getString(config_prefix_ + ".metadata");
    String tracked_metadata_pref = config_.getString(config_prefix_ + ".tracked_metadata");

    disk_base = map_.at(disk_base_name);
    disk_overlay = map_.at(disk_overlay_name);

    if (disk_overlay -> isReadOnly()) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Overlay disk has to be writable");
    }

    // TODO finish this constructor for registration
}


ReservationPtr DiskOverlay::reserve(UInt64 bytes) {
    return disk_overlay->reserve(bytes);
}

std::optional<UInt64> DiskOverlay::getTotalSpace() const
{
    return disk_overlay->getTotalSpace();
}

std::optional<UInt64> DiskOverlay::getAvailableSpace() const
{
    return disk_overlay->getAvailableSpace();
}

std::optional<UInt64> DiskOverlay::getUnreservedSpace() const
{
    return disk_overlay->getUnreservedSpace();
}

bool DiskOverlay::isTracked(const String& path) const {
    return tracked_metadata->exists(path);
}

void DiskOverlay::setTracked(const String& path) {
    auto trans = tracked_metadata->createTransaction();
    trans->writeInlineDataToFile(path, "");
    trans->commit();
}

bool DiskOverlay::isReplaced(const String& path) const {
    if (metadata->exists(path)) {
        std::string data = metadata->readInlineDataToString(path);
        if (!data.empty()) {
            return data == "r";
        }
    }
    return false;
}

void DiskOverlay::setReplaced(const String& path) {
    auto trans = tracked_metadata->createTransaction();
    trans->writeInlineDataToFile(path, "r");
    trans->commit();
}

bool DiskOverlay::exists(const String & path) const {
    return disk_overlay->exists(path) || (!isTracked(path) && disk_base->exists(path));
}

bool DiskOverlay::isFile(const String & path) const {
    return disk_overlay->isFile(path) || (!isTracked(path) && disk_base->isFile(path));
}

bool DiskOverlay::isDirectory(const String & path) const {
    return disk_overlay->isDirectory(path) || (!isTracked(path) && disk_base->isDirectory(path));
}

size_t DiskOverlay::getFileSize(const String & path) const {
    if (disk_overlay->exists(path)) {
        size_t size = disk_overlay->getFileSize(path);
        
        // We check if there is a base path for this file and in that case add its size to the result
        if (auto bpath = basePath(path); bpath) {
            size += disk_base->getFileSize(*bpath);
        }
        return size;
    }
    if (!isTracked(path) && disk_base->exists(path)) {
        return disk_base->getFileSize(path);
    }
    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File doesn't exist in getFileSize");
}

void DiskOverlay::createDirectory(const String & path) {
    if (exists(path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot create directory: path already exists");
    }
    disk_overlay->createDirectory(path);
}

void DiskOverlay::createDirectories(const String & path) {
    if (exists(path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot create directories: path already exists");
    }
    disk_overlay->createDirectories(path);
}

void DiskOverlay::clearDirectory(const String & path) {
    if (!exists(path)) {
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Cannot clear directory: path doesn't exist");
    }
    std::vector<String> files;
    listFiles(path, files);
    for (const String& file : files) {
        if (isFile(file)) {
            removeFile(file);
        }
    }
}

// Find the file in the base disk if it exists
std::optional<String> DiskOverlay::basePath(const String& path) const {
    if (!metadata->exists(path)) {
        return {};
    }
    String res = metadata->readInlineDataToString(path);
    if (res == "r") {
        return {};
    }
    return res;
}

void DiskOverlay::moveDirectory(const String & from_path, const String & to_path) {
    if (!exists(from_path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move directory: from_path doesn't exist");
    }
    if (exists(to_path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move directory: to_path already exists");
    }
    if (!isDirectory(from_path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move directory: from_path is not a directory");
    }
    createDirectory(to_path);

    std::vector<String> paths;
    listFiles(from_path, paths);

    for (const String& path : paths) {
        if (isFile(path)) {
            moveFile(path, to_path + fileName(path));
        } else {
            moveDirectory(path, to_path + fileName(path));
        }
    }

    removeDirectory(from_path);
}

class DiskOverlayDirectoryIterator final : public IDirectoryIterator
{
public:
    DiskOverlayDirectoryIterator() = default;
    DiskOverlayDirectoryIterator(DirectoryIteratorPtr overlay_iter_, DirectoryIteratorPtr base_iter_, std::shared_ptr<DiskOverlay> base)
    : done_overlay(false), overlay_iter(std::move(overlay_iter_)), base_iter(std::move(base_iter_)), base_ptr(base) {
        upd();
    }

    void next() override {
    }

    bool isValid() const override {
        return done_overlay ? base_iter->isValid() : overlay_iter->isValid();
    }

    String path() const override {
        return done_overlay ? base_iter->path() : overlay_iter->path();
    }

    String name() const override {
        return fileName(path());
    }

private:
    bool done_overlay;
    DirectoryIteratorPtr overlay_iter;
    DirectoryIteratorPtr base_iter;
    std::shared_ptr<DiskOverlay> base_ptr;

    void upd() {
        if (!done_overlay && !isValid()) {
            done_overlay = true;
            upd();
        }
        while (done_overlay && isValid() && base_ptr->isTracked(path())) {
            next();
        }
    }
};


DirectoryIteratorPtr DiskOverlay::iterateDirectory(const String & path) const {
    if (isReplaced(path)) {
        return disk_overlay->iterateDirectory(path);
    }
    return disk_base->iterateDirectory(path); // TODO write new directory iterator
}

// TODO when we create a file on disk_overlay, we need to create the parent directories.
// We ALSO need to update the trackedness for them

void DiskOverlay::createFile(const String & path) {
    if (exists(path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot create file: path already exists");
    }
    disk_overlay->createFile(path);
}

void DiskOverlay::moveFile(const String & from_path, const String & to_path) {
    if (!exists(from_path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move file: from_path doesn't exist");
    }
    if (exists(to_path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move file: to_path already exists");
    }
    if (!isFile(from_path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move file: from_path is not a file");
    }
    if (disk_overlay->exists(from_path)) {
        // In this case we need to move file in overlay disk and move metadata if there is any
        disk_overlay->moveFile(from_path, to_path);

        auto transaction = metadata->createTransaction();
        if (metadata->exists(from_path)) {
            String data = metadata->readInlineDataToString(from_path);
            transaction->writeInlineDataToFile(to_path, data);
            transaction->unlinkFile(from_path);
        }
        transaction->commit();
    } else {
        // In this case from_path exists on base disk. We want to create an empty file on overlay, set metadata
        // to show that it should be appended to from_path on base disk, and set that from_path is now tracked
        // by metadata
        disk_overlay->createFile(to_path);
        auto trans = metadata->createTransaction();
        trans->writeInlineDataToFile(to_path, from_path);
        trans->commit();
        setTracked(from_path);
    }
    disk_overlay->moveFile(from_path, to_path);
}

void DiskOverlay::replaceFile(const String & from_path, const String & to_path) {
    if (!exists(from_path) || !isFile(from_path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot replace old file with new: new doesn't exist or is not a file");
    }
    removeFileIfExists(to_path);
    moveFile(from_path, to_path);
}

void DiskOverlay::listFiles(const String & path, std::vector<String> & file_names) const {
    std::vector<String> files;
    disk_base->listFiles(path, files);
    for (const String& file : files) {
        if (!isTracked(file)) {
            file_names.push_back(file);
        }
    }
    disk_overlay->listFiles(path, file_names);
}

std::unique_ptr<ReadBufferFromFileBase> DiskOverlay::readFile(
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const {

    if (!isReplaced(path)) {
        return disk_base->readFile(path, settings, read_hint, file_size);
    }
    return disk_overlay->readFile(path, settings, read_hint, file_size);
    // TODO: write a version of read buffer from file base
}

std::unique_ptr<WriteBufferFromFileBase> DiskOverlay::writeFile(
        const String & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) {

    auto transaction = metadata->createTransaction();
    if (mode == WriteMode::Rewrite) {
        // This means that we don't need to look for this file on disk_base
        transaction->writeInlineDataToFile(path, "r");
    } else {
        if (!disk_overlay->exists(path)) {
            // This means that the beginning of the file is on disk_base at the same path
            transaction->writeInlineDataToFile(path, path);
        }
    }
    if (disk_base->exists(path)) {
        setTracked(path);
    }
    transaction->commit();

    return disk_overlay->writeFile(path, buf_size, mode, settings);
}

void DiskOverlay::removeFile(const String & path) {
    if (!exists(path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot remove file as it doesn't exist");
    }
    removeFileIfExists(path);
}

void DiskOverlay::removeFileIfExists(const String & path) {
    disk_overlay->removeFileIfExists(path);

    auto transaction = metadata->createTransaction();
    if (metadata->exists(path)) {
        transaction->unlinkFile(path);
    } else {
        if (disk_base->exists(path)) {
            setTracked(path); // This will ensure that this file will not appear in listing operations
        }
    }
    transaction->commit();
}

void DiskOverlay::removeDirectory(const String & path) {
    if (!isDirectoryEmpty(path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot remove directory as path is not an empty directory");
    }
    if (disk_base->exists(path)) {
        setTracked(path);
    }
    disk_overlay->removeDirectory(path);
}

void DiskOverlay::removeRecursive(const String & dirpath) {
    if (!exists(dirpath)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot remove path as it doesn't exist");
    }

    std::vector<String> paths;
    listFiles(dirpath, paths);

    for (const String& path : paths) {
        if (isFile(path)) {
            removeFile(path);
        } else {
            removeRecursive(path);
        }
    }
    removeDirectory(dirpath);
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
