#include "DiskOverlay.h"
#include "DiskFactory.h"
#include "Disks/DirectoryIterator.h"
#include "Disks/IDisk.h"
#include "Disks/ObjectStorages/IObjectStorage_fwd.h"
#include "loadLocalDiskConfig.h"

#include <iostream>

namespace DB {

String dataPath(String path) {
    if (!path.empty() && path.back() == '/') {
        path.pop_back();
    }
    path += ".data";
    for (size_t i = 0; i < path.size(); ++i) {
        if (path[i] == '/') {
            path[i] = '_';
        }
    }
    return path;
}

String mergePath(String parent, const String& child) {
    if (!parent.empty() && parent.back() != '/') {
        parent += '/';
    }
    parent += child;
    return parent;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int FILE_DOESNT_EXIST;
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
    return tracked_metadata->exists(dataPath(path));
}

void DiskOverlay::setTracked(const String& path) {
    auto trans = tracked_metadata->createTransaction();
    trans->writeInlineDataToFile(dataPath(path), "");
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

void DiskOverlay::ensureHaveDirectories(const String& path) {
    if (!disk_overlay->exists(path)) {
        ensureHaveDirectories(parentPath(path));
        disk_overlay->createDirectory(path);
        if (disk_base->exists(path)) {
            setTracked(path);
        }
    }
}

void DiskOverlay::createDirectory(const String & path) {
    if (exists(path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot create directory: path already exists");
    }
    if (!exists(parentPath(path))) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot create directory: parent path doesn't exist");
    }
    ensureHaveDirectories(path);
}

void DiskOverlay::createDirectories(const String & path) {
    if (exists(path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot create directories: path already exists");
    }
    ensureHaveDirectories(path);
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
    if (!metadata->exists(dataPath(path))) {
        return {};
    }
    String res = metadata->readInlineDataToString(dataPath(path));
    if (res == "r") {
        return {};
    }
    return res;
}

void DiskOverlay::moveDirectory(const String & from_path, const String & to_path) {
    std::cout << "trying to move directory: " << from_path << " -> " << to_path << std::endl;
    if (!exists(from_path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move directory: from_path doesn't exist");
    }
    if (!isDirectory(from_path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move directory: from_path is not a directory");
    }
    if (exists(to_path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move directory: to_path already exists");
    }
    if (!exists(parentPath(to_path)) || !isDirectory(parentPath(to_path))) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move directory: parent path of to_path doesn't exist or is not a directory");
    }
    ensureHaveDirectories(to_path);

    std::vector<String> paths;
    listFiles(from_path, paths);

    for (const String& path : paths) {
        String fullfrompath = mergePath(from_path, path), fulltopath = mergePath(to_path, path);
        if (isFile(fullfrompath)) {
            moveFile(fullfrompath, fulltopath);
        } else {
            moveDirectory(fullfrompath, fulltopath);
        }
    }

    std::cout << "start removing" << std::endl;
    removeDirectory(from_path);
    std::cout << "done removing" << std::endl;
}

class DiskOverlayDirectoryIterator final : public IDirectoryIterator
{
public:
    DiskOverlayDirectoryIterator() = default;
    DiskOverlayDirectoryIterator(DirectoryIteratorPtr overlay_iter_, DirectoryIteratorPtr base_iter_, MetadataStoragePtr tracked_metadata_)
    : done_overlay(false), overlay_iter(std::move(overlay_iter_)), base_iter(std::move(base_iter_)), tracked_metadata(tracked_metadata_) {
        upd();
    }

    void next() override {
        if (!done_overlay) {
            overlay_iter->next();
        } else {
            base_iter->next();
        }
        upd();
    }

    bool isValid() const override {
        return done_overlay ? base_iter->isValid() : true;
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
    MetadataStoragePtr tracked_metadata;

    void upd() {
        if (!done_overlay && !overlay_iter->isValid()) {
            done_overlay = true;
        }
        while (done_overlay && base_iter->isValid() && tracked_metadata->exists(dataPath(base_iter->path()))) {
            next();
        }
    }
};

DirectoryIteratorPtr DiskOverlay::iterateDirectory(const String & path) const {
    return std::make_unique<DiskOverlayDirectoryIterator>(disk_overlay->iterateDirectory(path), disk_base->iterateDirectory(path), tracked_metadata);
}

void DiskOverlay::createFile(const String & path) {
    if (exists(path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot create file: path already exists");
    }
    if (!exists(parentPath(path))) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot create file: parent path doesn't exist");
    }
    ensureHaveDirectories(parentPath(path));
    disk_overlay->createFile(path);
}

void DiskOverlay::moveFile(const String & from_path, const String & to_path) {
    std::cout << "Trying to move file: " << from_path << " -> " << to_path << std::endl;
    if (!exists(from_path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move file: from_path doesn't exist");
    }
    if (!isFile(from_path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move file: from_path is not a file");
    }
    if (exists(to_path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move file: to_path already exists");
    }
    if (!exists(parentPath(to_path))) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move file: parent of to_path doesn't exist");
    }

    ensureHaveDirectories(parentPath(to_path));

    if (disk_overlay->exists(from_path)) {
        // In this case we need to move file in overlay disk and move metadata if there is any
        disk_overlay->moveFile(from_path, to_path);

        auto transaction = metadata->createTransaction();
        if (metadata->exists(dataPath(from_path))) {
            String data = metadata->readInlineDataToString(dataPath(from_path));
            transaction->writeInlineDataToFile(dataPath(to_path), data);
            transaction->unlinkFile(dataPath(from_path));
        }

        transaction->commit();
    } else {
        // In this case from_path exists on base disk. We want to create an empty file on overlay, set metadata
        // to show that it should be appended to from_path on base disk, and set that from_path is now tracked
        // by metadata
        disk_overlay->createFile(to_path);

        auto trans = metadata->createTransaction();
        trans->writeInlineDataToFile(dataPath(to_path), from_path);
        trans->commit();

        setTracked(from_path);
    }
}

void DiskOverlay::replaceFile(const String & from_path, const String & to_path) {
    if (!exists(from_path) || !isFile(from_path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot replace old file with new: new doesn't exist or is not a file");
    }
    if (!exists(parentPath(to_path))) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot replace old file with new: parent path of old doesn't exist");
    }
    ensureHaveDirectories(parentPath(to_path));

    removeFileIfExists(to_path);
    moveFile(from_path, to_path);
}

void DiskOverlay::listFiles(const String & path, std::vector<String> & file_names) const {
    if (!exists(path) || !isDirectory(path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can't list files: path doesn't exist or isn't a directory");
    }

    file_names.clear();
    std::vector<String> files;

    if (disk_base->exists(path) && disk_base->isDirectory(path)) {
        disk_base->listFiles(path, files);
        for (const String& file : files) {
            if (!isTracked(mergePath(path, file))) {
                file_names.push_back(file);
            }
        }
    }

    if (disk_overlay->exists(path)) {
        disk_overlay->listFiles(path, files);
        file_names.insert(file_names.end(), files.begin(), files.end());
    }
}

std::unique_ptr<ReadBufferFromFileBase> DiskOverlay::readFile(
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const {

    if (disk_base->exists(path)) {
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

    if (!exists(parentPath(path)) || !isDirectory(parentPath(path))) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can't write to file: parent path doesn't exist or is not a folder");
    }
    ensureHaveDirectories(parentPath(path));

    auto transaction = metadata->createTransaction();
    if (mode == WriteMode::Rewrite) {
        // This means that we don't need to look for this file on disk_base
        transaction->writeInlineDataToFile(dataPath(path), "r");
    } else {
        if (!disk_overlay->exists(path)) {
            // This means that the beginning of the file is on disk_base at the same path
            transaction->writeInlineDataToFile(dataPath(path), path);
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
    if (metadata->exists(dataPath(path))) {
        transaction->unlinkFile(dataPath(path));
    } else {
        if (disk_base->exists(path)) {
            setTracked(path); // This will ensure that this file will not appear in listing operations
        }
    }
    transaction->commit();
}

void DiskOverlay::removeDirectory(const String & path) {
    if (!exists(path) || !isDirectoryEmpty(path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot remove directory as path doesn't exist or is not an empty directory");
    }
    if (disk_base->exists(path)) {
        setTracked(path);
    }
    if (disk_overlay->exists(path)) {
        disk_overlay->removeDirectory(path);
    }
}

void DiskOverlay::removeRecursive(const String & dirpath) {
    if (!exists(dirpath)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot remove path as it doesn't exist");
    }

    std::vector<String> paths;
    listFiles(dirpath, paths);

    for (const String& path : paths) {
        String full_path = mergePath(dirpath, path);
        if (isFile(full_path)) {
            removeFile(full_path);
        } else {
            removeRecursive(full_path);
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
