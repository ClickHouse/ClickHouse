#include "DiskOverlay.h"
#include "DiskFactory.h"
#include "loadLocalDiskConfig.h"

#include <iostream>

namespace DB {

// DiskOverlay::DiskOverlay(const String & name_, std::shared_ptr<IDisk> disk_base_, std::shared_ptr<IDisk> disk_overlay_) :
//     IDisk(name_), disk_base(disk_base_), disk_overlay(disk_overlay_)
// {
// }

DiskOverlay::DiskOverlay(const String & name_, const Poco::Util::AbstractConfiguration & config_, const String & config_prefix_, const DisksMap & map_) : IDisk(name_) {
    String disk_base_name = config_.getString(config_prefix_ + ".disk_base");
    String disk_overlay_name = config_.getString(config_prefix_ + ".disk_overlay");
    String metadata_name = config_.getString(config_prefix_ + ".metadata");

    // std::cerr << "GO" << std::endl;
    // for (auto [name, ptr] : map_) {
    //     std::cerr << name << std::endl;
    // }
    // std::cerr << "Mine: " << config_prefix_ << std::endl;

    disk_base = map_.at(disk_base_name);
    disk_overlay = map_.at(disk_overlay_name);
    metadata = MetadataStorageFactory;

    if (disk_overlay -> isReadOnly()) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Overlay disk has to be writable");
    }
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

bool DiskOverlay::isReplaced(const String& path) const {
    auto it = overlay_info.find(path);
    if (it != overlay_info.end()) {
        return it->second.type == OverlayInfo::Type::rewrite;
    }
    return false;
}

void DiskOverlay::setReplaced(const String& path) {
    overlay_info[path].type = OverlayInfo::Type::rewrite;
    overlay_info[path].orig_path = {};
}

bool DiskOverlay::exists(const String & path) const {
    return disk_overlay->exists(path) || (!isReplaced(path) && disk_base->exists(basePath(path)));
}

bool DiskOverlay::isFile(const String & path) const {
    return disk_overlay->isFile(path) && (!isReplaced(path) && disk_base->isFile(basePath(path)));
}

bool DiskOverlay::isDirectory(const String & path) const {
    return disk_overlay->isDirectory(path) && (!isReplaced(path) && disk_base->isDirectory(basePath(path)));
}

size_t DiskOverlay::getFileSize(const String & path) const {
    size_t size = disk_overlay->getFileSize(path);
    if (!isReplaced(path)) {
        size += disk_base->getFileSize(basePath(path));
    }
    return size;
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
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot create directories: path already exists");
    }
    setReplaced(path);
    disk_overlay->clearDirectory(path);
}

void DiskOverlay::movePath(const String & path, const String & new_path) {
    overlay_info[new_path] = overlay_info[path];
    overlay_info.erase(path);
}

String DiskOverlay::basePath(const String& path) const {
    auto iter = overlay_info.find(path);
    if (iter == overlay_info.end()) {
        return path;
    }
    if (iter->second.orig_path) {
        return *iter->second.orig_path;
    }
    return path;
}

void DiskOverlay::moveDirectory(const String & from_path, const String & to_path) {
    if (!exists(from_path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move directory: from_path doesn't exist");
    }
    if (exists(to_path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move directory: to_path already exists");
    }
    movePath(from_path, to_path);
    disk_overlay->moveDirectory(from_path, to_path);
}

DirectoryIteratorPtr DiskOverlay::iterateDirectory(const String & path) const {
    if (isReplaced(path)) {
        return disk_overlay->iterateDirectory(path);
    }
    return disk_base->iterateDirectory(path); // TODO write new directory iterator
}

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
    movePath(from_path, to_path);
    disk_overlay->moveFile(from_path, to_path);
}

void DiskOverlay::replaceFile(const String & from_path, const String & to_path) {
    if (!exists(from_path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot replace old file with new: new doesn't exist");
    }
    movePath(from_path, to_path);
    disk_overlay->replaceFile(from_path, to_path);
}

void DiskOverlay::listFiles(const String & path, std::vector<String> & file_names) const {
    if (!isReplaced(path)) {
        disk_base->listFiles(basePath(path), file_names);
    }
    disk_overlay->listFiles(path, file_names);
    // This doesn't work since same file maybe listed by the same name
    // or by different names, some files may be in another directory already
    // TODO
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
}

std::unique_ptr<WriteBufferFromFileBase> DiskOverlay::writeFile(
        const String & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings) {

    if (!isReplaced(path)) {
        return disk_base->writeFile(path, buf_size, mode, settings);
    }
    return disk_overlay->writeFile(path, buf_size, mode, settings);
}

void DiskOverlay::removeFile(const String & path) {
    if (!exists(path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot remove file as it doesn't exist");
    }
    disk_overlay->removeFileIfExists(path); // Not removeFile since file can be only on base disk
    setReplaced(path);
}
void DiskOverlay::removeFileIfExists(const String & path) {
    disk_overlay->removeFileIfExists(path);
    setReplaced(path);
}
void DiskOverlay::removeDirectory(const String & path) {
    if (!isDirectoryEmpty(path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot remove directory as path is not an empty directory");
    }
    setReplaced(path);
    disk_overlay->removeDirectory(path);
}

void DiskOverlay::removeRecursive(const String & path) {
    if (!exists(path)) {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot remove path as it doesn't exist");
    }
    setReplaced(path);
    if (disk_overlay->exists(path)) {
        disk_overlay->removeRecursive(path);
    }
}

void registerDiskOverlay(DiskFactory & factory, bool global_skip_access_check)
{
    auto creator = [global_skip_access_check](
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & map) -> DiskPtr
    {
        std::shared_ptr<IDisk> disk = std::make_shared<DiskOverlay>(name, config, config_prefix, map);

        bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);
        disk->startup(context, skip_access_check);
        return disk;
    };
    factory.registerDiskType("overlay", creator);
}

}
