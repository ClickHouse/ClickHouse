#include "DiskRemote.h"
#include <Common/createHardLink.h>
#include "DiskFactory.h"

#include <Disks/LocalDirectorySyncGuard.h>
#include <Interpreters/Context.h>
#include <Common/filesystemHelpers.h>
#include <Common/quoteString.h>
#include <Common/atomicRename.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Disks/ObjectStorages/LocalObjectStorage.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/ObjectStorages/FakeMetadataStorageFromDisk.h>

#include <fstream>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

#include <Disks/DiskFactory.h>
#include <Common/randomSeed.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromTemporaryFile.h>
#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>

namespace DB
{
//
//const String & DiskRemote::getPath() const
//{
//    return ""; // TODO Придумать какой путь
//}
//UInt64 DiskRemote::getTotalSpace() const
//{
//    return 0; // TODO запрашивать
//}
//
//UInt64 DiskRemote::getAvailableSpace() const {
//    return 0; // TODO запрашивать
//}
//
//UInt64 DiskRemote::getUnreservedSpace() const
//{
//    std::lock_guard lock(reservation_mutex);
//    auto available_space = getAvailableSpace();
//    available_space -= std::min(available_space, reserved_bytes);
//    return available_space;
//}
//
//bool DiskRemote::exists(const String & path) const {
//    return false; // TODO запрашивать
//}
//
//bool DiskRemote::isFile(const String & path) const {
//    return false; // TODO запрашивать
//}
//
//bool DiskRemote::isDirectory(const String & path) const {
//    return false; // TODO запрашивать
//}
//
//size_t DiskRemote::getFileSize(const String & path) const {
//    return 0; // TODO запрашивать
//}
//
//void DiskRemote::createDirectory(const String & path) {
//    // TODO запрашивать
//}
//
//void DiskRemote::createDirectories(const String & path) {
//    // TODO запрашивать
//}
//
//void DiskRemote::clearDirectory(const String & path) {
//    // TODO запрашивать
//}
//
//void DiskRemote::moveDirectory(const String & from_path, const String & to_path) {
//    // TODO запрашивать
//}
//
//DirectoryIteratorPtr DiskRemote::iterateDirectory(const String & path) const {
//    return {}; // TODO запрашивать
//}
//
//void DiskRemote::createFile(const String & path) {
//    // TODO запрашивать
//}
//
//void DiskRemote::moveFile(const String & from_path, const String & to_path) {
//    // TODO запрашивать
//}
//
//void DiskRemote::replaceFile(const String & from_path, const String & to_path) {
//    // TODO запрашивать
//}
//
//void DiskRemote::copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path) {
//    // TODO использовать IDisk реализацию если диски разные
//}
//
//void DiskRemote::copyDirectoryContent(const String & from_dir, const std::shared_ptr<IDisk> & to_disk, const String & to_dir) {
//    // TODO использовать IDisk реализацию если диски разные
//}
//
//void DiskRemote::listFiles(const String & path, std::vector<String> & file_names) const {
//    // TODO запрашивать
//}
//
//std::unique_ptr<ReadBufferFromFileBase> DiskRemote::readFile(
//    const String & path,
//    const ReadSettings & settings,
//    std::optional<size_t> read_hint,
//    std::optional<size_t> file_size) const {
//    return {}; // TODO запрашивать
//}
//
//std::unique_ptr<WriteBufferFromFileBase> DiskRemote::writeFile(
//    const String & path,
//    size_t buf_size,
//    WriteMode mode,
//    const WriteSettings & settings) {
//    return {}; // TODO запрашивать
//}
//
//void DiskRemote::removeFile(const String & path) {
//    // TODO запрашивать
//}
//
//void DiskRemote::removeFileIfExists(const String & path) {
//    // TODO запрашивать
//}
//
//void DiskRemote::removeDirectory(const String & path) {
//    // TODO запрашивать
//}
//
//void DiskRemote::removeRecursive(const String & path) {
//    // TODO запрашивать
//}
//
//void DiskRemote::setLastModified(const String & path, const Poco::Timestamp & timestamp) {
//    // TODO запрашивать
//}
//
//Poco::Timestamp DiskRemote::getLastModified(const String & path) const {
//    return {};// TODO запрашивать
//}
//
//time_t DiskRemote::getLastChanged(const String & path) const {
//    return 0;// TODO запрашивать
//}
//
//void DiskRemote::setReadOnly(const String & path) {
//    // TODO запрашивать
//}
//
//void DiskRemote::createHardLink(const String & src_path, const String & dst_path) {
//    // TODO запрашивать
//}
//
//void DiskRemote::truncateFile(const String & path, size_t size) {
//    // TODO запрашивать
//}
//
//DataSourceDescription DiskRemote::getDataSourceDescription() const {
//    return {}; // TODO описывается локально
//}
//
//void DiskRemote::shutdown() {
//    // TODO прикрутить дисконнект
//}
//
//void DiskRemote::startupImpl(ContextPtr context) {
//    // TODO прикрутить коннект
//}
//
//void DiskRemote::applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr context, const String & config_prefix, const DisksMap &) {
//    // TODO
//}
//
//
//
//void registerDiskLocal(DiskFactory & factory, bool global_skip_access_check)
//{
//    auto creator = [global_skip_access_check](
//        const String & name,
//        const Poco::Util::AbstractConfiguration & config,
//        const String & config_prefix,
//        ContextPtr context,
//        const DisksMap & map) -> DiskPtr
//    {
//        String path;
//        UInt64 keep_free_space_bytes;
////        loadDiskLocalConfig(name, config, config_prefix, context, path, keep_free_space_bytes);
//
//        for (const auto & [disk_name, disk_ptr] : map)
//            if (path == disk_ptr->getPath())
//                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk {} and disk {} cannot have the same path ({})", name, disk_name, path);
//
//        bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);
//        std::shared_ptr<IDisk> disk
//            = std::make_shared<DiskLocal>(name, path, keep_free_space_bytes, context, config.getUInt("local_disk_check_period_ms", 0));
//        disk->startup(context, skip_access_check);
//        return disk;
//    };
//    factory.registerDiskType("local", creator);
//}

} // DB
