#include "MetadataStorageFromStaticFilesWebServer.h"
#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/StaticDirectoryIterator.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <Common/escapeForFileName.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
}

MetadataStorageFromStaticFilesWebServer::MetadataStorageFromStaticFilesWebServer(
    const WebObjectStorage & object_storage_)
    : object_storage(object_storage_)
{
}

MetadataTransactionPtr MetadataStorageFromStaticFilesWebServer::createTransaction()
{
    return std::make_shared<MetadataStorageFromStaticFilesWebServerTransaction>(*this);
}

const std::string & MetadataStorageFromStaticFilesWebServer::getPath() const
{
    return root_path;
}

bool MetadataStorageFromStaticFilesWebServer::exists(const std::string & path) const
{
    return object_storage.exists(path);
}

void MetadataStorageFromStaticFilesWebServer::assertExists(const std::string & path) const
{
    if (!exists(path))
#ifdef NDEBUG
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "There is no path {}", path);
#else
    {
        std::string all_files;
        std::shared_lock shared_lock(object_storage.metadata_mutex);
        for (const auto & [file, _] : object_storage.files)
        {
            if (!all_files.empty())
                all_files += ", ";
            all_files += file;
        }
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "There is no path {} (available files: {})", path, all_files);
    }
#endif
}

bool MetadataStorageFromStaticFilesWebServer::isFile(const std::string & path) const
{
    assertExists(path);
    std::shared_lock shared_lock(object_storage.metadata_mutex);
    return object_storage.files.at(path).type == WebObjectStorage::FileType::File;
}

bool MetadataStorageFromStaticFilesWebServer::isDirectory(const std::string & path) const
{
    assertExists(path);
    std::shared_lock shared_lock(object_storage.metadata_mutex);
    return object_storage.files.at(path).type == WebObjectStorage::FileType::Directory;
}

uint64_t MetadataStorageFromStaticFilesWebServer::getFileSize(const String & path) const
{
    assertExists(path);
    std::shared_lock shared_lock(object_storage.metadata_mutex);
    return object_storage.files.at(path).size;
}

StoredObjects MetadataStorageFromStaticFilesWebServer::getStorageObjects(const std::string & path) const
{
    assertExists(path);

    auto fs_path = fs::path(object_storage.url) / path;
    std::string remote_path = fs_path.parent_path() / (escapeForFileName(fs_path.stem()) + fs_path.extension().string());
    remote_path = remote_path.substr(object_storage.url.size());

    std::shared_lock shared_lock(object_storage.metadata_mutex);
    return {StoredObject(remote_path, object_storage.files.at(path).size, path)};
}

std::vector<std::string> MetadataStorageFromStaticFilesWebServer::listDirectory(const std::string & path) const
{
    std::vector<std::string> result;
    std::shared_lock shared_lock(object_storage.metadata_mutex);
    for (const auto & [file_path, _] : object_storage.files)
    {
        if (file_path.starts_with(path))
            result.push_back(file_path);
    }
    return result;
}

DirectoryIteratorPtr MetadataStorageFromStaticFilesWebServer::iterateDirectory(const std::string & path) const
{
    std::vector<fs::path> dir_file_paths;

    if (!exists(path))
        return std::make_unique<StaticDirectoryIterator>(std::move(dir_file_paths));

    std::shared_lock shared_lock(object_storage.metadata_mutex);
    for (const auto & [file_path, _] : object_storage.files)
    {
        if (fs::path(parentPath(file_path)) / "" == fs::path(path) / "")
            dir_file_paths.emplace_back(file_path);
    }

    LOG_TRACE(object_storage.log, "Iterate directory {} with {} files", path, dir_file_paths.size());
    return std::make_unique<StaticDirectoryIterator>(std::move(dir_file_paths));
}

const IMetadataStorage & MetadataStorageFromStaticFilesWebServerTransaction::getStorageForNonTransactionalReads() const
{
    return metadata_storage;
}

void MetadataStorageFromStaticFilesWebServerTransaction::createDirectory(const std::string &)
{
    /// Noop.
}

void MetadataStorageFromStaticFilesWebServerTransaction::createDirectoryRecursive(const std::string &)
{
    /// Noop.
}

}
