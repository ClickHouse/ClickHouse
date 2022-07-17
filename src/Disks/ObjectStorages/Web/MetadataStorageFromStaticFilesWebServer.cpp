#include "MetadataStorageFromStaticFilesWebServer.h"
#include <Disks/IDisk.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int FILE_DOESNT_EXIST;
    extern const int NETWORK_ERROR;
}

class DiskWebServerDirectoryIterator final : public IDirectoryIterator
{
public:
    explicit DiskWebServerDirectoryIterator(std::vector<fs::path> && dir_file_paths_)
        : dir_file_paths(std::move(dir_file_paths_)), iter(dir_file_paths.begin()) {}

    void next() override { ++iter; }

    bool isValid() const override { return iter != dir_file_paths.end(); }

    String path() const override { return iter->string(); }

    String name() const override { return iter->filename(); }

private:
    std::vector<fs::path> dir_file_paths;
    std::vector<fs::path>::iterator iter;
};


MetadataStorageFromStaticFilesWebServer::MetadataStorageFromStaticFilesWebServer(
    const WebObjectStorage & object_storage_)
    : object_storage(object_storage_)
{
}

MetadataTransactionPtr MetadataStorageFromStaticFilesWebServer::createTransaction() const
{
    return std::make_shared<MetadataStorageFromStaticFilesWebServerTransaction>(*this);
}

const std::string & MetadataStorageFromStaticFilesWebServer::getPath() const
{
    return root_path;
}

bool MetadataStorageFromStaticFilesWebServer::exists(const std::string & path) const
{
    return object_storage.files.contains(path);
}

void MetadataStorageFromStaticFilesWebServer::assertExists(const std::string & path) const
{
    initializeIfNeeded(path);

    if (!exists(path))
#ifdef NDEBUG
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "There is no path {}", path);
#else
    {
        std::string all_files;
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
    return object_storage.files.at(path).type == WebObjectStorage::FileType::File;
}

bool MetadataStorageFromStaticFilesWebServer::isDirectory(const std::string & path) const
{
    assertExists(path);
    return object_storage.files.at(path).type == WebObjectStorage::FileType::Directory;
}

uint64_t MetadataStorageFromStaticFilesWebServer::getFileSize(const String & path) const
{
    assertExists(path);
    return object_storage.files.at(path).size;
}

StoredObjects MetadataStorageFromStaticFilesWebServer::getStorageObjects(const std::string & path) const
{
    assertExists(path);
    return {StoredObject::create(object_storage, path, object_storage.files.at(path).size, true)};
}

std::vector<std::string> MetadataStorageFromStaticFilesWebServer::listDirectory(const std::string & path) const
{
    std::vector<std::string> result;
    for (const auto & [file_path, _] : object_storage.files)
    {
        if (file_path.starts_with(path))
            result.push_back(file_path);
    }
    return result;
}

bool MetadataStorageFromStaticFilesWebServer::initializeIfNeeded(const std::string & path) const
{
    if (object_storage.files.find(path) == object_storage.files.end())
    {
        try
        {
            object_storage.initialize(fs::path(object_storage.url) / path);
        }
        catch (...)
        {
            const auto message = getCurrentExceptionMessage(false);
            bool can_throw = CurrentThread::isInitialized() && CurrentThread::get().getQueryContext();
            if (can_throw)
                throw Exception(ErrorCodes::NETWORK_ERROR, "Cannot load disk metadata. Error: {}", message);

            LOG_TRACE(&Poco::Logger::get("DiskWeb"), "Cannot load disk metadata. Error: {}", message);
            return false;
        }
    }

    return true;
}

DirectoryIteratorPtr MetadataStorageFromStaticFilesWebServer::iterateDirectory(const std::string & path) const
{
    std::vector<fs::path> dir_file_paths;

    if (!initializeIfNeeded(path))
        return std::make_unique<DiskWebServerDirectoryIterator>(std::move(dir_file_paths));

    assertExists(path);

    for (const auto & [file_path, _] : object_storage.files)
        if (parentPath(file_path) == path)
            dir_file_paths.emplace_back(file_path);

    LOG_TRACE(object_storage.log, "Iterate directory {} with {} files", path, dir_file_paths.size());
    return std::make_unique<DiskWebServerDirectoryIterator>(std::move(dir_file_paths));
}

std::string MetadataStorageFromStaticFilesWebServer::readFileToString(const std::string &) const
{
    WebObjectStorage::throwNotAllowed();
}

Poco::Timestamp MetadataStorageFromStaticFilesWebServer::getLastModified(const std::string &) const
{
    return {};
}

time_t MetadataStorageFromStaticFilesWebServer::getLastChanged(const std::string &) const
{
    return {};
}

uint32_t MetadataStorageFromStaticFilesWebServer::getHardlinkCount(const std::string &) const
{
    return 1;
}

const IMetadataStorage & MetadataStorageFromStaticFilesWebServerTransaction::getStorageForNonTransactionalReads() const
{
    return metadata_storage;
}

void MetadataStorageFromStaticFilesWebServerTransaction::writeStringToFile(const std::string &, const std::string &)
{
    WebObjectStorage::throwNotAllowed();
}

void MetadataStorageFromStaticFilesWebServerTransaction::setLastModified(const std::string &, const Poco::Timestamp &)
{
    WebObjectStorage::throwNotAllowed();
}

void MetadataStorageFromStaticFilesWebServerTransaction::unlinkFile(const std::string &)
{
    WebObjectStorage::throwNotAllowed();
}

void MetadataStorageFromStaticFilesWebServerTransaction::removeRecursive(const std::string &)
{
    WebObjectStorage::throwNotAllowed();
}

void MetadataStorageFromStaticFilesWebServerTransaction::removeDirectory(const std::string &)
{
    WebObjectStorage::throwNotAllowed();
}

void MetadataStorageFromStaticFilesWebServerTransaction::moveFile(const std::string &, const std::string &)
{
    WebObjectStorage::throwNotAllowed();
}

void MetadataStorageFromStaticFilesWebServerTransaction::moveDirectory(const std::string &, const std::string &)
{
    WebObjectStorage::throwNotAllowed();
}

void MetadataStorageFromStaticFilesWebServerTransaction::replaceFile(const std::string &, const std::string &)
{
    WebObjectStorage::throwNotAllowed();
}

void MetadataStorageFromStaticFilesWebServerTransaction::setReadOnly(const std::string &)
{
    WebObjectStorage::throwNotAllowed();
}

void MetadataStorageFromStaticFilesWebServerTransaction::createHardLink(const std::string &, const std::string &)
{
    WebObjectStorage::throwNotAllowed();
}

void MetadataStorageFromStaticFilesWebServerTransaction::addBlobToMetadata(const std::string &, const std::string &, uint64_t)
{
    WebObjectStorage::throwNotAllowed();
}

void MetadataStorageFromStaticFilesWebServerTransaction::unlinkMetadata(const std::string &)
{
    WebObjectStorage::throwNotAllowed();
}

void MetadataStorageFromStaticFilesWebServerTransaction::createDirectory(const std::string &)
{
    /// Noop.
}

void MetadataStorageFromStaticFilesWebServerTransaction::createDirectoryRecursive(const std::string &)
{
    /// Noop.
}

void MetadataStorageFromStaticFilesWebServerTransaction::createEmptyMetadataFile(const std::string & /* path */)
{
    /// Noop.
}

void MetadataStorageFromStaticFilesWebServerTransaction::createMetadataFile(
    const std::string & /* path */, const std::string & /* blob_name */, uint64_t /* size_in_bytes */)
{
    /// Noop.
}

void MetadataStorageFromStaticFilesWebServerTransaction::commit()
{
    /// Noop.
}

std::unordered_map<String, String> MetadataStorageFromStaticFilesWebServer::getSerializedMetadata(const std::vector<String> &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "getSerializedMetadata is not implemented for MetadataStorageFromStaticFilesWebServer");
}

}
