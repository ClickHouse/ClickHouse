#include <Disks/DiskObjectStorage/MetadataStorages/Web/MetadataStorageFromStaticFilesWebServer.h>
#include <Disks/IDisk.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StaticDirectoryIterator.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Interpreters/Context.h>
#include <Storages/PartitionCommands.h>
#include <Common/escapeForFileName.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>

#include <Poco/Exception.h>

#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
}

namespace fs = std::filesystem;

MetadataStorageFromStaticFilesWebServer::MetadataStorageFromStaticFilesWebServer(
    const WebObjectStorage & object_storage_)
    : object_storage(object_storage_)
    , log(getLogger("MetadataStorageFromStaticFilesWebServer"))
{
}

MetadataTransactionPtr MetadataStorageFromStaticFilesWebServer::createTransaction()
{
    throwNotImplemented();
}

const std::string & MetadataStorageFromStaticFilesWebServer::getPath() const
{
    static const String no_root;
    return no_root;
}

bool MetadataStorageFromStaticFilesWebServer::existsFileOrDirectory(const std::string & path) const
{
    return tryGetFileInfo(path) != nullptr;
}

void MetadataStorageFromStaticFilesWebServer::assertExists(const std::string & path) const
{
    if (!existsFileOrDirectory(path))
#ifdef NDEBUG
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "There is no path {}", path);
#else
    {
        std::string all_files;
        std::shared_lock shared_lock(metadata_mutex);
        for (const auto & [file, _] : files)
        {
            if (!all_files.empty())
                all_files += ", ";
            all_files += file;
        }
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "There is no path {} (available files: {})", path, all_files);
    }
#endif
}

bool MetadataStorageFromStaticFilesWebServer::existsFile(const std::string & path) const
{
    auto file_info = tryGetFileInfo(path);
    return file_info && file_info->type == FileType::File;
}

bool MetadataStorageFromStaticFilesWebServer::existsDirectory(const std::string & path) const
{
    auto file_info = tryGetFileInfo(path);
    return file_info && file_info->type == FileType::Directory;
}

uint64_t MetadataStorageFromStaticFilesWebServer::getFileSize(const String & path) const
{
    assertExists(path);
    auto file_info = getFileInfo(path);
    return file_info->size;
}

std::optional<uint64_t> MetadataStorageFromStaticFilesWebServer::getFileSizeIfExists(const String & path) const
{
    auto file_info = tryGetFileInfo(path);
    if (file_info)
        return file_info->size;
    else
        return std::nullopt;
}

StoredObjects MetadataStorageFromStaticFilesWebServer::getStorageObjects(const std::string & path) const
{
    assertExists(path);

    auto fs_path = fs::path(object_storage.getBaseURL()) / path;
    std::string remote_path = fs_path.parent_path() / (escapeForFileName(fs_path.stem()) + fs_path.extension().string());
    remote_path = remote_path.substr(object_storage.getBaseURL().size());

    auto file_info = getFileInfo(path);
    return {StoredObject(remote_path, path, file_info->size)};
}

std::optional<StoredObjects> MetadataStorageFromStaticFilesWebServer::getStorageObjectsIfExist(const std::string & path) const
{
    auto fs_path = fs::path(object_storage.getBaseURL()) / path;
    std::string remote_path = fs_path.parent_path() / (escapeForFileName(fs_path.stem()) + fs_path.extension().string());
    remote_path = remote_path.substr(object_storage.getBaseURL().size());

    if (auto file_info = tryGetFileInfo(path))
        return StoredObjects{StoredObject(remote_path, path, file_info->size)};
    return std::nullopt;
}

std::vector<std::string> MetadataStorageFromStaticFilesWebServer::listDirectory(const std::string & path) const
{
    std::vector<std::string> result;
    std::shared_lock shared_lock(metadata_mutex);
    for (const auto & [file_path, _] : files)
    {
        if (file_path.starts_with(path))
            result.push_back(file_path); /// It looks more like recursive listing, not sure it is right
    }
    return result;
}

DirectoryIteratorPtr MetadataStorageFromStaticFilesWebServer::iterateDirectory(const std::string & path) const
{
    std::vector<fs::path> dir_file_paths;

    if (!existsDirectory(path))
        return std::make_unique<StaticDirectoryIterator>(std::move(dir_file_paths));

    dir_file_paths = listDirectoryInternal(path);
    LOG_TRACE(log, "Iterate directory {} with {} files", path, dir_file_paths.size());
    return std::make_unique<StaticDirectoryIterator>(std::move(dir_file_paths));
}

std::pair<MetadataStorageFromStaticFilesWebServer::FileDataPtr, std::vector<fs::path>>
MetadataStorageFromStaticFilesWebServer::loadFiles(const String & path, const std::unique_lock<SharedMutex> &) const
{
    std::vector<fs::path> loaded_files;
    auto full_url = fs::path(object_storage.getBaseURL()) / path;

    LOG_TRACE(log, "Adding directory: {} ({})", path, full_url);

    FileDataPtr result;
    try
    {
        Poco::Net::HTTPBasicCredentials credentials{};

        auto timeouts = ConnectionTimeouts::getHTTPTimeouts(
            object_storage.getContext()->getSettingsRef(),
            object_storage.getContext()->getServerSettings());

        auto metadata_buf = BuilderRWBufferFromHTTP(Poco::URI(fs::path(full_url) / ".index"))
                                .withConnectionGroup(HTTPConnectionGroupType::DISK)
                                .withSettings(object_storage.getContext()->getReadSettings())
                                .withTimeouts(timeouts)
                                .withHostFilter(&object_storage.getContext()->getRemoteHostFilter())
                                .withSkipNotFound(true)
                                .withHeaders(object_storage.getHeaders())
                                .create(credentials);

        String file_name;

        while (!metadata_buf->eof())
        {
            readText(file_name, *metadata_buf);
            assertChar('\t', *metadata_buf);

            bool is_directory;
            readBoolText(is_directory, *metadata_buf);
            size_t size = 0;
            if (!is_directory)
            {
                assertChar('\t', *metadata_buf);
                readIntText(size, *metadata_buf);
            }
            assertChar('\n', *metadata_buf);

            FileDataPtr file_data = is_directory
                ? FileData::createDirectoryInfo(false)
                : FileData::createFileInfo(size);

            auto file_path = fs::path(path) / file_name;
            const bool inserted = files.add(file_path, file_data).second;
            if (!inserted)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Loading data for {} more than once", file_path);

            LOG_TRACE(log, "Adding file: {}, size: {}", file_path, size);
            loaded_files.emplace_back(file_path);
        }

        /// Check for not found url after read attempt, because of delayed initialization.
        if (metadata_buf->hasNotFoundURL())
            return {};

        auto [it, inserted] = files.add(path, FileData::createDirectoryInfo(true));
        if (!inserted)
        {
             if (it->second->loaded_children)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Loading data for {} more than once", path);

             it->second->loaded_children = true;
        }

        return std::pair(it->second, loaded_files);
    }
    catch (HTTPException & e)
    {
        e.addMessage("while loading disk metadata");
        throw;
    }
    catch (Exception & e)
    {
        e.addMessage("while loading disk metadata");
        throw;
    }
}

MetadataStorageFromStaticFilesWebServer::FileDataPtr MetadataStorageFromStaticFilesWebServer::tryGetFileInfo(const String & path) const
{
    std::shared_lock shared_lock(metadata_mutex);

    bool is_file = fs::path(path).has_extension();
    if (auto it = files.find(path, is_file); it != files.end())
        return it->second;

    if (is_file)
    {
        shared_lock.unlock();

        const auto parent_path = fs::path(path).parent_path();
        auto parent_info = tryGetFileInfo(parent_path);
        if (!parent_info)
        {
            return nullptr;
        }

        if (!parent_info->loaded_children)
        {
            std::unique_lock unique_lock(metadata_mutex);
            if (!parent_info->loaded_children)
                loadFiles(parent_path, unique_lock);
        }

        shared_lock.lock();

        if (auto jt = files.find(path, is_file); jt != files.end())
            return jt->second;

        return nullptr;
    }

    auto it = std::lower_bound(
        files.begin(), files.end(), path, [](const auto & file, const std::string & path_) { return file.first < path_; });
    if (it != files.end())
    {
        if (startsWith(it->first, path) || (it != files.begin() && startsWith(std::prev(it)->first, path)))
        {
            shared_lock.unlock();
            std::unique_lock unique_lock(metadata_mutex);

            /// Add this directory path not files cache to simplify further checks for this path.
            return files.add(path, FileData::createDirectoryInfo(false)).first->second;
        }
    }

    shared_lock.unlock();
    std::unique_lock unique_lock(metadata_mutex);

    if (auto jt = files.find(path, is_file); jt != files.end())
        return jt->second;
    return loadFiles(path, unique_lock).first;
}

std::vector<std::filesystem::path> MetadataStorageFromStaticFilesWebServer::listDirectoryInternal(const String & path) const
{
    auto file_info = tryGetFileInfo(path);
    if (!file_info)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "No such file: {}", path);

    if (file_info->type != FileType::Directory)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File {} is not a directory", path);

    std::vector<std::filesystem::path> result;
    if (!file_info->loaded_children)
    {
        std::unique_lock unique_lock(metadata_mutex);
        if (!file_info->loaded_children)
            return loadFiles(path, unique_lock).second;
    }
    std::shared_lock shared_lock(metadata_mutex);
    for (const auto & [file_path, _] : files)
    {
        if (fs::path(parentPath(file_path)) / "" == fs::path(path) / "")
            result.emplace_back(file_path);
    }
    return result;
}

MetadataStorageFromStaticFilesWebServer::FileDataPtr MetadataStorageFromStaticFilesWebServer::getFileInfo(const String & path) const
{
    auto file_info = tryGetFileInfo(path);
    if (!file_info)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "No such file: {}", path);
    return file_info;
}

}
