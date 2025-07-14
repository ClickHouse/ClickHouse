#include <Disks/ObjectStorages/Web/WebObjectStorage.h>

#include <Common/logger_useful.h>
#include <Common/escapeForFileName.h>

#include <Core/ServerSettings.h>

#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>

#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/ReadBufferFromWebServer.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/IO/getThreadPoolReader.h>

#include <Storages/MergeTree/MergeTreeData.h>

#include <Poco/Exception.h>
#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int FILE_DOESNT_EXIST;
}

std::pair<WebObjectStorage::FileDataPtr, std::vector<fs::path>>
WebObjectStorage::loadFiles(const String & path, const std::unique_lock<std::shared_mutex> &) const
{
    std::vector<fs::path> loaded_files;
    auto full_url = fs::path(url) / path;

    LOG_TRACE(log, "Adding directory: {} ({})", path, full_url);

    FileDataPtr result;
    try
    {
        Poco::Net::HTTPBasicCredentials credentials{};

        auto timeouts = ConnectionTimeouts::getHTTPTimeouts(
            getContext()->getSettingsRef(),
            getContext()->getServerSettings());

        auto metadata_buf = BuilderRWBufferFromHTTP(Poco::URI(fs::path(full_url) / ".index"))
                                .withConnectionGroup(HTTPConnectionGroupType::DISK)
                                .withSettings(getContext()->getReadSettings())
                                .withTimeouts(timeouts)
                                .withHostFilter(&getContext()->getRemoteHostFilter())
                                .withSkipNotFound(true)
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

            LOG_TRACE(getLogger("DiskWeb"), "Adding file: {}, size: {}", file_path, size);
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


WebObjectStorage::WebObjectStorage(
    const String & url_,
    ContextPtr context_)
    : WithContext(context_->getGlobalContext())
    , url(url_)
    , log(getLogger("WebObjectStorage"))
{
}

bool WebObjectStorage::exists(const StoredObject & object) const
{
    return exists(object.remote_path);
}

bool WebObjectStorage::exists(const std::string & path) const
{
    LOG_TRACE(getLogger("DiskWeb"), "Checking existence of path: {}", path);
    return tryGetFileInfo(path) != nullptr;
}

WebObjectStorage::FileDataPtr WebObjectStorage::getFileInfo(const String & path) const
{
    auto file_info = tryGetFileInfo(path);
    if (!file_info)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "No such file: {}", path);
    return file_info;
}

std::vector<std::filesystem::path> WebObjectStorage::listDirectory(const String & path) const
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

WebObjectStorage::FileDataPtr WebObjectStorage::tryGetFileInfo(const String & path) const
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

std::unique_ptr<ReadBufferFromFileBase> WebObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    return std::make_unique<ReadBufferFromWebServer>(
        fs::path(url) / object.remote_path,
        getContext(),
        object.bytes_size,
        read_settings,
        read_settings.remote_read_buffer_use_external_buffer);
}

void WebObjectStorage::throwNotAllowed()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only read-only operations are supported in WebObjectStorage");
}

std::unique_ptr<WriteBufferFromFileBase> WebObjectStorage::writeObject( /// NOLINT
    const StoredObject & /* object */,
    WriteMode /* mode */,
    std::optional<ObjectAttributes> /* attributes */,
    size_t /* buf_size */,
    const WriteSettings & /* write_settings */)
{
    throwNotAllowed();
}

void WebObjectStorage::removeObjectIfExists(const StoredObject &)
{
    throwNotAllowed();
}

void WebObjectStorage::removeObjectsIfExist(const StoredObjects &)
{
    throwNotAllowed();
}

void WebObjectStorage::copyObject(const StoredObject &, const StoredObject &, const ReadSettings &, const WriteSettings &, std::optional<ObjectAttributes>) // NOLINT
{
    throwNotAllowed();
}

void WebObjectStorage::shutdown()
{
}

void WebObjectStorage::startup()
{
}

ObjectMetadata WebObjectStorage::getObjectMetadata(const std::string & /* path */) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Metadata is not supported for {}", getName());
}

}
