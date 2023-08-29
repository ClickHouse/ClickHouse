#include <Disks/ObjectStorages/Web/WebObjectStorage.h>

#include <Common/logger_useful.h>
#include <Common/escapeForFileName.h>

#include <IO/ConnectionTimeoutsContext.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/SeekAvoidingReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Disks/IO/ReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/WriteIndirectBufferFromRemoteFS.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/ReadBufferFromWebServer.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>

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
    extern const int NETWORK_ERROR;
}

void WebObjectStorage::initialize(const String & uri_path) const
{
    std::vector<String> directories_to_load;
    LOG_TRACE(log, "Loading metadata for directory: {}", uri_path);

    try
    {
        Poco::Net::HTTPBasicCredentials credentials{};
        ReadWriteBufferFromHTTP metadata_buf(
            Poco::URI(fs::path(uri_path) / ".index"),
            Poco::Net::HTTPRequest::HTTP_GET,
            ReadWriteBufferFromHTTP::OutStreamCallback(),
            ConnectionTimeouts::getHTTPTimeouts(getContext()),
            credentials);

        String file_name;
        FileData file_data{};

        String dir_name = fs::path(uri_path.substr(url.size())) / "";
        LOG_TRACE(&Poco::Logger::get("DiskWeb"), "Adding directory: {}", dir_name);

        while (!metadata_buf.eof())
        {
            readText(file_name, metadata_buf);
            assertChar('\t', metadata_buf);

            bool is_directory;
            readBoolText(is_directory, metadata_buf);
            if (!is_directory)
            {
                assertChar('\t', metadata_buf);
                readIntText(file_data.size, metadata_buf);
            }
            assertChar('\n', metadata_buf);

            file_data.type = is_directory ? FileType::Directory : FileType::File;
            String file_path = fs::path(uri_path) / file_name;
            if (file_data.type == FileType::Directory)
            {
                directories_to_load.push_back(file_path);
            }

            file_path = file_path.substr(url.size());
            files.emplace(std::make_pair(file_path, file_data));
            LOG_TRACE(&Poco::Logger::get("DiskWeb"), "Adding file: {}, size: {}", file_path, file_data.size);
        }

        files.emplace(std::make_pair(dir_name, FileData({ .type = FileType::Directory })));
    }
    catch (Exception & e)
    {
        e.addMessage("while loading disk metadata");
        throw;
    }

    for (const auto & directory_path : directories_to_load)
        initialize(directory_path);
}


WebObjectStorage::WebObjectStorage(
    const String & url_,
    ContextPtr context_)
    : WithContext(context_->getBufferContext())
    , url(url_)
    , log(&Poco::Logger::get("WebObjectStorage"))
{
}

bool WebObjectStorage::exists(const StoredObject & object) const
{
    const auto & path = object.absolute_path;

    LOG_TRACE(&Poco::Logger::get("DiskWeb"), "Checking existence of path: {}", path);

    if (files.find(path) != files.end())
        return true;

    if (path.ends_with(MergeTreeData::FORMAT_VERSION_FILE_NAME) && files.find(fs::path(path).parent_path() / "") == files.end())
    {
        try
        {
            initialize(fs::path(url) / fs::path(path).parent_path());
            return files.find(path) != files.end();
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

    return false;
}

std::unique_ptr<ReadBufferFromFileBase> WebObjectStorage::readObjects( /// NOLINT
    const StoredObjects & objects,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
    if (objects.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "WebObjectStorage support read only from single object");

    return readObject(objects[0], read_settings, read_hint, file_size);

}

std::unique_ptr<ReadBufferFromFileBase> WebObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{
    const auto & path = object.absolute_path;
    LOG_TRACE(log, "Read from path: {}", path);

    auto iter = files.find(path);
    if (iter == files.end())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File path {} does not exist", path);

    auto fs_path = fs::path(url) / path;
    auto remote_path = fs_path.parent_path() / (escapeForFileName(fs_path.stem()) + fs_path.extension().string());
    remote_path = remote_path.string().substr(url.size());

    StoredObjects objects;
    objects.emplace_back(remote_path, iter->second.size);

    auto read_buffer_creator =
         [this, read_settings]
         (const std::string & path_, size_t read_until_position) -> std::shared_ptr<ReadBufferFromFileBase>
     {
         return std::make_shared<ReadBufferFromWebServer>(
             fs::path(url) / path_,
             getContext(),
             read_settings,
             /* use_external_buffer */true,
             read_until_position);
     };

    auto web_impl = std::make_unique<ReadBufferFromRemoteFSGather>(std::move(read_buffer_creator), objects, read_settings);

    if (read_settings.remote_fs_method == RemoteFSReadMethod::threadpool)
    {
        auto reader = IObjectStorage::getThreadPoolReader();
        return std::make_unique<AsynchronousReadIndirectBufferFromRemoteFS>(reader, read_settings, std::move(web_impl), min_bytes_for_seek);
    }
    else
    {
        auto buf = std::make_unique<ReadIndirectBufferFromRemoteFS>(std::move(web_impl));
        return std::make_unique<SeekAvoidingReadBuffer>(std::move(buf), min_bytes_for_seek);
    }
}

void WebObjectStorage::listPrefix(const std::string & path, RelativePathsWithSize & children) const
{
    for (const auto & [file_path, file_info] : files)
    {
        if (file_info.type == FileType::File && file_path.starts_with(path))
        {
            children.emplace_back(file_path, file_info.size);
        }
    }
}

void WebObjectStorage::throwNotAllowed()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Only read-only operations are supported");
}

std::unique_ptr<WriteBufferFromFileBase> WebObjectStorage::writeObject( /// NOLINT
    const StoredObject & /* object */,
    WriteMode /* mode */,
    std::optional<ObjectAttributes> /* attributes */,
    FinalizeCallback && /* finalize_callback */,
    size_t /* buf_size */,
    const WriteSettings & /* write_settings */)
{
    throwNotAllowed();
}

void WebObjectStorage::removeObject(const StoredObject &)
{
    throwNotAllowed();
}

void WebObjectStorage::removeObjects(const StoredObjects &)
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

void WebObjectStorage::copyObject(const StoredObject &, const StoredObject &, std::optional<ObjectAttributes>) // NOLINT
{
    throwNotAllowed();
}

void WebObjectStorage::shutdown()
{
}

void WebObjectStorage::startup()
{
}

void WebObjectStorage::applyNewSettings(
    const Poco::Util::AbstractConfiguration & /* config */, const std::string & /* config_prefix */, ContextPtr /* context */)
{
}

ObjectMetadata WebObjectStorage::getObjectMetadata(const std::string & /* path */) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Metadata is not supported for {}", getName());
}

std::unique_ptr<IObjectStorage> WebObjectStorage::cloneObjectStorage(
    const std::string & /* new_namespace */,
    const Poco::Util::AbstractConfiguration & /* config */,
    const std::string & /* config_prefix */, ContextPtr /* context */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "cloneObjectStorage() is not implemented for WebObjectStorage");
}

}
