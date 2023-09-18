#include <Disks/ObjectStorages/Web/WebObjectStorage.h>

#include <Common/logger_useful.h>
#include <Common/escapeForFileName.h>

#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

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
}

void WebObjectStorage::initialize(const String & uri_path, const std::unique_lock<std::shared_mutex> & lock) const
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
            ConnectionTimeouts::getHTTPTimeouts(
                getContext()->getSettingsRef(),
                {getContext()->getConfigRef().getUInt("keep_alive_timeout", DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT), 0}),
            credentials,
            /* max_redirects= */ 0,
            /* buffer_size_= */ DBMS_DEFAULT_BUFFER_SIZE,
            getContext()->getReadSettings());

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
            LOG_TRACE(&Poco::Logger::get("DiskWeb"), "Adding file: {}, size: {}", file_path, file_data.size);

            files.emplace(std::make_pair(file_path, file_data));
        }

        files.emplace(std::make_pair(dir_name, FileData({ .type = FileType::Directory })));
    }
    catch (HTTPException & e)
    {
        /// 404 - no files
        if (e.getHTTPStatus() == Poco::Net::HTTPResponse::HTTP_NOT_FOUND)
            return;

        e.addMessage("while loading disk metadata");
        throw;
    }
    catch (Exception & e)
    {
        e.addMessage("while loading disk metadata");
        throw;
    }

    for (const auto & directory_path : directories_to_load)
        initialize(directory_path, lock);
}


WebObjectStorage::WebObjectStorage(
    const String & url_,
    ContextPtr context_)
    : WithContext(context_->getGlobalContext())
    , url(url_)
    , log(&Poco::Logger::get("WebObjectStorage"))
{
}

bool WebObjectStorage::exists(const StoredObject & object) const
{
    return exists(object.remote_path);
}

bool WebObjectStorage::exists(const std::string & path) const
{
    LOG_TRACE(&Poco::Logger::get("DiskWeb"), "Checking existence of path: {}", path);

    std::shared_lock shared_lock(metadata_mutex);

    if (files.find(path) == files.end())
    {
        shared_lock.unlock();
        std::unique_lock unique_lock(metadata_mutex);
        if (files.find(path) == files.end())
        {
            fs::path index_file_dir = fs::path(url) / path;
            if (index_file_dir.has_extension())
                index_file_dir = index_file_dir.parent_path();

            initialize(index_file_dir, unique_lock);
        }
        /// Files are never deleted from `files` as disk is read only, so no worry that we unlock now.
        unique_lock.unlock();
        shared_lock.lock();
    }

    if (files.empty())
        return false;

    if (files.contains(path))
        return true;

    /// `object_storage.files` contains files + directories only inside `metadata_path / uuid_3_digit / uuid /`
    /// (specific table files only), but we need to be able to also tell if `exists(<metadata_path>)`, for example.
    auto it = std::lower_bound(
        files.begin(), files.end(), path,
        [](const auto & file, const std::string & path_) { return file.first < path_; }
    );

    if (it == files.end())
        return false;

    if (startsWith(it->first, path)
        || (it != files.begin() && startsWith(std::prev(it)->first, path)))
        return true;

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
    auto read_buffer_creator =
         [this, read_settings]
         (const std::string & path_, size_t read_until_position) -> std::unique_ptr<ReadBufferFromFileBase>
     {
         return std::make_unique<ReadBufferFromWebServer>(
             fs::path(url) / path_,
             getContext(),
             read_settings,
             /* use_external_buffer */true,
             read_until_position);
     };

    auto global_context = Context::getGlobalContextInstance();

    switch (read_settings.remote_fs_method)
    {
        case RemoteFSReadMethod::read:
        {
            return std::make_unique<ReadBufferFromRemoteFSGather>(
                std::move(read_buffer_creator),
                StoredObjects{object},
                read_settings,
                global_context->getFilesystemCacheLog(),
                /* use_external_buffer */false);
        }
        case RemoteFSReadMethod::threadpool:
        {
            auto impl = std::make_unique<ReadBufferFromRemoteFSGather>(
                std::move(read_buffer_creator),
                StoredObjects{object},
                read_settings,
                global_context->getFilesystemCacheLog(),
                /* use_external_buffer */true);

            auto & reader = global_context->getThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
            return std::make_unique<AsynchronousBoundedReadBuffer>(
                std::move(impl), reader, read_settings,
                global_context->getAsyncReadCounters(),
                global_context->getFilesystemReadPrefetchesLog());
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
