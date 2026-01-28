#include <Disks/DiskObjectStorage/ObjectStorages/Web/WebObjectStorage.h>

#include <Disks/DiskObjectStorage/MetadataStorages/Web/MetadataStorageFromIndexPages.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>

#include <Disks/IO/ReadBufferFromWebServer.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Poco/Timestamp.h>

#include <deque>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int FILE_DOESNT_EXIST;
}

WebObjectStorage::WebObjectStorage(
    const String & url_,
    const String & query_fragment_,
    ContextPtr context_,
    HTTPHeaderEntries headers_)
    : WithContext(context_->getGlobalContext())
    , url(url_)
    , query_fragment(query_fragment_)
    , headers(std::move(headers_))
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
    return tryGetObjectMetadata(path, /* with_tags */ false).has_value();
}

void WebObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const
{
    MetadataStorageFromIndexPages metadata_storage(*this);

    std::string normalized_path = path;
    if (normalized_path == "/")
        normalized_path.clear();
    if (normalized_path.starts_with('/'))
        normalized_path.erase(0, 1);
    if (!normalized_path.empty() && !normalized_path.ends_with('/'))
        normalized_path += '/';

    std::deque<std::string> pending_directories;
    pending_directories.push_back(normalized_path);

    std::unordered_set<std::string> visited_directories;

    while (!pending_directories.empty())
    {
        if (max_keys && children.size() >= max_keys)
            break;

        auto current = std::move(pending_directories.front());
        pending_directories.pop_front();

        if (!visited_directories.emplace(current).second)
            continue;

        auto entries = metadata_storage.listDirectory(current);
        for (const auto & entry : entries)
        {
            if (max_keys && children.size() >= max_keys)
                break;

            if (entry.ends_with('/'))
            {
                pending_directories.push_back(entry);
                continue;
            }

            auto metadata = tryGetObjectMetadata(entry, /* with_tags */ false);
            children.emplace_back(std::make_shared<RelativePathWithMetadata>(entry, std::move(metadata)));
        }
    }
}

std::unique_ptr<ReadBufferFromFileBase> WebObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t>) const
{
    return std::make_unique<ReadBufferFromWebServer>(
        buildURL(object.remote_path),
        getContext(),
        object.bytes_size,
        read_settings,
        read_settings.remote_read_buffer_use_external_buffer,
        /* read_until_position */ 0,
        headers);
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

ObjectStorageKeyGeneratorPtr WebObjectStorage::createKeyGenerator() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "createKeyGenerator is not supported for {}", getName());
}

ObjectMetadata WebObjectStorage::getObjectMetadata(const std::string & path, bool with_tags) const
{
    auto metadata = tryGetObjectMetadata(path, with_tags);
    if (!metadata)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "No such file: {}", path);
    return *metadata;
}

std::optional<ObjectMetadata> WebObjectStorage::tryGetObjectMetadata(const std::string & path, bool) const
{
    Poco::Net::HTTPBasicCredentials credentials{};
    auto timeouts = ConnectionTimeouts::getHTTPTimeouts(
        getContext()->getSettingsRef(),
        getContext()->getServerSettings());

    auto response_buf = BuilderRWBufferFromHTTP(Poco::URI(buildURL(path), false))
                            .withConnectionGroup(HTTPConnectionGroupType::DISK)
                            .withSettings(getContext()->getReadSettings())
                            .withTimeouts(timeouts)
                            .withHostFilter(&getContext()->getRemoteHostFilter())
                            .withSkipNotFound(true)
                            .withDelayInit(false)
                            .withHeaders(headers)
                            .create(credentials);

    if (response_buf->hasNotFoundURL())
        return std::nullopt;

    ObjectMetadata metadata;
    auto file_info = response_buf->getFileInfo();
    if (file_info.file_size)
        metadata.size_bytes = *file_info.file_size;
    if (file_info.last_modified)
        metadata.last_modified = Poco::Timestamp::fromEpochTime(*file_info.last_modified);
    return metadata;
}

std::string WebObjectStorage::buildURL(const std::string & path) const
{
    if (path.empty())
        return url + query_fragment;

    std::string result = url;
    if (!result.ends_with('/'))
        result += '/';
    if (path.starts_with('/'))
        result += path.substr(1);
    else
        result += path;
    return result + query_fragment;
}

}
