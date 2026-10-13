#include <Disks/DiskObjectStorage/ObjectStorages/Web/WebObjectStorage.h>

#include <Disks/DiskObjectStorage/MetadataStorages/Web/MetadataStorageFromIndexPages.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Web/OriginComparisonUtils.h>
#include <Common/logger_useful.h>
#include <Common/CurrentThread.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

#include <Disks/IO/ReadBufferFromWebServer.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Poco/Timestamp.h>

#include <deque>
#include <limits>
#include <unordered_set>

namespace DB
{

namespace
{
    unsigned getEffectivePort(const Poco::URI & uri)
    {
        if (const auto port = uri.getPort())
            return port;

        const auto & scheme = uri.getScheme();
        if (scheme == "http")
            return 80;
        if (scheme == "https")
            return 443;

        return 0;
    }

    String getOriginCacheKey(const Poco::URI & uri)
    {
        return fmt::format("{}://{}:{}", uri.getScheme(), uri.getHost(), getEffectivePort(uri));
    }

    std::string stripLeadingSlashes(std::string path)
    {
        while (path.starts_with('/'))
            path.erase(0, 1);
        return path;
    }

    bool pathPartEndsWithSlash(const std::string & path)
    {
        const auto path_end = path.find_first_of("?#");
        if (path_end == std::string::npos)
            return path.ends_with('/');
        if (path_end == 0)
            return false;
        return path[path_end - 1] == '/';
    }
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int FILE_DOESNT_EXIST;
}

namespace Setting
{
    extern const SettingsUInt64 url_wildcard_max_directories_to_read;
    extern const SettingsBool enable_url_encoding;
    extern const SettingsUInt64 max_http_get_redirects;
}

WebObjectStorage::WebObjectStorage(
    const String & url_,
    const String & query_fragment_,
    ContextPtr context_,
    HTTPHeaderEntries headers_,
    size_t max_directories_to_read_)
    : WebObjectStorage(
        URLShards{{URL{.base_url = url_, .query_fragment = query_fragment_}}},
        context_,
        std::move(headers_),
        max_directories_to_read_)
{
}

WebObjectStorage::WebObjectStorage(
    URLShards url_shards_,
    ContextPtr context_,
    HTTPHeaderEntries headers_,
    size_t max_directories_to_read_)
    /// The object storage may outlive the context it was created with (e.g. a dynamic `web` disk
    /// created inside a query stays in the disks map after the query context expires),
    /// so pin the global context here. Per-request settings are taken from the current
    /// query context via `getRequestContext`.
    : WithContext(context_->getGlobalContext())
    , url_shards(std::move(url_shards_))
    , headers(std::move(headers_))
    , max_directories_to_read(max_directories_to_read_)
    , log(getLogger("WebObjectStorage"))
{
    if (url_shards.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "At least one URL shard is required");
    for (const auto & url_shard : url_shards)
    {
        if (url_shard.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "At least one URL option is required for each URL shard");
    }
}

ContextPtr WebObjectStorage::getRequestContext() const
{
    if (CurrentThread::isInitialized())
    {
        if (auto query_context = CurrentThread::tryGetQueryContext())
            return query_context;
    }

    return getContext();
}

bool WebObjectStorage::exists(const StoredObject & object) const
{
    return exists(object.remote_path);
}

bool WebObjectStorage::exists(const std::string & path) const
{
    LOG_TRACE(log, "Checking existence of path: {}", path);
    return tryGetObjectMetadata(path, /* with_tags */ false).has_value();
}

std::string WebObjectStorage::getEffectiveRelativePathForKey(const RelativePathWithMetadata & relative_path) const
{
    /// Two listed entries that differ only by an explicit vs. inherited query/fragment resolve to the
    /// same effective URL in `buildURL`, so deduplication must use the effective form (see thread on
    /// duplicate reads). Keying by the raw listing token would let the same object be read twice.
    if (relative_path.path_for_deduplication)
        return *relative_path.path_for_deduplication;

    const auto read_source_index = relative_path.read_source_index;
    if (!read_source_index || *read_source_index >= url_shards.size() || url_shards[*read_source_index].empty())
        return relative_path.relative_path;
    const auto & url_option = url_shards[*read_source_index].front();
    return WebIndexPage::getEffectiveRelativePathForDeduplication(
        relative_path.relative_path, url_option.base_url + url_option.query_fragment);
}

void WebObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const
{
    MetadataStorageFromIndexPages metadata_storage(*this);
    const size_t max_directories_to_read_for_query = getMaxDirectoriesToRead();

    std::string normalized_path = stripLeadingSlashes(path);
    if (normalized_path == "/")
        normalized_path.clear();
    if (!normalized_path.empty() && !normalized_path.ends_with('/'))
        normalized_path += '/';

    std::deque<RelativePathWithMetadata> pending_directories;
    pending_directories.emplace_back(normalized_path, std::optional<size_t>{});
    std::unordered_set<std::string> known_directories;
    known_directories.emplace(fmt::format("{}:{}", std::numeric_limits<size_t>::max(), normalized_path));
    std::unordered_set<std::string> known_files;

    while (!pending_directories.empty())
    {
        auto current = pending_directories.front();
        pending_directories.pop_front();

        auto entries = metadata_storage.listDirectoryWithMetadata(
            current.relative_path, current.read_source_index, current.path_for_glob_matching);
        for (const auto & entry : entries)
        {
            const auto effective_relative_path = getEffectiveRelativePathForKey(*entry);
            const auto key = fmt::format("{}:{}", entry->read_source_index.value_or(std::numeric_limits<size_t>::max()), effective_relative_path);

            if (pathPartEndsWithSlash(entry->relative_path))
            {
                if (!known_directories.emplace(key).second)
                    continue;

                if (max_directories_to_read_for_query && known_directories.size() > max_directories_to_read_for_query)
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Too many directories while expanding URL wildcard, maximum: {}. This limit is controlled by "
                        "setting `url_wildcard_max_directories_to_read`",
                        max_directories_to_read_for_query);
                }
                pending_directories.emplace_back(*entry);
                continue;
            }

            if (!known_files.emplace(key).second)
                continue;

            /// `max_keys` (mapped from `glob_expansion_max_elements`) is a hard limit on the number of
            /// matched files, not a pagination window: web listing has no continuation, so silently
            /// returning a truncated set would produce incomplete query results. Fail loudly instead.
            if (max_keys && children.size() >= max_keys)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The number of files matching the URL wildcard exceeds the limit of {}. This limit is "
                    "controlled by setting `glob_expansion_max_elements`",
                    max_keys);

            children.emplace_back(entry);
        }
    }
}

size_t WebObjectStorage::getMaxDirectoriesToRead() const
{
    if (CurrentThread::isInitialized())
    {
        if (const auto query_context = CurrentThread::tryGetQueryContext())
            return query_context->getSettingsRef()[Setting::url_wildcard_max_directories_to_read];
    }

    return max_directories_to_read;
}

std::unique_ptr<ReadBufferFromFileBase> WebObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    bool use_external_buffer,
    bool /* restrict_seek */) const
{
    auto urls = object.read_source_index ? buildURLs(object.remote_path, *object.read_source_index) : buildURLs(object.remote_path);
    /// The async reader (`AsynchronousBoundedReadBuffer` -> `ThreadPoolRemoteFSReader`) reads into an
    /// external buffer regardless of the number of failover URLs, and asserts that the wrapped reader
    /// honors it. The caller (`ReadPipeline`) passes `use_external_buffer` accordingly even when
    /// `urls.size() > 1`; `ReadBufferFromWebServer` selects a working failover URL with a separate
    /// connectivity probe in that case.
    return std::make_unique<ReadBufferFromWebServer>(
        std::move(urls),
        getRequestContext(),
        object.bytes_size,
        read_settings,
        use_external_buffer,
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

std::optional<ObjectMetadata> WebObjectStorage::tryGetObjectMetadata(const std::string & path, bool with_tags) const
{
    return tryGetObjectMetadata(RelativePathWithMetadata(path), with_tags);
}

ObjectMetadata WebObjectStorage::getObjectMetadata(const RelativePathWithMetadata & path, bool with_tags) const
{
    auto metadata = tryGetObjectMetadata(path, with_tags);
    if (!metadata)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "No such file: {}", path.getPath());
    return *metadata;
}

std::optional<ObjectMetadata> WebObjectStorage::tryGetObjectMetadata(const RelativePathWithMetadata & path, bool) const
{
    const auto request_context = getRequestContext();
    const auto & settings = request_context->getSettingsRef();
    const bool enable_url_encoding = settings[Setting::enable_url_encoding];
    const auto max_redirects = settings[Setting::max_http_get_redirects];
    auto timeouts = ConnectionTimeouts::getHTTPTimeouts(
        settings,
        request_context->getServerSettings());

    auto get_metadata_from_uri = [&](const Poco::URI & uri) -> std::optional<ObjectMetadata>
    {
        /// Carry the same HTTP request semantics as direct `url()` reads: credentials from the URL
        /// userinfo, `max_http_get_redirects`, and `enable_url_encoding`.
        Poco::Net::HTTPBasicCredentials credentials;
        setCredentialsFromURL(credentials, uri);

        auto create_probe_buffer = [&](const String & method)
        {
            auto read_settings = request_context->getReadSettings();
            /// On the `GET` fallback path we already know `HEAD` is unavailable for this origin. A chunked
            /// `GET` response carries no `Content-Length`, so the buffer would otherwise lazily call
            /// `getFileInfo` and re-issue a `HEAD` (which can fail, e.g. `501 Not Implemented`) before the
            /// object is ever read. Disable it so the metadata probe relies solely on the `GET` response.
            if (method == Poco::Net::HTTPRequest::HTTP_GET)
                read_settings.http_settings.make_head_request = false;
            return BuilderRWBufferFromHTTP(uri)
                .withConnectionGroup(HTTPConnectionGroupType::DISK)
                .withMethod(method)
                .withSettings(read_settings)
                .withTimeouts(timeouts)
                .withRedirects(max_redirects)
                .withEnableUrlEncoding(enable_url_encoding)
                .withHostFilter(&request_context->getRemoteHostFilter())
                .withSkipNotFound(true)
                .withDelayInit(false)
                .withHeaders(headers)
                .create(credentials);
        };

        std::unique_ptr<ReadWriteBufferFromHTTP> response_buf;
        const auto head_support = getHeadSupportForOrigin(uri);
        if (head_support == HeadSupport::Unsupported)
        {
            response_buf = create_probe_buffer(Poco::Net::HTTPRequest::HTTP_GET);
        }
        else
        {
            try
            {
                response_buf = create_probe_buffer(Poco::Net::HTTPRequest::HTTP_HEAD);
                setHeadSupportForOrigin(uri, HeadSupport::Supported);
            }
            catch (const HTTPException & e)
            {
                /// Many servers/CDNs serve `GET` but reject `HEAD` with a non-`405` status (e.g. webhdfs
                /// answers `400`, others use `403`). Mirror `ReadWriteBufferFromHTTP::getFileInfo`: treat any
                /// non-transient `4xx` (plus `501 Not Implemented`) as "HEAD unsupported" and fall back to a
                /// `GET` probe, while still propagating transient errors so a flaky `HEAD` does not
                /// permanently disable it for the origin.
                const auto status = e.getHTTPStatus();
                const bool head_unsupported =
                    (status >= 400 && status <= 499
                        && status != Poco::Net::HTTPResponse::HTTP_TOO_MANY_REQUESTS
                        && status != Poco::Net::HTTPResponse::HTTP_REQUEST_TIMEOUT
                        && status != Poco::Net::HTTPResponse::HTTP_MISDIRECTED_REQUEST)
                    || status == Poco::Net::HTTPResponse::HTTP_NOT_IMPLEMENTED;

                if (head_unsupported)
                {
                    setHeadSupportForOrigin(uri, HeadSupport::Unsupported);
                    response_buf = create_probe_buffer(Poco::Net::HTTPRequest::HTTP_GET);
                }
                else
                {
                    throw;
                }
            }
        }

        if (response_buf->hasNotFoundURL())
            return std::nullopt;

        ObjectMetadata metadata;
        auto file_info = response_buf->getFileInfo();
        /// When the response has no `Content-Length` (e.g. chunked transfer encoding), the size is unknown.
        /// Leaving `is_size_known = true` with `size_bytes = 0` would make a non-empty body look like a
        /// known-empty file, so that `engine_url_skip_empty_files` could skip it and read-buffer sizing
        /// decisions would use the wrong size.
        metadata.is_size_known = file_info.file_size.has_value();
        if (file_info.file_size)
            metadata.size_bytes = *file_info.file_size;
        /// A missing `Last-Modified` header means the modification time is unknown. Keep that distinct from a
        /// real epoch-`0` timestamp so the schema/count caches (which compare `last_modified` against their
        /// registration time) skip the cache instead of reusing a value that may be stale after the remote
        /// file changes.
        metadata.is_last_modified_known = file_info.last_modified.has_value();
        if (file_info.last_modified)
            metadata.last_modified = Poco::Timestamp::fromEpochTime(*file_info.last_modified);
        for (const auto & header : response_buf->getResponseHeaders())
        {
            const auto & tuple = header.safeGet<Tuple>();
            const auto & header_name = tuple[0].safeGet<String>();
            const auto & header_value = tuple[1].safeGet<String>();
            metadata.attributes.emplace(header_name, header_value);
            if (header_name == "ETag")
                metadata.etag = header_value;
        }
        return metadata;
    };

    std::exception_ptr last_exception;
    bool has_not_found = false;
    auto urls = path.read_source_index ? buildURLs(path.getPath(), *path.read_source_index) : buildURLs(path.getPath());
    for (const auto & url : urls)
    {
        try
        {
            auto metadata = get_metadata_from_uri(Poco::URI(url, enable_url_encoding));
            if (metadata)
                return metadata;
            has_not_found = true;
        }
        catch (...)
        {
            last_exception = std::current_exception();
        }
    }

    if (last_exception)
        std::rethrow_exception(last_exception);

    if (has_not_found)
        return std::nullopt;

    return std::nullopt;
}

WebObjectStorage::HeadSupport WebObjectStorage::getHeadSupportForOrigin(const Poco::URI & uri) const
{
    const auto origin = getOriginCacheKey(uri);
    std::lock_guard lock(head_support_mutex);
    if (const auto it = head_support_by_origin.find(origin); it != head_support_by_origin.end())
    {
        head_support_lru.splice(head_support_lru.begin(), head_support_lru, it->second);
        return it->second->second;
    }
    return HeadSupport::Unknown;
}

void WebObjectStorage::setHeadSupportForOrigin(const Poco::URI & uri, HeadSupport support) const
{
    const auto origin = getOriginCacheKey(uri);
    std::lock_guard lock(head_support_mutex);

    if (const auto it = head_support_by_origin.find(origin); it != head_support_by_origin.end())
    {
        it->second->second = support;
        head_support_lru.splice(head_support_lru.begin(), head_support_lru, it->second);
        return;
    }

    head_support_lru.emplace_front(origin, support);
    head_support_by_origin.emplace(origin, head_support_lru.begin());

    if (head_support_lru.size() > max_head_support_cache_size)
    {
        auto last = std::prev(head_support_lru.end());
        head_support_by_origin.erase(last->first);
        head_support_lru.pop_back();
    }
}

std::string WebObjectStorage::buildURL(const std::string & path) const
{
    return buildURL(url_shards.front().front(), path);
}

std::vector<String> WebObjectStorage::buildURLs(const std::string & path) const
{
    std::vector<String> result;
    size_t urls_count = 0;
    for (const auto & url_shard : url_shards)
        urls_count += url_shard.size();

    result.reserve(urls_count);
    for (const auto & url_shard : url_shards)
    {
        for (const auto & url_option : url_shard)
            result.push_back(buildURL(url_option, path));
    }
    return result;
}

std::vector<String> WebObjectStorage::buildURLs(const std::string & path, size_t shard_index) const
{
    if (shard_index >= url_shards.size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid URL shard index: {}", shard_index);

    const auto & url_shard = url_shards[shard_index];
    std::vector<String> result;
    result.reserve(url_shard.size());
    for (const auto & url_option : url_shard)
        result.push_back(buildURL(url_option, path));
    return result;
}

std::string WebObjectStorage::buildURL(const URL & url_option, const std::string & path)
{
    if (path.empty())
        return url_option.base_url + url_option.query_fragment;

    Poco::URI base_uri(url_option.base_url, false);
    auto base_path = base_uri.getPath();
    if (!base_path.ends_with('/'))
        base_path += '/';

    Poco::URI path_uri(stripLeadingSlashes(path), false);
    base_uri.setPath(base_path + stripLeadingSlashes(path_uri.getPath()));

    Poco::URI source_uri(url_option.base_url + url_option.query_fragment, false);
    if (!path_uri.getRawQuery().empty())
        base_uri.setRawQuery(path_uri.getRawQuery());
    else
        base_uri.setRawQuery(source_uri.getRawQuery());

    if (!path_uri.getFragment().empty())
        base_uri.setFragment(path_uri.getFragment());
    else
        base_uri.setFragment(source_uri.getFragment());

    return base_uri.toString();
}

}
