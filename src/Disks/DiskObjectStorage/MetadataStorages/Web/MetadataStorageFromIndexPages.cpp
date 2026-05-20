#include <Disks/DiskObjectStorage/MetadataStorages/Web/MetadataStorageFromIndexPages.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Web/OriginComparisonUtils.h>

#include <Disks/DiskObjectStorage/MetadataStorages/StaticDirectoryIterator.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>
#include <Core/ServerSettings.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Common/re2.h>

#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_ALL_DATA;
}

namespace ServerSetting
{
    extern const ServerSettingsUInt64 max_http_index_page_size;
}

namespace
{
    bool shouldSkipHrefCandidate(const std::string & url_candidate)
    {
        if (url_candidate.empty() || url_candidate.starts_with('#'))
            return true;

        Poco::URI candidate_uri;
        try
        {
            candidate_uri = Poco::URI(url_candidate, false);
        }
        catch (const Poco::Exception &)
        {
            return true;
        }

        if (!candidate_uri.isRelative())
        {
            const auto & scheme = candidate_uri.getScheme();
            return scheme != "http" && scheme != "https";
        }

        return false;
    }

    std::string ensureTrailingSlash(std::string path)
    {
        if (!path.empty() && !path.ends_with("/"))
            path += '/';
        return path;
    }

    std::string ensureTrailingSlashInPath(std::string url)
    {
        Poco::URI uri(url, false);
        uri.setPath(ensureTrailingSlash(uri.getPath()));
        return uri.toString();
    }

    std::string getPathPrefixForMatching(const std::string & url)
    {
        const Poco::URI uri(url, false);
        return ensureTrailingSlash(uri.getPath());
    }

    std::string stripLeadingSlash(std::string path)
    {
        while (path.starts_with("/"))
            path.erase(0, 1);
        return path;
    }

    const re2::RE2 & getURLRegex()
    {
        static const re2::RE2 regex(
            R"((https?://[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]+|(?:\.\./|\.?/)?[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]+))");
        return regex;
    }

    std::string getEffectiveRelativePathForDeduplication(const std::string & relative, const std::string & source_url)
    {
        Poco::URI relative_uri(relative, false);
        const Poco::URI source_uri(source_url, false);

        if (relative_uri.getRawQuery().empty())
            relative_uri.setQuery(source_uri.getRawQuery());
        if (relative_uri.getFragment().empty())
            relative_uri.setFragment(source_uri.getFragment());

        return relative_uri.toString();
    }
}

MetadataStorageFromIndexPages::MetadataStorageFromIndexPages(const WebObjectStorage & object_storage_)
    : object_storage(object_storage_)
    , log(getLogger("MetadataStorageFromIndexPages"))
{
}

MetadataTransactionPtr MetadataStorageFromIndexPages::createTransaction()
{
    throwNotImplemented();
}

const std::string & MetadataStorageFromIndexPages::getPath() const
{
    static const String no_root;
    return no_root;
}

bool MetadataStorageFromIndexPages::existsFile(const std::string & path) const
{
    return object_storage.tryGetObjectMetadata(path, /* with_tags */ false).has_value();
}

bool MetadataStorageFromIndexPages::existsDirectory(const std::string & path) const
{
    RelativePathsWithMetadata files;
    return tryListDirectory(path, files, std::nullopt);
}

bool MetadataStorageFromIndexPages::existsFileOrDirectory(const std::string & path) const
{
    return existsFile(path) || existsDirectory(path);
}

uint64_t MetadataStorageFromIndexPages::getFileSize(const String & path) const
{
    auto metadata = object_storage.getObjectMetadata(path, /* with_tags */ false);
    return metadata.size_bytes;
}

std::optional<uint64_t> MetadataStorageFromIndexPages::getFileSizeIfExists(const String & path) const
{
    if (auto metadata = object_storage.tryGetObjectMetadata(path, /* with_tags */ false))
        return metadata->size_bytes;
    return std::nullopt;
}

std::vector<std::string> MetadataStorageFromIndexPages::listDirectory(const std::string & path) const
{
    auto entries = listDirectoryWithMetadata(path);

    std::vector<std::string> result;
    result.reserve(entries.size());
    for (const auto & entry : entries)
        result.push_back(entry->relative_path);
    return result;
}

RelativePathsWithMetadata MetadataStorageFromIndexPages::listDirectoryWithMetadata(const std::string & path, std::optional<size_t> shard_index) const
{
    RelativePathsWithMetadata result;
    if (!tryListDirectory(path, result, shard_index))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "There is no path {}", path);
    return result;
}

DirectoryIteratorPtr MetadataStorageFromIndexPages::iterateDirectory(const std::string & path) const
{
    RelativePathsWithMetadata files;
    if (!tryListDirectory(path, files, std::nullopt))
        return std::make_unique<StaticDirectoryIterator>(std::vector<std::filesystem::path>{});

    std::vector<std::filesystem::path> entries;
    entries.reserve(files.size());
    for (const auto & file : files)
        entries.emplace_back(file->relative_path);
    return std::make_unique<StaticDirectoryIterator>(std::move(entries));
}

StoredObjects MetadataStorageFromIndexPages::getStorageObjects(const std::string & path) const
{
    auto metadata = object_storage.getObjectMetadata(path, /* with_tags */ false);
    return {StoredObject(path, path, metadata.size_bytes)};
}

std::optional<StoredObjects> MetadataStorageFromIndexPages::getStorageObjectsIfExist(const std::string & path) const
{
    auto metadata = object_storage.tryGetObjectMetadata(path, /* with_tags */ false);
    if (!metadata)
        return std::nullopt;
    return StoredObjects{StoredObject(path, path, metadata->size_bytes)};
}

std::vector<std::string> MetadataStorageFromIndexPages::makeListingURLs(const std::string & path, size_t shard_index) const
{
    return object_storage.buildURLs(ensureTrailingSlashInPath(stripLeadingSlash(path)), shard_index);
}

std::string MetadataStorageFromIndexPages::readIndexPage(const std::string & url) const
{
    Poco::Net::HTTPBasicCredentials credentials{};
    auto timeouts = ConnectionTimeouts::getHTTPTimeouts(
        object_storage.getContext()->getSettingsRef(),
        object_storage.getContext()->getServerSettings());

    auto buf = BuilderRWBufferFromHTTP(Poco::URI(url, false))
                   .withConnectionGroup(HTTPConnectionGroupType::DISK)
                   .withSettings(object_storage.getContext()->getReadSettings())
                   .withTimeouts(timeouts)
                   .withHostFilter(&object_storage.getContext()->getRemoteHostFilter())
                   .withHeaders(object_storage.getHeaders())
                   .create(credentials);

    std::string body;
    WriteBufferFromString out(body);
    const auto limit = object_storage.getContext()->getServerSettings()[ServerSetting::max_http_index_page_size];
    copyDataMaxBytes(*buf, out, limit);
    out.finalize();
    return body;
}

std::vector<std::string> MetadataStorageFromIndexPages::extractURLs(
    const std::string & page_body,
    const std::string & listing_url,
    const std::string & base_url,
    const std::string & source_url,
    const std::string & path) const
{
    std::vector<std::string> result;
    std::unordered_set<std::string> seen_effective_relative;
    const auto & regex = getURLRegex();
    const Poco::URI listing_uri(listing_url, false);
    const Poco::URI base_uri(base_url, false);
    re2::StringPiece input(page_body);
    re2::StringPiece match;
    bool found_valid_href_url = false;

    static const re2::RE2 href_regex(R"((?i)(?:href|src)\s*=\s*['"]([^'"]+)['"])");
    re2::StringPiece href_input(page_body);
    re2::StringPiece href_match;
    while (re2::RE2::FindAndConsume(&href_input, href_regex, &href_match))
    {
        std::string url_candidate(href_match.data(), href_match.size());

        if (shouldSkipHrefCandidate(url_candidate))
            continue;

        Poco::URI candidate_uri;
        try
        {
            candidate_uri = Poco::URI(url_candidate, false);
        }
        catch (const Poco::Exception &)
        {
            continue;
        }

        if (candidate_uri.isRelative())
        {
            Poco::URI resolved(listing_url);
            resolved.resolve(candidate_uri);
            candidate_uri = resolved;
        }
        candidate_uri.normalize();

        if (candidate_uri.getPath().empty())
            continue;

        if (!WebIndexPage::isSameOrigin(candidate_uri, base_uri))
            continue;

        if (!WebIndexPage::hasPathPrefix(candidate_uri, listing_uri))
            continue;

        auto relative = WebIndexPage::getRelativePathWithQueryAndFragment(candidate_uri, base_uri);
        if (relative.empty())
            continue;

        if (!relative.starts_with(path))
            continue;

        if (!seen_effective_relative.emplace(getEffectiveRelativePathForDeduplication(relative, source_url)).second)
            continue;

        found_valid_href_url = true;
        result.push_back(relative);

    }

    if (!found_valid_href_url)
    {
        while (re2::RE2::FindAndConsume(&input, regex, &match))
        {
            std::string url_candidate(match.data(), match.size());

            Poco::URI candidate_uri;
            try
            {
                candidate_uri = Poco::URI(url_candidate, false);
            }
            catch (const Poco::Exception &)
            {
                continue;
            }

            if (candidate_uri.isRelative())
            {
                Poco::URI resolved(listing_url);
                resolved.resolve(candidate_uri);
                candidate_uri = resolved;
            }
            candidate_uri.normalize();

            if (candidate_uri.getPath().empty())
                continue;

            if (!WebIndexPage::isSameOrigin(candidate_uri, base_uri))
                continue;

            if (!WebIndexPage::hasPathPrefix(candidate_uri, listing_uri))
                continue;

            auto relative = WebIndexPage::getRelativePathWithQueryAndFragment(candidate_uri, base_uri);
            if (relative.empty())
                continue;

            if (!relative.starts_with(path))
                continue;

            if (!seen_effective_relative.emplace(getEffectiveRelativePathForDeduplication(relative, source_url)).second)
                continue;

            result.push_back(relative);

        }
    }

    return result;
}

bool MetadataStorageFromIndexPages::tryListDirectory(
    const std::string & path,
    RelativePathsWithMetadata & result,
    std::optional<size_t> requested_shard_index) const
{
    const auto normalized_path = ensureTrailingSlashInPath(stripLeadingSlash(path));
    const auto path_prefix = getPathPrefixForMatching(normalized_path);
    const auto & url_shards = object_storage.getURLShards();
    if (requested_shard_index && *requested_shard_index >= url_shards.size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid URL shard index: {}", *requested_shard_index);

    bool has_listed_directory = false;

    const auto first_shard_index = requested_shard_index.value_or(0);
    const auto end_shard_index = requested_shard_index ? *requested_shard_index + 1 : url_shards.size();

    for (size_t shard_index = first_shard_index; shard_index != end_shard_index; ++shard_index)
    {
        const auto listing_urls = makeListingURLs(normalized_path, shard_index);
        const auto & url_options = url_shards[shard_index];
        std::exception_ptr shard_exception;
        bool has_not_found = false;
        bool has_listed_shard = false;

        for (size_t i = 0; i != listing_urls.size(); ++i)
        {
            const auto & listing_url = listing_urls[i];
            try
            {
                auto body = readIndexPage(listing_url);
                auto entries = extractURLs(
                    body,
                    listing_url,
                    url_options[i].base_url,
                    url_options[i].base_url + url_options[i].query_fragment,
                    path_prefix);

                result.reserve(result.size() + entries.size());
                for (auto & entry : entries)
                    result.emplace_back(std::make_shared<RelativePathWithMetadata>(std::move(entry), shard_index));

                has_listed_directory = true;
                has_listed_shard = true;
                break;
            }
            catch (const HTTPException & e)
            {
                if (e.getHTTPStatus() == Poco::Net::HTTPResponse::HTTP_NOT_FOUND)
                {
                    has_not_found = true;
                    continue;
                }
                shard_exception = std::current_exception();
            }
            catch (const Exception & e)
            {
                if (e.code() == ErrorCodes::CANNOT_READ_ALL_DATA)
                    shard_exception = std::make_exception_ptr(
                        Exception(ErrorCodes::BAD_ARGUMENTS, "Index page '{}' exceeds max_http_index_page_size", listing_url));
                else
                    shard_exception = std::current_exception();
            }
            catch (...)
            {
                shard_exception = std::current_exception();
            }
        }

        if (!has_listed_shard && shard_exception)
            std::rethrow_exception(shard_exception);

        if (!has_listed_shard && has_not_found)
            return false;
    }

    if (has_listed_directory)
        return true;

    return false;
}

}
