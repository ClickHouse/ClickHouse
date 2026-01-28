#include <Disks/DiskObjectStorage/MetadataStorages/Web/MetadataStorageFromIndexPages.h>

#include <Disks/DiskObjectStorage/ObjectStorages/StaticDirectoryIterator.h>
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
    std::string ensureTrailingSlash(std::string path)
    {
        if (!path.empty() && !path.ends_with("/"))
            path += '/';
        return path;
    }

    std::string stripLeadingSlash(std::string path)
    {
        if (path.starts_with("/"))
            path.erase(0, 1);
        return path;
    }

    bool isSameOrigin(const Poco::URI & lhs, const Poco::URI & rhs)
    {
        return lhs.getScheme() == rhs.getScheme()
            && lhs.getHost() == rhs.getHost()
            && lhs.getPort() == rhs.getPort();
    }

    const re2::RE2 & getURLRegex()
    {
        static const re2::RE2 regex(
            R"((https?://[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]+|(?:\.\./|\.?/)?[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]+))");
        return regex;
    }
}

MetadataStorageFromIndexPages::MetadataStorageFromIndexPages(const WebObjectStorage & object_storage_)
    : object_storage(object_storage_)
    , log(getLogger("MetadataStorageFromIndexPages"))
    , base_uri(object_storage.getBaseURL())
{
    auto base_path = base_uri.getPath();
    base_uri.setPath(ensureTrailingSlash(base_path));
    base_uri.setQuery({});
    base_uri.setFragment({});
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
    std::vector<std::string> files;
    return tryListDirectory(path, files);
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
    std::vector<std::string> result;
    if (!tryListDirectory(path, result))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "There is no path {}", path);
    return result;
}

DirectoryIteratorPtr MetadataStorageFromIndexPages::iterateDirectory(const std::string & path) const
{
    std::vector<std::string> files;
    if (!tryListDirectory(path, files))
        return std::make_unique<StaticDirectoryIterator>(std::vector<std::filesystem::path>{});

    std::vector<std::filesystem::path> entries;
    entries.reserve(files.size());
    for (const auto & file : files)
        entries.emplace_back(file);
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

std::string MetadataStorageFromIndexPages::makeListingURL(const std::string & path) const
{
    auto normalized_path = ensureTrailingSlash(path);
    auto listing_uri = base_uri;
    listing_uri.setPath(ensureTrailingSlash(base_uri.getPath()) + stripLeadingSlash(normalized_path));
    listing_uri.setQuery({});
    listing_uri.setFragment({});
    return listing_uri.toString();
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
    const std::string & page_body, const std::string & listing_url, const std::string & path) const
{
    std::vector<std::string> result;
    std::unordered_set<std::string> seen;
    const auto & regex = getURLRegex();
    re2::StringPiece input(page_body);
    re2::StringPiece match;
    bool used_href_extraction = false;

    const re2::RE2 href_regex(R"((?i)(?:href|src)\s*=\s*['"]([^'"]+)['"])");
    re2::StringPiece href_input(page_body);
    re2::StringPiece href_match;
    while (re2::RE2::FindAndConsume(&href_input, href_regex, &href_match))
    {
        used_href_extraction = true;
        std::string url_candidate(href_match.data(), href_match.size());
        if (!seen.emplace(url_candidate).second)
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

        if (candidate_uri.getPath().empty())
            continue;

        if (!isSameOrigin(candidate_uri, base_uri))
            continue;

        auto candidate_str = candidate_uri.toString();
        if (!startsWith(candidate_str, listing_url))
            continue;

        const auto base_prefix = base_uri.toString();
        if (!candidate_str.starts_with(base_prefix))
            continue;

        auto relative = candidate_str.substr(base_prefix.size());
        if (relative.empty())
            continue;

        if (!relative.starts_with(path))
            continue;

        result.push_back(relative);

    }

    if (!used_href_extraction)
    {
        while (re2::RE2::FindAndConsume(&input, regex, &match))
        {
            std::string url_candidate(match.data(), match.size());
            if (!seen.emplace(url_candidate).second)
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

            if (candidate_uri.getPath().empty())
                continue;

            if (!isSameOrigin(candidate_uri, base_uri))
                continue;

            auto candidate_str = candidate_uri.toString();
            if (!startsWith(candidate_str, listing_url))
                continue;

            const auto base_prefix = base_uri.toString();
            if (!candidate_str.starts_with(base_prefix))
                continue;

            auto relative = candidate_str.substr(base_prefix.size());
            if (relative.empty())
                continue;

            if (!relative.starts_with(path))
                continue;

            result.push_back(relative);

        }
    }

    return result;
}

bool MetadataStorageFromIndexPages::tryListDirectory(const std::string & path, std::vector<std::string> & result) const
{
    const auto listing_url = makeListingURL(path);
    try
    {
        auto body = readIndexPage(listing_url);
        result = extractURLs(body, listing_url, ensureTrailingSlash(path));
        return true;
    }
    catch (const HTTPException & e)
    {
        if (e.getHTTPStatus() == Poco::Net::HTTPResponse::HTTP_NOT_FOUND)
            return false;
        throw;
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::CANNOT_READ_ALL_DATA)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Index page '{}' exceeds max_http_index_page_size", listing_url);
        throw;
    }
}

}
