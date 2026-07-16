#include <Disks/DiskObjectStorage/MetadataStorages/Web/MetadataStorageFromIndexPages.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Web/OriginComparisonUtils.h>

#include <Disks/DiskObjectStorage/MetadataStorages/StaticDirectoryIterator.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <Common/logger_useful.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Common/re2.h>

#include <optional>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int UNKNOWN_FILE_SIZE;
}

namespace ServerSetting
{
    extern const ServerSettingsUInt64 max_http_index_page_size;
}

namespace Setting
{
    extern const SettingsBool enable_url_encoding;
    extern const SettingsUInt64 max_http_get_redirects;
}

namespace
{
    StoredObject makeStoredObjectFromMetadata(const std::string & path, const ObjectMetadata & metadata)
    {
        if (metadata.is_size_known)
            return StoredObject(path, path, metadata.size_bytes);

        return StoredObject(path, path);
    }

    /// Decode the HTML entities that commonly appear in `href`/`src` attribute values so the
    /// extracted URL matches what a browser would actually request. For example, index pages
    /// frequently render signed download links as `href="part.tsv?x=1&amp;token=..."`; without
    /// decoding, the literal `&amp;` would be sent to the server and the final `GET` would fail.
    /// Handles the named entities relevant to URLs plus numeric (`&#...;`) and hex (`&#x...;`)
    /// references; unrecognized entities are left untouched.
    std::string decodeHTMLEntities(const std::string & input)
    {
        if (input.find('&') == std::string::npos)
            return input;

        std::string result;
        result.reserve(input.size());

        size_t i = 0;
        while (i < input.size())
        {
            if (input[i] != '&')
            {
                result += input[i];
                ++i;
                continue;
            }

            const size_t semicolon = input.find(';', i + 1);
            /// Entity references are short; if there is no nearby `;`, treat `&` as a literal.
            if (semicolon == std::string::npos || semicolon - i > 12)
            {
                result += input[i];
                ++i;
                continue;
            }

            const std::string entity = input.substr(i + 1, semicolon - i - 1);
            std::optional<UInt32> code_point;

            if (entity == "amp")
                code_point = '&';
            else if (entity == "lt")
                code_point = '<';
            else if (entity == "gt")
                code_point = '>';
            else if (entity == "quot")
                code_point = '"';
            else if (entity == "apos")
                code_point = '\'';
            else if (entity.size() > 2 && (entity[0] == '#') && (entity[1] == 'x' || entity[1] == 'X'))
            {
                UInt32 value = 0;
                bool valid = true;
                for (size_t j = 2; j < entity.size() && valid; ++j)
                {
                    const char c = entity[j];
                    UInt32 digit = 0;
                    if (c >= '0' && c <= '9')
                        digit = static_cast<UInt32>(c - '0');
                    else if (c >= 'a' && c <= 'f')
                        digit = static_cast<UInt32>(c - 'a' + 10);
                    else if (c >= 'A' && c <= 'F')
                        digit = static_cast<UInt32>(c - 'A' + 10);
                    else
                        valid = false;
                    if (valid)
                        value = value * 16 + digit;
                }
                if (valid)
                    code_point = value;
            }
            else if (entity.size() > 1 && entity[0] == '#')
            {
                UInt32 value = 0;
                bool valid = true;
                for (size_t j = 1; j < entity.size() && valid; ++j)
                {
                    const char c = entity[j];
                    if (c >= '0' && c <= '9')
                        value = value * 10 + static_cast<UInt32>(c - '0');
                    else
                        valid = false;
                }
                if (valid)
                    code_point = value;
            }

            if (!code_point)
            {
                result += input[i];
                ++i;
                continue;
            }

            char utf8_bytes[4];
            const size_t num_bytes = UTF8::convertCodePointToUTF8(static_cast<int>(*code_point), utf8_bytes, sizeof(utf8_bytes));
            if (num_bytes == 0)
            {
                /// Code point cannot be encoded; keep the original entity verbatim.
                result.append(input, i, semicolon - i + 1);
            }
            else
                result.append(utf8_bytes, num_bytes);

            i = semicolon + 1;
        }

        return result;
    }

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

    std::string getListingPrefixURLForMatching(const std::string & url)
    {
        Poco::URI uri(url, false);
        auto path = uri.getPath();
        if (path.empty())
            path = "/";
        else if (!path.ends_with('/'))
        {
            const auto slash_pos = path.find_last_of('/');
            path = slash_pos == std::string::npos ? "/" : path.substr(0, slash_pos + 1);
        }

        uri.setPath(path);
        uri.setQuery("");
        uri.setFragment("");
        return uri.toString();
    }

    std::string stripLeadingSlash(std::string path)
    {
        while (path.starts_with("/"))
            path.erase(0, 1);
        return path;
    }

    void rejectOriginChangingRedirect(const std::string & requested_url, const std::string & final_url)
    {
        const Poco::URI requested_uri(requested_url, false);
        const Poco::URI final_uri(final_url, false);
        if (!WebIndexPage::isSameOrigin(requested_uri, final_uri))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Index page '{}' was redirected to a different origin '{}'",
                requested_url,
                final_url);
    }

    using WebIndexPage::getEffectiveRelativePathForDeduplication;

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
    return tryListDirectory(path, files, std::nullopt, std::nullopt);
}

bool MetadataStorageFromIndexPages::existsFileOrDirectory(const std::string & path) const
{
    return existsFile(path) || existsDirectory(path);
}

uint64_t MetadataStorageFromIndexPages::getFileSize(const String & path) const
{
    auto metadata = object_storage.getObjectMetadata(path, /* with_tags */ false);
    if (!metadata.is_size_known)
        throw Exception(ErrorCodes::UNKNOWN_FILE_SIZE, "Cannot determine size of object {}", path);
    return metadata.size_bytes;
}

std::optional<uint64_t> MetadataStorageFromIndexPages::getFileSizeIfExists(const String & path) const
{
    if (auto metadata = object_storage.tryGetObjectMetadata(path, /* with_tags */ false))
    {
        if (metadata->is_size_known)
            return metadata->size_bytes;
    }
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

RelativePathsWithMetadata MetadataStorageFromIndexPages::listDirectoryWithMetadata(
    const std::string & path,
    std::optional<size_t> shard_index,
    const std::optional<String> & path_for_glob_matching) const
{
    RelativePathsWithMetadata result;
    if (!tryListDirectory(path, result, shard_index, path_for_glob_matching))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "There is no path {}", path);
    return result;
}

DirectoryIteratorPtr MetadataStorageFromIndexPages::iterateDirectory(const std::string & path) const
{
    RelativePathsWithMetadata files;
    if (!tryListDirectory(path, files, std::nullopt, std::nullopt))
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
    return {makeStoredObjectFromMetadata(path, metadata)};
}

std::optional<StoredObjects> MetadataStorageFromIndexPages::getStorageObjectsIfExist(const std::string & path) const
{
    auto metadata = object_storage.tryGetObjectMetadata(path, /* with_tags */ false);
    if (!metadata)
        return std::nullopt;
    return StoredObjects{makeStoredObjectFromMetadata(path, *metadata)};
}

std::vector<std::string> MetadataStorageFromIndexPages::makeListingURLs(const std::string & path, size_t shard_index) const
{
    return object_storage.buildURLs(ensureTrailingSlashInPath(stripLeadingSlash(path)), shard_index);
}

MetadataStorageFromIndexPages::IndexPage MetadataStorageFromIndexPages::readIndexPage(const std::string & url) const
{
    const auto request_context = object_storage.getRequestContext();
    const auto & settings = request_context->getSettingsRef();
    const bool enable_url_encoding = settings[Setting::enable_url_encoding];

    /// Carry the same HTTP request semantics as direct `url()` reads: honor `enable_url_encoding`,
    /// follow up to `max_http_get_redirects` redirects, and authenticate with credentials parsed from
    /// the URL's userinfo (e.g. `http://user:pass@host`).
    Poco::URI uri(url, enable_url_encoding);
    Poco::Net::HTTPBasicCredentials credentials;
    setCredentialsFromURL(credentials, uri);

    auto timeouts = ConnectionTimeouts::getHTTPTimeouts(
        settings,
        request_context->getServerSettings());

    auto buf = BuilderRWBufferFromHTTP(uri)
                   .withConnectionGroup(HTTPConnectionGroupType::DISK)
                   .withSettings(request_context->getReadSettings())
                   .withTimeouts(timeouts)
                   .withRedirects(settings[Setting::max_http_get_redirects])
                   .withEnableUrlEncoding(enable_url_encoding)
                   .withHostFilter(&request_context->getRemoteHostFilter())
                   .withHeaders(object_storage.getHeaders())
                   .withRedirectCallback([](const Poco::URI & requested_uri, const Poco::URI & redirect_uri)
                   {
                       rejectOriginChangingRedirect(requested_uri.toString(), redirect_uri.toString());
                   })
                   .create(credentials);

    std::string body;
    WriteBufferFromString out(body);
    const auto limit = request_context->getServerSettings()[ServerSetting::max_http_index_page_size];
    copyDataMaxBytes(*buf, out, limit);
    out.finalize();
    return {.body = std::move(body), .final_url = buf->getCurrentURI().toString()};
}

std::vector<std::string> MetadataStorageFromIndexPages::extractURLs(
    const std::string & page_body,
    const std::string & listing_url,
    const std::string & listing_prefix_url,
    const std::string & base_url,
    const std::string & source_url,
    const std::string & path) const
{
    std::vector<std::string> result;
    std::unordered_set<std::string> seen_effective_relative;
    const auto & regex = getURLRegex();
    Poco::URI listing_uri(listing_url, false);
    listing_uri.normalize();
    const std::string listing_path = listing_uri.getPath();
    const Poco::URI listing_prefix_uri(listing_prefix_url, false);
    const Poco::URI base_uri(base_url, false);

    /// Apache-style ordering anchors such as `?C=N;O=D` resolve to the current listing directory with
    /// only a different query or fragment. They are not child directories; treating them as such would
    /// make recursive wildcard expansion re-fetch the same page and quickly exhaust
    /// `url_wildcard_max_directories_to_read`. Real child links such as `subdir/?token=abc` carry a
    /// distinct path component and are kept.
    auto is_self_directory_link = [&](const Poco::URI & candidate)
    {
        return candidate.getPath() == listing_path
            && (!candidate.getRawQuery().empty() || !candidate.getFragment().empty());
    };

    re2::StringPiece input(page_body);
    re2::StringPiece match;
    bool found_valid_href_url = false;

    static const re2::RE2 href_regex(R"((?i)(?:href|src)\s*=\s*['"]([^'"]+)['"])");
    re2::StringPiece href_input(page_body);
    re2::StringPiece href_match;
    while (re2::RE2::FindAndConsume(&href_input, href_regex, &href_match))
    {
        std::string url_candidate = decodeHTMLEntities(std::string(href_match.data(), href_match.size()));

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

        if (is_self_directory_link(candidate_uri))
            continue;

        if (!WebIndexPage::isSameOrigin(candidate_uri, base_uri))
            continue;

        if (!WebIndexPage::hasPathPrefix(candidate_uri, listing_prefix_uri))
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

            if (is_self_directory_link(candidate_uri))
                continue;

            if (!WebIndexPage::isSameOrigin(candidate_uri, base_uri))
                continue;

            if (!WebIndexPage::hasPathPrefix(candidate_uri, listing_prefix_uri))
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
    std::optional<size_t> requested_shard_index,
    const std::optional<String> & path_for_glob_matching) const
{
    const auto normalized_path = ensureTrailingSlashInPath(stripLeadingSlash(path));
    const auto path_prefix = getPathPrefixForMatching(path_for_glob_matching.value_or(normalized_path));
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
                auto index_page = readIndexPage(listing_url);
                rejectOriginChangingRedirect(listing_url, index_page.final_url);
                const auto final_listing_prefix_url = getListingPrefixURLForMatching(index_page.final_url);
                const auto final_path_prefix = WebIndexPage::getPathPrefixRelativeToBase(
                    Poco::URI(final_listing_prefix_url, false),
                    Poco::URI(url_options[i].base_url, false));
                auto entries = extractURLs(
                    index_page.body,
                    index_page.final_url,
                    final_listing_prefix_url,
                    url_options[i].base_url,
                    url_options[i].base_url + url_options[i].query_fragment,
                    final_path_prefix);

                result.reserve(result.size() + entries.size());
                for (auto & entry : entries)
                {
                    auto relative_path = std::make_shared<RelativePathWithMetadata>(std::move(entry), shard_index);
                    relative_path->path_for_deduplication = WebIndexPage::getEffectiveRelativePathForDeduplication(
                        relative_path->relative_path, url_options[i].base_url + url_options[i].query_fragment);
                    if (relative_path->relative_path.starts_with(final_path_prefix))
                    {
                        const auto entry_path_for_glob_matching = path_prefix + relative_path->relative_path.substr(final_path_prefix.size());
                        if (entry_path_for_glob_matching != relative_path->relative_path)
                            relative_path->path_for_glob_matching = entry_path_for_glob_matching;
                    }
                    relative_path->derive_file_name_from_url_path = true;
                    result.emplace_back(std::move(relative_path));
                }

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
